/*
 * Copyright (C) 2024 The Nanocloud Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Shared helpers for the filesystem-backed Kubernetes store.
//!
//! The store persists resource payloads under `NANOCLOUD_KEYSPACE` using
//! JSON-encoded value files. Writes are staged to temporary files and renamed
//! into place to avoid torn writes, and in-process locks keep multi-file
//! sequences from interleaving. Callers should use the helpers in this module
//! to ensure a consistent serialization format and basic atomicity when
//! touching multiple paths.

use crate::nanocloud::k8s::pod::ObjectMeta;
use crate::nanocloud::logger::log_debug;
use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::Keyspace;
use crate::nanocloud::Config;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fmt::Display;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, TryLockError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const K8S_KEYSPACE: Keyspace = Keyspace::new("k8s");
pub const STATEFULSET_PREFIX: &str = "/statefulsets";
pub const DEPLOYMENT_PREFIX: &str = "/deployments";
pub const DAEMONSET_PREFIX: &str = "/daemonsets";
pub const POD_PREFIX: &str = "/pods";
pub const CONFIGMAP_PREFIX: &str = "/configmaps";
pub const ENDPOINTS_PREFIX: &str = "/endpoints";
pub const SERVICE_PREFIX: &str = "/services";
pub const BUNDLE_PREFIX: &str = "/bundles";
pub const BUNDLE_OWNER_FILE: &str = "_owners.json";
pub const DEVICE_PREFIX: &str = "/devices";
pub const SNAPSHOT_PREFIX: &str = "/volumesnapshots";
pub const JOB_PREFIX: &str = "/jobs";
pub const KEYSPACE_VALUE_FILE: &str = "_value_";
pub const CONTROLLER_KEYSPACE_ROOT: &str = "controllers";
pub const REPLICASET_DIR: &str = "replicasets";
pub const POD_IP_ANNOTATION: &str = "nanocloud.io/pod-ip";
pub const NETWORK_POLICY_DIR: &str = "networkpolicies";
const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_RESOURCE_VERSION: &str = "1";

/// Indicates which on-disk encoding should be used for a resource payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationFormat {
    /// Plain JSON stored verbatim.
    Json,
    /// Base64-encoded JSON for cases where raw bytes are expected.
    #[allow(dead_code)]
    Base64Json,
}

/// Returns the configured serialization format for a resource.
///
/// The mapping is centralized here to avoid ad-hoc format choices across
/// managers. All k8s resources currently use JSON, but the enum leaves room
/// for format-specific migrations when needed.
pub fn serialization_format_for(resource: &str) -> SerializationFormat {
    let _ = resource;
    SerializationFormat::Json
}

/// Serializes a resource according to the configured format, wrapping
/// errors with the provided context.
pub fn serialize_for_store<T: Serialize>(
    resource: &str,
    value: &T,
    context: &str,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    match serialization_format_for(resource) {
        SerializationFormat::Json => serde_json::to_string(value)
            .map_err(|err| with_context(err, format!("{context} (json)"))),
        SerializationFormat::Base64Json => serde_json::to_string(value)
            .map(|json| BASE64_STANDARD.encode(json))
            .map_err(|err| with_context(err, format!("{context} (base64 json)"))),
    }
}

/// Deserializes a resource payload from its configured format.
pub fn deserialize_from_store<T: DeserializeOwned>(
    resource: &str,
    raw: &str,
    context: &str,
) -> Result<T, Box<dyn Error + Send + Sync>> {
    match serialization_format_for(resource) {
        SerializationFormat::Json => {
            serde_json::from_str(raw).map_err(|err| with_context(err, format!("{context} (json)")))
        }
        SerializationFormat::Base64Json => {
            let decoded = BASE64_STANDARD
                .decode(raw)
                .map_err(|err| with_context(err, format!("{context} (base64 decode)")))?;
            serde_json::from_slice(&decoded)
                .map_err(|err| with_context(err, format!("{context} (base64 json)")))
        }
    }
}

pub fn normalize_namespace(namespace: Option<&str>) -> String {
    namespace
        .filter(|ns| !ns.is_empty())
        .unwrap_or("default")
        .to_string()
}

pub fn namespaced_key(prefix: &str, namespace: Option<&str>, name: &str) -> String {
    format!("{}/{}/{}", prefix, normalize_namespace(namespace), name)
}

pub fn namespaced_root(prefix: &str) -> PathBuf {
    Config::Keyspace
        .get_path()
        .join("k8s")
        .join(prefix.trim_start_matches('/'))
}

pub fn value_file_path(prefix: &str, namespace: &str, name: &str) -> PathBuf {
    namespaced_root(prefix)
        .join(namespace)
        .join(name)
        .join(KEYSPACE_VALUE_FILE)
}

pub fn controller_root() -> PathBuf {
    Config::Keyspace.get_path().join(CONTROLLER_KEYSPACE_ROOT)
}

pub fn controller_component_root(component: &str) -> PathBuf {
    controller_root().join(component)
}

pub fn ensure_resource_version(metadata: &mut ObjectMeta) -> String {
    match metadata.resource_version.as_deref() {
        Some(rv) if !rv.trim().is_empty() => rv.to_string(),
        _ => {
            metadata.resource_version = Some(DEFAULT_RESOURCE_VERSION.to_string());
            DEFAULT_RESOURCE_VERSION.to_string()
        }
    }
}

pub fn bump_resource_version(metadata: &mut ObjectMeta) -> String {
    let next = metadata
        .resource_version
        .as_deref()
        .and_then(|rv| rv.parse::<u64>().ok())
        .map(|value| value.saturating_add(1))
        .unwrap_or(1);
    metadata.resource_version = Some(next.to_string());
    next.to_string()
}

/// Minimal opt-in in-memory cache for hot resource reads.
///
/// The cache is intended for frequently requested resources such as services
/// or endpoints. It is disabled by default and can be toggled via an
/// environment variable passed to [`HotResourceCache::new`]. Each operation is
/// instrumented with a debug log to make cache behavior easy to trace.
pub struct HotResourceCache<T> {
    name: &'static str,
    enabled: AtomicBool,
    entries: Mutex<HashMap<String, T>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HotResourceCacheMetrics {
    pub enabled: bool,
    pub hits: u64,
    pub misses: u64,
    pub entries: usize,
}

impl<T: Clone> HotResourceCache<T> {
    pub fn new(name: &'static str, env_var: &'static str) -> Self {
        let enabled = env::var(env_var)
            .map(|value| {
                matches!(
                    value.to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);

        HotResourceCache {
            name,
            enabled: AtomicBool::new(enabled),
            entries: Mutex::new(HashMap::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, key: &str) -> Option<T> {
        if !self.enabled.load(Ordering::Relaxed) {
            return None;
        }
        let guard = self.entries.lock().unwrap_or_else(|err| err.into_inner());
        let value = guard.get(key).cloned();
        if value.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
            log_debug(
                "k8s-store-cache",
                "cache hit",
                &[("cache", self.name), ("key", key)],
            );
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            log_debug(
                "k8s-store-cache",
                "cache miss",
                &[("cache", self.name), ("key", key)],
            );
        }
        value
    }

    pub fn insert(&self, key: String, value: T) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        let mut guard = self.entries.lock().unwrap_or_else(|err| err.into_inner());
        guard.insert(key.clone(), value);
        log_debug(
            "k8s-store-cache",
            "cache store",
            &[("cache", self.name), ("key", key.as_str())],
        );
    }

    pub fn invalidate(&self, key: &str) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        let mut guard = self.entries.lock().unwrap_or_else(|err| err.into_inner());
        if guard.remove(key).is_some() {
            log_debug(
                "k8s-store-cache",
                "cache invalidate",
                &[("cache", self.name), ("key", key)],
            );
        }
    }

    #[allow(dead_code)]
    pub fn clear_prefix(&self, prefix: Option<&str>) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        let mut guard = self.entries.lock().unwrap_or_else(|err| err.into_inner());
        if let Some(prefix) = prefix {
            guard.retain(|key, _| !key.starts_with(prefix));
        } else {
            guard.clear();
        }
        log_debug(
            "k8s-store-cache",
            "cache cleared",
            &[("cache", self.name), ("prefix", prefix.unwrap_or("<all>"))],
        );
    }

    #[allow(dead_code)]
    pub fn metrics(&self) -> HotResourceCacheMetrics {
        let guard = self.entries.lock().unwrap_or_else(|err| err.into_inner());
        HotResourceCacheMetrics {
            enabled: self.enabled.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            entries: guard.len(),
        }
    }
}

#[derive(Debug)]
pub enum ResourceLockError {
    Timeout { key: String, waited: Duration },
    Poisoned { key: String },
}

impl Display for ResourceLockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceLockError::Timeout { key, waited } => write!(
                f,
                "timed out acquiring lock for '{}' after {:?}",
                key, waited
            ),
            ResourceLockError::Poisoned { key } => {
                write!(f, "lock for '{}' is poisoned by previous panic", key)
            }
        }
    }
}

impl Error for ResourceLockError {}

#[derive(Debug)]
pub enum ValidationError {
    EmptyName {
        resource: &'static str,
    },
    EmptyNamespace {
        resource: &'static str,
    },
    NameMismatch {
        resource: &'static str,
        expected: String,
        found: String,
    },
    NamespaceMismatch {
        resource: &'static str,
        expected: String,
        found: String,
    },
}

impl Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::EmptyName { resource } => {
                write!(f, "{resource} name must not be empty")
            }
            ValidationError::EmptyNamespace { resource } => {
                write!(f, "{resource} namespace must not be empty when provided")
            }
            ValidationError::NameMismatch {
                resource,
                expected,
                found,
            } => write!(
                f,
                "{resource} metadata.name '{found}' does not match requested name '{expected}'"
            ),
            ValidationError::NamespaceMismatch {
                resource,
                expected,
                found,
            } => write!(
                f,
                "{resource} metadata.namespace '{}' does not match requested namespace '{}'",
                found, expected
            ),
        }
    }
}

impl Error for ValidationError {}

pub fn validate_resource_target(
    resource: &'static str,
    name: &str,
    namespace: Option<&str>,
    metadata_name: Option<&str>,
    metadata_namespace: Option<&str>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if name.trim().is_empty() {
        return Err(Box::new(ValidationError::EmptyName { resource }));
    }
    if namespace.is_some_and(|ns| ns.trim().is_empty()) {
        return Err(Box::new(ValidationError::EmptyNamespace { resource }));
    }
    if let Some(meta_name) = metadata_name {
        if meta_name != name {
            return Err(Box::new(ValidationError::NameMismatch {
                resource,
                expected: name.to_string(),
                found: meta_name.to_string(),
            }));
        }
    }

    if let Some(meta_ns) = metadata_namespace {
        if meta_ns.trim().is_empty() {
            return Err(Box::new(ValidationError::EmptyNamespace { resource }));
        }

        if let Some(requested_ns) = namespace {
            let expected = normalize_namespace(Some(requested_ns));
            let found = normalize_namespace(Some(meta_ns));
            if expected != found {
                return Err(Box::new(ValidationError::NamespaceMismatch {
                    resource,
                    expected,
                    found,
                }));
            }
        }
    }

    Ok(())
}

fn locks_registry() -> &'static Mutex<HashMap<String, Arc<Mutex<()>>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn acquire_lock_with_timeout<'a>(
    lock: &'a Arc<Mutex<()>>,
    key: &str,
    timeout: Duration,
) -> Result<std::sync::MutexGuard<'a, ()>, ResourceLockError> {
    let start = Instant::now();
    loop {
        match lock.try_lock() {
            Ok(guard) => return Ok(guard),
            Err(TryLockError::Poisoned(_)) => {
                return Err(ResourceLockError::Poisoned {
                    key: key.to_string(),
                })
            }
            Err(TryLockError::WouldBlock) => {
                if start.elapsed() >= timeout {
                    return Err(ResourceLockError::Timeout {
                        key: key.to_string(),
                        waited: timeout,
                    });
                }
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

/// Executes `work` while holding a process-wide mutex scoped to `key`.
///
/// This guards multi-file write sequences that cannot rely solely on the
/// underlying keyspace lock, preventing concurrent callers from interleaving
/// updates to the same logical resource.
pub fn with_resource_lock<T, F>(key: &str, work: F) -> Result<T, Box<dyn Error + Send + Sync>>
where
    F: FnOnce() -> Result<T, Box<dyn Error + Send + Sync>>,
{
    with_resource_lock_timeout(key, DEFAULT_LOCK_TIMEOUT, work)
}

pub fn with_resource_lock_timeout<T, F>(
    key: &str,
    timeout: Duration,
    work: F,
) -> Result<T, Box<dyn Error + Send + Sync>>
where
    F: FnOnce() -> Result<T, Box<dyn Error + Send + Sync>>,
{
    let lock_arc = {
        let mut registry = locks_registry()
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        registry
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };

    let guard = acquire_lock_with_timeout(&lock_arc, key, timeout)
        .map_err(|err| -> Box<dyn Error + Send + Sync> { Box::new(err) })?;
    let result = work();
    drop(guard);
    result
}

fn ownership_path(prefix: &str, namespace: Option<&str>, name: &str, file: &str) -> PathBuf {
    namespaced_root(prefix)
        .join(normalize_namespace(namespace))
        .join(name)
        .join(file)
}

pub fn load_ownership<T>(
    prefix: &str,
    namespace: Option<&str>,
    name: &str,
    file: &str,
) -> Result<T, Box<dyn Error + Send + Sync>>
where
    T: DeserializeOwned + Default,
{
    let path = ownership_path(prefix, namespace, name, file);
    match fs::read_to_string(&path) {
        Ok(contents) => serde_json::from_str(&contents).map_err(|err| {
            with_context(
                err,
                format!("Failed to parse ownership metadata '{}'", path.display()),
            )
        }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(T::default()),
        Err(err) => Err(with_context(
            err,
            format!("Failed to read ownership metadata '{}'", path.display()),
        )),
    }
}

pub fn save_ownership<T>(
    prefix: &str,
    namespace: Option<&str>,
    name: &str,
    file: &str,
    value: &T,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    T: Serialize,
{
    let path = ownership_path(prefix, namespace, name, file);
    let payload = serde_json::to_string_pretty(value).map_err(|err| {
        with_context(
            err,
            format!(
                "Failed to serialize ownership metadata for '{}/{}'",
                namespace.unwrap_or("default"),
                name
            ),
        )
    })?;
    write_atomic_files(&[(&path, payload.as_str())]).map_err(|err| {
        with_context(
            err,
            format!("Failed to persist ownership metadata '{}'", path.display()),
        )
    })
}

pub fn delete_ownership(
    prefix: &str,
    namespace: Option<&str>,
    name: &str,
    file: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let path = ownership_path(prefix, namespace, name, file);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(with_context(
            err,
            format!("Failed to delete ownership metadata '{}'", path.display()),
        )),
    }
}

/// Writes the provided `(path, contents)` pairs by staging to temporary files
/// and renaming them into place.
///
/// This pattern ensures readers never observe partially written payloads. Callers
/// should prefer this helper when persisting multiple related files (e.g. value
/// and ownership metadata) to keep updates consistent.
pub fn write_atomic_files(writes: &[(&Path, &str)]) -> Result<(), Box<dyn Error + Send + Sync>> {
    if writes.is_empty() {
        return Ok(());
    }

    let mut staged = Vec::with_capacity(writes.len());
    for (index, (final_path, contents)) in writes.iter().enumerate() {
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to create parent directory '{}' for '{}'",
                        parent.display(),
                        final_path.display()
                    ),
                )
            })?;
        }

        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let temp_name = final_path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| format!(".{name}.tmp{suffix}{index}"))
            .unwrap_or_else(|| format!(".tmp{suffix}{index}"));
        let temp_path = final_path
            .parent()
            .map(|parent| parent.join(&temp_name))
            .unwrap_or_else(|| PathBuf::from(&temp_name));

        let mut file = fs::File::create(&temp_path).map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to create temporary file '{}' for '{}'",
                    temp_path.display(),
                    final_path.display()
                ),
            )
        })?;
        file.write_all(contents.as_bytes()).map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to write temporary file '{}' for '{}'",
                    temp_path.display(),
                    final_path.display()
                ),
            )
        })?;
        file.sync_all().map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to sync temporary file '{}' for '{}'",
                    temp_path.display(),
                    final_path.display()
                ),
            )
        })?;

        staged.push((temp_path, final_path.to_path_buf()));
    }

    for (temp_path, final_path) in &staged {
        fs::rename(temp_path, final_path).map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to atomically replace '{}' with '{}'",
                    final_path.display(),
                    temp_path.display()
                ),
            )
        })?;
    }

    // Best-effort cleanup of any remaining temporary files.
    for (temp_path, _) in staged {
        let _ = fs::remove_file(temp_path);
    }

    Ok(())
}
