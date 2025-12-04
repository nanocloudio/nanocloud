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

use crate::nanocloud::cni::provider::CniPlugin;
use crate::nanocloud::cni::{cni_plugin, CniResult};
use crate::nanocloud::controller::runtime::ControllerRuntime;
use crate::nanocloud::dns::DnsService;
use crate::nanocloud::engine::log::{container_log_dir, set_container_log_root};
use crate::nanocloud::engine::Profile;
use crate::nanocloud::k8s::pod::{ContainerProbe, PodSpec, ProbeExec};
use crate::nanocloud::k8s::store as pod_store;
use crate::nanocloud::observability::metrics;
use crate::nanocloud::oci::distribution::{load_manifest_from_store, parse_image_reference};
use crate::nanocloud::oci::runtime::{
    container_base_dir, container_refs_dir, container_root_path, netns_dir, ContainerStatus,
    ExecRequest,
};
use crate::nanocloud::oci::runtime_provider::ContainerRuntime;
use crate::nanocloud::oci::{container_runtime, image_store_root, OciImage, OciManifest};
use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::util::security::{EncryptionKey, TlsInfo};

use crate::nanocloud::logger::{log_error, log_info, log_warn};
use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::error::Error;
use std::ffi::{CStr, CString};
use std::fs::OpenOptions;
use std::future::Future;
use std::io::{self, BufReader, ErrorKind, Write};
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::os::fd::IntoRawFd;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::ptr;
use std::str;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::spawn_blocking;
use tokio::time::{sleep, timeout, Instant};

use nix::fcntl::{open, OFlag};
use nix::sys::stat::Mode;
use nix::unistd::geteuid;

const DEFAULT_BRIDGE_DNS_ADDR: &str = "172.20.0.1";
const DEFAULT_CNI_TIMEOUT: Duration = Duration::from_secs(20);
const DEFAULT_ROOTFS_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_OCI_CONFIG_TIMEOUT: Duration = Duration::from_secs(20);
const DEFAULT_RUNTIME_CREATE_TIMEOUT: Duration = Duration::from_secs(20);
const DEFAULT_METADATA_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_ROLLBACK_STEP_TIMEOUT: Duration = Duration::from_secs(10);

/// Timeout configuration for kubelet container lifecycle phases.
#[derive(Clone, Debug)]
pub struct ContainerRuntimeConfig {
    pub cni_timeout: Duration,
    pub rootfs_timeout: Duration,
    pub oci_prep_timeout: Duration,
    pub runtime_create_timeout: Duration,
    pub metadata_timeout: Duration,
}

impl Default for ContainerRuntimeConfig {
    fn default() -> Self {
        Self {
            cni_timeout: DEFAULT_CNI_TIMEOUT,
            rootfs_timeout: DEFAULT_ROOTFS_TIMEOUT,
            oci_prep_timeout: DEFAULT_OCI_CONFIG_TIMEOUT,
            runtime_create_timeout: DEFAULT_RUNTIME_CREATE_TIMEOUT,
            metadata_timeout: DEFAULT_METADATA_TIMEOUT,
        }
    }
}

fn ensure_cni_prerequisites() -> Result<(), Box<dyn Error + Send + Sync>> {
    let allow_unprivileged = std::env::var("NANOCLOUD_CNI_ALLOW_UNPRIVILEGED")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if !allow_unprivileged && geteuid().as_raw() != 0 {
        return Err(new_error(
            "CNI provisioning requires root privileges; set NANOCLOUD_CNI_ALLOW_UNPRIVILEGED=1 to override for testing",
        ));
    }

    let netns_path = netns_dir();
    let metadata = std::fs::metadata(&netns_path).map_err(|err| {
        with_context(
            err,
            format!(
                "Network namespace directory {} is not accessible",
                netns_path.display()
            ),
        )
    })?;
    if !metadata.is_dir() {
        return Err(new_error(format!(
            "Network namespace path {} is not a directory",
            netns_path.display()
        )));
    }

    Ok(())
}

/// Host path configuration for kubelet runtime assets and container logs.
#[derive(Clone, Debug)]
pub struct KubeletPathConfig {
    pub runtime_dir: PathBuf,
    pub log_root: PathBuf,
}

impl Default for KubeletPathConfig {
    fn default() -> Self {
        Self {
            runtime_dir: PathBuf::from("/var/run/nanocloud.io"),
            log_root: std::env::var("NANOCLOUD_LOG_ROOT")
                .map(PathBuf::from)
                .unwrap_or_else(|_| container_base_dir()),
        }
    }
}

impl KubeletPathConfig {
    pub fn load() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let config = ControllerRuntime::shared()
            .dependency::<KubeletPathConfig>()
            .as_deref()
            .cloned()
            .unwrap_or_default();
        config.validated()
    }

    fn validated(self) -> Result<Self, Box<dyn Error + Send + Sync>> {
        validate_directory(&self.runtime_dir, "runtime directory")?;
        validate_directory(&self.log_root, "log root")?;
        // Ensure the log root override is applied for downstream log helpers.
        set_container_log_root(self.log_root.clone())
            .map_err(|err| with_context(err, "Failed to configure container log root"))?;
        Ok(self)
    }
}

async fn rollback_creation(context: CreationContext, container_name: &str) {
    rollback_creation_inner(context, container_name, RollbackHandles::default()).await;
}

async fn rollback_creation_inner(
    context: CreationContext,
    container_name: &str,
    handles: RollbackHandles,
) {
    let mut rollback_steps = Vec::new();

    if context.container_created {
        let container_id = context.container_id.clone();
        let runtime = handles.runtime.clone();
        rollback_steps.push((
            "runtime_container",
            spawn_blocking(move || {
                runtime.delete(&container_id).map_err(|err| {
                    with_context(err, format!("Failed to delete container {}", container_id))
                })
            }),
        ));
    }

    if context.network_provisioned && !context.host_network {
        let container_id = context.container_id.clone();
        let cni = handles.cni.clone();
        rollback_steps.push((
            "cni",
            spawn_blocking(move || {
                let cni_args: HashMap<String, String> = [
                    ("CNI_COMMAND", "DEL"),
                    ("CNI_CONTAINERID", container_id.as_str()),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
                ensure_cni_prerequisites()?;
                metrics::observe_cni_operation(metrics::CniOperation::Delete, || {
                    cni.delete(&cni_args)
                })
                .map_err(|err| with_context(err, "Failed to rollback CNI network"))
            }),
        ));
    }

    if let Some(base) = context.rootfs_base.clone() {
        rollback_steps.push(("rootfs", spawn_blocking(move || cleanup_rootfs(&base))));
    }

    if let Some(link) = context.name_link.clone() {
        rollback_steps.push((
            "name_link",
            spawn_blocking(move || {
                if let Err(err) = std::fs::remove_file(&link) {
                    if err.kind() != ErrorKind::NotFound {
                        return Err(with_context(
                            Box::new(err),
                            format!("Failed to remove container name link {}", link.display()),
                        ));
                    }
                }
                Ok(())
            }),
        ));
    }

    for (label, task) in rollback_steps {
        tokio::pin!(task);
        match timeout(DEFAULT_ROLLBACK_STEP_TIMEOUT, &mut task).await {
            Ok(Ok(Ok(()))) => {
                debug!(
                    "Rollback step '{}' succeeded for container {}",
                    label, context.container_id
                );
            }
            Ok(Ok(Err(err))) => {
                log_warn(
                    "kubelet",
                    "Rollback step failed",
                    &[
                        ("container", container_name),
                        ("id", context.container_id.as_str()),
                        ("step", label),
                        ("error", err.to_string().as_str()),
                    ],
                );
            }
            Ok(Err(join_err)) => {
                log_warn(
                    "kubelet",
                    "Rollback task join error",
                    &[
                        ("container", container_name),
                        ("id", context.container_id.as_str()),
                        ("step", label),
                        ("error", join_err.to_string().as_str()),
                    ],
                );
            }
            Err(_elapsed) => {
                task.as_mut().abort();
                let _ = task.await;
                let timeout_secs = DEFAULT_ROLLBACK_STEP_TIMEOUT.as_secs().to_string();
                log_warn(
                    "kubelet",
                    "Rollback step timed out",
                    &[
                        ("container", container_name),
                        ("id", context.container_id.as_str()),
                        ("step", label),
                        ("timeout", timeout_secs.as_str()),
                        ("error", "deadline exceeded"),
                    ],
                );
            }
        }
    }
}

#[cfg(test)]
async fn rollback_creation_with_handles(
    context: CreationContext,
    container_name: &str,
    handles: RollbackHandles,
) {
    rollback_creation_inner(context, container_name, handles).await;
}

fn cleanup_rootfs(base: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
    let rootfs = base.join("rootfs");
    if rootfs.exists() {
        let _ = nix::mount::umount(&rootfs);
    }
    if let Err(err) = std::fs::remove_dir_all(base) {
        if err.kind() != ErrorKind::NotFound {
            return Err(with_context(
                Box::new(err),
                format!("Failed to remove container root {}", base.display()),
            ));
        }
    }
    Ok(())
}

fn random_container_id() -> Result<String, Box<dyn Error + Send + Sync>> {
    String::from_utf8(EncryptionKey::gen_random_bytes(32, "hex").to_vec())
        .map_err(|_| new_error("Failed to convert random bytes to String"))
}

fn generate_container_id_with<F>(mut id_source: F) -> Result<String, Box<dyn Error + Send + Sync>>
where
    F: FnMut() -> Result<String, Box<dyn Error + Send + Sync>>,
{
    const MAX_ATTEMPTS: usize = 3;
    for attempt in 0..MAX_ATTEMPTS {
        let candidate = id_source()?;

        let valid_chars = candidate
            .chars()
            .all(|ch| ch.is_ascii_digit() || (ch.is_ascii_lowercase() && ch.is_ascii_hexdigit()));
        if !valid_chars || candidate.len() != 64 {
            return Err(new_error(format!(
                "Generated container ID '{}' is not a 64-character lowercase hex string",
                candidate
            )));
        }

        let base = container_root_path(&candidate);
        if base.exists() {
            if attempt + 1 == MAX_ATTEMPTS {
                return Err(new_error(
                    "Container ID collision detected after maximum retries",
                ));
            }
            continue;
        }

        return Ok(candidate);
    }

    Err(new_error("Unable to generate container ID after retries"))
}

fn generate_container_id() -> Result<String, Box<dyn Error + Send + Sync>> {
    generate_container_id_with(random_container_id)
}

impl ContainerRuntimeConfig {
    fn load() -> Self {
        ControllerRuntime::shared()
            .dependency::<ContainerRuntimeConfig>()
            .as_deref()
            .cloned()
            .unwrap_or_default()
            .validated()
    }

    fn validated(mut self) -> Self {
        if self.cni_timeout.is_zero() {
            self.cni_timeout = DEFAULT_CNI_TIMEOUT;
        }
        if self.rootfs_timeout.is_zero() {
            self.rootfs_timeout = DEFAULT_ROOTFS_TIMEOUT;
        }
        if self.oci_prep_timeout.is_zero() {
            self.oci_prep_timeout = DEFAULT_OCI_CONFIG_TIMEOUT;
        }
        if self.runtime_create_timeout.is_zero() {
            self.runtime_create_timeout = DEFAULT_RUNTIME_CREATE_TIMEOUT;
        }
        if self.metadata_timeout.is_zero() {
            self.metadata_timeout = DEFAULT_METADATA_TIMEOUT;
        }
        self
    }
}

fn validate_directory(path: &Path, label: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    std::fs::create_dir_all(path).map_err(|err| {
        with_context(
            err,
            format!("Failed to create {} {}", label, path.display()),
        )
    })?;
    let metadata = std::fs::metadata(path).map_err(|err| {
        with_context(
            err,
            format!("Failed to read {} metadata at {}", label, path.display()),
        )
    })?;
    if !metadata.is_dir() {
        return Err(new_error(format!(
            "{} {} is not a directory",
            label,
            path.display()
        )));
    }

    let probe = path.join(".nanocloud-write-check");
    match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&probe)
    {
        Ok(_) => {
            let _ = std::fs::remove_file(&probe);
            Ok(())
        }
        Err(err) => Err(with_context(
            err,
            format!("{} {} is not writable", label, path.display()),
        )),
    }
}

#[derive(Clone, Copy, Debug)]
enum CreationPhase {
    IdGeneration,
    Network,
    Rootfs,
    OciConfig,
    RuntimeCreate,
    Metadata,
}

impl CreationPhase {
    fn as_str(&self) -> &'static str {
        match self {
            CreationPhase::IdGeneration => "id_generation",
            CreationPhase::Network => "network",
            CreationPhase::Rootfs => "rootfs",
            CreationPhase::OciConfig => "oci_config",
            CreationPhase::RuntimeCreate => "runtime_create",
            CreationPhase::Metadata => "metadata",
        }
    }
}

pub trait ContainerCreationHook: Send + Sync + 'static {
    fn phase_started(&self, _phase: &str, _container: &str, _host_network: bool) {}

    fn phase_succeeded(
        &self,
        _phase: &str,
        _container: &str,
        _host_network: bool,
        _elapsed: Duration,
    ) {
    }

    fn phase_failed(
        &self,
        _phase: &str,
        _container: &str,
        _host_network: bool,
        _error: &str,
        _elapsed: Duration,
    ) {
    }
}

#[derive(Clone)]
pub struct ContainerCreationHooks {
    hook: Arc<dyn ContainerCreationHook>,
}

impl ContainerCreationHooks {
    #[allow(dead_code)]
    pub fn new(hook: Arc<dyn ContainerCreationHook>) -> Self {
        Self { hook }
    }

    fn handler(&self) -> Arc<dyn ContainerCreationHook> {
        Arc::clone(&self.hook)
    }
}

impl Default for ContainerCreationHooks {
    fn default() -> Self {
        Self {
            hook: Arc::new(LoggingContainerCreationHook),
        }
    }
}

#[derive(Clone)]
struct LoggingContainerCreationHook;

impl ContainerCreationHook for LoggingContainerCreationHook {
    fn phase_started(&self, phase: &str, container: &str, host_network: bool) {
        log_info(
            "kubelet",
            "Container phase started",
            &[
                ("phase", phase),
                ("container", container),
                ("host_network", if host_network { "true" } else { "false" }),
            ],
        );
    }

    fn phase_succeeded(&self, phase: &str, container: &str, host_network: bool, elapsed: Duration) {
        let elapsed_ms = elapsed.as_millis().to_string();
        log_info(
            "kubelet",
            "Container phase completed",
            &[
                ("phase", phase),
                ("container", container),
                ("host_network", if host_network { "true" } else { "false" }),
                ("elapsed_ms", elapsed_ms.as_str()),
            ],
        );
    }

    fn phase_failed(
        &self,
        phase: &str,
        container: &str,
        host_network: bool,
        error: &str,
        elapsed: Duration,
    ) {
        let elapsed_ms = elapsed.as_millis().to_string();
        log_error(
            "kubelet",
            "Container phase failed",
            &[
                ("phase", phase),
                ("container", container),
                ("host_network", if host_network { "true" } else { "false" }),
                ("elapsed_ms", elapsed_ms.as_str()),
                ("error", error),
            ],
        );
    }
}

fn creation_hooks() -> ContainerCreationHooks {
    ControllerRuntime::shared()
        .dependency::<ContainerCreationHooks>()
        .as_deref()
        .cloned()
        .unwrap_or_default()
}

struct CreationContext {
    container_id: String,
    host_network: bool,
    network_provisioned: bool,
    rootfs_base: Option<PathBuf>,
    container_created: bool,
    name_link: Option<PathBuf>,
}

impl CreationContext {
    fn new(container_id: String, host_network: bool) -> Self {
        Self {
            container_id,
            host_network,
            network_provisioned: false,
            rootfs_base: None,
            container_created: false,
            name_link: None,
        }
    }

    fn mark_network(&mut self) {
        self.network_provisioned = true;
    }

    fn mark_rootfs(&mut self, base: PathBuf) {
        self.rootfs_base = Some(base);
    }

    fn mark_container_created(&mut self) {
        self.container_created = true;
    }

    fn mark_name_link(&mut self, link: PathBuf) {
        self.name_link = Some(link);
    }
}

#[derive(Clone)]
struct RollbackHandles {
    runtime: Arc<dyn ContainerRuntime>,
    cni: Arc<dyn CniPlugin>,
}

impl Default for RollbackHandles {
    fn default() -> Self {
        Self {
            runtime: container_runtime(),
            cni: cni_plugin(),
        }
    }
}

fn wrap_phase_error(
    err: Box<dyn Error + Send + Sync>,
    phase: CreationPhase,
    container_id: &str,
    container_name: &str,
) -> Box<dyn Error + Send + Sync> {
    with_context(
        err,
        format!(
            "Container {} (id {}) {} phase failed",
            container_name,
            container_id,
            phase.as_str()
        ),
    )
}

async fn run_async_phase<F, T>(
    phase: CreationPhase,
    container_id: &str,
    container_name: &str,
    host_network: bool,
    timeout_duration: Duration,
    hooks: &ContainerCreationHooks,
    fut: F,
) -> Result<T, Box<dyn Error + Send + Sync>>
where
    F: Future<Output = Result<T, Box<dyn Error + Send + Sync>>>,
{
    let handler = hooks.handler();
    handler.phase_started(phase.as_str(), container_name, host_network);
    let start = Instant::now();
    match timeout(timeout_duration, fut).await {
        Ok(inner) => match inner {
            Ok(value) => {
                handler.phase_succeeded(
                    phase.as_str(),
                    container_name,
                    host_network,
                    start.elapsed(),
                );
                Ok(value)
            }
            Err(err) => {
                let wrapped = wrap_phase_error(err, phase, container_id, container_name);
                handler.phase_failed(
                    phase.as_str(),
                    container_name,
                    host_network,
                    &wrapped.to_string(),
                    start.elapsed(),
                );
                Err(wrapped)
            }
        },
        Err(_) => {
            let wrapped = wrap_phase_error(
                new_error(format!(
                    "{} phase exceeded {:?} timeout",
                    phase.as_str(),
                    timeout_duration
                )),
                phase,
                container_id,
                container_name,
            );
            handler.phase_failed(
                phase.as_str(),
                container_name,
                host_network,
                &wrapped.to_string(),
                start.elapsed(),
            );
            Err(wrapped)
        }
    }
}

async fn run_blocking_phase<T, F>(
    phase: CreationPhase,
    container_id: &str,
    container_name: &str,
    host_network: bool,
    timeout_duration: Duration,
    hooks: &ContainerCreationHooks,
    f: F,
) -> Result<T, Box<dyn Error + Send + Sync>>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, Box<dyn Error + Send + Sync>> + Send + 'static,
{
    let handler = hooks.handler();
    handler.phase_started(phase.as_str(), container_name, host_network);
    let start = Instant::now();
    let mut handle = spawn_blocking(f);
    let timer = sleep(timeout_duration);
    tokio::pin!(timer);
    tokio::select! {
        result = &mut handle => {
            let elapsed = start.elapsed();
            match result {
                Ok(inner) => match inner {
                    Ok(value) => {
                        handler.phase_succeeded(phase.as_str(), container_name, host_network, elapsed);
                        Ok(value)
                    }
                    Err(err) => {
                        let wrapped = wrap_phase_error(err, phase, container_id, container_name);
                        handler.phase_failed(phase.as_str(), container_name, host_network, &wrapped.to_string(), elapsed);
                        Err(wrapped)
                    }
                },
                Err(join_err) => {
                    let wrapped = wrap_phase_error(
                        new_error(format!(
                            "Blocking task for {} failed: {}",
                            phase.as_str(),
                            join_err
                        )),
                        phase,
                        container_id,
                        container_name,
                    );
                    handler.phase_failed(phase.as_str(), container_name, host_network, &wrapped.to_string(), elapsed);
                    Err(wrapped)
                }
            }
        }
        _ = &mut timer => {
            handle.abort();
            let elapsed = start.elapsed();
            let wrapped = wrap_phase_error(
                new_error(format!(
                    "{} phase exceeded {:?} timeout",
                    phase.as_str(),
                    timeout_duration
                )),
                phase,
                container_id,
                container_name,
            );
            handler.phase_failed(phase.as_str(), container_name, host_network, &wrapped.to_string(), elapsed);
            Err(wrapped)
        }
    }
}

/// Builds a resolv.conf suitable for the container namespace using the DNS service or a host override.
///
/// If `host_resolv` is provided, the contents are validated and returned verbatim; otherwise the
/// cluster DNS settings are rendered. Returns `None` when dependencies are missing or validation fails.
pub fn build_resolv_conf(host_resolv: Option<&str>) -> Option<String> {
    if let Some(path) = host_resolv {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                if let Err(err) = validate_resolv_conf_contents(&contents) {
                    log_error(
                        "kubelet",
                        "Host resolv.conf validation failed",
                        &[("path", path), ("error", err.to_string().as_str())],
                    );
                    return None;
                }
                return Some(contents);
            }
            Err(err) => {
                log_error(
                    "kubelet",
                    "Failed to read host resolv.conf for override",
                    &[("path", path), ("error", err.to_string().as_str())],
                );
                return None;
            }
        }
    }

    let dns = ControllerRuntime::shared().dependency::<DnsService>();
    let Some(dns) = dns else {
        log_error(
            "kubelet",
            "DNS service unavailable; cannot generate resolv.conf",
            &[("dependency", "DnsService")],
        );
        return None;
    };

    let config = dns.config().clone();
    let nameserver = match config.listen_address {
        IpAddr::V4(addr) if !addr.is_unspecified() => addr.to_string(),
        IpAddr::V6(addr) if !addr.is_unspecified() => addr.to_string(),
        _ => {
            log_warn(
                "kubelet",
                "DNS listen address unspecified; falling back to bridge address",
                &[("default", DEFAULT_BRIDGE_DNS_ADDR)],
            );
            DEFAULT_BRIDGE_DNS_ADDR.to_string()
        }
    };
    let domain = config.cluster_domain.trim();
    if domain.is_empty() {
        log_error(
            "kubelet",
            "Cluster domain is empty; cannot generate resolv.conf",
            &[("dependency", "DnsService")],
        );
        return None;
    }

    let contents = format!(
        "nameserver {nameserver}
search svc.{domain} {domain}
options ndots:5
"
    );
    if let Err(err) = validate_resolv_conf_contents(&contents) {
        log_error(
            "kubelet",
            "Generated resolv.conf failed validation",
            &[("error", err.to_string().as_str())],
        );
        return None;
    }

    Some(contents)
}

fn validate_resolv_conf_contents(contents: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut nameservers = 0usize;
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        match parts.next() {
            Some("nameserver") => {
                let Some(addr) = parts.next() else {
                    return Err(new_error("nameserver directive missing address"));
                };
                addr.parse::<IpAddr>()
                    .map_err(|_| new_error(format!("Invalid nameserver address: {addr}")))?;
                nameservers += 1;
            }
            Some("search") => {
                let domains: Vec<&str> = parts.collect();
                if domains.is_empty() {
                    return Err(new_error("search directive missing domains"));
                }
                if domains.iter().any(|domain| !is_valid_search_domain(domain)) {
                    return Err(new_error("search directive contains invalid domain"));
                }
            }
            _ => {}
        }
    }

    if nameservers == 0 {
        return Err(new_error(
            "resolv.conf must include at least one nameserver entry",
        ));
    }

    Ok(())
}

fn is_valid_search_domain(domain: &str) -> bool {
    if domain.is_empty() {
        return false;
    }
    domain
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '.')
}

/// Describes a created container and the artifacts needed to start it.
#[derive(Deserialize, Serialize)]
pub struct Container {
    pub id: String,
    pub name: String,
    pub cni_config: Option<CniResult>,
    pub oci_image: OciImage,
    pub image_manifest: OciManifest,
}

/// Creates the container bundle, network, and metadata for the provided pod specification.
///
/// All blocking work (CNI, filesystem, OCI config) is performed with timeouts, and any failure
/// triggers rollback of already-created artifacts.
pub async fn create_container(
    container_name: &str,
    oci_image: &OciImage,
    oci_manifest: &OciManifest,
    pod_spec: &PodSpec,
) -> Result<Container, Box<dyn Error + Send + Sync>> {
    log_info(
        "kubelet",
        "Creating OCI container",
        &[("container", container_name)],
    );
    let config = ContainerRuntimeConfig::load();
    let hooks = creation_hooks();

    let container_id = run_async_phase(
        CreationPhase::IdGeneration,
        "<pending>",
        container_name,
        pod_spec.host_network,
        config.metadata_timeout,
        &hooks,
        async { generate_container_id() },
    )
    .await?;
    let mut context = CreationContext::new(container_id.clone(), pod_spec.host_network);

    let main_container = pod_spec.containers.first().ok_or_else(|| {
        new_error(format!(
            "Pod spec for {} does not define a container",
            container_name
        ))
    })?;

    let mut cni_result = None;
    if pod_spec.host_network {
        log_info(
            "kubelet",
            "Skipping CNI provisioning for host-networked container",
            &[("container", container_name)],
        );
    } else {
        let id = container_id.clone();
        let spec = pod_spec.clone();
        let name = container_name.to_string();
        cni_result = Some(
            run_blocking_phase(
                CreationPhase::Network,
                &container_id,
                container_name,
                pod_spec.host_network,
                config.cni_timeout,
                &hooks,
                move || provision_container_network(&id, &name, &spec),
            )
            .await?,
        );
        context.mark_network();
    }

    log_info(
        "kubelet",
        "Preparing container root filesystem",
        &[("container", container_name)],
    );
    let rootfs_base = container_root_path(&container_id);
    context.mark_rootfs(rootfs_base.clone());
    context.mark_name_link(container_refs_dir().join(container_name));
    let rootfs_id = container_id.clone();
    let rootfs_manifest = oci_manifest.clone();
    let rootfs_name = container_name.to_string();
    let rootfs_phase = run_blocking_phase(
        CreationPhase::Rootfs,
        &container_id,
        container_name,
        pod_spec.host_network,
        config.rootfs_timeout,
        &hooks,
        move || create_rootfs(&rootfs_id, &rootfs_name, &rootfs_manifest),
    )
    .await;
    if let Err(err) = rootfs_phase {
        rollback_creation(context, container_name).await;
        return Err(err);
    }

    let bundle_path = container_root_path(&container_id);
    let oci_args: HashMap<String, String> = HashMap::from([(
        "OCI_BUNDLE".to_string(),
        bundle_path.to_string_lossy().to_string(),
    )]);
    let runtime = container_runtime();
    let oci_config = runtime.configure_from_spec(
        &container_id,
        container_name,
        main_container,
        &pod_spec.volumes,
        pod_spec.host_network,
        &pod_spec.security,
    );
    let config_json = serde_json::to_string(&oci_config)
        .map_err(|e| with_context(e, "Failed to serialize OCI runtime config"))?;
    let config_path = bundle_path.join("config.json");
    let oci_phase = run_blocking_phase(
        CreationPhase::OciConfig,
        &container_id,
        container_name,
        pod_spec.host_network,
        config.oci_prep_timeout,
        &hooks,
        {
            let config_path = config_path.clone();
            let config_json = config_json.clone();
            move || {
                let config_dir = config_path.parent().ok_or_else(|| {
                    new_error(format!(
                        "OCI configuration path {} has no parent directory",
                        config_path.display()
                    ))
                })?;
                std::fs::create_dir_all(config_dir).map_err(|e| {
                    with_context(
                        e,
                        format!(
                            "Failed to create OCI config directory {}",
                            config_dir.display()
                        ),
                    )
                })?;
                std::fs::File::create(&config_path)
                    .map_err(|e| {
                        with_context(
                            e,
                            format!("Failed to create OCI config file {}", config_path.display()),
                        )
                    })?
                    .write_all(config_json.as_bytes())
                    .map_err(|e| {
                        with_context(
                            e,
                            format!("Failed to write OCI config file {}", config_path.display()),
                        )
                    })?;
                Ok(())
            }
        },
    )
    .await;
    if let Err(err) = oci_phase {
        rollback_creation(context, container_name).await;
        return Err(err);
    }

    let runtime_phase = run_blocking_phase(
        CreationPhase::RuntimeCreate,
        &container_id,
        container_name,
        pod_spec.host_network,
        config.runtime_create_timeout,
        &hooks,
        {
            let runtime = runtime.clone();
            let id = container_id.clone();
            let args = oci_args.clone();
            let config_bytes = config_json.clone().into_bytes();
            move || {
                runtime
                    .create(&id, &args, config_bytes.clone())
                    .map_err(|e| with_context(e, format!("Failed to create OCI container {}", id)))
            }
        },
    )
    .await;
    if let Err(err) = runtime_phase {
        rollback_creation(context, container_name).await;
        return Err(err);
    }
    context.mark_container_created();
    log_info(
        "kubelet",
        "OCI container created",
        &[("container", container_name), ("id", container_id.as_str())],
    );

    let digest_phase = run_blocking_phase(
        CreationPhase::Metadata,
        &container_id,
        container_name,
        pod_spec.host_network,
        config.metadata_timeout,
        &hooks,
        {
            let bundle = bundle_path.clone();
            let digest = oci_manifest.config.digest.clone();
            move || {
                std::fs::File::create(bundle.join("image_digest"))
                    .map_err(|e| with_context(e, "Failed to create image digest file"))?
                    .write_all(digest.as_bytes())
                    .map_err(|e| with_context(e, "Failed to write image digest"))?;
                Ok(())
            }
        },
    )
    .await;
    if let Err(err) = digest_phase {
        rollback_creation(context, container_name).await;
        return Err(err);
    }

    Ok(Container {
        id: container_id.clone(),
        name: container_name.to_string(),
        cni_config: cni_result.clone(),
        oci_image: oci_image.clone(),
        image_manifest: oci_manifest.clone(),
    })
}

fn provision_container_network(
    container_id: &str,
    container_name: &str,
    pod_spec: &PodSpec,
) -> Result<CniResult, Box<dyn Error + Send + Sync>> {
    ensure_cni_prerequisites()?;
    let netns_path = netns_dir().join(container_id);
    let mut cni_args: HashMap<String, String> = HashMap::new();
    cni_args.insert("CNI_COMMAND".to_string(), "ADD".to_string());
    cni_args.insert("CNI_CONTAINERID".to_string(), container_id.to_string());
    cni_args.insert(
        "CNI_NETNS".to_string(),
        netns_path.to_string_lossy().to_string(),
    );
    cni_args.insert("CNI_IFNAME".to_string(), "nanocloud0".to_string());
    cni_args.insert("CNI_PATH".to_string(), "/opt/cni/bin".to_string());

    let port_mappings: Vec<_> = pod_spec
        .containers
        .iter()
        .flat_map(|container| container.ports.iter())
        .filter_map(|port| {
            port.host_port.map(|host_port| {
                let mut mapping = json!({
                    "hostPort": host_port,
                    "containerPort": port.container_port,
                });
                if let Some(protocol) = port.protocol.as_deref() {
                    if !protocol.is_empty() {
                        mapping["protocol"] = json!(protocol.to_lowercase());
                    }
                }
                if let Some(host_ip) = port.host_ip.as_deref() {
                    if !host_ip.trim().is_empty() {
                        mapping["hostIP"] = json!(host_ip);
                    }
                }
                mapping
            })
        })
        .collect();

    let mut cni_config = json!({
        "cniVersion": "1.0.0",
        "name": "nanocloud",
        "type": "bridge",
        "bridge": "nanocloud0",
    });
    if !port_mappings.is_empty() {
        cni_config["runtimeConfig"] = json!({ "portMappings": port_mappings });
    }
    let cni_config = serde_json::to_string(&cni_config)?;
    let plugin = cni_plugin();
    let result = metrics::observe_cni_operation(metrics::CniOperation::Add, || {
        plugin.add(&cni_args, cni_config.clone().into_bytes())
    })?;
    let cni_json = serde_json::to_string(&result)
        .map_err(|e| with_context(e, "Failed to serialize CNI result"))?;
    log_info(
        "kubelet",
        "CNI provisioning result",
        &[("container", container_name), ("result", cni_json.as_str())],
    );
    Ok(result)
}

async fn provision_container_network_blocking(
    container_id: &str,
    container_name: &str,
    pod_spec: &PodSpec,
) -> Result<CniResult, Box<dyn Error + Send + Sync>> {
    let config = ContainerRuntimeConfig::load();
    let hooks = creation_hooks();
    let id = container_id.to_string();
    let name = container_name.to_string();
    let spec = pod_spec.clone();
    run_blocking_phase(
        CreationPhase::Network,
        container_id,
        container_name,
        pod_spec.host_network,
        config.cni_timeout,
        &hooks,
        move || provision_container_network(&id, &name, &spec),
    )
    .await
}

/// Removes the container runtime state and tears down associated CNI resources when applicable.
pub fn remove_container(
    container_name: &str,
    container_id: &str,
    host_network: bool,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // let remove_options = RemoveContainerOptions { force: true, ..Default::default() };
    if !host_network {
        // Remove network interface
        let cni_args: HashMap<String, String> =
            [("CNI_COMMAND", "DEL"), ("CNI_CONTAINERID", container_id)]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
        log_info(
            "kubelet",
            "Removing CNI network interface",
            &[("container_id", container_id)],
        );
        ensure_cni_prerequisites()?;
        metrics::observe_cni_operation(metrics::CniOperation::Delete, || {
            cni_plugin().delete(&cni_args)
        })?;
    } else {
        log_info(
            "kubelet",
            "Skipping CNI teardown for host-networked container",
            &[("container", container_name)],
        );
    }

    log_info(
        "kubelet",
        "Removing OCI container",
        &[("container", container_name), ("id", container_id)],
    );
    container_runtime().delete(container_id)?;

    log_info(
        "kubelet",
        "OCI container removed",
        &[("container", container_name), ("id", container_id)],
    );
    Ok(())
}

/// Starts an existing container by wiring runtime files, running lifecycle hooks, and issuing the start signal.
///
/// Resolves the stored pod spec to rebuild runtime metadata (DNS, env, TLS), reprovisions CNI if needed,
/// and updates readiness status.
pub async fn start_container(
    namespace: Option<&str>,
    app: &str,
    container_name: &str,
    container_id: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let stored_workload = pod_store::load_pod_manifest(namespace, app)
        .map_err(|e| with_context(e, format!("Failed to load pod spec for {}", container_name)))?;
    let pod_spec = stored_workload.as_ref().map(|workload| &workload.spec);
    let host_network = pod_spec.map(|spec| spec.host_network).unwrap_or(false);
    let container_spec = pod_spec.and_then(|spec| spec.containers.first()).cloned();
    let requires_profile = stored_workload
        .as_ref()
        .map(|workload| {
            workload
                .metadata
                .annotations
                .get("nanocloud.io/profile-managed")
                .map(|value| value == "true")
                .unwrap_or(false)
        })
        .unwrap_or(false);
    let lifecycle_hooks = container_spec
        .as_ref()
        .and_then(|container| container.lifecycle.clone());
    let pre_start_exec = lifecycle_hooks
        .as_ref()
        .and_then(|hooks| hooks.pre_start.clone());
    let post_start_exec = lifecycle_hooks
        .as_ref()
        .and_then(|hooks| hooks.post_start.clone());

    let runtime = container_runtime();
    let creation_cfg = ContainerRuntimeConfig::load();
    let path_cfg = KubeletPathConfig::load()?;
    let creation_hooks = creation_hooks();

    let mut runtime_config: HashMap<String, Vec<u8>> = if requires_profile {
        let profile = Profile::load(namespace, app).await.map_err(|e| {
            with_context(e, format!("Failed to load profile for {}", container_name))
        })?;
        profile.config.clone()
    } else {
        HashMap::new()
    };

    if !requires_profile {
        if let Some(spec) = container_spec.as_ref() {
            for env in &spec.env {
                if let Some(value) = &env.value {
                    runtime_config
                        .entry(format!("env.{}", env.name))
                        .or_insert_with(|| value.as_bytes().to_vec());
                }
            }
        }
    }

    let tls_info = generate_runtime_tls(app)?;
    let resolv_conf_template = build_resolv_conf(if host_network {
        Some("/etc/resolv.conf")
    } else {
        None
    })
    .ok_or_else(|| new_error("Unable to generate resolv.conf for container runtime"))?;

    let readiness_probe = container_spec
        .as_ref()
        .and_then(|container| container.readiness_probe.clone())
        .or_else(|| {
            if requires_profile {
                Some(ContainerProbe {
                    exec: Some(ProbeExec {
                        command: vec!["ready.sh".to_string()],
                    }),
                    initial_delay_seconds: Some(5),
                    period_seconds: Some(5),
                    ..ContainerProbe::default()
                })
            } else {
                None
            }
        });

    let mut attempt = 0;
    let creation_timeout = Duration::from_secs(30);
    let mut creation_backoff = Duration::from_millis(100);
    let wait_deadline = Instant::now() + creation_timeout;
    loop {
        attempt += 1;

        let mut state = runtime.state(container_id)?;
        if state.status == ContainerStatus::Stopped {
            log_info(
                "kubelet",
                "Container stopped; recreating init process",
                &[("container", container_name)],
            );
            ensure_rootfs_mounted(
                container_id,
                container_spec
                    .as_ref()
                    .and_then(|container| container.image.as_deref()),
            )
            .map_err(|e| {
                with_context(
                    e,
                    format!("Failed to prepare root filesystem for {}", container_name),
                )
            })?;
            runtime.recreate(container_id)?;
            state = runtime.state(container_id)?;
        }

        match state.status {
            ContainerStatus::Created => {}
            ContainerStatus::Creating | ContainerStatus::Unknown => {
                if Instant::now() >= wait_deadline {
                    return Err(new_error(format!(
                        "Container {} did not reach Created state within {:?} (last observed {:?})",
                        container_name, creation_timeout, state.status
                    )));
                }
                log::debug!(
                    "Waiting for container '{}' to reach Created state (attempt {}, status {:?})",
                    container_name,
                    attempt,
                    state.status
                );
                sleep(creation_backoff).await;
                creation_backoff = (creation_backoff * 2).min(Duration::from_secs(1));
                continue;
            }
            ContainerStatus::Running => {
                log_info(
                    "kubelet",
                    "Container already running; skipping start",
                    &[("container", container_name), ("id", container_id)],
                );
                return Ok(());
            }
            ContainerStatus::Stopped => {
                // handled above; continue to next loop to recreate if needed
                continue;
            }
            ContainerStatus::Paused => {
                return Err(new_error(format!(
                    "Container {} is paused; resume or recreate before starting",
                    container_name
                )));
            }
        }

        let mut reprovisioned_ip: Option<String> = None;
        if !host_network && state.network.ip_addresses.is_empty() {
            if let Some(spec) = pod_spec {
                log_warn(
                    "kubelet",
                    "Container missing network addresses; attempting to reprovision CNI",
                    &[("container", container_name), ("id", container_id)],
                );
                let result =
                    provision_container_network_blocking(container_id, container_name, spec)
                        .await
                        .map_err(|e| {
                            with_context(
                                e,
                                format!(
                                    "Failed to reprovision CNI network for container {}",
                                    container_name
                                ),
                            )
                        })?;
                reprovisioned_ip = result
                    .ips
                    .first()
                    .and_then(|entry| entry.address.split('/').next().map(str::to_string));
                state = runtime.state(container_id)?;
            } else {
                log_warn(
                    "kubelet",
                    "Pod spec unavailable while reprovisioning container network",
                    &[("container", container_name), ("id", container_id)],
                );
            }
        }

        let ip = state
            .network
            .ip_addresses
            .first()
            .and_then(|addr| addr.split('/').next().map(str::to_string))
            .or_else(|| reprovisioned_ip.clone())
            .or_else(|| {
                if host_network {
                    container_spec
                        .as_ref()
                        .and_then(|container| {
                            container.ports.iter().find_map(|port| port.host_ip.clone())
                        })
                        .or(Some("127.0.0.1".to_string()))
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                new_error(format!(
                    "Container {} does not have an assigned IP address",
                    container_name
                ))
            })?;

        run_blocking_phase(
            CreationPhase::Metadata,
            container_id,
            container_name,
            host_network,
            creation_cfg.metadata_timeout,
            &creation_hooks,
            {
                let id = container_id.to_string();
                let ip_for_files = ip.clone();
                move || setup_container_files(&id, &ip_for_files)
            },
        )
        .await
        .map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to persist network metadata for container '{}'",
                    container_name
                ),
            )
        })?;

        let resolv_conf = Some(resolv_conf_template.as_str());
        log_info(
            "kubelet",
            "Configuring runtime files",
            &[("container", container_name), ("id", container_id)],
        );
        let app_name = app.to_string();
        let ip_for_runtime = ip.clone();
        let runtime_config_clone = runtime_config.clone();
        let tls_info_clone = tls_info.clone();
        let resolv_conf_owned = resolv_conf.map(|value| value.to_string());
        let namespace_runtime = runtime.clone();
        let container_id_owned = container_id.to_string();
        let paths_for_runtime = path_cfg.clone();
        run_blocking_phase(
            CreationPhase::Metadata,
            container_id,
            container_name,
            host_network,
            creation_cfg.metadata_timeout,
            &creation_hooks,
            move || {
                let resolv_conf_for_ns = resolv_conf_owned.clone();
                namespace_runtime
                    .with_namespace(
                        &container_id_owned,
                        Box::new(move || {
                            let resolv_conf_ref_inner = resolv_conf_for_ns.as_deref();
                            setup_runtime_files(
                                &app_name,
                                &ip_for_runtime,
                                &runtime_config_clone,
                                &tls_info_clone,
                                resolv_conf_ref_inner,
                                &paths_for_runtime,
                            )
                        }),
                    )
                    .map_err(|e| with_context(e, "Failed to setup runtime files"))
            },
        )
        .await?;

        let pid = state.pid.ok_or_else(|| {
            new_error(format!(
                "Container {} does not have an init pid",
                container_name
            ))
        })?;
        let pid_str = pid.to_string();
        log_info(
            "kubelet",
            "Container process started",
            &[("container", container_name), ("pid", pid_str.as_str())],
        );

        let log_dir = container_log_dir(container_id);
        std::fs::create_dir_all(&log_dir).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to create container log directory {}",
                    log_dir.display()
                ),
            )
        })?;

        if let Some(exec) = pre_start_exec.as_ref() {
            run_exec_handler(container_id, exec)
                .map_err(|e| with_context(e, "preStart hook failed"))?;
        }

        match runtime.send_start(container_id) {
            Ok(()) => break,
            Err(err) => {
                let missing_start_control = err
                    .downcast_ref::<io::Error>()
                    .map(|io_err| io_err.kind() == ErrorKind::NotFound)
                    .unwrap_or(false);
                if missing_start_control && attempt == 1 {
                    log_warn(
                        "kubelet",
                        "Start control missing while restoring container; recreating init process",
                        &[("container", container_name), ("id", container_id)],
                    );
                    runtime.kill(container_id.to_string()).await?;
                    continue;
                }
                return Err(err);
            }
        }
    }

    if let Some(probe) = readiness_probe {
        runtime.set_status(container_id, ContainerStatus::Creating)?;
        let container_id_owned = container_id.to_string();
        let readiness_runtime = runtime.clone();
        tokio::spawn(async move {
            if let Err(err) = wait_for_readiness_probe(&container_id_owned, &probe).await {
                log_warn(
                    "kubelet",
                    "Readiness probe failed",
                    &[
                        ("container_id", container_id_owned.as_str()),
                        ("error", err.to_string().as_str()),
                    ],
                );
                return;
            }
            if let Err(err) =
                readiness_runtime.set_status(&container_id_owned, ContainerStatus::Running)
            {
                log_error(
                    "kubelet",
                    "Failed to update container state",
                    &[
                        ("container_id", container_id_owned.as_str()),
                        ("error", err.to_string().as_str()),
                    ],
                );
            }
        });
    } else {
        runtime.set_status(container_id, ContainerStatus::Running)?;
    }

    if let Some(exec) = post_start_exec.clone() {
        let container_id_owned = container_id.to_string();
        tokio::spawn(async move {
            if let Err(err) = run_exec_handler(&container_id_owned, &exec) {
                log_warn(
                    "kubelet",
                    "postStart hook failed",
                    &[
                        ("container_id", container_id_owned.as_str()),
                        ("error", err.to_string().as_str()),
                    ],
                );
            }
        });
    }

    Ok(())
}
async fn wait_for_readiness_probe(
    container_id: &str,
    probe: &ContainerProbe,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Some(exec) = &probe.exec {
        wait_for_readiness_command(
            container_id,
            &exec.command,
            probe.initial_delay_seconds,
            probe.period_seconds,
        )
        .await
    } else {
        Ok(())
    }
}

async fn wait_for_readiness_command(
    container_id: &str,
    command: &[String],
    initial_delay_seconds: Option<i32>,
    period_seconds: Option<i32>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if command.is_empty() {
        return Ok(());
    }

    if let Some(delay) = initial_delay_seconds {
        if delay > 0 {
            sleep(Duration::from_secs(delay.max(0) as u64)).await;
        }
    }

    let period = period_seconds.unwrap_or(1).max(1) as u64;

    let runtime = container_runtime();
    loop {
        let cmd: Vec<String> = command.to_owned();
        let result = runtime.exec(container_id, Box::new(move || prepare_null_stdio_exec(cmd)));

        match result {
            Ok(exec_result) => {
                if matches!(
                    exec_result.wait_status,
                    nix::sys::wait::WaitStatus::Exited(_, 0)
                ) {
                    log_readiness_exec_proc_guard(container_id, "succeeded");
                    break;
                }
                log_readiness_exec_proc_guard(container_id, "failed");
                let message =
                    format_wait_status_failure("Readiness command", exec_result.wait_status);
                log_info(
                    "kubelet",
                    "Readiness probe reported not ready",
                    &[("container_id", container_id), ("error", message.as_str())],
                );
                sleep(Duration::from_secs(period)).await;
            }
            Err(err) => {
                log_readiness_exec_proc_guard(container_id, "failed");
                log_info(
                    "kubelet",
                    "Readiness probe reported not ready",
                    &[
                        ("container_id", container_id),
                        ("error", err.to_string().as_str()),
                    ],
                );
                sleep(Duration::from_secs(period)).await;
            }
        }
    }

    Ok(())
}

fn log_readiness_exec_proc_guard(container_id: &str, context: &str) {
    if let Some(mounted) = container_runtime().take_exec_proc_mount_status() {
        debug!(
            "Readiness exec {} for {} (proc guard mounted /proc = {})",
            context, container_id, mounted
        );
    }
}

fn prepare_null_stdio_exec(
    command: Vec<String>,
) -> Result<ExecRequest, Box<dyn Error + Send + Sync>> {
    if command.is_empty() {
        return Err(new_error("Readiness command is empty"));
    }

    redirect_standard_to_null(libc::STDIN_FILENO, true)?;
    redirect_standard_to_null(libc::STDOUT_FILENO, false)?;
    redirect_standard_to_null(libc::STDERR_FILENO, false)?;

    let mut parts = command.into_iter();
    let program = parts
        .next()
        .ok_or_else(|| new_error("Readiness command missing executable"))?;
    let args: Vec<String> = parts.collect();

    Ok(ExecRequest {
        program,
        args,
        env: None,
    })
}

fn redirect_standard_to_null(
    target: i32,
    read_only: bool,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let flags = if read_only {
        OFlag::O_RDONLY
    } else {
        OFlag::O_WRONLY
    };
    let fd = open("/dev/null", flags, Mode::empty())
        .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?
        .into_raw_fd();
    if unsafe { libc::dup2(fd, target) } == -1 {
        let err = io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(Box::new(err));
    }
    if unsafe { libc::close(fd) } == -1 {
        return Err(Box::new(io::Error::last_os_error()));
    }
    Ok(())
}

fn format_wait_status_failure(label: &str, status: nix::sys::wait::WaitStatus) -> String {
    match status {
        nix::sys::wait::WaitStatus::Exited(_, code) => {
            format!("{label} exited with status {code}")
        }
        nix::sys::wait::WaitStatus::Signaled(_, signal, _) => {
            format!("{label} terminated by signal {signal}")
        }
        other => format!("{label} failed with {:?}", other),
    }
}

fn run_exec_handler(
    container_id: &str,
    exec: &ProbeExec,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if exec.command.is_empty() {
        return Ok(());
    }

    let cmd = exec.command.clone();
    let runtime = container_runtime();
    let exec_result = runtime
        .exec(container_id, Box::new(move || prepare_null_stdio_exec(cmd)))
        .map_err(|err| with_context(err, "Failed to execute lifecycle command"))?;

    match exec_result.wait_status {
        nix::sys::wait::WaitStatus::Exited(_, 0) => Ok(()),
        status => Err(new_error(format_wait_status_failure(
            "Lifecycle command",
            status,
        ))),
    }
}
fn create_rootfs(
    id: &str,
    name: &str,
    oci_manifest: &OciManifest,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let refs_dir = container_refs_dir();
    let name_link = refs_dir.join(name);
    if std::fs::exists(&name_link)? {
        return Err(new_error(format!(
            "Container with name {} already exists",
            name
        )));
    }
    std::fs::create_dir_all(&refs_dir).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to create container refs directory {}",
                refs_dir.display()
            ),
        )
    })?;

    let base = container_root_path(id);
    let rootfs = base.join("rootfs");
    let overlay_dir = base.join("overlay");
    let upper = overlay_dir.join("upper");
    let work = overlay_dir.join("work");
    let overlay_root = image_store_root().join("overlay");
    let lower_dirs = oci_manifest
        .layers
        .iter()
        .rev()
        .map(|l| {
            overlay_root
                .join(&l.digest[7..])
                .to_string_lossy()
                .to_string()
        })
        .collect::<Vec<_>>();
    let lower = if lower_dirs.is_empty() {
        let fallback = overlay_dir.join("lower");
        std::fs::create_dir_all(&fallback).map_err(|e| {
            with_context(
                e,
                format!("Failed to create fallback lowerdir {}", fallback.display()),
            )
        })?;
        fallback.to_string_lossy().to_string()
    } else {
        lower_dirs.join(":")
    };

    for dir in [&rootfs, &upper, &work] {
        std::fs::create_dir_all(dir).map_err(|e| {
            with_context(
                e,
                format!("Failed to create overlay directory {}", dir.display()),
            )
        })?;
    }

    let source = CString::new("overlay")?;
    let target = CString::new(rootfs.to_string_lossy().to_string())?;
    let fstype = CString::new("overlay")?;
    let options = CString::new(format!(
        "lowerdir={},upperdir={},workdir={},metacopy=on,redirect_dir=on",
        &lower,
        upper.to_string_lossy(),
        work.to_string_lossy()
    ))?;

    nix::mount::mount(
        Some(source.as_c_str()),
        target.as_c_str(),
        Some(fstype.as_c_str()),
        nix::mount::MsFlags::from_bits_truncate(0),
        Some(options.as_c_str()),
    )
    .map_err(|e| with_context(e, format!("Failed to mount overlay for container {}", name)))?;

    if let Err(err) = std::os::unix::fs::symlink(format!("../sha256/{}", id), &name_link) {
        let _ = nix::mount::umount(target.as_c_str());
        return Err(with_context(
            err,
            format!(
                "Failed to create container name symlink {} -> {}",
                name_link.display(),
                id
            ),
        ));
    }

    Ok(())
}

fn ensure_rootfs_mounted(
    container_id: &str,
    image_reference: Option<&str>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let base = container_root_path(container_id);
    let rootfs = base.join("rootfs");
    if rootfs.exists() && is_rootfs_mounted(&rootfs)? {
        return Ok(());
    }

    let manifest = load_manifest_for_container(container_id, image_reference)?;
    let overlay_dir = base.join("overlay");
    let upper = overlay_dir.join("upper");
    let work = overlay_dir.join("work");

    for dir in [&rootfs, &upper, &work] {
        std::fs::create_dir_all(dir).map_err(|e| {
            with_context(
                e,
                format!("Failed to prepare overlay directory {}", dir.display()),
            )
        })?;
    }

    let lower_dirs = overlay_lowerdirs(&manifest)?;
    let lower = if lower_dirs.is_empty() {
        let fallback = overlay_dir.join("lower");
        std::fs::create_dir_all(&fallback).map_err(|e| {
            with_context(
                e,
                format!("Failed to create fallback lowerdir {}", fallback.display()),
            )
        })?;
        fallback.to_string_lossy().to_string()
    } else {
        lower_dirs.join(":")
    };

    let source = CString::new("overlay")?;
    let target = CString::new(rootfs.to_string_lossy().to_string())?;
    let fstype = CString::new("overlay")?;
    let options = CString::new(format!(
        "lowerdir={},upperdir={},workdir={},metacopy=on,redirect_dir=on",
        &lower,
        upper.to_string_lossy(),
        work.to_string_lossy()
    ))?;

    nix::mount::mount(
        Some(source.as_c_str()),
        target.as_c_str(),
        Some(fstype.as_c_str()),
        nix::mount::MsFlags::from_bits_truncate(0),
        Some(options.as_c_str()),
    )
    .map_err(|e| {
        with_context(
            e,
            format!("Failed to remount overlay for container {}", container_id),
        )
    })?;

    debug!(
        "Remounted overlay filesystem for container {} at {}",
        container_id,
        rootfs.display()
    );

    Ok(())
}

fn load_manifest_for_container(
    container_id: &str,
    image_reference: Option<&str>,
) -> Result<OciManifest, Box<dyn Error + Send + Sync>> {
    if let Some(image) = image_reference {
        match parse_image_reference(image) {
            Ok(reference) => match load_manifest_from_store(&reference) {
                Ok(manifest) => return Ok(manifest),
                Err(err) => {
                    debug!(
                        "Failed to load manifest for image {} from cache: {}",
                        image, err
                    );
                }
            },
            Err(err) => {
                debug!("Failed to parse image reference {}: {}", image, err);
            }
        }
    }

    let digest_path = container_root_path(container_id).join("image_digest");
    let raw_digest = std::fs::read_to_string(&digest_path).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to read cached image digest for container {}",
                container_id
            ),
        )
    })?;
    let digest = raw_digest.trim();
    if digest.is_empty() {
        return Err(new_error(format!(
            "Cached image digest missing for container {}",
            container_id
        )));
    }

    find_manifest_by_config_digest(digest)
}

fn find_manifest_by_config_digest(
    digest: &str,
) -> Result<OciManifest, Box<dyn Error + Send + Sync>> {
    let refs_root = image_store_root().join("refs");
    let mut stack: Vec<PathBuf> = vec![refs_root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(with_context(
                    err,
                    format!("Failed to read manifest references in {}", dir.display()),
                ));
            }
        };
        for entry in entries {
            let entry = entry.map_err(|err| {
                with_context(
                    err,
                    format!("Failed to inspect manifest entry in {}", dir.display()),
                )
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|err| {
                with_context(err, format!("Failed to inspect {}", path.display()))
            })?;
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if file_type.is_file() || file_type.is_symlink() {
                match std::fs::File::open(&path) {
                    Ok(file) => {
                        let reader = BufReader::new(file);
                        if let Ok(manifest) = serde_json::from_reader::<_, OciManifest>(reader) {
                            if manifest.config.digest == digest {
                                return Ok(manifest);
                            }
                        }
                    }
                    Err(err) if err.kind() == ErrorKind::NotFound => continue,
                    Err(err) => {
                        return Err(with_context(
                            err,
                            format!("Failed to open manifest reference {}", path.display()),
                        ));
                    }
                }
            }
        }
    }

    Err(new_error(format!(
        "Cached manifest for config digest {} not found",
        digest
    )))
}

fn overlay_lowerdirs(manifest: &OciManifest) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
    let mut dirs = Vec::new();
    for layer in manifest.layers.iter().rev() {
        if !layer.digest.starts_with("sha256:") || layer.digest.len() != 71 {
            return Err(new_error(format!(
                "Unsupported layer digest format: {}",
                layer.digest
            )));
        }
        let path = image_store_root()
            .join("overlay")
            .join(&layer.digest[7..])
            .to_string_lossy()
            .to_string();
        dirs.push(path);
    }
    Ok(dirs)
}

fn is_rootfs_mounted(path: &Path) -> std::io::Result<bool> {
    match std::fs::metadata(path) {
        Ok(meta) => match path.parent() {
            Some(parent) => {
                let parent_meta = std::fs::metadata(parent)?;
                Ok(meta.dev() != parent_meta.dev() || meta.ino() == parent_meta.ino())
            }
            None => Ok(true),
        },
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

pub fn get_container_id_by_name(name: &str) -> Option<String> {
    let link = container_refs_dir().join(name);
    std::fs::read_link(link).ok().and_then(|t| {
        t.components()
            .next_back()
            .and_then(|comp| comp.as_os_str().to_str())
            .map(|s| s.to_string())
    })
}

pub fn resolve_container_id(
    namespace: Option<&str>,
    app: &str,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let container_name = namespace
        .map(|ns| format!("{}-{}", ns, app))
        .unwrap_or_else(|| app.to_string());
    get_container_id_by_name(&container_name)
        .ok_or_else(|| new_error(format!("Service '{}' not found", container_name)))
}

/*
fn get_port_bindings(image_info: &ImageInfo) -> HashMap<String, Option<Vec<PortBinding>>> {
    // Ports should be configured in the form <port_number>/<protocol>, and may optionally be prefixed with <bind_ip_address>:
    image_info.get_ports().iter().map(|(_name, port)| {
        let parts: Vec<&str> = port.split(':').rev().collect::<Vec<&str>>();
        let port = String::from(*parts.get(0).expect("Invalid port"));
        let ip : Option<String> = if let Some(ip) = parts.get(1) { Some((*ip).to_string()) } else { None };
        (port.clone(), Some(vec![PortBinding { host_ip: ip, host_port: Some(port.clone()) }]))
    }).collect()
}

fn get_exposed_ports(image_info: &ImageInfo) -> HashMap<String, HashMap<(), ()>> {
    // Exposed ports should be configured in the form <port_number>/<protocol>
    image_info.get_ports().iter().map(|(_name, port)| {
        (port.clone(), HashMap::new())
    }).collect()
}

fn get_mounts(container_name: &str, image_info: &ImageInfo) -> Vec<Mount> {
    image_info.get_volumes().iter()
        .map(|(key, path)| {
            Mount {
                source: Some(format!("{}-{}", container_name, key)),
                target: Some(path.clone()),
                typ: Some(MountTypeEnum::VOLUME),
                ..Default::default()
            }
        })
        .collect()
}
*/
// Helper function to ensure a volume is created if it doesn't already exist
/*
async fn exec_container(docker: &Docker, container_id: &str, command: Vec<&str>, stdin: Option<Vec<u8>>) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Prepare the exec command to run inside the container
    let exec_options = CreateExecOptions {
        cmd: Some(command),
        attach_stdin: stdin.as_ref().map(|_| true),
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        ..Default::default()
    };

    // Create an exec instance
    let exec_results = docker
        .create_exec(container_id, exec_options)
        .await
        .map_err(|e| format!("Failed to create exec instance: {}", e))?;

    // Start the exec with the given input
    let mut start_exec_results = docker
        .start_exec(&exec_results.id, None)
        .await
        .map_err(|e| format!("Failed to start exec: {}", e))?;

    if let StartExecResults::Attached { ref mut output, ref mut input, .. } = start_exec_results {
        // Write to stdin if input is provided
        if let Some(stdin_data) = stdin {
            input.write_all(&stdin_data)
                .await
                .map_err(|e| format!("Failed to write data to stdin: {}", e))?;
            input.shutdown()
                .await
                .map_err(|e| format!("Failed to close input: {}", e))?;
        }

        stream_logs(output).await?;
    }

    Ok(())
}
*/
// use std::io::Read;
// fn setup_network_files(app: &str, ip: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
//     let contents = format!(
//         "127.0.0.1   localhost
// ::1         localhost
// {ip}   {app}.nanocloud.local {app}
// "
// );

// let file = std::fs::File::create("/etc/hosts")?;
// file.set_permissions(std::fs::Permissions::from_mode(0o644))?;

// let mut writer = std::io::BufWriter::new(file);
// writer.write_all(contents.as_bytes())?;
// writer.flush()?;
//     // // let path = format!("{}/env", &runtime_dir);
//     // // let mut file = std::fs::File::create(&path)?;
//     // // file.write_all(env_vars.join("\n").as_bytes())?;
//     // // std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444))?;
//     let mut file = std::fs::File::open("/etc/hosts")?;
//     let mut contents = String::new();
//     file.read_to_string(&mut contents)?;
//     print!("{}", contents);

//     Ok(())
// }

pub fn setup_container_files(
    container_id: &str,
    ip_address: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let network_dir = container_root_path(container_id).join("network");
    std::fs::create_dir_all(&network_dir).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to create network state directory {}",
                network_dir.display()
            ),
        )
    })?;
    let ip_path = network_dir.join("ip_address");
    let mut file = std::fs::File::create(&ip_path).map_err(|e| {
        with_context(
            e,
            format!("Failed to create IP address file {}", ip_path.display()),
        )
    })?;
    file.write_all(ip_address.as_bytes()).map_err(|e| {
        with_context(
            e,
            format!("Failed to write IP address file {}", ip_path.display()),
        )
    })?;
    file.flush().map_err(|e| {
        with_context(
            e,
            format!("Failed to flush IP address file {}", ip_path.display()),
        )
    })?;

    Ok(())
}

fn ensure_private_mount_namespace() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo")
        .map_err(|e| with_context(e, "Failed to read /proc/self/mountinfo"))?;

    for line in mountinfo.lines() {
        let fields: Vec<&str> = line.split(' ').collect();
        if fields.len() < 7 {
            continue;
        }
        if fields[4] != "/" {
            continue;
        }

        let dash_index = fields
            .iter()
            .position(|field| *field == "-")
            .ok_or_else(|| new_error("Malformed mountinfo entry for root mount"))?;
        let optional = &fields[6..dash_index];

        if optional
            .iter()
            .any(|field| field.starts_with("shared:") || field.starts_with("master:"))
        {
            return Err(new_error(
                "Root mount namespace is shared with the host; refusing to rewrite container /etc/hosts",
            ));
        }

        return Ok(());
    }

    Err(new_error(
        "Unable to locate root mount entry in /proc/self/mountinfo",
    ))
}

/// Materializes runtime assets inside the container namespace (hosts, resolv.conf, env, TLS).
///
/// Uses the configured runtime directory for placing env and TLS files and writes host/DNS data into
/// the container namespace, leaving permissions restrictive for runtime consumers.
pub fn setup_runtime_files(
    app: &str,
    ip: &str,
    config: &HashMap<String, Vec<u8>>,
    tls_info: &TlsInfo,
    resolv_conf: Option<&str>,
    paths: &KubeletPathConfig,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let runtime = container_runtime();
    let proc_guard_status = runtime.take_exec_proc_mount_status();
    let proc_guard_mounted = proc_guard_status.unwrap_or(false);
    match proc_guard_status {
        Some(true) => debug!("Exec helper mounted /proc before configuring runtime files"),
        Some(false) => debug!("Exec helper verified /proc mount before configuring runtime files"),
        None => debug!(
            "Exec helper did not report /proc guard status; proceeding with runtime files setup"
        ),
    }

    let namespace_check = match ensure_private_mount_namespace() {
        Ok(()) => Ok(()),
        Err(err) if proc_guard_mounted => {
            debug!(
                "Initial mount namespace check failed after /proc guard (retrying): {}",
                err
            );
            ensure_private_mount_namespace()
        }
        Err(err) => Err(err),
    };

    namespace_check.map_err(|e| {
        with_context(
            e,
            "Mount namespace not private; refusing to configure runtime files",
        )
    })?;
    // Create hosts file
    let hosts = format!(
        "127.0.0.1   localhost
::1         localhost
{ip}   {app}.nanocloud.local {app}
"
    );
    let mut file = std::fs::File::create("/etc/hosts")
        .map_err(|e| with_context(e, "Failed to create /etc/hosts"))?;
    file.set_permissions(std::fs::Permissions::from_mode(0o644))
        .map_err(|e| with_context(e, "Failed to set permissions on /etc/hosts"))?;
    file.write_all(hosts.as_bytes())
        .map_err(|e| with_context(e, "Failed to write /etc/hosts"))?;

    if let Some(contents) = resolv_conf {
        let mut resolv = std::fs::File::create("/etc/resolv.conf")
            .map_err(|e| with_context(e, "Failed to create /etc/resolv.conf"))?;
        resolv
            .set_permissions(std::fs::Permissions::from_mode(0o644))
            .map_err(|e| with_context(e, "Failed to set permissions on /etc/resolv.conf"))?;
        resolv
            .write_all(contents.as_bytes())
            .map_err(|e| with_context(e, "Failed to write /etc/resolv.conf"))?;
    }
    // let mut writer = std::io::BufWriter::new(file);
    // writer.write_all(hosts.as_bytes())?;
    // writer.flush()?;

    // setup_network_files(app, &ip)?;
    // let runtime_dir = format!("/var/lib/nanocloud.io/container/sha256/{}/rootfs/var/run/nanocloud.io", container_id);
    let runtime_dir = paths.runtime_dir.as_path();
    std::fs::create_dir_all(runtime_dir).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to create runtime directory {}",
                runtime_dir.display()
            ),
        )
    })?;

    // Set environment variables.
    let mut env_vars: Vec<String> = Vec::with_capacity(config.len());
    for (key, value) in config {
        let rendered = String::from_utf8(value.clone()).map_err(|e| {
            with_context(
                e,
                format!("Configuration value '{}' contains invalid UTF-8", key),
            )
        })?;
        env_vars.push(format!(
            "NANOCLOUD_{}_{}='{}'",
            app.to_uppercase(),
            key.to_uppercase().replace('.', "_"),
            rendered
        ));
    }
    let env_path = runtime_dir.join("env");
    let mut file = std::fs::File::create(&env_path).map_err(|e| {
        with_context(
            e,
            format!("Failed to create env file {}", env_path.display()),
        )
    })?;
    file.set_permissions(std::fs::Permissions::from_mode(0o444))
        .map_err(|e| {
            with_context(
                e,
                format!("Failed to set permissions on {}", env_path.display()),
            )
        })?;
    file.write_all(env_vars.join("\n").as_bytes())
        .map_err(|e| {
            with_context(
                e,
                format!("Failed to write env file {}", env_path.display()),
            )
        })?;

    // Set up TLS certs
    let tls_dir = runtime_dir.join("tls");
    std::fs::create_dir_all(&tls_dir).map_err(|e| {
        with_context(
            e,
            format!("Failed to create TLS directory {}", tls_dir.display()),
        )
    })?;
    for (name, content) in [
        ("key.pem", &tls_info.key),
        ("cert.pem", &tls_info.cert),
        ("ca.pem", &tls_info.ca),
    ] {
        let path = tls_dir.join(name);
        let mut file = std::fs::File::create(&path).map_err(|e| {
            with_context(e, format!("Failed to create TLS file {}", path.display()))
        })?;
        file.set_permissions(std::fs::Permissions::from_mode(0o444))
            .map_err(|e| {
                with_context(
                    e,
                    format!("Failed to set permissions on {}", path.display()),
                )
            })?;
        file.write_all(content)
            .map_err(|e| with_context(e, format!("Failed to write TLS file {}", path.display())))?;
    }

    Ok(())
}

pub fn generate_runtime_tls(app: &str) -> Result<TlsInfo, Box<dyn Error + Send + Sync>> {
    let mut san = vec![format!("{}.nanocloud.local", app)];
    if let Ok(ip) = get_local_ip() {
        san.push(ip);
    }
    TlsInfo::create(app, Some(&san))
        .map_err(|e| with_context(e, "Failed to generate runtime TLS assets"))
}

pub async fn kill(container_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    container_runtime().kill(container_id.to_string()).await
}

pub fn exec_in_container<F>(
    container_id: &str,
    action: F,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    F: FnOnce() -> Result<(), Box<dyn Error + Send + Sync>> + Send + 'static,
{
    container_runtime().with_namespace(container_id, Box::new(action))
}

fn get_local_ip() -> Result<String, Box<dyn Error + Send + Sync>> {
    unsafe {
        let mut ifap: *mut libc::ifaddrs = ptr::null_mut();

        if libc::getifaddrs(&mut ifap) != 0 {
            return Err(new_error("Failed to list network interfaces"));
        }

        let mut current = ifap;
        let mut selected_ip: Option<String> = None;

        while !current.is_null() {
            let iface = &*current;

            if !iface.ifa_addr.is_null() && (*iface.ifa_addr).sa_family as i32 == libc::AF_INET {
                let name = CStr::from_ptr(iface.ifa_name).to_str().unwrap_or("");
                if name == "lo" {
                    current = (*current).ifa_next;
                    continue;
                }

                let sockaddr: *const libc::sockaddr_in = iface.ifa_addr as *const libc::sockaddr_in;
                let ip = Ipv4Addr::from((*sockaddr).sin_addr.s_addr.to_be());
                selected_ip = Some(ip.to_string());
                break;
            }

            current = (*current).ifa_next;
        }

        libc::freeifaddrs(ifap);

        selected_ip.ok_or_else(|| new_error("Unable to detect local IP address"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::cni::provider::CniPlugin;
    use crate::nanocloud::controller::runtime::ControllerRuntime;
    use crate::nanocloud::dns::{DnsConfig, DnsService};
    use crate::nanocloud::oci::runtime_provider::ContainerRuntime;
    use nix::sys::signal::Signal;
    use nix::sys::wait::WaitStatus;
    use nix::unistd::Pid;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::future::Future;
    use std::net::{IpAddr, Ipv4Addr};
    use std::pin::Pin;
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::Duration;
    use tempfile::tempdir;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    thread_local! {
        static ENV_LOCK_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    struct EnvGuard {
        key: &'static str,
        previous: Option<String>,
        _lock: Option<std::sync::MutexGuard<'static, ()>>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let lock = ENV_LOCK_DEPTH.with(|depth| {
                let current = depth.get();
                depth.set(current + 1);
                if current == 0 {
                    Some(env_lock().lock().expect("env lock"))
                } else {
                    None
                }
            });
            let previous = std::env::var(key).ok();
            std::env::set_var(key, value);
            Self {
                key,
                previous,
                _lock: lock,
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match self.previous.take() {
                Some(prev) => std::env::set_var(self.key, prev),
                None => std::env::remove_var(self.key),
            }
            ENV_LOCK_DEPTH.with(|depth| {
                let current = depth
                    .get()
                    .checked_sub(1)
                    .expect("env lock depth underflow");
                depth.set(current);
                if current == 0 {
                    if let Some(lock) = self._lock.take() {
                        drop(lock);
                    }
                }
            });
        }
    }

    struct DependencyGuard<T: Send + Sync + 'static> {
        previous: Option<Arc<T>>,
    }

    impl<T: Send + Sync + 'static> DependencyGuard<T> {
        fn replace(value: Arc<T>) -> Self {
            let previous = ControllerRuntime::shared().register_dependency(value);
            Self { previous }
        }
    }

    impl<T: Send + Sync + 'static> Drop for DependencyGuard<T> {
        fn drop(&mut self) {
            let runtime = ControllerRuntime::shared();
            if let Some(prev) = self.previous.take() {
                let _ = runtime.register_dependency(prev);
            } else {
                runtime.clear_dependency::<T>();
            }
        }
    }

    #[derive(Default)]
    struct RecordingRuntime {
        deleted: Mutex<Vec<String>>,
    }

    impl RecordingRuntime {
        fn deletions(&self) -> Vec<String> {
            self.deleted.lock().unwrap().clone()
        }
    }

    impl ContainerRuntime for RecordingRuntime {
        fn configure_from_spec(
            &self,
            _container_id: &str,
            _container_name: &str,
            _container: &crate::nanocloud::k8s::pod::ContainerSpec,
            _volumes: &[crate::nanocloud::k8s::pod::VolumeSpec],
            _host_network: bool,
            _security: &crate::nanocloud::k8s::pod::PodSecurityContext,
        ) -> crate::nanocloud::oci::runtime::OciConfig {
            unimplemented!("configure_from_spec not used in tests");
        }

        fn create(
            &self,
            _container_id: &str,
            _env: &HashMap<String, String>,
            _config: Vec<u8>,
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            Ok(())
        }

        fn recreate(&self, _container_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
            Ok(())
        }

        fn delete(&self, container_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
            self.deleted.lock().unwrap().push(container_id.to_string());
            Ok(())
        }

        fn state(
            &self,
            _container_id: &str,
        ) -> Result<crate::nanocloud::oci::runtime::ContainerState, Box<dyn Error + Send + Sync>>
        {
            Err(new_error("state not implemented for test runtime"))
        }

        fn list(
            &self,
        ) -> Result<
            Vec<crate::nanocloud::oci::runtime::ContainerSummary>,
            Box<dyn Error + Send + Sync>,
        > {
            Ok(Vec::new())
        }

        fn send_start(&self, _container_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
            Ok(())
        }

        fn set_status(
            &self,
            _container_id: &str,
            _status: crate::nanocloud::oci::runtime::ContainerStatus,
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            Ok(())
        }

        fn with_namespace(
            &self,
            _container_id: &str,
            action: Box<dyn crate::nanocloud::oci::runtime_provider::NamespaceAction>,
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            action.run()
        }

        fn exec(
            &self,
            _container_id: &str,
            _prepare: Box<dyn crate::nanocloud::oci::runtime_provider::ExecPrepare>,
        ) -> Result<crate::nanocloud::oci::runtime::ExecResult, Box<dyn Error + Send + Sync>>
        {
            Err(new_error("exec not implemented for test runtime"))
        }

        fn kill(
            &self,
            _container_id: String,
        ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error + Send + Sync>>> + Send>>
        {
            Box::pin(async { Ok(()) })
        }

        fn take_exec_proc_mount_status(&self) -> Option<bool> {
            None
        }
    }

    #[derive(Default)]
    struct RecordingCni {
        deletes: Mutex<usize>,
    }

    impl RecordingCni {
        fn delete_calls(&self) -> usize {
            *self.deletes.lock().unwrap()
        }
    }

    impl CniPlugin for RecordingCni {
        fn reconcile_cni_artifacts(
            &self,
        ) -> Result<crate::nanocloud::cni::CniReconciliationReport, Box<dyn Error + Send + Sync>>
        {
            Err(new_error("reconcile not needed in tests"))
        }

        fn bridge(&self, _name: &str, _cidr: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
            Ok(())
        }

        fn add(
            &self,
            _env: &HashMap<String, String>,
            _config: Vec<u8>,
        ) -> Result<crate::nanocloud::cni::CniResult, Box<dyn Error + Send + Sync>> {
            Err(new_error("add not needed in tests"))
        }

        fn delete(
            &self,
            _env: &HashMap<String, String>,
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            let mut guard = self.deletes.lock().unwrap();
            *guard += 1;
            Ok(())
        }
    }

    #[test]
    fn build_resolv_conf_uses_host_override_when_valid() {
        let dir = tempdir().expect("tempdir");
        let host_resolv = dir.path().join("resolv.conf");
        std::fs::write(&host_resolv, "nameserver 1.1.1.1\n").expect("write resolv.conf");

        let contents =
            build_resolv_conf(host_resolv.to_str()).expect("resolv.conf should be returned");
        assert_eq!(contents.trim(), "nameserver 1.1.1.1");
    }

    #[test]
    fn build_resolv_conf_rejects_invalid_host_override() {
        let dir = tempdir().expect("tempdir");
        let host_resolv = dir.path().join("resolv.conf");
        std::fs::write(&host_resolv, "search invalid domain\n").expect("write resolv.conf");

        let contents = build_resolv_conf(host_resolv.to_str());
        assert!(contents.is_none(), "invalid override should be rejected");
    }

    #[test]
    fn build_resolv_conf_returns_none_without_dns_dependency() {
        let runtime = ControllerRuntime::shared();
        let previous = runtime.dependency::<DnsService>();
        runtime.clear_dependency::<DnsService>();

        let contents = build_resolv_conf(None);
        assert!(
            contents.is_none(),
            "missing dns dependency should return None"
        );

        if let Some(prev) = previous {
            let _ = runtime.register_dependency(prev);
        }
    }

    #[test]
    fn build_resolv_conf_generates_with_dependency_and_fallbacks() {
        let config = DnsConfig::new(
            "cluster.local".to_string(),
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            53,
            30,
            Vec::new(),
            512,
        )
        .expect("dns config");
        let service = Arc::new(DnsService::new(config));
        let _guard = DependencyGuard::replace(service);

        let contents = build_resolv_conf(None).expect("generated resolv.conf");
        assert!(
            contents.contains(DEFAULT_BRIDGE_DNS_ADDR),
            "should fall back to bridge address when listen address is unspecified"
        );
        assert!(
            contents.contains("search svc.cluster.local cluster.local"),
            "should include cluster search domains"
        );
    }

    #[tokio::test]
    async fn rollback_cleans_up_runtime_and_network_state() {
        let root = tempdir().expect("rootfs");
        let netns_dir = root.path().join("netns");
        std::fs::create_dir_all(&netns_dir).expect("netns dir");
        let _netns_guard = EnvGuard::set(
            "NANOCLOUD_NETNS_DIR",
            netns_dir.to_str().expect("netns path"),
        );
        let _allow_guard = EnvGuard::set("NANOCLOUD_CNI_ALLOW_UNPRIVILEGED", "1");

        let container_base = root.path().join("abc");
        let rootfs = container_base.join("rootfs");
        std::fs::create_dir_all(&rootfs).expect("rootfs dir");
        let name_link = root.path().join("name-link");
        std::fs::write(&name_link, "placeholder").expect("name link");

        let runtime = Arc::new(RecordingRuntime::default());
        let cni = Arc::new(RecordingCni::default());
        let handles = RollbackHandles {
            runtime: runtime.clone(),
            cni: cni.clone(),
        };

        let mut context = CreationContext::new("abc".into(), false);
        context.mark_network();
        context.mark_rootfs(container_base.clone());
        context.mark_container_created();
        context.mark_name_link(name_link.clone());

        rollback_creation_with_handles(context, "svc", handles).await;

        assert!(
            !container_base.exists(),
            "rollback should remove container root"
        );
        assert!(
            !name_link.exists(),
            "rollback should remove name link entries"
        );
        assert_eq!(runtime.deletions(), vec!["abc".to_string()]);
        assert_eq!(cni.delete_calls(), 1);
    }

    #[tokio::test]
    async fn rollback_skips_network_teardown_for_host_mode() {
        let root = tempdir().expect("rootfs");
        let container_base = root.path().join("def");
        std::fs::create_dir_all(container_base.join("rootfs")).expect("rootfs dir");
        let name_link = root.path().join("name-link");
        std::fs::write(&name_link, "placeholder").expect("name link");

        let runtime = Arc::new(RecordingRuntime::default());
        let cni = Arc::new(RecordingCni::default());
        let handles = RollbackHandles {
            runtime: runtime.clone(),
            cni: cni.clone(),
        };

        let mut context = CreationContext::new("def".into(), true);
        context.mark_network();
        context.mark_rootfs(container_base.clone());
        context.mark_name_link(name_link.clone());

        rollback_creation_with_handles(context, "svc", handles).await;

        assert_eq!(cni.delete_calls(), 0, "host network skips CNI teardown");
        assert!(
            !container_base.exists(),
            "rootfs should still be cleaned for host-network containers"
        );
    }

    #[tokio::test]
    async fn blocking_phase_offloads_work_without_stalling_async_tasks() {
        let hooks = ContainerCreationHooks::default();
        let block_for = Duration::from_millis(120);

        let blocking = run_blocking_phase(
            CreationPhase::Rootfs,
            "id",
            "container",
            false,
            Duration::from_secs(1),
            &hooks,
            move || {
                std::thread::sleep(block_for);
                Ok(())
            },
        );

        let progress = tokio::time::timeout(Duration::from_millis(200), async {
            tokio::time::sleep(Duration::from_millis(30)).await;
        });

        let (blocking_result, progress_result) = tokio::join!(blocking, progress);
        assert!(progress_result.is_ok(), "async tasks should make progress");
        assert!(blocking_result.is_ok(), "blocking phase should complete");
    }

    #[test]
    fn format_wait_status_failure_reports_signal() {
        let status = WaitStatus::Signaled(Pid::from_raw(42), Signal::SIGTERM, false);
        let message = format_wait_status_failure("Probe", status);
        assert!(
            message.contains("signal SIGTERM"),
            "message should mention terminating signal: {message}"
        );
    }

    #[test]
    fn format_wait_status_failure_reports_exit_code() {
        let status = WaitStatus::Exited(Pid::from_raw(7), 3);
        let message = format_wait_status_failure("Probe", status);
        assert!(
            message.contains("status 3"),
            "message should mention exit status: {message}"
        );
    }

    #[test]
    fn generate_container_id_produces_lower_hex_without_collisions() {
        let temp = tempdir().expect("container root tempdir");
        let _root_guard = EnvGuard::set(
            "NANOCLOUD_CONTAINER_ROOT",
            temp.path().to_str().expect("container root"),
        );

        let mut seen = HashSet::new();
        for _ in 0..8 {
            let id = generate_container_id().expect("generated id");
            assert_eq!(id.len(), 64);
            assert!(
                id.chars().all(|ch| ch.is_ascii_hexdigit()
                    && (!ch.is_ascii_alphabetic() || ch.is_ascii_lowercase())),
                "id should be lowercase hex: {id}"
            );
            assert!(seen.insert(id.clone()), "duplicate id {id}");
            assert!(
                !container_root_path(&id).exists(),
                "container path for {id} should not be pre-existing"
            );
        }
    }

    #[test]
    fn generate_container_id_reports_collisions() {
        let temp = tempdir().expect("container root tempdir");
        let _root_guard = EnvGuard::set(
            "NANOCLOUD_CONTAINER_ROOT",
            temp.path().to_str().expect("container root"),
        );

        let existing = "a".repeat(64);
        std::fs::create_dir_all(container_root_path(&existing)).expect("existing container dir");
        let mut attempts = 0usize;
        let result = generate_container_id_with(|| {
            attempts += 1;
            Ok(existing.clone())
        });

        assert!(result.is_err(), "collisions should surface as errors");
        assert!(
            attempts >= 3,
            "should retry multiple times before giving up on collisions"
        );
    }

    #[test]
    fn generate_container_id_rejects_invalid_candidates() {
        let result = generate_container_id_with(|| Ok("not-hex".to_string()));
        assert!(
            result.is_err(),
            "non-hexadecimal candidates should not be accepted"
        );
    }
}
