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

use super::statefulsets::{delete, load};
use crate::nanocloud::k8s::pod::Pod;
use crate::nanocloud::k8s::statefulset::StatefulSet;
use crate::nanocloud::k8s::store::common::{
    bump_resource_version, namespaced_key, namespaced_root, validate_resource_target,
    value_file_path, with_resource_lock, K8S_KEYSPACE, POD_IP_ANNOTATION, POD_PREFIX,
};
use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::is_missing_value_error;

use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::io::ErrorKind;
use std::sync::{Mutex, OnceLock};

#[derive(Debug)]
pub struct StoredPod {
    pub namespace: Option<String>,
    pub name: String,
    pub workload: Pod,
}

fn pod_cache() -> &'static Mutex<HashMap<String, Pod>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Pod>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn list_pod_manifests() -> Result<Vec<StoredPod>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = namespaced_root(POD_PREFIX);
    let namespace_entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
        Err(err) => {
            return Err(with_context(
                err,
                format!("Failed to read Pod root directory '{}'", root.display()),
            ))
        }
    };

    for namespace_entry in namespace_entries {
        let namespace_entry = namespace_entry.map_err(|err| {
            with_context(
                err,
                format!("Failed to iterate Pod namespaces in '{}'", root.display()),
            )
        })?;
        let file_type = namespace_entry.file_type().map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to inspect Pod namespace entry '{}'",
                    namespace_entry.path().display()
                ),
            )
        })?;
        if !file_type.is_dir() {
            continue;
        }
        let namespace_name = match namespace_entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };

        let namespace_path = namespace_entry.path();
        let pod_entries = fs::read_dir(&namespace_path).map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to read Pod namespace directory '{}'",
                    namespace_path.display()
                ),
            )
        })?;

        for pod_entry in pod_entries {
            let pod_entry = pod_entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate Pod directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            let entry_type = pod_entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect Pod entry '{}'",
                        pod_entry.path().display()
                    ),
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let pod_name = match pod_entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = value_file_path(POD_PREFIX, &namespace_name, &pod_name);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!("Failed to load Pod payload '{}'", value_path.display()),
                    ))
                }
            };

            let mut workload: Pod = serde_json::from_str(&raw).map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to deserialize Pod '{}' from '{}'",
                        pod_name,
                        value_path.display()
                    ),
                )
            })?;
            workload.status = None;
            if workload.metadata.name.is_none() {
                workload.metadata.name = Some(pod_name.clone());
            }
            let namespace = workload
                .metadata
                .namespace
                .clone()
                .filter(|ns| !ns.is_empty())
                .or_else(|| {
                    if namespace_name == "default" {
                        None
                    } else {
                        Some(namespace_name.clone())
                    }
                });

            results.push(StoredPod {
                namespace,
                name: pod_name,
                workload,
            });
        }
    }

    Ok(results)
}

pub fn save_pod_manifest(
    namespace: Option<&str>,
    app: &str,
    workload: &Pod,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    validate_resource_target(
        "Pod",
        app,
        namespace,
        workload.metadata.name.as_deref(),
        workload.metadata.namespace.as_deref(),
    )?;
    let key = make_pod_key(namespace, app);
    with_resource_lock(&key, || {
        let mut sanitized = workload.clone();
        if let Some(status) = workload
            .status
            .as_ref()
            .and_then(|status| status.pod_ip.as_deref())
        {
            sanitized
                .metadata
                .annotations
                .insert(POD_IP_ANNOTATION.to_string(), status.to_string());
        } else {
            sanitized.metadata.annotations.remove(POD_IP_ANNOTATION);
        }
        sanitized.status = None;
        bump_resource_version(&mut sanitized.metadata);
        let payload = serde_json::to_string(&sanitized).map_err(|err| {
            with_context(err, format!("Failed to serialize Pod for key '{}'", key))
        })?;
        K8S_KEYSPACE
            .put(&key, &payload)
            .map_err(|err| with_context(err, format!("Failed to persist Pod '{}'", key)))?;
        {
            let mut cache = pod_cache().lock().unwrap_or_else(|err| err.into_inner());
            cache.insert(key.clone(), sanitized);
        }
        if let Err(err) = delete(namespace, app) {
            if !is_missing_value_error(err.as_ref()) {
                return Err(err);
            }
        }
        Ok(())
    })
}

pub fn load_pod_manifest(
    namespace: Option<&str>,
    app: &str,
) -> Result<Option<Pod>, Box<dyn Error + Send + Sync>> {
    let key = make_pod_key(namespace, app);
    if let Ok(cache) = pod_cache().lock() {
        if let Some(pod) = cache.get(&key) {
            return Ok(Some(pod.clone()));
        }
    }
    let raw = K8S_KEYSPACE
        .get_optional(&key)
        .map_err(|err| with_context(err, format!("Failed to load Pod '{}' from keyspace", key)))?;
    if let Some(raw) = raw {
        let mut pod: Pod = serde_json::from_str(&raw)
            .map_err(|err| with_context(err, format!("Failed to parse Pod from key '{}'", key)))?;
        pod.status = None;
        if let Ok(mut cache) = pod_cache().lock() {
            cache.insert(key.clone(), pod.clone());
        }
        Ok(Some(pod))
    } else {
        match load(namespace, app)? {
            Some(legacy) => {
                let converted = pod_from_statefulset(&legacy);
                if let Ok(mut cache) = pod_cache().lock() {
                    cache.insert(key.clone(), converted.clone());
                }
                Ok(Some(converted))
            }
            None => Ok(None),
        }
    }
}

pub fn delete_pod_manifest(
    namespace: Option<&str>,
    app: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_pod_key(namespace, app);
    validate_resource_target("Pod", app, namespace, None, None)?;
    with_resource_lock(&key, || {
        match K8S_KEYSPACE.delete(&key) {
            Ok(()) => {}
            Err(err) => {
                if !is_missing_value_error(err.as_ref()) {
                    return Err(with_context(
                        err,
                        format!("Failed to delete Pod '{}' from keyspace", key),
                    ));
                }
            }
        }
        if let Err(err) = delete(namespace, app) {
            if !is_missing_value_error(err.as_ref()) {
                return Err(err);
            }
        }
        if let Ok(mut cache) = pod_cache().lock() {
            cache.remove(&key);
        }
        Ok(())
    })
}

fn pod_from_statefulset(workload: &StatefulSet) -> Pod {
    let mut metadata = workload.spec.template.metadata.clone();
    metadata.name = workload.metadata.name.clone();
    metadata.namespace = workload.metadata.namespace.clone();
    Pod {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        metadata,
        spec: workload.spec.template.spec.clone(),
        status: None,
    }
}

fn make_pod_key(namespace: Option<&str>, app: &str) -> String {
    namespaced_key(POD_PREFIX, namespace, app)
}
