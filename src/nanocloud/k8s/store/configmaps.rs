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

use crate::nanocloud::k8s::configmap::ConfigMap;
use crate::nanocloud::k8s::store::common::{
    deserialize_from_store, ensure_resource_version, namespaced_key, namespaced_root,
    normalize_namespace, serialize_for_store, validate_resource_target, with_resource_lock,
    CONFIGMAP_PREFIX, K8S_KEYSPACE,
};
use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::is_missing_value_error;

use std::error::Error;
use std::fs;
use std::io::ErrorKind;

pub fn save_config_map(
    namespace: Option<&str>,
    name: &str,
    config_map: &ConfigMap,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    validate_resource_target(
        "ConfigMap",
        name,
        namespace,
        config_map.metadata.name.as_deref(),
        config_map.metadata.namespace.as_deref(),
    )?;
    let key = namespaced_key(CONFIGMAP_PREFIX, namespace, name);
    with_resource_lock(&key, || {
        let mut payload = config_map.clone();
        ensure_resource_version(&mut payload.metadata);
        let payload = serialize_for_store(
            "ConfigMap",
            &payload,
            &format!("Failed to serialize ConfigMap for key '{}'", key),
        )?;
        K8S_KEYSPACE
            .put(&key, &payload)
            .map_err(|err| with_context(err, format!("Failed to persist ConfigMap '{}'", key)))
    })
}

pub fn load_config_map(
    namespace: Option<&str>,
    name: &str,
) -> Result<Option<ConfigMap>, Box<dyn Error + Send + Sync>> {
    let key = namespaced_key(CONFIGMAP_PREFIX, namespace, name);
    let raw = match K8S_KEYSPACE.get_optional(&key).map_err(|err| {
        with_context(
            err,
            format!("Failed to load ConfigMap '{}' from keyspace", key),
        )
    })? {
        Some(raw) => raw,
        None => return Ok(None),
    };
    let parsed = deserialize_from_store(
        "ConfigMap",
        &raw,
        &format!("Failed to parse ConfigMap from key '{}'", key),
    )?;
    Ok(Some(parsed))
}

pub fn delete_config_map(
    namespace: Option<&str>,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = namespaced_key(CONFIGMAP_PREFIX, namespace, name);
    validate_resource_target("ConfigMap", name, namespace, None, None)?;
    with_resource_lock(&key, || match K8S_KEYSPACE.delete(&key) {
        Ok(()) => Ok(()),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(())
            } else {
                Err(with_context(
                    err,
                    format!("Failed to delete ConfigMap '{}' from keyspace", key),
                ))
            }
        }
    })
}

pub fn list_config_maps(
    namespace: Option<&str>,
) -> Result<Vec<ConfigMap>, Box<dyn Error + Send + Sync>> {
    match namespace {
        Some(ns) => collect_configmaps(ns),
        None => {
            let mut items = Vec::new();
            let root = namespaced_root(CONFIGMAP_PREFIX);
            let entries = match fs::read_dir(&root) {
                Ok(entries) => entries,
                Err(err) if err.kind() == ErrorKind::NotFound => return Ok(items),
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!(
                            "Failed to read ConfigMap root directory '{}'",
                            root.display()
                        ),
                    ))
                }
            };

            for entry in entries {
                let entry = entry.map_err(|err| {
                    with_context(
                        err,
                        format!(
                            "Failed to iterate ConfigMap root directory '{}'",
                            root.display()
                        ),
                    )
                })?;
                let path = entry.path();
                let file_type = entry.file_type().map_err(|err| {
                    with_context(
                        err,
                        format!(
                            "Failed to inspect ConfigMap namespace directory '{}'",
                            path.display()
                        ),
                    )
                })?;
                if !file_type.is_dir() {
                    continue;
                }
                let namespace = match entry.file_name().into_string() {
                    Ok(ns) => ns,
                    Err(_) => continue,
                };
                items.extend(collect_configmaps(&namespace)?);
            }

            Ok(items)
        }
    }
}

fn collect_configmaps(namespace: &str) -> Result<Vec<ConfigMap>, Box<dyn Error + Send + Sync>> {
    let mut items = Vec::new();
    let normalized = normalize_namespace(Some(namespace));
    let dir = namespaced_root(CONFIGMAP_PREFIX).join(&normalized);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(items),
        Err(err) => {
            return Err(with_context(
                err,
                format!(
                    "Failed to read ConfigMap namespace directory '{}'",
                    dir.display()
                ),
            ))
        }
    };

    for entry in entries {
        let entry = entry.map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to iterate ConfigMap namespace directory '{}'",
                    dir.display()
                ),
            )
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to inspect ConfigMap namespace entry '{}'",
                    path.display()
                ),
            )
        })?;
        if !file_type.is_dir() {
            continue;
        }
        let name = match entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };
        if let Some(config_map) = load_config_map(Some(normalized.as_str()), &name)? {
            items.push(config_map);
        }
    }

    Ok(items)
}
