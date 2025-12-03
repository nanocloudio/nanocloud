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

use crate::nanocloud::k8s::statefulset::StatefulSet;
use crate::nanocloud::k8s::store::common::{
    bump_resource_version, namespaced_key, namespaced_root, normalize_namespace,
    validate_resource_target, value_file_path, with_resource_lock, K8S_KEYSPACE,
    STATEFULSET_PREFIX,
};
use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::is_missing_value_error;

use std::error::Error;
use std::fs;
use std::io::ErrorKind;

#[derive(Debug)]
pub struct StoredStatefulSet {
    pub namespace: Option<String>,
    pub name: String,
    pub workload: StatefulSet,
}

pub fn list_stateful_sets() -> Result<Vec<StoredStatefulSet>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = namespaced_root(STATEFULSET_PREFIX);
    let namespace_entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
        Err(err) => {
            return Err(with_context(
                err,
                format!(
                    "Failed to read StatefulSet root directory '{}'",
                    root.display()
                ),
            ))
        }
    };

    for namespace_entry in namespace_entries {
        let namespace_entry = namespace_entry.map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to iterate StatefulSet namespaces in '{}'",
                    root.display()
                ),
            )
        })?;
        let file_type = namespace_entry.file_type().map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to inspect StatefulSet namespace entry '{}'",
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
        let service_entries = fs::read_dir(&namespace_path).map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to read StatefulSet namespace directory '{}'",
                    namespace_path.display()
                ),
            )
        })?;

        for service_entry in service_entries {
            let service_entry = service_entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate StatefulSet directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            let entry_type = service_entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect StatefulSet entry '{}'",
                        service_entry.path().display()
                    ),
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let service_name = match service_entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = value_file_path(STATEFULSET_PREFIX, &namespace_name, &service_name);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!(
                            "Failed to load StatefulSet payload '{}'",
                            value_path.display()
                        ),
                    ))
                }
            };

            let mut workload: StatefulSet = serde_json::from_str(&raw).map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to deserialize StatefulSet '{}' from '{}'",
                        service_name,
                        value_path.display()
                    ),
                )
            })?;

            if workload.metadata.name.is_none() {
                workload.metadata.name = Some(service_name.clone());
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

            results.push(StoredStatefulSet {
                namespace,
                name: service_name,
                workload,
            });
        }
    }

    Ok(results)
}

pub fn list_stateful_sets_for(
    namespace: Option<&str>,
) -> Result<Vec<StatefulSet>, Box<dyn Error + Send + Sync>> {
    let filter = namespace.map(|ns| normalize_namespace(Some(ns)));
    let mut filtered = Vec::new();
    for stored in list_stateful_sets()? {
        let namespace_value = normalize_namespace(stored.namespace.as_deref());
        if filter
            .as_ref()
            .is_none_or(|candidate| candidate == &namespace_value)
        {
            let mut workload = stored.workload;
            if workload.metadata.name.is_none() {
                workload.metadata.name = Some(stored.name.clone());
            }
            workload.metadata.namespace = Some(namespace_value.clone());
            if workload.metadata.resource_version.is_none() {
                workload.metadata.resource_version = Some("1".to_string());
            }
            filtered.push(workload);
        }
    }
    Ok(filtered)
}

pub fn get_stateful_set(
    namespace: Option<&str>,
    name: &str,
) -> Result<Option<StatefulSet>, Box<dyn Error + Send + Sync>> {
    let namespace_value = normalize_namespace(namespace);
    let value_path = value_file_path(STATEFULSET_PREFIX, &namespace_value, name);
    let raw = match fs::read_to_string(&value_path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(with_context(
                err,
                format!(
                    "Failed to load StatefulSet payload '{}'",
                    value_path.display()
                ),
            ))
        }
    };

    let mut workload: StatefulSet = serde_json::from_str(&raw).map_err(|err| {
        with_context(
            err,
            format!(
                "Failed to deserialize StatefulSet '{}' from '{}'",
                name,
                value_path.display()
            ),
        )
    })?;

    if workload.metadata.name.is_none() {
        workload.metadata.name = Some(name.to_string());
    }
    workload.metadata.namespace = Some(namespace_value);

    Ok(Some(workload))
}

pub fn save(
    namespace: Option<&str>,
    app: &str,
    workload: &StatefulSet,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    validate_resource_target(
        "StatefulSet",
        app,
        namespace,
        workload.metadata.name.as_deref(),
        workload.metadata.namespace.as_deref(),
    )?;
    let key = make_statefulset_key(namespace, app);
    with_resource_lock(&key, || {
        let mut payload = workload.clone();
        bump_resource_version(&mut payload.metadata);
        let payload = serde_json::to_string(&payload).map_err(|err| {
            with_context(
                err,
                format!("Failed to serialize StatefulSet for key '{}'", key),
            )
        })?;
        K8S_KEYSPACE
            .put(&key, &payload)
            .map_err(|err| with_context(err, format!("Failed to persist StatefulSet '{}'", key)))
    })
}

pub fn load(
    namespace: Option<&str>,
    app: &str,
) -> Result<Option<StatefulSet>, Box<dyn Error + Send + Sync>> {
    let key = make_statefulset_key(namespace, app);
    match K8S_KEYSPACE.get(&key) {
        Ok(raw) => {
            let parsed = serde_json::from_str(&raw).map_err(|err| {
                with_context(
                    err,
                    format!("Failed to parse StatefulSet from key '{}'", key),
                )
            })?;
            Ok(Some(parsed))
        }
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(None)
            } else {
                Err(with_context(
                    err,
                    format!("Failed to load StatefulSet '{}' from keyspace", key),
                ))
            }
        }
    }
}

pub fn delete(namespace: Option<&str>, app: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_statefulset_key(namespace, app);
    validate_resource_target("StatefulSet", app, namespace, None, None)?;
    with_resource_lock(&key, || match K8S_KEYSPACE.delete(&key) {
        Ok(()) => Ok(()),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(())
            } else {
                Err(with_context(
                    err,
                    format!("Failed to delete StatefulSet '{}' from keyspace", key),
                ))
            }
        }
    })
}

fn make_statefulset_key(namespace: Option<&str>, app: &str) -> String {
    namespaced_key(STATEFULSET_PREFIX, namespace, app)
}
