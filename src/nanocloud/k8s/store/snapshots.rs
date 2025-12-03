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

use crate::nanocloud::api::types::VolumeSnapshot;
use crate::nanocloud::k8s::store::common::{
    bump_resource_version, deserialize_from_store, namespaced_key, namespaced_root,
    normalize_namespace, serialize_for_store, validate_resource_target, value_file_path,
    with_resource_lock, K8S_KEYSPACE, SNAPSHOT_PREFIX,
};
use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::is_missing_value_error;

use std::error::Error;
use std::fs;
use std::io::ErrorKind;

pub fn list_volume_snapshots(
    namespace: Option<&str>,
) -> Result<Vec<VolumeSnapshot>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = namespaced_root(SNAPSHOT_PREFIX);

    let mut namespaces: Vec<String> = Vec::new();
    if let Some(ns) = namespace {
        namespaces.push(normalize_namespace(Some(ns)));
    } else {
        match fs::read_dir(&root) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry.map_err(|err| {
                        with_context(
                            err,
                            format!(
                                "Failed to iterate VolumeSnapshot namespaces in '{}'",
                                root.display()
                            ),
                        )
                    })?;
                    if !entry
                        .file_type()
                        .map_err(|err| {
                            with_context(
                                err,
                                format!(
                                    "Failed to inspect VolumeSnapshot namespace entry '{}'",
                                    entry.path().display()
                                ),
                            )
                        })?
                        .is_dir()
                    {
                        continue;
                    }
                    if let Ok(name) = entry.file_name().into_string() {
                        namespaces.push(name);
                    }
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
            Err(err) => {
                return Err(with_context(
                    err,
                    format!(
                        "Failed to read VolumeSnapshot root directory '{}'",
                        root.display()
                    ),
                ))
            }
        }
    }

    for ns in namespaces {
        let namespace_path = root.join(&ns);
        let entries = match fs::read_dir(&namespace_path) {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(with_context(
                    err,
                    format!(
                        "Failed to read VolumeSnapshot namespace directory '{}'",
                        namespace_path.display()
                    ),
                ))
            }
        };

        for entry in entries {
            let entry = entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate VolumeSnapshot directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            let entry_type = entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect VolumeSnapshot entry '{}'",
                        entry.path().display()
                    ),
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let name = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = value_file_path(SNAPSHOT_PREFIX, &ns, &name);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!(
                            "Failed to load VolumeSnapshot payload '{}'",
                            value_path.display()
                        ),
                    ))
                }
            };

            let mut snapshot: VolumeSnapshot = deserialize_from_store(
                "VolumeSnapshot",
                &raw,
                &format!(
                    "Failed to deserialize VolumeSnapshot '{}' from '{}'",
                    name,
                    value_path.display()
                ),
            )?;

            if snapshot.api_version.is_empty() {
                snapshot.api_version = "nanocloud.io/v1".to_string();
            }
            if snapshot.kind.is_empty() {
                snapshot.kind = "VolumeSnapshot".to_string();
            }
            if snapshot.metadata.name.is_none() {
                snapshot.metadata.name = Some(name.clone());
            }
            if snapshot.metadata.namespace.is_none() {
                snapshot.metadata.namespace = Some(ns.clone());
            }
            if snapshot.metadata.resource_version.is_none() {
                snapshot.metadata.resource_version = Some("1".to_string());
            }

            results.push(snapshot);
        }
    }

    Ok(results)
}

pub fn save_volume_snapshot(
    namespace: Option<&str>,
    name: &str,
    snapshot: &VolumeSnapshot,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    validate_resource_target(
        "VolumeSnapshot",
        name,
        namespace,
        snapshot.metadata.name.as_deref(),
        snapshot.metadata.namespace.as_deref(),
    )?;
    let key = namespaced_key(SNAPSHOT_PREFIX, namespace, name);
    with_resource_lock(&key, || {
        let mut payload = snapshot.clone();
        bump_resource_version(&mut payload.metadata);
        let payload = serialize_for_store(
            "VolumeSnapshot",
            &payload,
            &format!("Failed to serialize VolumeSnapshot for key '{}'", key),
        )?;
        K8S_KEYSPACE
            .put(&key, &payload)
            .map_err(|err| with_context(err, format!("Failed to persist VolumeSnapshot '{}'", key)))
    })
}

pub fn delete_volume_snapshot(
    namespace: Option<&str>,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = namespaced_key(SNAPSHOT_PREFIX, namespace, name);
    validate_resource_target("VolumeSnapshot", name, namespace, None, None)?;
    with_resource_lock(&key, || match K8S_KEYSPACE.delete(&key) {
        Ok(()) => Ok(()),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(())
            } else {
                Err(with_context(
                    err,
                    format!("Failed to delete VolumeSnapshot '{}' from keyspace", key),
                ))
            }
        }
    })
}
