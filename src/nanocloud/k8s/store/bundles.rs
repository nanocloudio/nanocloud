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

use crate::nanocloud::api::types::Bundle;
use crate::nanocloud::k8s::ownership::BundleFieldOwnership;
use crate::nanocloud::k8s::store::common::{
    bump_resource_version, delete_ownership, deserialize_from_store, load_ownership,
    namespaced_key, namespaced_root, normalize_namespace, save_ownership, serialize_for_store,
    validate_resource_target, value_file_path, with_resource_lock, BUNDLE_OWNER_FILE,
    BUNDLE_PREFIX, K8S_KEYSPACE,
};
use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::is_missing_value_error;

use std::error::Error;
use std::fs;
use std::io::ErrorKind;

pub fn list_bundles(namespace: Option<&str>) -> Result<Vec<Bundle>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = namespaced_root(BUNDLE_PREFIX);

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
                                "Failed to iterate Bundle namespaces in '{}'",
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
                                    "Failed to inspect Bundle namespace entry '{}'",
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
                    format!("Failed to read Bundle root directory '{}'", root.display()),
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
                        "Failed to read Bundle namespace directory '{}'",
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
                        "Failed to iterate Bundle directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            let entry_type = entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect Bundle entry '{}'",
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

            let value_path = value_file_path(BUNDLE_PREFIX, &ns, &name);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!("Failed to load Bundle payload '{}'", value_path.display()),
                    ))
                }
            };

            let mut bundle: Bundle = deserialize_from_store(
                "Bundle",
                &raw,
                &format!(
                    "Failed to deserialize Bundle '{}' from '{}'",
                    name,
                    value_path.display()
                ),
            )?;

            if bundle.metadata.name.is_none() {
                bundle.metadata.name = Some(name.clone());
            }
            if bundle.metadata.namespace.is_none() {
                bundle.metadata.namespace = Some(ns.clone());
            }
            if bundle.metadata.resource_version.is_none() {
                bundle.metadata.resource_version = Some("1".to_string());
            }

            results.push(bundle);
        }
    }

    Ok(results)
}

pub fn load_bundle_field_ownership(
    namespace: Option<&str>,
    name: &str,
) -> Result<BundleFieldOwnership, Box<dyn Error + Send + Sync>> {
    load_ownership(BUNDLE_PREFIX, namespace, name, BUNDLE_OWNER_FILE)
}

pub fn save_bundle_field_ownership(
    namespace: Option<&str>,
    name: &str,
    ownership: &BundleFieldOwnership,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    validate_resource_target("Bundle", name, namespace, None, None)?;
    let key = namespaced_key(BUNDLE_PREFIX, namespace, name);
    with_resource_lock(&key, || {
        save_ownership(BUNDLE_PREFIX, namespace, name, BUNDLE_OWNER_FILE, ownership)
    })
}

pub fn delete_bundle_field_ownership(
    namespace: Option<&str>,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    validate_resource_target("Bundle", name, namespace, None, None)?;
    let key = namespaced_key(BUNDLE_PREFIX, namespace, name);
    with_resource_lock(&key, || {
        delete_ownership(BUNDLE_PREFIX, namespace, name, BUNDLE_OWNER_FILE)
    })
}

pub fn save_bundle(
    namespace: Option<&str>,
    name: &str,
    bundle: &Bundle,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    validate_resource_target(
        "Bundle",
        name,
        namespace,
        bundle.metadata.name.as_deref(),
        bundle.metadata.namespace.as_deref(),
    )?;
    let key = namespaced_key(BUNDLE_PREFIX, namespace, name);
    with_resource_lock(&key, || {
        let mut payload = bundle.clone();
        bump_resource_version(&mut payload.metadata);
        let payload = serialize_for_store(
            "Bundle",
            &payload,
            &format!("Failed to serialize Bundle for key '{}'", key),
        )?;
        K8S_KEYSPACE
            .put(&key, &payload)
            .map_err(|err| with_context(err, format!("Failed to persist Bundle '{}'", key)))
    })
}

pub fn delete_bundle(
    namespace: Option<&str>,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = namespaced_key(BUNDLE_PREFIX, namespace, name);
    validate_resource_target("Bundle", name, namespace, None, None)?;
    with_resource_lock(&key, || match K8S_KEYSPACE.delete(&key) {
        Ok(()) => Ok(()),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(())
            } else {
                Err(with_context(
                    err,
                    format!("Failed to delete Bundle '{}' from keyspace", key),
                ))
            }
        }
    })
}
