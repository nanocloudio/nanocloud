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

use crate::nanocloud::k8s::deployment::Deployment;
use crate::nanocloud::k8s::store::common::{
    bump_resource_version, namespaced_key, namespaced_root, normalize_namespace,
    validate_resource_target, value_file_path, with_resource_lock, DEPLOYMENT_PREFIX, K8S_KEYSPACE,
};
use crate::nanocloud::logger::log_warn;
use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::is_missing_value_error;

use std::error::Error;
use std::fs;
use std::io::ErrorKind;

#[derive(Debug)]
pub struct StoredDeployment {
    pub namespace: Option<String>,
    pub name: String,
    pub workload: Deployment,
}

pub fn list_deployments() -> Result<Vec<StoredDeployment>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = namespaced_root(DEPLOYMENT_PREFIX);
    let namespace_entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
        Err(err) => {
            return Err(with_context(
                err,
                format!(
                    "Failed to read Deployment root directory '{}'",
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
                    "Failed to iterate Deployment namespaces in '{}'",
                    root.display()
                ),
            )
        })?;
        let file_type = namespace_entry.file_type().map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to inspect Deployment namespace entry '{}'",
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
        let deployment_entries = fs::read_dir(&namespace_path).map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to read Deployment namespace directory '{}'",
                    namespace_path.display()
                ),
            )
        })?;

        for deployment_entry in deployment_entries {
            let deployment_entry = deployment_entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate Deployment directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            let entry_type = deployment_entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect Deployment entry '{}'",
                        deployment_entry.path().display()
                    ),
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let deployment_name = match deployment_entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = value_file_path(DEPLOYMENT_PREFIX, &namespace_name, &deployment_name);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) => {
                    log_warn(
                        "store",
                        "Failed to read Deployment payload",
                        &[
                            ("path", value_path.display().to_string().as_str()),
                            ("error", &err.to_string()),
                        ],
                    );
                    continue;
                }
            };

            let mut workload: Deployment = match serde_json::from_str(&raw) {
                Ok(workload) => workload,
                Err(err) => {
                    log_warn(
                        "store",
                        "Failed to deserialize Deployment",
                        &[
                            ("path", value_path.display().to_string().as_str()),
                            ("error", &err.to_string()),
                        ],
                    );
                    continue;
                }
            };

            if workload.api_version.is_empty() {
                workload.api_version = "apps/v1".to_string();
            }
            if workload.kind.is_empty() {
                workload.kind = "Deployment".to_string();
            }

            if workload.metadata.name.is_none() {
                workload.metadata.name = Some(deployment_name.clone());
            }
            let namespace_opt = Some(namespace_name.clone()).filter(|ns| ns != "default");
            workload.metadata.namespace = Some(normalize_namespace(namespace_opt.as_deref()));
            if workload.metadata.resource_version.is_none() {
                workload.metadata.resource_version = Some("1".to_string());
            }

            results.push(StoredDeployment {
                namespace: namespace_opt,
                name: deployment_name,
                workload,
            });
        }
    }

    Ok(results)
}

pub fn list_deployments_for(
    namespace: Option<&str>,
) -> Result<Vec<Deployment>, Box<dyn Error + Send + Sync>> {
    let filter = namespace.map(|ns| normalize_namespace(Some(ns)));
    let mut filtered = Vec::new();
    for stored in list_deployments()? {
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

pub fn get_deployment(
    namespace: Option<&str>,
    name: &str,
) -> Result<Option<Deployment>, Box<dyn Error + Send + Sync>> {
    let namespace_value = normalize_namespace(namespace);
    let value_path = value_file_path(DEPLOYMENT_PREFIX, &namespace_value, name);
    let raw = match fs::read_to_string(&value_path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(with_context(
                err,
                format!(
                    "Failed to load Deployment payload '{}'",
                    value_path.display()
                ),
            ))
        }
    };

    let mut workload: Deployment = serde_json::from_str(&raw).map_err(|err| {
        with_context(
            err,
            format!(
                "Failed to deserialize Deployment '{}' from '{}'",
                name,
                value_path.display()
            ),
        )
    })?;

    if workload.api_version.is_empty() {
        workload.api_version = "apps/v1".to_string();
    }
    if workload.kind.is_empty() {
        workload.kind = "Deployment".to_string();
    }

    if workload.metadata.name.is_none() {
        workload.metadata.name = Some(name.to_string());
    }
    workload.metadata.namespace = Some(namespace_value.clone());
    if workload.metadata.resource_version.is_none() {
        workload.metadata.resource_version = Some("1".to_string());
    }

    Ok(Some(workload))
}

pub fn delete_deployment(
    namespace: Option<&str>,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_deployment_key(namespace, name);
    validate_resource_target("Deployment", name, namespace, None, None)?;
    with_resource_lock(&key, || match K8S_KEYSPACE.delete(&key) {
        Ok(()) => Ok(()),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(())
            } else {
                Err(with_context(
                    err,
                    format!("Failed to delete Deployment '{}' from keyspace", key),
                ))
            }
        }
    })
}

fn make_deployment_key(namespace: Option<&str>, name: &str) -> String {
    namespaced_key(DEPLOYMENT_PREFIX, namespace, name)
}
