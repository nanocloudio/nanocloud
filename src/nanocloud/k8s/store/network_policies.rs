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

use crate::nanocloud::k8s::networkpolicy::NetworkPolicy;
use crate::nanocloud::k8s::store::common::{namespaced_root, value_file_path, NETWORK_POLICY_DIR};
use crate::nanocloud::util::error::with_context;

use std::error::Error;
use std::fs;
use std::io::ErrorKind;

#[derive(Debug)]
pub struct StoredNetworkPolicy {
    pub namespace: Option<String>,
    pub name: String,
    pub policy: NetworkPolicy,
}

pub fn list_network_policies() -> Result<Vec<StoredNetworkPolicy>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = namespaced_root(NETWORK_POLICY_DIR);
    let namespace_entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
        Err(err) => {
            return Err(with_context(
                err,
                format!(
                    "Failed to read NetworkPolicy root directory '{}'",
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
                    "Failed to iterate NetworkPolicy namespaces in '{}'",
                    root.display()
                ),
            )
        })?;
        let entry_type = namespace_entry.file_type().map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to inspect NetworkPolicy namespace entry '{}'",
                    namespace_entry.path().display()
                ),
            )
        })?;
        if !entry_type.is_dir() {
            continue;
        }
        let namespace_name = match namespace_entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };

        let namespace_path = namespace_entry.path();
        let policy_entries = fs::read_dir(&namespace_path).map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to read NetworkPolicy namespace directory '{}'",
                    namespace_path.display()
                ),
            )
        })?;

        for policy_entry in policy_entries {
            let policy_entry = policy_entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate NetworkPolicy directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            let entry_type = policy_entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect NetworkPolicy entry '{}'",
                        policy_entry.path().display()
                    ),
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let policy_name = match policy_entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = value_file_path(NETWORK_POLICY_DIR, &namespace_name, &policy_name);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!(
                            "Failed to read NetworkPolicy value for '{}'",
                            value_path.display()
                        ),
                    ))
                }
            };
            let policy: NetworkPolicy = serde_json::from_str(&raw).map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to parse stored NetworkPolicy '{}'",
                        value_path.display()
                    ),
                )
            })?;

            let namespace = Some(namespace_name.clone()).filter(|ns| ns != "default");
            results.push(StoredNetworkPolicy {
                namespace,
                name: policy_name,
                policy,
            });
        }
    }

    Ok(results)
}
