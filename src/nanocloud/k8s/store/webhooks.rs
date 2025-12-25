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

//! Filesystem-backed storage for Webhook CRDs.

use crate::nanocloud::k8s::store::common::{
    bump_resource_version, deserialize_from_store, ensure_resource_version, namespaced_root,
    normalize_namespace, serialize_for_store, validate_resource_target, value_file_path,
    write_atomic_files,
};
use crate::nanocloud::k8s::webhook::Webhook;
use crate::nanocloud::util::error::with_context;

use std::error::Error;
use std::fs;
use std::io::ErrorKind;

pub const WEBHOOK_DIR: &str = "webhooks";

#[derive(Debug)]
pub struct StoredWebhook {
    pub namespace: Option<String>,
    pub name: String,
    pub webhook: Webhook,
}

/// Lists all stored Webhooks across all namespaces.
pub fn list_webhooks() -> Result<Vec<StoredWebhook>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = namespaced_root(WEBHOOK_DIR);
    let namespace_entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
        Err(err) => {
            return Err(with_context(
                err,
                format!("Failed to read Webhook root directory '{}'", root.display()),
            ))
        }
    };

    for namespace_entry in namespace_entries {
        let namespace_entry = namespace_entry.map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to iterate Webhook namespaces in '{}'",
                    root.display()
                ),
            )
        })?;
        let entry_type = namespace_entry.file_type().map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to inspect Webhook namespace entry '{}'",
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
        let webhook_entries = fs::read_dir(&namespace_path).map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to read Webhook namespace directory '{}'",
                    namespace_path.display()
                ),
            )
        })?;

        for webhook_entry in webhook_entries {
            let webhook_entry = webhook_entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate Webhook directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            let entry_type = webhook_entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect Webhook entry '{}'",
                        webhook_entry.path().display()
                    ),
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let webhook_name = match webhook_entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = value_file_path(WEBHOOK_DIR, &namespace_name, &webhook_name);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!(
                            "Failed to read Webhook value for '{}'",
                            value_path.display()
                        ),
                    ))
                }
            };
            let webhook: Webhook =
                deserialize_from_store("Webhook", &raw, &value_path.display().to_string())?;

            let namespace = Some(namespace_name.clone()).filter(|ns| ns != "default");
            results.push(StoredWebhook {
                namespace,
                name: webhook_name,
                webhook,
            });
        }
    }

    Ok(results)
}

/// Lists Webhooks in a specific namespace.
pub fn list_webhooks_for(
    namespace: Option<&str>,
) -> Result<Vec<StoredWebhook>, Box<dyn Error + Send + Sync>> {
    let all = list_webhooks()?;
    let ns = normalize_namespace(namespace);
    Ok(all
        .into_iter()
        .filter(|stored| normalize_namespace(stored.namespace.as_deref()) == ns)
        .collect())
}

/// Gets a single Webhook by namespace and name.
pub fn get_webhook(
    namespace: Option<&str>,
    name: &str,
) -> Result<Option<Webhook>, Box<dyn Error + Send + Sync>> {
    let ns = normalize_namespace(namespace);
    let value_path = value_file_path(WEBHOOK_DIR, &ns, name);
    let raw = match fs::read_to_string(&value_path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(with_context(
                err,
                format!("Failed to read Webhook '{}/{}'", ns, name),
            ))
        }
    };
    let webhook: Webhook = deserialize_from_store("Webhook", &raw, &format!("{}/{}", ns, name))?;
    Ok(Some(webhook))
}

/// Saves a Webhook to the store.
pub fn save_webhook(
    namespace: Option<&str>,
    name: &str,
    mut webhook: Webhook,
) -> Result<Webhook, Box<dyn Error + Send + Sync>> {
    validate_resource_target(
        "Webhook",
        name,
        namespace,
        webhook.metadata.name.as_deref(),
        webhook.metadata.namespace.as_deref(),
    )?;

    let ns = normalize_namespace(namespace);

    // Ensure metadata is populated
    if webhook.metadata.name.is_none() {
        webhook.metadata.name = Some(name.to_string());
    }
    if webhook.metadata.namespace.is_none() && ns != "default" {
        webhook.metadata.namespace = Some(ns.clone());
    }

    // Handle resource version
    let existing = get_webhook(namespace, name)?;
    if existing.is_some() {
        bump_resource_version(&mut webhook.metadata);
    } else {
        ensure_resource_version(&mut webhook.metadata);
    }

    let value_path = value_file_path(WEBHOOK_DIR, &ns, name);
    let payload = serialize_for_store("Webhook", &webhook, &format!("Webhook {}/{}", ns, name))?;
    write_atomic_files(&[(&value_path, &payload)])?;

    Ok(webhook)
}

/// Deletes a Webhook from the store.
pub fn delete_webhook(
    namespace: Option<&str>,
    name: &str,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let ns = normalize_namespace(namespace);
    let webhook_dir = namespaced_root(WEBHOOK_DIR).join(&ns).join(name);

    match fs::remove_dir_all(&webhook_dir) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
        Err(err) => Err(with_context(
            err,
            format!("Failed to delete Webhook '{}/{}'", ns, name),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::job::JobSpec;
    use crate::nanocloud::k8s::pod::{ContainerSpec, ObjectMeta, PodSpec};
    use crate::nanocloud::k8s::statefulset::PodTemplateSpec;
    use crate::nanocloud::k8s::webhook::{WebhookJobTemplate, WebhookSpec};
    use tempfile::TempDir;

    fn make_job_template() -> WebhookJobTemplate {
        WebhookJobTemplate {
            metadata: ObjectMeta {
                generate_name: Some("webhook-job-".to_string()),
                ..Default::default()
            },
            spec: JobSpec {
                template: PodTemplateSpec {
                    metadata: ObjectMeta::default(),
                    spec: PodSpec {
                        containers: vec![ContainerSpec {
                            name: "runner".to_string(),
                            image: Some("runner:latest".to_string()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                },
                ..Default::default()
            },
        }
    }

    fn with_temp_keyspace<F, T>(test: F) -> T
    where
        F: FnOnce() -> T,
    {
        use crate::nanocloud::test_support::keyspace_lock;
        let _lock = keyspace_lock().lock();
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let keyspace_prev = std::env::var("NANOCLOUD_KEYSPACE").ok();
        std::env::set_var("NANOCLOUD_KEYSPACE", temp_dir.path());
        let result = test();
        if let Some(prev) = keyspace_prev {
            std::env::set_var("NANOCLOUD_KEYSPACE", prev);
        } else {
            std::env::remove_var("NANOCLOUD_KEYSPACE");
        }
        result
    }

    #[test]
    fn save_and_get_webhook() {
        with_temp_keyspace(|| {
            let webhook = Webhook::new(
                "test-webhook",
                WebhookSpec::new("/webhooks/test", make_job_template()),
            );

            let saved = save_webhook(Some("default"), "test-webhook", webhook).unwrap();
            assert!(saved.metadata.resource_version.is_some());

            let loaded = get_webhook(Some("default"), "test-webhook").unwrap();
            assert!(loaded.is_some());
            let loaded = loaded.unwrap();
            assert_eq!(loaded.name(), "test-webhook");
            assert_eq!(loaded.spec.path, "/webhooks/test");
        });
    }

    #[test]
    fn list_webhooks_returns_all() {
        with_temp_keyspace(|| {
            let webhook1 = Webhook::new(
                "webhook1",
                WebhookSpec::new("/webhooks/one", make_job_template()),
            );
            let webhook2 = Webhook::new(
                "webhook2",
                WebhookSpec::new("/webhooks/two", make_job_template()),
            );

            save_webhook(Some("default"), "webhook1", webhook1).unwrap();
            save_webhook(Some("other"), "webhook2", webhook2).unwrap();

            let all = list_webhooks().unwrap();
            assert_eq!(all.len(), 2);

            let default_only = list_webhooks_for(Some("default")).unwrap();
            assert_eq!(default_only.len(), 1);
            assert_eq!(default_only[0].name, "webhook1");
        });
    }

    #[test]
    fn delete_webhook_removes_resource() {
        with_temp_keyspace(|| {
            let webhook = Webhook::new(
                "to-delete",
                WebhookSpec::new("/webhooks/delete", make_job_template()),
            );

            save_webhook(Some("default"), "to-delete", webhook).unwrap();
            assert!(get_webhook(Some("default"), "to-delete").unwrap().is_some());

            let deleted = delete_webhook(Some("default"), "to-delete").unwrap();
            assert!(deleted);

            assert!(get_webhook(Some("default"), "to-delete").unwrap().is_none());

            // Deleting again should return false
            let deleted_again = delete_webhook(Some("default"), "to-delete").unwrap();
            assert!(!deleted_again);
        });
    }
}
