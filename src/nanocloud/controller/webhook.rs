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

//! Webhook controller for edge webhook handling.
//!
//! Watches Webhook CRDs, validates their configuration, manages secret references
//! and dedupe state, and emits events when validation or Job creation succeeds/fails.

use crate::nanocloud::controller::events::{EventRecorder, InvolvedObjectRef};
use crate::nanocloud::controller::runtime::{
    ControllerRuntime, ControllerTarget, ControllerWorkItem,
};
use crate::nanocloud::controller::watch::{ControllerWatchEvent, ControllerWatchManager};
use crate::nanocloud::k8s::store::{get_webhook, list_webhooks, normalize_namespace, save_webhook};
use crate::nanocloud::k8s::webhook::Webhook;
use crate::nanocloud::logger::{log_debug, log_error, log_info, log_warn};
use crate::nanocloud::observability::metrics::{self, ControllerReconcileResult};
use crate::nanocloud::util::KeyspaceEventType;

use std::io;
use std::sync::Arc;
use tokio::task::JoinHandle;

const COMPONENT: &str = "webhook-controller";
const WEBHOOK_PREFIX: &str = "/webhooks";

/// Spawns the Webhook controller background task.
pub fn spawn() -> JoinHandle<()> {
    tokio::spawn(async move {
        let runtime = ControllerRuntime::shared();
        let recorder = EventRecorder::new(COMPONENT);

        start_webhook_executor(&runtime, recorder.clone());
        bootstrap_existing_webhooks(&runtime).await;
        watch_webhook_events(runtime, recorder).await;
    })
}

fn start_webhook_executor(runtime: &Arc<ControllerRuntime>, recorder: EventRecorder) {
    if let Err(err) = runtime.spawn_executor(move |item| {
        let recorder = recorder.clone();
        async move {
            if let ControllerTarget::Webhook { namespace, name } = &item.target {
                if let Err(err) =
                    reconcile_webhook(namespace.clone(), name.clone(), recorder.clone()).await
                {
                    log_error(
                        COMPONENT,
                        "Webhook reconciliation failed",
                        &[
                            ("namespace", namespace.as_deref().unwrap_or("default")),
                            ("webhook", name.as_str()),
                            ("error", err.as_str()),
                        ],
                    );
                    return Err(
                        Box::new(io::Error::other(err)) as Box<dyn std::error::Error + Send + Sync>
                    );
                }
            }
            Ok(())
        }
    }) {
        log_error(
            COMPONENT,
            "Failed to start webhook dispatcher",
            &[("error", err.to_string().as_str())],
        );
    }
}

async fn bootstrap_existing_webhooks(runtime: &Arc<ControllerRuntime>) {
    match list_webhooks() {
        Ok(existing) => {
            if !existing.is_empty() {
                log_info(
                    COMPONENT,
                    "Reconciling existing Webhooks on startup",
                    &[("count", existing.len().to_string().as_str())],
                );
            }
            for stored in existing {
                enqueue_webhook(runtime, stored.namespace, stored.name).await;
            }
        }
        Err(err) => {
            log_error(
                COMPONENT,
                "Failed to list existing Webhooks",
                &[("error", err.to_string().as_str())],
            );
        }
    }
}

async fn watch_webhook_events(runtime: Arc<ControllerRuntime>, recorder: EventRecorder) {
    let manager = ControllerWatchManager::shared();
    let mut subscription = manager.subscribe(WEBHOOK_PREFIX, None);

    while let Some(event) = subscription.recv().await {
        match event.event_type {
            KeyspaceEventType::Deleted => {
                if let Some((ns, name)) = parse_webhook_key(event.key.as_str()) {
                    let ns_label = ns.clone().unwrap_or_else(|| "default".to_string());
                    log_debug(
                        COMPONENT,
                        "Webhook deleted",
                        &[("namespace", ns_label.as_str()), ("webhook", name.as_str())],
                    );
                    enqueue_webhook(&runtime, ns, name).await;
                }
            }
            KeyspaceEventType::Added | KeyspaceEventType::Modified => {
                if let Some((namespace, name)) = webhook_identity(&event) {
                    enqueue_webhook(&runtime, namespace, name).await;
                } else {
                    log_warn(
                        COMPONENT,
                        "Webhook event missing identity",
                        &[("key", event.key.as_str())],
                    );
                }
            }
        }
    }

    drop(recorder);
}

fn webhook_identity(event: &ControllerWatchEvent) -> Option<(Option<String>, String)> {
    if let Some(value) = event.value.as_ref() {
        if let Ok(webhook) = serde_json::from_str::<Webhook>(value) {
            let name = webhook.metadata.name.clone().unwrap_or_default();
            if !name.is_empty() {
                return Some((webhook.metadata.namespace.clone(), name));
            }
        }
    }
    parse_webhook_key(event.key.as_str())
}

fn parse_webhook_key(key: &str) -> Option<(Option<String>, String)> {
    let parts: Vec<&str> = key
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    if parts.len() != 3 || parts[0] != WEBHOOK_PREFIX.trim_start_matches('/') {
        return None;
    }
    let namespace = if parts[1].eq_ignore_ascii_case("default") {
        Some("default".to_string())
    } else {
        Some(parts[1].to_string())
    };
    let name = parts[2].to_string();
    Some((namespace, name))
}

async fn enqueue_webhook(
    runtime: &Arc<ControllerRuntime>,
    namespace: Option<String>,
    name: String,
) {
    let item = ControllerWorkItem::webhook(namespace.as_deref(), name.as_str());
    match runtime.work_queue().enqueue(item).await {
        Ok(true) => {}
        Ok(false) => {
            log_debug(
                COMPONENT,
                "Coalesced webhook reconciliation request",
                &[
                    ("namespace", namespace.as_deref().unwrap_or("default")),
                    ("webhook", name.as_str()),
                ],
            );
        }
        Err(err) => {
            log_warn(
                COMPONENT,
                "Failed to enqueue webhook reconciliation",
                &[
                    ("namespace", namespace.as_deref().unwrap_or("default")),
                    ("webhook", name.as_str()),
                    ("error", err.to_string().as_str()),
                ],
            );
        }
    }
}

async fn reconcile_webhook(
    namespace: Option<String>,
    name: String,
    recorder: EventRecorder,
) -> Result<(), String> {
    let namespace_label = normalize_namespace(namespace.as_deref());

    let Some(mut webhook) = get_webhook(namespace.as_deref(), &name).map_err(|e| e.to_string())?
    else {
        log_debug(
            COMPONENT,
            "Webhook missing during reconciliation",
            &[
                ("namespace", namespace_label.as_str()),
                ("webhook", name.as_str()),
            ],
        );
        metrics::record_controller_reconcile("webhook", ControllerReconcileResult::Success);
        return Ok(());
    };

    log_info(
        COMPONENT,
        "Reconciling webhook",
        &[
            ("namespace", namespace_label.as_str()),
            ("name", name.as_str()),
            ("path", webhook.spec.path.as_str()),
        ],
    );

    // Validate the webhook specification
    let validation_result = webhook.validate();
    let mut issues: Vec<String> = Vec::new();

    if let Err(err) = validation_result {
        issues.push(err.to_string());
    }

    // Additional validation: check secret reference if configured
    if let Some(ref secret_ref) = webhook.spec.secret_ref {
        // In a real implementation, we'd verify the secret exists
        // For now, we just validate the reference is well-formed
        if secret_ref.name.is_empty() {
            issues.push("secretRef.name must not be empty".to_string());
        }
        if secret_ref.key.is_empty() {
            issues.push("secretRef.key must not be empty".to_string());
        }
    }

    let is_ready = issues.is_empty();
    let reason = if is_ready {
        None
    } else {
        Some("ValidationFailed")
    };
    let message = if is_ready {
        None
    } else {
        Some(issues.join("; "))
    };

    // Update the webhook status
    let mut status = webhook.status.take().unwrap_or_default();
    status.set_ready(is_ready, reason, message.as_deref());

    webhook.status = Some(status);

    // Persist the updated webhook
    if let Err(err) = save_webhook(namespace.as_deref(), &name, webhook.clone()) {
        log_error(
            COMPONENT,
            "Failed to save webhook status",
            &[
                ("namespace", namespace_label.as_str()),
                ("webhook", name.as_str()),
                ("error", err.to_string().as_str()),
            ],
        );
        return Err(err.to_string());
    }

    // Emit Kubernetes event
    let involved = InvolvedObjectRef {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "Webhook".to_string(),
        name: name.clone(),
        uid: webhook.metadata.uid.clone(),
        namespace: Some(namespace_label.clone()),
    };

    let (event_reason, event_type, event_message) = if is_ready {
        (
            "WebhookReconciled",
            "Normal",
            format!(
                "Webhook {} reconciled successfully, listening on path {}",
                name, webhook.spec.path
            ),
        )
    } else {
        (
            "WebhookValidationFailed",
            "Warning",
            format!(
                "Webhook {} validation failed: {}",
                name,
                message.as_deref().unwrap_or("unknown error")
            ),
        )
    };

    recorder
        .record(
            Some(namespace_label.as_str()),
            &involved,
            event_reason,
            event_type,
            event_message.as_str(),
        )
        .await;

    let result = if is_ready {
        ControllerReconcileResult::Success
    } else {
        ControllerReconcileResult::Error
    };
    metrics::record_controller_reconcile("webhook", result);

    if is_ready {
        log_info(
            COMPONENT,
            "Webhook reconciled",
            &[
                ("namespace", namespace_label.as_str()),
                ("name", name.as_str()),
                ("ready", "true"),
            ],
        );
    } else {
        log_warn(
            COMPONENT,
            "Webhook not ready",
            &[
                ("namespace", namespace_label.as_str()),
                ("name", name.as_str()),
                ("reason", message.as_deref().unwrap_or("unknown")),
            ],
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_webhook_key_extracts_namespace_and_name() {
        let result = parse_webhook_key("/webhooks/default/my-webhook");
        assert_eq!(
            result,
            Some((Some("default".to_string()), "my-webhook".to_string()))
        );

        let result = parse_webhook_key("/webhooks/production/github-deploy");
        assert_eq!(
            result,
            Some((Some("production".to_string()), "github-deploy".to_string()))
        );
    }

    #[test]
    fn parse_webhook_key_returns_none_for_invalid() {
        assert_eq!(parse_webhook_key("/other/default/name"), None);
        assert_eq!(parse_webhook_key("/webhooks/default"), None);
        assert_eq!(parse_webhook_key(""), None);
    }
}
