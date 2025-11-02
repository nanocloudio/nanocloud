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

use crate::nanocloud::api::types::{
    Bundle, BundleCondition, BundleConditionStatus, BundlePhase, BundleSnapshotSource,
    BundleStatus, BundleWorkloadRef,
};
use crate::nanocloud::controller::watch::ControllerWatchManager;
use crate::nanocloud::engine::container;
use crate::nanocloud::events::in_memory::InMemoryEventBus;
use crate::nanocloud::events::{EventEnvelope, EventKey, EventPublisher, EventTopic, EventType};
use crate::nanocloud::k8s::bundle_manager::BundleRegistry;
use crate::nanocloud::logger::{log_debug, log_error, log_info, log_warn};
use crate::nanocloud::util::KeyspaceEventType;

use chrono::{SecondsFormat, Utc};
use serde_json::json;
use std::sync::Arc;
use tokio::task::JoinHandle;

const COMPONENT: &str = "bundle-controller";
const BUNDLE_PREFIX: &str = "/bundles";

pub fn spawn() -> JoinHandle<()> {
    tokio::spawn(async move {
        let registry = BundleRegistry::shared();
        let event_bus = InMemoryEventBus::global();
        // Ensure existing bundles are reconciled on startup.
        for bundle in registry.list(None).await {
            if let Err(err) = reconcile_bundle(
                Arc::clone(&registry),
                Arc::clone(&event_bus),
                bundle.clone(),
            )
            .await
            {
                let message = err;
                log_error(
                    COMPONENT,
                    "Failed to reconcile bundle during startup",
                    &[("error", message.as_str())],
                );
            }
        }

        let manager = ControllerWatchManager::shared();
        let mut subscription = manager.subscribe(BUNDLE_PREFIX, None);
        while let Some(event) = subscription.recv().await {
            match event.event_type {
                KeyspaceEventType::Deleted => continue,
                KeyspaceEventType::Added | KeyspaceEventType::Modified => {
                    let Some(value) = event.value else {
                        continue;
                    };
                    match serde_json::from_str::<Bundle>(&value) {
                        Ok(bundle) => {
                            if let Err(err) = reconcile_bundle(
                                Arc::clone(&registry),
                                Arc::clone(&event_bus),
                                bundle.clone(),
                            )
                            .await
                            {
                                log_error(
                                    COMPONENT,
                                    "Bundle reconciliation failed",
                                    &[
                                        (
                                            "bundle",
                                            bundle.metadata.name.as_deref().unwrap_or("<unnamed>"),
                                        ),
                                        ("error", err.as_str()),
                                    ],
                                );
                            }
                        }
                        Err(err) => {
                            let message = err.to_string();
                            log_error(
                                COMPONENT,
                                "Failed to deserialize bundle from keyspace event",
                                &[("error", message.as_str())],
                            );
                        }
                    }
                }
            }
        }
    })
}

async fn reconcile_bundle(
    registry: Arc<BundleRegistry>,
    event_bus: Arc<InMemoryEventBus>,
    bundle: Bundle,
) -> Result<(), String> {
    let bundle_for_event = bundle.clone();
    let result = reconcile_bundle_inner(registry, bundle).await;
    publish_bundle_event(
        Arc::clone(&event_bus),
        &bundle_for_event,
        result.as_ref().err().map(|e| e.as_str()),
    )
    .await;
    result
}

async fn reconcile_bundle_inner(
    registry: Arc<BundleRegistry>,
    bundle: Bundle,
) -> Result<(), String> {
    let Some(resource_version) = bundle.metadata.resource_version.as_deref() else {
        return Err("bundle missing resourceVersion".to_string());
    };

    let mut expected_resource_version = resource_version.to_string();
    let mut current_rv = resource_version
        .parse::<i64>()
        .map_err(|e| format!("invalid resourceVersion '{resource_version}': {e}"))?;
    if let Some(status) = bundle.status.as_ref() {
        if status
            .observed_generation
            .map(|observed| observed >= current_rv)
            .unwrap_or(false)
        {
            return Ok(());
        }
    }

    let resolved_namespace = bundle
        .spec
        .namespace
        .as_deref()
        .or(bundle.metadata.namespace.as_deref())
        .unwrap_or("default")
        .to_string();
    let bundle_name = bundle
        .metadata
        .name
        .as_deref()
        .unwrap_or(bundle.spec.service.as_str())
        .to_string();
    let service = bundle.spec.service.clone();
    let namespace_ref = if resolved_namespace == "default" {
        None
    } else {
        Some(resolved_namespace.as_str())
    };

    log_info(
        COMPONENT,
        "Reconciling bundle",
        &[
            ("namespace", resolved_namespace.as_str()),
            ("name", bundle_name.as_str()),
        ],
    );

    let install_options = bundle.spec.options.clone();

    let snapshot_path = match resolve_snapshot_source(bundle.spec.snapshot.as_ref()) {
        Ok(path) => path,
        Err(message) => {
            log_error(
                COMPONENT,
                "Bundle reconciliation failed",
                &[
                    ("namespace", resolved_namespace.as_str()),
                    ("name", bundle_name.as_str()),
                    ("error", message.as_str()),
                ],
            );
            let status = BundleStatus {
                observed_generation: Some(current_rv),
                phase: Some(BundlePhase::Failed),
                conditions: vec![make_condition(
                    "WorkloadApplied",
                    BundleConditionStatus::False,
                    Some("SnapshotInvalid".to_string()),
                    Some(message),
                )],
                workload: None,
                last_reconciled_time: Some(now_timestamp()),
            };
            apply_status(
                registry,
                &resolved_namespace,
                &bundle_name,
                status,
                Some(expected_resource_version.as_str()),
            )
            .await?;
            return Ok(());
        }
    };

    let install_result = match container::install(
        namespace_ref,
        &service,
        install_options,
        snapshot_path.as_deref(),
        bundle.spec.update,
    )
    .await
    {
        Ok(result) => result,
        Err(err) => {
            let message = err.to_string();
            log_error(
                COMPONENT,
                "Bundle installation failed",
                &[
                    ("namespace", resolved_namespace.as_str()),
                    ("name", bundle_name.as_str()),
                    ("error", message.as_str()),
                ],
            );
            let status = BundleStatus {
                observed_generation: Some(current_rv),
                phase: Some(BundlePhase::Failed),
                conditions: vec![make_condition(
                    "WorkloadApplied",
                    BundleConditionStatus::False,
                    Some("InstallFailed".to_string()),
                    Some(message),
                )],
                workload: None,
                last_reconciled_time: Some(now_timestamp()),
            };
            apply_status(
                registry,
                &resolved_namespace,
                &bundle_name,
                status,
                Some(expected_resource_version.as_str()),
            )
            .await?;
            return Ok(());
        }
    };
    let pod = install_result.pod;
    let (profile_key, persisted_options) = install_result
        .profile
        .to_serialized_fields()
        .map_err(|err| err.to_string())?;

    let updated_bundle = registry
        .update_spec_profile(
            &resolved_namespace,
            &bundle_name,
            profile_key.clone(),
            persisted_options.clone(),
        )
        .await
        .map_err(|err| err.to_string())?;
    if let Some(rv) = updated_bundle.metadata.resource_version.as_ref() {
        expected_resource_version = rv.clone();
        if let Ok(parsed) = rv.parse::<i64>() {
            current_rv = parsed;
        }
    }

    let default_workload_name = if namespace_ref.is_none() {
        service.clone()
    } else {
        format!("{}-{}", resolved_namespace, service)
    };
    let workload_name = pod.metadata.name.clone().unwrap_or(default_workload_name);
    let workload_namespace = pod
        .metadata
        .namespace
        .clone()
        .unwrap_or(resolved_namespace.clone());

    if bundle.spec.start {
        if let Err(err) = container::start(namespace_ref, &service).await {
            let message = err.to_string();
            log_error(
                COMPONENT,
                "Bundle start failed",
                &[
                    ("namespace", resolved_namespace.as_str()),
                    ("name", bundle_name.as_str()),
                    ("error", message.as_str()),
                ],
            );
            let status = BundleStatus {
                observed_generation: Some(current_rv),
                phase: Some(BundlePhase::Failed),
                conditions: vec![
                    make_condition(
                        "WorkloadApplied",
                        BundleConditionStatus::True,
                        Some("InstallSucceeded".to_string()),
                        None,
                    ),
                    make_condition(
                        "Started",
                        BundleConditionStatus::False,
                        Some("StartFailed".to_string()),
                        Some(message),
                    ),
                ],
                workload: Some(BundleWorkloadRef {
                    name: workload_name.clone(),
                    namespace: Some(workload_namespace.clone()),
                    uid: None,
                }),
                last_reconciled_time: Some(now_timestamp()),
            };
            apply_status(
                registry,
                &resolved_namespace,
                &bundle_name,
                status,
                Some(expected_resource_version.as_str()),
            )
            .await?;
            return Ok(());
        }
    }

    let mut conditions = Vec::new();
    conditions.push(make_condition(
        "WorkloadApplied",
        BundleConditionStatus::True,
        Some("InstallSucceeded".to_string()),
        None,
    ));
    if bundle.spec.start {
        conditions.push(make_condition(
            "Started",
            BundleConditionStatus::True,
            Some("StartCompleted".to_string()),
            None,
        ));
    } else {
        conditions.push(make_condition(
            "Started",
            BundleConditionStatus::False,
            Some("StartSkipped".to_string()),
            Some("Bundle spec requested start=false; workload left stopped".to_string()),
        ));
    }

    let status = BundleStatus {
        observed_generation: Some(current_rv),
        phase: Some(BundlePhase::Ready),
        conditions,
        workload: Some(BundleWorkloadRef {
            name: workload_name.clone(),
            namespace: Some(workload_namespace.clone()),
            uid: None,
        }),
        last_reconciled_time: Some(now_timestamp()),
    };

    apply_status(
        registry,
        &resolved_namespace,
        &bundle_name,
        status,
        Some(expected_resource_version.as_str()),
    )
    .await?;

    log_info(
        COMPONENT,
        "Bundle reconciled",
        &[
            ("namespace", resolved_namespace.as_str()),
            ("name", bundle_name.as_str()),
            ("phase", "Ready"),
        ],
    );

    Ok(())
}

async fn publish_bundle_event(bus: Arc<InMemoryEventBus>, bundle: &Bundle, error: Option<&str>) {
    let resolved_namespace = bundle
        .spec
        .namespace
        .as_deref()
        .or(bundle.metadata.namespace.as_deref())
        .unwrap_or("default")
        .to_string();

    let bundle_name = bundle
        .metadata
        .name
        .as_deref()
        .unwrap_or(bundle.spec.service.as_str())
        .to_string();

    let phase = bundle
        .status
        .as_ref()
        .and_then(|status| status.phase.as_ref());
    let phase_label = phase.map(|p| format!("{p:?}"));
    let failure_condition = bundle.status.as_ref().and_then(|status| {
        status
            .conditions
            .iter()
            .find(|condition| condition.status != BundleConditionStatus::True)
            .cloned()
    });
    let failure_reason = failure_condition
        .as_ref()
        .and_then(|condition| condition.reason.clone());
    let failure_detail = failure_condition
        .as_ref()
        .and_then(|condition| condition.message.clone());
    let explicit_error = error.map(|value| value.to_string());
    let is_failure = explicit_error.is_some() || matches!(phase, Some(BundlePhase::Failed));

    let reason_text = if is_failure {
        failure_reason
            .clone()
            .unwrap_or_else(|| "BundleReconcileFailed".to_string())
    } else {
        "BundleReconciled".to_string()
    };

    let message_text = if is_failure {
        explicit_error
            .clone()
            .or_else(|| failure_detail.clone())
            .unwrap_or_else(|| format!("Bundle {} reconciliation failed", bundle_name))
    } else if let Some(label) = phase_label.clone() {
        format!("Bundle {} reconciled to phase {}", bundle_name, label)
    } else {
        format!("Bundle {} reconciled successfully", bundle_name)
    };

    let event_type = if is_failure {
        EventType::Custom("error")
    } else {
        EventType::Updated
    };

    let status_attr = if is_failure { "error" } else { "success" };

    let payload_namespace = resolved_namespace.clone();
    let payload_name = bundle_name.clone();
    let payload = if is_failure {
        json!({
            "status": "error",
            "namespace": payload_namespace,
            "name": payload_name,
            "error": message_text,
            "reason": reason_text,
        })
    } else {
        json!({
            "status": "success",
            "namespace": payload_namespace,
            "name": payload_name,
            "resourceVersion": bundle.metadata.resource_version,
            "phase": phase_label,
            "reason": reason_text,
        })
    };

    let payload_bytes = match serde_json::to_vec(&payload) {
        Ok(bytes) => bytes,
        Err(err) => {
            log_warn(
                COMPONENT,
                "Failed to encode bundle event payload",
                &[("error", err.to_string().as_str())],
            );
            Vec::new()
        }
    };

    let envelope = EventEnvelope::new(
        EventTopic::new("controller", "bundles.reconcile"),
        EventKey::new(resolved_namespace.clone(), bundle_name.clone()),
        event_type,
        payload_bytes,
        "application/json",
    )
    .with_attribute("component", COMPONENT.to_string())
    .with_attribute("namespace", resolved_namespace)
    .with_attribute("bundle", bundle_name)
    .with_attribute("status", status_attr.to_string());

    if let Err(err) = bus.publish(envelope).await {
        log_warn(
            COMPONENT,
            "Failed to publish bundle event",
            &[("error", err.to_string().as_str())],
        );
    }
}

async fn apply_status(
    registry: Arc<BundleRegistry>,
    namespace: &str,
    name: &str,
    status: BundleStatus,
    expected_rv: Option<&str>,
) -> Result<(), String> {
    let updated = registry
        .update_status(namespace, name, status, expected_rv)
        .await
        .map_err(|err| err.to_string())?;

    let phase_label = updated
        .status
        .as_ref()
        .and_then(|s| s.phase.as_ref())
        .map(|phase| format!("{phase:?}"))
        .unwrap_or_else(|| "None".to_string());
    let condition_count = updated
        .status
        .as_ref()
        .map(|s| s.conditions.len())
        .unwrap_or(0)
        .to_string();
    let workload_name = updated
        .status
        .as_ref()
        .and_then(|s| s.workload.as_ref())
        .map(|w| w.name.clone())
        .unwrap_or_else(|| "<none>".to_string());
    let observed_generation = updated
        .status
        .as_ref()
        .and_then(|s| s.observed_generation)
        .map(|g| g.to_string())
        .unwrap_or_else(|| "None".to_string());
    let resource_version = updated
        .metadata
        .resource_version
        .clone()
        .unwrap_or_else(|| "unknown".to_string());

    log_debug(
        COMPONENT,
        "persisted bundle status",
        &[
            ("namespace", namespace),
            ("bundle", name),
            ("phase", phase_label.as_str()),
            ("conditions", condition_count.as_str()),
            ("workload", workload_name.as_str()),
            ("observed_generation", observed_generation.as_str()),
            ("resource_version", resource_version.as_str()),
        ],
    );

    Ok(())
}

fn now_timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn make_condition(
    condition_type: &str,
    status: BundleConditionStatus,
    reason: Option<String>,
    message: Option<String>,
) -> BundleCondition {
    BundleCondition {
        condition_type: condition_type.to_string(),
        status,
        reason,
        message,
        last_transition_time: Some(now_timestamp()),
    }
}

fn resolve_snapshot_source(
    snapshot: Option<&BundleSnapshotSource>,
) -> Result<Option<String>, String> {
    match snapshot {
        Some(spec) => {
            let trimmed = spec.source.trim();
            if trimmed.is_empty() {
                return Err("snapshot source must not be empty".to_string());
            }
            if let Some(path) = trimmed.strip_prefix("file://") {
                if path.is_empty() {
                    return Err("snapshot file URI missing path".to_string());
                }
                return Ok(Some(path.to_string()));
            }
            if trimmed.contains("://") {
                return Err(format!("Unsupported snapshot scheme '{}'", trimmed));
            }
            Ok(Some(trimmed.to_string()))
        }
        None => Ok(None),
    }
}
