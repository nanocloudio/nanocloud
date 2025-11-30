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
    BindingHistoryEntry, BindingHistoryStatus, Bundle, BundleCondition, BundleConditionKind,
    BundleConditionStatus, BundlePhase, BundleSnapshotSource, BundleStatus, BundleWorkloadRef,
};
use crate::nanocloud::controller::events::{EventRecorder, InvolvedObjectRef};
use crate::nanocloud::controller::runtime::{
    ControllerRuntime, ControllerTarget, ControllerWorkItem,
};
use crate::nanocloud::controller::status::BundleConditionReason;
use crate::nanocloud::controller::watch::{ControllerWatchEvent, ControllerWatchManager};
use crate::nanocloud::engine::container;
use crate::nanocloud::engine::profile::is_reserved_profile_key;
use crate::nanocloud::events::in_memory::InMemoryEventBus;
use crate::nanocloud::events::{EventEnvelope, EventKey, EventPublisher, EventTopic, EventType};
use crate::nanocloud::k8s::bundle_manager::{BundleRegistry, BUNDLE_FINALIZER};
use crate::nanocloud::k8s::pod::OwnerReference;
use crate::nanocloud::k8s::store::normalize_namespace;
use crate::nanocloud::logger::{log_debug, log_error, log_info, log_warn};
use crate::nanocloud::observability::{
    metrics::{self, ControllerReconcileResult},
    tracing,
};
use crate::nanocloud::security::{PRIVILEGE_ESCALATION_DENIED, SECURITY_POLICY_VIOLATION};
use crate::nanocloud::util::KeyspaceEventType;

use chrono::{SecondsFormat, Utc};
use serde_json::json;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use tokio::task::JoinHandle;

const COMPONENT: &str = "bundle-controller";
const BUNDLE_PREFIX: &str = "/bundles";

pub fn spawn() -> JoinHandle<()> {
    tokio::spawn(async move {
        let runtime = ControllerRuntime::shared();
        let registry = BundleRegistry::shared();
        let event_bus = InMemoryEventBus::global();
        let recorder = EventRecorder::new(COMPONENT);

        start_bundle_executor(
            &runtime,
            Arc::clone(&registry),
            Arc::clone(&event_bus),
            recorder.clone(),
        );
        bootstrap_existing_bundles(&runtime, &registry).await;
        watch_bundle_events(runtime, registry, event_bus, recorder).await;
    })
}

fn start_bundle_executor(
    runtime: &Arc<ControllerRuntime>,
    registry: Arc<BundleRegistry>,
    event_bus: Arc<InMemoryEventBus>,
    recorder: EventRecorder,
) {
    if let Err(err) = runtime.spawn_executor(move |item| {
        let registry = Arc::clone(&registry);
        let event_bus = Arc::clone(&event_bus);
        let recorder = recorder.clone();
        async move {
            if let ControllerTarget::Bundle { namespace, name } = &item.target {
                if let Err(err) = reconcile_bundle(
                    Arc::clone(&registry),
                    Arc::clone(&event_bus),
                    recorder.clone(),
                    namespace.clone(),
                    name.clone(),
                )
                .await
                {
                    log_error(
                        COMPONENT,
                        "Bundle reconciliation failed",
                        &[
                            ("namespace", namespace.as_deref().unwrap_or("default")),
                            ("bundle", name.as_str()),
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
            "Failed to start bundle dispatcher",
            &[("error", err.to_string().as_str())],
        );
    }
}

async fn bootstrap_existing_bundles(
    runtime: &Arc<ControllerRuntime>,
    registry: &Arc<BundleRegistry>,
) {
    let existing = registry.list(None).await;
    if !existing.is_empty() {
        log_info(
            COMPONENT,
            "Reconciling existing Bundles on startup",
            &[("count", existing.len().to_string().as_str())],
        );
    }
    for bundle in existing {
        let namespace = bundle.metadata.namespace.clone();
        let name = bundle
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| bundle.spec.service.clone());
        enqueue_bundle(runtime, namespace, name).await;
    }
}

async fn watch_bundle_events(
    runtime: Arc<ControllerRuntime>,
    _registry: Arc<BundleRegistry>,
    _event_bus: Arc<InMemoryEventBus>,
    recorder: EventRecorder,
) {
    let manager = ControllerWatchManager::shared();
    let mut subscription = manager.subscribe(BUNDLE_PREFIX, None);
    while let Some(event) = subscription.recv().await {
        match event.event_type {
            KeyspaceEventType::Deleted => {
                if let Some((ns, name)) = parse_bundle_key(event.key.as_str()) {
                    let ns_label = ns.clone().unwrap_or_else(|| "default".to_string());
                    log_debug(
                        COMPONENT,
                        "Bundle deleted",
                        &[("namespace", ns_label.as_str()), ("bundle", name.as_str())],
                    );
                    // Finalizer cleanup handled in reconcile.
                    enqueue_bundle(&runtime, ns, name).await;
                }
            }
            KeyspaceEventType::Added | KeyspaceEventType::Modified => {
                if let Some((namespace, name)) = bundle_identity(&event) {
                    enqueue_bundle(&runtime, namespace, name).await;
                } else {
                    log_warn(
                        COMPONENT,
                        "Bundle event missing identity",
                        &[("key", event.key.as_str())],
                    );
                }
            }
        }
    }

    // Ensure recorder stays alive for executor.
    drop(recorder);
}

fn bundle_identity(event: &ControllerWatchEvent) -> Option<(Option<String>, String)> {
    if let Some(value) = event.value.as_ref() {
        if let Ok(bundle) = serde_json::from_str::<Bundle>(value) {
            let name = bundle
                .metadata
                .name
                .clone()
                .unwrap_or_else(|| bundle.spec.service.clone());
            return Some((bundle.metadata.namespace.clone(), name));
        }
    }
    parse_bundle_key(event.key.as_str())
}

fn parse_bundle_key(key: &str) -> Option<(Option<String>, String)> {
    let parts: Vec<&str> = key
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    if parts.len() != 3 || parts[0] != BUNDLE_PREFIX.trim_start_matches('/') {
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

async fn enqueue_bundle(runtime: &Arc<ControllerRuntime>, namespace: Option<String>, name: String) {
    let item = ControllerWorkItem::bundle(namespace.as_deref(), name.as_str());
    match runtime.work_queue().enqueue(item).await {
        Ok(true) => {}
        Ok(false) => {
            log_debug(
                COMPONENT,
                "Coalesced bundle reconciliation request",
                &[
                    ("namespace", namespace.as_deref().unwrap_or("default")),
                    ("bundle", name.as_str()),
                ],
            );
        }
        Err(err) => {
            log_warn(
                COMPONENT,
                "Failed to enqueue bundle reconciliation",
                &[
                    ("namespace", namespace.as_deref().unwrap_or("default")),
                    ("bundle", name.as_str()),
                    ("error", err.to_string().as_str()),
                ],
            );
        }
    }
}

async fn refresh_bundle_gauges(registry: &Arc<BundleRegistry>) {
    let mut ready = 0i64;
    let mut degraded = 0i64;
    for bundle in registry.list(None).await {
        let phase = bundle
            .status
            .as_ref()
            .and_then(|status| status.phase.as_ref());
        if matches!(phase, Some(BundlePhase::Running)) {
            ready += 1;
        } else {
            degraded += 1;
        }
    }
    metrics::set_bundle_gauges(ready, degraded);
}

async fn reconcile_bundle(
    registry: Arc<BundleRegistry>,
    event_bus: Arc<InMemoryEventBus>,
    recorder: EventRecorder,
    namespace: Option<String>,
    name: String,
) -> Result<(), String> {
    let namespace_label = normalize_namespace(namespace.as_deref());
    let Some(bundle) = registry.get(&namespace_label, &name).await else {
        log_debug(
            COMPONENT,
            "Bundle missing during reconciliation",
            &[
                ("namespace", namespace_label.as_str()),
                ("bundle", name.as_str()),
            ],
        );
        return Ok(());
    };
    let bundle_for_event = bundle.clone();
    let span_name = bundle
        .metadata
        .name
        .clone()
        .unwrap_or_else(|| bundle.spec.service.clone());
    let registry_for_span = Arc::clone(&registry);
    let result = tracing::with_span(
        "controller.bundle",
        format!("{}/{}", namespace_label, span_name),
        reconcile_bundle_inner(registry_for_span, bundle),
    )
    .await;
    publish_bundle_event(
        Arc::clone(&event_bus),
        &bundle_for_event,
        result.as_ref().err().map(|e| e.as_str()),
        &recorder,
    )
    .await;
    let controller_result = if result.is_ok() {
        ControllerReconcileResult::Success
    } else {
        ControllerReconcileResult::Error
    };
    metrics::record_controller_reconcile("bundle", controller_result);
    refresh_bundle_gauges(&registry).await;
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

    let mut next_observed_generation = current_rv.saturating_add(1);
    let previous_conditions: HashMap<BundleConditionKind, BundleCondition> = bundle
        .status
        .as_ref()
        .map(|status| {
            status
                .conditions
                .iter()
                .map(|condition| (condition.condition_type, condition.clone()))
                .collect()
        })
        .unwrap_or_default();
    let mut latest_binding_history = previous_binding_history(&bundle);

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

    if bundle.metadata.deletion_timestamp.is_some() {
        return handle_bundle_deletion(
            Arc::clone(&registry),
            bundle,
            &previous_conditions,
            next_observed_generation,
            &resolved_namespace,
            &bundle_name,
            namespace_ref,
            expected_resource_version.as_str(),
            &latest_binding_history,
        )
        .await;
    }

    log_info(
        COMPONENT,
        "Reconciling bundle",
        &[
            ("namespace", resolved_namespace.as_str()),
            ("name", bundle_name.as_str()),
        ],
    );

    let install_options = bundle
        .spec
        .options
        .iter()
        .filter(|(key, _)| !is_reserved_profile_key(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();

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
            let conditions = build_conditions(
                &previous_conditions,
                ConditionSpec::new(
                    BundleConditionStatus::False,
                    BundleConditionReason::InstallFailed,
                    Some(message.clone()),
                ),
                ConditionSpec::new(
                    BundleConditionStatus::Unknown,
                    BundleConditionReason::BindingsPending,
                    Some("Bindings blocked until install completes".to_string()),
                ),
                ConditionSpec::new(
                    BundleConditionStatus::False,
                    BundleConditionReason::BackupFailed,
                    Some(message.clone()),
                ),
            );
            let status = BundleStatus {
                observed_generation: Some(next_observed_generation),
                phase: Some(BundlePhase::Failed),
                conditions,
                workload: None,
                last_reconciled_time: Some(now_timestamp()),
                binding_history: latest_binding_history.clone(),
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
        bundle.spec.security.clone(),
        bundle.spec.runtime.clone(),
        bundle_owner_reference(&bundle),
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
            let bindings_condition = bindings_condition_from_history(&latest_binding_history);
            let conditions = build_conditions(
                &previous_conditions,
                ConditionSpec::new(
                    BundleConditionStatus::False,
                    BundleConditionReason::InstallFailed,
                    Some(message.clone()),
                ),
                bindings_condition,
                ConditionSpec::new(
                    BundleConditionStatus::Unknown,
                    BundleConditionReason::BackupPending,
                    None,
                ),
            );
            let status = BundleStatus {
                observed_generation: Some(next_observed_generation),
                phase: Some(BundlePhase::Failed),
                conditions,
                workload: None,
                last_reconciled_time: Some(now_timestamp()),
                binding_history: latest_binding_history.clone(),
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
    let container::InstallResult { pod, profile } = install_result;
    let binding_history_snapshot = profile.binding_history_entries();
    latest_binding_history = binding_history_snapshot.clone();
    let bindings_condition = bindings_condition_from_history(&latest_binding_history);
    let (profile_key, persisted_options_raw) = profile
        .to_serialized_fields()
        .map_err(|err| err.to_string())?;
    let persisted_options: HashMap<String, String> = persisted_options_raw
        .into_iter()
        .filter(|(key, _)| !is_reserved_profile_key(key))
        .collect();

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
            next_observed_generation = current_rv.saturating_add(1);
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
            let conditions = build_conditions(
                &previous_conditions,
                ConditionSpec::new(
                    BundleConditionStatus::False,
                    BundleConditionReason::InstallFailed,
                    Some(message.clone()),
                ),
                bindings_condition.clone(),
                ConditionSpec::new(
                    BundleConditionStatus::Unknown,
                    BundleConditionReason::BackupPending,
                    None,
                ),
            );
            let status = BundleStatus {
                observed_generation: Some(next_observed_generation),
                phase: Some(BundlePhase::Failed),
                conditions,
                workload: Some(BundleWorkloadRef {
                    name: workload_name.clone(),
                    namespace: Some(workload_namespace.clone()),
                    uid: None,
                }),
                last_reconciled_time: Some(now_timestamp()),
                binding_history: latest_binding_history.clone(),
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

    let install_condition = if bundle.spec.start {
        ConditionSpec::new(
            BundleConditionStatus::True,
            BundleConditionReason::InstallReady,
            None,
        )
    } else {
        ConditionSpec::new(
            BundleConditionStatus::True,
            BundleConditionReason::InstallReady,
            Some("Bundle spec requested start=false; workload left stopped".to_string()),
        )
    };
    let backup_condition = ConditionSpec::new(
        BundleConditionStatus::True,
        BundleConditionReason::BackupHealthy,
        None,
    );
    let conditions = build_conditions(
        &previous_conditions,
        install_condition,
        bindings_condition.clone(),
        backup_condition.clone(),
    );
    let phase_value = if matches!(bindings_condition.status, BundleConditionStatus::False)
        || matches!(backup_condition.status, BundleConditionStatus::False)
    {
        BundlePhase::Failed
    } else if bundle.spec.update {
        BundlePhase::Updating
    } else {
        BundlePhase::Running
    };

    let status = BundleStatus {
        observed_generation: Some(next_observed_generation),
        phase: Some(phase_value.clone()),
        conditions,
        workload: Some(BundleWorkloadRef {
            name: workload_name.clone(),
            namespace: Some(workload_namespace.clone()),
            uid: None,
        }),
        last_reconciled_time: Some(now_timestamp()),
        binding_history: latest_binding_history.clone(),
    };

    apply_status(
        registry,
        &resolved_namespace,
        &bundle_name,
        status,
        Some(expected_resource_version.as_str()),
    )
    .await?;

    let phase_label = format!("{:?}", phase_value);
    log_info(
        COMPONENT,
        "Bundle reconciled",
        &[
            ("namespace", resolved_namespace.as_str()),
            ("name", bundle_name.as_str()),
            ("phase", phase_label.as_str()),
        ],
    );

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_bundle_deletion(
    registry: Arc<BundleRegistry>,
    bundle: Bundle,
    previous_conditions: &HashMap<BundleConditionKind, BundleCondition>,
    next_observed_generation: i64,
    namespace: &str,
    bundle_name: &str,
    namespace_ref: Option<&str>,
    expected_resource_version: &str,
    history: &[BindingHistoryEntry],
) -> Result<(), String> {
    let install_condition = ConditionSpec::new(
        BundleConditionStatus::False,
        BundleConditionReason::Uninstalling,
        Some("Waiting for workload cleanup".to_string()),
    );
    let conditions = build_conditions(
        previous_conditions,
        install_condition,
        bindings_condition_from_history(history),
        ConditionSpec::new(
            BundleConditionStatus::Unknown,
            BundleConditionReason::BackupPending,
            None,
        ),
    );

    let status = BundleStatus {
        observed_generation: Some(next_observed_generation),
        phase: Some(BundlePhase::Uninstalling),
        conditions,
        workload: None,
        last_reconciled_time: Some(now_timestamp()),
        binding_history: history.to_vec(),
    };

    apply_status(
        Arc::clone(&registry),
        namespace,
        bundle_name,
        status,
        Some(expected_resource_version),
    )
    .await?;

    if bundle
        .metadata
        .finalizers
        .iter()
        .any(|finalizer| finalizer == BUNDLE_FINALIZER)
    {
        let plan = container::BackupPlan {
            owner: bundle_name.to_string(),
            retention: 1,
        };
        container::uninstall(namespace_ref, &bundle.spec.service, plan)
            .await
            .map_err(|err| err.to_string())?;
    }

    registry
        .finalize_delete(namespace, bundle_name)
        .await
        .map_err(|err| err.to_string())
}

fn bundle_owner_reference(bundle: &Bundle) -> Option<OwnerReference> {
    let uid = bundle.metadata.uid.as_ref()?.clone();
    let name = bundle.metadata.name.clone()?;
    Some(OwnerReference {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "Bundle".to_string(),
        name,
        uid,
        controller: Some(true),
        block_owner_deletion: Some(true),
    })
}

async fn publish_bundle_event(
    bus: Arc<InMemoryEventBus>,
    bundle: &Bundle,
    error: Option<&str>,
    recorder: &EventRecorder,
) {
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
    let mut failure_reason = failure_condition
        .as_ref()
        .and_then(|condition| condition.reason.clone());
    let mut failure_detail = failure_condition
        .as_ref()
        .and_then(|condition| condition.message.clone());

    if let Some(detail) = failure_detail.as_mut() {
        if let Some((reason, normalized)) = normalize_security_error(detail) {
            failure_reason = Some(reason.to_string());
            *detail = with_security_hint(&normalized);
        }
    }
    let mut explicit_error = error.map(|value| value.to_string());
    let is_failure = explicit_error.is_some() || matches!(phase, Some(BundlePhase::Failed));

    let mut reason_text = if is_failure {
        failure_reason
            .clone()
            .unwrap_or_else(|| "BundleReconcileFailed".to_string())
    } else {
        "BundleReconciled".to_string()
    };

    if let Some(err_text) = explicit_error.as_mut() {
        if let Some((reason, detail)) = normalize_security_error(err_text) {
            reason_text = reason.to_string();
            *err_text = with_security_hint(&detail);
        }
    }

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
    .with_attribute("namespace", resolved_namespace.clone())
    .with_attribute("bundle", bundle_name.clone())
    .with_attribute("status", status_attr.to_string());

    if let Err(err) = bus.publish(envelope).await {
        log_warn(
            COMPONENT,
            "Failed to publish bundle event",
            &[("error", err.to_string().as_str())],
        );
    }

    let involved = InvolvedObjectRef {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "Bundle".to_string(),
        name: bundle_name.clone(),
        uid: bundle.metadata.uid.clone(),
        namespace: Some(resolved_namespace.clone()),
    };
    let kube_event_type = if is_failure { "Warning" } else { "Normal" };
    recorder
        .record(
            Some(resolved_namespace.as_str()),
            &involved,
            reason_text.as_str(),
            kube_event_type,
            message_text.as_str(),
        )
        .await;
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

const SECURITY_EVENT_HINT: &str =
    "Defaults drop all caps; set spec.security.allowPrivileged/extraCapabilities to opt in.";

fn with_security_hint(detail: &str) -> String {
    format!("{} Hint: {}", detail, SECURITY_EVENT_HINT)
}

fn normalize_security_error(message: &str) -> Option<(&'static str, String)> {
    if let Some(detail) = message.strip_prefix(SECURITY_POLICY_VIOLATION) {
        return Some(("SecurityPolicyViolation", detail.trim().to_string()));
    }
    if let Some(detail) = message.strip_prefix(PRIVILEGE_ESCALATION_DENIED) {
        return Some(("PrivilegeEscalationDenied", detail.trim().to_string()));
    }
    None
}

fn now_timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn make_condition(
    previous: Option<&BundleCondition>,
    condition_type: BundleConditionKind,
    status: BundleConditionStatus,
    reason: BundleConditionReason,
    message: Option<String>,
) -> BundleCondition {
    let last_transition_time = match previous {
        Some(existing) if existing.status == status => existing
            .last_transition_time
            .clone()
            .or_else(|| Some(now_timestamp())),
        _ => Some(now_timestamp()),
    };

    BundleCondition {
        condition_type,
        status,
        reason: Some(reason.as_str().to_string()),
        message,
        last_transition_time,
    }
}

#[derive(Clone)]
struct ConditionSpec {
    status: BundleConditionStatus,
    reason: BundleConditionReason,
    message: Option<String>,
}

impl ConditionSpec {
    fn new(
        status: BundleConditionStatus,
        reason: BundleConditionReason,
        message: Option<String>,
    ) -> Self {
        ConditionSpec {
            status,
            reason,
            message,
        }
    }
}

fn build_conditions(
    previous: &HashMap<BundleConditionKind, BundleCondition>,
    install: ConditionSpec,
    bindings: ConditionSpec,
    backup: ConditionSpec,
) -> Vec<BundleCondition> {
    let ConditionSpec {
        status,
        reason,
        message,
    } = install;
    let install_condition = make_condition(
        previous.get(&BundleConditionKind::InstallReady),
        BundleConditionKind::InstallReady,
        status,
        reason,
        message,
    );

    let ConditionSpec {
        status,
        reason,
        message,
    } = bindings;
    let bindings_condition = make_condition(
        previous.get(&BundleConditionKind::BindingsReady),
        BundleConditionKind::BindingsReady,
        status,
        reason,
        message,
    );

    let ConditionSpec {
        status,
        reason,
        message,
    } = backup;
    let backup_condition = make_condition(
        previous.get(&BundleConditionKind::BackupHealthy),
        BundleConditionKind::BackupHealthy,
        status,
        reason,
        message,
    );

    vec![install_condition, bindings_condition, backup_condition]
}

fn previous_binding_history(bundle: &Bundle) -> Vec<BindingHistoryEntry> {
    bundle
        .status
        .as_ref()
        .map(|status| status.binding_history.clone())
        .unwrap_or_default()
}

fn bindings_condition_from_history(history: &[BindingHistoryEntry]) -> ConditionSpec {
    if let Some(entry) = history.iter().find(|entry| {
        matches!(
            entry.status,
            BindingHistoryStatus::Failed | BindingHistoryStatus::TimedOut
        )
    }) {
        let message = entry
            .message
            .clone()
            .or_else(|| Some(format!("Binding {} failed", entry.binding_id.clone())));
        return ConditionSpec::new(
            BundleConditionStatus::False,
            BundleConditionReason::BindingsFailed,
            message,
        );
    }

    if history.is_empty() {
        ConditionSpec::new(
            BundleConditionStatus::Unknown,
            BundleConditionReason::BindingsPending,
            None,
        )
    } else {
        ConditionSpec::new(
            BundleConditionStatus::True,
            BundleConditionReason::BindingsReady,
            None,
        )
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

#[cfg(test)]
mod tests {
    use super::*;

    fn condition_map(pairs: Vec<BundleCondition>) -> HashMap<BundleConditionKind, BundleCondition> {
        pairs
            .into_iter()
            .map(|condition| (condition.condition_type, condition))
            .collect()
    }

    fn sample_condition(
        kind: BundleConditionKind,
        status: BundleConditionStatus,
        reason: &str,
        last_transition: Option<&str>,
    ) -> BundleCondition {
        BundleCondition {
            condition_type: kind,
            status,
            reason: Some(reason.to_string()),
            message: None,
            last_transition_time: last_transition.map(|value| value.to_string()),
        }
    }

    #[test]
    fn failure_conditions_mark_all_dependencies_false() {
        let previous = HashMap::new();
        let conditions = build_conditions(
            &previous,
            ConditionSpec::new(
                BundleConditionStatus::False,
                BundleConditionReason::InstallFailed,
                Some("install failed".to_string()),
            ),
            ConditionSpec::new(
                BundleConditionStatus::False,
                BundleConditionReason::BindingsFailed,
                Some("bindings blocked".to_string()),
            ),
            ConditionSpec::new(
                BundleConditionStatus::False,
                BundleConditionReason::BackupFailed,
                Some("backup missing".to_string()),
            ),
        );
        assert_condition(
            &conditions,
            BundleConditionKind::InstallReady,
            BundleConditionStatus::False,
            "InstallFailed",
        );
        assert_condition(
            &conditions,
            BundleConditionKind::BindingsReady,
            BundleConditionStatus::False,
            "BindingsFailed",
        );
        assert_condition(
            &conditions,
            BundleConditionKind::BackupHealthy,
            BundleConditionStatus::False,
            "BackupFailed",
        );
    }

    #[test]
    fn install_ready_carries_message() {
        let previous = HashMap::new();
        let conditions = build_conditions(
            &previous,
            ConditionSpec::new(
                BundleConditionStatus::True,
                BundleConditionReason::InstallReady,
                Some("start disabled".to_string()),
            ),
            ConditionSpec::new(
                BundleConditionStatus::Unknown,
                BundleConditionReason::BindingsPending,
                None,
            ),
            ConditionSpec::new(
                BundleConditionStatus::Unknown,
                BundleConditionReason::BackupPending,
                None,
            ),
        );
        assert_condition(
            &conditions,
            BundleConditionKind::InstallReady,
            BundleConditionStatus::True,
            "InstallReady",
        );
        let install = conditions
            .iter()
            .find(|cond| cond.condition_type == BundleConditionKind::InstallReady)
            .expect("install condition expected");
        assert_eq!(install.message.as_deref(), Some("start disabled"));
    }

    #[test]
    fn normalize_security_error_strips_prefixes() {
        let policy = normalize_security_error(
            "[SecurityPolicyViolation] Capability 'CAP_SYS_ADMIN' is not allowed",
        );
        assert_eq!(
            policy,
            Some((
                "SecurityPolicyViolation",
                "Capability 'CAP_SYS_ADMIN' is not allowed".to_string()
            ))
        );

        let privilege = normalize_security_error(
            "[PrivilegeEscalationDenied] Failed to configure capabilities",
        );
        assert_eq!(
            privilege,
            Some((
                "PrivilegeEscalationDenied",
                "Failed to configure capabilities".to_string()
            ))
        );

        assert_eq!(normalize_security_error("other error"), None);
    }

    #[test]
    fn last_transition_preserved_when_status_stable() {
        let previous_ts = "2025-01-10T10:00:00Z";
        let previous = condition_map(vec![sample_condition(
            BundleConditionKind::InstallReady,
            BundleConditionStatus::True,
            "InstallReady",
            Some(previous_ts),
        )]);
        let conditions = build_conditions(
            &previous,
            ConditionSpec::new(
                BundleConditionStatus::True,
                BundleConditionReason::InstallReady,
                None,
            ),
            ConditionSpec::new(
                BundleConditionStatus::Unknown,
                BundleConditionReason::BindingsPending,
                None,
            ),
            ConditionSpec::new(
                BundleConditionStatus::Unknown,
                BundleConditionReason::BackupPending,
                None,
            ),
        );
        let ready = conditions
            .iter()
            .find(|cond| cond.condition_type == BundleConditionKind::InstallReady)
            .expect("install condition expected");
        assert_eq!(ready.last_transition_time.as_deref(), Some(previous_ts));
    }

    #[test]
    fn last_transition_updates_on_status_change() {
        let previous = condition_map(vec![sample_condition(
            BundleConditionKind::InstallReady,
            BundleConditionStatus::True,
            "InstallReady",
            Some("2025-01-10T10:00:00Z"),
        )]);
        let conditions = build_conditions(
            &previous,
            ConditionSpec::new(
                BundleConditionStatus::Unknown,
                BundleConditionReason::InstallReady,
                None,
            ),
            ConditionSpec::new(
                BundleConditionStatus::Unknown,
                BundleConditionReason::BindingsPending,
                None,
            ),
            ConditionSpec::new(
                BundleConditionStatus::False,
                BundleConditionReason::BackupFailed,
                Some("boom".to_string()),
            ),
        );
        let ready = conditions
            .iter()
            .find(|cond| cond.condition_type == BundleConditionKind::InstallReady)
            .expect("install condition expected");
        assert_ne!(
            ready.last_transition_time.as_deref(),
            Some("2025-01-10T10:00:00Z")
        );
        assert_eq!(
            ready.reason.as_deref(),
            Some(BundleConditionReason::InstallReady.as_str())
        );
    }

    fn assert_condition(
        conditions: &[BundleCondition],
        kind: BundleConditionKind,
        expected_status: BundleConditionStatus,
        expected_reason: &str,
    ) {
        let condition = conditions
            .iter()
            .find(|cond| cond.condition_type == kind)
            .unwrap_or_else(|| panic!("condition {:?} missing", kind));
        assert_eq!(condition.status, expected_status);
        assert_eq!(condition.reason.as_deref(), Some(expected_reason));
    }
}
