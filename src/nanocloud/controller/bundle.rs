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
    BindingHistoryEntry, Bundle, BundleCondition, BundleConditionKind, BundleConditionStatus,
    BundlePhase, BundleSnapshotSource, BundleStatus, BundleWorkloadRef,
};
use crate::nanocloud::controller::status::BundleConditionReason;
use crate::nanocloud::controller::watch::ControllerWatchManager;
use crate::nanocloud::engine::container;
use crate::nanocloud::engine::profile::is_reserved_profile_key;
use crate::nanocloud::events::in_memory::InMemoryEventBus;
use crate::nanocloud::events::{EventEnvelope, EventKey, EventPublisher, EventTopic, EventType};
use crate::nanocloud::k8s::bundle_manager::BundleRegistry;
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

async fn refresh_bundle_gauges(registry: &Arc<BundleRegistry>) {
    let mut ready = 0i64;
    let mut degraded = 0i64;
    for bundle in registry.list(None).await {
        let phase = bundle
            .status
            .as_ref()
            .and_then(|status| status.phase.as_ref());
        if matches!(phase, Some(BundlePhase::Ready)) {
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
    bundle: Bundle,
) -> Result<(), String> {
    let span_namespace = bundle
        .metadata
        .namespace
        .clone()
        .or_else(|| bundle.spec.namespace.clone())
        .unwrap_or_else(|| "default".to_string());
    let span_name = bundle
        .metadata
        .name
        .clone()
        .unwrap_or_else(|| bundle.spec.service.clone());
    let bundle_for_event = bundle.clone();
    let registry_for_span = Arc::clone(&registry);
    let result = tracing::with_span(
        "controller.bundle",
        format!("{}/{}", span_namespace, span_name),
        reconcile_bundle_inner(registry_for_span, bundle),
    )
    .await;
    publish_bundle_event(
        Arc::clone(&event_bus),
        &bundle_for_event,
        result.as_ref().err().map(|e| e.as_str()),
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
                    BundleConditionReason::ProfileFailed,
                    Some(message.clone()),
                ),
                ConditionSpec::new(
                    BundleConditionStatus::False,
                    BundleConditionReason::SecretsFailed,
                    Some("Secrets blocked by profile failure".to_string()),
                ),
                ConditionSpec::new(
                    BundleConditionStatus::False,
                    BundleConditionReason::WorkloadPending,
                    Some("Workloads never applied".to_string()),
                ),
                ConditionSpec::new(
                    BundleConditionStatus::False,
                    BundleConditionReason::StartPending,
                    Some("Bundle never started".to_string()),
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
            let conditions = build_conditions(
                &previous_conditions,
                ConditionSpec::new(
                    BundleConditionStatus::True,
                    BundleConditionReason::ProfilePersisted,
                    None,
                ),
                ConditionSpec::new(
                    BundleConditionStatus::True,
                    BundleConditionReason::SecretsDistributed,
                    None,
                ),
                ConditionSpec::new(
                    BundleConditionStatus::False,
                    BundleConditionReason::WorkloadFailed,
                    Some(message.clone()),
                ),
                ConditionSpec::new(
                    BundleConditionStatus::False,
                    BundleConditionReason::StartPending,
                    Some("Bundle failed before start completed".to_string()),
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
                    BundleConditionStatus::True,
                    BundleConditionReason::ProfilePersisted,
                    None,
                ),
                ConditionSpec::new(
                    BundleConditionStatus::True,
                    BundleConditionReason::SecretsDistributed,
                    None,
                ),
                ConditionSpec::new(
                    BundleConditionStatus::True,
                    BundleConditionReason::WorkloadApplied,
                    None,
                ),
                ConditionSpec::new(
                    BundleConditionStatus::False,
                    BundleConditionReason::StartFailed,
                    Some(message),
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

    let ready_condition = if bundle.spec.start {
        ConditionSpec::new(
            BundleConditionStatus::True,
            BundleConditionReason::StartCompleted,
            None,
        )
    } else {
        ConditionSpec::new(
            BundleConditionStatus::True,
            BundleConditionReason::StartSkipped,
            Some("Bundle spec requested start=false; workload left stopped".to_string()),
        )
    };
    let conditions = build_conditions(
        &previous_conditions,
        ConditionSpec::new(
            BundleConditionStatus::True,
            BundleConditionReason::ProfilePersisted,
            None,
        ),
        ConditionSpec::new(
            BundleConditionStatus::True,
            BundleConditionReason::SecretsDistributed,
            None,
        ),
        ConditionSpec::new(
            BundleConditionStatus::True,
            BundleConditionReason::WorkloadApplied,
            None,
        ),
        ready_condition,
    );

    let status = BundleStatus {
        observed_generation: Some(next_observed_generation),
        phase: Some(BundlePhase::Ready),
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
            *err_text = detail;
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
    profile: ConditionSpec,
    secrets: ConditionSpec,
    bound: ConditionSpec,
    ready: ConditionSpec,
) -> Vec<BundleCondition> {
    let mut conditions = Vec::new();
    let ConditionSpec {
        status,
        reason,
        message,
    } = profile;
    conditions.push(make_condition(
        previous.get(&BundleConditionKind::ProfilePrepared),
        BundleConditionKind::ProfilePrepared,
        status,
        reason,
        message,
    ));

    let ConditionSpec {
        status,
        reason,
        message,
    } = secrets;
    conditions.push(make_condition(
        previous.get(&BundleConditionKind::SecretsProvisioned),
        BundleConditionKind::SecretsProvisioned,
        status,
        reason,
        message,
    ));

    let ConditionSpec {
        status,
        reason,
        message,
    } = bound;
    conditions.push(make_condition(
        previous.get(&BundleConditionKind::Bound),
        BundleConditionKind::Bound,
        status,
        reason,
        message,
    ));

    let ConditionSpec {
        status,
        reason,
        message,
    } = ready;
    conditions.push(make_condition(
        previous.get(&BundleConditionKind::Ready),
        BundleConditionKind::Ready,
        status,
        reason,
        message,
    ));

    conditions
}

fn previous_binding_history(bundle: &Bundle) -> Vec<BindingHistoryEntry> {
    bundle
        .status
        .as_ref()
        .map(|status| status.binding_history.clone())
        .unwrap_or_default()
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
                BundleConditionReason::ProfileFailed,
                Some("profile failed".to_string()),
            ),
            ConditionSpec::new(
                BundleConditionStatus::False,
                BundleConditionReason::SecretsFailed,
                Some("secrets blocked".to_string()),
            ),
            ConditionSpec::new(
                BundleConditionStatus::False,
                BundleConditionReason::WorkloadPending,
                None,
            ),
            ConditionSpec::new(
                BundleConditionStatus::False,
                BundleConditionReason::StartPending,
                None,
            ),
        );
        assert_condition(
            &conditions,
            BundleConditionKind::ProfilePrepared,
            BundleConditionStatus::False,
            "ProfileFailed",
        );
        assert_condition(
            &conditions,
            BundleConditionKind::SecretsProvisioned,
            BundleConditionStatus::False,
            "SecretsFailed",
        );
        assert_condition(
            &conditions,
            BundleConditionKind::Bound,
            BundleConditionStatus::False,
            "WorkloadPending",
        );
        assert_condition(
            &conditions,
            BundleConditionKind::Ready,
            BundleConditionStatus::False,
            "StartPending",
        );
    }

    #[test]
    fn start_skipped_marks_ready_true() {
        let previous = HashMap::new();
        let conditions = build_conditions(
            &previous,
            ConditionSpec::new(
                BundleConditionStatus::True,
                BundleConditionReason::ProfilePersisted,
                None,
            ),
            ConditionSpec::new(
                BundleConditionStatus::True,
                BundleConditionReason::SecretsDistributed,
                None,
            ),
            ConditionSpec::new(
                BundleConditionStatus::True,
                BundleConditionReason::WorkloadApplied,
                None,
            ),
            ConditionSpec::new(
                BundleConditionStatus::True,
                BundleConditionReason::StartSkipped,
                Some("start disabled".to_string()),
            ),
        );
        assert_condition(
            &conditions,
            BundleConditionKind::Ready,
            BundleConditionStatus::True,
            "StartSkipped",
        );
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
            BundleConditionKind::Ready,
            BundleConditionStatus::True,
            "StartCompleted",
            Some(previous_ts),
        )]);
        let conditions = build_conditions(
            &previous,
            ConditionSpec::new(
                BundleConditionStatus::True,
                BundleConditionReason::ProfilePersisted,
                None,
            ),
            ConditionSpec::new(
                BundleConditionStatus::True,
                BundleConditionReason::SecretsDistributed,
                None,
            ),
            ConditionSpec::new(
                BundleConditionStatus::True,
                BundleConditionReason::WorkloadApplied,
                None,
            ),
            ConditionSpec::new(
                BundleConditionStatus::True,
                BundleConditionReason::StartCompleted,
                None,
            ),
        );
        let ready = conditions
            .iter()
            .find(|cond| cond.condition_type == BundleConditionKind::Ready)
            .expect("ready condition expected");
        assert_eq!(ready.last_transition_time.as_deref(), Some(previous_ts));
    }

    #[test]
    fn last_transition_updates_on_status_change() {
        let previous = condition_map(vec![sample_condition(
            BundleConditionKind::Ready,
            BundleConditionStatus::True,
            "StartCompleted",
            Some("2025-01-10T10:00:00Z"),
        )]);
        let conditions = build_conditions(
            &previous,
            ConditionSpec::new(
                BundleConditionStatus::True,
                BundleConditionReason::ProfilePersisted,
                None,
            ),
            ConditionSpec::new(
                BundleConditionStatus::True,
                BundleConditionReason::SecretsDistributed,
                None,
            ),
            ConditionSpec::new(
                BundleConditionStatus::True,
                BundleConditionReason::WorkloadApplied,
                None,
            ),
            ConditionSpec::new(
                BundleConditionStatus::False,
                BundleConditionReason::StartFailed,
                Some("boom".to_string()),
            ),
        );
        let ready = conditions
            .iter()
            .find(|cond| cond.condition_type == BundleConditionKind::Ready)
            .expect("ready condition expected");
        assert_ne!(
            ready.last_transition_time.as_deref(),
            Some("2025-01-10T10:00:00Z")
        );
        assert_eq!(
            ready.reason.as_deref(),
            Some(BundleConditionReason::StartFailed.as_str())
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
