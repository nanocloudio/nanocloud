use crate::nanocloud::api::types::{VolumeSnapshot, VolumeSnapshotPhase, VolumeSnapshotStatus};
use crate::nanocloud::controller::events::{EventRecorder, InvolvedObjectRef};
use crate::nanocloud::controller::runtime::{
    ControllerRuntime, ControllerTarget, ControllerWorkItem, HandlerResult,
};
use crate::nanocloud::controller::watch::{ControllerWatchEvent, ControllerWatchManager};
use crate::nanocloud::engine::container::backup_directory;
use crate::nanocloud::engine::{register_streaming_backup, streaming_backup_enabled, Snapshot};
use crate::nanocloud::k8s::bundle_manager::BundleRegistry;
use crate::nanocloud::k8s::pod::OwnerReference;
use crate::nanocloud::k8s::store::{
    list_volume_snapshots, normalize_namespace, save_volume_snapshot,
};
use crate::nanocloud::logger::{log_debug, log_error, log_info, log_warn};
use crate::nanocloud::observability::{
    metrics::{self, ControllerReconcileResult, SnapshotOperation},
    tracing,
};
use crate::nanocloud::util::KeyspaceEventType;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use chrono::{SecondsFormat, Utc};
use serde_json;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::sync::Arc;
use tokio::task::JoinHandle;

const COMPONENT: &str = "snapshot-controller";
const SNAPSHOT_PREFIX: &str = "/volumesnapshots";

pub fn spawn() -> JoinHandle<()> {
    tokio::spawn(async move {
        let runtime = ControllerRuntime::shared();
        let recorder = EventRecorder::new(COMPONENT);

        start_snapshot_executor(&runtime, recorder.clone());
        bootstrap_snapshots(&runtime).await;
        watch_snapshot_events(runtime, recorder).await;
    })
}

fn start_snapshot_executor(runtime: &Arc<ControllerRuntime>, recorder: EventRecorder) {
    let recorder = recorder.clone();
    if let Err(err) = runtime.spawn_executor(move |item| {
        let recorder = recorder.clone();
        async move {
            if let ControllerTarget::VolumeSnapshot { namespace, name } = &item.target {
                let namespace_label = namespace.clone();
                if let Some(snapshot) = load_snapshot(namespace_label.as_deref(), name) {
                    return process_snapshot(snapshot, &recorder).await;
                }

                log_warn(
                    COMPONENT,
                    "VolumeSnapshot missing during reconciliation",
                    &[
                        ("namespace", namespace.as_deref().unwrap_or("default")),
                        ("snapshot", name.as_str()),
                    ],
                );
            }

            Ok(())
        }
    }) {
        log_error(
            COMPONENT,
            "Failed to start snapshot controller dispatcher",
            &[("error", err.to_string().as_str())],
        );
    }
}

async fn bootstrap_snapshots(runtime: &Arc<ControllerRuntime>) {
    match list_volume_snapshots(None) {
        Ok(existing) => {
            if !existing.is_empty() {
                let count_text = existing.len().to_string();
                log_info(
                    COMPONENT,
                    "Reconciling existing VolumeSnapshots on startup",
                    &[("count", count_text.as_str())],
                );
            }
            for snapshot in existing {
                let namespace = snapshot.metadata.namespace.clone();
                let name = snapshot
                    .metadata
                    .name
                    .clone()
                    .unwrap_or_else(|| "<unnamed>".to_string());
                enqueue_snapshot(runtime, namespace, name).await;
            }
        }
        Err(err) => {
            log_error(
                COMPONENT,
                "Failed to list existing VolumeSnapshots",
                &[("error", err.to_string().as_str())],
            );
        }
    }
}

async fn watch_snapshot_events(runtime: Arc<ControllerRuntime>, recorder: EventRecorder) {
    let manager = ControllerWatchManager::shared();
    let mut subscription = manager.subscribe(SNAPSHOT_PREFIX, None);
    while let Some(event) = subscription.recv().await {
        match event.event_type {
            KeyspaceEventType::Deleted => {
                if let Some((namespace, name)) = parse_snapshot_key(event.key.as_str()) {
                    log_debug(
                        COMPONENT,
                        "VolumeSnapshot deleted",
                        &[
                            ("namespace", namespace.as_deref().unwrap_or("default")),
                            ("snapshot", name.as_str()),
                        ],
                    );
                    enqueue_snapshot(&runtime, namespace, name).await;
                }
            }
            KeyspaceEventType::Added | KeyspaceEventType::Modified => {
                if let Some((namespace, name)) = snapshot_identity(&event) {
                    enqueue_snapshot(&runtime, namespace, name).await;
                } else {
                    log_warn(
                        COMPONENT,
                        "VolumeSnapshot event missing payload",
                        &[("key", event.key.as_str())],
                    );
                }
            }
        }
    }

    drop(recorder);
}

fn snapshot_identity(event: &ControllerWatchEvent) -> Option<(Option<String>, String)> {
    if let Some(value) = event.value.as_ref() {
        if let Ok(snapshot) = serde_json::from_str::<VolumeSnapshot>(value) {
            let name = snapshot
                .metadata
                .name
                .clone()
                .unwrap_or_else(|| "<unnamed>".to_string());
            return Some((snapshot.metadata.namespace.clone(), name));
        }
    }
    parse_snapshot_key(event.key.as_str())
}

fn parse_snapshot_key(key: &str) -> Option<(Option<String>, String)> {
    let parts: Vec<&str> = key
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    if parts.len() != 3 || parts[0] != SNAPSHOT_PREFIX.trim_start_matches('/') {
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

async fn enqueue_snapshot(
    runtime: &Arc<ControllerRuntime>,
    namespace: Option<String>,
    name: String,
) {
    let item = ControllerWorkItem::volume_snapshot(namespace.as_deref(), name.as_str());
    match runtime.work_queue().enqueue(item).await {
        Ok(true) => {}
        Ok(false) => {
            log_debug(
                COMPONENT,
                "Coalesced VolumeSnapshot reconciliation request",
                &[
                    ("namespace", namespace.as_deref().unwrap_or("default")),
                    ("snapshot", name.as_str()),
                ],
            );
        }
        Err(err) => {
            log_warn(
                COMPONENT,
                "Failed to enqueue VolumeSnapshot reconciliation",
                &[
                    ("namespace", namespace.as_deref().unwrap_or("default")),
                    ("snapshot", name.as_str()),
                    ("error", err.to_string().as_str()),
                ],
            );
        }
    }
}

fn load_snapshot(namespace: Option<&str>, name: &str) -> Option<VolumeSnapshot> {
    match list_volume_snapshots(namespace) {
        Ok(snapshots) => snapshots
            .into_iter()
            .find(|snapshot| snapshot.metadata.name.as_deref() == Some(name)),
        Err(err) => {
            log_error(
                COMPONENT,
                "Failed to load VolumeSnapshots",
                &[
                    ("namespace", namespace.unwrap_or("default")),
                    ("snapshot", name),
                    ("error", err.to_string().as_str()),
                ],
            );
            None
        }
    }
}

async fn snapshot_owner_reference(
    namespace: Option<&str>,
    service: &str,
) -> Option<OwnerReference> {
    let registry = BundleRegistry::shared();
    let namespace_value = normalize_namespace(namespace);
    let bundle = registry.get(&namespace_value, service).await?;
    let uid = bundle.metadata.uid.clone()?;
    let name = bundle.metadata.name.clone()?;

    Some(OwnerReference {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "Bundle".to_string(),
        name,
        uid,
        controller: Some(false),
        block_owner_deletion: Some(false),
    })
}

async fn process_snapshot(snapshot: VolumeSnapshot, recorder: &EventRecorder) -> HandlerResult {
    let namespace = snapshot.metadata.namespace.clone();
    let name = snapshot
        .metadata
        .name
        .clone()
        .unwrap_or_else(|| "<unnamed>".to_string());
    let namespace_label = namespace.as_deref();
    let namespace_value = namespace_label.unwrap_or("default");
    let mut snapshot_for_failure = snapshot.clone();

    let reconcile_future = metrics::observe_snapshot_operation(
        namespace_label,
        name.as_str(),
        SnapshotOperation::Reconcile,
        async move { reconcile_snapshot(snapshot).await },
    );
    let span_label = format!("{}/{}", namespace_value, name);
    let result = tracing::with_span("controller.snapshot", span_label, reconcile_future).await;

    if let Err(err) = &result {
        if let Err(status_err) = mark_snapshot_failed(
            &mut snapshot_for_failure,
            namespace_label,
            name.as_str(),
            err,
        ) {
            log_error(
                COMPONENT,
                "Failed to update VolumeSnapshot failure status",
                &[
                    ("namespace", namespace_value),
                    ("snapshot", name.as_str()),
                    ("error", status_err.to_string().as_str()),
                ],
            );
        }
        log_error(
            COMPONENT,
            "VolumeSnapshot reconciliation failed",
            &[
                ("namespace", namespace_value),
                ("snapshot", name.as_str()),
                ("error", err.to_string().as_str()),
            ],
        );
    }
    let event_reason = if result.is_ok() {
        "SnapshotReady"
    } else {
        "SnapshotFailed"
    };
    let event_type = if result.is_ok() { "Normal" } else { "Warning" };
    let event_message = result
        .as_ref()
        .map(|_| format!("VolumeSnapshot {} reconciled", name))
        .unwrap_or_else(|err| err.to_string());
    let involved = InvolvedObjectRef {
        api_version: if snapshot_for_failure.api_version.is_empty() {
            "nanocloud.io/v1".to_string()
        } else {
            snapshot_for_failure.api_version.clone()
        },
        kind: "VolumeSnapshot".to_string(),
        name: name.clone(),
        uid: snapshot_for_failure.metadata.uid.clone(),
        namespace: snapshot_for_failure.metadata.namespace.clone(),
    };
    recorder
        .record(
            snapshot_for_failure.metadata.namespace.as_deref(),
            &involved,
            event_reason,
            event_type,
            event_message.as_str(),
        )
        .await;
    let reconcile_outcome = if result.is_ok() {
        ControllerReconcileResult::Success
    } else {
        ControllerReconcileResult::Error
    };
    metrics::record_controller_reconcile("snapshot", reconcile_outcome);
    result.map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)
}

async fn reconcile_snapshot(mut snapshot: VolumeSnapshot) -> Result<(), SnapshotError> {
    let namespace_value = snapshot.metadata.namespace.as_deref().unwrap_or("default");
    let namespace_ref = snapshot.metadata.namespace.as_deref();
    let name = snapshot
        .metadata
        .name
        .as_deref()
        .ok_or(SnapshotError::MissingMetadata("metadata.name"))?;
    let service_name = snapshot.spec.service.trim();
    if service_name.is_empty() {
        return Err(SnapshotError::InvalidSpec(
            "spec.service must be provided".to_string(),
        ));
    }
    let claim_name = snapshot.spec.volume_claim.trim();
    if claim_name.is_empty() {
        return Err(SnapshotError::InvalidSpec(
            "spec.volumeClaim must be provided".to_string(),
        ));
    }

    log_debug(
        COMPONENT,
        "Reconciling VolumeSnapshot",
        &[
            ("namespace", namespace_value),
            ("snapshot", name),
            ("service", service_name),
            ("claim", claim_name),
        ],
    );

    if snapshot.metadata.owner_references.is_empty() {
        if let Some(owner) = snapshot_owner_reference(namespace_ref, service_name).await {
            snapshot.metadata.owner_references.push(owner);
        }
    }

    let base_dir = backup_directory("snapshot", Some(namespace_value), service_name);
    let snapshot_dir = base_dir.join("snapshots");
    fs::create_dir_all(&snapshot_dir).map_err(|err| {
        SnapshotError::Operation(format!(
            "Failed to prepare snapshot directory '{}': {}",
            snapshot_dir.display(),
            err
        ))
    })?;

    let artifact_path = snapshot_dir.join(format!("{name}.tar"));
    let artifact_str = artifact_path
        .to_str()
        .ok_or_else(|| {
            SnapshotError::InvalidSpec("snapshot artifact path contains invalid UTF-8".to_string())
        })?
        .to_string();

    let summary = Snapshot::save(
        namespace_ref,
        service_name,
        Some(claim_name),
        artifact_str.as_str(),
    )
    .await
    .map_err(|err| SnapshotError::Operation(err.to_string()))?;

    if streaming_backup_enabled() {
        let label = format!("{}/{}", namespace_value, service_name);
        if let Err(err) = register_streaming_backup(label, &artifact_path) {
            let error_text = err.to_string();
            log_warn(
                COMPONENT,
                "Failed to register streaming backup",
                &[
                    ("path", artifact_str.as_str()),
                    ("error", error_text.as_str()),
                ],
            );
        }
    }

    let entry = summary
        .entries
        .iter()
        .find(|entry| entry.claim == claim_name)
        .ok_or_else(|| {
            SnapshotError::Operation(format!("Snapshot entry for claim '{}' missing", claim_name))
        })?;

    let archive_bytes = fs::read(&artifact_path).map_err(|err| {
        SnapshotError::Operation(format!(
            "Failed to read snapshot artifact '{}': {}",
            artifact_path.display(),
            err
        ))
    })?;
    let payload = BASE64.encode(&archive_bytes);
    let completion_time = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

    let status = VolumeSnapshotStatus {
        phase: Some(VolumeSnapshotPhase::Ready),
        snapshot_id: Some(entry.snapshot_id.clone()),
        volume_id: Some(entry.volume_id.clone()),
        artifact_ref: Some(artifact_str.clone()),
        size_bytes: Some(entry.size_bytes),
        payload: Some(payload),
        message: None,
        completion_time: Some(completion_time),
    };

    let resource_version = next_resource_version(snapshot.metadata.resource_version.as_deref());
    snapshot.metadata.resource_version = Some(resource_version);
    snapshot.status = Some(status);

    save_volume_snapshot(namespace_ref, name, &snapshot)
        .map_err(|err| SnapshotError::Operation(err.to_string()))?;

    log_info(
        COMPONENT,
        "VolumeSnapshot artifact stored",
        &[
            ("namespace", namespace_value),
            ("service", service_name),
            ("snapshot", name),
            ("claim", claim_name),
            ("path", artifact_str.as_str()),
        ],
    );

    Ok(())
}

fn mark_snapshot_failed(
    snapshot: &mut VolumeSnapshot,
    namespace: Option<&str>,
    name: &str,
    error: &SnapshotError,
) -> Result<(), SnapshotError> {
    if snapshot.metadata.name.is_none() {
        return Err(SnapshotError::MissingMetadata("metadata.name"));
    }
    let status = VolumeSnapshotStatus {
        phase: Some(VolumeSnapshotPhase::Failed),
        snapshot_id: None,
        volume_id: None,
        artifact_ref: None,
        size_bytes: None,
        payload: None,
        message: Some(error.to_string()),
        completion_time: Some(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)),
    };
    let resource_version = next_resource_version(snapshot.metadata.resource_version.as_deref());
    snapshot.metadata.resource_version = Some(resource_version);
    snapshot.status = Some(status);
    save_volume_snapshot(namespace, name, snapshot)
        .map_err(|err| SnapshotError::Operation(err.to_string()))?;
    Ok(())
}

fn next_resource_version(current: Option<&str>) -> String {
    current
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
        .saturating_add(1)
        .to_string()
}

#[derive(Debug)]
enum SnapshotError {
    MissingMetadata(&'static str),
    InvalidSpec(String),
    Operation(String),
}

impl Display for SnapshotError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SnapshotError::MissingMetadata(field) => {
                write!(f, "snapshot missing {field}")
            }
            SnapshotError::InvalidSpec(reason) => f.write_str(reason),
            SnapshotError::Operation(reason) => f.write_str(reason),
        }
    }
}

impl Error for SnapshotError {}
