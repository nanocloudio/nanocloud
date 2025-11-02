use crate::nanocloud::api::types::{VolumeSnapshot, VolumeSnapshotPhase, VolumeSnapshotStatus};
use crate::nanocloud::controller::watch::ControllerWatchManager;
use crate::nanocloud::engine::container::backup_directory;
use crate::nanocloud::engine::{register_streaming_backup, streaming_backup_enabled, Snapshot};
use crate::nanocloud::k8s::store::{list_volume_snapshots, save_volume_snapshot};
use crate::nanocloud::logger::{log_debug, log_error, log_info, log_warn};
use crate::nanocloud::observability::metrics::{self, SnapshotOperation};
use crate::nanocloud::util::KeyspaceEventType;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use chrono::{SecondsFormat, Utc};
use serde_json;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs;
use tokio::task::JoinHandle;

const COMPONENT: &str = "snapshot-controller";
const SNAPSHOT_PREFIX: &str = "/volumesnapshots";

pub fn spawn() -> JoinHandle<()> {
    tokio::spawn(async move {
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
                    process_snapshot(snapshot).await;
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

        let manager = ControllerWatchManager::shared();
        let mut subscription = manager.subscribe(SNAPSHOT_PREFIX, None);
        while let Some(event) = subscription.recv().await {
            match event.event_type {
                KeyspaceEventType::Deleted => {
                    let key = event.key.as_str();
                    log_debug(COMPONENT, "VolumeSnapshot deleted", &[("key", key)]);
                }
                KeyspaceEventType::Added | KeyspaceEventType::Modified => {
                    let Some(value) = event.value else {
                        log_warn(COMPONENT, "VolumeSnapshot event missing payload", &[]);
                        continue;
                    };
                    match serde_json::from_str::<VolumeSnapshot>(&value) {
                        Ok(snapshot) => {
                            process_snapshot(snapshot).await;
                        }
                        Err(err) => {
                            log_error(
                                COMPONENT,
                                "Failed to decode VolumeSnapshot payload",
                                &[("error", err.to_string().as_str())],
                            );
                        }
                    }
                }
            }
        }
    })
}

async fn process_snapshot(snapshot: VolumeSnapshot) {
    let namespace = snapshot.metadata.namespace.clone();
    let name = snapshot
        .metadata
        .name
        .clone()
        .unwrap_or_else(|| "<unnamed>".to_string());
    let namespace_label = namespace.as_deref();
    let namespace_value = namespace_label.unwrap_or("default");
    let mut snapshot_for_failure = snapshot.clone();

    let result = metrics::observe_snapshot_operation(
        namespace_label,
        name.as_str(),
        SnapshotOperation::Reconcile,
        async move { reconcile_snapshot(snapshot).await },
    )
    .await;

    if let Err(err) = result {
        if let Err(status_err) = mark_snapshot_failed(
            &mut snapshot_for_failure,
            namespace_label,
            name.as_str(),
            &err,
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
        register_streaming_backup(label, &artifact_path);
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
