use std::collections::HashMap;
use std::error::Error;

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::nanocloud::csi::types::{Snapshot, Volume};
use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::util::{is_missing_value_error, Keyspace};

use super::paths::{service_key, snapshot_key, volume_key};
use super::CsiDriver;

pub const CSI_KEYSPACE: Keyspace = Keyspace::new("csi");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredVolume {
    pub volume: Volume,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub parameters: HashMap<String, String>,
    pub path: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub publications: Vec<String>,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encrypted: Option<StoredEncryptedVolume>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub snapshot: Snapshot,
    #[serde(rename = "archivePath")]
    pub archive_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredEncryptedVolume {
    #[serde(rename = "keyName")]
    pub key_name: String,
    pub mapper: String,
    #[serde(rename = "filesystem")]
    pub filesystem: String,
    #[serde(rename = "backingPath")]
    pub backing_path: String,
    #[serde(rename = "sizeBytes")]
    pub size_bytes: u64,
}

pub fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub fn read_service_index(
    namespace: &str,
    service: &str,
) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
    // Service indices live under /services/<namespace>/<service> and store volume IDs.
    let key = service_key(namespace, service);
    match CSI_KEYSPACE.get(&key) {
        Ok(raw) => serde_json::from_str(&raw)
            .map_err(|e| with_context(e, format!("Failed to parse service index {}", key))),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(Vec::new())
            } else {
                Err(with_context(
                    err,
                    format!("Failed to load service index {}", key),
                ))
            }
        }
    }
}

pub fn write_service_index(
    namespace: &str,
    service: &str,
    ids: &[String],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = service_key(namespace, service);
    if ids.is_empty() {
        match CSI_KEYSPACE.delete(&key) {
            Ok(()) => Ok(()),
            Err(err) => {
                if is_missing_value_error(err.as_ref()) {
                    Ok(())
                } else {
                    Err(with_context(
                        err,
                        format!("Failed to delete service index {}", key),
                    ))
                }
            }
        }
    } else {
        let payload = serde_json::to_string(ids)
            .map_err(|e| with_context(e, format!("Failed to serialize service index {}", key)))?;
        CSI_KEYSPACE
            .put(&key, &payload)
            .map_err(|e| with_context(e, format!("Failed to store service index {}", key)))
    }
}

pub fn add_volume_to_service(
    namespace: &str,
    service: &str,
    volume_id: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut index = read_service_index(namespace, service)?;
    if !index.contains(&volume_id.to_string()) {
        index.push(volume_id.to_string());
        write_service_index(namespace, service, &index)?;
    }
    Ok(())
}

pub fn remove_volume_from_service(
    namespace: &str,
    service: &str,
    volume_id: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut index = read_service_index(namespace, service)?;
    let original_len = index.len();
    index.retain(|entry| entry != volume_id);
    if original_len != index.len() {
        write_service_index(namespace, service, &index)?;
    }
    Ok(())
}

pub fn persist_volume(stored: &StoredVolume) -> Result<(), Box<dyn Error + Send + Sync>> {
    let payload = serde_json::to_string(stored)
        .map_err(|e| with_context(e, "Failed to serialize volume record"))?;
    CSI_KEYSPACE
        .put(&volume_key(&stored.volume.volume_id), &payload)
        .map_err(|e| with_context(e, "Failed to persist volume record"))
}

pub fn delete_volume_record(volume_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    CSI_KEYSPACE
        .delete(&volume_key(volume_id))
        .map_err(|e| with_context(e, "Failed to delete volume record"))
}

pub fn load_volume(volume_id: &str) -> Result<Option<StoredVolume>, Box<dyn Error + Send + Sync>> {
    let key = volume_key(volume_id);
    match CSI_KEYSPACE.get(&key) {
        Ok(raw) => serde_json::from_str(&raw)
            .map(Some)
            .map_err(|e| with_context(e, "Failed to deserialize stored volume")),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(None)
            } else {
                Err(with_context(
                    err,
                    format!("Failed to load volume {}", volume_id),
                ))
            }
        }
    }
}

pub fn list_service_volumes(
    namespace: &str,
    service: &str,
) -> Result<Vec<StoredVolume>, Box<dyn Error + Send + Sync>> {
    let ids = read_service_index(namespace, service)?;
    let mut volumes = Vec::new();
    for id in ids {
        if let Some(volume) = load_volume(&id)? {
            volumes.push(volume);
        }
    }
    Ok(volumes)
}

pub fn persist_snapshot(snapshot: &StoredSnapshot) -> Result<(), Box<dyn Error + Send + Sync>> {
    let payload = serde_json::to_string(snapshot)
        .map_err(|e| with_context(e, "Failed to serialize snapshot record"))?;
    CSI_KEYSPACE
        .put(&snapshot_key(&snapshot.snapshot.snapshot_id), &payload)
        .map_err(|e| with_context(e, "Failed to persist snapshot record"))
}

pub fn load_snapshot(
    snapshot_id: &str,
) -> Result<Option<StoredSnapshot>, Box<dyn Error + Send + Sync>> {
    let key = snapshot_key(snapshot_id);
    match CSI_KEYSPACE.get(&key) {
        Ok(raw) => serde_json::from_str(&raw)
            .map(Some)
            .map_err(|e| with_context(e, "Failed to deserialize stored snapshot")),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(None)
            } else {
                Err(with_context(
                    err,
                    format!("Failed to load snapshot {}", snapshot_id),
                ))
            }
        }
    }
}

pub fn delete_snapshot_record(snapshot_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    CSI_KEYSPACE
        .delete(&snapshot_key(snapshot_id))
        .map_err(|e| with_context(e, "Failed to delete snapshot record"))
}

pub fn ensure_volume_exists(volume_id: &str) -> Result<StoredVolume, Box<dyn Error + Send + Sync>> {
    load_volume(volume_id)?.ok_or_else(|| new_error(format!("Volume '{}' not found", volume_id)))
}

impl CsiDriver {
    #[allow(dead_code)]
    pub fn load_volume(
        &self,
        volume_id: &str,
    ) -> Result<Option<StoredVolume>, Box<dyn Error + Send + Sync>> {
        load_volume(volume_id)
    }

    #[allow(dead_code)]
    pub fn list_service_volumes(
        &self,
        namespace: &str,
        service: &str,
    ) -> Result<Vec<StoredVolume>, Box<dyn Error + Send + Sync>> {
        list_service_volumes(namespace, service)
    }

    #[allow(dead_code)]
    pub fn load_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<Option<StoredSnapshot>, Box<dyn Error + Send + Sync>> {
        load_snapshot(snapshot_id)
    }
}
