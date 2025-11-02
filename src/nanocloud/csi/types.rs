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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CapacityRange {
    #[serde(rename = "requiredBytes", skip_serializing_if = "Option::is_none")]
    pub required_bytes: Option<u64>,
    #[serde(rename = "limitBytes", skip_serializing_if = "Option::is_none")]
    pub limit_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MountVolumeCapability {
    #[serde(rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mount_flags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum AccessMode {
    #[serde(rename = "SINGLE_NODE_WRITER")]
    #[default]
    SingleNodeWriter,
    #[serde(rename = "SINGLE_NODE_READER_ONLY")]
    SingleNodeReaderOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VolumeCapability {
    #[serde(rename = "accessMode", skip_serializing_if = "Option::is_none")]
    pub access_mode: Option<AccessMode>,
    #[serde(rename = "mount", skip_serializing_if = "Option::is_none")]
    pub mount: Option<MountVolumeCapability>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeContentSourceSnapshot {
    #[serde(rename = "snapshotId")]
    pub snapshot_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VolumeContentSource {
    #[serde(rename = "snapshot", skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<VolumeContentSourceSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Volume {
    #[serde(rename = "volumeId")]
    pub volume_id: String,
    #[serde(rename = "capacityBytes")]
    pub capacity_bytes: u64,
    #[serde(
        default,
        rename = "volumeContext",
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub volume_context: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    #[serde(rename = "snapshotId")]
    pub snapshot_id: String,
    #[serde(rename = "sourceVolumeId")]
    pub source_volume_id: String,
    #[serde(rename = "sizeBytes")]
    pub size_bytes: u64,
    #[serde(rename = "readyToUse")]
    pub ready_to_use: bool,
    #[serde(rename = "creationTime")]
    pub creation_time: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CreateVolumeRequest {
    pub name: String,
    #[serde(rename = "capacityRange", skip_serializing_if = "Option::is_none")]
    pub capacity_range: Option<CapacityRange>,
    #[serde(
        rename = "volumeCapabilities",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub volume_capabilities: Vec<VolumeCapability>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub parameters: HashMap<String, String>,
    #[serde(rename = "contentSource", skip_serializing_if = "Option::is_none")]
    pub content_source: Option<VolumeContentSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateVolumeResponse {
    pub volume: Volume,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DeleteVolumeRequest {
    #[serde(rename = "volumeId")]
    pub volume_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodePublishVolumeRequest {
    #[serde(rename = "volumeId")]
    pub volume_id: String,
    #[serde(rename = "targetPath")]
    pub target_path: String,
    #[serde(default)]
    pub readonly: bool,
    #[serde(rename = "volumeCapability", skip_serializing_if = "Option::is_none")]
    pub volume_capability: Option<VolumeCapability>,
    #[serde(
        rename = "volumeContext",
        default,
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub volume_context: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePublishVolumeResponse {
    #[serde(rename = "publishPath")]
    pub publish_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeUnpublishVolumeRequest {
    #[serde(rename = "volumeId")]
    pub volume_id: String,
    #[serde(rename = "targetPath")]
    pub target_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CreateSnapshotRequest {
    pub name: String,
    #[serde(rename = "sourceVolumeId")]
    pub source_volume_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSnapshotResponse {
    pub snapshot: Snapshot,
    #[serde(rename = "archivePath")]
    pub archive_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DeleteSnapshotRequest {
    #[serde(rename = "snapshotId")]
    pub snapshot_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn access_mode_defaults_to_single_node_writer() {
        assert!(matches!(
            AccessMode::default(),
            AccessMode::SingleNodeWriter
        ));
    }

    #[test]
    fn volume_capability_serializes_access_mode_and_mount() {
        let capability = VolumeCapability {
            access_mode: Some(AccessMode::SingleNodeReaderOnly),
            mount: Some(MountVolumeCapability {
                fs_type: Some("ext4".into()),
                mount_flags: vec!["ro".into(), "noatime".into()],
            }),
        };

        let value = serde_json::to_value(&capability).expect("serialize capability");
        assert_eq!(value["accessMode"], json!("SINGLE_NODE_READER_ONLY"));
        assert_eq!(value["mount"]["fsType"], json!("ext4"));
        assert_eq!(value["mount"]["mount_flags"], json!(["ro", "noatime"]));
    }

    #[test]
    fn volume_content_source_serializes_snapshot_id() {
        let source = VolumeContentSource {
            snapshot: Some(VolumeContentSourceSnapshot {
                snapshot_id: "snap-123".into(),
            }),
        };

        let value = serde_json::to_value(&source).expect("serialize content source");
        assert_eq!(value["snapshot"]["snapshotId"], json!("snap-123"));
    }
}
