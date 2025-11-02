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

use super::pod::ObjectMeta;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Minimal representation of Kubernetes resource requirements.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ResourceRequirements {
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub requests: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub limits: HashMap<String, String>,
}

/// PersistentVolumeClaimSpec matching the subset Nanocloud requires.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PersistentVolumeClaimSpec {
    #[serde(rename = "accessModes", default, skip_serializing_if = "Vec::is_empty")]
    pub access_modes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
    #[serde(rename = "storageClassName", skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,
    #[serde(rename = "volumeMode", skip_serializing_if = "Option::is_none")]
    pub volume_mode: Option<String>,
}

impl Default for PersistentVolumeClaimSpec {
    fn default() -> Self {
        PersistentVolumeClaimSpec {
            access_modes: vec!["ReadWriteOnce".to_string()],
            resources: None,
            storage_class_name: None,
            volume_mode: None,
        }
    }
}

/// PersistentVolumeClaim template embedded within StatefulSets.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PersistentVolumeClaim {
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub metadata: ObjectMeta,
    pub spec: PersistentVolumeClaimSpec,
}
