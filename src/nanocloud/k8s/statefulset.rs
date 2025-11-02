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

use super::persistentvolumeclaim::PersistentVolumeClaim;
use super::pod::{ListMeta, ObjectMeta, PodSpec};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// High-level condition describing StatefulSet rollout state.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StatefulSetCondition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Minimal label selector supporting exact-match labels.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct LabelSelector {
    #[serde(
        rename = "matchLabels",
        default,
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub match_labels: HashMap<String, String>,
}

/// Template describing the pods managed by the StatefulSet.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PodTemplateSpec {
    pub metadata: ObjectMeta,
    pub spec: PodSpec,
}

/// Minimal StatefulSet specification to support Nanocloud workloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StatefulSetSpec {
    #[serde(rename = "serviceName")]
    pub service_name: String,
    pub replicas: i32,
    pub selector: LabelSelector,
    pub template: PodTemplateSpec,
    #[serde(rename = "updateStrategy", default)]
    pub update_strategy: StatefulSetUpdateStrategy,
    #[serde(
        rename = "volumeClaimTemplates",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub volume_claim_templates: Vec<PersistentVolumeClaim>,
}

/// StatefulSet update behaviour.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "camelCase")]
pub struct StatefulSetUpdateStrategy {
    #[serde(rename = "type", default)]
    pub r#type: StatefulSetUpdateStrategyType,
    #[serde(rename = "rollingUpdate", skip_serializing_if = "Option::is_none")]
    pub rolling_update: Option<StatefulSetRollingUpdate>,
}

impl Default for StatefulSetUpdateStrategy {
    fn default() -> Self {
        Self {
            r#type: StatefulSetUpdateStrategyType::RollingUpdate,
            rolling_update: Some(StatefulSetRollingUpdate::default()),
        }
    }
}

impl StatefulSetUpdateStrategy {
    pub fn is_on_delete(&self) -> bool {
        matches!(self.r#type, StatefulSetUpdateStrategyType::OnDelete)
    }

    pub fn partition(&self) -> u32 {
        self.rolling_update
            .as_ref()
            .and_then(|config| config.partition)
            .unwrap_or(0)
    }
}

/// Supported update strategy types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum StatefulSetUpdateStrategyType {
    #[serde(rename = "RollingUpdate")]
    #[default]
    RollingUpdate,
    #[serde(rename = "OnDelete")]
    OnDelete,
}

/// Rolling update configuration for StatefulSets.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StatefulSetRollingUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<u32>,
}

/// Basic runtime status for a StatefulSet.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StatefulSetStatus {
    #[serde(rename = "readyReplicas", skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
    #[serde(rename = "currentReplicas", skip_serializing_if = "Option::is_none")]
    pub current_replicas: Option<i32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<StatefulSetCondition>,
}

/// StatefulSet object description.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StatefulSet {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: StatefulSetSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<StatefulSetStatus>,
}

impl StatefulSet {
    #[cfg(test)]
    pub fn new(metadata: ObjectMeta, spec: StatefulSetSpec) -> Self {
        Self {
            api_version: "apps/v1".to_string(),
            kind: "StatefulSet".to_string(),
            metadata,
            spec,
            status: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StatefulSetList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ListMeta,
    pub items: Vec<StatefulSet>,
}

impl StatefulSetList {
    pub fn from_items(items: Vec<StatefulSet>) -> Self {
        StatefulSetList {
            api_version: "apps/v1".to_string(),
            kind: "StatefulSetList".to_string(),
            metadata: ListMeta::default(),
            items,
        }
    }
}
