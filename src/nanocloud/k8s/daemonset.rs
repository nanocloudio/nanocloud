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

use super::deployment::RollingUpdateValue;
use super::pod::{ListMeta, ObjectMeta};
use super::statefulset::{LabelSelector, PodTemplateSpec};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DaemonSetSpec {
    #[serde(default)]
    pub selector: LabelSelector,
    pub template: PodTemplateSpec,
    #[serde(rename = "updateStrategy", default)]
    pub update_strategy: DaemonSetUpdateStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "camelCase")]
pub struct DaemonSetUpdateStrategy {
    #[serde(rename = "type", default)]
    pub r#type: DaemonSetUpdateStrategyType,
    #[serde(rename = "rollingUpdate", skip_serializing_if = "Option::is_none")]
    pub rolling_update: Option<DaemonSetRollingUpdate>,
}

impl Default for DaemonSetUpdateStrategy {
    fn default() -> Self {
        Self {
            r#type: DaemonSetUpdateStrategyType::RollingUpdate,
            rolling_update: Some(DaemonSetRollingUpdate::default()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum DaemonSetUpdateStrategyType {
    #[serde(rename = "RollingUpdate")]
    #[default]
    RollingUpdate,
    #[serde(rename = "OnDelete")]
    OnDelete,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DaemonSetRollingUpdate {
    #[serde(rename = "maxUnavailable", skip_serializing_if = "Option::is_none")]
    pub max_unavailable: Option<RollingUpdateValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DaemonSetStatus {
    #[serde(
        rename = "currentNumberScheduled",
        skip_serializing_if = "Option::is_none"
    )]
    pub current_number_scheduled: Option<i32>,
    #[serde(rename = "numberReady", skip_serializing_if = "Option::is_none")]
    pub number_ready: Option<i32>,
    #[serde(
        rename = "updatedNumberScheduled",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_number_scheduled: Option<i32>,
    #[serde(rename = "numberUnavailable", skip_serializing_if = "Option::is_none")]
    pub number_unavailable: Option<i32>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub conditions: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DaemonSet {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: DaemonSetSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<DaemonSetStatus>,
}

impl DaemonSet {
    #[cfg(test)]
    pub fn new(metadata: ObjectMeta, spec: DaemonSetSpec) -> Self {
        DaemonSet {
            api_version: "apps/v1".to_string(),
            kind: "DaemonSet".to_string(),
            metadata,
            spec,
            status: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daemonset_new_sets_defaults() {
        let metadata = ObjectMeta {
            name: Some("demo".to_string()),
            ..Default::default()
        };
        let spec = DaemonSetSpec {
            selector: LabelSelector {
                match_labels: HashMap::new(),
            },
            template: PodTemplateSpec::default(),
            update_strategy: DaemonSetUpdateStrategy::default(),
        };

        let daemonset = DaemonSet::new(metadata.clone(), spec.clone());

        assert_eq!(daemonset.kind, "DaemonSet");
        assert_eq!(daemonset.api_version, "apps/v1");
        assert_eq!(daemonset.metadata.name, metadata.name);
        assert_eq!(
            daemonset.spec.template.metadata.name,
            spec.template.metadata.name
        );
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DaemonSetList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ListMeta,
    pub items: Vec<DaemonSet>,
}

impl DaemonSetList {
    pub fn from_items(items: Vec<DaemonSet>) -> Self {
        DaemonSetList {
            api_version: "apps/v1".to_string(),
            kind: "DaemonSetList".to_string(),
            metadata: ListMeta::default(),
            items,
        }
    }
}
