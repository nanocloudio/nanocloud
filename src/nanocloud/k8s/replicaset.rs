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

use super::pod::{ListMeta, ObjectMeta};
use super::statefulset::{LabelSelector, PodTemplateSpec};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ReplicaSetSpec {
    pub replicas: i32,
    #[serde(default)]
    pub selector: LabelSelector,
    pub template: PodTemplateSpec,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ReplicaSetStatus {
    #[serde(rename = "replicas", skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    #[serde(rename = "readyReplicas", skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
    #[serde(rename = "availableReplicas", skip_serializing_if = "Option::is_none")]
    pub available_replicas: Option<i32>,
    #[serde(
        rename = "fullyLabeledReplicas",
        skip_serializing_if = "Option::is_none"
    )]
    pub fully_labeled_replicas: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ReplicaSet {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: ReplicaSetSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ReplicaSetStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ReplicaSetList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ListMeta,
    pub items: Vec<ReplicaSet>,
}

impl ReplicaSetList {
    pub fn from_items(items: Vec<ReplicaSet>) -> Self {
        Self {
            api_version: "apps/v1".to_string(),
            kind: "ReplicaSetList".to_string(),
            metadata: ListMeta::default(),
            items,
        }
    }
}
