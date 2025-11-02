#![allow(dead_code)]

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
use serde_json::Value;
use std::collections::HashMap;

/// Minimal representation of Kubernetes object metadata.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ObjectMeta {
    pub name: Option<String>,
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub labels: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub annotations: HashMap<String, String>,
    #[serde(rename = "resourceVersion", skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
}

/// Metadata included with Kubernetes list responses.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListMeta {
    #[serde(rename = "resourceVersion", skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
    #[serde(rename = "continue", skip_serializing_if = "Option::is_none")]
    pub continue_token: Option<String>,
    #[serde(rename = "remainingItemCount", skip_serializing_if = "Option::is_none")]
    pub remaining_item_count: Option<u32>,
}

/// Minimal container specification derived from Kubernetes `Container`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ContainerSpec {
    pub name: String,
    pub image: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(skip)]
    pub image_command: Vec<String>,
    #[serde(skip)]
    pub image_args: Vec<String>,
    #[serde(rename = "envFrom", default, skip_serializing_if = "Vec::is_empty")]
    pub env_from: Vec<EnvFromSource>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<ContainerEnvVar>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<ContainerPort>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volume_mounts: Vec<VolumeMount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ContainerResources>,
    pub working_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<Lifecycle>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    pub liveness_probe: Option<ContainerProbe>,
    pub readiness_probe: Option<ContainerProbe>,
}

/// Minimal environment variable spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ContainerEnvVar {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(rename = "valueFrom", skip_serializing_if = "Option::is_none")]
    pub value_from: Option<EnvVarSource>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EnvFromSource {
    #[serde(rename = "configMapRef", skip_serializing_if = "Option::is_none")]
    pub config_map_ref: Option<ConfigMapEnvSource>,
    #[serde(rename = "secretRef", skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretEnvSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ConfigMapEnvSource {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EnvVarSource {
    #[serde(rename = "configMapKeyRef", skip_serializing_if = "Option::is_none")]
    pub config_map_key_ref: Option<ConfigMapKeySelector>,
    #[serde(rename = "secretKeyRef", skip_serializing_if = "Option::is_none")]
    pub secret_key_ref: Option<SecretKeySelector>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ConfigMapKeySelector {
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SecretEnvSource {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SecretKeySelector {
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// Container port declaration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ContainerPort {
    pub container_port: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_port: Option<u16>,
}

/// Describes how a volume is mounted inside the container.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct VolumeMount {
    pub name: String,
    pub mount_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
}

/// Minimal resource declaration for CPU, memory, and PIDs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ContainerResources {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_shares: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_quota: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_period: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_limit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_swap: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pids_limit: Option<i64>,
}

/// Simplified volume specification supporting the sources needed by Nanocloud.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct VolumeSpec {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_path: Option<HostPathVolumeSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub empty_dir: Option<EmptyDirVolumeSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret: Option<SecretVolumeSource>,
    #[serde(rename = "configMap", skip_serializing_if = "Option::is_none")]
    pub config_map: Option<ConfigMapVolumeSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projected: Option<ProjectedVolumeSource>,
    #[serde(
        rename = "persistentVolumeClaim",
        skip_serializing_if = "Option::is_none"
    )]
    pub persistent_volume_claim: Option<PersistentVolumeClaimVolumeSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encrypted: Option<EncryptedVolumeSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EncryptedVolumeSource {
    pub device: String,
    #[serde(rename = "keyName")]
    pub key_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filesystem: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ConfigMapVolumeSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "defaultMode", skip_serializing_if = "Option::is_none")]
    pub default_mode: Option<i32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<KeyToPath>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct KeyToPath {
    pub key: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<i32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ProjectedVolumeSource {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<VolumeProjection>,
    #[serde(rename = "defaultMode", skip_serializing_if = "Option::is_none")]
    pub default_mode: Option<i32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct VolumeProjection {
    #[serde(rename = "configMap", skip_serializing_if = "Option::is_none")]
    pub config_map: Option<ConfigMapProjection>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ConfigMapProjection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<KeyToPath>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct HostPathVolumeSource {
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EmptyDirVolumeSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub medium: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SecretVolumeSource {
    #[serde(rename = "secretName")]
    pub secret_name: String,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub items: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PersistentVolumeClaimVolumeSource {
    #[serde(rename = "claimName")]
    pub claim_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
}

/// Liveness/readiness probe configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ContainerProbe {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exec: Option<ProbeExec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_get: Option<ProbeHttpGet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_delay_seconds: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period_seconds: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ProbeExec {
    pub command: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Lifecycle {
    #[serde(rename = "preStart", skip_serializing_if = "Option::is_none")]
    pub pre_start: Option<ProbeExec>,
    #[serde(rename = "postStart", skip_serializing_if = "Option::is_none")]
    pub post_start: Option<ProbeExec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ProbeHttpGet {
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheme: Option<String>,
}

/// Minimal pod specification capturing the data Nanocloud needs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PodSpec {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub init_containers: Vec<ContainerSpec>,
    pub containers: Vec<ContainerSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<VolumeSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_account_name: Option<String>,
    #[serde(rename = "nodeName", skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub host_network: bool,
    #[serde(default)]
    pub security: PodSecurityContext,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PodSecurityContext {
    #[serde(rename = "allowPrivileged", default)]
    pub allow_privileged: bool,
    #[serde(
        rename = "extraCapabilities",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub extra_capabilities: Vec<String>,
    #[serde(rename = "seccompProfile", skip_serializing_if = "Option::is_none")]
    pub seccomp_profile: Option<PodSeccompProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PodSeccompProfile {
    #[serde(rename = "type")]
    pub profile_type: String,
    #[serde(rename = "localhostProfile", skip_serializing_if = "Option::is_none")]
    pub localhost_profile: Option<String>,
}

const fn is_false(value: &bool) -> bool {
    !*value
}

/// Pod object wrapper for convenience when exporting manifests.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Pod {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: PodSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<PodStatus>,
}

impl Pod {
    pub fn new(metadata: ObjectMeta, spec: PodSpec) -> Self {
        Self {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            metadata,
            spec,
            status: None,
        }
    }
}

/// Runtime status snapshot for the pod to surface through watch APIs.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PodStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(rename = "podIP", skip_serializing_if = "Option::is_none")]
    pub pod_ip: Option<String>,
    #[serde(rename = "startTime", skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(rename = "conditions", default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<PodCondition>,
    #[serde(
        rename = "containerStatuses",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub container_statuses: Vec<ContainerStatus>,
}

/// Container-level status information compatible with kubectl expectations.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ContainerStatus {
    pub name: String,
    #[serde(rename = "restartCount")]
    pub restart_count: u32,
    pub ready: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(rename = "imageID", skip_serializing_if = "Option::is_none")]
    pub image_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<HashMap<String, Value>>,
}

/// Minimal Pod condition representation used by kubectl wait/describe.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PodCondition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub status: String,
    #[serde(rename = "lastTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}
