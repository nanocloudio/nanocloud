use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::nanocloud::k8s::pod::{ListMeta, ObjectMeta};
use crate::nanocloud::k8s::table::Table as KubeTable;

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NetworkPolicyRuleDebug {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NetworkPolicyChainDebug {
    pub namespace: String,
    pub pod: String,
    #[serde(rename = "podIP")]
    pub pod_ip: String,
    pub direction: String,
    pub chain: String,
    pub rules: Vec<NetworkPolicyRuleDebug>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NetworkPolicySummary {
    pub namespace: String,
    pub name: String,
    #[serde(rename = "policyTypes")]
    pub policy_types: Vec<String>,
    #[serde(rename = "podSelector")]
    pub pod_selector: HashMap<String, String>,
    #[serde(rename = "ingressRules")]
    pub ingress_rules: usize,
    #[serde(rename = "egressRules")]
    pub egress_rules: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NetworkPolicyDebugResponse {
    pub policies: Vec<NetworkPolicySummary>,
    pub chains: Vec<NetworkPolicyChainDebug>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CaRequest {
    pub common_name: String,
    #[serde(default)]
    pub additional: Option<Vec<String>>,
}

fn default_start() -> bool {
    true
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ServiceActionResponse {
    pub service: String,
    pub namespace: Option<String>,
    pub action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AsyncStatus {
    pub status: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LogQuery {
    #[serde(default)]
    pub follow: Option<bool>,
    #[serde(default)]
    pub container: Option<String>,
    #[serde(default)]
    pub previous: Option<bool>,
    #[serde(rename = "tailLines", default)]
    pub tail_lines: Option<u64>,
    #[serde(rename = "sinceSeconds", default)]
    pub since_seconds: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ApplyConflict {
    pub path: String,
    #[serde(rename = "existingManager")]
    pub existing_manager: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ErrorBody {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflicts: Option<Vec<ApplyConflict>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ServiceAccountTokenRequest {
    #[serde(rename = "singleUseToken")]
    pub single_use_token: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ServiceAccountTokenResponse {
    #[serde(rename = "token")]
    pub jwt: String,
    #[serde(rename = "expiresAt")]
    pub expires_at: String,
    #[serde(rename = "issuedAt")]
    pub issued_at: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CertificateSpec {
    #[serde(rename = "csr")]
    pub csr_pem: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CertificateStatus {
    #[serde(rename = "certificate")]
    pub certificate_pem: String,
    #[serde(rename = "caBundle")]
    pub ca_bundle_pem: String,
    #[serde(
        rename = "expirationTimestamp",
        skip_serializing_if = "Option::is_none"
    )]
    pub expiration_timestamp: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CertificateRequest {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub spec: CertificateSpec,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CertificateResponse {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub status: CertificateStatus,
}

pub type PodTable = KubeTable;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BundleSpec {
    /// Target service identifier (matching the OCI manifest name).
    pub service: String,
    /// Optional namespace scope; defaults to `default` when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// User-supplied installation options.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub options: HashMap<String, String>,
    /// Wrapped profile encryption key (base64-encoded).
    #[serde(rename = "key", skip_serializing_if = "Option::is_none")]
    pub profile_key: Option<String>,
    /// Optional snapshot source used to seed persistent volumes at install time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<BundleSnapshotSource>,
    /// Indicates whether the workload should be started after reconciliation.
    #[serde(default = "default_start")]
    pub start: bool,
    /// Forces a fresh pull of the service image even if cached layers already exist.
    #[serde(default, skip_serializing_if = "is_false")]
    pub update: bool,
    /// Optional runtime security profile.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<BundleSecurityProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BundleSnapshotSource {
    /// URI referencing the snapshot payload (e.g. `keyspace://profiles/...` or `file:///path`).
    pub source: String,
    /// Media type for the snapshot artifact (default `application/x-tar`).
    #[serde(rename = "mediaType", skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BundleSecurityProfile {
    /// Gate for privileged capability requests (CAP_SYS_ADMIN, CAP_SYS_PTRACE, etc).
    #[serde(rename = "allowPrivileged", default)]
    pub allow_privileged: bool,
    /// Explicit capabilities to inject into the container's allowed set.
    #[serde(
        rename = "extraCapabilities",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub extra_capabilities: Vec<String>,
    /// Optional seccomp profile override.
    #[serde(rename = "seccompProfile", skip_serializing_if = "Option::is_none")]
    pub seccomp_profile: Option<BundleSeccompProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BundleSeccompProfile {
    #[serde(rename = "type")]
    pub profile_type: BundleSeccompProfileType,
    #[serde(rename = "localhostProfile", skip_serializing_if = "Option::is_none")]
    pub localhost_profile: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "PascalCase")]
pub enum BundleSeccompProfileType {
    Baseline,
    RuntimeDefault,
    Localhost,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "UPPERCASE")]
pub enum BundlePhase {
    Pending,
    Validating,
    Rendering,
    Applying,
    Ready,
    Failed,
    Deleting,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "UPPERCASE")]
pub enum BundleConditionStatus {
    True,
    False,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "PascalCase")]
pub enum BundleConditionKind {
    Ready,
    Bound,
    SecretsProvisioned,
    ProfilePrepared,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BundleCondition {
    /// Logical condition identifier (e.g. `Ready`, `Bound`).
    #[serde(rename = "type")]
    pub condition_type: BundleConditionKind,
    /// Overall status (True/False/Unknown).
    pub status: BundleConditionStatus,
    /// Machine readable reason (e.g. `ProfileMissing`, `ApplySucceeded`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Human readable context for the condition.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// RFC3339 timestamp for the last transition.
    #[serde(rename = "lastTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BundleWorkloadRef {
    /// Workload name materialised from the bundle.
    pub name: String,
    /// Namespace hosting the workload; defaults to the bundle namespace when omitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Optional UID of the managed resource.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BundleStatus {
    /// Observed bundle generation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// High-level reconciliation phase.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<BundlePhase>,
    /// Detailed condition set mirroring Kubernetes conventions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<BundleCondition>,
    /// Workload reference emitted by the controller (StatefulSet/Deployment/etc).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workload: Option<BundleWorkloadRef>,
    /// Timestamp of the last successful reconciliation.
    #[serde(rename = "lastReconciledTime", skip_serializing_if = "Option::is_none")]
    pub last_reconciled_time: Option<String>,
    /// Latest binding execution outcomes.
    #[serde(
        rename = "bindingHistory",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub binding_history: Vec<BindingHistoryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BindingHistoryStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    TimedOut,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BindingHistoryEntry {
    #[serde(rename = "bindingId")]
    pub binding_id: String,
    pub service: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    pub status: BindingHistoryStatus,
    /// Total attempts recorded for the binding.
    pub attempts: u32,
    #[serde(rename = "lastStartedAt", skip_serializing_if = "Option::is_none")]
    pub last_started_at: Option<String>,
    #[serde(rename = "lastFinishedAt", skip_serializing_if = "Option::is_none")]
    pub last_finished_at: Option<String>,
    #[serde(rename = "durationMs", skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,
    #[serde(rename = "eventId", skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BundleProfileExportManifest {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: BundleProfileExportMetadata,
    /// Hex-encoded SHA256 digest of the profile payload.
    pub digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BundleProfileExportMetadata {
    pub name: String,
    pub namespace: String,
    pub service: String,
    #[serde(rename = "resourceVersion", skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BundleProfileArtifact {
    #[serde(flatten)]
    pub data: BundleProfileArtifactData,
    pub integrity: BundleProfileArtifactIntegrity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BundleProfileArtifactData {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: BundleProfileExportMetadata,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "profileKey")]
    pub profile_key: String,
    pub options: HashMap<String, String>,
    pub secrets: Vec<BundleProfileSecret>,
    pub bindings: Vec<BindingHistoryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BundleProfileSecret {
    pub name: String,
    #[serde(rename = "cipherText")]
    pub cipher_text: String,
    #[serde(rename = "keyId")]
    pub key_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BundleProfileArtifactIntegrity {
    #[serde(rename = "sha256")]
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Bundle {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: BundleSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<BundleStatus>,
}

/// Payload accepted by the `/status` subresource.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BundleStatusPatch {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub status: BundleStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BundleList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ListMeta,
    pub items: Vec<Bundle>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeviceSpec {
    /// Stable device identity hash allocated by the provisioning client.
    pub hash: String,
    /// Subject string embedded in the issued client certificate (e.g. `device:<hash>`).
    #[serde(rename = "certificateSubject")]
    pub certificate_subject: String,
    /// Optional human readable context for operators.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeviceStatus {
    /// RFC3339 timestamp of the most recent certificate issuance for this device.
    #[serde(
        rename = "certificateIssuedAt",
        skip_serializing_if = "Option::is_none"
    )]
    pub certificate_issued_at: Option<String>,
    /// RFC3339 timestamp when the active certificate expires.
    #[serde(
        rename = "certificateExpiresAt",
        skip_serializing_if = "Option::is_none"
    )]
    pub certificate_expires_at: Option<String>,
    /// RFC3339 timestamp reported by the device during its last status update.
    #[serde(rename = "lastSeen", skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Device {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: DeviceSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<DeviceStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeviceList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ListMeta,
    pub items: Vec<Device>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct VolumeSnapshotSpec {
    /// Workload service identifier owning the targeted volume claim.
    pub service: String,
    /// PersistentVolumeClaim name bound to the volume that should be snapshotted.
    #[serde(rename = "volumeClaim")]
    pub volume_claim: String,
    /// Optional free-form description for audit trails.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "UPPERCASE")]
pub enum VolumeSnapshotPhase {
    Pending,
    Preparing,
    Ready,
    Failed,
    Deleting,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct VolumeSnapshotStatus {
    /// Observed reconciliation phase.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<VolumeSnapshotPhase>,
    /// Identifier returned by the CSI driver for the snapshot.
    #[serde(rename = "snapshotId", skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
    /// CSI volume identifier associated with the snapshot.
    #[serde(rename = "volumeId", skip_serializing_if = "Option::is_none")]
    pub volume_id: Option<String>,
    /// Reference to the stored artifact (e.g. file:// path).
    #[serde(rename = "artifactRef", skip_serializing_if = "Option::is_none")]
    pub artifact_ref: Option<String>,
    /// Total size of the archived payload in bytes.
    #[serde(rename = "sizeBytes", skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// Base64-encoded snapshot archive payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
    /// Human-readable message for failures or additional context.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Timestamp indicating when the snapshot completed successfully.
    #[serde(rename = "completionTime", skip_serializing_if = "Option::is_none")]
    pub completion_time: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct VolumeSnapshot {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: VolumeSnapshotSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<VolumeSnapshotStatus>,
}
