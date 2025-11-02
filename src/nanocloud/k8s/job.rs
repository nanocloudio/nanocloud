use super::pod::{ListMeta, ObjectMeta};
use super::statefulset::{LabelSelector, PodTemplateSpec};
use serde::{Deserialize, Serialize};

/// Condition describing Job lifecycle state.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct JobCondition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub status: String,
    #[serde(rename = "lastProbeTime", skip_serializing_if = "Option::is_none")]
    pub last_probe_time: Option<String>,
    #[serde(rename = "lastTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Pod execution template and completion policy for a Job.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct JobSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completions: Option<i32>,
    #[serde(rename = "backoffLimit", skip_serializing_if = "Option::is_none")]
    pub backoff_limit: Option<i32>,
    #[serde(
        rename = "activeDeadlineSeconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub active_deadline_seconds: Option<i64>,
    #[serde(
        rename = "ttlSecondsAfterFinished",
        skip_serializing_if = "Option::is_none"
    )]
    pub ttl_seconds_after_finished: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<LabelSelector>,
    #[serde(rename = "manualSelector", skip_serializing_if = "Option::is_none")]
    pub manual_selector: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suspend: Option<bool>,
    pub template: PodTemplateSpec,
}

/// Observed runtime status for a Job.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct JobStatus {
    #[serde(rename = "startTime", skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(rename = "completionTime", skip_serializing_if = "Option::is_none")]
    pub completion_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub succeeded: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed: Option<i32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<JobCondition>,
}

/// Batch Job resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Job {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: JobSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<JobStatus>,
}

impl Job {
    pub fn new(metadata: ObjectMeta, spec: JobSpec) -> Self {
        Self {
            api_version: "batch/v1".to_string(),
            kind: "Job".to_string(),
            metadata,
            spec,
            status: None,
        }
    }
}

/// Aggregated list of Jobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct JobList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ListMeta,
    pub items: Vec<Job>,
}

impl JobList {
    pub fn from_items(items: Vec<Job>) -> Self {
        Self {
            api_version: "batch/v1".to_string(),
            kind: "JobList".to_string(),
            metadata: ListMeta::default(),
            items,
        }
    }
}
