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

//! Webhook CRD types for edge webhook handling.
//!
//! Webhooks define HTTP endpoints that trigger Job creation when called.
//! They support shared secret/HMAC validation and deduplication.
//!
//! # Example Webhook
//!
//! ```yaml
//! apiVersion: nanocloud.io/v1
//! kind: Webhook
//! metadata:
//!   name: github-deploy
//!   namespace: default
//! spec:
//!   path: /webhooks/github/deploy
//!   secretRef:
//!     name: github-webhook-secret
//!     key: token
//!   hmacHeader: X-Hub-Signature-256
//!   hmacAlgorithm: sha256
//!   dedupeKeys:
//!     - "$.headers['X-GitHub-Delivery']"
//!   jobTemplate:
//!     metadata:
//!       generateName: github-deploy-
//!     spec:
//!       template:
//!         spec:
//!           containers:
//!             - name: deploy
//!               image: deploy-runner:latest
//!               env:
//!                 - name: PAYLOAD
//!                   value: "{{.body}}"
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::job::JobSpec;
use super::pod::{ListMeta, ObjectMeta};

/// API version for Webhook resources.
pub const API_VERSION: &str = "nanocloud.io/v1";

/// Kind for Webhook resources.
pub const KIND: &str = "Webhook";

/// Supported HMAC algorithms.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum HmacAlgorithm {
    /// SHA-256 (default, recommended)
    #[default]
    Sha256,
    /// SHA-1 (legacy, for compatibility)
    Sha1,
    /// SHA-512
    Sha512,
}

impl HmacAlgorithm {
    /// Get the OpenSSL message digest for this algorithm.
    pub fn digest_name(&self) -> &'static str {
        match self {
            HmacAlgorithm::Sha256 => "sha256",
            HmacAlgorithm::Sha1 => "sha1",
            HmacAlgorithm::Sha512 => "sha512",
        }
    }

    /// Get the expected signature prefix (e.g., "sha256=").
    pub fn signature_prefix(&self) -> &'static str {
        match self {
            HmacAlgorithm::Sha256 => "sha256=",
            HmacAlgorithm::Sha1 => "sha1=",
            HmacAlgorithm::Sha512 => "sha512=",
        }
    }
}

/// Reference to a Kubernetes Secret containing the webhook shared secret.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SecretKeyRef {
    /// Name of the Secret.
    pub name: String,

    /// Key within the Secret's data.
    pub key: String,

    /// Namespace of the Secret.
    /// Defaults to the Webhook's namespace if not specified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

impl SecretKeyRef {
    /// Create a new SecretKeyRef.
    pub fn new(name: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            key: key.into(),
            namespace: None,
        }
    }

    /// Set the namespace.
    #[must_use]
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Resolve the namespace, using the provided default if not set.
    pub fn resolved_namespace<'a>(&'a self, default: &'a str) -> &'a str {
        self.namespace.as_deref().unwrap_or(default)
    }
}

/// Job template for webhook-triggered jobs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct WebhookJobTemplate {
    /// Metadata for the generated Job.
    /// Supports generateName for unique job names.
    pub metadata: ObjectMeta,

    /// Job specification (without metadata).
    pub spec: JobSpec,
}

/// Specification for a Webhook.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct WebhookSpec {
    /// The URL path where this webhook listens.
    /// Must start with '/'.
    pub path: String,

    /// Reference to a Secret containing the shared secret for validation.
    /// If not specified, no signature validation is performed.
    #[serde(rename = "secretRef", skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretKeyRef>,

    /// HTTP header containing the HMAC signature.
    /// Common values: "X-Hub-Signature-256" (GitHub), "X-Signature" (generic)
    #[serde(rename = "hmacHeader", skip_serializing_if = "Option::is_none")]
    pub hmac_header: Option<String>,

    /// HMAC algorithm to use for signature validation.
    /// Defaults to SHA-256.
    #[serde(
        rename = "hmacAlgorithm",
        default,
        skip_serializing_if = "is_default_algorithm"
    )]
    pub hmac_algorithm: HmacAlgorithm,

    /// JSONPath expressions to extract deduplication keys from the request.
    /// If any expression matches a previously seen value, the webhook is ignored.
    /// Supports expressions like:
    /// - "$.headers['X-GitHub-Delivery']" - Extract from header
    /// - "$.body.id" - Extract from JSON body
    #[serde(rename = "dedupeKeys", default, skip_serializing_if = "Vec::is_empty")]
    pub dedupe_keys: Vec<String>,

    /// TTL for deduplication entries in seconds.
    /// Defaults to 3600 (1 hour).
    #[serde(rename = "dedupeTtlSeconds", skip_serializing_if = "Option::is_none")]
    pub dedupe_ttl_seconds: Option<u64>,

    /// Template for the Job to create when the webhook is triggered.
    #[serde(rename = "jobTemplate")]
    pub job_template: WebhookJobTemplate,

    /// Environment variable mappings from webhook data.
    /// Keys are env var names, values are JSONPath expressions.
    #[serde(
        rename = "envMappings",
        default,
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub env_mappings: HashMap<String, String>,

    /// Argument mappings for the container command.
    /// Values are JSONPath expressions that get appended to container args.
    #[serde(rename = "argMappings", default, skip_serializing_if = "Vec::is_empty")]
    pub arg_mappings: Vec<String>,

    /// Maximum body size in bytes. Defaults to 1MB.
    #[serde(rename = "maxBodySize", skip_serializing_if = "Option::is_none")]
    pub max_body_size: Option<usize>,

    /// Request timeout in seconds. Defaults to 30.
    #[serde(rename = "timeoutSeconds", skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
}

fn is_default_algorithm(alg: &HmacAlgorithm) -> bool {
    *alg == HmacAlgorithm::Sha256
}

impl Default for WebhookSpec {
    fn default() -> Self {
        Self {
            path: String::new(),
            secret_ref: None,
            hmac_header: None,
            hmac_algorithm: HmacAlgorithm::Sha256,
            dedupe_keys: Vec::new(),
            dedupe_ttl_seconds: None,
            job_template: WebhookJobTemplate::default(),
            env_mappings: HashMap::new(),
            arg_mappings: Vec::new(),
            max_body_size: None,
            timeout_seconds: None,
        }
    }
}

impl WebhookSpec {
    /// Create a new WebhookSpec with the given path and job template.
    pub fn new(path: impl Into<String>, job_template: WebhookJobTemplate) -> Self {
        Self {
            path: path.into(),
            job_template,
            ..Default::default()
        }
    }

    /// Set the secret reference for HMAC validation.
    #[must_use]
    pub fn with_secret(mut self, secret_ref: SecretKeyRef, hmac_header: impl Into<String>) -> Self {
        self.secret_ref = Some(secret_ref);
        self.hmac_header = Some(hmac_header.into());
        self
    }

    /// Set the HMAC algorithm.
    #[must_use]
    pub fn with_hmac_algorithm(mut self, algorithm: HmacAlgorithm) -> Self {
        self.hmac_algorithm = algorithm;
        self
    }

    /// Add a deduplication key expression.
    #[must_use]
    pub fn with_dedupe_key(mut self, key_expr: impl Into<String>) -> Self {
        self.dedupe_keys.push(key_expr.into());
        self
    }

    /// Set the deduplication TTL.
    #[must_use]
    pub fn with_dedupe_ttl(mut self, seconds: u64) -> Self {
        self.dedupe_ttl_seconds = Some(seconds);
        self
    }

    /// Add an environment variable mapping.
    #[must_use]
    pub fn with_env_mapping(mut self, name: impl Into<String>, expr: impl Into<String>) -> Self {
        self.env_mappings.insert(name.into(), expr.into());
        self
    }

    /// Get the effective deduplication TTL in seconds.
    pub fn effective_dedupe_ttl(&self) -> u64 {
        self.dedupe_ttl_seconds.unwrap_or(3600)
    }

    /// Get the effective maximum body size.
    pub fn effective_max_body_size(&self) -> usize {
        self.max_body_size.unwrap_or(1024 * 1024) // 1MB default
    }

    /// Get the effective timeout in seconds.
    pub fn effective_timeout(&self) -> u64 {
        self.timeout_seconds.unwrap_or(30)
    }

    /// Check if HMAC validation is configured.
    pub fn requires_hmac_validation(&self) -> bool {
        self.secret_ref.is_some() && self.hmac_header.is_some()
    }

    /// Validate the WebhookSpec.
    pub fn validate(&self) -> Result<(), WebhookValidationError> {
        if self.path.is_empty() {
            return Err(WebhookValidationError::MissingPath);
        }

        if !self.path.starts_with('/') {
            return Err(WebhookValidationError::InvalidPath(
                "path must start with '/'".to_string(),
            ));
        }

        // If secret_ref is set, hmac_header must also be set
        if self.secret_ref.is_some() && self.hmac_header.is_none() {
            return Err(WebhookValidationError::MissingHmacHeader);
        }

        // Validate job template has required fields
        if self.job_template.spec.template.spec.containers.is_empty() {
            return Err(WebhookValidationError::InvalidJobTemplate(
                "job template must have at least one container".to_string(),
            ));
        }

        Ok(())
    }
}

/// Condition for Webhook status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct WebhookCondition {
    /// Type of condition (e.g., "Ready", "SecretResolved").
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status of the condition ("True", "False", "Unknown").
    pub status: String,

    /// Last time the condition transitioned.
    #[serde(rename = "lastTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,

    /// Human-readable reason for the condition.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Human-readable message with details.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl WebhookCondition {
    /// Create a new condition.
    pub fn new(condition_type: impl Into<String>, status: impl Into<String>) -> Self {
        Self {
            condition_type: condition_type.into(),
            status: status.into(),
            last_transition_time: None,
            reason: None,
            message: None,
        }
    }

    /// Create a "Ready" condition set to True.
    pub fn ready() -> Self {
        Self::new("Ready", "True")
    }

    /// Create a "Ready" condition set to False with reason.
    pub fn not_ready(reason: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            condition_type: "Ready".to_string(),
            status: "False".to_string(),
            last_transition_time: None,
            reason: Some(reason.into()),
            message: Some(message.into()),
        }
    }
}

/// Status of a Webhook.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct WebhookStatus {
    /// Conditions describing the current state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<WebhookCondition>,

    /// Total number of times this webhook has been triggered.
    #[serde(rename = "triggerCount", skip_serializing_if = "Option::is_none")]
    pub trigger_count: Option<u64>,

    /// Number of Jobs successfully created.
    #[serde(rename = "successCount", skip_serializing_if = "Option::is_none")]
    pub success_count: Option<u64>,

    /// Number of failed Job creations.
    #[serde(rename = "failureCount", skip_serializing_if = "Option::is_none")]
    pub failure_count: Option<u64>,

    /// Number of deduplicated (skipped) webhook calls.
    #[serde(rename = "dedupeCount", skip_serializing_if = "Option::is_none")]
    pub dedupe_count: Option<u64>,

    /// Last time the webhook was successfully triggered.
    #[serde(rename = "lastTriggerTime", skip_serializing_if = "Option::is_none")]
    pub last_trigger_time: Option<String>,

    /// Name of the last Job created.
    #[serde(rename = "lastJobName", skip_serializing_if = "Option::is_none")]
    pub last_job_name: Option<String>,
}

impl WebhookStatus {
    /// Check if the Webhook is ready.
    pub fn is_ready(&self) -> bool {
        self.conditions
            .iter()
            .any(|c| c.condition_type == "Ready" && c.status == "True")
    }

    /// Set the Ready condition.
    pub fn set_ready(&mut self, ready: bool, reason: Option<&str>, message: Option<&str>) {
        // Remove existing Ready condition
        self.conditions.retain(|c| c.condition_type != "Ready");

        let condition = if ready {
            WebhookCondition::ready()
        } else {
            WebhookCondition::not_ready(
                reason.unwrap_or("Unknown"),
                message.unwrap_or("Webhook is not ready"),
            )
        };

        self.conditions.push(condition);
    }

    /// Increment the trigger count.
    pub fn record_trigger(&mut self) {
        self.trigger_count = Some(self.trigger_count.unwrap_or(0) + 1);
    }

    /// Record a successful job creation.
    pub fn record_success(&mut self, job_name: &str, timestamp: &str) {
        self.success_count = Some(self.success_count.unwrap_or(0) + 1);
        self.last_trigger_time = Some(timestamp.to_string());
        self.last_job_name = Some(job_name.to_string());
    }

    /// Record a failed job creation.
    pub fn record_failure(&mut self) {
        self.failure_count = Some(self.failure_count.unwrap_or(0) + 1);
    }

    /// Record a deduplicated webhook call.
    pub fn record_dedupe(&mut self) {
        self.dedupe_count = Some(self.dedupe_count.unwrap_or(0) + 1);
    }
}

/// Webhook resource for edge webhook handling.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Webhook {
    /// API version (always "nanocloud.io/v1").
    #[serde(rename = "apiVersion")]
    pub api_version: String,

    /// Kind (always "Webhook").
    pub kind: String,

    /// Standard object metadata.
    pub metadata: ObjectMeta,

    /// Desired state of the Webhook.
    pub spec: WebhookSpec,

    /// Observed state of the Webhook.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<WebhookStatus>,
}

impl Default for Webhook {
    fn default() -> Self {
        Self {
            api_version: API_VERSION.to_string(),
            kind: KIND.to_string(),
            metadata: ObjectMeta::default(),
            spec: WebhookSpec::default(),
            status: None,
        }
    }
}

impl Webhook {
    /// Create a new Webhook with the given name and spec.
    pub fn new(name: impl Into<String>, spec: WebhookSpec) -> Self {
        Self {
            metadata: ObjectMeta {
                name: Some(name.into()),
                ..Default::default()
            },
            spec,
            ..Default::default()
        }
    }

    /// Get the Webhook name.
    pub fn name(&self) -> &str {
        self.metadata.name.as_deref().unwrap_or("")
    }

    /// Get the Webhook namespace.
    pub fn namespace(&self) -> &str {
        self.metadata.namespace.as_deref().unwrap_or("default")
    }

    /// Validate the Webhook.
    pub fn validate(&self) -> Result<(), WebhookValidationError> {
        if self.name().is_empty() {
            return Err(WebhookValidationError::MissingName);
        }
        self.spec.validate()
    }

    /// Check if the Webhook is ready.
    pub fn is_ready(&self) -> bool {
        self.status.as_ref().is_some_and(|s| s.is_ready())
    }

    /// Get the endpoint path for this webhook.
    pub fn path(&self) -> &str {
        &self.spec.path
    }
}

/// List of Webhook resources.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct WebhookList {
    /// API version.
    #[serde(rename = "apiVersion")]
    pub api_version: String,

    /// Kind (always "WebhookList").
    pub kind: String,

    /// List metadata.
    pub metadata: ListMeta,

    /// List of Webhooks.
    pub items: Vec<Webhook>,
}

impl Default for WebhookList {
    fn default() -> Self {
        Self {
            api_version: API_VERSION.to_string(),
            kind: "WebhookList".to_string(),
            metadata: ListMeta::default(),
            items: Vec::new(),
        }
    }
}

impl WebhookList {
    /// Create a new WebhookList with the given items.
    pub fn new(items: Vec<Webhook>) -> Self {
        Self {
            items,
            ..Default::default()
        }
    }
}

/// Validation errors for Webhook resources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebhookValidationError {
    /// Webhook name is missing.
    MissingName,
    /// Path is required but missing.
    MissingPath,
    /// Path is invalid.
    InvalidPath(String),
    /// HMAC header is required when secret_ref is set.
    MissingHmacHeader,
    /// Job template is invalid.
    InvalidJobTemplate(String),
}

impl std::fmt::Display for WebhookValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingName => write!(f, "webhook name is required"),
            Self::MissingPath => write!(f, "path is required"),
            Self::InvalidPath(msg) => write!(f, "invalid path: {}", msg),
            Self::MissingHmacHeader => write!(f, "hmacHeader is required when secretRef is set"),
            Self::InvalidJobTemplate(msg) => write!(f, "invalid job template: {}", msg),
        }
    }
}

impl std::error::Error for WebhookValidationError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::pod::{ContainerSpec, PodSpec};
    use crate::nanocloud::k8s::statefulset::PodTemplateSpec;

    fn make_job_template() -> WebhookJobTemplate {
        WebhookJobTemplate {
            metadata: ObjectMeta {
                generate_name: Some("webhook-job-".to_string()),
                ..Default::default()
            },
            spec: JobSpec {
                template: PodTemplateSpec {
                    metadata: ObjectMeta::default(),
                    spec: PodSpec {
                        containers: vec![ContainerSpec {
                            name: "runner".to_string(),
                            image: Some("runner:latest".to_string()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                },
                ..Default::default()
            },
        }
    }

    #[test]
    fn webhook_spec_validation() {
        // Valid spec
        let spec = WebhookSpec::new("/webhooks/test", make_job_template());
        assert!(spec.validate().is_ok());

        // Missing path
        let spec = WebhookSpec {
            path: String::new(),
            job_template: make_job_template(),
            ..Default::default()
        };
        assert_eq!(spec.validate(), Err(WebhookValidationError::MissingPath));

        // Invalid path (no leading slash)
        let spec = WebhookSpec {
            path: "webhooks/test".to_string(),
            job_template: make_job_template(),
            ..Default::default()
        };
        assert!(matches!(
            spec.validate(),
            Err(WebhookValidationError::InvalidPath(_))
        ));
    }

    #[test]
    fn webhook_spec_hmac_validation() {
        // Secret ref without hmac header is invalid
        let spec = WebhookSpec {
            path: "/webhooks/test".to_string(),
            secret_ref: Some(SecretKeyRef::new("my-secret", "token")),
            hmac_header: None,
            job_template: make_job_template(),
            ..Default::default()
        };
        assert_eq!(
            spec.validate(),
            Err(WebhookValidationError::MissingHmacHeader)
        );

        // With both secret_ref and hmac_header is valid
        let spec = WebhookSpec::new("/webhooks/test", make_job_template())
            .with_secret(SecretKeyRef::new("my-secret", "token"), "X-Signature");
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn webhook_spec_empty_containers() {
        let spec = WebhookSpec {
            path: "/webhooks/test".to_string(),
            job_template: WebhookJobTemplate {
                metadata: ObjectMeta::default(),
                spec: JobSpec {
                    template: PodTemplateSpec {
                        metadata: ObjectMeta::default(),
                        spec: PodSpec::default(),
                    },
                    ..Default::default()
                },
            },
            ..Default::default()
        };
        assert!(matches!(
            spec.validate(),
            Err(WebhookValidationError::InvalidJobTemplate(_))
        ));
    }

    #[test]
    fn webhook_status_counters() {
        let mut status = WebhookStatus::default();

        status.record_trigger();
        assert_eq!(status.trigger_count, Some(1));

        status.record_success("job-abc", "2024-01-01T00:00:00Z");
        assert_eq!(status.success_count, Some(1));
        assert_eq!(status.last_job_name, Some("job-abc".to_string()));

        status.record_failure();
        assert_eq!(status.failure_count, Some(1));

        status.record_dedupe();
        assert_eq!(status.dedupe_count, Some(1));
    }

    #[test]
    fn webhook_ready_status() {
        let mut status = WebhookStatus::default();
        assert!(!status.is_ready());

        status.set_ready(true, None, None);
        assert!(status.is_ready());

        status.set_ready(false, Some("SecretNotFound"), Some("Secret not found"));
        assert!(!status.is_ready());
    }

    #[test]
    fn hmac_algorithm_properties() {
        assert_eq!(HmacAlgorithm::Sha256.digest_name(), "sha256");
        assert_eq!(HmacAlgorithm::Sha256.signature_prefix(), "sha256=");

        assert_eq!(HmacAlgorithm::Sha1.digest_name(), "sha1");
        assert_eq!(HmacAlgorithm::Sha1.signature_prefix(), "sha1=");

        assert_eq!(HmacAlgorithm::Sha512.digest_name(), "sha512");
        assert_eq!(HmacAlgorithm::Sha512.signature_prefix(), "sha512=");
    }

    #[test]
    fn secret_key_ref_namespace_resolution() {
        let sref = SecretKeyRef::new("my-secret", "token");
        assert_eq!(sref.resolved_namespace("default"), "default");

        let sref = SecretKeyRef::new("my-secret", "token").with_namespace("other");
        assert_eq!(sref.resolved_namespace("default"), "other");
    }

    #[test]
    fn webhook_serialization() {
        let webhook = Webhook::new(
            "my-webhook",
            WebhookSpec::new("/webhooks/test", make_job_template())
                .with_secret(
                    SecretKeyRef::new("my-secret", "token"),
                    "X-Hub-Signature-256",
                )
                .with_dedupe_key("$.headers['X-Request-ID']"),
        );

        let json = serde_json::to_string_pretty(&webhook).unwrap();
        let parsed: Webhook = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.name(), "my-webhook");
        assert_eq!(parsed.spec.path, "/webhooks/test");
        assert!(parsed.spec.secret_ref.is_some());
        assert_eq!(parsed.spec.dedupe_keys.len(), 1);
    }

    #[test]
    fn default_values() {
        let spec = WebhookSpec::default();
        assert_eq!(spec.effective_dedupe_ttl(), 3600);
        assert_eq!(spec.effective_max_body_size(), 1024 * 1024);
        assert_eq!(spec.effective_timeout(), 30);
        assert!(!spec.requires_hmac_validation());
    }
}
