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

//! Webhook trigger validation and Job creation.
//!
//! This module handles incoming webhook requests:
//!
//! - HMAC signature validation (shared secret verification)
//! - Deduplication via event ID/signature tracking
//! - Job creation from webhook templates
//! - Environment variable and argument mapping from payloads
//!
//! # Usage
//!
//! ```ignore
//! use nanocloud::edge::webhook_trigger::{WebhookTrigger, WebhookContext};
//!
//! let trigger = WebhookTrigger::new(dedupe_cache);
//!
//! // Validate and process a webhook
//! let context = WebhookContext {
//!     headers: request_headers,
//!     body: body_bytes,
//!     webhook: webhook_spec,
//!     secret: Some(shared_secret),
//! };
//!
//! match trigger.process(context).await {
//!     Ok(job) => { /* Job was created */ }
//!     Err(WebhookError::Duplicate) => { /* Deduplicated */ }
//!     Err(e) => { /* Handle error */ }
//! }
//! ```

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use chrono::{SecondsFormat, Utc};
use openssl::hash::MessageDigest;
use openssl::pkey::PKey;
use openssl::sign::Signer;
use serde_json::Value;

use crate::nanocloud::k8s::job::Job;
use crate::nanocloud::k8s::pod::ContainerEnvVar;
use crate::nanocloud::k8s::webhook::{HmacAlgorithm, Webhook};

/// Errors that can occur during webhook processing.
#[derive(Debug)]
pub enum WebhookError {
    /// HMAC signature validation failed.
    InvalidSignature(String),
    /// Required HMAC header is missing.
    MissingSignature,
    /// The webhook is a duplicate (already processed).
    Duplicate,
    /// Failed to parse the request body.
    InvalidBody(String),
    /// Failed to extract a required value from the payload.
    ExtractionFailed(String),
    /// The webhook configuration is invalid.
    InvalidConfig(String),
    /// Job creation failed.
    JobCreationFailed(String),
}

impl std::fmt::Display for WebhookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSignature(msg) => write!(f, "invalid signature: {}", msg),
            Self::MissingSignature => write!(f, "missing signature header"),
            Self::Duplicate => write!(f, "duplicate webhook (already processed)"),
            Self::InvalidBody(msg) => write!(f, "invalid request body: {}", msg),
            Self::ExtractionFailed(msg) => write!(f, "extraction failed: {}", msg),
            Self::InvalidConfig(msg) => write!(f, "invalid webhook configuration: {}", msg),
            Self::JobCreationFailed(msg) => write!(f, "job creation failed: {}", msg),
        }
    }
}

impl std::error::Error for WebhookError {}

/// Context for processing a webhook request.
pub struct WebhookContext<'a> {
    /// HTTP headers from the request.
    pub headers: &'a axum::http::HeaderMap,
    /// Raw request body.
    pub body: &'a [u8],
    /// The Webhook CRD being triggered.
    pub webhook: &'a Webhook,
    /// The shared secret for HMAC validation (if required).
    pub secret: Option<&'a [u8]>,
}

/// Entry in the deduplication cache.
struct DedupeEntry {
    /// Time when the entry was added.
    added_at: Instant,
    /// TTL for this entry.
    ttl: Duration,
}

impl DedupeEntry {
    fn is_expired(&self) -> bool {
        self.added_at.elapsed() > self.ttl
    }
}

/// Cache for deduplication.
pub struct DedupeCache {
    entries: RwLock<HashMap<String, DedupeEntry>>,
}

impl Default for DedupeCache {
    fn default() -> Self {
        Self::new()
    }
}

impl DedupeCache {
    /// Create a new empty cache.
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Check if a key exists in the cache (not expired).
    pub fn contains(&self, key: &str) -> bool {
        let entries = self.entries.read().expect("dedupe lock poisoned");
        if let Some(entry) = entries.get(key) {
            !entry.is_expired()
        } else {
            false
        }
    }

    /// Insert a key into the cache with the given TTL.
    /// Returns true if the key was newly inserted, false if it already existed.
    pub fn insert(&self, key: String, ttl: Duration) -> bool {
        let mut entries = self.entries.write().expect("dedupe lock poisoned");

        // Clean up expired entries occasionally
        if entries.len() > 1000 {
            entries.retain(|_, e| !e.is_expired());
        }

        if let Some(entry) = entries.get(&key) {
            if !entry.is_expired() {
                return false;
            }
        }

        entries.insert(
            key,
            DedupeEntry {
                added_at: Instant::now(),
                ttl,
            },
        );
        true
    }

    /// Remove expired entries from the cache.
    pub fn cleanup(&self) {
        let mut entries = self.entries.write().expect("dedupe lock poisoned");
        entries.retain(|_, e| !e.is_expired());
    }

    /// Get the current number of entries in the cache.
    pub fn len(&self) -> usize {
        self.entries.read().expect("dedupe lock poisoned").len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Webhook trigger processor.
pub struct WebhookTrigger {
    dedupe_cache: DedupeCache,
}

impl Default for WebhookTrigger {
    fn default() -> Self {
        Self::new()
    }
}

impl WebhookTrigger {
    /// Create a new WebhookTrigger with its own deduplication cache.
    pub fn new() -> Self {
        Self {
            dedupe_cache: DedupeCache::new(),
        }
    }

    /// Create a WebhookTrigger with a shared deduplication cache.
    pub fn with_cache(cache: DedupeCache) -> Self {
        Self {
            dedupe_cache: cache,
        }
    }

    /// Process a webhook request.
    ///
    /// This function:
    /// 1. Validates the HMAC signature (if configured)
    /// 2. Checks for duplicates using dedupe keys
    /// 3. Creates a Job from the webhook template
    ///
    /// Returns the created Job on success.
    pub fn process(&self, context: WebhookContext<'_>) -> Result<Job, WebhookError> {
        let spec = &context.webhook.spec;

        // Step 1: Validate HMAC signature if required
        if spec.requires_hmac_validation() {
            self.validate_signature(&context)?;
        }

        // Step 2: Check for duplicates
        if !spec.dedupe_keys.is_empty() {
            self.check_dedupe(&context)?;
        }

        // Step 3: Create the job
        self.create_job(&context)
    }

    /// Validate the HMAC signature.
    fn validate_signature(&self, context: &WebhookContext<'_>) -> Result<(), WebhookError> {
        let spec = &context.webhook.spec;

        // Get the secret
        let secret = context
            .secret
            .ok_or_else(|| WebhookError::InvalidConfig("secret not provided".to_string()))?;

        // Get the signature header
        let header_name = spec
            .hmac_header
            .as_ref()
            .ok_or(WebhookError::MissingSignature)?;

        let signature_header = context
            .headers
            .get(header_name.as_str())
            .and_then(|v| v.to_str().ok())
            .ok_or(WebhookError::MissingSignature)?;

        // Compute the expected signature
        let expected = compute_hmac(context.body, secret, &spec.hmac_algorithm)?;

        // Compare signatures
        let prefix = spec.hmac_algorithm.signature_prefix();
        let provided_signature = signature_header
            .strip_prefix(prefix)
            .unwrap_or(signature_header);

        if !constant_time_compare(&expected, provided_signature) {
            return Err(WebhookError::InvalidSignature(
                "HMAC signature mismatch".to_string(),
            ));
        }

        Ok(())
    }

    /// Check for duplicate webhook calls.
    fn check_dedupe(&self, context: &WebhookContext<'_>) -> Result<(), WebhookError> {
        let spec = &context.webhook.spec;
        let ttl = Duration::from_secs(spec.effective_dedupe_ttl());

        // Parse body as JSON for JSONPath extraction
        let body_json: Option<Value> = if !context.body.is_empty() {
            serde_json::from_slice(context.body).ok()
        } else {
            None
        };

        // Extract dedupe keys and check cache
        for key_expr in &spec.dedupe_keys {
            if let Some(value) = extract_value(key_expr, context.headers, body_json.as_ref()) {
                // Build a unique cache key
                let cache_key = format!(
                    "{}:{}:{}:{}",
                    context.webhook.namespace(),
                    context.webhook.name(),
                    key_expr,
                    value
                );

                if !self.dedupe_cache.insert(cache_key, ttl) {
                    return Err(WebhookError::Duplicate);
                }
            }
        }

        Ok(())
    }

    /// Create a Job from the webhook template.
    fn create_job(&self, context: &WebhookContext<'_>) -> Result<Job, WebhookError> {
        let spec = &context.webhook.spec;
        let template = &spec.job_template;

        // Parse body as JSON for value extraction
        let body_json: Option<Value> = if !context.body.is_empty() {
            serde_json::from_slice(context.body).ok()
        } else {
            None
        };

        // Build job metadata
        let mut job_metadata = template.metadata.clone();
        job_metadata.namespace = Some(context.webhook.namespace().to_string());

        // Generate a unique name if using generateName
        if let Some(ref prefix) = job_metadata.generate_name {
            let suffix = generate_suffix();
            job_metadata.name = Some(format!("{}{}", prefix, suffix));
            job_metadata.generate_name = None;
        }

        // Add timestamp
        job_metadata.creation_timestamp =
            Some(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true));

        // Add webhook labels
        job_metadata.labels.insert(
            "nanocloud.io/webhook".to_string(),
            context.webhook.name().to_string(),
        );

        // Clone the job spec and modify containers
        let mut job_spec = template.spec.clone();

        // Apply env mappings to containers
        if !spec.env_mappings.is_empty() || !spec.arg_mappings.is_empty() {
            let containers = &mut job_spec.template.spec.containers;
            for container in containers.iter_mut() {
                // Add environment variables from mappings
                for (env_name, expr) in &spec.env_mappings {
                    if let Some(value) = extract_value(expr, context.headers, body_json.as_ref()) {
                        container.env.push(ContainerEnvVar {
                            name: env_name.clone(),
                            value: Some(value),
                            value_from: None,
                        });
                    }
                }

                // Add arguments from mappings
                for expr in &spec.arg_mappings {
                    if let Some(value) = extract_value(expr, context.headers, body_json.as_ref()) {
                        container.args.push(value);
                    }
                }
            }
        }

        // Add WEBHOOK_BODY env var with the raw body
        if !context.body.is_empty() {
            let body_str = String::from_utf8_lossy(context.body).to_string();
            for container in job_spec.template.spec.containers.iter_mut() {
                container.env.push(ContainerEnvVar {
                    name: "WEBHOOK_BODY".to_string(),
                    value: Some(body_str.clone()),
                    value_from: None,
                });
            }
        }

        // Create the job
        let job = Job::new(job_metadata, job_spec);

        Ok(job)
    }

    /// Get a reference to the deduplication cache.
    pub fn cache(&self) -> &DedupeCache {
        &self.dedupe_cache
    }
}

/// Compute HMAC signature for a payload.
fn compute_hmac(
    payload: &[u8],
    secret: &[u8],
    algorithm: &HmacAlgorithm,
) -> Result<String, WebhookError> {
    let digest = match algorithm {
        HmacAlgorithm::Sha256 => MessageDigest::sha256(),
        HmacAlgorithm::Sha1 => MessageDigest::sha1(),
        HmacAlgorithm::Sha512 => MessageDigest::sha512(),
    };

    let pkey = PKey::hmac(secret)
        .map_err(|e| WebhookError::InvalidSignature(format!("failed to create HMAC key: {}", e)))?;

    let mut signer = Signer::new(digest, &pkey)
        .map_err(|e| WebhookError::InvalidSignature(format!("failed to create signer: {}", e)))?;

    signer
        .update(payload)
        .map_err(|e| WebhookError::InvalidSignature(format!("failed to update HMAC: {}", e)))?;

    let signature = signer
        .sign_to_vec()
        .map_err(|e| WebhookError::InvalidSignature(format!("failed to sign: {}", e)))?;

    Ok(hex::encode(signature))
}

/// Constant-time comparison to prevent timing attacks.
fn constant_time_compare(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut result = 0u8;
    for (x, y) in a.bytes().zip(b.bytes()) {
        result |= x ^ y;
    }
    result == 0
}

/// Extract a value using a simple path expression.
///
/// Supports:
/// - `$.headers['Header-Name']` - Extract from headers
/// - `$.body.field.subfield` - Extract from JSON body
/// - `$.body['field']` - Extract from JSON body with bracket notation
fn extract_value(
    expr: &str,
    headers: &axum::http::HeaderMap,
    body: Option<&Value>,
) -> Option<String> {
    let expr = expr.trim();

    // Handle header extraction
    if expr.starts_with("$.headers[") {
        // Extract header name from $.headers['Header-Name'] or $.headers["Header-Name"]
        let start = expr.find('[')? + 1;
        let end = expr.rfind(']')?;
        let header_part = &expr[start..end];

        // Remove quotes
        let header_name = header_part.trim_matches(|c| c == '\'' || c == '"');

        return headers
            .get(header_name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
    }

    // Handle body extraction
    if let Some(path) = expr.strip_prefix("$.body") {
        let body = body?;

        if path.is_empty() {
            return Some(body.to_string());
        }

        // Parse the path and navigate the JSON
        let mut current = body;
        let path = path.trim_start_matches('.');

        for part in split_json_path(path) {
            match current {
                Value::Object(map) => {
                    current = map.get(&part)?;
                }
                Value::Array(arr) => {
                    let index: usize = part.parse().ok()?;
                    current = arr.get(index)?;
                }
                _ => return None,
            }
        }

        // Convert to string
        return match current {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(b.to_string()),
            Value::Null => Some("null".to_string()),
            _ => Some(current.to_string()),
        };
    }

    None
}

/// Split a JSON path into parts, handling both dot and bracket notation.
fn split_json_path(path: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_bracket = false;

    for c in path.chars() {
        match c {
            '[' => {
                if !current.is_empty() {
                    parts.push(current.clone());
                    current.clear();
                }
                in_bracket = true;
            }
            ']' => {
                if !current.is_empty() {
                    // Remove quotes if present
                    let trimmed = current.trim_matches(|c| c == '\'' || c == '"');
                    parts.push(trimmed.to_string());
                    current.clear();
                }
                in_bracket = false;
            }
            '.' if !in_bracket => {
                if !current.is_empty() {
                    parts.push(current.clone());
                    current.clear();
                }
            }
            _ => {
                current.push(c);
            }
        }
    }

    if !current.is_empty() {
        parts.push(current);
    }

    parts
}

/// Generate a random suffix for job names.
fn generate_suffix() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let chars: Vec<char> = (0..5)
        .map(|_| {
            let idx = rng.gen_range(0..36);
            if idx < 10 {
                (b'0' + idx) as char
            } else {
                (b'a' + idx - 10) as char
            }
        })
        .collect();
    chars.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue};

    #[test]
    fn dedupe_cache_basic() {
        let cache = DedupeCache::new();

        assert!(cache.insert("key1".to_string(), Duration::from_secs(60)));
        assert!(!cache.insert("key1".to_string(), Duration::from_secs(60)));
        assert!(cache.contains("key1"));
        assert!(!cache.contains("key2"));
    }

    #[test]
    fn dedupe_cache_expiry() {
        let cache = DedupeCache::new();

        // Insert with zero TTL (expires immediately)
        assert!(cache.insert("key1".to_string(), Duration::from_secs(0)));

        // After a small delay, the entry should be expired
        std::thread::sleep(Duration::from_millis(10));
        assert!(!cache.contains("key1"));

        // Can insert again after expiry
        assert!(cache.insert("key1".to_string(), Duration::from_secs(60)));
    }

    #[test]
    fn hmac_computation() {
        let payload = b"test payload";
        let secret = b"test-secret";

        let sha256 = compute_hmac(payload, secret, &HmacAlgorithm::Sha256).unwrap();
        assert!(!sha256.is_empty());
        assert_eq!(sha256.len(), 64); // SHA256 produces 32 bytes = 64 hex chars

        let sha1 = compute_hmac(payload, secret, &HmacAlgorithm::Sha1).unwrap();
        assert_eq!(sha1.len(), 40); // SHA1 produces 20 bytes = 40 hex chars

        let sha512 = compute_hmac(payload, secret, &HmacAlgorithm::Sha512).unwrap();
        assert_eq!(sha512.len(), 128); // SHA512 produces 64 bytes = 128 hex chars
    }

    #[test]
    fn constant_time_compare_works() {
        assert!(constant_time_compare("abc", "abc"));
        assert!(!constant_time_compare("abc", "abd"));
        assert!(!constant_time_compare("abc", "ab"));
        assert!(!constant_time_compare("ab", "abc"));
    }

    #[test]
    fn extract_header_value() {
        let mut headers = HeaderMap::new();
        headers.insert("X-Request-ID", HeaderValue::from_static("12345"));
        headers.insert("Content-Type", HeaderValue::from_static("application/json"));

        assert_eq!(
            extract_value("$.headers['X-Request-ID']", &headers, None),
            Some("12345".to_string())
        );
        assert_eq!(
            extract_value("$.headers[\"Content-Type\"]", &headers, None),
            Some("application/json".to_string())
        );
        assert_eq!(extract_value("$.headers['Missing']", &headers, None), None);
    }

    #[test]
    fn extract_body_value() {
        let headers = HeaderMap::new();
        let body: Value = serde_json::json!({
            "id": "abc123",
            "nested": {
                "value": 42
            },
            "array": [1, 2, 3]
        });

        assert_eq!(
            extract_value("$.body.id", &headers, Some(&body)),
            Some("abc123".to_string())
        );
        assert_eq!(
            extract_value("$.body.nested.value", &headers, Some(&body)),
            Some("42".to_string())
        );
        assert_eq!(
            extract_value("$.body['nested']['value']", &headers, Some(&body)),
            Some("42".to_string())
        );
        assert_eq!(
            extract_value("$.body.array[0]", &headers, Some(&body)),
            Some("1".to_string())
        );
        assert_eq!(extract_value("$.body.missing", &headers, Some(&body)), None);
    }

    #[test]
    fn split_json_path_parsing() {
        assert_eq!(split_json_path("field.subfield"), vec!["field", "subfield"]);
        assert_eq!(
            split_json_path("field['subfield']"),
            vec!["field", "subfield"]
        );
        assert_eq!(split_json_path("array[0]"), vec!["array", "0"]);
        assert_eq!(
            split_json_path("nested.array[0].value"),
            vec!["nested", "array", "0", "value"]
        );
    }

    #[test]
    fn generate_suffix_format() {
        let suffix = generate_suffix();
        assert_eq!(suffix.len(), 5);
        assert!(suffix.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn webhook_error_display() {
        let err = WebhookError::Duplicate;
        assert_eq!(err.to_string(), "duplicate webhook (already processed)");

        let err = WebhookError::InvalidSignature("mismatch".to_string());
        assert_eq!(err.to_string(), "invalid signature: mismatch");
    }
}
