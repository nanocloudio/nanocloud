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

//! End-to-end tests for Webhook resources and edge server webhook handling.
//!
//! Tests verify:
//! - Webhook persistence and retrieval
//! - HMAC validation configuration
//! - Deduplication key handling
//! - Job template configuration
//! - Controller reconciliation

use nanocloud::nanocloud::k8s::job::JobSpec;
use nanocloud::nanocloud::k8s::pod::{ContainerSpec, ObjectMeta, PodSpec};
use nanocloud::nanocloud::k8s::statefulset::PodTemplateSpec;
use nanocloud::nanocloud::k8s::store::{
    delete_webhook, get_webhook, list_webhooks, list_webhooks_for, save_webhook,
};
use nanocloud::nanocloud::k8s::webhook::{
    HmacAlgorithm, SecretKeyRef, Webhook, WebhookJobTemplate, WebhookSpec,
};
use nanocloud::nanocloud::test_support::keyspace_lock;
use std::env;
use std::fs;
use std::sync::MutexGuard;
use tempfile::TempDir;

struct TestEnv {
    _dir: TempDir,
    _lock: MutexGuard<'static, ()>,
    keyspace_previous: Option<String>,
    lock_previous: Option<String>,
}

impl TestEnv {
    fn new() -> Self {
        let lock = keyspace_lock().lock();
        let dir = tempfile::tempdir().expect("tempdir");
        let keyspace_previous = env::var("NANOCLOUD_KEYSPACE").ok();
        env::set_var("NANOCLOUD_KEYSPACE", dir.path());

        let lock_previous = env::var("NANOCLOUD_LOCK_FILE").ok();
        let lock_path = dir.path().join("nanocloud.lock");
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).expect("lock dir");
        }
        fs::File::create(&lock_path).expect("lock file");
        env::set_var("NANOCLOUD_LOCK_FILE", lock_path.to_string_lossy().as_ref());

        Self {
            _dir: dir,
            _lock: lock,
            keyspace_previous,
            lock_previous,
        }
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        if let Some(previous) = self.keyspace_previous.as_ref() {
            env::set_var("NANOCLOUD_KEYSPACE", previous);
        } else {
            env::remove_var("NANOCLOUD_KEYSPACE");
        }

        if let Some(previous) = self.lock_previous.as_ref() {
            env::set_var("NANOCLOUD_LOCK_FILE", previous);
        } else {
            env::remove_var("NANOCLOUD_LOCK_FILE");
        }
    }
}

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
fn webhook_crud_operations() {
    let _env = TestEnv::new();

    // Create a webhook
    let webhook = Webhook::new(
        "github-deploy",
        WebhookSpec::new("/webhooks/github/deploy", make_job_template())
            .with_secret(
                SecretKeyRef::new("github-secret", "token"),
                "X-Hub-Signature-256",
            )
            .with_hmac_algorithm(HmacAlgorithm::Sha256)
            .with_dedupe_key("$.headers['X-GitHub-Delivery']"),
    );

    // Save the webhook
    let saved = save_webhook(Some("default"), "github-deploy", webhook).expect("save webhook");
    assert!(saved.metadata.resource_version.is_some());
    assert_eq!(saved.name(), "github-deploy");

    // Read the webhook
    let loaded = get_webhook(Some("default"), "github-deploy")
        .expect("get webhook")
        .expect("webhook exists");
    assert_eq!(loaded.spec.path, "/webhooks/github/deploy");
    assert!(loaded.spec.secret_ref.is_some());
    assert_eq!(
        loaded.spec.hmac_header,
        Some("X-Hub-Signature-256".to_string())
    );
    assert_eq!(loaded.spec.dedupe_keys.len(), 1);

    // Update the webhook
    let mut updated = loaded.clone();
    updated.spec.timeout_seconds = Some(60);
    let saved_again =
        save_webhook(Some("default"), "github-deploy", updated).expect("update webhook");

    // Verify resource version was bumped
    let original_rv: i64 = saved
        .metadata
        .resource_version
        .as_ref()
        .unwrap()
        .parse()
        .unwrap();
    let updated_rv: i64 = saved_again
        .metadata
        .resource_version
        .as_ref()
        .unwrap()
        .parse()
        .unwrap();
    assert!(updated_rv > original_rv);

    // Delete the webhook
    let deleted = delete_webhook(Some("default"), "github-deploy").expect("delete webhook");
    assert!(deleted);

    // Verify deletion
    let gone = get_webhook(Some("default"), "github-deploy").expect("get deleted webhook");
    assert!(gone.is_none());
}

#[test]
fn webhook_list_operations() {
    let _env = TestEnv::new();

    // Create webhooks in different namespaces
    let webhooks = vec![
        ("default", "webhook-1", "/hooks/one"),
        ("default", "webhook-2", "/hooks/two"),
        ("production", "webhook-3", "/hooks/three"),
        ("staging", "webhook-4", "/hooks/four"),
    ];

    for (ns, name, path) in &webhooks {
        let webhook = Webhook::new(*name, WebhookSpec::new(*path, make_job_template()));
        save_webhook(Some(ns), name, webhook).expect("save webhook");
    }

    // List all webhooks
    let all = list_webhooks().expect("list all webhooks");
    assert_eq!(all.len(), 4);

    // List webhooks by namespace
    let default_webhooks = list_webhooks_for(Some("default")).expect("list default webhooks");
    assert_eq!(default_webhooks.len(), 2);

    let prod_webhooks = list_webhooks_for(Some("production")).expect("list production webhooks");
    assert_eq!(prod_webhooks.len(), 1);
    assert_eq!(prod_webhooks[0].name, "webhook-3");
}

#[test]
fn webhook_hmac_algorithms() {
    // SHA-256 (default)
    let algo = HmacAlgorithm::Sha256;
    assert_eq!(algo.digest_name(), "sha256");
    assert_eq!(algo.signature_prefix(), "sha256=");

    // SHA-1 (legacy)
    let algo = HmacAlgorithm::Sha1;
    assert_eq!(algo.digest_name(), "sha1");
    assert_eq!(algo.signature_prefix(), "sha1=");

    // SHA-512
    let algo = HmacAlgorithm::Sha512;
    assert_eq!(algo.digest_name(), "sha512");
    assert_eq!(algo.signature_prefix(), "sha512=");

    // Default is SHA-256
    let default_algo = HmacAlgorithm::default();
    assert_eq!(default_algo, HmacAlgorithm::Sha256);
}

#[test]
fn webhook_validation_errors() {
    // Missing path
    let mut webhook = Webhook::new(
        "invalid",
        WebhookSpec {
            path: String::new(),
            job_template: make_job_template(),
            ..Default::default()
        },
    );
    assert!(webhook.validate().is_err());

    // Invalid path (no leading slash)
    webhook = Webhook::new(
        "invalid",
        WebhookSpec {
            path: "webhooks/test".to_string(),
            job_template: make_job_template(),
            ..Default::default()
        },
    );
    assert!(webhook.validate().is_err());

    // Secret ref without HMAC header
    webhook = Webhook::new(
        "invalid",
        WebhookSpec {
            path: "/webhooks/test".to_string(),
            secret_ref: Some(SecretKeyRef::new("secret", "key")),
            hmac_header: None,
            job_template: make_job_template(),
            ..Default::default()
        },
    );
    assert!(webhook.validate().is_err());

    // Empty job template containers
    webhook = Webhook::new(
        "invalid",
        WebhookSpec {
            path: "/webhooks/test".to_string(),
            job_template: WebhookJobTemplate {
                metadata: ObjectMeta::default(),
                spec: JobSpec {
                    template: PodTemplateSpec {
                        metadata: ObjectMeta::default(),
                        spec: PodSpec::default(), // No containers
                    },
                    ..Default::default()
                },
            },
            ..Default::default()
        },
    );
    assert!(webhook.validate().is_err());

    // Valid webhook
    webhook = Webhook::new(
        "valid",
        WebhookSpec::new("/webhooks/test", make_job_template()),
    );
    assert!(webhook.validate().is_ok());
}

#[test]
fn webhook_secret_ref_namespace_resolution() {
    // SecretKeyRef without namespace uses webhook's namespace
    let sref = SecretKeyRef::new("my-secret", "token");
    assert_eq!(sref.resolved_namespace("default"), "default");
    assert_eq!(sref.resolved_namespace("production"), "production");

    // SecretKeyRef with explicit namespace overrides
    let sref_with_ns = SecretKeyRef::new("my-secret", "token").with_namespace("shared-secrets");
    assert_eq!(sref_with_ns.resolved_namespace("default"), "shared-secrets");
    assert_eq!(
        sref_with_ns.resolved_namespace("production"),
        "shared-secrets"
    );
}

#[test]
fn webhook_default_values() {
    let spec = WebhookSpec::new("/webhooks/test", make_job_template());

    // Deduplication TTL defaults to 1 hour
    assert_eq!(spec.effective_dedupe_ttl(), 3600);

    // Max body size defaults to 1MB
    assert_eq!(spec.effective_max_body_size(), 1024 * 1024);

    // Timeout defaults to 30 seconds
    assert_eq!(spec.effective_timeout(), 30);

    // HMAC validation not required by default
    assert!(!spec.requires_hmac_validation());
}

#[test]
fn webhook_hmac_validation_config() {
    // Without secret ref - no validation required
    let spec = WebhookSpec::new("/webhooks/test", make_job_template());
    assert!(!spec.requires_hmac_validation());

    // With secret ref and HMAC header - validation required
    let spec = WebhookSpec::new("/webhooks/test", make_job_template())
        .with_secret(SecretKeyRef::new("secret", "key"), "X-Signature");
    assert!(spec.requires_hmac_validation());
}

#[test]
fn webhook_deduplication_config() {
    // Multiple dedupe keys
    let spec = WebhookSpec::new("/webhooks/test", make_job_template())
        .with_dedupe_key("$.headers['X-Request-ID']")
        .with_dedupe_key("$.body.id")
        .with_dedupe_ttl(7200);

    assert_eq!(spec.dedupe_keys.len(), 2);
    assert_eq!(spec.dedupe_keys[0], "$.headers['X-Request-ID']");
    assert_eq!(spec.dedupe_keys[1], "$.body.id");
    assert_eq!(spec.effective_dedupe_ttl(), 7200);
}

#[test]
fn webhook_env_mappings() {
    let spec = WebhookSpec::new("/webhooks/test", make_job_template())
        .with_env_mapping("COMMIT_SHA", "$.body.after")
        .with_env_mapping("REPO_NAME", "$.body.repository.full_name");

    assert_eq!(spec.env_mappings.len(), 2);
    assert_eq!(
        spec.env_mappings.get("COMMIT_SHA"),
        Some(&"$.body.after".to_string())
    );
    assert_eq!(
        spec.env_mappings.get("REPO_NAME"),
        Some(&"$.body.repository.full_name".to_string())
    );
}

#[test]
fn webhook_status_tracking() {
    let mut status = nanocloud::nanocloud::k8s::webhook::WebhookStatus::default();

    // Initially not ready
    assert!(!status.is_ready());
    assert_eq!(status.trigger_count, None);
    assert_eq!(status.success_count, None);

    // Record triggers
    status.record_trigger();
    status.record_trigger();
    assert_eq!(status.trigger_count, Some(2));

    // Record success
    status.record_success("job-abc123", "2024-01-15T10:30:00Z");
    assert_eq!(status.success_count, Some(1));
    assert_eq!(status.last_job_name, Some("job-abc123".to_string()));
    assert_eq!(
        status.last_trigger_time,
        Some("2024-01-15T10:30:00Z".to_string())
    );

    // Record failure
    status.record_failure();
    status.record_failure();
    assert_eq!(status.failure_count, Some(2));

    // Record dedupe
    status.record_dedupe();
    assert_eq!(status.dedupe_count, Some(1));

    // Set ready
    status.set_ready(true, None, None);
    assert!(status.is_ready());
}

#[test]
fn webhook_serialization_roundtrip() {
    let webhook = Webhook::new(
        "full-featured",
        WebhookSpec::new("/webhooks/github/push", make_job_template())
            .with_secret(
                SecretKeyRef::new("github-secret", "token").with_namespace("secrets"),
                "X-Hub-Signature-256",
            )
            .with_hmac_algorithm(HmacAlgorithm::Sha256)
            .with_dedupe_key("$.headers['X-GitHub-Delivery']")
            .with_dedupe_ttl(7200)
            .with_env_mapping("COMMIT_SHA", "$.body.after"),
    );

    // Serialize to JSON
    let json = serde_json::to_string_pretty(&webhook).expect("serialize webhook");

    // Deserialize back
    let parsed: Webhook = serde_json::from_str(&json).expect("deserialize webhook");

    assert_eq!(parsed.name(), "full-featured");
    assert_eq!(parsed.spec.path, "/webhooks/github/push");
    assert!(parsed.spec.secret_ref.is_some());
    let secret_ref = parsed.spec.secret_ref.as_ref().unwrap();
    assert_eq!(secret_ref.name, "github-secret");
    assert_eq!(secret_ref.namespace, Some("secrets".to_string()));
    assert_eq!(
        parsed.spec.hmac_header,
        Some("X-Hub-Signature-256".to_string())
    );
    assert_eq!(parsed.spec.hmac_algorithm, HmacAlgorithm::Sha256);
    assert_eq!(
        parsed.spec.dedupe_keys,
        vec!["$.headers['X-GitHub-Delivery']"]
    );
    assert_eq!(parsed.spec.dedupe_ttl_seconds, Some(7200));
}

#[test]
fn webhook_resource_version_management() {
    let _env = TestEnv::new();

    // Create new webhook should set resource version to 1
    let webhook = Webhook::new(
        "versioned",
        WebhookSpec::new("/webhooks/test", make_job_template()),
    );
    let saved = save_webhook(Some("default"), "versioned", webhook).expect("save");
    assert_eq!(saved.metadata.resource_version, Some("1".to_string()));

    // Update should increment resource version
    let mut updated = saved.clone();
    updated.spec.timeout_seconds = Some(60);
    let saved2 = save_webhook(Some("default"), "versioned", updated).expect("update");
    assert_eq!(saved2.metadata.resource_version, Some("2".to_string()));

    // Another update should increment again
    let mut updated2 = saved2.clone();
    updated2.spec.timeout_seconds = Some(120);
    let saved3 = save_webhook(Some("default"), "versioned", updated2).expect("update again");
    assert_eq!(saved3.metadata.resource_version, Some("3".to_string()));
}

#[test]
fn webhook_job_template_validation() {
    // Template with generate_name
    let mut template = make_job_template();
    template.metadata.generate_name = Some("deploy-".to_string());

    let spec = WebhookSpec::new("/webhooks/test", template);
    let webhook = Webhook::new("test", spec);
    assert!(webhook.validate().is_ok());

    // Template container should have name
    let loaded = webhook.spec.job_template.spec.template.spec.containers[0]
        .name
        .clone();
    assert_eq!(loaded, "runner");
}
