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

//! Key rotation support for secrets.
//!
//! This module provides helpers to safely rotate encryption keys for stored secrets.
//!
//! # Key Rotation Process
//!
//! Key rotation involves re-encrypting all secrets with a new data encryption key (DEK).
//! The process is designed to be:
//!
//! 1. **Safe**: Secrets remain accessible during rotation; failures don't corrupt data
//! 2. **Atomic per-secret**: Each secret is re-encrypted atomically
//! 3. **Resumable**: Partial rotations can be completed on retry
//! 4. **Observable**: Progress is reported via callbacks
//!
//! # Usage
//!
//! ```ignore
//! use nanocloud::secrets::{KeyspaceSecretStore, rotation::RotationConfig};
//!
//! let store = KeyspaceSecretStore::new();
//! let config = RotationConfig::default();
//! let result = rotation::rotate_all_secrets(&store, config)?;
//! println!("Rotated {} secrets, {} failed", result.success_count, result.failure_count);
//! ```
//!
//! # Security Considerations
//!
//! - Plaintext secret data is held in memory only during re-encryption
//! - Old encrypted data is atomically replaced with new encrypted data
//! - Failures leave the original encrypted data intact
//! - No sensitive data is logged during rotation

use std::error::Error;

use crate::nanocloud::logger::log_info;
use crate::nanocloud::secrets::error::SecretError;
use crate::nanocloud::secrets::KeyspaceSecretStore;

const ROTATION_COMPONENT: &str = "secrets.rotation";

/// Configuration for key rotation operations.
#[derive(Clone)]
pub struct RotationConfig {
    /// Whether to continue rotating remaining secrets after a failure.
    /// Default: true
    pub continue_on_error: bool,

    /// Optional callback invoked before each secret is rotated.
    /// Receives (namespace, name). Return false to skip this secret.
    pub pre_rotate_filter: Option<fn(&str, &str) -> bool>,

    /// Optional callback invoked after each secret is rotated.
    /// Receives (namespace, name, success).
    pub post_rotate_callback: Option<fn(&str, &str, bool)>,
}

impl Default for RotationConfig {
    fn default() -> Self {
        Self {
            continue_on_error: true,
            pre_rotate_filter: None,
            post_rotate_callback: None,
        }
    }
}

/// Result of a key rotation operation.
#[derive(Debug, Default)]
pub struct RotationResult {
    /// Number of secrets successfully rotated.
    pub success_count: usize,

    /// Number of secrets that failed to rotate.
    pub failure_count: usize,

    /// Number of secrets skipped (filtered out).
    pub skipped_count: usize,

    /// Details of failures (namespace/name, error message).
    /// Limited to avoid memory issues with large failure sets.
    pub failures: Vec<(String, String, String)>,
}

impl RotationResult {
    /// Maximum number of failure details to store.
    const MAX_FAILURE_DETAILS: usize = 100;

    fn record_success(&mut self) {
        self.success_count += 1;
    }

    fn record_failure(&mut self, namespace: &str, name: &str, error: &str) {
        self.failure_count += 1;
        if self.failures.len() < Self::MAX_FAILURE_DETAILS {
            self.failures.push((
                namespace.to_string(),
                name.to_string(),
                error.to_string(),
            ));
        }
    }

    fn record_skip(&mut self) {
        self.skipped_count += 1;
    }

    /// Returns true if all secrets were successfully rotated (no failures).
    pub fn is_complete(&self) -> bool {
        self.failure_count == 0
    }

    /// Returns the total number of secrets processed.
    pub fn total_processed(&self) -> usize {
        self.success_count + self.failure_count + self.skipped_count
    }
}

/// Rotates a single secret by re-encrypting it with a new data key.
///
/// This function:
/// 1. Reads and decrypts the secret
/// 2. Re-encrypts with a fresh data key
/// 3. Atomically writes the new encrypted data
///
/// The operation is atomic: either the secret is fully rotated or left unchanged.
///
/// # Arguments
///
/// * `store` - The secret store to operate on
/// * `namespace` - The secret's namespace
/// * `name` - The secret's name
///
/// # Returns
///
/// `Ok(())` if rotation succeeded, `Err` if it failed (original data unchanged).
pub fn rotate_secret(
    store: &KeyspaceSecretStore,
    namespace: &str,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Read and decrypt the secret
    let stored = store.get(namespace, name)?
        .ok_or_else(|| SecretError::NotFound {
            namespace: namespace.to_string(),
            name: name.to_string(),
        })?;

    // Re-encrypt by writing with a new data key
    // The put operation generates a fresh data key and writes atomically
    store.put(stored.secret)?;

    log_info(
        ROTATION_COMPONENT,
        "Secret key rotated successfully",
        &[("namespace", namespace), ("name", name)],
    );

    Ok(())
}

/// Rotates all secrets in the store with new data keys.
///
/// This function iterates through all secrets and re-encrypts each one
/// with a fresh data key. The operation is atomic per-secret.
///
/// # Arguments
///
/// * `store` - The secret store to operate on
/// * `config` - Configuration controlling rotation behavior
///
/// # Returns
///
/// A `RotationResult` summarizing the operation outcome.
///
/// # Example
///
/// ```ignore
/// let store = KeyspaceSecretStore::new();
/// let result = rotate_all_secrets(&store, RotationConfig::default())?;
/// if result.is_complete() {
///     println!("All {} secrets rotated", result.success_count);
/// } else {
///     println!("Rotation incomplete: {} failures", result.failure_count);
/// }
/// ```
pub fn rotate_all_secrets(
    store: &KeyspaceSecretStore,
    config: RotationConfig,
) -> Result<RotationResult, Box<dyn Error + Send + Sync>> {
    let mut result = RotationResult::default();

    // List all secrets
    let secrets = store.list(None)?;
    let total = secrets.len();

    log_info(
        ROTATION_COMPONENT,
        "Starting key rotation",
        &[("total_secrets", &total.to_string())],
    );

    for stored in secrets {
        let namespace = &stored.secret.namespace;
        let name = &stored.secret.name;

        // Check filter
        if let Some(filter) = config.pre_rotate_filter {
            if !filter(namespace, name) {
                result.record_skip();
                continue;
            }
        }

        // Attempt rotation
        let rotation_result = rotate_secret(store, namespace, name);

        // Record result
        let success = rotation_result.is_ok();
        if success {
            result.record_success();
        } else {
            let error_msg = rotation_result
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default();
            result.record_failure(namespace, name, &error_msg);

            if !config.continue_on_error {
                log_info(
                    ROTATION_COMPONENT,
                    "Key rotation stopped due to error",
                    &[
                        ("namespace", namespace),
                        ("name", name),
                        ("processed", &result.total_processed().to_string()),
                    ],
                );
                break;
            }
        }

        // Invoke callback
        if let Some(callback) = config.post_rotate_callback {
            callback(namespace, name, success);
        }
    }

    log_info(
        ROTATION_COMPONENT,
        "Key rotation completed",
        &[
            ("success", &result.success_count.to_string()),
            ("failed", &result.failure_count.to_string()),
            ("skipped", &result.skipped_count.to_string()),
        ],
    );

    Ok(result)
}

/// Rotates all secrets in a specific namespace with new data keys.
///
/// This function is similar to `rotate_all_secrets` but limits rotation
/// to secrets within the specified namespace.
///
/// # Arguments
///
/// * `store` - The secret store to operate on
/// * `namespace` - The namespace to rotate secrets in
/// * `config` - Configuration controlling rotation behavior
///
/// # Returns
///
/// A `RotationResult` summarizing the operation outcome.
pub fn rotate_namespace_secrets(
    store: &KeyspaceSecretStore,
    namespace: &str,
    config: RotationConfig,
) -> Result<RotationResult, Box<dyn Error + Send + Sync>> {
    let mut result = RotationResult::default();

    // List secrets in the specified namespace
    let secrets = store.list(Some(namespace))?;
    let total = secrets.len();

    log_info(
        ROTATION_COMPONENT,
        "Starting namespace key rotation",
        &[
            ("namespace", namespace),
            ("total_secrets", &total.to_string()),
        ],
    );

    for stored in secrets {
        let ns = &stored.secret.namespace;
        let name = &stored.secret.name;

        // Check filter
        if let Some(filter) = config.pre_rotate_filter {
            if !filter(ns, name) {
                result.record_skip();
                continue;
            }
        }

        // Attempt rotation
        let rotation_result = rotate_secret(store, ns, name);

        // Record result
        let success = rotation_result.is_ok();
        if success {
            result.record_success();
        } else {
            let error_msg = rotation_result
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default();
            result.record_failure(ns, name, &error_msg);

            if !config.continue_on_error {
                break;
            }
        }

        // Invoke callback
        if let Some(callback) = config.post_rotate_callback {
            callback(ns, name, success);
        }
    }

    log_info(
        ROTATION_COMPONENT,
        "Namespace key rotation completed",
        &[
            ("namespace", namespace),
            ("success", &result.success_count.to_string()),
            ("failed", &result.failure_count.to_string()),
            ("skipped", &result.skipped_count.to_string()),
        ],
    );

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::secrets::SecretMaterial;
    use crate::nanocloud::util::security::SecureAssets;
    use serial_test::serial;
    use std::collections::BTreeMap;
    use std::env;
    use std::fs;
    use tempfile::tempdir;

    fn sample_secret(namespace: &str, name: &str) -> SecretMaterial {
        let mut data = BTreeMap::new();
        data.insert("key".to_string(), format!("value-{}", name));
        SecretMaterial {
            namespace: namespace.to_string(),
            name: name.to_string(),
            type_name: "Opaque".to_string(),
            immutable: false,
            data,
            resource_version: None,
        }
    }

    #[test]
    #[serial]
    fn rotate_single_secret() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let keyspace_dir = temp_dir.path().join("keyspace");
        fs::create_dir_all(&keyspace_dir).expect("failed to prepare keyspace dir");
        env::set_var(
            "NANOCLOUD_KEYSPACE",
            keyspace_dir.to_string_lossy().to_string(),
        );

        let assets_dir = temp_dir.path().join("assets");
        fs::create_dir_all(&assets_dir).expect("failed to prepare assets dir");
        SecureAssets::generate(&assets_dir, false).expect("failed to generate secure assets");
        env::set_var(
            "NANOCLOUD_SECURE_ASSETS",
            assets_dir.to_string_lossy().to_string(),
        );

        let store = KeyspaceSecretStore::new();

        // Create a secret
        let secret = sample_secret("default", "rotation-test");
        let original = store.put(secret).expect("put should succeed");
        let original_digest = original.digest.clone();

        // Rotate
        rotate_secret(&store, "default", "rotation-test").expect("rotation should succeed");

        // Verify secret is still accessible with same data but different digest
        let rotated = store
            .get("default", "rotation-test")
            .expect("get should succeed")
            .expect("secret should exist");

        assert_eq!(rotated.secret.data.get("key"), Some(&"value-rotation-test".to_string()));
        // Digest should be different due to new data key
        assert_ne!(rotated.digest, original_digest);

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn rotate_all_secrets_success() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let keyspace_dir = temp_dir.path().join("keyspace");
        fs::create_dir_all(&keyspace_dir).expect("failed to prepare keyspace dir");
        env::set_var(
            "NANOCLOUD_KEYSPACE",
            keyspace_dir.to_string_lossy().to_string(),
        );

        let assets_dir = temp_dir.path().join("assets");
        fs::create_dir_all(&assets_dir).expect("failed to prepare assets dir");
        SecureAssets::generate(&assets_dir, false).expect("failed to generate secure assets");
        env::set_var(
            "NANOCLOUD_SECURE_ASSETS",
            assets_dir.to_string_lossy().to_string(),
        );

        let store = KeyspaceSecretStore::new();

        // Create multiple secrets
        for i in 0..3 {
            let secret = sample_secret("default", &format!("secret-{}", i));
            store.put(secret).expect("put should succeed");
        }

        // Rotate all
        let result = rotate_all_secrets(&store, RotationConfig::default())
            .expect("rotation should succeed");

        assert_eq!(result.success_count, 3);
        assert_eq!(result.failure_count, 0);
        assert!(result.is_complete());

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn rotate_with_filter() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let keyspace_dir = temp_dir.path().join("keyspace");
        fs::create_dir_all(&keyspace_dir).expect("failed to prepare keyspace dir");
        env::set_var(
            "NANOCLOUD_KEYSPACE",
            keyspace_dir.to_string_lossy().to_string(),
        );

        let assets_dir = temp_dir.path().join("assets");
        fs::create_dir_all(&assets_dir).expect("failed to prepare assets dir");
        SecureAssets::generate(&assets_dir, false).expect("failed to generate secure assets");
        env::set_var(
            "NANOCLOUD_SECURE_ASSETS",
            assets_dir.to_string_lossy().to_string(),
        );

        let store = KeyspaceSecretStore::new();

        // Create multiple secrets
        for i in 0..5 {
            let secret = sample_secret("default", &format!("secret-{}", i));
            store.put(secret).expect("put should succeed");
        }

        // Rotate only even-numbered secrets
        fn filter(_ns: &str, name: &str) -> bool {
            name.ends_with('0') || name.ends_with('2') || name.ends_with('4')
        }

        let config = RotationConfig {
            pre_rotate_filter: Some(filter),
            ..Default::default()
        };

        let result = rotate_all_secrets(&store, config).expect("rotation should succeed");

        assert_eq!(result.success_count, 3); // 0, 2, 4
        assert_eq!(result.skipped_count, 2); // 1, 3
        assert_eq!(result.failure_count, 0);

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn rotate_nonexistent_secret_fails() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let keyspace_dir = temp_dir.path().join("keyspace");
        fs::create_dir_all(&keyspace_dir).expect("failed to prepare keyspace dir");
        env::set_var(
            "NANOCLOUD_KEYSPACE",
            keyspace_dir.to_string_lossy().to_string(),
        );

        let assets_dir = temp_dir.path().join("assets");
        fs::create_dir_all(&assets_dir).expect("failed to prepare assets dir");
        SecureAssets::generate(&assets_dir, false).expect("failed to generate secure assets");
        env::set_var(
            "NANOCLOUD_SECURE_ASSETS",
            assets_dir.to_string_lossy().to_string(),
        );

        let store = KeyspaceSecretStore::new();

        let result = rotate_secret(&store, "default", "nonexistent");
        assert!(result.is_err());

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn rotate_namespace_secrets_only() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let keyspace_dir = temp_dir.path().join("keyspace");
        fs::create_dir_all(&keyspace_dir).expect("failed to prepare keyspace dir");
        env::set_var(
            "NANOCLOUD_KEYSPACE",
            keyspace_dir.to_string_lossy().to_string(),
        );

        let assets_dir = temp_dir.path().join("assets");
        fs::create_dir_all(&assets_dir).expect("failed to prepare assets dir");
        SecureAssets::generate(&assets_dir, false).expect("failed to generate secure assets");
        env::set_var(
            "NANOCLOUD_SECURE_ASSETS",
            assets_dir.to_string_lossy().to_string(),
        );

        let store = KeyspaceSecretStore::new();

        // Create secrets in different namespaces
        store.put(sample_secret("ns1", "secret-a")).expect("put should succeed");
        store.put(sample_secret("ns1", "secret-b")).expect("put should succeed");
        store.put(sample_secret("ns2", "secret-c")).expect("put should succeed");

        // Rotate only ns1
        let result = rotate_namespace_secrets(&store, "ns1", RotationConfig::default())
            .expect("rotation should succeed");

        assert_eq!(result.success_count, 2);
        assert_eq!(result.failure_count, 0);

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn rotation_result_tracking() {
        let mut result = RotationResult::default();

        result.record_success();
        result.record_success();
        result.record_failure("ns", "name1", "error1");
        result.record_skip();

        assert_eq!(result.success_count, 2);
        assert_eq!(result.failure_count, 1);
        assert_eq!(result.skipped_count, 1);
        assert_eq!(result.total_processed(), 4);
        assert!(!result.is_complete());
        assert_eq!(result.failures.len(), 1);
        assert_eq!(result.failures[0], ("ns".to_string(), "name1".to_string(), "error1".to_string()));
    }
}
