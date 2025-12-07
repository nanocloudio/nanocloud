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

//! Secret storage module providing encrypted, integrity-verified secret persistence.
//!
//! This module provides:
//! - Encrypted secret storage backed by a keyspace
//! - HMAC-based integrity verification
//! - Atomic write operations via write-then-rename
//! - Per-secret locking to prevent concurrent write races
//!
//! # Architecture
//!
//! The module is organized into submodules separating concerns:
//! - [`crypto`]: Encryption, decryption, and HMAC operations
//! - [`record`]: Record format encoding/decoding and associated data
//! - [`storage`]: Keyspace I/O operations with atomic writes and locking
//! - [`validation`]: Metadata and timestamp validation
//! - [`error`]: Standardized error types for secret operations
//! - [`metrics`]: Observability hooks for monitoring secret operations
//! - [`rotation`]: Key rotation helpers for re-encrypting secrets
//!
//! # Record Format
//!
//! Secrets are persisted as JSON files in the keyspace with the following structure:
//!
//! ```json
//! {
//!   "metadata": {
//!     "namespace": "default",
//!     "name": "my-secret",
//!     "type": "Opaque",
//!     "immutable": false,
//!     "resource_version": "12345"
//!   },
//!   "ciphertext": "<base64-encoded encrypted payload>",
//!   "wrapped_key": "<base64-encoded wrapped data key>",
//!   "digest": "<64-char hex HMAC-SHA256>",
//!   "created_at": "2024-01-15T10:30:00Z"
//! }
//! ```
//!
//! ## Field Descriptions
//!
//! | Field | Description |
//! |-------|-------------|
//! | `metadata` | Cleartext metadata for indexing/listing without decryption |
//! | `metadata.namespace` | Kubernetes-style namespace for secret isolation |
//! | `metadata.name` | Unique name within the namespace |
//! | `metadata.type` | Secret type (e.g., "Opaque", "kubernetes.io/tls") |
//! | `metadata.immutable` | If true, secret cannot be updated after creation |
//! | `metadata.resource_version` | Optional version for optimistic concurrency control |
//! | `ciphertext` | AES-256-GCM encrypted payload containing actual secret data |
//! | `wrapped_key` | Data encryption key (DEK) wrapped by the KMS master key |
//! | `digest` | HMAC-SHA256 of secret data for integrity verification |
//! | `created_at` | RFC 3339 timestamp of when the secret was created/updated |
//!
//! ## Encrypted Payload Structure
//!
//! The decrypted `ciphertext` contains:
//!
//! ```json
//! {
//!   "type": "Opaque",
//!   "immutable": false,
//!   "data": {
//!     "username": "admin",
//!     "password": "s3cr3t"
//!   }
//! }
//! ```
//!
//! # Security Model
//!
//! ## Encryption
//!
//! - **Envelope Encryption**: Each secret is encrypted with a unique Data Encryption Key (DEK).
//!   The DEK is then wrapped (encrypted) by the KMS master key.
//! - **Algorithm**: AES-256-GCM provides authenticated encryption with associated data (AEAD).
//! - **Key Derivation**: Fresh 256-bit random DEKs are generated for each write operation.
//!
//! ## Integrity Protection
//!
//! - **HMAC-SHA256**: A keyed hash is computed over the secret data to detect tampering.
//! - **Associated Data**: The ciphertext is bound to its metadata (namespace, name, type, etc.)
//!   using AEAD. Tampering with metadata causes decryption to fail.
//! - **Digest Verification**: On read, the HMAC is recomputed and compared to the stored value.
//!   Mismatches trigger an integrity error and are logged for security auditing.
//!
//! ## Key Management
//!
//! - **Master Key**: A 256-bit AES key stored in the secure assets directory.
//! - **Key Wrapping**: DEKs are wrapped using AES-256-GCM with the master key.
//! - **Key Rotation**: The [`rotation`] module provides helpers for re-encrypting secrets
//!   with fresh DEKs. Master key rotation requires external coordination.
//!
//! ## Security Assumptions
//!
//! 1. **File System Permissions**: The keyspace directory has appropriate permissions
//!    (typically 0700) restricting access to the nanocloud process user.
//! 2. **Master Key Protection**: The secure assets directory containing the master key
//!    has restricted permissions and is not accessible to unprivileged users.
//! 3. **Memory Safety**: Rust's memory safety guarantees prevent many classes of
//!    vulnerabilities. Secret data is not pinned in memory; use external tools
//!    for enhanced memory protection if required.
//! 4. **No Defense Against Root**: An attacker with root access to the system can
//!    extract the master key and decrypt all secrets.
//! 5. **Logging Safety**: Only namespace and name identifiers are logged; secret
//!    values are never included in logs or error messages.
//!
//! # Concurrency Model
//!
//! ## Per-Secret Locking
//!
//! The storage layer uses a global registry of per-secret mutexes to serialize
//! write operations to the same secret:
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │          Global Lock Registry                   │
//! │  ┌─────────────────────────────────────────┐   │
//! │  │ HashMap<String, Arc<Mutex<()>>>         │   │
//! │  │                                         │   │
//! │  │ "/secrets/default/db-creds" → Mutex     │   │
//! │  │ "/secrets/prod/api-key"     → Mutex     │   │
//! │  └─────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────┘
//! ```
//!
//! - **Write-Write Serialization**: Multiple writers to the same secret are serialized.
//! - **Read-Write Concurrency**: Reads do not acquire the per-secret lock; they rely
//!   on atomic file writes for consistency.
//! - **Different Secrets**: Writes to different secrets proceed in parallel.
//! - **Lock Timeout**: Locks have a 30-second timeout to prevent indefinite blocking.
//!
//! ## Atomic Writes
//!
//! File writes use the write-then-rename pattern:
//!
//! 1. Write complete content to a temporary file in the same directory
//! 2. Call `fsync` to ensure data is persisted to disk
//! 3. Atomically rename the temporary file to the target path
//!
//! This ensures readers always see either the old or new complete content, never
//! a partial write. The keyspace layer handles this automatically.
//!
//! ## Thread Safety Summary
//!
//! | Operation | Concurrent with same secret | Concurrent with other secrets |
//! |-----------|----------------------------|-------------------------------|
//! | Read      | Safe (atomic file reads)   | Safe                          |
//! | Write     | Serialized (per-secret lock)| Parallel                      |
//! | Delete    | Serialized (per-secret lock)| Parallel                      |
//! | List      | Safe (reads are atomic)    | Safe                          |
//!
//! # Error Handling
//!
//! The [`error::SecretError`] enum provides structured error types:
//!
//! - **Validation**: Input validation failures (empty namespace, invalid characters)
//! - **NotFound**: Secret does not exist
//! - **Io**: File system or keyspace errors
//! - **Crypto**: Encryption/decryption failures
//! - **Integrity**: HMAC verification failures (tampering detected)
//! - **Encode/Decode**: JSON serialization errors
//! - **Lock**: Lock acquisition timeout
//! - **KeyspaceRoot**: Configuration or permission issues
//!
//! ## Error Recovery
//!
//! - **Transient errors** (Io, Lock): May be retried after a delay
//! - **Corruption errors** (Decode, Integrity): Delete and recreate the secret
//! - **Validation errors**: Fix the input and retry
//!
//! # Testing Guidelines
//!
//! ## Test Isolation
//!
//! All secrets module tests follow these isolation patterns:
//!
//! 1. **Temporary directories**: Tests use `tempfile::tempdir()` for all file operations.
//!    The temp directory is automatically cleaned up when the test completes.
//!
//! 2. **Serial execution**: Tests that modify environment variables (like `NANOCLOUD_KEYSPACE`
//!    or `NANOCLOUD_SECURE_ASSETS`) are marked with `#[serial]` from the `serial_test` crate.
//!    This prevents race conditions when tests run in parallel.
//!
//! 3. **Environment scoping**: Tests set environment variables for the duration of the test
//!    and restore the previous values when done.
//!
//! ## Running Tests
//!
//! ```bash
//! # Run secrets tests with serial execution
//! cargo test --lib secrets:: -- --test-threads=1
//!
//! # Run benchmarks (requires feature flag)
//! cargo bench --features secrets-bench
//! ```
//!
//! ## Doc Tests
//!
//! Examples in this documentation use `ignore` because they require:
//! - A configured secure assets directory with valid key material
//! - A writable keyspace directory
//! - Proper environment variables set
//!
//! For runnable examples, see the unit tests in this module.
//!
//! # Example Usage
//!
//! ## Basic CRUD Operations
//!
//! ```ignore
//! use nanocloud::secrets::{KeyspaceSecretStore, SecretMaterial};
//! use std::collections::BTreeMap;
//!
//! // Create a new secret store
//! let store = KeyspaceSecretStore::new();
//!
//! // Create a secret
//! let mut data = BTreeMap::new();
//! data.insert("username".to_string(), "admin".to_string());
//! data.insert("password".to_string(), "s3cr3t".to_string());
//!
//! let secret = SecretMaterial {
//!     namespace: "default".to_string(),
//!     name: "db-creds".to_string(),
//!     type_name: "Opaque".to_string(),
//!     immutable: false,
//!     data,
//!     resource_version: None,
//! };
//!
//! // Store the secret (encrypted with integrity protection)
//! let stored = store.put(secret)?;
//! println!("Secret stored with digest: {}", stored.digest);
//!
//! // Retrieve the secret (decrypted and integrity-verified)
//! if let Some(retrieved) = store.get("default", "db-creds")? {
//!     println!("Password: {}", retrieved.secret.data.get("password").unwrap());
//! }
//!
//! // Delete the secret
//! store.delete("default", "db-creds")?;
//! ```
//!
//! ## Handling Integrity Errors
//!
//! ```ignore
//! use nanocloud::secrets::{KeyspaceSecretStore, SecretError};
//!
//! let store = KeyspaceSecretStore::new();
//!
//! match store.get("default", "my-secret") {
//!     Ok(Some(secret)) => {
//!         println!("Secret retrieved successfully");
//!     }
//!     Ok(None) => {
//!         println!("Secret not found");
//!     }
//!     Err(e) => {
//!         // Check if this is an integrity error (possible tampering)
//!         let err_msg = e.to_string();
//!         if err_msg.contains("integrity") || err_msg.contains("HMAC") {
//!             eprintln!("WARNING: Possible tampering detected for secret!");
//!             // In production, you might want to:
//!             // 1. Log to security audit system
//!             // 2. Alert operations team
//!             // 3. Consider deleting and recreating the secret
//!         } else {
//!             eprintln!("Error retrieving secret: {}", e);
//!         }
//!     }
//! }
//! ```
//!
//! ## Using the Cache for Performance
//!
//! ```ignore
//! use nanocloud::secrets::{KeyspaceSecretStore, SecretMaterial};
//! use nanocloud::secrets::cache::{CachedSecretStore, CacheConfig};
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! // Create the underlying store
//! let store = Arc::new(KeyspaceSecretStore::new());
//!
//! // Wrap with caching (2 minute TTL)
//! let cached_store = CachedSecretStore::with_config(
//!     store,
//!     CacheConfig {
//!         ttl: Duration::from_secs(120),
//!         max_entries: 500,
//!         enabled: true,
//!     },
//! );
//!
//! // First get - cache miss, reads from disk
//! let secret = cached_store.get("default", "frequently-accessed")?;
//!
//! // Subsequent gets within TTL - cache hit, no decryption needed
//! let secret = cached_store.get("default", "frequently-accessed")?;
//! ```
//!
//! ## Configuration Validation
//!
//! ```ignore
//! use nanocloud::secrets::config::{validate_key_material, ConfigError};
//!
//! // Validate at startup
//! match validate_key_material() {
//!     Ok(result) => {
//!         println!("Key material validated:");
//!         println!("  Key size: {} bits", result.key_size_bits);
//!     }
//!     Err(e) => {
//!         // Error messages include remediation hints
//!         eprintln!("Configuration error:\n{}", e);
//!         std::process::exit(1);
//!     }
//! }
//! ```
//!
//! ## Key Rotation
//!
//! ```ignore
//! use nanocloud::secrets::KeyspaceSecretStore;
//! use nanocloud::secrets::rotation::{rotate_all_secrets, RotationConfig};
//!
//! let store = KeyspaceSecretStore::new();
//!
//! // Rotate all secrets with fresh data encryption keys
//! let config = RotationConfig {
//!     continue_on_error: true,
//!     pre_rotate_filter: None,
//!     post_rotate_callback: Some(|ns, name, success| {
//!         if success {
//!             println!("Rotated {}/{}", ns, name);
//!         }
//!     }),
//! };
//!
//! let result = rotate_all_secrets(&store, config)?;
//! println!("Rotation complete: {} succeeded, {} failed",
//!          result.success_count, result.failure_count);
//! ```

#[allow(dead_code)]
pub mod cache;
#[allow(dead_code)]
pub mod config;
mod crypto;
mod error;
#[allow(dead_code)]
pub mod metrics;
mod record;
#[allow(dead_code)]
pub mod rotation;
mod storage;
mod validation;

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};

#[allow(unused_imports)]
pub use error::SecretError;
use record::{SecretRecordMetadata, SecretStoreRecord};
use storage::SecretStorage;

use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::Keyspace;

/// Material representing a secret's content and metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretMaterial {
    pub namespace: String,
    pub name: String,
    pub type_name: String,
    pub immutable: bool,
    pub data: BTreeMap<String, String>,
    pub resource_version: Option<String>,
}

/// A stored secret with computed digest and creation timestamp.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredSecret {
    pub secret: SecretMaterial,
    pub digest: String,
    pub created_at: DateTime<Utc>,
}

/// Keyspace-backed secret store with encryption, integrity verification, and concurrency control.
pub struct KeyspaceSecretStore {
    storage: SecretStorage,
}

impl KeyspaceSecretStore {
    pub fn new() -> Self {
        Self {
            storage: SecretStorage::new(Arc::new(Keyspace::new("secrets"))),
        }
    }

    fn record_key(namespace: &str, name: &str) -> String {
        format!("/secrets/{namespace}/{name}")
    }
}

impl Default for KeyspaceSecretStore {
    fn default() -> Self {
        Self::new()
    }
}

use std::error::Error;

impl KeyspaceSecretStore {
    /// Retrieves a secret by namespace and name.
    ///
    /// Returns `Ok(None)` if the secret does not exist.
    /// Returns an error if the secret exists but fails integrity verification.
    pub fn get(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<StoredSecret>, Box<dyn Error + Send + Sync>> {
        // Validate input parameters
        validation::validate_namespace(namespace).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
        validation::validate_name(name).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        let key = KeyspaceSecretStore::record_key(namespace, name);

        // Read raw record from storage
        let raw_record = match self.storage.read(&key).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)? {
            Some(value) => value,
            None => return Ok(None),
        };

        // Decode the stored record
        let record: SecretStoreRecord = record::decode_record(&raw_record)
            .map_err(|e| with_context(e, "Failed to decode secret record"))?;

        // Validate timestamps
        let created_at = validation::parse_timestamp(&record.created_at)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        // Decrypt and verify integrity
        let (payload, computed_digest) = crypto::decrypt_and_verify(
            &key,
            &record.metadata,
            &record.ciphertext,
            &record.wrapped_key,
            &record.digest,
        ).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        // Reconstruct secret material from decrypted payload
        let secret = SecretMaterial {
            namespace: record.metadata.namespace,
            name: record.metadata.name,
            type_name: payload.type_name,
            immutable: payload.immutable,
            data: payload.data,
            resource_version: record.metadata.resource_version,
        };

        Ok(Some(StoredSecret {
            secret,
            digest: computed_digest,
            created_at,
        }))
    }

    /// Stores or updates a secret.
    ///
    /// The secret is encrypted with a new data key, and an HMAC digest is computed
    /// for integrity verification. The write is performed atomically.
    pub fn put(&self, secret: SecretMaterial) -> Result<StoredSecret, Box<dyn Error + Send + Sync>> {
        // Validate input parameters
        validation::validate_namespace(&secret.namespace).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
        validation::validate_name(&secret.name).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
        validation::validate_type_name(&secret.type_name).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        let created_at = Utc::now();
        let record_key = KeyspaceSecretStore::record_key(&secret.namespace, &secret.name);

        // Encrypt and compute integrity digest
        let (ciphertext, wrapped_key, digest) = crypto::encrypt_secret(&record_key, &secret)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        // Build the storage record
        let record = record::build_record(
            SecretRecordMetadata {
                namespace: secret.namespace.clone(),
                name: secret.name.clone(),
                type_name: secret.type_name.clone(),
                immutable: secret.immutable,
                resource_version: secret.resource_version.clone(),
            },
            ciphertext,
            wrapped_key,
            digest.clone(),
            created_at,
        );

        // Encode and persist atomically with locking
        let serialized = record::encode_record(&record)
            .map_err(|e| with_context(e, "Failed to encode secret record"))?;

        self.storage.write_atomic(&record_key, &serialized)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        Ok(StoredSecret {
            secret,
            digest,
            created_at,
        })
    }

    /// Deletes a secret by namespace and name.
    ///
    /// Returns `Ok(())` even if the secret does not exist.
    pub fn delete(&self, namespace: &str, name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        validation::validate_namespace(namespace).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
        validation::validate_name(name).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        let key = KeyspaceSecretStore::record_key(namespace, name);
        self.storage.delete(&key).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }

    /// Lists all secrets, optionally filtered by namespace.
    pub fn list(&self, namespace: Option<&str>) -> Result<Vec<StoredSecret>, Box<dyn Error + Send + Sync>> {
        use crate::nanocloud::Config;
        use std::fs;

        if let Some(ns) = namespace {
            validation::validate_namespace(ns).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
        }

        let root = Config::Keyspace.get_path().join("secrets").join("secrets");
        if !root.exists() {
            return Ok(Vec::new());
        }

        let namespace_filter =
            namespace.map(|ns| crate::nanocloud::k8s::store::normalize_namespace(Some(ns)));
        let mut items = Vec::new();

        for ns_entry in fs::read_dir(&root).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to read secrets namespace directory '{}'",
                    root.display()
                ),
            )
        })? {
            let entry = ns_entry.map_err(|e| {
                with_context(
                    e,
                    format!(
                        "Failed to iterate secrets namespace directory '{}'",
                        root.display()
                    ),
                )
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|e| {
                with_context(
                    e,
                    format!(
                        "Failed to inspect secrets namespace entry '{}'",
                        path.display()
                    ),
                )
            })?;
            if !file_type.is_dir() {
                continue;
            }
            let ns_name = match entry.file_name().into_string() {
                Ok(value) => value,
                Err(_) => continue,
            };
            let normalized_ns = crate::nanocloud::k8s::store::normalize_namespace(Some(&ns_name));
            if namespace_filter
                .as_ref()
                .is_some_and(|target| target != &normalized_ns)
            {
                continue;
            }

            for secret_entry in fs::read_dir(&path).map_err(|e| {
                with_context(
                    e,
                    format!("Failed to iterate secrets directory '{}'", path.display()),
                )
            })? {
                let entry = secret_entry.map_err(|e| {
                    with_context(
                        e,
                        format!(
                            "Failed to inspect secret entry in namespace '{}'",
                            normalized_ns
                        ),
                    )
                })?;
                let secret_path = entry.path();
                let file_type = entry.file_type().map_err(|e| {
                    with_context(
                        e,
                        format!("Failed to inspect secret path '{}'", secret_path.display()),
                    )
                })?;
                if !file_type.is_dir() {
                    continue;
                }
                let secret_name = match entry.file_name().into_string() {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                if let Some(secret) = self.get(&normalized_ns, &secret_name)? {
                    items.push(secret);
                }
            }
        }

        Ok(items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::util::security::SecureAssets;
    use serial_test::serial;
    use std::env;
    use std::fs;
    use tempfile::tempdir;

    fn sample_secret() -> SecretMaterial {
        let mut data = BTreeMap::new();
        data.insert("username".to_string(), "admin".to_string());
        data.insert("password".to_string(), "s3cr3t".to_string());
        SecretMaterial {
            namespace: "default".to_string(),
            name: "db-creds".to_string(),
            type_name: "Opaque".to_string(),
            immutable: false,
            data,
            resource_version: None,
        }
    }

    #[test]
    #[serial]
    fn round_trip_secret_storage() {
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

        let secret = sample_secret();
        let store = KeyspaceSecretStore::new();
        let stored = store
            .put(secret.clone())
            .expect("expected secret write to succeed");

        assert_eq!(stored.secret.data.len(), 2);
        assert!(!stored.digest.is_empty());

        let fetched = store
            .get(&secret.namespace, &secret.name)
            .expect("expected secret get to succeed")
            .expect("secret should exist");
        assert_eq!(fetched.secret.data, secret.data);

        store
            .delete(&secret.namespace, &secret.name)
            .expect("expected delete to succeed");

        let missing = store
            .get(&secret.namespace, &secret.name)
            .expect("expected get after delete to succeed");
        assert!(missing.is_none());

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn validation_rejects_empty_namespace() {
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
        let result = store.get("", "test");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("validation"));

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn validation_rejects_empty_name() {
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
        let result = store.get("default", "");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("validation"));

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn get_nonexistent_secret_returns_none() {
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
        let result = store.get("default", "nonexistent");
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn update_secret_overwrites_data() {
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

        // Create initial secret
        let mut secret = sample_secret();
        store.put(secret.clone()).expect("initial put should succeed");

        // Update with new data
        secret.data.insert("new-key".to_string(), "new-value".to_string());
        store.put(secret.clone()).expect("update put should succeed");

        // Verify the update
        let fetched = store
            .get(&secret.namespace, &secret.name)
            .expect("get should succeed")
            .expect("secret should exist");
        assert_eq!(fetched.secret.data.len(), 3);
        assert_eq!(fetched.secret.data.get("new-key"), Some(&"new-value".to_string()));

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn delete_nonexistent_secret_succeeds() {
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
        let result = store.delete("default", "nonexistent");
        assert!(result.is_ok());

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn tampered_digest_detected() {
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
        let secret = sample_secret();
        store.put(secret.clone()).expect("put should succeed");

        // Find the _value_ file by walking the keyspace directory
        let record_path = find_value_file(&keyspace_dir, &secret.namespace, &secret.name)
            .unwrap_or_else(|| {
                panic!(
                    "should find record file in {:?}, namespace={}, name={}",
                    keyspace_dir, secret.namespace, secret.name
                )
            });

        let record_content = fs::read_to_string(&record_path).expect("should read record");
        // Replace the existing digest value with zeroes
        let tampered_content = record_content
            .lines()
            .map(|line| {
                if line.contains("\"digest\":") {
                    "  \"digest\": \"0000000000000000000000000000000000000000000000000000000000000000\","
                } else {
                    line
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&record_path, tampered_content).expect("should write tampered record");

        // Attempting to get should fail with integrity error
        let result = store.get(&secret.namespace, &secret.name);
        assert!(result.is_err());
        let err_str = result.unwrap_err().to_string();
        assert!(err_str.contains("integrity") || err_str.contains("HMAC") || err_str.contains("mismatch"));

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    /// Helper to find the _value_ file for a secret in the keyspace directory.
    fn find_value_file(
        keyspace_dir: &std::path::Path,
        namespace: &str,
        name: &str,
    ) -> Option<std::path::PathBuf> {
        use walkdir::WalkDir;

        for entry in WalkDir::new(keyspace_dir).into_iter().filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.file_name() == Some(std::ffi::OsStr::new("_value_")) {
                // Check if this path contains the namespace and name
                let path_str = path.to_string_lossy();
                if path_str.contains(namespace) && path_str.contains(name) {
                    return Some(path.to_path_buf());
                }
            }
        }
        None
    }

    #[test]
    #[serial]
    fn corrupted_json_detected() {
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
        let secret = sample_secret();
        store.put(secret.clone()).expect("put should succeed");

        // Find the _value_ file by walking the keyspace directory
        let record_path = find_value_file(&keyspace_dir, &secret.namespace, &secret.name)
            .unwrap_or_else(|| {
                panic!(
                    "should find record file in {:?}, namespace={}, name={}",
                    keyspace_dir, secret.namespace, secret.name
                )
            });

        fs::write(&record_path, "{ invalid json").expect("should write corrupted record");

        // Attempting to get should fail with decode error
        let result = store.get(&secret.namespace, &secret.name);
        assert!(result.is_err());

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn multiple_secrets_same_namespace() {
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

        // Create multiple secrets in same namespace
        let mut secret1 = sample_secret();
        secret1.name = "secret-1".to_string();

        let mut secret2 = sample_secret();
        secret2.name = "secret-2".to_string();
        secret2.data.insert("extra".to_string(), "data".to_string());

        store.put(secret1.clone()).expect("put secret1 should succeed");
        store.put(secret2.clone()).expect("put secret2 should succeed");

        // Both should be retrievable independently
        let fetched1 = store.get("default", "secret-1").expect("get should succeed").expect("secret should exist");
        let fetched2 = store.get("default", "secret-2").expect("get should succeed").expect("secret should exist");

        assert_eq!(fetched1.secret.data.len(), 2);
        assert_eq!(fetched2.secret.data.len(), 3);

        // Deleting one shouldn't affect the other
        store.delete("default", "secret-1").expect("delete should succeed");
        assert!(store.get("default", "secret-1").expect("get should succeed").is_none());
        assert!(store.get("default", "secret-2").expect("get should succeed").is_some());

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    // ==================== Concurrent write/read tests ====================

    #[test]
    #[serial]
    fn concurrent_writes_to_same_secret() {
        use std::sync::Arc;
        use std::thread;

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

        let store = Arc::new(KeyspaceSecretStore::new());
        let num_writers = 5;
        let mut handles = vec![];

        // Spawn multiple writers to the same secret
        for i in 0..num_writers {
            let store = Arc::clone(&store);
            let handle = thread::spawn(move || {
                let mut data = BTreeMap::new();
                data.insert("writer".to_string(), format!("writer-{}", i));
                data.insert("value".to_string(), format!("value-{}", i));

                let secret = SecretMaterial {
                    namespace: "default".to_string(),
                    name: "concurrent-secret".to_string(),
                    type_name: "Opaque".to_string(),
                    immutable: false,
                    data,
                    resource_version: None,
                };

                // Each writer attempts to write
                store.put(secret).expect("put should succeed");
            });
            handles.push(handle);
        }

        // Wait for all writers to complete
        for handle in handles {
            handle.join().expect("thread should complete");
        }

        // The secret should exist and be consistent (one of the writers won)
        let fetched = store
            .get("default", "concurrent-secret")
            .expect("get should succeed")
            .expect("secret should exist");

        // Should have exactly 2 keys from one of the writers
        assert_eq!(fetched.secret.data.len(), 2);
        assert!(fetched.secret.data.contains_key("writer"));
        assert!(fetched.secret.data.contains_key("value"));

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn concurrent_reads_during_write() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::thread;
        use std::time::Duration;

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

        let store = Arc::new(KeyspaceSecretStore::new());

        // Create initial secret
        let mut data = BTreeMap::new();
        data.insert("key".to_string(), "initial".to_string());
        let secret = SecretMaterial {
            namespace: "default".to_string(),
            name: "read-write-secret".to_string(),
            type_name: "Opaque".to_string(),
            immutable: false,
            data,
            resource_version: None,
        };
        store.put(secret).expect("initial put should succeed");

        let done = Arc::new(AtomicBool::new(false));
        let num_readers = 3;
        let mut handles = vec![];

        // Spawn readers
        for _ in 0..num_readers {
            let store = Arc::clone(&store);
            let done = Arc::clone(&done);
            let handle = thread::spawn(move || {
                let mut read_count = 0;
                while !done.load(Ordering::SeqCst) && read_count < 10 {
                    let result = store.get("default", "read-write-secret");
                    // Read should always succeed (either old or new value)
                    assert!(result.is_ok());
                    if let Ok(Some(stored)) = result {
                        // Value should be consistent (not corrupted)
                        assert!(stored.secret.data.contains_key("key"));
                    }
                    read_count += 1;
                    thread::sleep(Duration::from_millis(5));
                }
            });
            handles.push(handle);
        }

        // Spawn a writer
        {
            let store = Arc::clone(&store);
            let handle = thread::spawn(move || {
                for i in 0..5 {
                    let mut data = BTreeMap::new();
                    data.insert("key".to_string(), format!("value-{}", i));
                    let secret = SecretMaterial {
                        namespace: "default".to_string(),
                        name: "read-write-secret".to_string(),
                        type_name: "Opaque".to_string(),
                        immutable: false,
                        data,
                        resource_version: None,
                    };
                    store.put(secret).expect("put should succeed");
                    thread::sleep(Duration::from_millis(10));
                }
            });
            handles.push(handle);
        }

        // Wait for writer to finish
        thread::sleep(Duration::from_millis(100));
        done.store(true, Ordering::SeqCst);

        // Wait for all threads
        for handle in handles {
            handle.join().expect("thread should complete");
        }

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn concurrent_writes_to_different_secrets() {
        use std::sync::Arc;
        use std::thread;

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

        let store = Arc::new(KeyspaceSecretStore::new());
        let num_secrets = 5;
        let mut handles = vec![];

        // Spawn writers to different secrets (should run in parallel without blocking)
        for i in 0..num_secrets {
            let store = Arc::clone(&store);
            let handle = thread::spawn(move || {
                let mut data = BTreeMap::new();
                data.insert("index".to_string(), format!("{}", i));

                let secret = SecretMaterial {
                    namespace: "default".to_string(),
                    name: format!("parallel-secret-{}", i),
                    type_name: "Opaque".to_string(),
                    immutable: false,
                    data,
                    resource_version: None,
                };

                store.put(secret).expect("put should succeed");
            });
            handles.push(handle);
        }

        // Wait for all writers
        for handle in handles {
            handle.join().expect("thread should complete");
        }

        // All secrets should exist
        for i in 0..num_secrets {
            let fetched = store
                .get("default", &format!("parallel-secret-{}", i))
                .expect("get should succeed")
                .expect("secret should exist");
            assert_eq!(
                fetched.secret.data.get("index"),
                Some(&format!("{}", i))
            );
        }

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    // ==================== Corruption-recovery tests ====================

    #[test]
    #[serial]
    fn malformed_json_error_and_recovery() {
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

        // Create a valid secret
        let mut data = BTreeMap::new();
        data.insert("key".to_string(), "value".to_string());
        let secret = SecretMaterial {
            namespace: "default".to_string(),
            name: "corruption-test".to_string(),
            type_name: "Opaque".to_string(),
            immutable: false,
            data: data.clone(),
            resource_version: None,
        };
        store.put(secret).expect("put should succeed");

        // Corrupt the stored record with malformed JSON
        let record_path = find_value_file(&keyspace_dir, "default", "corruption-test")
            .expect("should find record file");
        fs::write(&record_path, "{ malformed json without closing brace")
            .expect("should write corrupted record");

        // Get should fail with decode error
        let result = store.get("default", "corruption-test");
        assert!(result.is_err());
        let err_str = result.unwrap_err().to_string();
        assert!(
            err_str.contains("decode") || err_str.contains("Failed"),
            "Expected decode error, got: {}",
            err_str
        );

        // Recovery: delete and recreate
        store.delete("default", "corruption-test").expect("delete should succeed");

        let recovery_secret = SecretMaterial {
            namespace: "default".to_string(),
            name: "corruption-test".to_string(),
            type_name: "Opaque".to_string(),
            immutable: false,
            data,
            resource_version: None,
        };
        store.put(recovery_secret).expect("recovery put should succeed");

        // Now get should succeed
        let fetched = store
            .get("default", "corruption-test")
            .expect("get should succeed after recovery")
            .expect("secret should exist");
        assert_eq!(fetched.secret.data.get("key"), Some(&"value".to_string()));

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn hmac_mismatch_error_and_recovery() {
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

        // Create a valid secret
        let mut data = BTreeMap::new();
        data.insert("sensitive".to_string(), "data".to_string());
        let secret = SecretMaterial {
            namespace: "default".to_string(),
            name: "hmac-test".to_string(),
            type_name: "Opaque".to_string(),
            immutable: false,
            data: data.clone(),
            resource_version: None,
        };
        store.put(secret).expect("put should succeed");

        // Tamper with the digest
        let record_path = find_value_file(&keyspace_dir, "default", "hmac-test")
            .expect("should find record file");
        let record_content = fs::read_to_string(&record_path).expect("should read record");

        // Replace digest with invalid value
        let tampered_content = record_content
            .lines()
            .map(|line| {
                if line.contains("\"digest\":") {
                    "  \"digest\": \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\","
                } else {
                    line
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&record_path, tampered_content).expect("should write tampered record");

        // Get should fail with integrity error
        let result = store.get("default", "hmac-test");
        assert!(result.is_err());
        let err_str = result.unwrap_err().to_string();
        assert!(
            err_str.contains("integrity") || err_str.contains("HMAC") || err_str.contains("mismatch"),
            "Expected integrity error, got: {}",
            err_str
        );

        // Recovery: delete and recreate
        store.delete("default", "hmac-test").expect("delete should succeed");

        let recovery_secret = SecretMaterial {
            namespace: "default".to_string(),
            name: "hmac-test".to_string(),
            type_name: "Opaque".to_string(),
            immutable: false,
            data,
            resource_version: None,
        };
        store.put(recovery_secret).expect("recovery put should succeed");

        // Now get should succeed
        let fetched = store
            .get("default", "hmac-test")
            .expect("get should succeed after recovery")
            .expect("secret should exist");
        assert_eq!(fetched.secret.data.get("sensitive"), Some(&"data".to_string()));

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn partial_record_corruption_recovery() {
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

        // Create a valid secret
        let mut data = BTreeMap::new();
        data.insert("field1".to_string(), "value1".to_string());
        data.insert("field2".to_string(), "value2".to_string());
        let secret = SecretMaterial {
            namespace: "default".to_string(),
            name: "partial-corruption".to_string(),
            type_name: "Opaque".to_string(),
            immutable: false,
            data: data.clone(),
            resource_version: None,
        };
        store.put(secret).expect("put should succeed");

        // Truncate the record file (simulate partial write/crash)
        let record_path = find_value_file(&keyspace_dir, "default", "partial-corruption")
            .expect("should find record file");
        let record_content = fs::read_to_string(&record_path).expect("should read record");
        let truncated = &record_content[..record_content.len() / 2];
        fs::write(&record_path, truncated).expect("should write truncated record");

        // Get should fail
        let result = store.get("default", "partial-corruption");
        assert!(result.is_err());

        // Recovery: delete and recreate
        store.delete("default", "partial-corruption").expect("delete should succeed");

        let recovery_secret = SecretMaterial {
            namespace: "default".to_string(),
            name: "partial-corruption".to_string(),
            type_name: "Opaque".to_string(),
            immutable: false,
            data,
            resource_version: None,
        };
        store.put(recovery_secret).expect("recovery put should succeed");

        // Now get should succeed
        let fetched = store
            .get("default", "partial-corruption")
            .expect("get should succeed after recovery")
            .expect("secret should exist");
        assert_eq!(fetched.secret.data.len(), 2);

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn empty_file_corruption_recovery() {
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

        // Create a valid secret
        let mut data = BTreeMap::new();
        data.insert("key".to_string(), "value".to_string());
        let secret = SecretMaterial {
            namespace: "default".to_string(),
            name: "empty-file-test".to_string(),
            type_name: "Opaque".to_string(),
            immutable: false,
            data: data.clone(),
            resource_version: None,
        };
        store.put(secret).expect("put should succeed");

        // Replace with empty file
        let record_path = find_value_file(&keyspace_dir, "default", "empty-file-test")
            .expect("should find record file");
        fs::write(&record_path, "").expect("should write empty file");

        // Get should fail
        let result = store.get("default", "empty-file-test");
        assert!(result.is_err());

        // Recovery: delete and recreate
        store.delete("default", "empty-file-test").expect("delete should succeed");

        let recovery_secret = SecretMaterial {
            namespace: "default".to_string(),
            name: "empty-file-test".to_string(),
            type_name: "Opaque".to_string(),
            immutable: false,
            data,
            resource_version: None,
        };
        store.put(recovery_secret).expect("recovery put should succeed");

        // Now get should succeed
        let fetched = store
            .get("default", "empty-file-test")
            .expect("get should succeed after recovery")
            .expect("secret should exist");
        assert_eq!(fetched.secret.data.get("key"), Some(&"value".to_string()));

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }
}
