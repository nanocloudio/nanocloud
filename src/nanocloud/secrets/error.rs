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

//! Standardized error types for secret operations.
//!
//! This module provides a consistent error enum covering all secret operation failures:
//! - Validation errors for malformed metadata
//! - NotFound errors for missing secrets
//! - IO errors for keyspace operations
//! - Crypto errors for encryption/decryption failures
//! - Integrity errors for HMAC verification failures (tamper detection)
//! - Encoding/decoding errors for record serialization
//! - Lock errors for concurrency issues
//! - KeyspaceRoot errors for initialization/permission issues
//!
//! # Error Categories
//!
//! Errors are categorized to help callers handle them appropriately:
//! - **Validation**: Input data is malformed; caller should fix input
//! - **NotFound**: Secret doesn't exist; may be expected in some flows
//! - **Integrity**: Possible tampering detected; should be logged/alerted
//! - **Crypto/IO/Encode/Decode**: Infrastructure issues; may be retryable
//! - **Lock**: Concurrency contention; may be retryable after delay
//! - **KeyspaceRoot**: Configuration/permission issue; needs admin attention

use std::error::Error;
use std::fmt;

/// Error type for secret store operations.
#[derive(Debug)]
pub enum SecretError {
    /// Validation error for malformed or missing metadata.
    ///
    /// This error indicates the caller provided invalid input that should be corrected.
    Validation(String),

    /// Secret not found.
    ///
    /// The requested secret does not exist. This may be an expected condition
    /// in some workflows (e.g., checking if a secret exists before creating).
    NotFound {
        namespace: String,
        name: String,
    },

    /// IO error during keyspace operations.
    ///
    /// File system or keyspace access failed. May be transient and retryable.
    Io(String),

    /// Cryptographic operation failure (encryption, decryption, key wrapping).
    ///
    /// Encryption or decryption failed. Could indicate key issues or corrupted data.
    Crypto(String),

    /// Integrity check failure (HMAC mismatch).
    ///
    /// The stored secret failed HMAC verification, indicating possible tampering
    /// or corruption. This is a security-relevant event that should be logged.
    Integrity {
        /// The key path of the affected secret (namespace/name)
        key: String,
        /// Description of the integrity failure
        message: String,
    },

    /// Record encoding failure.
    ///
    /// Failed to serialize the secret record to JSON.
    Encode(String),

    /// Record decoding failure.
    ///
    /// Failed to deserialize the secret record from JSON. Could indicate
    /// corrupted data or incompatible format changes.
    Decode(String),

    /// Lock acquisition failure or timeout.
    ///
    /// Could not acquire the per-secret lock within the timeout period.
    /// May indicate high contention; caller can retry after a delay.
    Lock(String),

    /// Keyspace root configuration or permission error.
    ///
    /// The keyspace root directory is invalid, missing, or has incorrect permissions.
    /// Requires administrative attention to resolve.
    #[allow(dead_code)]
    KeyspaceRoot(String),
}

#[allow(dead_code)]
impl SecretError {
    /// Returns true if this error indicates possible tampering or corruption.
    ///
    /// Integrity errors should be logged and potentially alerted on,
    /// as they may indicate a security incident.
    pub fn is_integrity_error(&self) -> bool {
        matches!(self, SecretError::Integrity { .. })
    }

    /// Returns true if this error is likely transient and retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(self, SecretError::Lock(_) | SecretError::Io(_))
    }

    /// Returns the namespace and name for errors that have them.
    pub fn secret_identity(&self) -> Option<(&str, &str)> {
        match self {
            SecretError::NotFound { namespace, name } => Some((namespace, name)),
            SecretError::Integrity { key, .. } => {
                // Parse key format "/secrets/{namespace}/{name}"
                let parts: Vec<&str> = key.split('/').collect();
                if parts.len() >= 4 {
                    Some((parts[2], parts[3]))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

impl fmt::Display for SecretError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SecretError::Validation(msg) => write!(f, "Secret validation error: {}", msg),
            SecretError::NotFound { namespace, name } => {
                write!(f, "Secret '{}/{}' not found", namespace, name)
            }
            SecretError::Io(msg) => write!(f, "Secret IO error: {}", msg),
            SecretError::Crypto(msg) => write!(f, "Secret crypto error: {}", msg),
            SecretError::Integrity { key, message } => {
                write!(f, "Secret integrity check failed for '{}': {}", key, message)
            }
            SecretError::Encode(msg) => write!(f, "Secret encoding error: {}", msg),
            SecretError::Decode(msg) => write!(f, "Secret decoding error: {}", msg),
            SecretError::Lock(msg) => write!(f, "Secret lock error: {}", msg),
            SecretError::KeyspaceRoot(msg) => write!(f, "Secret keyspace root error: {}", msg),
        }
    }
}

impl Error for SecretError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integrity_error_detection() {
        let err = SecretError::Integrity {
            key: "/secrets/default/test".to_string(),
            message: "HMAC mismatch".to_string(),
        };
        assert!(err.is_integrity_error());

        let err = SecretError::Validation("bad input".to_string());
        assert!(!err.is_integrity_error());
    }

    #[test]
    fn retryable_error_detection() {
        let err = SecretError::Lock("timeout".to_string());
        assert!(err.is_retryable());

        let err = SecretError::Io("disk full".to_string());
        assert!(err.is_retryable());

        let err = SecretError::Validation("bad input".to_string());
        assert!(!err.is_retryable());
    }

    #[test]
    fn secret_identity_extraction() {
        let err = SecretError::NotFound {
            namespace: "prod".to_string(),
            name: "api-key".to_string(),
        };
        assert_eq!(err.secret_identity(), Some(("prod", "api-key")));

        let err = SecretError::Integrity {
            key: "/secrets/staging/db-creds".to_string(),
            message: "tampered".to_string(),
        };
        assert_eq!(err.secret_identity(), Some(("staging", "db-creds")));

        let err = SecretError::Validation("bad".to_string());
        assert_eq!(err.secret_identity(), None);
    }
}
