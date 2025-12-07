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

//! Record format encoding, decoding, and associated data computation.
//!
//! This module centralizes the secret record format:
//! - `SecretStoreRecord`: The persisted JSON structure
//! - `SecretRecordMetadata`: Metadata fields stored in cleartext
//! - `SecretCipherPayload`: The encrypted payload structure
//! - Associated data builder for HMAC computation
//!
//! # Record Format
//!
//! Secrets are stored as JSON with the following schema:
//! ```json
//! {
//!   "metadata": {
//!     "namespace": "...",
//!     "name": "...",
//!     "type": "...",
//!     "immutable": false,
//!     "resource_version": "..."
//!   },
//!   "ciphertext": "base64-encoded encrypted payload",
//!   "wrapped_key": "base64-encoded wrapped data key",
//!   "digest": "hex-encoded HMAC-SHA256",
//!   "created_at": "RFC3339 timestamp"
//! }
//! ```

use std::collections::BTreeMap;
use std::error::Error;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The persisted secret record structure.
#[derive(Debug, Serialize, Deserialize)]
pub struct SecretStoreRecord {
    pub metadata: SecretRecordMetadata,
    pub ciphertext: String,
    pub wrapped_key: String,
    pub digest: String,
    pub created_at: String,
}

/// Metadata fields stored alongside the encrypted secret.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretRecordMetadata {
    pub namespace: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub immutable: bool,
    pub resource_version: Option<String>,
}

/// The encrypted payload structure.
#[derive(Debug, Serialize, Deserialize)]
pub struct SecretCipherPayload {
    #[serde(rename = "type")]
    pub type_name: String,
    pub immutable: bool,
    pub data: BTreeMap<String, String>,
}

/// Associated data structure for HMAC computation.
///
/// This structure is serialized to JSON and used as additional authenticated data
/// during encryption/decryption to bind the ciphertext to its metadata context.
#[derive(Serialize)]
pub struct SecretAssociatedData<'a> {
    pub record_key: &'a str,
    pub namespace: &'a str,
    pub name: &'a str,
    #[serde(rename = "type")]
    pub type_name: &'a str,
    pub immutable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<&'a str>,
}

/// Builds the associated data bytes for HMAC computation.
///
/// The associated data binds the ciphertext to its metadata context,
/// ensuring that tampering with metadata fields will cause verification to fail.
pub fn build_associated_data(
    record_key: &str,
    metadata: &SecretRecordMetadata,
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let aad = SecretAssociatedData {
        record_key,
        namespace: &metadata.namespace,
        name: &metadata.name,
        type_name: &metadata.type_name,
        immutable: metadata.immutable,
        resource_version: metadata.resource_version.as_deref(),
    };
    serde_json::to_vec(&aad).map_err(|e| {
        Box::new(e) as Box<dyn Error + Send + Sync>
    })
}

/// Builds a complete secret store record.
pub fn build_record(
    metadata: SecretRecordMetadata,
    ciphertext: String,
    wrapped_key: String,
    digest: String,
    created_at: DateTime<Utc>,
) -> SecretStoreRecord {
    SecretStoreRecord {
        metadata,
        ciphertext,
        wrapped_key,
        digest,
        created_at: created_at.to_rfc3339(),
    }
}

/// Builds a cipher payload from secret data.
pub fn build_payload(
    type_name: String,
    immutable: bool,
    data: BTreeMap<String, String>,
) -> SecretCipherPayload {
    SecretCipherPayload {
        type_name,
        immutable,
        data,
    }
}

/// Encodes a secret record to JSON.
pub fn encode_record(record: &SecretStoreRecord) -> Result<String, Box<dyn Error + Send + Sync>> {
    serde_json::to_string_pretty(record).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
}

/// Decodes a secret record from JSON.
pub fn decode_record(json: &str) -> Result<SecretStoreRecord, Box<dyn Error + Send + Sync>> {
    serde_json::from_str(json).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
}

/// Encodes a cipher payload to JSON bytes.
pub fn encode_payload(payload: &SecretCipherPayload) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    serde_json::to_vec(payload).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
}

/// Decodes a cipher payload from JSON bytes.
pub fn decode_payload(bytes: &[u8]) -> Result<SecretCipherPayload, Box<dyn Error + Send + Sync>> {
    serde_json::from_slice(bytes).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_round_trip() {
        let metadata = SecretRecordMetadata {
            namespace: "default".to_string(),
            name: "test-secret".to_string(),
            type_name: "Opaque".to_string(),
            immutable: false,
            resource_version: Some("123".to_string()),
        };

        let record = build_record(
            metadata,
            "ciphertext-data".to_string(),
            "wrapped-key-data".to_string(),
            "digest-hex".to_string(),
            Utc::now(),
        );

        let encoded = encode_record(&record).expect("encoding should succeed");
        let decoded = decode_record(&encoded).expect("decoding should succeed");

        assert_eq!(decoded.metadata.namespace, "default");
        assert_eq!(decoded.metadata.name, "test-secret");
        assert_eq!(decoded.ciphertext, "ciphertext-data");
        assert_eq!(decoded.wrapped_key, "wrapped-key-data");
        assert_eq!(decoded.digest, "digest-hex");
    }

    #[test]
    fn payload_round_trip() {
        let mut data = BTreeMap::new();
        data.insert("key1".to_string(), "value1".to_string());
        data.insert("key2".to_string(), "value2".to_string());

        let payload = build_payload("Opaque".to_string(), true, data.clone());

        let encoded = encode_payload(&payload).expect("encoding should succeed");
        let decoded = decode_payload(&encoded).expect("decoding should succeed");

        assert_eq!(decoded.type_name, "Opaque");
        assert!(decoded.immutable);
        assert_eq!(decoded.data, data);
    }

    #[test]
    fn associated_data_consistency() {
        let metadata = SecretRecordMetadata {
            namespace: "prod".to_string(),
            name: "api-key".to_string(),
            type_name: "Opaque".to_string(),
            immutable: true,
            resource_version: None,
        };

        let aad1 = build_associated_data("/secrets/prod/api-key", &metadata)
            .expect("building associated data should succeed");
        let aad2 = build_associated_data("/secrets/prod/api-key", &metadata)
            .expect("building associated data should succeed");

        // Same inputs should produce same associated data
        assert_eq!(aad1, aad2);

        // Different key should produce different associated data
        let aad3 = build_associated_data("/secrets/prod/other-key", &metadata)
            .expect("building associated data should succeed");
        assert_ne!(aad1, aad3);
    }

    #[test]
    fn associated_data_with_resource_version() {
        let metadata_with_version = SecretRecordMetadata {
            namespace: "default".to_string(),
            name: "test".to_string(),
            type_name: "Opaque".to_string(),
            immutable: false,
            resource_version: Some("v1".to_string()),
        };

        let metadata_without_version = SecretRecordMetadata {
            namespace: "default".to_string(),
            name: "test".to_string(),
            type_name: "Opaque".to_string(),
            immutable: false,
            resource_version: None,
        };

        let aad_with = build_associated_data("/secrets/default/test", &metadata_with_version)
            .expect("building associated data should succeed");
        let aad_without = build_associated_data("/secrets/default/test", &metadata_without_version)
            .expect("building associated data should succeed");

        // Resource version presence should affect associated data
        assert_ne!(aad_with, aad_without);
    }
}
