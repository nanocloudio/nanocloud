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

//! Cryptographic operations for secret encryption, decryption, and integrity verification.
//!
//! This module provides:
//! - Envelope encryption using data keys wrapped by the KMS
//! - HMAC-SHA256 digest computation for integrity verification
//! - Decryption with associated data verification
//! - Tamper detection with logging (no sensitive data exposed)

use std::collections::BTreeMap;

use openssl::hash::MessageDigest;
use openssl::pkey::PKey;
use openssl::sign::Signer;

use crate::nanocloud::logger::log_warn;
use crate::nanocloud::secrets::error::SecretError;
use crate::nanocloud::secrets::record::{
    build_associated_data, build_payload, decode_payload, encode_payload, SecretCipherPayload,
    SecretRecordMetadata,
};
use crate::nanocloud::secrets::SecretMaterial;
use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::security::kms::ENCRYPTED_BLOB_PREFIX;
use crate::nanocloud::util::security::EncryptionKey;

const SECRETS_COMPONENT: &str = "secrets";

/// Computes an HMAC-SHA256 digest over secret data.
///
/// The digest is computed over the data entries in sorted key order:
/// `key1=value1\nkey2=value2\n...`
pub fn compute_digest(
    data: &BTreeMap<String, String>,
    mac_key: &[u8],
) -> Result<String, SecretError> {
    let mut buffer = Vec::new();
    for (key, value) in data {
        buffer.extend_from_slice(key.as_bytes());
        buffer.push(b'=');
        buffer.extend_from_slice(value.as_bytes());
        buffer.push(b'\n');
    }

    let pkey = PKey::hmac(mac_key).map_err(|e| {
        SecretError::Crypto(with_context(e, "Failed to initialise HMAC key").to_string())
    })?;

    let mut signer = Signer::new(MessageDigest::sha256(), &pkey).map_err(|e| {
        SecretError::Crypto(with_context(e, "Failed to initialise HMAC signer").to_string())
    })?;

    signer
        .update(&buffer)
        .map_err(|e| SecretError::Crypto(with_context(e, "Failed to compute HMAC").to_string()))?;

    let digest = signer
        .sign_to_vec()
        .map_err(|e| SecretError::Crypto(with_context(e, "Failed to finalise HMAC").to_string()))?;

    Ok(digest.iter().map(|byte| format!("{:02x}", byte)).collect())
}

/// Encrypts a secret and returns (ciphertext, wrapped_key, digest).
pub fn encrypt_secret(
    record_key: &str,
    secret: &SecretMaterial,
) -> Result<(String, String, String), SecretError> {
    let payload = build_payload(
        secret.type_name.clone(),
        secret.immutable,
        secret.data.clone(),
    );

    let plaintext = encode_payload(&payload).map_err(|e| {
        SecretError::Encode(with_context(e, "Failed to encode secret payload").to_string())
    })?;

    let metadata = SecretRecordMetadata {
        namespace: secret.namespace.clone(),
        name: secret.name.clone(),
        type_name: secret.type_name.clone(),
        immutable: secret.immutable,
        resource_version: secret.resource_version.clone(),
    };

    let data_key = EncryptionKey::new(None);
    let associated_data = build_associated_data(record_key, &metadata).map_err(|e| {
        SecretError::Encode(with_context(e, "Failed to build associated data").to_string())
    })?;

    let ciphertext = data_key
        .encrypt_with_context(&plaintext, &associated_data)
        .map_err(|e| {
            SecretError::Crypto(with_context(e, "Failed to encrypt secret payload").to_string())
        })?;

    let wrapped_key = data_key
        .wrap()
        .map_err(|e| SecretError::Crypto(with_context(e, "Failed to wrap data key").to_string()))?;

    let digest = compute_digest(&payload.data, data_key.key_bytes())?;

    Ok((ciphertext, wrapped_key, digest))
}

/// Decrypts a secret and verifies its integrity.
///
/// Returns the decrypted payload and computed digest on success.
/// Returns an error if decryption fails or the digest does not match.
pub fn decrypt_and_verify(
    record_key: &str,
    metadata: &SecretRecordMetadata,
    ciphertext: &str,
    wrapped_key: &str,
    expected_digest: &str,
) -> Result<(SecretCipherPayload, String), SecretError> {
    // Unwrap the data key
    let wrapped_key_string = wrapped_key.to_string();
    let encryption_key = EncryptionKey::unwrap(&wrapped_key_string).map_err(|e| {
        SecretError::Crypto(with_context(e, "Failed to unwrap data key").to_string())
    })?;

    // Build associated data for authenticated decryption
    let associated_data = build_associated_data(record_key, metadata).map_err(|e| {
        SecretError::Encode(with_context(e, "Failed to build associated data").to_string())
    })?;

    // Attempt decryption with associated data, falling back to legacy decryption
    let ciphertext_string = ciphertext.to_string();
    let plaintext = match encryption_key.decrypt_with_context(&ciphertext_string, &associated_data)
    {
        Ok(value) => value,
        Err(err) if ciphertext.starts_with(ENCRYPTED_BLOB_PREFIX) => {
            // Fallback for legacy secrets without associated data
            encryption_key.decrypt(&ciphertext_string).map_err(|_| {
                SecretError::Crypto(
                    with_context(err, "Failed to decrypt secret payload").to_string(),
                )
            })?
        }
        Err(err) => {
            return Err(SecretError::Crypto(
                with_context(err, "Failed to decrypt secret payload").to_string(),
            ))
        }
    };

    // Decode the payload
    let payload: SecretCipherPayload = decode_payload(&plaintext).map_err(|e| {
        SecretError::Decode(with_context(e, "Failed to decode decrypted payload").to_string())
    })?;

    // Verify integrity by recomputing the digest
    let computed_digest = compute_digest(&payload.data, encryption_key.key_bytes())?;
    if computed_digest != expected_digest {
        // Log integrity failure with namespace/name only - no sensitive data
        log_integrity_failure(record_key, metadata, "HMAC digest mismatch");
        return Err(SecretError::Integrity {
            key: record_key.to_string(),
            message: "HMAC digest mismatch".to_string(),
        });
    }

    Ok((payload, computed_digest))
}

/// Logs an integrity failure for tamper detection.
///
/// Logs only namespace/name to avoid exposing sensitive data.
/// This function is called when HMAC verification fails, which may indicate
/// tampering or data corruption.
fn log_integrity_failure(record_key: &str, metadata: &SecretRecordMetadata, reason: &str) {
    log_warn(
        SECRETS_COMPONENT,
        "Secret integrity check failed - possible tampering detected",
        &[
            ("namespace", &metadata.namespace),
            ("name", &metadata.name),
            ("reason", reason),
            ("key", record_key),
        ],
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_consistency() {
        let mut data = BTreeMap::new();
        data.insert("a".to_string(), "1".to_string());
        data.insert("b".to_string(), "2".to_string());

        let key = b"test-key-32-bytes-long-exactly!!";

        let digest1 = compute_digest(&data, key).expect("digest should succeed");
        let digest2 = compute_digest(&data, key).expect("digest should succeed");

        assert_eq!(digest1, digest2);
    }

    #[test]
    fn digest_changes_with_data() {
        let mut data1 = BTreeMap::new();
        data1.insert("key".to_string(), "value1".to_string());

        let mut data2 = BTreeMap::new();
        data2.insert("key".to_string(), "value2".to_string());

        let key = b"test-key-32-bytes-long-exactly!!";

        let digest1 = compute_digest(&data1, key).expect("digest should succeed");
        let digest2 = compute_digest(&data2, key).expect("digest should succeed");

        assert_ne!(digest1, digest2);
    }

    #[test]
    fn digest_changes_with_key() {
        let mut data = BTreeMap::new();
        data.insert("key".to_string(), "value".to_string());

        let key1 = b"test-key-32-bytes-long-exactly!!";
        let key2 = b"different-key-32-bytes-exactly!!";

        let digest1 = compute_digest(&data, key1).expect("digest should succeed");
        let digest2 = compute_digest(&data, key2).expect("digest should succeed");

        assert_ne!(digest1, digest2);
    }

    #[test]
    fn digest_order_independence() {
        // BTreeMap maintains sorted order, so insertion order shouldn't matter
        let mut data1 = BTreeMap::new();
        data1.insert("z".to_string(), "1".to_string());
        data1.insert("a".to_string(), "2".to_string());

        let mut data2 = BTreeMap::new();
        data2.insert("a".to_string(), "2".to_string());
        data2.insert("z".to_string(), "1".to_string());

        let key = b"test-key-32-bytes-long-exactly!!";

        let digest1 = compute_digest(&data1, key).expect("digest should succeed");
        let digest2 = compute_digest(&data2, key).expect("digest should succeed");

        assert_eq!(digest1, digest2);
    }
}
