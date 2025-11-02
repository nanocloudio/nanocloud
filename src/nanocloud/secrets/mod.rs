use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use openssl::hash::MessageDigest;
use openssl::pkey::PKey;
use openssl::sign::Signer;
use serde::{Deserialize, Serialize};

use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::util::security::kms::ENCRYPTED_BLOB_PREFIX;
use crate::nanocloud::util::security::EncryptionKey;
use crate::nanocloud::util::Keyspace;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretMaterial {
    pub namespace: String,
    pub name: String,
    pub type_name: String,
    pub immutable: bool,
    pub data: BTreeMap<String, String>,
    pub resource_version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredSecret {
    pub secret: SecretMaterial,
    pub digest: String,
    pub created_at: DateTime<Utc>,
}

pub struct KeyspaceSecretStore {
    keyspace: Arc<Keyspace>,
}

impl KeyspaceSecretStore {
    pub fn new() -> Self {
        Self {
            keyspace: Arc::new(Keyspace::new("secrets")),
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

impl KeyspaceSecretStore {
    pub fn get(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<StoredSecret>, Box<dyn Error + Send + Sync>> {
        let key = KeyspaceSecretStore::record_key(namespace, name);
        let raw_record = match self.keyspace.get(&key) {
            Ok(value) => value,
            Err(err) => {
                if is_not_found(err.as_ref()) {
                    return Ok(None);
                }
                return Err(with_context(err, "Failed to load encrypted secret"));
            }
        };

        let record: SecretStoreRecord = serde_json::from_str(&raw_record)
            .map_err(|e| with_context(e, "Failed to decode encrypted secret record"))?;
        let created_at = DateTime::parse_from_rfc3339(&record.created_at)
            .map_err(|e| with_context(e, "Invalid secret creation timestamp"))?
            .with_timezone(&Utc);

        let encryption_key = EncryptionKey::unwrap(&record.wrapped_key)
            .map_err(|e| with_context(e, "Failed to unwrap secret data key"))?;
        let associated_data = secret_associated_data(
            &key,
            &record.metadata.namespace,
            &record.metadata.name,
            &record.metadata.type_name,
            record.metadata.immutable,
            record.metadata.resource_version.as_deref(),
        )?;
        let plaintext = match encryption_key
            .decrypt_with_context(&record.ciphertext, &associated_data)
        {
            Ok(value) => value,
            Err(err) if record.ciphertext.starts_with(ENCRYPTED_BLOB_PREFIX) => encryption_key
                .decrypt(&record.ciphertext)
                .map_err(|_| with_context(err, "Failed to decrypt secret payload with context"))?,
            Err(err) => return Err(with_context(err, "Failed to decrypt secret payload")),
        };

        let payload: SecretCipherPayload = serde_json::from_slice(&plaintext)
            .map_err(|e| with_context(e, "Failed to deserialize decrypted secret payload"))?;

        let computed_digest = compute_digest(&payload.data, encryption_key.key_bytes())
            .map_err(|e| with_context(e, "Failed to recompute secret digest"))?;
        if computed_digest != record.digest {
            return Err(with_context(
                new_error("Secret digest mismatch"),
                format!("Encrypted secret '{}' failed HMAC verification", key),
            ));
        }

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
}

#[derive(Debug, Serialize, Deserialize)]
struct SecretStoreRecord {
    metadata: SecretRecordMetadata,
    ciphertext: String,
    wrapped_key: String,
    digest: String,
    created_at: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct SecretRecordMetadata {
    namespace: String,
    name: String,
    #[serde(rename = "type")]
    type_name: String,
    immutable: bool,
    resource_version: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SecretCipherPayload {
    #[serde(rename = "type")]
    type_name: String,
    immutable: bool,
    data: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct SecretAssociatedData<'a> {
    record_key: &'a str,
    namespace: &'a str,
    name: &'a str,
    #[serde(rename = "type")]
    type_name: &'a str,
    immutable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    resource_version: Option<&'a str>,
}

fn compute_digest(
    data: &BTreeMap<String, String>,
    mac_key: &[u8],
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let mut buffer = Vec::new();
    for (key, value) in data {
        buffer.extend_from_slice(key.as_bytes());
        buffer.push(b'=');
        buffer.extend_from_slice(value.as_bytes());
        buffer.push(b'\n');
    }
    let pkey = PKey::hmac(mac_key)
        .map_err(|e| with_context(e, "Failed to initialise HMAC key for secret digest"))?;
    let mut signer = Signer::new(MessageDigest::sha256(), &pkey)
        .map_err(|e| with_context(e, "Failed to initialise HMAC signer for secret digest"))?;
    signer
        .update(&buffer)
        .map_err(|e| with_context(e, "Failed to compute secret digest HMAC"))?;
    let digest = signer
        .sign_to_vec()
        .map_err(|e| with_context(e, "Failed to finalise secret digest HMAC"))?;
    Ok(digest.iter().map(|byte| format!("{:02x}", byte)).collect())
}

fn secret_associated_data(
    record_key: &str,
    namespace: &str,
    name: &str,
    type_name: &str,
    immutable: bool,
    resource_version: Option<&str>,
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let aad = SecretAssociatedData {
        record_key,
        namespace,
        name,
        type_name,
        immutable,
        resource_version,
    };
    serde_json::to_vec(&aad).map_err(|e| with_context(e, "Failed to encode secret associated data"))
}

fn is_not_found(err: &dyn Error) -> bool {
    err.to_string().contains("Value file not found")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::util::security::SecureAssets;
    use crate::nanocloud::util::Keyspace;
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
        let stored = write_secret(secret.clone()).expect("expected secret write to succeed");

        assert_eq!(stored.secret.data.len(), 2);
        assert!(!stored.digest.is_empty());

        let store = KeyspaceSecretStore::new();
        let fetched = store
            .get(&secret.namespace, &secret.name)
            .expect("expected secret get to succeed")
            .expect("secret should exist");
        assert_eq!(fetched.secret.data, secret.data);

        delete_secret(&secret.namespace, &secret.name).expect("expected delete to succeed");

        let missing = store
            .get(&secret.namespace, &secret.name)
            .expect("expected get after delete to succeed");
        assert!(missing.is_none());

        env::remove_var("NANOCLOUD_KEYSPACE");
        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    fn write_secret(secret: SecretMaterial) -> Result<StoredSecret, Box<dyn Error + Send + Sync>> {
        let created_at = Utc::now();
        let record_key = KeyspaceSecretStore::record_key(&secret.namespace, &secret.name);
        let (ciphertext, wrapped_key, digest) = encrypt_secret_payload(&record_key, &secret)?;

        let record = SecretStoreRecord {
            metadata: SecretRecordMetadata {
                namespace: secret.namespace.clone(),
                name: secret.name.clone(),
                type_name: secret.type_name.clone(),
                immutable: secret.immutable,
                resource_version: secret.resource_version.clone(),
            },
            ciphertext,
            wrapped_key,
            digest: digest.clone(),
            created_at: created_at.to_rfc3339(),
        };

        let serialized = serde_json::to_string_pretty(&record)
            .map_err(|e| with_context(e, "Failed to encode encrypted secret"))?;
        Keyspace::new("secrets")
            .put(&record_key, &serialized)
            .map_err(|e| with_context(e, "Failed to persist encrypted secret"))?;

        Ok(StoredSecret {
            secret,
            digest,
            created_at,
        })
    }

    fn delete_secret(namespace: &str, name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = KeyspaceSecretStore::record_key(namespace, name);
        match Keyspace::new("secrets").delete(&key) {
            Ok(_) => Ok(()),
            Err(err) => Err(with_context(err, "Failed to delete encrypted secret")),
        }
    }

    fn encrypt_secret_payload(
        record_key: &str,
        secret: &SecretMaterial,
    ) -> Result<(String, String, String), Box<dyn Error + Send + Sync>> {
        let payload = SecretCipherPayload {
            type_name: secret.type_name.clone(),
            immutable: secret.immutable,
            data: secret.data.clone(),
        };

        let plaintext = serde_json::to_vec(&payload)
            .map_err(|e| with_context(e, "Failed to encode secret payload"))?;

        let data_key = EncryptionKey::new(None);
        let associated_data = super::secret_associated_data(
            record_key,
            &secret.namespace,
            &secret.name,
            &secret.type_name,
            secret.immutable,
            secret.resource_version.as_deref(),
        )?;
        let ciphertext = data_key
            .encrypt_with_context(&plaintext, &associated_data)
            .map_err(|e| with_context(e, "Failed to encrypt secret payload"))?;
        let wrapped_key = data_key
            .wrap()
            .map_err(|e| with_context(e, "Failed to wrap secret data key"))?;

        let digest = super::compute_digest(&payload.data, data_key.key_bytes())
            .map_err(|e| with_context(e, "Failed to compute secret digest"))?;

        Ok((ciphertext, wrapped_key, digest))
    }
}
