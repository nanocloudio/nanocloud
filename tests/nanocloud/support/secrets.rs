use std::collections::BTreeMap;
use std::error::Error;

use chrono::Utc;
use nanocloud::nanocloud::engine::container;
use nanocloud::nanocloud::engine::container::{ContainerSpec, VolumeSpec};
use nanocloud::nanocloud::secrets::{SecretMaterial, StoredSecret};
use nanocloud::nanocloud::util::error::{new_error, with_context};
use nanocloud::nanocloud::util::security::EncryptionKey;
use nanocloud::nanocloud::util::Keyspace;
use openssl::hash::MessageDigest;
use openssl::pkey::PKey;
use openssl::sign::Signer;
use serde::{Deserialize, Serialize};

/// Resolves environment variables sourced from secrets/config maps without invoking the full
/// container installation flow.
pub fn resolve_env_with_secrets(
    namespace: &str,
    container: &mut ContainerSpec,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    container::resolve_env_with_secrets_for_testing(namespace, container)
}

/// Materializes a projected secret volume to the local filesystem for assertions.
pub fn materialize_secret_volume(
    namespace: &str,
    container_name: &str,
    volume: &mut VolumeSpec,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    container::materialize_secret_volume_for_testing(namespace, container_name, volume)
}

/// Persists a secret into the keyspace store using the production encoding.
pub fn write_secret(secret: SecretMaterial) -> Result<StoredSecret, Box<dyn Error + Send + Sync>> {
    let created_at = Utc::now();
    let record_key = record_key(&secret.namespace, &secret.name);
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

/// Removes a secret from the keyspace store.
pub fn delete_secret(namespace: &str, name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = record_key(namespace, name);
    match Keyspace::new("secrets").delete(&key) {
        Ok(_) => Ok(()),
        Err(err) => {
            if err.to_string().contains("Value file not found") {
                Ok(())
            } else {
                Err(with_context(err, "Failed to delete encrypted secret"))
            }
        }
    }
}

fn record_key(namespace: &str, name: &str) -> String {
    format!("/secrets/{namespace}/{name}")
}

fn encrypt_secret_payload(
    record_key: &str,
    secret: &SecretMaterial,
) -> Result<(String, String, String), Box<dyn Error + Send + Sync>> {
    let payload = SecretCipherPayload {
        r#type: secret.type_name.clone(),
        immutable: secret.immutable,
        data: secret.data.clone(),
    };

    let plaintext = serde_json::to_vec(&payload)
        .map_err(|e| with_context(e, "Failed to encode secret payload"))?;

    let data_key = EncryptionKey::new(None);
    let associated_data = secret_associated_data(
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

    let digest = compute_digest(&payload.data, data_key.key_bytes())
        .map_err(|e| with_context(e, "Failed to compute secret digest"))?;

    Ok((ciphertext, wrapped_key, digest))
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
        r#type: type_name,
        immutable,
        resource_version,
    };
    serde_json::to_vec(&aad).map_err(|e| with_context(e, "Failed to encode secret associated data"))
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

#[derive(Serialize)]
struct SecretAssociatedData<'a> {
    record_key: &'a str,
    namespace: &'a str,
    name: &'a str,
    #[serde(rename = "type")]
    r#type: &'a str,
    immutable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    resource_version: Option<&'a str>,
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
struct SecretStoreRecordMetadata {
    namespace: String,
    #[serde(rename = "type")]
    r#type: String,
    immutable: bool,
    resource_version: Option<String>,
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct SecretCipherPayload {
    #[serde(rename = "type")]
    r#type: String,
    immutable: bool,
    data: BTreeMap<String, String>,
}
