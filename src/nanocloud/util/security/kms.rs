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

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use openssl::pkey::{PKey, Private};
use openssl::rand::rand_bytes;
use openssl::rsa::Padding;
use openssl::sha::sha256;
use openssl::symm::{Cipher, Crypter, Mode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};

use super::assets::load_secret_key;
use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::Config;

const ENVELOPE_VERSION: u32 = 1;
pub(crate) const ENCRYPTED_BLOB_PREFIX: &str = "v1:";
const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;

static GLOBAL_KMS: OnceLock<Arc<dyn KeyManagementService>> = OnceLock::new();
type KeyMaterial = (String, Arc<PKey<Private>>);

#[allow(dead_code)]
pub fn register_global_kms(
    provider: Arc<dyn KeyManagementService>,
) -> Result<(), Arc<dyn KeyManagementService>> {
    GLOBAL_KMS.set(provider)
}

pub fn global_kms() -> Arc<dyn KeyManagementService> {
    GLOBAL_KMS
        .get_or_init(|| Arc::new(LocalKms::new()) as Arc<dyn KeyManagementService>)
        .clone()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EncryptedDataKey {
    pub version: u32,
    pub key_id: String,
    pub ciphertext: String,
}

#[derive(Clone, Debug)]
pub struct GeneratedDataKey {
    pub plaintext: Vec<u8>,
    pub envelope: EncryptedDataKey,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EncryptedBlob {
    pub nonce: String,
    pub ciphertext: String,
    pub tag: String,
}

pub trait KeyManagementService: Send + Sync {
    fn generate_data_key(&self) -> Result<GeneratedDataKey, Box<dyn Error + Send + Sync>>;
    fn encrypt_data_key(
        &self,
        plaintext_key: &[u8],
    ) -> Result<EncryptedDataKey, Box<dyn Error + Send + Sync>>;
    fn decrypt_data_key(
        &self,
        envelope: &EncryptedDataKey,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>>;

    fn default_key_id(&self) -> Option<String> {
        None
    }
}

pub struct LocalKms {
    key_cache: RwLock<HashMap<String, Arc<PKey<Private>>>>,
    primary_key_id: RwLock<Option<String>>,
}

impl LocalKms {
    pub fn new() -> Self {
        Self {
            key_cache: RwLock::new(HashMap::new()),
            primary_key_id: RwLock::new(None),
        }
    }

    pub fn key_id(&self) -> Option<String> {
        if let Ok(guard) = self.primary_key_id.read() {
            if let Some(current) = guard.as_ref() {
                return Some(current.clone());
            }
        }
        self.primary_key().ok().map(|(id, _)| id)
    }

    fn encrypt_with_rsa(
        &self,
        plaintext: &[u8],
    ) -> Result<(String, Vec<u8>), Box<dyn Error + Send + Sync>> {
        let (key_id, secret_key) = self.primary_key()?;
        let ciphertext = encrypt_with_key(&secret_key, plaintext)?;
        Ok((key_id, ciphertext))
    }

    fn generate_plaintext_key(&self) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let mut key = vec![0u8; 32];
        rand_bytes(&mut key)
            .map_err(|e| with_context(e, "Failed to generate data key material"))?;
        Ok(key)
    }

    fn primary_key(&self) -> Result<KeyMaterial, Box<dyn Error + Send + Sync>> {
        let key =
            load_secret_key().map_err(|e| with_context(e, "Failed to load service secret key"))?;
        let fingerprint = fingerprint_key(&key)?;
        let key_arc = {
            let mut cache = self
                .key_cache
                .write()
                .map_err(|_| new_error("Failed to acquire KMS key cache lock"))?;
            if let Some(existing) = cache.get(&fingerprint) {
                existing.clone()
            } else {
                let arc = Arc::new(key.clone());
                cache.insert(fingerprint.clone(), arc.clone());
                arc
            }
        };
        if let Ok(mut guard) = self.primary_key_id.write() {
            *guard = Some(fingerprint.clone());
        }
        Ok((fingerprint, key_arc))
    }

    fn key_for_id(&self, key_id: &str) -> Result<Arc<PKey<Private>>, Box<dyn Error + Send + Sync>> {
        // Ensure the primary key cache is populated.
        if let Ok((current_id, key)) = self.primary_key() {
            if current_id == key_id {
                return Ok(key);
            }
        }

        if let Ok(cache) = self.key_cache.read() {
            if let Some(existing) = cache.get(key_id) {
                return Ok(existing.clone());
            }
        }

        if let Some(loaded) = self
            .load_additional_key(key_id)
            .map_err(|e| with_context(e, "Failed to load archived service secret key"))?
        {
            return Ok(loaded);
        }

        Err(with_context(
            new_error(format!("Unknown data key identifier '{}'", key_id)),
            "Data key identifier mismatch",
        ))
    }

    fn load_additional_key(
        &self,
        key_id: &str,
    ) -> Result<Option<Arc<PKey<Private>>>, Box<dyn Error + Send + Sync>> {
        for path in additional_key_candidates(key_id) {
            if !path.exists() {
                continue;
            }
            let key = load_key_from_path(&path)?;
            let fingerprint = fingerprint_key(&key)?;
            if fingerprint != key_id {
                continue;
            }
            let arc = Arc::new(key);
            if let Ok(mut cache) = self.key_cache.write() {
                cache.insert(key_id.to_string(), arc.clone());
            }
            return Ok(Some(arc));
        }
        Ok(None)
    }
}

impl Default for LocalKms {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyManagementService for LocalKms {
    fn generate_data_key(&self) -> Result<GeneratedDataKey, Box<dyn Error + Send + Sync>> {
        let generated = self.generate_plaintext_key()?;
        let envelope = self.encrypt_data_key(&generated)?;
        Ok(GeneratedDataKey {
            plaintext: generated,
            envelope,
        })
    }

    fn encrypt_data_key(
        &self,
        plaintext_key: &[u8],
    ) -> Result<EncryptedDataKey, Box<dyn Error + Send + Sync>> {
        let (key_id, ciphertext) = self.encrypt_with_rsa(plaintext_key)?;
        Ok(EncryptedDataKey {
            version: ENVELOPE_VERSION,
            key_id,
            ciphertext: STANDARD.encode(ciphertext),
        })
    }

    fn decrypt_data_key(
        &self,
        envelope: &EncryptedDataKey,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        if envelope.version != ENVELOPE_VERSION {
            return Err(with_context(
                new_error(format!("Unsupported data key version {}", envelope.version)),
                "Data key version mismatch",
            ));
        }
        let ciphertext = STANDARD
            .decode(&envelope.ciphertext)
            .map_err(|e| with_context(e, "Failed to decode RSA-encrypted data key"))?;
        let key = self.key_for_id(&envelope.key_id)?;
        decrypt_with_key(&key, &ciphertext)
    }

    fn default_key_id(&self) -> Option<String> {
        self.key_id()
    }
}

fn encrypt_with_key(
    key: &PKey<Private>,
    plaintext: &[u8],
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let public_key = key
        .rsa()
        .map_err(|e| with_context(e, "Failed to access RSA key material"))?;
    let mut encrypted = vec![0; public_key.size() as usize];
    let written = public_key
        .public_encrypt(plaintext, &mut encrypted, Padding::PKCS1_OAEP)
        .map_err(|e| with_context(e, "Failed to encrypt data key with RSA-OAEP"))?;
    encrypted.truncate(written);
    Ok(encrypted)
}

fn decrypt_with_key(
    key: &PKey<Private>,
    ciphertext: &[u8],
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let rsa_key = key
        .rsa()
        .map_err(|e| with_context(e, "Failed to access RSA key material"))?;
    let mut decrypted = vec![0; rsa_key.size() as usize];
    let len = rsa_key
        .private_decrypt(ciphertext, &mut decrypted, Padding::PKCS1_OAEP)
        .map_err(|e| with_context(e, "Failed to decrypt data key with RSA-OAEP"))?;
    decrypted.truncate(len);
    Ok(decrypted)
}

fn fingerprint_key(key: &PKey<Private>) -> Result<String, Box<dyn Error + Send + Sync>> {
    let public_der = key
        .public_key_to_der()
        .map_err(|e| with_context(e, "Failed to export service public key"))?;
    Ok(sha256(&public_der)
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect())
}

fn load_key_from_path(path: &Path) -> Result<PKey<Private>, Box<dyn Error + Send + Sync>> {
    let data = fs::read(path).map_err(|e| {
        with_context(
            e,
            format!("Failed to read archived secret key '{}'", path.display()),
        )
    })?;
    PKey::private_key_from_pem(&data).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to parse archived secret key PEM at '{}'",
                path.display()
            ),
        )
    })
}

fn additional_key_candidates(key_id: &str) -> Vec<PathBuf> {
    let base = Config::SecureAssets.get_path();
    vec![
        base.join(format!("secret-{key_id}.key")),
        base.join(format!("secret.{key_id}.key")),
        base.join("keys").join(format!("{key_id}.key")),
        base.join("archive").join(format!("{key_id}.key")),
    ]
}

pub fn encrypt_blob(
    key: &[u8],
    plaintext: &[u8],
    associated_data: Option<&[u8]>,
) -> Result<EncryptedBlob, Box<dyn Error + Send + Sync>> {
    let cipher = Cipher::aes_256_gcm();
    let mut nonce = vec![0u8; NONCE_SIZE];
    rand_bytes(&mut nonce)
        .map_err(|e| with_context(e, "Failed to generate nonce for AES-GCM encryption"))?;

    let mut crypter = Crypter::new(cipher, Mode::Encrypt, key, Some(&nonce))
        .map_err(|e| with_context(e, "Failed to initialise AES-GCM encrypter"))?;
    if let Some(aad) = associated_data {
        crypter
            .aad_update(aad)
            .map_err(|e| with_context(e, "Failed to process AES-GCM associated data"))?;
    }
    let mut buffer = vec![0u8; plaintext.len() + cipher.block_size()];
    let mut count = crypter
        .update(plaintext, &mut buffer)
        .map_err(|e| with_context(e, "Failed to encrypt payload with AES-GCM"))?;
    count += crypter
        .finalize(&mut buffer[count..])
        .map_err(|e| with_context(e, "Failed to finalise AES-GCM encryption"))?;
    buffer.truncate(count);

    let mut tag = vec![0u8; TAG_SIZE];
    crypter
        .get_tag(&mut tag)
        .map_err(|e| with_context(e, "Failed to extract AES-GCM tag"))?;

    Ok(EncryptedBlob {
        nonce: STANDARD.encode(nonce),
        ciphertext: STANDARD.encode(buffer),
        tag: STANDARD.encode(tag),
    })
}

pub fn decrypt_blob(
    key: &[u8],
    blob: &EncryptedBlob,
    associated_data: Option<&[u8]>,
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let cipher = Cipher::aes_256_gcm();
    let nonce = STANDARD
        .decode(&blob.nonce)
        .map_err(|e| with_context(e, "Failed to decode AES-GCM nonce"))?;
    let ciphertext = STANDARD
        .decode(&blob.ciphertext)
        .map_err(|e| with_context(e, "Failed to decode AES-GCM ciphertext"))?;
    let tag = STANDARD
        .decode(&blob.tag)
        .map_err(|e| with_context(e, "Failed to decode AES-GCM tag"))?;

    let mut crypter = Crypter::new(cipher, Mode::Decrypt, key, Some(&nonce))
        .map_err(|e| with_context(e, "Failed to initialise AES-GCM decrypter"))?;
    crypter
        .set_tag(&tag)
        .map_err(|e| with_context(e, "Failed to set AES-GCM authentication tag"))?;
    if let Some(aad) = associated_data {
        crypter
            .aad_update(aad)
            .map_err(|e| with_context(e, "Failed to process AES-GCM associated data"))?;
    }

    let mut buffer = vec![0u8; ciphertext.len() + cipher.block_size()];
    let mut count = crypter
        .update(&ciphertext, &mut buffer)
        .map_err(|e| with_context(e, "Failed to decrypt payload with AES-GCM"))?;
    count += crypter
        .finalize(&mut buffer[count..])
        .map_err(|e| with_context(e, "Failed to finalise AES-GCM decryption"))?;
    buffer.truncate(count);

    Ok(buffer)
}

pub fn encode_encrypted_blob(blob: &EncryptedBlob) -> Result<String, Box<dyn Error + Send + Sync>> {
    let payload = serde_json::to_vec(blob)
        .map_err(|e| with_context(e, "Failed to serialise encrypted blob"))?;
    Ok(format!(
        "{}{}",
        ENCRYPTED_BLOB_PREFIX,
        STANDARD.encode(payload)
    ))
}

pub fn decode_encrypted_blob(
    encoded: &str,
) -> Result<(EncryptedBlob, bool), Box<dyn Error + Send + Sync>> {
    if let Some(rest) = encoded.strip_prefix(ENCRYPTED_BLOB_PREFIX) {
        let bytes = STANDARD
            .decode(rest)
            .map_err(|e| with_context(e, "Failed to decode encrypted blob wrapper"))?;
        let blob = serde_json::from_slice(&bytes)
            .map_err(|e| with_context(e, "Failed to parse encrypted blob wrapper"))?;
        Ok((blob, true))
    } else {
        Err(with_context(
            new_error("Missing encrypted blob prefix"),
            "Encrypted blob is not in the expected format",
        ))
    }
}

pub fn encode_encrypted_key(
    envelope: &EncryptedDataKey,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let payload = serde_json::to_vec(envelope)
        .map_err(|e| with_context(e, "Failed to serialise encrypted data key"))?;
    Ok(format!(
        "{}{}",
        ENCRYPTED_BLOB_PREFIX,
        STANDARD.encode(payload)
    ))
}

pub fn decode_encrypted_key(
    encoded: &str,
) -> Result<(EncryptedDataKey, bool), Box<dyn Error + Send + Sync>> {
    if let Some(rest) = encoded.strip_prefix(ENCRYPTED_BLOB_PREFIX) {
        let bytes = STANDARD
            .decode(rest)
            .map_err(|e| with_context(e, "Failed to decode encrypted data key wrapper"))?;
        let envelope = serde_json::from_slice(&bytes)
            .map_err(|e| with_context(e, "Failed to parse encrypted data key wrapper"))?;
        Ok((envelope, true))
    } else {
        Err(with_context(
            new_error("Missing encrypted data key prefix"),
            "Encrypted data key is not in the expected format",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_blob() {
        let key = vec![42u8; 32];
        let plaintext = b"hello world";

        let blob = encrypt_blob(&key, plaintext, None).expect("encrypt");
        let encoded = encode_encrypted_blob(&blob).expect("encode");
        let (decoded, _) = decode_encrypted_blob(&encoded).expect("decode");
        let recovered = decrypt_blob(&key, &decoded, None).expect("decrypt");
        assert_eq!(recovered, plaintext);
    }
}
