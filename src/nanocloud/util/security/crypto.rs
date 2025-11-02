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
use openssl::rand::rand_bytes;
use openssl::rsa::Padding;
use openssl::symm::Cipher;
use std::error::Error;
use std::iter;
use std::sync::OnceLock;

use super::assets::load_secret_key;
use super::kms::{self, EncryptedDataKey, ENCRYPTED_BLOB_PREFIX};
use crate::nanocloud::util::error::with_context;

pub struct EncryptionKey {
    key: Vec<u8>,
    envelope: OnceLock<EncryptedDataKey>,
}

impl EncryptionKey {
    pub fn new(key: Option<Vec<u8>>) -> Self {
        match key {
            Some(bytes) => Self::with_plaintext(bytes),
            None => {
                if let Ok(super::kms::GeneratedDataKey {
                    plaintext,
                    envelope,
                }) = super::kms::global_kms().generate_data_key()
                {
                    let encryption_key = EncryptionKey {
                        key: plaintext,
                        envelope: OnceLock::new(),
                    };
                    let _ = encryption_key.envelope.set(envelope);
                    encryption_key
                } else {
                    let mut random = vec![0u8; 32];
                    rand_bytes(&mut random).expect("Failed to generate random key material");
                    EncryptionKey {
                        key: random,
                        envelope: OnceLock::new(),
                    }
                }
            }
        }
    }

    fn with_plaintext(bytes: Vec<u8>) -> Self {
        EncryptionKey {
            key: bytes,
            envelope: OnceLock::new(),
        }
    }

    pub fn wrap(&self) -> Result<String, Box<dyn Error + Send + Sync>> {
        if let Some(envelope) = self.envelope.get() {
            return kms::encode_encrypted_key(envelope);
        }
        let generated = super::kms::global_kms().encrypt_data_key(&self.key)?;
        if self.envelope.set(generated).is_err() {
            // Another thread initialised the envelope first; fall through to use the stored value.
        }
        let envelope = self
            .envelope
            .get()
            .expect("envelope must be set after wrap initialisation");
        kms::encode_encrypted_key(envelope)
    }

    pub fn key_id(&self) -> Option<&str> {
        self.envelope.get().map(|env| env.key_id.as_str())
    }

    pub fn encrypt(&self, plaintext: &[u8]) -> Result<String, Box<dyn Error + Send + Sync>> {
        let blob = kms::encrypt_blob(&self.key, plaintext, None)?;
        kms::encode_encrypted_blob(&blob)
    }

    #[cfg_attr(not(test), allow(dead_code))] // Prepares for context-aware secret storage; only invoked during secret writes today.
    pub fn encrypt_with_context(
        &self,
        plaintext: &[u8],
        context: &[u8],
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        let blob = kms::encrypt_blob(&self.key, plaintext, Some(context))?;
        kms::encode_encrypted_blob(&blob)
    }

    pub fn unwrap(encoded: &String) -> Result<Self, Box<dyn Error + Send + Sync>> {
        if encoded.starts_with(ENCRYPTED_BLOB_PREFIX) {
            let (envelope, _) = kms::decode_encrypted_key(encoded)?;
            let plaintext = super::kms::global_kms().decrypt_data_key(&envelope)?;
            let key = EncryptionKey {
                key: plaintext,
                envelope: OnceLock::new(),
            };
            let _ = key.envelope.set(envelope);
            return Ok(key);
        }

        let encrypted_key = STANDARD
            .decode(encoded)
            .map_err(|e| with_context(e, "Failed to decode RSA-encrypted key from base64"))?;
        let plaintext = legacy_decrypt_rsa(&encrypted_key)?;
        let kms = super::kms::global_kms();
        let envelope = kms.encrypt_data_key(&plaintext)?;
        let key = EncryptionKey {
            key: plaintext,
            envelope: OnceLock::new(),
        };
        let _ = key.envelope.set(envelope);
        Ok(key)
    }

    pub fn decrypt(
        &self,
        encrypted_base64: &String,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        if encrypted_base64.starts_with(ENCRYPTED_BLOB_PREFIX) {
            let (blob, _) = kms::decode_encrypted_blob(encrypted_base64)?;
            return kms::decrypt_blob(&self.key, &blob, None);
        }

        let encrypted_data = STANDARD
            .decode(encrypted_base64)
            .map_err(|e| with_context(e, "Failed to decode ciphertext from base64"))?;
        legacy_decrypt_aes(&self.key, &encrypted_data)
    }

    pub fn decrypt_with_context(
        &self,
        encrypted_base64: &String,
        context: &[u8],
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        if encrypted_base64.starts_with(ENCRYPTED_BLOB_PREFIX) {
            let (blob, _) = kms::decode_encrypted_blob(encrypted_base64)?;
            return kms::decrypt_blob(&self.key, &blob, Some(context));
        }
        self.decrypt(encrypted_base64)
    }

    pub fn gen_random_bytes(length: usize, charset: &str) -> Vec<u8> {
        gen_random_bytes(length, charset)
    }

    pub(crate) fn key_bytes(&self) -> &[u8] {
        &self.key
    }
}

fn legacy_decrypt_rsa(encrypted_data: &[u8]) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let secret_key =
        load_secret_key().map_err(|e| with_context(e, "Failed to load service secret key"))?;
    let rsa_key = secret_key
        .rsa()
        .map_err(|e| with_context(e, "Failed to access RSA key material"))?;
    let mut decrypted_data = vec![0; rsa_key.size() as usize];
    let len = rsa_key
        .private_decrypt(encrypted_data, &mut decrypted_data, Padding::PKCS1)
        .map_err(|e| with_context(e, "Failed to decrypt data with RSA"))?;
    decrypted_data.truncate(len);
    Ok(decrypted_data)
}

fn legacy_decrypt_aes(
    aes_key: &[u8],
    encrypted_data: &[u8],
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let cipher = Cipher::aes_256_cbc();
    openssl::symm::decrypt(cipher, aes_key, None, encrypted_data)
        .map_err(|e| with_context(e, "Failed to decrypt data with AES-256-CBC"))
}

fn gen_random_bytes(length: usize, charset: &str) -> Vec<u8> {
    let max_value: u8 = match charset {
        "hex" => 255,
        "base64" => 255,
        "charset26" => 233,
        "charset73" => 221,
        _ => panic!("Unsupported charset: {charset}"),
    };

    let raw: Vec<u8> = iter::repeat_with(|| {
        let mut buffer = [0u8; 1];
        rand_bytes(&mut buffer).expect("Failed to generate random bytes");
        buffer[0]
    })
    .filter(|&byte| byte <= max_value)
    .take(length)
    .collect();

    match charset {
        "hex" => raw
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
            .into_bytes(),
        "base64" => STANDARD.encode(raw).into_bytes(),
        "charset26" => raw
            .iter()
            .map(|byte| {
                let letters = [
                    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
                    'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
                ];
                letters[(byte % 26) as usize]
            })
            .collect::<String>()
            .into_bytes(),
        "charset73" => raw
            .iter()
            .map(|byte| {
                let letters = [
                    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
                    'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
                    'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
                    'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '!', '@',
                    '#', '$', '%', '^', '&', '*', '(', ')', '-', '_', '+', '=',
                ];
                letters[(byte % 73) as usize]
            })
            .collect::<String>()
            .into_bytes(),
        _ => panic!("Unsupported charset: {charset}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD;

    #[test]
    fn encrypt_decrypt_round_trip() {
        let key = EncryptionKey::new(None);
        let plaintext = b"super-secret";
        let ciphertext = key.encrypt(plaintext).expect("encrypt");
        let decrypted = key.decrypt(&ciphertext).expect("decrypt");
        assert_eq!(plaintext, &decrypted[..]);
    }

    #[test]
    fn legacy_ciphertext_still_decrypts() {
        let key = EncryptionKey::new(None);
        let plaintext = b"legacy-secret";
        let legacy_cipher = {
            let cipher = Cipher::aes_256_cbc();
            let ciphertext =
                openssl::symm::encrypt(cipher, &key.key, None, plaintext).expect("legacy encrypt");
            STANDARD.encode(ciphertext)
        };
        let decrypted = key.decrypt(&legacy_cipher).expect("legacy decrypt");
        assert_eq!(plaintext, &decrypted[..]);
    }
}
