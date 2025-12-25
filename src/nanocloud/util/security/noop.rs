use std::error::Error;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;

use super::kms::{
    self, EncryptedDataKey, GeneratedDataKey, KeyManagementService, ENVELOPE_VERSION,
};
use crate::nanocloud::util::error::new_error;

const NOOP_KEY_ID: &str = "noop-test-kms";

#[derive(Default)]
struct NoOpKms;

impl NoOpKms {
    fn encode_envelope(
        &self,
        plaintext_key: &[u8],
    ) -> Result<EncryptedDataKey, Box<dyn Error + Send + Sync>> {
        Ok(EncryptedDataKey {
            version: ENVELOPE_VERSION,
            key_id: NOOP_KEY_ID.to_string(),
            ciphertext: STANDARD.encode(plaintext_key),
        })
    }
}

impl KeyManagementService for NoOpKms {
    fn generate_data_key(&self) -> Result<GeneratedDataKey, Box<dyn Error + Send + Sync>> {
        let plaintext = vec![0_u8; 32];
        let envelope = self.encode_envelope(&plaintext)?;
        Ok(GeneratedDataKey {
            plaintext,
            envelope,
        })
    }

    fn encrypt_data_key(
        &self,
        plaintext_key: &[u8],
    ) -> Result<EncryptedDataKey, Box<dyn Error + Send + Sync>> {
        self.encode_envelope(plaintext_key)
    }

    fn decrypt_data_key(
        &self,
        envelope: &EncryptedDataKey,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        if envelope.key_id != NOOP_KEY_ID {
            return Err(new_error(format!(
                "Unexpected key identifier '{}' (expected '{}')",
                envelope.key_id, NOOP_KEY_ID
            )));
        }
        STANDARD
            .decode(envelope.ciphertext.as_bytes())
            .map_err(|e| new_error(format!("Failed to decode noop ciphertext: {e}")))
    }

    fn default_key_id(&self) -> Option<String> {
        Some(NOOP_KEY_ID.to_string())
    }
}

/// Installs a deterministic, no-op KMS implementation for tests.
///
/// The helper is gated behind the `security-test-noop` feature so production
/// builds continue to use the full OpenSSL-backed helpers.
pub fn install_noop_security_helpers() {
    kms::override_global_kms_for_tests(Arc::new(NoOpKms));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::util::security::EncryptionKey;

    #[test]
    fn noop_helpers_produce_stable_keys() {
        install_noop_security_helpers();

        let key = EncryptionKey::new(None);
        assert_eq!(key.key_bytes().len(), 32);

        let wrapped = key.wrap().expect("wrap key");
        let unwrapped = EncryptionKey::unwrap(&wrapped).expect("unwrap key");
        assert_eq!(unwrapped.key_bytes(), key.key_bytes());
    }
}
