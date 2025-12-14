use std::env;
use std::path::PathBuf;

use nanocloud::nanocloud::util::security::{EncryptionKey, SecureAssets, TlsInfo};
use openssl::x509::X509;
use tempfile::TempDir;

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set_path(key: &'static str, value: PathBuf) -> Self {
        let previous = env::var(key).ok();
        env::set_var(key, &value);
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(prev) = self.previous.as_ref() {
            env::set_var(self.key, prev);
        } else {
            env::remove_var(self.key);
        }
    }
}

#[test]
fn encryption_key_round_trip_and_rejects_tampering() {
    let key = EncryptionKey::new(Some(vec![0x11; 32]));
    let plaintext = b"nanocloud-secret-material";

    let ciphertext = key.encrypt(plaintext).expect("encrypt succeeds");
    assert_ne!(ciphertext, String::from_utf8_lossy(plaintext));

    let recovered = key.decrypt(&ciphertext).expect("decrypt succeeds");
    assert_eq!(recovered, plaintext);

    let mut tampered_bytes = ciphertext.clone().into_bytes();
    let last = tampered_bytes.len() - 1;
    tampered_bytes[last] = tampered_bytes[last].wrapping_add(1);
    let tampered = String::from_utf8(tampered_bytes).expect("ascii ciphertext");
    let err = key.decrypt(&tampered).expect_err("tampered data must fail");
    let message = err.to_string();
    assert!(
        message.contains("Failed to decode")
            || message.contains("Failed to decrypt")
            || message.contains("Failed to parse"),
        "unexpected error: {message}"
    );
}

#[test]
fn tls_info_generates_certs_with_uri_sans() {
    let temp = TempDir::new().expect("tempdir");
    let secure_dir = temp.path().join("secure");
    let _assets_guard = EnvGuard::set_path("NANOCLOUD_SECURE_ASSETS", secure_dir.clone());
    SecureAssets::generate(&secure_dir, false).expect("secure assets");

    let uri = "spiffe://node-a";
    let tls = TlsInfo::create("node-a", Some(&vec![uri.to_string()])).expect("issue cert");
    let wrapped = tls.wrap();
    assert!(!wrapped.cert.is_empty());
    assert!(!wrapped.key.is_empty());

    let cert = X509::from_pem(&tls.cert).expect("parse issued cert");
    let extensions = cert
        .subject_alt_names()
        .expect("certificate must include SAN");
    assert!(
        extensions.iter().any(|name| name.uri() == Some(uri)),
        "URI SAN '{uri}' missing from certificate"
    );
}

#[test]
fn tls_info_errors_without_secure_assets() {
    let temp = TempDir::new().expect("tempdir");
    let secure_dir = temp.path().join("secure-missing");
    let _assets_guard = EnvGuard::set_path("NANOCLOUD_SECURE_ASSETS", secure_dir);

    let err = TlsInfo::create("missing", None)
        .err()
        .expect("missing CA must error");
    let message = err.to_string();
    assert!(
        message.contains("Failed to load CA certificate")
            || message.contains("Failed to load CA private key")
            || message.contains("Secure assets directory"),
        "unexpected error: {message}"
    );
}
