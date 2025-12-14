#![cfg(feature = "crypto-bench")]

//! Criterion benchmarks for encryption helper hot paths.
//!
//! Run with: `cargo bench --features crypto-bench crypto`

use criterion::{criterion_group, criterion_main, Criterion};
use nanocloud::nanocloud::util::security::{EncryptionKey, SecureAssets};
use std::env;
use std::fs;
use std::sync::Arc;
use tempfile::TempDir;

struct CryptoBenchEnv {
    _dir: TempDir,
    key: Arc<EncryptionKey>,
    payload: Vec<u8>,
    ciphertext: String,
}

impl CryptoBenchEnv {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("crypto bench tempdir");
        let assets_dir = dir.path().join("secure");
        fs::create_dir_all(&assets_dir).expect("crypto bench assets");
        env::set_var(
            "NANOCLOUD_SECURE_ASSETS",
            assets_dir.to_string_lossy().to_string(),
        );
        SecureAssets::generate(&assets_dir, false).expect("generate secure assets");

        let key = Arc::new(EncryptionKey::new(None));
        let payload = vec![0xAA; 2048];
        let ciphertext = key.encrypt(&payload).expect("encrypt payload");

        Self {
            _dir: dir,
            key,
            payload,
            ciphertext,
        }
    }
}

fn bench_encrypt(c: &mut Criterion) {
    let env = CryptoBenchEnv::new();
    let key = Arc::clone(&env.key);
    let payload = env.payload.clone();
    c.bench_function("encryption_key_encrypt", move |b| {
        b.iter(|| key.encrypt(&payload).expect("encryption should succeed"))
    });
}

fn bench_decrypt(c: &mut Criterion) {
    let env = CryptoBenchEnv::new();
    let key = Arc::clone(&env.key);
    let ciphertext = env.ciphertext.clone();
    c.bench_function("encryption_key_decrypt", move |b| {
        b.iter(|| key.decrypt(&ciphertext).expect("decryption should succeed"))
    });
}

criterion_group!(crypto_benches, bench_encrypt, bench_decrypt);
criterion_main!(crypto_benches);
