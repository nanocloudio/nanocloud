#![cfg(feature = "secrets-bench")]

//! Performance benchmarks for secret store operations.
//!
//! Run with: `cargo bench --features secrets-bench`
//!
//! These benchmarks measure:
//! - Put (encrypt + write) operations
//! - Get (read + decrypt) operations
//! - Cache hit vs miss performance
//! - Concurrent access patterns

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nanocloud::nanocloud::secrets::cache::{CacheConfig, CachedSecretStore, SecretCache};
use nanocloud::nanocloud::secrets::{KeyspaceSecretStore, SecretMaterial, StoredSecret};
use nanocloud::nanocloud::util::security::SecureAssets;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

/// Sets up a temporary environment for benchmarking.
struct BenchEnv {
    _temp_dir: tempfile::TempDir,
    store: KeyspaceSecretStore,
}

impl BenchEnv {
    fn new() -> Self {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let keyspace_dir = temp_dir.path().join("keyspace");
        std::fs::create_dir_all(&keyspace_dir).expect("failed to prepare keyspace dir");
        std::env::set_var(
            "NANOCLOUD_KEYSPACE",
            keyspace_dir.to_string_lossy().to_string(),
        );

        let assets_dir = temp_dir.path().join("assets");
        std::fs::create_dir_all(&assets_dir).expect("failed to prepare assets dir");
        SecureAssets::generate(&assets_dir, false).expect("failed to generate secure assets");
        std::env::set_var(
            "NANOCLOUD_SECURE_ASSETS",
            assets_dir.to_string_lossy().to_string(),
        );

        let store = KeyspaceSecretStore::new();

        Self {
            _temp_dir: temp_dir,
            store,
        }
    }
}

fn sample_secret(namespace: &str, name: &str, data_size: usize) -> SecretMaterial {
    let mut data = BTreeMap::new();
    // Create data entries to reach approximate size
    let value = "x".repeat(data_size.max(1));
    data.insert("data".to_string(), value);

    SecretMaterial {
        namespace: namespace.to_string(),
        name: name.to_string(),
        type_name: "Opaque".to_string(),
        immutable: false,
        data,
        resource_version: None,
    }
}

/// Benchmark put (encrypt + write) operations.
fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("secrets_put");

    for size in [64, 256, 1024, 4096].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let env = BenchEnv::new();
            let mut counter = 0u64;

            b.iter(|| {
                counter += 1;
                let secret = sample_secret("default", &format!("bench-{}", counter), size);
                env.store.put(secret).expect("put should succeed")
            })
        });
    }

    group.finish();
}

/// Benchmark get (read + decrypt) operations.
fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("secrets_get");

    for size in [64, 256, 1024, 4096].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let env = BenchEnv::new();

            // Pre-create secret
            let secret = sample_secret("default", "bench-get", size);
            env.store.put(secret).expect("put should succeed");

            b.iter(|| {
                env.store
                    .get("default", "bench-get")
                    .expect("get should succeed")
                    .expect("secret should exist")
            })
        });
    }

    group.finish();
}

/// Benchmark cache hit performance.
fn bench_cache_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("secrets_cache");

    group.bench_function("cache_hit", |b| {
        let cache = SecretCache::with_config(CacheConfig {
            ttl: Duration::from_secs(300),
            max_entries: 1000,
            enabled: true,
        });

        // Pre-populate cache
        let stored = StoredSecret {
            secret: sample_secret("default", "cached", 256),
            digest: "test-digest".to_string(),
            created_at: chrono::Utc::now(),
        };
        cache.put("default", "cached", stored);

        b.iter(|| cache.get("default", "cached").expect("should hit cache"))
    });

    group.bench_function("cache_miss", |b| {
        let cache = SecretCache::with_config(CacheConfig {
            ttl: Duration::from_secs(300),
            max_entries: 1000,
            enabled: true,
        });

        b.iter(|| {
            // Always miss - key doesn't exist
            cache.get("default", "nonexistent")
        })
    });

    group.finish();
}

/// Benchmark cached store vs direct store.
fn bench_cached_vs_direct(c: &mut Criterion) {
    let mut group = c.benchmark_group("secrets_cached_vs_direct");

    group.bench_function("direct_get", |b| {
        let env = BenchEnv::new();

        // Pre-create secret
        let secret = sample_secret("default", "bench-direct", 256);
        env.store.put(secret).expect("put should succeed");

        b.iter(|| {
            env.store
                .get("default", "bench-direct")
                .expect("get should succeed")
        })
    });

    group.bench_function("cached_get_warm", |b| {
        let env = BenchEnv::new();
        let cached_store = CachedSecretStore::with_config(
            Arc::new(env.store),
            CacheConfig {
                ttl: Duration::from_secs(300),
                max_entries: 1000,
                enabled: true,
            },
        );

        // Pre-create and warm cache
        let secret = sample_secret("default", "bench-cached", 256);
        cached_store.put(secret).expect("put should succeed");
        // Warm the cache
        cached_store.get("default", "bench-cached").expect("get should succeed");

        b.iter(|| {
            cached_store
                .get("default", "bench-cached")
                .expect("get should succeed")
        })
    });

    group.finish();
}

/// Benchmark put-get round trip.
fn bench_round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("secrets_round_trip");

    group.bench_function("put_then_get", |b| {
        let env = BenchEnv::new();
        let mut counter = 0u64;

        b.iter(|| {
            counter += 1;
            let name = format!("bench-rt-{}", counter);
            let secret = sample_secret("default", &name, 256);

            // Put
            env.store.put(secret).expect("put should succeed");

            // Get
            env.store
                .get("default", &name)
                .expect("get should succeed")
                .expect("secret should exist")
        })
    });

    group.finish();
}

/// Benchmark multiple secrets in same namespace.
fn bench_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("secrets_list");

    for count in [10, 50, 100].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            let env = BenchEnv::new();

            // Pre-create secrets
            for i in 0..count {
                let secret = sample_secret("bench-ns", &format!("secret-{}", i), 64);
                env.store.put(secret).expect("put should succeed");
            }

            b.iter(|| {
                env.store
                    .list(Some("bench-ns"))
                    .expect("list should succeed")
            })
        });
    }

    group.finish();
}

criterion_group!(
    name = secrets;
    config = Criterion::default().sample_size(50);
    targets = bench_put, bench_get, bench_cache_hit, bench_cached_vs_direct, bench_round_trip, bench_list
);
criterion_main!(secrets);
