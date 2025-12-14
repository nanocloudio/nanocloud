#![cfg(feature = "keyspace-bench")]

//! Criterion benchmarks for Keyspace hot paths.
//!
//! Run with: `cargo bench --features keyspace-bench keyspace`

use criterion::{criterion_group, criterion_main, Criterion};
use nanocloud::nanocloud::util::Keyspace;
use std::env;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::TempDir;

struct KeyspaceBenchEnv {
    _dir: TempDir,
    keyspace: Keyspace,
}

impl KeyspaceBenchEnv {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("keyspace bench tempdir");
        let root = dir.path().join("keyspace");
        fs::create_dir_all(&root).expect("keyspace bench root");
        let lock_file = dir.path().join("lockfile");
        env::set_var("NANOCLOUD_KEYSPACE", &root);
        env::set_var("NANOCLOUD_LOCK_FILE", &lock_file);

        Self {
            _dir: dir,
            keyspace: Keyspace::new("bench"),
        }
    }
}

fn bench_put(c: &mut Criterion) {
    let env = KeyspaceBenchEnv::new();
    let counter = AtomicUsize::new(0);
    c.bench_function("keyspace_put", |b| {
        b.iter(|| {
            let id = counter.fetch_add(1, Ordering::Relaxed);
            let key = format!("/bench/{}", id);
            env.keyspace.put(&key, "value").expect("put should succeed");
        })
    });
}

fn bench_get_optional(c: &mut Criterion) {
    let env = KeyspaceBenchEnv::new();
    env.keyspace
        .put("/bench/read", "value")
        .expect("seed read key");
    c.bench_function("keyspace_get_optional", |b| {
        b.iter(|| {
            env.keyspace
                .get_optional("/bench/read")
                .expect("get should succeed")
                .expect("value present");
        })
    });
}

criterion_group!(keyspace_benches, bench_put, bench_get_optional);
criterion_main!(keyspace_benches);
