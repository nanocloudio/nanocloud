#![allow(dead_code)]

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex, MutexGuard, OnceLock,
};

pub struct KeyspaceTestLock {
    inner: Mutex<()>,
}

impl KeyspaceTestLock {
    pub const fn new() -> Self {
        Self {
            inner: Mutex::new(()),
        }
    }

    pub fn lock(&self) -> MutexGuard<'_, ()> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Default for KeyspaceTestLock {
    fn default() -> Self {
        Self::new()
    }
}

/// Global mutex used by tests that manipulate the keyspace environment to avoid
/// interfering with each other when running in parallel.
pub fn keyspace_lock() -> &'static KeyspaceTestLock {
    static LOCK: OnceLock<KeyspaceTestLock> = OnceLock::new();
    LOCK.get_or_init(KeyspaceTestLock::new)
}

fn target_dir() -> PathBuf {
    if let Ok(dir) = env::var("CARGO_TARGET_DIR") {
        PathBuf::from(dir)
    } else if let Ok(dir) = env::var("NANOCLOUD_TEST_TARGET_DIR") {
        PathBuf::from(dir)
    } else {
        env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("target")
    }
}

/// Returns a unique directory under `target/test-output/<component>/`.
/// The directory is created eagerly and returned to the caller.
pub fn test_output_dir(component: &str) -> PathBuf {
    static COUNTER: OnceLock<AtomicU64> = OnceLock::new();
    let counter = COUNTER.get_or_init(|| AtomicU64::new(0));
    let mut path = target_dir();
    path.push("test-output");
    path.push(component);
    path.push(format!(
        "pid{}-{}",
        std::process::id(),
        counter.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&path).expect("create test output directory");
    path
}
