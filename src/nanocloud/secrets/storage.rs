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

//! Keyspace I/O operations with atomic writes and per-secret locking.
//!
//! This module provides:
//! - Atomic write operations via write-then-rename (provided by keyspace)
//! - Per-secret locking to prevent concurrent write races
//! - Read operations with proper error handling
//! - Keyspace root validation on initialization

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::nanocloud::secrets::error::SecretError;
use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::{is_missing_value_error, Keyspace};
use crate::nanocloud::Config;

/// Default timeout for acquiring a per-secret lock.
const LOCK_TIMEOUT: Duration = Duration::from_secs(30);

/// Global registry of per-secret locks.
fn lock_registry() -> &'static Mutex<HashMap<String, Arc<Mutex<()>>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Acquires or creates a lock for a specific secret key.
fn get_secret_lock(key: &str) -> Arc<Mutex<()>> {
    let registry = lock_registry();
    let mut guard = registry
        .lock()
        .expect("secret lock registry poisoned");
    guard
        .entry(key.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

/// RAII guard for per-secret locks with timeout support.
struct SecretLockGuard {
    _guard: std::sync::MutexGuard<'static, ()>,
}

impl SecretLockGuard {
    /// Attempts to acquire a lock for the given key with a timeout.
    fn acquire(key: &str, timeout: Duration) -> Result<Self, SecretError> {
        let lock = get_secret_lock(key);
        let start = Instant::now();

        // Try to acquire the lock with polling and timeout
        loop {
            // We need to convert the Arc<Mutex<()>> to a 'static reference
            // This is safe because the lock is stored in a static registry
            let lock_ptr = Arc::into_raw(lock.clone());
            let static_lock: &'static Mutex<()> = unsafe { &*lock_ptr };

            match static_lock.try_lock() {
                Ok(guard) => {
                    // Don't forget to re-wrap the raw pointer
                    let _ = unsafe { Arc::from_raw(lock_ptr) };
                    return Ok(SecretLockGuard { _guard: guard });
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    // Re-wrap the raw pointer
                    let _ = unsafe { Arc::from_raw(lock_ptr) };

                    if start.elapsed() >= timeout {
                        return Err(SecretError::Lock(format!(
                            "Timeout acquiring lock for secret '{}'",
                            key
                        )));
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    let _ = unsafe { Arc::from_raw(lock_ptr) };
                    return Err(SecretError::Lock(format!(
                        "Lock poisoned for secret '{}'",
                        key
                    )));
                }
            }
        }
    }
}

/// Storage layer for secrets backed by a keyspace.
pub struct SecretStorage {
    keyspace: Arc<Keyspace>,
}

impl SecretStorage {
    /// Creates a new secret storage backed by the given keyspace.
    pub fn new(keyspace: Arc<Keyspace>) -> Self {
        Self { keyspace }
    }

    /// Validates and optionally creates the keyspace root directory.
    ///
    /// This method checks:
    /// - That the keyspace root path is valid
    /// - Creates the directory if it doesn't exist (with appropriate permissions)
    /// - Verifies the directory is writable
    ///
    /// Call this during initialization to ensure the storage is ready.
    #[allow(dead_code)]
    pub fn validate_keyspace_root() -> Result<(), SecretError> {
        let keyspace_root = Config::Keyspace.get_path();
        let secrets_root = keyspace_root.join("secrets");

        // Create secrets root if it doesn't exist
        if !secrets_root.exists() {
            fs::create_dir_all(&secrets_root).map_err(|e| {
                SecretError::KeyspaceRoot(format!(
                    "Failed to create secrets keyspace root '{}': {}",
                    secrets_root.display(),
                    e
                ))
            })?;
        }

        // Verify it's a directory
        if !secrets_root.is_dir() {
            return Err(SecretError::KeyspaceRoot(format!(
                "Secrets keyspace root '{}' exists but is not a directory",
                secrets_root.display()
            )));
        }

        // Verify write permissions by attempting to create and remove a test file
        validate_directory_writable(&secrets_root)?;

        Ok(())
    }

    /// Reads a secret record from the keyspace.
    ///
    /// Returns `Ok(None)` if the secret does not exist.
    pub fn read(&self, key: &str) -> Result<Option<String>, SecretError> {
        match self.keyspace.get(key) {
            Ok(value) => Ok(Some(value)),
            Err(err) => {
                if is_not_found(err.as_ref()) {
                    Ok(None)
                } else {
                    Err(SecretError::Io(
                        with_context(err, "Failed to read secret from keyspace").to_string(),
                    ))
                }
            }
        }
    }

    /// Writes a secret record atomically with per-secret locking.
    ///
    /// The write is performed atomically via write-then-rename (handled by keyspace).
    /// A per-secret lock prevents concurrent writers from racing.
    pub fn write_atomic(&self, key: &str, value: &str) -> Result<(), SecretError> {
        // Acquire per-secret lock to prevent concurrent writes
        let _lock = SecretLockGuard::acquire(key, LOCK_TIMEOUT)?;

        // Keyspace already provides atomic writes via persist_atomically
        self.keyspace.put(key, value).map_err(|e| {
            SecretError::Io(with_context(e, "Failed to write secret to keyspace").to_string())
        })
    }

    /// Deletes a secret from the keyspace.
    ///
    /// Returns `Ok(())` even if the secret does not exist.
    pub fn delete(&self, key: &str) -> Result<(), SecretError> {
        // Acquire per-secret lock to prevent concurrent operations
        let _lock = SecretLockGuard::acquire(key, LOCK_TIMEOUT)?;

        match self.keyspace.delete(key) {
            Ok(_) => Ok(()),
            Err(err) => {
                if is_missing_value_error(err.as_ref()) {
                    Ok(())
                } else {
                    Err(SecretError::Io(
                        with_context(err, "Failed to delete secret from keyspace").to_string(),
                    ))
                }
            }
        }
    }
}

/// Checks if an error indicates a not-found condition.
fn is_not_found(err: &dyn std::error::Error) -> bool {
    err.to_string().contains("Value file not found")
}

/// Validates that a directory is writable by creating and removing a test file.
fn validate_directory_writable(dir: &Path) -> Result<(), SecretError> {
    use std::io::Write;

    let test_file = dir.join(".write_test");
    let mut file = fs::File::create(&test_file).map_err(|e| {
        SecretError::KeyspaceRoot(format!(
            "Secrets keyspace root '{}' is not writable: {}",
            dir.display(),
            e
        ))
    })?;

    file.write_all(b"test").map_err(|e| {
        let _ = fs::remove_file(&test_file);
        SecretError::KeyspaceRoot(format!(
            "Secrets keyspace root '{}' write test failed: {}",
            dir.display(),
            e
        ))
    })?;

    fs::remove_file(&test_file).map_err(|e| {
        SecretError::KeyspaceRoot(format!(
            "Failed to clean up write test file in '{}': {}",
            dir.display(),
            e
        ))
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    #[test]
    fn lock_acquisition_succeeds() {
        let key = "test/lock/success";
        let _guard = SecretLockGuard::acquire(key, Duration::from_secs(1))
            .expect("lock acquisition should succeed");
    }

    #[test]
    fn lock_reentrant_same_thread_blocks() {
        // This test verifies that the lock is not reentrant
        // (acquiring twice on the same thread would deadlock)
        // We use a short timeout to detect this
        let key = "test/lock/reentrant";
        let _guard1 = SecretLockGuard::acquire(key, Duration::from_secs(1))
            .expect("first lock should succeed");

        let result = SecretLockGuard::acquire(key, Duration::from_millis(50));
        assert!(matches!(result, Err(SecretError::Lock(_))));
    }

    #[test]
    fn concurrent_writers_serialized() {
        let key = "test/lock/concurrent";
        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        for _ in 0..5 {
            let counter = Arc::clone(&counter);
            let key = key.to_string();
            let handle = thread::spawn(move || {
                let _guard = SecretLockGuard::acquire(&key, Duration::from_secs(5))
                    .expect("lock should succeed");

                // Simulate some work
                let current = counter.load(Ordering::SeqCst);
                thread::sleep(Duration::from_millis(10));
                counter.store(current + 1, Ordering::SeqCst);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("thread should complete");
        }

        // All increments should have completed
        assert_eq!(counter.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn different_keys_not_blocked() {
        let key1 = "test/lock/key1";
        let key2 = "test/lock/key2";

        let _guard1 = SecretLockGuard::acquire(key1, Duration::from_secs(1))
            .expect("first lock should succeed");

        // Different key should acquire immediately
        let _guard2 = SecretLockGuard::acquire(key2, Duration::from_millis(50))
            .expect("second lock should succeed for different key");
    }
}
