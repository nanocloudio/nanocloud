use std::env;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use nanocloud::nanocloud::test_support::keyspace_lock;
use nanocloud::nanocloud::util::{Keyspace, SingleUseTokenOutcome};
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

    fn set_str(key: &'static str, value: &str) -> Self {
        let previous = env::var(key).ok();
        env::set_var(key, value);
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
fn readers_observe_latest_committed_value() {
    let _guard = keyspace_lock().lock();

    let temp = TempDir::new().expect("tempdir");
    let keyspace_dir = temp.path().join("keyspace");
    let lock_file = temp.path().join("lockfile");
    let _keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", keyspace_dir);
    let _lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", lock_file);

    let iterations = 64;
    let keyspace = Arc::new(Keyspace::new("concurrency"));
    let sync_put = Arc::new(Barrier::new(2));
    let sync_read = Arc::new(Barrier::new(2));

    let writer = {
        let keyspace = Arc::clone(&keyspace);
        let sync_put = Arc::clone(&sync_put);
        let sync_read = Arc::clone(&sync_read);
        thread::spawn(move || {
            for i in 0..iterations {
                let value = format!("value-{i}");
                keyspace.put("/shared", &value).expect("writer put");
                sync_put.wait();
                sync_read.wait();
            }
        })
    };

    for i in 0..iterations {
        sync_put.wait();
        let expected = format!("value-{i}");
        let observed = keyspace.get("/shared").expect("reader get");
        assert_eq!(observed, expected, "iteration {i}");
        sync_read.wait();
    }

    writer.join().expect("writer thread");
}

#[test]
fn single_use_tokens_resist_replay_under_contention() {
    let _guard = keyspace_lock().lock();

    let temp = TempDir::new().expect("tempdir");
    let keyspace_dir = temp.path().join("keyspace");
    let lock_file = temp.path().join("lockfile");
    let _keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", keyspace_dir);
    let _lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", lock_file);

    let keyspace = Arc::new(Keyspace::new("singleuse"));
    keyspace
        .put_with_ttl("/token", "secret", Duration::from_secs(60))
        .expect("seed single-use token");

    let threads = 8;
    let successes = Arc::new(AtomicUsize::new(0));
    let replays = Arc::new(AtomicUsize::new(0));
    let misses = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..threads {
        let keyspace = Arc::clone(&keyspace);
        let successes = Arc::clone(&successes);
        let replays = Arc::clone(&replays);
        let misses = Arc::clone(&misses);
        handles.push(thread::spawn(move || {
            let outcome = keyspace
                .consume_single_use("/token", Duration::from_secs(60))
                .expect("consume outcome");
            match outcome {
                SingleUseTokenOutcome::Consumed { .. } => {
                    successes.fetch_add(1, Ordering::SeqCst);
                }
                SingleUseTokenOutcome::Replay => {
                    replays.fetch_add(1, Ordering::SeqCst);
                }
                SingleUseTokenOutcome::NotFound => {
                    misses.fetch_add(1, Ordering::SeqCst);
                }
                other => panic!("unexpected outcome: {:?}", other),
            }
        }));
    }

    for handle in handles {
        handle.join().expect("consumer thread");
    }

    assert_eq!(successes.load(Ordering::SeqCst), 1);
    assert_eq!(
        replays.load(Ordering::SeqCst) + misses.load(Ordering::SeqCst),
        threads - 1
    );
}

#[test]
fn per_key_locking_serializes_writers_with_contention() {
    let _guard = keyspace_lock().lock();

    let temp = TempDir::new().expect("tempdir");
    let keyspace_dir = temp.path().join("keyspace");
    let lock_file = temp.path().join("lockfile");
    let _keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", keyspace_dir);
    let _lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", lock_file);
    let _per_key_guard = EnvGuard::set_str("NANOCLOUD_KEYSPACE_PER_KEY_LOCKS", "1");
    let _timeout_guard = EnvGuard::set_str("NANOCLOUD_KEYSPACE_LOCK_TIMEOUT_SECS", "5");

    let keyspace = Arc::new(Keyspace::new("per-key"));
    let writers = 4;
    let barrier = Arc::new(Barrier::new(writers));
    let expected_values: Vec<_> = (0..writers).map(|id| format!("writer-{id}")).collect();

    let mut handles = Vec::new();
    for value in expected_values.iter() {
        let value = value.clone();
        let keyspace = Arc::clone(&keyspace);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            keyspace.put("/shared", &value).expect("writer put");
        }));
    }

    for handle in handles {
        handle.join().expect("writer thread");
    }

    let observed = keyspace.get("/shared").expect("final read");
    assert!(
        expected_values
            .iter()
            .any(|candidate| candidate == &observed),
        "observed value '{observed}' was not written by any thread"
    );
}

#[test]
fn per_key_lock_timeout_errors_when_lock_held() {
    let _guard = keyspace_lock().lock();

    let temp = TempDir::new().expect("tempdir");
    let keyspace_dir = temp.path().join("keyspace");
    let lock_file = temp.path().join("lockfile");
    let _keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", keyspace_dir);
    let _lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", lock_file);
    let _per_key_guard = EnvGuard::set_str("NANOCLOUD_KEYSPACE_PER_KEY_LOCKS", "1");
    let _timeout_guard = EnvGuard::set_str("NANOCLOUD_KEYSPACE_LOCK_TIMEOUT_SECS", "0");

    let keyspace = Arc::new(Keyspace::new("per-key"));
    let barrier = Arc::new(Barrier::new(2));
    let (tx, rx) = mpsc::channel();

    for id in 0..2 {
        let keyspace = Arc::clone(&keyspace);
        let barrier = Arc::clone(&barrier);
        let tx = tx.clone();
        thread::spawn(move || {
            barrier.wait();
            let result = keyspace.put("/shared", &format!("writer-{id}"));
            tx.send(result).expect("send result");
        });
    }
    drop(tx);

    let mut successes = 0;
    let mut failures = 0;
    let mut failure_message = String::new();
    for result in rx {
        match result {
            Ok(()) => successes += 1,
            Err(err) => {
                failures += 1;
                failure_message = err.to_string();
            }
        }
    }

    assert_eq!(successes, 1, "exactly one writer should succeed");
    assert_eq!(failures, 1, "exactly one writer should time out");
    assert!(
        failure_message.contains("Failed to lock key"),
        "unexpected error: {failure_message}"
    );
    assert!(
        failure_message.contains("Timed out") || failure_message.contains("already held"),
        "contention reason missing: {failure_message}"
    );
    assert!(
        keyspace.get("/shared").is_ok(),
        "a value should still be present after contention"
    );
}

#[test]
fn large_values_remain_consistent_under_contention() {
    let _guard = keyspace_lock().lock();

    let temp = TempDir::new().expect("tempdir");
    let keyspace_dir = temp.path().join("keyspace");
    let lock_file = temp.path().join("lockfile");
    let _keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", keyspace_dir);
    let _lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", lock_file);

    let keyspace = Arc::new(Keyspace::new("large"));
    let payload_one = Arc::new("A".repeat(256 * 1024));
    let payload_two = Arc::new("B".repeat(256 * 1024));
    keyspace
        .put("/blob", payload_one.as_str())
        .expect("seed large value");

    let writer = {
        let keyspace = Arc::clone(&keyspace);
        let payload_one = Arc::clone(&payload_one);
        let payload_two = Arc::clone(&payload_two);
        thread::spawn(move || {
            for i in 0..16 {
                let next = if i % 2 == 0 {
                    payload_two.as_str()
                } else {
                    payload_one.as_str()
                };
                keyspace.put("/blob", next).expect("writer replace");
            }
        })
    };

    let reader = {
        let keyspace = Arc::clone(&keyspace);
        let payload_one = Arc::clone(&payload_one);
        let payload_two = Arc::clone(&payload_two);
        thread::spawn(move || {
            for _ in 0..64 {
                let value = keyspace.get("/blob").expect("reader get");
                if value != payload_one.as_str() && value != payload_two.as_str() {
                    panic!("observed partial data of length {}", value.len());
                }
            }
        })
    };

    writer.join().expect("writer thread");
    reader.join().expect("reader thread");
}
