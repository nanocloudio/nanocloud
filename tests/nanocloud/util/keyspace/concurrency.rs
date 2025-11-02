use std::env;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
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
