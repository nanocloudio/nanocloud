use std::env;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use nanocloud::nanocloud::test_support::keyspace_lock;
use nanocloud::nanocloud::util::Keyspace;
use tempfile::TempDir;

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: PathBuf) -> Self {
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
fn expiring_keys_are_removed_on_access() {
    let _guard = keyspace_lock().lock();

    let temp = TempDir::new().expect("tempdir");
    let keyspace_dir = temp.path().join("keyspace");
    let lock_file = temp.path().join("lockfile");
    let _keyspace_guard = EnvGuard::set("NANOCLOUD_KEYSPACE", keyspace_dir.clone());
    let _lock_guard = EnvGuard::set("NANOCLOUD_LOCK_FILE", lock_file);

    let keyspace = Keyspace::new("tokens");
    keyspace
        .put_with_ttl("/token", "secret", Duration::from_secs(1))
        .expect("put with ttl");

    assert_eq!(keyspace.get("/token").expect("get"), "secret");

    thread::sleep(Duration::from_secs(2));
    assert!(keyspace.get("/token").is_err());

    let value_path = keyspace_dir.join("tokens/token/_value_");
    assert!(
        !value_path.exists(),
        "value file should be removed after expiry"
    );
}
