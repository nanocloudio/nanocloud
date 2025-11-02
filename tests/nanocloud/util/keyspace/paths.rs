use std::env;
use std::path::PathBuf;

use nanocloud::nanocloud::test_support::keyspace_lock;
use nanocloud::nanocloud::util::Keyspace;
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
fn allowed_keys_are_persisted() {
    let _guard = keyspace_lock().lock();

    let temp = TempDir::new().expect("tempdir");
    let keyspace_dir = temp.path().join("keyspace");
    let lock_file = temp.path().join("lockfile");
    let _keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", keyspace_dir.clone());
    let _lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", lock_file);

    let keyspace = Keyspace::new("paths");

    let allowed = [
        "/token",
        "/alpha.numeric-123",
        "/MIXED.Case-Name",
        "/nested/path-segment",
    ];

    for key in allowed {
        keyspace.put(key, "value").expect("allowed key persisted");
    }

    let nested_value = keyspace
        .get("/nested/path-segment")
        .expect("nested value retrieved");
    assert_eq!(nested_value, "value");

    for key in allowed {
        let mut on_disk = keyspace_dir.join("paths");
        for segment in key.trim_start_matches('/').split('/') {
            on_disk = on_disk.join(segment);
        }
        assert!(
            on_disk.join("_value_").exists(),
            "expected value file for '{}'",
            key
        );
    }
}

#[test]
fn disallowed_keys_are_rejected() {
    let _guard = keyspace_lock().lock();

    let temp = TempDir::new().expect("tempdir");
    let keyspace_dir = temp.path().join("keyspace");
    let lock_file = temp.path().join("lockfile");
    let _keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", keyspace_dir);
    let _lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", lock_file);

    let keyspace = Keyspace::new("paths");

    let invalid = [
        "",
        "relative/path",
        "/trailing/",
        "/contains_underscore",
        "/unicode/雪",
        "/dotdot/../escape",
    ];

    for key in invalid {
        let result = keyspace.put(key, "value");
        assert!(result.is_err(), "invalid key '{key}' should be rejected");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("Key path"),
            "unexpected error for '{key}': {err}"
        );
    }
}

#[test]
fn excessive_depth_is_rejected() {
    let _guard = keyspace_lock().lock();

    let temp = TempDir::new().expect("tempdir");
    let keyspace_dir = temp.path().join("keyspace");
    let lock_file = temp.path().join("lockfile");
    let _keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", keyspace_dir);
    let _lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", lock_file);

    let keyspace = Keyspace::new("paths");

    let deep_key = format!(
        "/{}",
        std::iter::repeat_n("seg", 17).collect::<Vec<_>>().join("/")
    );
    let err = keyspace
        .put(&deep_key, "value")
        .expect_err("deep key rejected");
    assert!(
        err.to_string().contains("max depth"),
        "unexpected error message: {err}"
    );

    let long_segment = format!("/{}", "a".repeat(513));
    let err = keyspace
        .put(&long_segment, "value")
        .expect_err("long key rejected");
    assert!(
        err.to_string().contains("max length"),
        "unexpected error message: {err}"
    );
}
