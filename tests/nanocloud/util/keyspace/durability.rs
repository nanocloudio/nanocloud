use std::env;
use std::fs;
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
fn simulated_rename_failure_preserves_previous_value() {
    let _guard = keyspace_lock().lock();

    let temp = TempDir::new().expect("tempdir");
    let keyspace_dir = temp.path().join("keyspace");
    let lock_file = temp.path().join("lockfile");
    let _keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", keyspace_dir.clone());
    let _lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", lock_file);

    let keyspace = Keyspace::new("tokens");
    keyspace.put("/token", "stable").expect("initial put");

    let key_dir = keyspace_dir.join("tokens/token");
    let value_file = key_dir.join("_value_");
    assert_eq!(
        fs::read_to_string(&value_file).expect("read value file"),
        "stable"
    );

    {
        let _fail_guard = EnvGuard::set_str("NANOCLOUD_FAIL_KEYSPACE_RENAME", "1");
        let err = keyspace
            .put("/token", "new-value")
            .expect_err("rename failure not surfaced");
        assert!(
            err.to_string().contains("Simulated rename failure"),
            "unexpected error: {err}"
        );
        assert!(
            !key_dir.join("_value_.tmp").exists(),
            "temporary file should be removed on failure"
        );
    }

    assert_eq!(
        keyspace.get("/token").expect("value after failure"),
        "stable"
    );
    assert!(
        !key_dir.join("_value_.tmp").exists(),
        "temporary file should not persist after failure"
    );

    keyspace.put("/token", "fresh").expect("final put succeeds");
    assert_eq!(
        keyspace.get("/token").expect("value after recovery"),
        "fresh"
    );
}

#[test]
fn repair_removes_partial_artifacts_without_touching_committed_values() {
    let _guard = keyspace_lock().lock();

    let temp = TempDir::new().expect("tempdir");
    let keyspace_dir = temp.path().join("keyspace");
    let lock_file = temp.path().join("lockfile");
    let _keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", keyspace_dir.clone());
    let _lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", lock_file);

    let partition_dir = keyspace_dir.join("repair");
    let valid_dir = partition_dir.join("valid");
    fs::create_dir_all(&valid_dir).expect("create valid key dir");
    fs::write(valid_dir.join("_value_"), "confirmed").expect("write value");
    fs::write(valid_dir.join("_value_.tmp"), "stale").expect("write temp value");

    let orphan_dir = partition_dir.join("orphan");
    fs::create_dir_all(&orphan_dir).expect("create orphan dir");
    fs::write(orphan_dir.join("_value_.tmp"), "orphan").expect("write orphan temp");

    let expiry_only_dir = partition_dir.join("expiry_only");
    fs::create_dir_all(&expiry_only_dir).expect("create expiry dir");
    fs::write(expiry_only_dir.join("_expiry_"), "1700000000").expect("write expiry");

    let nested_dir = partition_dir.join("parent/child");
    fs::create_dir_all(&nested_dir).expect("create nested dir");
    fs::write(nested_dir.join("_value_.tmp"), "nested").expect("write nested temp");

    let keyspace = Keyspace::new("repair");
    keyspace
        .put("/healthy", "value")
        .expect("repair-triggering put");
    assert_eq!(
        keyspace.get("/healthy").expect("read healthy value"),
        "value"
    );

    assert_eq!(
        fs::read_to_string(valid_dir.join("_value_")).expect("read confirmed value"),
        "confirmed"
    );
    assert!(
        !valid_dir.join("_value_.tmp").exists(),
        "temporary file should be removed from committed key directory"
    );
    assert!(
        !orphan_dir.exists(),
        "orphan directory should be removed during repair"
    );
    assert!(
        !expiry_only_dir.exists(),
        "expiry-only directory should be removed during repair"
    );
    assert!(
        !partition_dir.join("parent").exists(),
        "nested partial directories should be removed during repair"
    );
}
