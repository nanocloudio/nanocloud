use std::env;
use std::fs;
use std::path::PathBuf;

use nanocloud::nanocloud::test_support::keyspace_lock;
use nanocloud::nanocloud::util::{is_missing_value_error, Keyspace};
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
fn keyspace_supports_basic_crud_operations() {
    let _guard = keyspace_lock().lock();

    let temp = TempDir::new().expect("tempdir");
    let keyspace_dir = temp.path().join("keyspace");
    let lock_file = temp.path().join("lockfile");
    let _keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", keyspace_dir.clone());
    let _lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", lock_file);

    let keyspace = Keyspace::new("crud");
    keyspace.put("/alpha", "one").expect("initial put succeeds");
    assert_eq!(keyspace.get("/alpha").expect("read after put"), "one");

    keyspace
        .put_with_ttl("/alpha", "two", std::time::Duration::from_secs(30))
        .expect("put with ttl");
    let (value, expiry) = keyspace
        .get_with_expiry("/alpha")
        .expect("read value + expiry");
    assert_eq!(value, "two");
    assert!(expiry.is_some());

    keyspace.delete("/alpha").expect("delete succeeds");
    let err = keyspace
        .get("/alpha")
        .expect_err("deleted key should be missing");
    assert!(
        is_missing_value_error(err.as_ref()),
        "unexpected error: {err}"
    );

    let missing_err = keyspace.get("/missing").expect_err("missing key");
    assert!(
        is_missing_value_error(missing_err.as_ref()),
        "unexpected error: {missing_err}"
    );

    // Ensure delete cleans the on-disk directory as well.
    assert!(
        !keyspace_dir.join("crud/alpha").exists(),
        "key directory should be removed after delete"
    );
}

#[test]
fn corrupted_value_files_surface_errors() {
    let _guard = keyspace_lock().lock();

    let temp = TempDir::new().expect("tempdir");
    let keyspace_dir = temp.path().join("keyspace");
    let lock_file = temp.path().join("lockfile");
    let _keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", keyspace_dir.clone());
    let _lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", lock_file);

    let corrupted_dir = keyspace_dir.join("crud/corrupted");
    fs::create_dir_all(&corrupted_dir).expect("create corrupted key dir");
    fs::write(corrupted_dir.join("_value_"), vec![0_u8, 0xff]).expect("write corrupted contents");

    let keyspace = Keyspace::new("crud");
    let err = keyspace
        .get("/corrupted")
        .expect_err("reading corrupted data must fail");
    assert!(
        err.to_string().contains("Failed to read value file"),
        "unexpected error: {err}"
    );
}
