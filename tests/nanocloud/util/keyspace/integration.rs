use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;

use nanocloud::nanocloud::secrets::{KeyspaceSecretStore, SecretMaterial};
use nanocloud::nanocloud::test_support::{keyspace_lock, reset_keyspace_partition_watch};
use nanocloud::nanocloud::util::security::SecureAssets;
use nanocloud::nanocloud::util::{Keyspace, KeyspaceEventType};
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

struct TestEnv {
    _temp: TempDir,
    _keyspace: EnvGuard,
    _lock: EnvGuard,
    _secure: EnvGuard,
}

fn prepare_env() -> TestEnv {
    let temp = TempDir::new().expect("tempdir");
    let base = temp.path().to_path_buf();
    let keyspace_dir = base.join("keyspace");
    let lock_file = base.join("lockfile");
    let secure_dir = base.join("secure");
    let keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", keyspace_dir);
    let lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", lock_file);
    let secure_guard = EnvGuard::set_path("NANOCLOUD_SECURE_ASSETS", secure_dir.clone());
    SecureAssets::generate(&secure_dir, false).expect("generate secure assets");
    reset_keyspace_partition_watch("secrets");
    TestEnv {
        _temp: temp,
        _keyspace: keyspace_guard,
        _lock: lock_guard,
        _secure: secure_guard,
    }
}

fn sample_secret(resource_version: Option<&str>, payload: &str) -> SecretMaterial {
    let mut data = BTreeMap::new();
    data.insert("password".into(), payload.into());
    SecretMaterial {
        namespace: "default".into(),
        name: "demo".into(),
        type_name: "Opaque".into(),
        immutable: false,
        data,
        resource_version: resource_version.map(|value| value.to_string()),
    }
}

#[test]
fn secret_store_notifies_keyspace_consumers() {
    let _lock = keyspace_lock().lock();
    let _env = prepare_env();

    let store = KeyspaceSecretStore::new();
    let keyspace = Keyspace::new("secrets");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        let mut watch = keyspace.watch("/secrets/default/demo", None);

        store
            .put(sample_secret(None, "initial"))
            .expect("persist secret");
        let added = watch.next().await.expect("added event");
        assert_eq!(added.event_type, KeyspaceEventType::Added);
        assert_eq!(added.key, "/secrets/default/demo");

        store
            .put(sample_secret(Some("2"), "updated"))
            .expect("update secret");
        let modified = watch.next().await.expect("modified event");
        assert_eq!(modified.event_type, KeyspaceEventType::Modified);

        store
            .delete("default", "demo")
            .expect("delete secret via store");
        let deleted = watch.next().await.expect("deleted event");
        assert_eq!(deleted.event_type, KeyspaceEventType::Deleted);
        assert_eq!(deleted.value, None);
    });
}

#[test]
fn secret_store_events_resume_from_resource_versions() {
    let _lock = keyspace_lock().lock();
    let _env = prepare_env();

    let store = KeyspaceSecretStore::new();
    let keyspace = Keyspace::new("secrets");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        let mut initial = keyspace.watch("/secrets/default/demo", None);
        store
            .put(sample_secret(None, "first"))
            .expect("insert first secret");
        let first = initial.next().await.expect("first event");
        assert_eq!(first.event_type, KeyspaceEventType::Added);
        let resume_version = first.resource_version;

        store
            .put(sample_secret(Some("second"), "second"))
            .expect("insert updated secret");

        let mut resumed = keyspace.watch("/secrets/default/demo", Some(resume_version));
        let resumed_event = resumed.next().await.expect("resumed event");
        assert_eq!(resumed_event.event_type, KeyspaceEventType::Modified);
        assert!(resumed_event.resource_version > resume_version);
    });
}
