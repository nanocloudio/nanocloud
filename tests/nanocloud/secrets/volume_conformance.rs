use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use nanocloud::nanocloud::k8s::pod::{SecretVolumeSource, VolumeSpec};
use nanocloud::nanocloud::secrets::{KeyspaceSecretStore, SecretMaterial};
use nanocloud::nanocloud::test_support::{keyspace_lock, test_output_dir};
use nanocloud::nanocloud::util::security::SecureAssets;
use crate::support::secrets::{materialize_secret_volume, write_secret};

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set_path(key: &'static str, value: &Path) -> Self {
        let previous = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(prev) = self.previous.as_ref() {
            std::env::set_var(self.key, prev);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

fn persist_secret(data: BTreeMap<String, String>) {
    let secret = SecretMaterial {
        namespace: "default".to_string(),
        name: "db-login".to_string(),
        type_name: "Opaque".to_string(),
        immutable: false,
        data,
        resource_version: None,
    };
    write_secret(secret).expect("secret material persisted to keyspace");
}

fn prepare_secret_test_env(component: &str) -> (TempEnv, PathBuf) {
    let base = test_output_dir(component);
    let keyspace_dir = base.join("keyspace");
    let assets_dir = base.join("assets");
    let lock_file = base.join("lock");
    let secret_root = base.join("secret-volumes");

    fs::create_dir_all(&keyspace_dir).expect("create keyspace dir");
    fs::create_dir_all(&assets_dir).expect("create assets dir");
    fs::create_dir_all(&secret_root).expect("create secret root");
    File::create(&lock_file).expect("create lock file");

    SecureAssets::generate(&assets_dir, false).expect("generate secure assets");

    let keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", &keyspace_dir);
    let lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", &lock_file);
    let assets_guard = EnvGuard::set_path("NANOCLOUD_SECURE_ASSETS", &assets_dir);
    let secret_guard = EnvGuard::set_path("NANOCLOUD_SECRET_VOLUME_ROOT", &secret_root);

    (
        TempEnv {
            _keyspace_guard: keyspace_guard,
            _lock_guard: lock_guard,
            _assets_guard: assets_guard,
            _secret_guard: secret_guard,
        },
        secret_root,
    )
}

struct TempEnv {
    _keyspace_guard: EnvGuard,
    _lock_guard: EnvGuard,
    _assets_guard: EnvGuard,
    _secret_guard: EnvGuard,
}

#[test]
fn secret_volume_materialization_creates_expected_files() {
    let _lock = keyspace_lock().lock();
    let (_env, secret_root) = prepare_secret_test_env("secret-volume-materialization");

    let store = KeyspaceSecretStore::new();
    let mut data = BTreeMap::new();
    data.insert("username".to_string(), "service-user".to_string());
    data.insert("password".to_string(), "s3cr3t!".to_string());
    persist_secret(data);

    let mut volume = VolumeSpec {
        name: "app-secrets".into(),
        secret: Some(SecretVolumeSource {
            secret_name: "db-login".into(),
            items: HashMap::from([
                ("username".into(), "creds/user.txt".into()),
                ("password".into(), "creds/pass.txt".into()),
            ]),
            optional: None,
        }),
        ..Default::default()
    };

    materialize_secret_volume("default", "svc-api", &mut volume)
        .expect("materialize secret volume");

    let host_path = volume
        .host_path
        .as_ref()
        .expect("host path assigned")
        .path
        .clone();
    let host_dir = PathBuf::from(&host_path);
    assert!(host_dir.starts_with(&secret_root));
    assert!(volume.secret.is_none(), "secret source should be cleared");

    let user_path = host_dir.join("creds/user.txt");
    let pass_path = host_dir.join("creds/pass.txt");
    let username = fs::read_to_string(&user_path).expect("read username file");
    let password = fs::read_to_string(&pass_path).expect("read password file");
    assert_eq!(username, "service-user");
    assert_eq!(password, "s3cr3t!");

    let mode = fs::metadata(&pass_path)
        .expect("stat pass file")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o440, "secret files should be read-only");
}

#[test]
fn optional_secret_volume_suppresses_missing_secret() {
    let _lock = keyspace_lock().lock();
    let (_env, secret_root) = prepare_secret_test_env("secret-volume-optional");

    let mut volume = VolumeSpec {
        name: "optional-secrets".into(),
        secret: Some(SecretVolumeSource {
            secret_name: "missing".into(),
            items: HashMap::new(),
            optional: Some(true),
        }),
        ..Default::default()
    };

    materialize_secret_volume("default", "svc-api", &mut volume)
        .expect("optional secret should not error");

    assert!(
        volume.host_path.is_none(),
        "optional volume should not bind a host path when secret is missing"
    );

    let mount_dir = secret_root.join("svc-api").join("optional-secrets");
    assert!(
        !mount_dir.exists(),
        "optional secret volume directory should be cleaned up"
    );
}
