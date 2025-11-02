use std::collections::BTreeMap;
use std::env;

use nanocloud::nanocloud::k8s::pod::{
    ContainerEnvVar, ContainerSpec, EnvVarSource, SecretKeySelector,
};
use nanocloud::nanocloud::secrets::{KeyspaceSecretStore, SecretMaterial};
use nanocloud::nanocloud::test_support::keyspace_lock;
use nanocloud::nanocloud::util::security::SecureAssets;
use crate::support::secrets::{resolve_env_with_secrets, write_secret};
use tempfile::TempDir;

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set_path(key: &'static str, value: impl AsRef<str>) -> Self {
        let previous = env::var(key).ok();
        env::set_var(key, value.as_ref());
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

fn materialize_secret(password: &str) {
    let mut data = BTreeMap::new();
    data.insert("password".to_string(), password.to_string());
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

#[test]
fn secret_rotation_updates_env_values() {
    let _lock = keyspace_lock().lock();

    let temp = TempDir::new().expect("tempdir");
    let keyspace_dir = temp.path().join("keyspace");
    let lock_path = temp.path().join("lock");
    let assets_dir = temp.path().join("assets");

    let _keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", keyspace_dir.to_string_lossy());
    let _lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", lock_path.to_string_lossy());
    let _assets_guard = EnvGuard::set_path("NANOCLOUD_SECURE_ASSETS", assets_dir.to_string_lossy());

    std::fs::create_dir_all(&assets_dir).expect("create secure assets dir");
    SecureAssets::generate(&assets_dir, false).expect("secure assets generated");

    let store = KeyspaceSecretStore::new();
    materialize_secret("initial-pass");

    let mut container = ContainerSpec {
        env: vec![ContainerEnvVar {
            name: "DB_PASSWORD".into(),
            value: None,
            value_from: Some(EnvVarSource {
                config_map_key_ref: None,
                secret_key_ref: Some(SecretKeySelector {
                    key: "password".into(),
                    name: Some("db-login".into()),
                    optional: None,
                }),
            }),
        }],
        ..Default::default()
    };

    resolve_env_with_secrets("default", &mut container)
        .expect("env injection should succeed");
    assert_eq!(
        container
            .env
            .first()
            .and_then(|env| env.value.clone())
            .as_deref(),
        Some("initial-pass")
    );

    // Rotate the secret payload.
    materialize_secret("rotated-pass");
    if let Some(env_var) = container.env.first_mut() {
        env_var.value = None;
        env_var.value_from = Some(EnvVarSource {
            config_map_key_ref: None,
            secret_key_ref: Some(SecretKeySelector {
                key: "password".into(),
                name: Some("db-login".into()),
                optional: None,
            }),
        });
    }

    resolve_env_with_secrets("default", &mut container)
        .expect("env injection should succeed after rotation");
    assert_eq!(
        container
            .env
            .first()
            .and_then(|env| env.value.clone())
            .as_deref(),
        Some("rotated-pass")
    );
}
