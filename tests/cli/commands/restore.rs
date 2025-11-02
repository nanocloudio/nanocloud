use nanocloud::nanocloud::cli::args::RestoreArgs;
use nanocloud::nanocloud::cli::commands::restore::handle_restore;
use nanocloud::nanocloud::csi::{CsiVolume, StoredVolume};
use nanocloud::nanocloud::engine::Snapshot;
use nanocloud::nanocloud::test_support::keyspace_lock;
use nanocloud::nanocloud::util::Keyspace;

use chrono::{SecondsFormat, Utc};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set_path(key: &'static str, value: &Path) -> Self {
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

struct SnapshotTestEnv {
    _temp: TempDir,
    backup_dir: PathBuf,
    csi_root: PathBuf,
    _keyspace_env: EnvGuard,
    _lock_env: EnvGuard,
    _backup_env: EnvGuard,
    _csi_env: EnvGuard,
}

impl SnapshotTestEnv {
    fn new() -> Self {
        let temp = TempDir::new().expect("tempdir");
        let base = temp.path();

        let keyspace_dir = base.join("keyspace");
        fs::create_dir_all(&keyspace_dir).expect("keyspace dir");

        let lock_file = base.join("lock");
        if let Some(parent) = lock_file.parent() {
            fs::create_dir_all(parent).expect("lock dir");
        }
        fs::File::create(&lock_file).expect("lock file");

        let backup_dir = base.join("backups");
        fs::create_dir_all(&backup_dir).expect("backup dir");

        let csi_root = base.join("csi");
        fs::create_dir_all(&csi_root).expect("csi dir");

        let keyspace_env = EnvGuard::set_path("NANOCLOUD_KEYSPACE", &keyspace_dir);
        let lock_env = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", &lock_file);
        let backup_env = EnvGuard::set_path("NANOCLOUD_BACKUP", &backup_dir);
        let csi_env = EnvGuard::set_path("NANOCLOUD_CSI_ROOT", &csi_root);

        Self {
            _temp: temp,
            backup_dir,
            csi_root,
            _keyspace_env: keyspace_env,
            _lock_env: lock_env,
            _backup_env: backup_env,
            _csi_env: csi_env,
        }
    }

    fn backup_dir(&self) -> &Path {
        &self.backup_dir
    }

    fn temp_path(&self) -> &Path {
        self._temp.path()
    }

    fn write_sample_volume(&self, namespace: &str, service: &str, claim: &str, contents: &str) {
        let volume_id = format!("{}-{}", service, claim);
        let volume_dir = self.csi_root.join("volumes").join(&volume_id);
        fs::create_dir_all(&volume_dir).expect("volume dir");
        fs::write(volume_dir.join("data.txt"), contents).expect("write volume data");

        let mut context = HashMap::new();
        context.insert("namespace".to_string(), namespace.to_string());
        context.insert("service".to_string(), service.to_string());
        context.insert("claim".to_string(), claim.to_string());
        context.insert("path".to_string(), volume_dir.display().to_string());

        let mut parameters = HashMap::new();
        parameters.insert("namespace".to_string(), namespace.to_string());
        parameters.insert("service".to_string(), service.to_string());
        parameters.insert("claim".to_string(), claim.to_string());

        let stored = StoredVolume {
            volume: CsiVolume {
                volume_id: volume_id.clone(),
                capacity_bytes: 0,
                volume_context: context,
            },
            parameters,
            path: volume_dir.display().to_string(),
            publications: Vec::new(),
            created_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            encrypted: None,
        };

        let csi = Keyspace::new("csi");
        let volume_key = format!("/volumes/{}", volume_id);
        let volume_payload = serde_json::to_string(&stored).expect("serialize volume");
        csi.put(&volume_key, &volume_payload)
            .expect("persist volume record");

        let service_key = format!("/services/{}/{}", namespace, service);
        let index_payload =
            serde_json::to_string(&vec![volume_id.clone()]).expect("serialize index");
        csi.put(&service_key, &index_payload)
            .expect("persist service index");
    }
}

#[tokio::test]
async fn restore_command_rehydrates_volume_contents() {
    let _lock = keyspace_lock().lock();
    let env = SnapshotTestEnv::new();
    let namespace = "default";
    let service = "demo";
    let claim = "data";
    env.write_sample_volume(namespace, service, claim, "restored content");

    let artifact_path = env.backup_dir().join("snapshots").join("demo-snapshot.tar");
    if let Some(parent) = artifact_path.parent() {
        fs::create_dir_all(parent).expect("artifact parent");
    }

    Snapshot::save(
        Some(namespace),
        service,
        Some(claim),
        artifact_path.to_str().expect("artifact path"),
    )
    .await
    .expect("snapshot save");

    let restore_path = env.temp_path().join("restore").join(claim);
    let args = RestoreArgs {
        artifact: artifact_path.to_string_lossy().into_owned(),
        mappings: vec![(
            claim.to_string(),
            restore_path.to_string_lossy().into_owned(),
        )],
        service: Some(service.to_string()),
    };

    handle_restore(&args).await.expect("restore succeeded");

    let restored_file = restore_path.join("data.txt");
    let contents = fs::read_to_string(restored_file).expect("read restored file");
    assert_eq!(contents, "restored content");
}
