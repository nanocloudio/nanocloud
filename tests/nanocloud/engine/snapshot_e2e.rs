use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use chrono::{SecondsFormat, Utc};
use nanocloud::nanocloud::csi::{CsiVolume, StoredVolume};
use nanocloud::nanocloud::engine::Snapshot;
use nanocloud::nanocloud::test_support::keyspace_lock;
use nanocloud::nanocloud::util::Keyspace;
use tar::Archive;
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
        fs::create_dir_all(lock_file.parent().expect("lock file has parent directory"))
            .expect("lock dir");
        fs::File::create(&lock_file).expect("lock file");

        let backup_dir = base.join("backups");
        fs::create_dir_all(&backup_dir).expect("backup dir");

        let csi_root = base.join("csi");
        fs::create_dir_all(&csi_root).expect("csi root");

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

    fn volume_root(&self) -> PathBuf {
        self.csi_root.join("volumes")
    }

    fn write_sample_volume(
        &self,
        namespace: &str,
        service: &str,
        claim: &str,
        contents: &str,
    ) -> String {
        let volume_id = format!("{}-{}", service, claim);
        let volume_dir = self.volume_root().join(&volume_id);
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

        volume_id
    }
}

#[tokio::test]
async fn snapshot_save_captures_volume_artifact() {
    let lock = keyspace_lock().lock();
    let env = SnapshotTestEnv::new();
    let namespace = "default";
    let service = "web";
    let claim = "data";

    env.write_sample_volume(namespace, service, claim, "snapshot payload");

    let artifact_path = env.backup_dir().join("snapshots").join("web-sample.tar");
    if let Some(parent) = artifact_path.parent() {
        fs::create_dir_all(parent).expect("artifact parent dir");
    }

    drop(lock);

    let summary = Snapshot::save(
        Some(namespace),
        service,
        Some(claim),
        artifact_path.to_str().expect("artifact path utf8"),
    )
    .await
    .expect("snapshot saved");

    assert_eq!(summary.namespace, namespace);
    assert_eq!(summary.service, service);
    assert_eq!(summary.entries.len(), 1);
    let entry = &summary.entries[0];
    assert_eq!(entry.claim, claim);
    assert_eq!(entry.volume_id, format!("{}-{}", service, claim));
    assert!(entry.size_bytes > 0);

    assert!(
        artifact_path.exists(),
        "snapshot artifact should be created"
    );

    let file = fs::File::open(&artifact_path).expect("open artifact");
    let mut archive = Archive::new(file);
    let mut manifest_found = false;
    let mut volume_archive_found = false;

    for entry_result in archive.entries().expect("iterate archive entries") {
        let mut entry = entry_result.expect("archive entry");
        let path = entry
            .path()
            .expect("archive entry name")
            .to_string_lossy()
            .into_owned();
        if path == "manifest.json" {
            manifest_found = true;
            let mut data = String::new();
            entry.read_to_string(&mut data).expect("read manifest");
            let manifest: serde_json::Value =
                serde_json::from_str(&data).expect("parse manifest json");
            assert_eq!(manifest["service"], service);
            assert_eq!(manifest["namespace"], namespace);
        } else if path.starts_with("snapshots/") && path.ends_with(".tar") {
            volume_archive_found = true;
            let mut buffer = Vec::new();
            entry.read_to_end(&mut buffer).expect("read volume tar");
            assert!(
                !buffer.is_empty(),
                "captured snapshot tar should contain data"
            );
        }
    }

    assert!(manifest_found, "snapshot manifest must be present");
    assert!(
        volume_archive_found,
        "volume sub-archive should be included"
    );
}

#[tokio::test]
async fn snapshot_restore_rehydrates_volume_claim() {
    let env = SnapshotTestEnv::new();
    let namespace = "default";
    let service = "api";
    let claim = "config";
    {
        let _lock = keyspace_lock().lock();
        env.write_sample_volume(namespace, service, claim, "restored-from-snapshot");
    }

    let artifact_path = env.backup_dir().join("snapshots").join("api-restore.tar");
    if let Some(parent) = artifact_path.parent() {
        fs::create_dir_all(parent).expect("artifact parent dir");
    }

    Snapshot::save(
        Some(namespace),
        service,
        Some(claim),
        artifact_path.to_str().expect("artifact path utf8"),
    )
    .await
    .expect("snapshot saved");

    let restore_root = env.backup_dir().join("restore-testing");
    fs::create_dir_all(&restore_root).expect("restore root");
    let claim_path = restore_root.join(claim);

    let mut host_paths = HashMap::new();
    host_paths.insert(claim.to_string(), claim_path.display().to_string());

    let snapshot = Snapshot::new(artifact_path.to_str().expect("artifact path"))
        .expect("open snapshot artifact");
    snapshot
        .restore(service, &host_paths)
        .await
        .expect("restore succeeds");

    let restored_file = claim_path.join("data.txt");
    let contents = fs::read_to_string(restored_file).expect("read restored file");
    assert_eq!(contents, "restored-from-snapshot");
}
