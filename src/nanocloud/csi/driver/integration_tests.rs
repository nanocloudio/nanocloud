#![allow(clippy::unwrap_used)]
#![allow(clippy::await_holding_lock)]

use super::metadata;
use super::observer::CsiObserver;
use super::paths;
use super::*;
use crate::nanocloud::csi::types::{
    CreateSnapshotRequest, CreateVolumeRequest, CreateVolumeResponse, DeleteVolumeRequest,
    NodePublishVolumeRequest, NodeUnpublishVolumeRequest, Snapshot, Volume, VolumeCapability,
    VolumeContentSource, VolumeContentSourceSnapshot,
};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::os::unix::fs::symlink;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};
use tar::Archive;
use tempfile::TempDir;
use tokio::time::{sleep, Duration, Instant};

static ENV_MUTEX: Mutex<()> = Mutex::new(());

#[derive(Default)]
struct RecordingObserver {
    operations: Mutex<Vec<(String, bool)>>,
    locks: Mutex<Vec<(String, u128)>>,
    snapshots: Mutex<Vec<(String, u64, u128)>>,
}

impl CsiObserver for RecordingObserver {
    fn on_operation_start(&self, _op: &str) {}

    fn on_operation_end(&self, op: &str, result: Result<(), String>) {
        self.operations
            .lock()
            .unwrap()
            .push((op.to_string(), result.is_ok()));
    }

    fn on_lock_wait(&self, op: &str, volume: &str, wait_ms: u128) {
        self.locks
            .lock()
            .unwrap()
            .push((format!("{op}:{volume}"), wait_ms));
    }

    fn on_snapshot_complete(&self, snapshot_id: &str, bytes: u64, duration_ms: u128) {
        self.snapshots
            .lock()
            .unwrap()
            .push((snapshot_id.to_string(), bytes, duration_ms));
    }
}

fn base_parameters() -> HashMap<String, String> {
    let mut params = HashMap::new();
    params.insert("service".into(), "api".into());
    params.insert("claim".into(), "data".into());
    params
}

fn init_test_env(
    options: SnapshotOptions,
    observer: Option<Arc<dyn CsiObserver>>,
) -> (TempDir, Arc<CsiDriver>, MutexGuard<'static, ()>) {
    let guard = ENV_MUTEX.lock().unwrap();
    let tmp = tempfile::tempdir().expect("temp dir");
    let root = tmp.path();
    let csi_root = root.join("csi");
    let keyspace_root = root.join("keyspace");
    let secure_assets = root.join("secure_assets");
    let encrypted_root = root.join("encrypted");

    fs::create_dir_all(&csi_root).unwrap();
    fs::create_dir_all(&keyspace_root).unwrap();
    fs::create_dir_all(&secure_assets).unwrap();
    fs::create_dir_all(&encrypted_root).unwrap();

    env::set_var("NANOCLOUD_CSI_ROOT", &csi_root);
    env::set_var("NANOCLOUD_KEYSPACE", &keyspace_root);
    env::set_var("NANOCLOUD_LOCK_FILE", keyspace_root.join(".lock"));
    env::set_var("NANOCLOUD_SECURE_ASSETS", &secure_assets);
    env::set_var("NANOCLOUD_ENCRYPTED_VOLUMES", &encrypted_root);

    let driver = match observer {
        Some(obs) => CsiDriver::for_test_with(options, obs),
        None => CsiDriver::for_test(options),
    };
    (tmp, driver, guard)
}

async fn create_basic_volume(driver: &CsiDriver, name: &str) -> CreateVolumeResponse {
    let parameters = base_parameters();
    driver
        .create_volume(CreateVolumeRequest {
            name: name.to_string(),
            capacity_range: None,
            volume_capabilities: vec![VolumeCapability::default()],
            parameters,
            content_source: None,
        })
        .await
        .unwrap()
}

fn volume_path(response: &CreateVolumeResponse) -> PathBuf {
    PathBuf::from(
        response
            .volume
            .volume_context
            .get("path")
            .expect("volume path"),
    )
}

#[tokio::test]
async fn snapshot_rejects_symlink_outside_root() {
    let options = SnapshotOptions {
        symlink_policy: SnapshotSymlinkPolicy::Error,
        ..SnapshotOptions::default()
    };
    let (tmp, driver, _guard) = init_test_env(options, None);
    let create = create_basic_volume(&driver, "vol-1").await;
    let vol_path = volume_path(&create);
    assert!(vol_path.exists(), "volume path should exist");
    assert!(
        driver
            .load_volume(&create.volume.volume_id)
            .unwrap()
            .is_some(),
        "volume should be persisted"
    );

    let outside = tmp.path().join("outside.txt");
    fs::write(&outside, "outside").unwrap();
    let symlink_path = vol_path.join("escape");
    symlink(&outside, &symlink_path).unwrap();

    let result = driver
        .create_snapshot(CreateSnapshotRequest {
            name: "snap-1".into(),
            source_volume_id: create.volume.volume_id.clone(),
        })
        .await;
    assert!(result.is_err(), "snapshot unexpectedly succeeded");

    let archive_path = paths::snapshot_archive_path("snap-1");
    assert!(
        !archive_path.exists(),
        "snapshot archive should not be created on failure"
    );
    assert!(driver.load_snapshot("snap-1").unwrap().is_none());
}

#[tokio::test]
async fn snapshot_respects_size_and_depth_limits() {
    let options = SnapshotOptions {
        max_total_bytes: Some(16),
        max_depth: 1,
        symlink_policy: SnapshotSymlinkPolicy::Skip,
        throttle_bytes_per_chunk: None,
        throttle_sleep: Duration::from_millis(1),
    };
    let (_tmp, driver, _guard) = init_test_env(options, None);
    let create = create_basic_volume(&driver, "vol-2").await;
    let vol_path = volume_path(&create);
    assert!(
        driver
            .load_volume(&create.volume.volume_id)
            .unwrap()
            .is_some(),
        "volume should be persisted"
    );

    fs::create_dir_all(vol_path.join("nested/too/deep")).unwrap();
    fs::write(vol_path.join("big.txt"), b"this exceeds limit").unwrap();

    let err = driver
        .create_snapshot(CreateSnapshotRequest {
            name: "snap-2".into(),
            source_volume_id: create.volume.volume_id.clone(),
        })
        .await
        .expect_err("snapshot should fail");

    let message = format!("{err}");
    assert!(
        message.contains("Snapshot size limit exceeded") || message.contains("depth exceeded"),
        "unexpected error message: {message}"
    );
    assert!(driver.load_snapshot("snap-2").unwrap().is_none());
}

#[tokio::test]
async fn snapshot_archives_symlink_inside_root() {
    let (_tmp, driver, _guard) = init_test_env(SnapshotOptions::default(), None);
    let create = create_basic_volume(&driver, "vol-symlink").await;
    let vol_path = volume_path(&create);
    fs::write(vol_path.join("file.txt"), b"data").unwrap();
    symlink("file.txt", vol_path.join("link.txt")).unwrap();

    let resp = driver
        .create_snapshot(CreateSnapshotRequest {
            name: "snap-link".into(),
            source_volume_id: create.volume.volume_id.clone(),
        })
        .await
        .expect("snapshot should succeed");

    let file = fs::File::open(&resp.archive_path).unwrap();
    let mut archive = Archive::new(file);
    let mut found_link = false;
    for entry in archive.entries().unwrap() {
        let entry = entry.unwrap();
        let path = entry.path().unwrap().into_owned();
        if path.ends_with("link.txt") {
            found_link = entry.header().entry_type().is_symlink();
        }
    }
    assert!(found_link, "symlink entry should be archived");
}

#[tokio::test]
async fn snapshot_throttling_delays_creation() {
    let options = SnapshotOptions {
        throttle_bytes_per_chunk: Some(512),
        throttle_sleep: Duration::from_millis(50),
        ..SnapshotOptions::default()
    };
    let (_tmp, driver, _guard) = init_test_env(options, None);
    let create = create_basic_volume(&driver, "vol-throttle").await;
    let vol_path = volume_path(&create);
    fs::write(vol_path.join("big.bin"), vec![0u8; 1_024]).unwrap();

    let start = Instant::now();
    driver
        .create_snapshot(CreateSnapshotRequest {
            name: "snap-throttle".into(),
            source_volume_id: create.volume.volume_id.clone(),
        })
        .await
        .expect("snapshot should succeed");
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(40),
        "throttling should introduce noticeable delay, got {:?}",
        elapsed
    );
}

#[tokio::test]
async fn volume_lifecycle_happy_path() {
    let (_tmp, driver, _guard) = init_test_env(SnapshotOptions::default(), None);
    let create = create_basic_volume(&driver, "vol-3").await;
    let vol_path = volume_path(&create);
    assert!(vol_path.exists(), "volume directory missing");

    let target = driver
        .publish_root()
        .join("svc/data")
        .to_string_lossy()
        .to_string();
    let publish = driver
        .node_publish_volume(NodePublishVolumeRequest {
            volume_id: create.volume.volume_id.clone(),
            target_path: target.clone(),
            readonly: false,
            volume_capability: None,
            volume_context: HashMap::new(),
        })
        .await
        .expect("publish should succeed");
    assert_eq!(publish.publish_path, target);
    let metadata = fs::symlink_metadata(&target).unwrap();
    assert!(metadata.file_type().is_symlink());

    let stored = driver
        .load_volume(&create.volume.volume_id)
        .unwrap()
        .unwrap();
    assert!(stored.publications.contains(&target));

    driver
        .node_unpublish_volume(NodeUnpublishVolumeRequest {
            volume_id: create.volume.volume_id.clone(),
            target_path: target.clone(),
        })
        .await
        .expect("unpublish should succeed");
    assert!(
        !PathBuf::from(&target).exists(),
        "publish target should be removed"
    );
    let stored = driver
        .load_volume(&create.volume.volume_id)
        .unwrap()
        .unwrap();
    assert!(stored.publications.is_empty());

    driver
        .delete_volume(DeleteVolumeRequest {
            volume_id: create.volume.volume_id.clone(),
        })
        .await
        .expect("delete should succeed");
    assert!(
        driver
            .load_volume(&create.volume.volume_id)
            .unwrap()
            .is_none(),
        "volume should be removed from keyspace"
    );
    assert!(
        !vol_path.exists(),
        "volume directory should be removed from filesystem"
    );
}

#[tokio::test]
async fn per_volume_locks_serialize_same_volume() {
    let registry = VolumeLockRegistry::global();
    let start = Instant::now();

    let registry_one = registry.clone();
    let first = tokio::spawn(async move {
        let _guard = registry_one.lock("vol-lock").await;
        sleep(Duration::from_millis(120)).await;
        Instant::now()
    });

    sleep(Duration::from_millis(20)).await;

    let registry_two = registry.clone();
    let second = tokio::spawn(async move {
        let _guard = registry_two.lock("vol-lock").await;
        Instant::now()
    });

    let first_acquired = first.await.unwrap();
    let second_acquired = second.await.unwrap();
    assert!(
        second_acquired >= first_acquired,
        "second lock should wait for first to release"
    );
    assert!(
        second_acquired.duration_since(start) >= Duration::from_millis(100),
        "second lock acquired too early: {:?}",
        second_acquired.duration_since(start)
    );
}

#[tokio::test]
async fn different_volume_locks_do_not_block_each_other() {
    let registry = VolumeLockRegistry::global();
    let start = Instant::now();

    let a = tokio::spawn({
        let registry = registry.clone();
        async move {
            let _guard = registry.lock("vol-a").await;
            Instant::now()
        }
    });
    let b = tokio::spawn({
        let registry = registry.clone();
        async move {
            let _guard = registry.lock("vol-b").await;
            Instant::now()
        }
    });

    let (a_time, b_time) = tokio::join!(a, b);
    let a_time = a_time.unwrap();
    let b_time = b_time.unwrap();

    assert!(
        a_time.duration_since(start) < Duration::from_millis(50),
        "lock on vol-a was unexpectedly delayed"
    );
    assert!(
        b_time.duration_since(start) < Duration::from_millis(50),
        "lock on vol-b was unexpectedly delayed"
    );
}

#[tokio::test]
async fn create_volume_rolls_back_on_snapshot_failure() {
    let (_tmp, driver, _guard) = init_test_env(SnapshotOptions::default(), None);
    let params = base_parameters();
    let request = CreateVolumeRequest {
        name: "vol-rollback".into(),
        capacity_range: None,
        volume_capabilities: Vec::new(),
        parameters: params,
        content_source: Some(VolumeContentSource {
            snapshot: Some(VolumeContentSourceSnapshot {
                snapshot_id: "missing".into(),
            }),
        }),
    };

    let result = driver.create_volume(request).await;
    assert!(result.is_err(), "volume creation should fail");

    let vol_dir = paths::volume_path("vol-rollback");
    assert!(!vol_dir.exists(), "volume directory should be rolled back");
    assert!(
        driver
            .list_service_volumes("default", "api")
            .unwrap()
            .is_empty(),
        "service index should remain empty after rollback"
    );
}

#[tokio::test]
async fn metadata_roundtrips_volume_and_snapshot() {
    let (_tmp, _driver, _guard) = init_test_env(SnapshotOptions::default(), None);

    let stored_volume = StoredVolume {
        volume: Volume {
            volume_id: "meta-vol".into(),
            capacity_bytes: 123,
            volume_context: HashMap::new(),
        },
        parameters: base_parameters(),
        path: "/tmp/meta".into(),
        publications: Vec::new(),
        created_at: "now".into(),
        encrypted: None,
    };
    metadata::persist_volume(&stored_volume).unwrap();
    let loaded = metadata::load_volume("meta-vol").unwrap().unwrap();
    assert_eq!(loaded.volume.volume_id, "meta-vol");
    assert_eq!(loaded.volume.capacity_bytes, 123);

    let stored_snapshot = StoredSnapshot {
        snapshot: Snapshot {
            snapshot_id: "meta-snap".into(),
            source_volume_id: "meta-vol".into(),
            size_bytes: 1,
            ready_to_use: true,
            creation_time: "now".into(),
        },
        archive_path: "/tmp/meta-snap.tar".into(),
    };
    metadata::persist_snapshot(&stored_snapshot).unwrap();
    let loaded_snap = metadata::load_snapshot("meta-snap").unwrap().unwrap();
    assert_eq!(loaded_snap.snapshot.snapshot_id, "meta-snap");
    assert_eq!(loaded_snap.snapshot.source_volume_id, "meta-vol");
}

#[tokio::test]
async fn create_volume_requires_service_and_claim() {
    let (_tmp, driver, _guard) = init_test_env(SnapshotOptions::default(), None);
    let mut missing_service = HashMap::new();
    missing_service.insert("claim".into(), "data".into());

    let err = driver
        .create_volume(CreateVolumeRequest {
            name: "missing".into(),
            capacity_range: None,
            volume_capabilities: vec![],
            parameters: missing_service,
            content_source: None,
        })
        .await
        .expect_err("create volume should fail without service");
    assert!(
        format!("{err}").contains("must include 'service'"),
        "unexpected error: {err}"
    );

    let mut missing_claim = HashMap::new();
    missing_claim.insert("service".into(), "api".into());
    let err = driver
        .create_volume(CreateVolumeRequest {
            name: "missing-claim".into(),
            capacity_range: None,
            volume_capabilities: vec![],
            parameters: missing_claim,
            content_source: None,
        })
        .await
        .expect_err("create volume should fail without claim");
    assert!(
        format!("{err}").contains("must include 'claim'"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn publish_path_must_be_under_publish_root() {
    let (_tmp, driver, _guard) = init_test_env(SnapshotOptions::default(), None);
    let create = create_basic_volume(&driver, "vol-publish").await;

    let err = driver
        .node_publish_volume(NodePublishVolumeRequest {
            volume_id: create.volume.volume_id.clone(),
            target_path: "/tmp/outside".into(),
            readonly: false,
            volume_capability: None,
            volume_context: HashMap::new(),
        })
        .await
        .expect_err("publish outside root should fail");
    assert!(
        format!("{err}").contains("must be under publish root"),
        "unexpected error: {err}"
    );

    let stored = driver
        .load_volume(&create.volume.volume_id)
        .unwrap()
        .unwrap();
    assert!(
        stored.publications.is_empty(),
        "volume should not be published after failure"
    );
}

#[tokio::test]
async fn empty_publish_path_is_rejected() {
    let (_tmp, driver, _guard) = init_test_env(SnapshotOptions::default(), None);
    let create = create_basic_volume(&driver, "vol-empty").await;
    let err = driver
        .node_publish_volume(NodePublishVolumeRequest {
            volume_id: create.volume.volume_id.clone(),
            target_path: "".into(),
            readonly: false,
            volume_capability: None,
            volume_context: HashMap::new(),
        })
        .await
        .expect_err("publish should fail with empty path");
    assert!(
        format!("{err}").contains("Target path is required"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn publish_rolls_back_on_injected_failure() {
    let (_tmp, driver, _guard) = init_test_env(SnapshotOptions::default(), None);
    let create = create_basic_volume(&driver, "vol-pub-fail").await;
    let target = driver
        .publish_root()
        .join("svc/fail")
        .to_string_lossy()
        .to_string();

    env::set_var("NANOCLOUD_CSI_TEST_FAIL_PUBLISH_AFTER_SYMLINK", "1");
    let result = driver
        .node_publish_volume(NodePublishVolumeRequest {
            volume_id: create.volume.volume_id.clone(),
            target_path: target.clone(),
            readonly: false,
            volume_capability: None,
            volume_context: HashMap::new(),
        })
        .await;
    env::remove_var("NANOCLOUD_CSI_TEST_FAIL_PUBLISH_AFTER_SYMLINK");
    assert!(result.is_err(), "publish should fail when injection set");
    assert!(
        !PathBuf::from(&target).exists(),
        "symlink should be cleaned up on rollback"
    );
    let stored = driver
        .load_volume(&create.volume.volume_id)
        .unwrap()
        .unwrap();
    assert!(
        stored.publications.is_empty(),
        "publications should be rolled back on failure"
    );
}

#[tokio::test]
async fn unpublish_rolls_back_on_injected_failure() {
    let (_tmp, driver, _guard) = init_test_env(SnapshotOptions::default(), None);
    let create = create_basic_volume(&driver, "vol-unpub-fail").await;
    let target = driver
        .publish_root()
        .join("svc/unpub")
        .to_string_lossy()
        .to_string();
    driver
        .node_publish_volume(NodePublishVolumeRequest {
            volume_id: create.volume.volume_id.clone(),
            target_path: target.clone(),
            readonly: false,
            volume_capability: None,
            volume_context: HashMap::new(),
        })
        .await
        .unwrap();

    env::set_var("NANOCLOUD_CSI_TEST_FAIL_UNPUBLISH_AFTER_REMOVE", "1");
    let result = driver
        .node_unpublish_volume(NodeUnpublishVolumeRequest {
            volume_id: create.volume.volume_id.clone(),
            target_path: target.clone(),
        })
        .await;
    env::remove_var("NANOCLOUD_CSI_TEST_FAIL_UNPUBLISH_AFTER_REMOVE");

    assert!(result.is_err(), "unpublish should fail when injection set");
    assert!(
        PathBuf::from(&target).exists(),
        "symlink should be restored on rollback"
    );
    let stored = driver
        .load_volume(&create.volume.volume_id)
        .unwrap()
        .unwrap();
    assert!(
        stored.publications.contains(&target),
        "publication should remain after rollback"
    );
}

#[tokio::test]
async fn observer_captures_operations_and_snapshots() {
    let observer = Arc::new(RecordingObserver::default());
    let (_tmp, driver, _guard) = init_test_env(SnapshotOptions::default(), Some(observer.clone()));
    let create = create_basic_volume(&driver, "vol-observer").await;
    let target = driver
        .publish_root()
        .join("svc/obs")
        .to_string_lossy()
        .to_string();

    driver
        .node_publish_volume(NodePublishVolumeRequest {
            volume_id: create.volume.volume_id.clone(),
            target_path: target.clone(),
            readonly: false,
            volume_capability: None,
            volume_context: HashMap::new(),
        })
        .await
        .unwrap();

    driver
        .create_snapshot(CreateSnapshotRequest {
            name: "snap-observer".into(),
            source_volume_id: create.volume.volume_id.clone(),
        })
        .await
        .unwrap();

    let ops = observer.operations.lock().unwrap().clone();
    assert!(
        ops.iter()
            .any(|(op, ok)| op.contains("create_snapshot") && *ok),
        "observer should see snapshot completion"
    );
    assert!(
        ops.iter()
            .any(|(op, ok)| op.contains("node_publish") && *ok),
        "observer should see publish completion"
    );

    let locks = observer.locks.lock().unwrap();
    assert!(
        locks.iter().any(|(op, _)| op.contains("node_publish")),
        "lock wait should be recorded"
    );

    let snapshots = observer.snapshots.lock().unwrap();
    assert!(
        snapshots.iter().any(|(id, _, _)| id == "snap-observer"),
        "snapshot completion should be tracked"
    );
}
