use std::collections::HashMap;
use std::env;
use std::sync::MutexGuard;

use nanocloud::nanocloud::api::types::{
    Bundle, BundleSpec, Device, DeviceSpec, VolumeSnapshot, VolumeSnapshotPhase,
    VolumeSnapshotSpec, VolumeSnapshotStatus,
};
use nanocloud::nanocloud::k8s::bundle_manager::{BundleApplyOptions, BundleRegistry};
use nanocloud::nanocloud::k8s::device_manager::DeviceRegistry;
use nanocloud::nanocloud::k8s::pod::ObjectMeta;
use nanocloud::nanocloud::k8s::store::{
    delete_volume_snapshot, list_volume_snapshots, save_volume_snapshot,
};
use nanocloud::nanocloud::test_support::keyspace_lock;
use serde_json::json;
use serial_test::serial;
use tempfile::TempDir;

struct ManagerEnv {
    _guard: MutexGuard<'static, ()>,
    _tempdir: TempDir,
    previous_keyspace: Option<String>,
}

impl ManagerEnv {
    fn new() -> Self {
        let guard = keyspace_lock().lock();
        let tempdir = TempDir::new().expect("tempdir");
        let keyspace_root = tempdir.path().join("keyspace");
        std::fs::create_dir_all(&keyspace_root).expect("keyspace dir");
        let previous_keyspace = env::var("NANOCLOUD_KEYSPACE").ok();
        env::set_var("NANOCLOUD_KEYSPACE", &keyspace_root);

        ManagerEnv {
            _guard: guard,
            _tempdir: tempdir,
            previous_keyspace,
        }
    }
}

impl Drop for ManagerEnv {
    fn drop(&mut self) {
        match self.previous_keyspace.as_ref() {
            Some(value) => env::set_var("NANOCLOUD_KEYSPACE", value),
            None => env::remove_var("NANOCLOUD_KEYSPACE"),
        }
    }
}

fn bundle_fixture(service: &str, namespace: &str) -> Bundle {
    Bundle {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "Bundle".to_string(),
        metadata: ObjectMeta {
            name: Some(service.to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        spec: BundleSpec {
            service: service.to_string(),
            namespace: Some(namespace.to_string()),
            options: HashMap::new(),
            profile_key: None,
            snapshot: None,
            start: true,
            update: false,
            security: None,
            runtime: None,
        },
        status: None,
    }
}

fn device_fixture(hash: &str, namespace: &str) -> Device {
    Device {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "Device".to_string(),
        metadata: ObjectMeta {
            name: Some(format!("device-{hash}")),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        spec: DeviceSpec {
            hash: hash.to_string(),
            certificate_subject: format!("device:{hash}"),
            description: Some("integration device".to_string()),
        },
        status: None,
    }
}

fn snapshot_fixture(service: &str, namespace: &str) -> VolumeSnapshot {
    VolumeSnapshot {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "VolumeSnapshot".to_string(),
        metadata: ObjectMeta {
            name: Some("snap-integration".to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        spec: VolumeSnapshotSpec {
            service: service.to_string(),
            volume_claim: "data".to_string(),
            description: Some("integration snapshot".to_string()),
        },
        status: Some(VolumeSnapshotStatus {
            phase: Some(VolumeSnapshotPhase::Pending),
            ..Default::default()
        }),
    }
}

#[tokio::test]
#[serial]
async fn bundle_device_snapshot_lifecycle() {
    let _env = ManagerEnv::new();
    let bundles = BundleRegistry::shared();
    let devices = DeviceRegistry::shared();
    let namespace = "integration";
    let service = "svc-integration";

    let created_bundle = bundles
        .create(namespace, bundle_fixture(service, namespace))
        .await
        .expect("bundle created");
    let initial_rv = created_bundle
        .metadata
        .resource_version
        .as_deref()
        .and_then(|rv| rv.parse::<u64>().ok())
        .unwrap_or(1);

    let _first_apply = bundles
        .apply_bundle(
            namespace,
            service,
            json!({ "spec": { "options": { "mode": "fast" }}}),
            BundleApplyOptions {
                manager: "tests/bundle",
                force: true,
                dry_run: false,
            },
        )
        .await
        .expect("apply bundle");
    let second_apply = bundles
        .apply_bundle(
            namespace,
            service,
            json!({ "spec": { "update": true } }),
            BundleApplyOptions {
                manager: "tests/bundle",
                force: true,
                dry_run: false,
            },
        )
        .await
        .expect("second apply");
    let final_rv = second_apply
        .metadata
        .resource_version
        .as_deref()
        .and_then(|rv| rv.parse::<u64>().ok())
        .unwrap_or(initial_rv);
    assert!(
        final_rv >= initial_rv + 2,
        "resourceVersion should advance after sequential applies"
    );

    let created_device = devices
        .create(namespace, device_fixture("hash-a", namespace))
        .await
        .expect("device created");
    assert_eq!(
        created_device.metadata.namespace.as_deref(),
        Some(namespace)
    );
    assert!(
        created_device.metadata.resource_version.is_some(),
        "device should have a resourceVersion"
    );
    let listed_devices = devices.list(Some(namespace)).await;
    assert_eq!(
        listed_devices.len(),
        1,
        "device listing should include new item"
    );

    let snapshot = snapshot_fixture(service, namespace);
    save_volume_snapshot(Some(namespace), "snap-integration", &snapshot).expect("persist snapshot");
    let stored_snapshots = list_volume_snapshots(Some(namespace)).expect("list snapshots");
    let stored = stored_snapshots
        .iter()
        .find(|snap| snap.metadata.name.as_deref() == Some("snap-integration"))
        .expect("snapshot present");
    assert_eq!(stored.spec.service, service);
    assert!(
        stored.metadata.resource_version.is_some(),
        "snapshot should be versioned for future updates"
    );

    let _ = bundles.delete(namespace, service).await;
    let _ = devices.delete(namespace, "device-hash-a").await;
    let _ = delete_volume_snapshot(Some(namespace), "snap-integration");
}
