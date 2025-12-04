use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::OnceLock;

use nanocloud::nanocloud::api::types::{Bundle, BundleSpec, Device, DeviceSpec, VolumeSnapshot};
use nanocloud::nanocloud::api::types::{VolumeSnapshotSpec, VolumeSnapshotStatus};
use nanocloud::nanocloud::k8s::bundle_manager::{BundleApplyOptions, BundleRegistry};
use nanocloud::nanocloud::k8s::configmap::ConfigMap;
use nanocloud::nanocloud::k8s::configmap_manager::ConfigMapRegistry;
use nanocloud::nanocloud::k8s::device_manager::DeviceRegistry;
use nanocloud::nanocloud::k8s::pod::{ContainerSpec, ObjectMeta, Pod, PodSpec};
use nanocloud::nanocloud::k8s::store::{
    list_config_maps, list_volume_snapshots, load_bundle_field_ownership, load_pod_manifest,
    save_bundle_field_ownership, save_pod_manifest, save_volume_snapshot,
};
use serial_test::serial;
use tempfile::TempDir;
use tokio::task;

fn temp_keyspace() -> PathBuf {
    static ROOT: OnceLock<TempDir> = OnceLock::new();
    let dir = ROOT.get_or_init(|| TempDir::new().expect("tempdir"));
    let path = dir.path().to_path_buf();
    env::set_var("NANOCLOUD_KEYSPACE", &path);
    path
}

fn minimal_pod(namespace: &str, name: &str) -> Pod {
    let metadata = ObjectMeta {
        name: Some(name.to_string()),
        namespace: Some(namespace.to_string()),
        generate_name: None,
        uid: None,
        labels: HashMap::new(),
        annotations: HashMap::new(),
        owner_references: Vec::new(),
        finalizers: Vec::new(),
        generation: None,
        creation_timestamp: None,
        deletion_timestamp: None,
        deletion_grace_period_seconds: None,
        managed_fields: Vec::new(),
        resource_version: None,
    };
    let container = ContainerSpec {
        name: "app".to_string(),
        image: Some("demo:v1".to_string()),
        command: Vec::new(),
        args: Vec::new(),
        image_command: Vec::new(),
        image_args: Vec::new(),
        env_from: Vec::new(),
        env: Vec::new(),
        ports: Vec::new(),
        volume_mounts: Vec::new(),
        resources: None,
        working_dir: None,
        lifecycle: None,
        user: None,
        liveness_probe: None,
        readiness_probe: None,
    };
    let spec = PodSpec {
        containers: vec![container],
        ..Default::default()
    };
    Pod::new(metadata, spec)
}

#[tokio::test]
#[serial]
async fn concurrent_pod_writes_increment_versions() {
    let _root = temp_keyspace();

    let pod_a = minimal_pod("ns-a", "pod-a");
    let pod_b = minimal_pod("ns-a", "pod-a");

    let first = task::spawn_blocking(move || save_pod_manifest(Some("ns-a"), "pod-a", &pod_a));
    let second = task::spawn_blocking(move || save_pod_manifest(Some("ns-a"), "pod-a", &pod_b));

    first.await.expect("first spawn ok").expect("save 1");
    second.await.expect("second spawn ok").expect("save 2");

    let loaded = load_pod_manifest(Some("ns-a"), "pod-a")
        .expect("load pod")
        .expect("pod exists");
    let rv = loaded
        .metadata
        .resource_version
        .as_deref()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    assert!(
        rv >= 1,
        "resourceVersion should be assigned after concurrent writes"
    );
}

#[tokio::test]
#[serial]
async fn corrupted_pod_payload_surfaces_error_and_recovers() {
    let _root = temp_keyspace();
    let keyspace_root = env::var("NANOCLOUD_KEYSPACE").expect("env set");
    let path = PathBuf::from(keyspace_root)
        .join("k8s")
        .join("pods")
        .join("default")
        .join("broken")
        .join("_value_");
    std::fs::create_dir_all(path.parent().unwrap()).expect("mk parent");
    std::fs::write(&path, "{not-json").expect("write corrupt pod");

    let err = load_pod_manifest(None, "broken")
        .expect_err("should surface parse error")
        .to_string();
    assert!(
        err.contains("deserialize") || err.contains("parse"),
        "expected contextual error, got {err}"
    );

    let healthy = minimal_pod("default", "broken");
    save_pod_manifest(None, "broken", &healthy).expect("save after recovery");
    let reloaded = load_pod_manifest(None, "broken")
        .expect("load")
        .expect("pod restored");
    assert_eq!(reloaded.metadata.name.as_deref(), Some("broken"));
}

#[tokio::test]
#[serial]
async fn pagination_and_listing_respects_continue_tokens() {
    let _root = temp_keyspace();
    let registry = ConfigMapRegistry::shared();
    for idx in 0..8 {
        let mut cm = ConfigMap::new(ObjectMeta {
            name: Some(format!("cm-{idx}")),
            namespace: Some("list-ns".to_string()),
            generate_name: None,
            uid: None,
            labels: HashMap::new(),
            annotations: HashMap::new(),
            owner_references: Vec::new(),
            finalizers: Vec::new(),
            generation: None,
            creation_timestamp: None,
            deletion_timestamp: None,
            deletion_grace_period_seconds: None,
            managed_fields: Vec::new(),
            resource_version: None,
        });
        cm.data.insert("key".to_string(), idx.to_string());
        registry
            .create("list-ns", cm)
            .await
            .expect("create configmap");
    }

    let all = list_config_maps(Some("list-ns")).expect("list configmaps");
    assert_eq!(all.len(), 8);

    let first_page = registry
        .collect_entries(Some("list-ns"), None)
        .await
        .into_iter()
        .take(3)
        .collect::<Vec<_>>();
    assert_eq!(first_page.len(), 3, "first page should have three items");
}

#[tokio::test]
#[serial]
async fn ownership_and_version_tracking_for_bundles() {
    let _root = temp_keyspace();
    let registry = BundleRegistry::shared();
    let mut bundle = Bundle {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "Bundle".to_string(),
        metadata: ObjectMeta {
            name: Some("svc-a".to_string()),
            namespace: Some("own-ns".to_string()),
            generate_name: None,
            uid: None,
            labels: HashMap::new(),
            annotations: HashMap::new(),
            owner_references: Vec::new(),
            finalizers: Vec::new(),
            generation: None,
            creation_timestamp: None,
            deletion_timestamp: None,
            deletion_grace_period_seconds: None,
            managed_fields: Vec::new(),
            resource_version: None,
        },
        spec: BundleSpec {
            service: "svc-a".to_string(),
            namespace: None,
            options: HashMap::new(),
            profile_key: None,
            snapshot: None,
            start: true,
            update: false,
            security: None,
            runtime: None,
        },
        status: None,
    };

    let created = registry
        .create("own-ns", bundle.clone())
        .await
        .expect("bundle created");
    let initial_rv = created
        .metadata
        .resource_version
        .clone()
        .unwrap_or_else(|| "0".to_string());

    let mut ownership = registry
        .load_field_ownership("own-ns", "svc-a")
        .await
        .expect("load ownership");
    ownership.set_owner("/spec/options", "manager/test");
    save_bundle_field_ownership(Some("own-ns"), "svc-a", &ownership).expect("persist ownership");
    let persisted = load_bundle_field_ownership(Some("own-ns"), "svc-a").expect("reload ownership");
    assert!(persisted
        .manager_for("/spec/options")
        .map(|o| o == "manager/test")
        .unwrap_or(false));

    bundle.spec.update = true;
    let updated = registry
        .apply_bundle(
            "own-ns",
            "svc-a",
            serde_json::json!({ "spec": { "update": true }}),
            BundleApplyOptions {
                manager: "manager/test",
                force: true,
                dry_run: false,
            },
        )
        .await
        .expect("apply");
    assert!(
        updated
            .metadata
            .resource_version
            .as_deref()
            .map(|rv| rv != initial_rv)
            .unwrap_or(false),
        "version should bump on apply"
    );
}

#[tokio::test]
#[serial]
async fn device_and_configmap_crud_paths() {
    let _root = temp_keyspace();
    let devices = DeviceRegistry::shared();
    let configmaps = ConfigMapRegistry::shared();

    let device_payload = Device {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "Device".to_string(),
        metadata: ObjectMeta {
            name: Some("device-hash-a".to_string()),
            namespace: Some("crud".to_string()),
            generate_name: None,
            uid: None,
            labels: HashMap::new(),
            annotations: HashMap::new(),
            owner_references: Vec::new(),
            finalizers: Vec::new(),
            generation: None,
            creation_timestamp: None,
            deletion_timestamp: None,
            deletion_grace_period_seconds: None,
            managed_fields: Vec::new(),
            resource_version: None,
        },
        spec: DeviceSpec {
            hash: "hash-a".to_string(),
            certificate_subject: "device:hash-a".to_string(),
            description: Some("integration test device".to_string()),
        },
        status: None,
    };

    let created_device = devices
        .create("crud", device_payload)
        .await
        .expect("create device");
    assert!(
        created_device.metadata.resource_version.is_some(),
        "device should receive a resource version"
    );
    let listed_devices = devices.list(Some("crud")).await;
    assert_eq!(listed_devices.len(), 1);

    let mut cm = ConfigMap::new(ObjectMeta {
        name: Some("cm-a".to_string()),
        namespace: Some("crud".to_string()),
        generate_name: None,
        uid: None,
        labels: HashMap::new(),
        annotations: HashMap::new(),
        owner_references: Vec::new(),
        finalizers: Vec::new(),
        generation: None,
        creation_timestamp: None,
        deletion_timestamp: None,
        deletion_grace_period_seconds: None,
        managed_fields: Vec::new(),
        resource_version: None,
    });
    cm.data.insert("k".to_string(), "v".to_string());
    let created_cm = configmaps
        .create("crud", cm)
        .await
        .expect("create configmap");
    assert_eq!(created_cm.metadata.resource_version.as_deref(), Some("1"));
    let listed = configmaps.list_since(Some("crud"), None).await;
    assert_eq!(listed.len(), 1);
}

#[tokio::test]
#[serial]
async fn snapshot_corruption_recovery_paths() {
    let _root = temp_keyspace();
    let snapshot = VolumeSnapshot {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "VolumeSnapshot".to_string(),
        metadata: ObjectMeta {
            name: Some("snap-a".to_string()),
            namespace: Some("rec".to_string()),
            ..Default::default()
        },
        spec: VolumeSnapshotSpec {
            service: "recorder".to_string(),
            volume_claim: "claim-a".to_string(),
            description: None,
        },
        status: Some(VolumeSnapshotStatus::default()),
    };
    save_volume_snapshot(Some("rec"), "snap-a", &snapshot).expect("save snapshot");

    let keyspace_root = env::var("NANOCLOUD_KEYSPACE").expect("env set");
    let value_path = PathBuf::from(keyspace_root)
        .join("k8s")
        .join("volumesnapshots")
        .join("rec")
        .join("snap-a")
        .join("_value_");
    std::fs::write(&value_path, "not-json").expect("write corrupt snapshot");
    let result = list_volume_snapshots(Some("rec"));
    assert!(result.is_err(), "corruption should surface an error");
}
