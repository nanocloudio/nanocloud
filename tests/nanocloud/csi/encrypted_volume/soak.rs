use std::collections::HashMap;
use std::fs;

use super::support::EncryptedVolumeTestEnv;

use nanocloud::nanocloud::csi::{
    CreateVolumeRequest, CsiDriver, NodePublishVolumeRequest, NodeUnpublishVolumeRequest,
};

const ROTATION_ITERATIONS: usize = 5;
const RESIZE_ITERATIONS: usize = 5;

#[tokio::test]
async fn encrypted_volume_key_rotation_soak() {
    let env = EncryptedVolumeTestEnv::new("rotation-soak");
    let driver = CsiDriver::shared();
    let namespace = "default";
    let service = "rotate-svc";
    let claim = "data";

    let initial_key = "rotate-data-v0";
    env.ensure_volume_key(initial_key);

    let mut parameters = HashMap::new();
    parameters.insert("namespace".into(), namespace.into());
    parameters.insert("service".into(), service.into());
    parameters.insert("claim".into(), claim.into());
    parameters.insert("encryption.key".into(), initial_key.into());

    let created = driver
        .create_volume(CreateVolumeRequest {
            parameters,
            ..Default::default()
        })
        .await
        .expect("create encrypted volume");
    let volume_id = created.volume.volume_id.clone();

    for iteration in 1..=ROTATION_ITERATIONS {
        let key_name = format!("rotate-data-v{iteration}");
        env.ensure_volume_key(&key_name);
        env.update_encryption_key(&volume_id, &key_name);
        env.reset_log();

        let target_path = driver
            .publish_root()
            .join(format!("rotation/{iteration}/data"));
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent).expect("create publish parent");
        }

        driver
            .node_publish_volume(NodePublishVolumeRequest {
                volume_id: volume_id.clone(),
                target_path: target_path.display().to_string(),
                ..Default::default()
            })
            .await
            .expect("publish volume");

        driver
            .node_unpublish_volume(NodeUnpublishVolumeRequest {
                volume_id: volume_id.clone(),
                target_path: target_path.display().to_string(),
            })
            .await
            .expect("unpublish volume");

        let log = env.record_log();
        assert!(
            log.contains("ensure_luks"),
            "rotation iteration {iteration} should record ensure_luks: {log}"
        );
        assert!(
            log.contains("open_mapper"),
            "rotation iteration {iteration} should record open_mapper: {log}"
        );
        assert!(
            log.contains("close_mapper"),
            "rotation iteration {iteration} should record close_mapper: {log}"
        );
    }

    assert!(
        env.stored_publications(&volume_id).is_empty(),
        "publications should be cleared after rotation soak"
    );
}

#[tokio::test]
async fn encrypted_volume_resize_soak() {
    let env = EncryptedVolumeTestEnv::new("resize-soak");
    let driver = CsiDriver::shared();
    let namespace = "default";
    let service = "resize-svc";
    let claim = "state";

    let key_name = "resize-data";
    env.ensure_volume_key(key_name);

    let mut parameters = HashMap::new();
    parameters.insert("namespace".into(), namespace.into());
    parameters.insert("service".into(), service.into());
    parameters.insert("claim".into(), claim.into());
    parameters.insert("encryption.key".into(), key_name.into());

    let created = driver
        .create_volume(CreateVolumeRequest {
            parameters,
            ..Default::default()
        })
        .await
        .expect("create encrypted volume");
    let volume_id = created.volume.volume_id.clone();

    let mut current_size = env
        .stored_volume(&volume_id)
        .encrypted
        .expect("encrypted metadata required")
        .size_bytes;

    for iteration in 1..=RESIZE_ITERATIONS {
        current_size += 4 * 1024 * 1024;
        let backing_path = env.set_volume_size(&volume_id, current_size);
        env.reset_log();

        assert!(
            backing_path.exists(),
            "backing path should exist after resize: {}",
            backing_path.display()
        );

        let target_path = driver
            .publish_root()
            .join(format!("resize/{iteration}/state"));
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent).expect("create publish parent");
        }

        driver
            .node_publish_volume(NodePublishVolumeRequest {
                volume_id: volume_id.clone(),
                target_path: target_path.display().to_string(),
                ..Default::default()
            })
            .await
            .expect("publish resized volume");

        driver
            .node_unpublish_volume(NodeUnpublishVolumeRequest {
                volume_id: volume_id.clone(),
                target_path: target_path.display().to_string(),
            })
            .await
            .expect("unpublish resized volume");

        let log = env.record_log();
        assert!(
            log.contains("mount_mapper"),
            "resize iteration {iteration} should record mount_mapper: {log}"
        );
        assert!(
            log.contains("unmount"),
            "resize iteration {iteration} should record unmount: {log}"
        );
        assert!(
            log.contains("close_mapper"),
            "resize iteration {iteration} should record close_mapper: {log}"
        );
    }

    let final_record = env.stored_volume(&volume_id);
    let encrypted = final_record.encrypted.expect("encrypted metadata tracked");
    assert_eq!(
        encrypted.size_bytes, current_size,
        "stored metadata should reflect the last resize"
    );
    assert!(
        env.stored_publications(&volume_id).is_empty(),
        "publications should be cleared after resize soak"
    );
}
