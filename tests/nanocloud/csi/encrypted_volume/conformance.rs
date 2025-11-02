use std::collections::HashMap;
use std::fs;
use std::path::Path;

use super::support::EncryptedVolumeTestEnv;

use nanocloud::nanocloud::csi::{
    CreateVolumeRequest, CsiDriver, NodePublishVolumeRequest, NodeUnpublishVolumeRequest,
};

#[tokio::test]
async fn encrypted_volume_create_persists_encrypted_metadata() {
    let env = EncryptedVolumeTestEnv::new("create-metadata");
    env.ensure_volume_key("svc-api-data");

    let driver = CsiDriver::shared();

    let mut parameters = HashMap::new();
    parameters.insert("namespace".into(), "default".into());
    parameters.insert("service".into(), "svc-api".into());
    parameters.insert("claim".into(), "data".into());
    parameters.insert("encryption.enabled".into(), "true".into());
    parameters.insert("encryption.key".into(), "svc-api-data".into());
    parameters.insert("encryption.fs".into(), "xfs".into());

    let response = driver
        .create_volume(CreateVolumeRequest {
            parameters,
            ..Default::default()
        })
        .await
        .expect("create encrypted volume");

    let volume_id = response.volume.volume_id.clone();
    assert_eq!(
        response
            .volume
            .volume_context
            .get("namespace")
            .map(String::as_str),
        Some("default")
    );
    assert_eq!(
        response
            .volume
            .volume_context
            .get("encrypted")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        response
            .volume
            .volume_context
            .get("encrypted.filesystem")
            .map(String::as_str),
        Some("xfs")
    );

    let stored = env.stored_volume(&volume_id);
    let encrypted = stored.encrypted.expect("encrypted metadata persisted");
    assert_eq!(encrypted.key_name, "svc-api-data");
    assert_eq!(encrypted.filesystem, "xfs");
    assert!(
        encrypted.backing_path.ends_with("backing.luks"),
        "backing path should end with backing.luks but was {}",
        encrypted.backing_path
    );
    assert!(
        stored
            .volume
            .volume_context
            .contains_key("encrypted.backingPath"),
        "volume context should expose backing path"
    );

    let log = env.record_log();
    assert!(
        log.contains("ensure_luks"),
        "ensure_luks entry should be recorded: {log}"
    );
    assert!(
        log.contains("open_mapper"),
        "open_mapper entry should be recorded: {log}"
    );
    assert!(log.contains("mkfs"), "mkfs entry should be recorded: {log}");
    assert!(
        log.contains("close_mapper"),
        "close_mapper entry should be recorded: {log}"
    );
}

#[tokio::test]
async fn encrypted_volume_publish_and_unpublish_record_flow() {
    let env = EncryptedVolumeTestEnv::new("publish-unpublish");
    env.ensure_volume_key("svc-metrics-state");
    let driver = CsiDriver::shared();

    let mut parameters = HashMap::new();
    parameters.insert("namespace".into(), "default".into());
    parameters.insert("service".into(), "svc-metrics".into());
    parameters.insert("claim".into(), "state".into());
    parameters.insert("encryption.key".into(), "svc-metrics-state".into());
    parameters.insert("encryption.fs".into(), "ext4".into());

    let create = driver
        .create_volume(CreateVolumeRequest {
            parameters,
            ..Default::default()
        })
        .await
        .expect("create encrypted volume");

    let volume_id = create.volume.volume_id.clone();
    env.reset_log();

    let target_path = driver.publish_root().join("svc-metrics-main").join("state");
    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent).expect("create publish parent");
    }

    let publish_response = driver
        .node_publish_volume(NodePublishVolumeRequest {
            volume_id: volume_id.clone(),
            target_path: target_path.display().to_string(),
            ..Default::default()
        })
        .await
        .expect("publish volume");
    assert_eq!(
        publish_response.publish_path,
        target_path.display().to_string()
    );
    assert!(Path::new(&publish_response.publish_path).exists());

    let publications = env.stored_publications(&volume_id);
    assert_eq!(publications, vec![target_path.display().to_string()]);

    let log = env.record_log();
    assert!(
        log.contains("open_mapper"),
        "open_mapper entry should be recorded during publish: {log}"
    );
    assert!(
        log.contains("mount_mapper"),
        "mount_mapper entry should be recorded during publish: {log}"
    );

    env.reset_log();
    driver
        .node_unpublish_volume(NodeUnpublishVolumeRequest {
            volume_id: volume_id.clone(),
            target_path: target_path.display().to_string(),
        })
        .await
        .expect("unpublish volume");

    let publications = env.stored_publications(&volume_id);
    assert!(
        publications.is_empty(),
        "publications should be cleared after unpublish: {:?}",
        publications
    );

    let log = env.record_log();
    assert!(
        log.contains("unmount"),
        "unmount entry should be recorded during unpublish: {log}"
    );
    assert!(
        log.contains("close_mapper"),
        "close_mapper entry should be recorded during unpublish: {log}"
    );
}

#[tokio::test]
async fn encrypted_volume_requires_key_when_enabled() {
    let _env = EncryptedVolumeTestEnv::new("create-missing-key");
    let driver = CsiDriver::shared();

    let mut parameters = HashMap::new();
    parameters.insert("namespace".into(), "default".into());
    parameters.insert("service".into(), "svc-audit".into());
    parameters.insert("claim".into(), "archive".into());
    parameters.insert("encryption.enabled".into(), "true".into());

    let result = driver
        .create_volume(CreateVolumeRequest {
            parameters,
            ..Default::default()
        })
        .await;

    let err = result.expect_err("missing encryption key should fail");
    assert!(
        err.to_string()
            .contains("encryption.enabled was set but encryption.key is missing"),
        "unexpected error message: {err}"
    );
}
