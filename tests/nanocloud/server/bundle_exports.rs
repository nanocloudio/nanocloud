use std::collections::HashMap;
use std::env;
use std::io::{Cursor, Read};

use axum::extract::Path;
use nanocloud::nanocloud::api::types::{
    Bundle, BundleProfileArtifact, BundleProfileExportManifest, BundleSpec,
};
use nanocloud::nanocloud::engine::profile::{BindingStatus, Profile, RESERVED_BINDINGS_KEY};
use nanocloud::nanocloud::k8s::bundle_manager::BundleRegistry;
use nanocloud::nanocloud::k8s::pod::ObjectMeta;
use nanocloud::nanocloud::server::handlers::bundles::export_profile;
use nanocloud::nanocloud::test_support::keyspace_lock;
use serde_json;
use sha2::{Digest, Sha256};
use tar::Archive;
use tempfile::TempDir;

fn sample_bundle(namespace: &str, service: &str) -> Bundle {
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

#[tokio::test]
async fn export_profile_tarball_enforces_integrity_and_round_trip() {
    let _lock = keyspace_lock().lock();
    let temp_dir = TempDir::new().expect("tempdir");
    let keyspace_root = temp_dir.into_path();
    env::set_var("NANOCLOUD_KEYSPACE", &keyspace_root);

    let namespace = "default";
    let service = "bundle-export";
    let mut options = HashMap::new();
    options.insert("region".to_string(), "eu-central-1".to_string());
    options.insert("secret_token".to_string(), "super-secret".to_string());

    let mut profile = Profile::from_options(&options).expect("build profile");
    {
        let record = profile.binding_record_mut(
            "init-db",
            "database",
            &[String::from("/opt/bin/init-db")],
        );
        record.status = BindingStatus::Succeeded;
        record.attempts = 2;
        record.exit_code = Some(0);
        record.message = Some("completed".to_string());
    }
    let (profile_key, serialized_config) = profile
        .to_serialized_fields()
        .expect("serialize profile state");

    let mut bundle = sample_bundle(namespace, service);
    bundle.spec.profile_key = Some(profile_key.clone());
    bundle.spec.options = serialized_config.clone();

    let registry = BundleRegistry::shared();
    registry
        .create(namespace, bundle)
        .await
        .expect("bundle stored");

    let (headers, body) = export_profile(Path((namespace.to_string(), service.to_string())))
        .await
        .expect("profile export");

    assert_eq!(
        headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/x-tar"),
        "exports should be served as tar archives"
    );

    let mut manifest: Option<BundleProfileExportManifest> = None;
    let mut artifact: Option<BundleProfileArtifact> = None;
    let mut archive = Archive::new(Cursor::new(body.to_vec()));
    for entry in archive.entries().expect("read tar entries") {
        let mut entry = entry.expect("tar entry");
        let mut buffer = Vec::new();
        entry.read_to_end(&mut buffer).expect("read entry content");
        let path = entry
            .path()
            .expect("entry path")
            .into_owned()
            .display()
            .to_string();
        match path.as_str() {
            "manifest.json" => {
                manifest = Some(
                    serde_json::from_slice(&buffer).expect("parse manifest payload"),
                );
            }
            "profile.json" => {
                artifact =
                    Some(serde_json::from_slice(&buffer).expect("parse profile artifact"));
            }
            other => panic!("unexpected file in export: {}", other),
        }
    }

    let manifest = manifest.expect("manifest file");
    let artifact = artifact.expect("profile artifact file");

    assert_eq!(manifest.metadata.name, service);
    assert_eq!(manifest.metadata.namespace, namespace);
    assert_eq!(manifest.metadata.service, service);
    assert_eq!(
        artifact.data.metadata.namespace, namespace,
        "artifact metadata should mirror the bundle namespace"
    );
    assert_eq!(
        artifact.data.profile_key, profile_key,
        "artifact should carry the wrapped profile key"
    );
    assert_eq!(
        artifact
            .data
            .options
            .get("region")
            .map(String::as_str),
        Some("eu-central-1"),
        "non-secret option should be preserved"
    );
    assert!(
        artifact
            .data
            .secrets
            .iter()
            .any(|secret| secret.name == "secret_token"),
        "secret entries should be present in the export"
    );
    assert_eq!(
        artifact.data.bindings.len(),
        1,
        "binding history should be included in the artifact"
    );

    let data_bytes =
        serde_json::to_vec(&artifact.data).expect("serialize artifact data for digesting");
    let digest = format!("{:x}", Sha256::digest(data_bytes));
    assert_eq!(digest, artifact.integrity.sha256, "artifact integrity hash should match computed digest");
    assert_eq!(
        manifest.digest, digest,
        "manifest digest should mirror the artifact integrity hash"
    );

    let mut reinstall_options = artifact.data.options.clone();
    for secret in &artifact.data.secrets {
        reinstall_options.insert(secret.name.clone(), secret.cipher_text.clone());
    }
    reinstall_options.insert(
        RESERVED_BINDINGS_KEY.to_string(),
        serde_json::to_string(&artifact.data.bindings).expect("serialize bindings"),
    );
    let reconstructed = Profile::from_spec_fields(
        Some(&artifact.data.profile_key),
        &reinstall_options,
    )
    .expect("rebuild profile from export payload");
    assert_eq!(
        reconstructed.binding_history_entries(),
        artifact.data.bindings,
        "binding history should survive export/import round-trip"
    );

    registry
        .delete(namespace, service)
        .await
        .expect("cleanup bundle");
}
