use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::OnceLock;

use nanocloud::nanocloud::api::types::{Bundle, BundleSpec};
use nanocloud::nanocloud::k8s::bundle_manager::{
    BundleApplyError, BundleApplyOptions, BundleRegistry,
};
use nanocloud::nanocloud::k8s::pod::ObjectMeta;
use serial_test::serial;
use tempfile::TempDir;
use serde_json::json;

fn bundle_without_defaults() -> Bundle {
    Bundle {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "Bundle".to_string(),
        metadata: ObjectMeta {
            name: None,
            namespace: None,
            ..Default::default()
        },
        spec: BundleSpec {
            service: "demo".to_string(),
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
    }
}

fn ensure_keyspace_root() {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();
    ROOT.get_or_init(|| {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.into_path();
        env::set_var("NANOCLOUD_KEYSPACE", &path);
        path
    });
}

#[tokio::test]
#[serial]
async fn create_applies_namespace_and_name_defaults() {
    ensure_keyspace_root();

    let registry = BundleRegistry::shared();
    let stored = registry
        .create("testing", bundle_without_defaults())
        .await
        .expect("bundle created");

    assert_eq!(
        stored.metadata.name.as_deref(),
        Some("demo"),
        "metadata name should default to spec.service"
    );
    assert_eq!(
        stored.metadata.namespace.as_deref(),
        Some("testing"),
        "metadata namespace should default to target namespace"
    );
    assert_eq!(
        stored.spec.namespace.as_deref(),
        Some("testing"),
        "spec namespace should mirror metadata namespace"
    );
    assert!(stored.spec.start, "start flag should remain default true");

    registry
        .delete("testing", stored.metadata.name.as_deref().unwrap())
        .await
        .expect("cleanup bundle");
}

#[tokio::test]
#[serial]
async fn apply_conflicts_without_force() {
    ensure_keyspace_root();

    let registry = BundleRegistry::shared();
    let namespace = "default";
    let service = "ssa-conflict";

    let mut bundle = bundle_without_defaults();
    bundle.spec.service = service.to_string();

    registry
        .create(namespace, bundle)
        .await
        .expect("bundle created");

    let mut ownership = registry
        .load_field_ownership(namespace, service)
        .await
        .expect("load ownership");
    ownership.set_owner("/spec/options", "controller/bundle");
    registry
        .save_field_ownership(namespace, service, &ownership)
        .await
        .expect("persist ownership");

    let payload = json!({
        "spec": {
            "options": {
                "mode": "user"
            }
        }
    });

    let result = registry
        .apply_bundle(
            namespace,
            service,
            payload,
            BundleApplyOptions {
                manager: "cli.test/v1",
                force: false,
                dry_run: false,
            },
        )
        .await;

    match result {
        Err(BundleApplyError::Conflict { conflicts, .. }) => {
            assert_eq!(conflicts.len(), 1, "expected single conflicting path");
            assert_eq!(conflicts[0].path, "/spec/options");
            assert_eq!(conflicts[0].owner, "controller/bundle");
        }
        other => panic!("expected conflict error, got {:?}", other),
    }

    registry
        .delete(namespace, service)
        .await
        .expect("cleanup bundle");
}

#[tokio::test]
#[serial]
async fn apply_dry_run_does_not_persist_mutations() {
    ensure_keyspace_root();

    let registry = BundleRegistry::shared();
    let namespace = "default";
    let service = "ssa-dry-run";

    let mut bundle = bundle_without_defaults();
    bundle.spec.service = service.to_string();
    bundle
        .spec
        .options
        .insert("current".to_string(), "old".to_string());

    registry
        .create(namespace, bundle)
        .await
        .expect("bundle created");

    let payload = json!({
        "spec": {
            "options": {
                "current": "new",
                "extra": "value"
            }
        }
    });

    let updated = registry
        .apply_bundle(
            namespace,
            service,
            payload,
            BundleApplyOptions {
                manager: "cli.test/v1",
                force: false,
                dry_run: true,
            },
        )
        .await
        .expect("dry-run should succeed");

    assert_eq!(
        updated
            .spec
            .options
            .get("current")
            .map(String::as_str),
        Some("new"),
        "returned bundle should include staged value"
    );
    assert!(
        updated.spec.options.contains_key("extra"),
        "returned bundle should contain speculative option"
    );

    let stored = registry
        .get(namespace, service)
        .await
        .expect("bundle exists");
    assert_eq!(
        stored
            .spec
            .options
            .get("current")
            .map(String::as_str),
        Some("old"),
        "persistent spec should remain unchanged after dry-run"
    );
    assert!(
        !stored.spec.options.contains_key("extra"),
        "persistent spec should not include speculative option"
    );

    let ownership = registry
        .load_field_ownership(namespace, service)
        .await
        .expect("load ownership");
    assert!(
        ownership.manager_for("/spec/options").is_none(),
        "dry-run should not update ownership metadata"
    );

    registry
        .delete(namespace, service)
        .await
        .expect("cleanup bundle");
}
