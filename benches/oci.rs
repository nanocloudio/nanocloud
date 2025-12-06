#![cfg(feature = "oci-bench")]

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use nanocloud::nanocloud::oci::Registry;
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use tokio::runtime::Runtime;

fn bench_registry_pull_cached(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    c.bench_function("oci_pull_cached_fake_registry", |b| {
        b.iter_batched(
            || {
                let store_dir = tempdir().expect("store dir");
                std::env::set_var("NANOCLOUD_IMAGE_ROOT", store_dir.path());
                let fake_root = tempdir().expect("fake registry");
                std::env::set_var("NANOCLOUD_FAKE_REGISTRY", fake_root.path());

                let registry = "registry.bench";
                let repository = "demo/bench";
                let tag = "latest";
                let (manifest_bytes, config_digest, layer_digest) =
                    build_fake_manifest_bytes(registry, repository, tag);
                write_fake_registry(
                    fake_root.path(),
                    registry,
                    repository,
                    tag,
                    &manifest_bytes,
                    &config_digest,
                    &layer_digest,
                );
                (
                    store_dir,
                    fake_root,
                    format!("{registry}/{repository}:{tag}"),
                )
            },
            |(store_dir, _fake_root, reference)| {
                rt.block_on(async {
                    let manifest = Registry::pull(&reference, false, None)
                        .await
                        .expect("bench pull");
                    assert_eq!(
                        manifest.config.media_type,
                        "application/vnd.oci.image.config.v1+json"
                    );
                    // Second pull should hit cache.
                    let cached = Registry::pull(&reference, false, None)
                        .await
                        .expect("cached");
                    assert_eq!(cached.config.digest, manifest.config.digest);
                    std::env::remove_var("NANOCLOUD_FAKE_REGISTRY");
                    std::env::remove_var("NANOCLOUD_IMAGE_ROOT");
                    drop(store_dir);
                })
            },
            BatchSize::SmallInput,
        )
    });
}

fn build_fake_manifest_bytes(
    registry: &str,
    repository: &str,
    tag: &str,
) -> (Vec<u8>, String, String) {
    let config_bytes = br#"{"architecture":"amd64","rootfs":{"diff_ids":[],"type":"layers"}}"#;
    let layer_bytes = b"nanocloud-layer";
    let config_digest = format!("sha256:{:x}", Sha256::digest(config_bytes));
    let layer_digest = format!("sha256:{:x}", Sha256::digest(layer_bytes));
    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config_digest,
            "size": config_bytes.len(),
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar",
                "digest": layer_digest,
                "size": layer_bytes.len(),
            }
        ],
        "annotations": {
            "org.opencontainers.image.ref.name": format!("{registry}/{repository}:{tag}")
        }
    });
    (
        serde_json::to_vec_pretty(&manifest).expect("manifest json"),
        config_digest,
        layer_digest,
    )
}

fn write_fake_registry(
    root: &std::path::Path,
    registry: &str,
    repository: &str,
    tag: &str,
    manifest_bytes: &[u8],
    config_digest: &str,
    layer_digest: &str,
) {
    use std::fs;
    let manifest_path = root
        .join("manifests")
        .join(registry)
        .join(repository)
        .join(format!("{tag}.json"));
    fs::create_dir_all(manifest_path.parent().unwrap()).expect("manifest dir");
    fs::write(&manifest_path, manifest_bytes).expect("manifest write");

    let blobs_dir = root.join("blobs/sha256");
    fs::create_dir_all(&blobs_dir).expect("blob dir");

    let config_bytes = br#"{"architecture":"amd64","rootfs":{"diff_ids":[],"type":"layers"}}"#;
    let config_hex = &config_digest["sha256:".len()..];
    fs::write(blobs_dir.join(config_hex), config_bytes).expect("config blob");

    let layer_bytes = b"nanocloud-layer";
    let layer_hex = &layer_digest["sha256:".len()..];
    let layer_hash = format!("{:x}", Sha256::digest(layer_bytes));
    assert_eq!(layer_hash, layer_hex);
    fs::write(blobs_dir.join(layer_hex), layer_bytes).expect("layer blob");
}

criterion_group!(oci, bench_registry_pull_cached);
criterion_main!(oci);
