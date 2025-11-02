use crate::oci_harness::{EnvGuard, FakeRegistryFixture, HarnessArtifacts};
use nanocloud::nanocloud::engine::Image;
use nanocloud::nanocloud::oci::Registry;
use nanocloud::nanocloud::util::security::SecureAssets;
use serial_test::serial;
use std::collections::HashMap;
use tempfile::TempDir;

#[tokio::test]
#[serial]
async fn fake_registry_pull_reuses_cache_without_source_blobs() {
    let image_root = TempDir::new().expect("failed to create image root tempdir");
    let fake_registry = FakeRegistryFixture::new();
    let artifacts = HarnessArtifacts::new("image-pull-cache");
    let _image_guard = EnvGuard::set("NANOCLOUD_IMAGE_ROOT", image_root.path());
    let _registry_guard = fake_registry.activate();

    Registry::pull(fake_registry.image(), false)
        .await
        .expect("initial pull should succeed");

    fake_registry.remove_blob(fake_registry.config_digest());
    for digest in fake_registry.layer_digests() {
        fake_registry.remove_blob(&digest);
    }

    Registry::pull(fake_registry.image(), false)
        .await
        .expect("cached pull should succeed even when fake blobs removed");

    assert!(
        Registry::pull(fake_registry.image(), true).await.is_err(),
        "force update must fail when fake blobs are missing"
    );
    artifacts.write_text(
        "summary.txt",
        "Pulled fake-loop image twice; second run succeeded without blob sources. Forced update failed as expected.\n",
    );
}

#[tokio::test]
#[serial]
async fn macro_defaults_expand_expected_values() {
    let image_root = TempDir::new().expect("failed to create image root tempdir");
    let fake_registry = FakeRegistryFixture::new();
    let artifacts = HarnessArtifacts::new("macro-expansion");
    let _image_guard = EnvGuard::set("NANOCLOUD_IMAGE_ROOT", image_root.path());
    let _registry_guard = fake_registry.activate();
    let secure_dir = TempDir::new().expect("secure assets dir");
    SecureAssets::generate(secure_dir.path(), false).expect("generate secure assets");
    let _secure_guard = EnvGuard::set("NANOCLOUD_SECURE_ASSETS", secure_dir.path());

    let image = Image::load(None, "conformance/fake-loop", HashMap::new(), false)
        .await
        .expect("image load succeeds");

    let dns_value = image
        .config
        .options
        .get("macro_dns")
        .expect("macro_dns option present");
    assert!(
        !dns_value.is_empty(),
        "DNS macro should resolve to at least one server"
    );

    let rand_value = image
        .config
        .options
        .get("macro_rand")
        .expect("macro_rand present");
    assert_eq!(
        rand_value.len(),
        16,
        "rand hex macro should produce 16 characters for length 8"
    );
    assert!(
        rand_value.chars().all(|c| c.is_ascii_hexdigit()),
        "rand macro should emit hex characters"
    );

    let tls_value = image
        .config
        .options
        .get("macro_tls")
        .expect("macro_tls present");
    assert_eq!(
        tls_value, "/var/run/nanocloud.io/tls/cert.pem",
        "tls macro should map to cert path"
    );
    artifacts.write_text(
        "summary.txt",
        format!(
            "DNS macro resolved to {dns}; rand macro len {len}; tls macro -> {tls}\n",
            dns = dns_value,
            len = rand_value.len(),
            tls = tls_value
        ),
    );
}
