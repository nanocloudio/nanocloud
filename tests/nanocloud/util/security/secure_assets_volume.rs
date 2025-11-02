use std::fs;

use nanocloud::nanocloud::util::security::SecureAssets;
use tempfile::TempDir;

#[test]
fn volume_keys_generate_with_fingerprint() {
    let temp = TempDir::new().expect("tempdir");
    let dir = temp.path().to_path_buf();

    let info = SecureAssets::ensure_volume_keys(&dir, &[String::from("data-volume")], false)
        .expect("generate volume key");
    assert_eq!(info.len(), 1);
    let meta = &info[0];
    assert!(meta.created);
    assert_eq!(meta.volume, "data-volume");
    assert_eq!(meta.fingerprint.len(), 64);
    assert!(meta.fingerprint.chars().all(|c| c.is_ascii_hexdigit()));
    assert!(meta.path.exists());

    let contents = fs::read_to_string(&meta.path).expect("read key");
    assert!(!contents.trim().is_empty());

    let second = SecureAssets::ensure_volume_keys(&dir, &[String::from("data-volume")], false)
        .expect("reuse volume key");
    assert!(!second[0].created);
    assert_eq!(second[0].fingerprint, meta.fingerprint);
}

#[test]
fn invalid_volume_name_rejected() {
    let temp = TempDir::new().expect("tempdir");
    let dir = temp.path().to_path_buf();

    let err = SecureAssets::ensure_volume_keys(&dir, &[String::from("INVALID")], false)
        .expect_err("uppercase volume should fail");
    assert!(
        err.to_string()
            .contains("Volume name must use lowercase ASCII letters"),
        "unexpected error: {err}"
    );
}
