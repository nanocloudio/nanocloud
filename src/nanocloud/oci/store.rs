use std::path::PathBuf;

/// Returns the root directory for OCI image data, allowing tests to override it.
pub fn image_store_root() -> PathBuf {
    std::env::var("NANOCLOUD_IMAGE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/var/lib/nanocloud.io/image"))
}

/// Returns the path to a fake registry dataset when running in tests.
pub fn fake_registry_root() -> Option<PathBuf> {
    std::env::var("NANOCLOUD_FAKE_REGISTRY")
        .ok()
        .map(PathBuf::from)
}
