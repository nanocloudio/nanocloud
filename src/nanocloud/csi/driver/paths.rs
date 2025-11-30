use std::env;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use crate::nanocloud::util::error::{new_error, with_context};

pub const VOLUMES_PREFIX: &str = "/volumes";
pub const SERVICES_PREFIX: &str = "/services";
pub const SNAPSHOTS_PREFIX: &str = "/snapshots";

#[cfg_attr(test, allow(dead_code))]
pub const DEFAULT_STORAGE_ROOT: &str = "/var/lib/nanocloud.io/storage/csi";
pub const VOLUMES_DIR: &str = "volumes";
pub const PUBLISH_DIR: &str = "publish";
pub const SNAPSHOTS_DIR: &str = "snapshots";

pub fn storage_root() -> PathBuf {
    env::var("NANOCLOUD_CSI_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_storage_root())
}

#[cfg(not(test))]
fn default_storage_root() -> PathBuf {
    PathBuf::from(DEFAULT_STORAGE_ROOT)
}

#[cfg(test)]
fn default_storage_root() -> PathBuf {
    env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target"))
        .join("csi")
        .join("storage")
}

pub fn volume_root() -> PathBuf {
    storage_root().join(VOLUMES_DIR)
}

pub fn publish_root_path() -> PathBuf {
    storage_root().join(PUBLISH_DIR)
}

pub fn snapshot_root() -> PathBuf {
    storage_root().join(SNAPSHOTS_DIR)
}

pub fn volume_key(volume_id: &str) -> String {
    format!("{}/{}", VOLUMES_PREFIX, volume_id)
}

pub fn service_key(namespace: &str, service: &str) -> String {
    format!("{}/{}/{}", SERVICES_PREFIX, namespace, service)
}

pub fn snapshot_key(snapshot_id: &str) -> String {
    format!("{}/{}", SNAPSHOTS_PREFIX, snapshot_id)
}

pub fn volume_path(volume_id: &str) -> PathBuf {
    volume_root().join(volume_id)
}

#[allow(dead_code)]
pub fn publication_path(target_path: &str) -> PathBuf {
    PathBuf::from(target_path)
}

pub fn snapshot_archive_path(snapshot_id: &str) -> PathBuf {
    snapshot_root().join(format!("{}.tar", snapshot_id))
}

pub fn ensure_storage_roots() -> Result<(), Box<dyn Error + Send + Sync>> {
    let volume_dir = volume_root();
    fs::create_dir_all(&volume_dir).map_err(|e| {
        with_context(
            e,
            format!("Failed to create volume root {}", volume_dir.display()),
        )
    })?;
    let publish_dir = publish_root_path();
    fs::create_dir_all(&publish_dir).map_err(|e| {
        with_context(
            e,
            format!("Failed to create publish root {}", publish_dir.display()),
        )
    })?;
    let snapshot_dir = snapshot_root();
    fs::create_dir_all(&snapshot_dir).map_err(|e| {
        with_context(
            e,
            format!("Failed to create snapshot root {}", snapshot_dir.display()),
        )
    })?;
    Ok(())
}

#[allow(dead_code)]
pub fn ensure_under_root(root: &Path, path: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
    let canonical_root = root.canonicalize().map_err(|e| {
        with_context(
            e,
            format!("Failed to canonicalize storage root {}", root.display()),
        )
    })?;
    let canonical_path = path.canonicalize().map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to canonicalize path {} for containment check",
                path.display()
            ),
        )
    })?;
    if !canonical_path.starts_with(&canonical_root) {
        return Err(with_context(
            new_error("Path escapes storage root"),
            format!(
                "Resolved path {} is outside storage root {}",
                canonical_path.display(),
                canonical_root.display()
            ),
        ));
    }
    Ok(())
}
