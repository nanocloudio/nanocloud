use std::error::Error;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::util::security::volume::{
    close_mapper, ensure_luks_device, mkfs_device, mount_mapper, open_mapper, read_volume_key,
    sanitize_identifier, unmount_if_mounted,
};

use super::metadata::StoredEncryptedVolume;

pub const DEFAULT_ENCRYPTED_VOLUME_SIZE: u64 = 1 << 30; // 1 GiB

#[derive(Debug, Clone)]
pub struct CreateVolumeEncryptionConfig {
    pub key_name: String,
    pub filesystem: String,
}

pub fn parse_encryption_config(
    parameters: &std::collections::HashMap<String, String>,
) -> Result<Option<CreateVolumeEncryptionConfig>, Box<dyn Error + Send + Sync>> {
    if let Some(key) = parameters.get("encryption.key") {
        let filesystem = parameters
            .get("encryption.fs")
            .map(|value| value.to_string())
            .unwrap_or_else(|| "ext4".to_string());
        return Ok(Some(CreateVolumeEncryptionConfig {
            key_name: key.to_string(),
            filesystem,
        }));
    }

    if let Some(enabled) = parameters.get("encryption.enabled") {
        let normalized = enabled.to_ascii_lowercase();
        if normalized == "true" || normalized == "1" {
            return Err(new_error(
                "encryption.enabled was set but encryption.key is missing",
            ));
        }
    }

    Ok(None)
}

pub fn create_encrypted_backing(
    volume_path: &Path,
    capacity_bytes: u64,
    config: &CreateVolumeEncryptionConfig,
    volume_id: &str,
) -> Result<StoredEncryptedVolume, Box<dyn Error + Send + Sync>> {
    let mut backing_path = volume_path.to_path_buf();
    backing_path.push("backing.luks");

    let size_bytes = if capacity_bytes == 0 {
        DEFAULT_ENCRYPTED_VOLUME_SIZE
    } else {
        capacity_bytes
    };

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&backing_path)
        .map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to create encrypted backing file {}",
                    backing_path.display()
                ),
            )
        })?;
    file.set_len(size_bytes).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to size encrypted backing file {} to {} bytes",
                backing_path.display(),
                size_bytes
            ),
        )
    })?;
    file.sync_all().ok();
    drop(file);

    let key_bytes = read_volume_key(&config.key_name)?;
    let backing_str = backing_path.display().to_string();
    ensure_luks_device(&backing_str, &key_bytes)?;
    let mapper_suffix = sanitize_identifier(&format!("vol-{}", volume_id), "vol");
    let mapper = format!("ncld-{}", mapper_suffix);
    open_mapper(&backing_str, &mapper, &key_bytes)?;
    if let Err(err) = mkfs_device(&mapper, &config.filesystem) {
        let _ = close_mapper(&mapper);
        return Err(err);
    }
    close_mapper(&mapper)?;

    Ok(StoredEncryptedVolume {
        key_name: config.key_name.clone(),
        mapper: mapper.clone(),
        filesystem: config.filesystem.clone(),
        backing_path: backing_str,
        size_bytes,
    })
}

pub struct EncryptionMountGuard {
    mapper: String,
    mount_point: PathBuf,
    committed: bool,
}

impl EncryptionMountGuard {
    pub fn new(mapper: String, mount_point: PathBuf) -> Self {
        EncryptionMountGuard {
            mapper,
            mount_point,
            committed: false,
        }
    }

    pub fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for EncryptionMountGuard {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let _ = unmount_if_mounted(&self.mount_point);
        let _ = close_mapper(&self.mapper);
    }
}

pub fn mount_encrypted_volume(
    encrypted: &StoredEncryptedVolume,
    mount_point: &Path,
) -> Result<EncryptionMountGuard, Box<dyn Error + Send + Sync>> {
    std::fs::create_dir_all(mount_point).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to prepare encrypted mount directory {}",
                mount_point.display()
            ),
        )
    })?;
    unmount_if_mounted(mount_point)?;

    let key_bytes = read_volume_key(&encrypted.key_name)?;
    ensure_luks_device(&encrypted.backing_path, &key_bytes)?;
    open_mapper(&encrypted.backing_path, &encrypted.mapper, &key_bytes)?;
    if let Err(err) = mount_mapper(&encrypted.mapper, mount_point, &encrypted.filesystem) {
        let _ = close_mapper(&encrypted.mapper);
        return Err(err);
    }
    Ok(EncryptionMountGuard::new(
        encrypted.mapper.clone(),
        mount_point.to_path_buf(),
    ))
}
