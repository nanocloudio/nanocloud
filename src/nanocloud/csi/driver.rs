/*
 * Copyright (C) 2024 The Nanocloud Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use super::types::*;
use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::util::security::volume::{
    close_mapper, ensure_luks_device, mkfs_device, mount_mapper, open_mapper, read_volume_key,
    sanitize_identifier, unmount_if_mounted,
};
use crate::nanocloud::util::{is_missing_value_error, Keyspace};

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use tar::{Archive, Builder};

const CSI_KEYSPACE: Keyspace = Keyspace::new("csi");
const VOLUMES_PREFIX: &str = "/volumes";
const SERVICES_PREFIX: &str = "/services";
const SNAPSHOTS_PREFIX: &str = "/snapshots";

const DEFAULT_STORAGE_ROOT: &str = "/var/lib/nanocloud.io/storage/csi";
const VOLUMES_DIR: &str = "volumes";
const PUBLISH_DIR: &str = "publish";
const SNAPSHOTS_DIR: &str = "snapshots";
const DEFAULT_ENCRYPTED_VOLUME_SIZE: u64 = 1 << 30; // 1 GiB

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredVolume {
    pub volume: Volume,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub parameters: HashMap<String, String>,
    pub path: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub publications: Vec<String>,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encrypted: Option<StoredEncryptedVolume>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub snapshot: Snapshot,
    #[serde(rename = "archivePath")]
    pub archive_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredEncryptedVolume {
    #[serde(rename = "keyName")]
    pub key_name: String,
    pub mapper: String,
    #[serde(rename = "filesystem")]
    pub filesystem: String,
    #[serde(rename = "backingPath")]
    pub backing_path: String,
    #[serde(rename = "sizeBytes")]
    pub size_bytes: u64,
}

fn storage_root() -> PathBuf {
    env::var("NANOCLOUD_CSI_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_STORAGE_ROOT))
}

fn volume_root() -> PathBuf {
    storage_root().join(VOLUMES_DIR)
}

fn publish_root_path() -> PathBuf {
    storage_root().join(PUBLISH_DIR)
}

fn snapshot_root() -> PathBuf {
    storage_root().join(SNAPSHOTS_DIR)
}

fn sanitize_name(name: &str) -> String {
    let mut sanitized = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '.' || ch == '_' {
            sanitized.push(ch);
        } else {
            sanitized.push('-');
        }
    }
    sanitized.trim_matches('-').to_lowercase()
}

fn append_snapshot_directory<W: Write>(
    builder: &mut Builder<W>,
    root: &Path,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    append_snapshot_entry(builder, root, root)
}

fn append_snapshot_entry<W: Write>(
    builder: &mut Builder<W>,
    root: &Path,
    path: &Path,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let metadata = fs::symlink_metadata(path).map_err(|e| {
        with_context(
            e,
            format!("Failed to inspect snapshot source entry {}", path.display()),
        )
    })?;

    let relative = path.strip_prefix(root).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to compute relative snapshot path for {}",
                path.display()
            ),
        )
    })?;

    if !relative.as_os_str().is_empty() {
        builder.append_path_with_name(path, relative).map_err(|e| {
            with_context(
                e,
                format!("Failed to append snapshot entry '{}'", relative.display()),
            )
        })?;
    }

    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to traverse snapshot source directory {}",
                    path.display()
                ),
            )
        })? {
            let entry = entry.map_err(|e| {
                with_context(
                    e,
                    format!(
                        "Failed to iterate snapshot source directory {}",
                        path.display()
                    ),
                )
            })?;
            append_snapshot_entry(builder, root, &entry.path())?;
        }
    }

    Ok(())
}

fn namespace_from_params(parameters: &HashMap<String, String>) -> String {
    parameters
        .get("namespace")
        .cloned()
        .filter(|ns| !ns.is_empty())
        .unwrap_or_else(|| "default".to_string())
}

fn service_from_params(parameters: &HashMap<String, String>) -> Option<String> {
    parameters.get("service").cloned()
}

fn claim_from_params(parameters: &HashMap<String, String>) -> Option<String> {
    parameters.get("claim").cloned()
}

fn volume_key(volume_id: &str) -> String {
    format!("{}/{}", VOLUMES_PREFIX, volume_id)
}

fn service_key(namespace: &str, service: &str) -> String {
    format!("{}/{}/{}", SERVICES_PREFIX, namespace, service)
}

fn snapshot_key(snapshot_id: &str) -> String {
    format!("{}/{}", SNAPSHOTS_PREFIX, snapshot_id)
}

fn volume_path(volume_id: &str) -> PathBuf {
    volume_root().join(volume_id)
}

fn publication_path(target_path: &str) -> PathBuf {
    PathBuf::from(target_path)
}

fn snapshot_archive_path(snapshot_id: &str) -> PathBuf {
    snapshot_root().join(format!("{}.tar", snapshot_id))
}

fn ensure_storage_roots() -> Result<(), Box<dyn Error + Send + Sync>> {
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

fn required_capacity(range: &Option<CapacityRange>) -> u64 {
    range
        .as_ref()
        .and_then(|r| r.required_bytes)
        .or_else(|| range.as_ref().and_then(|r| r.limit_bytes))
        .unwrap_or(0)
}

fn read_service_index(
    namespace: &str,
    service: &str,
) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
    let key = service_key(namespace, service);
    match CSI_KEYSPACE.get(&key) {
        Ok(raw) => serde_json::from_str(&raw)
            .map_err(|e| with_context(e, format!("Failed to parse service index {}", key))),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(Vec::new())
            } else {
                Err(with_context(
                    err,
                    format!("Failed to load service index {}", key),
                ))
            }
        }
    }
}

fn write_service_index(
    namespace: &str,
    service: &str,
    ids: &[String],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = service_key(namespace, service);
    if ids.is_empty() {
        match CSI_KEYSPACE.delete(&key) {
            Ok(()) => Ok(()),
            Err(err) => {
                if is_missing_value_error(err.as_ref()) {
                    Ok(())
                } else {
                    Err(with_context(
                        err,
                        format!("Failed to delete service index {}", key),
                    ))
                }
            }
        }
    } else {
        let payload = serde_json::to_string(ids)
            .map_err(|e| with_context(e, format!("Failed to serialize service index {}", key)))?;
        CSI_KEYSPACE
            .put(&key, &payload)
            .map_err(|e| with_context(e, format!("Failed to store service index {}", key)))
    }
}

pub struct CsiDriver {
    publish_root: PathBuf,
}

static INSTANCE: OnceLock<Arc<CsiDriver>> = OnceLock::new();

impl CsiDriver {
    pub fn shared() -> Arc<CsiDriver> {
        INSTANCE
            .get_or_init(|| {
                ensure_storage_roots().expect("failed to prepare CSI storage roots");
                Arc::new(CsiDriver {
                    publish_root: publish_root_path(),
                })
            })
            .clone()
    }

    pub async fn create_volume(
        &self,
        request: CreateVolumeRequest,
    ) -> Result<CreateVolumeResponse, Box<dyn Error + Send + Sync>> {
        let namespace = namespace_from_params(&request.parameters);
        let service = service_from_params(&request.parameters)
            .ok_or_else(|| new_error("CreateVolumeRequest.parameters must include 'service'"))?;
        let claim = claim_from_params(&request.parameters)
            .ok_or_else(|| new_error("CreateVolumeRequest.parameters must include 'claim'"))?;

        let base_name = if request.name.is_empty() {
            format!("{}-{}-{}", namespace, service, claim)
        } else {
            request.name
        };
        let volume_id = sanitize_name(&base_name);

        let encryption = parse_encryption_config(&request.parameters)?;

        let path = volume_path(&volume_id);
        fs::create_dir_all(&path).map_err(|e| {
            with_context(
                e,
                format!("Failed to create volume directory {}", path.display()),
            )
        })?;

        if encryption.is_some() && request.content_source.is_some() {
            return Err(new_error(
                "Restoring encrypted volumes from existing content is not supported yet",
            ));
        }

        if let Some(content_source) = request.content_source.as_ref() {
            if let Some(snapshot_source) = &content_source.snapshot {
                self.restore_from_snapshot(&snapshot_source.snapshot_id, &path)?;
            }
        }

        let created_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        let mut capacity_bytes = required_capacity(&request.capacity_range);
        let mut volume_context = HashMap::new();
        volume_context.insert("path".to_string(), path.display().to_string());
        volume_context.insert("namespace".to_string(), namespace.clone());
        volume_context.insert("service".to_string(), service.clone());
        volume_context.insert("claim".to_string(), claim.clone());

        let mut stored_encrypted: Option<StoredEncryptedVolume> = None;

        if let Some(config) = encryption {
            let mut backing_path = path.clone();
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

            stored_encrypted = Some(StoredEncryptedVolume {
                key_name: config.key_name.clone(),
                mapper: mapper.clone(),
                filesystem: config.filesystem.clone(),
                backing_path: backing_str.clone(),
                size_bytes,
            });

            volume_context.insert("encrypted".to_string(), "true".to_string());
            volume_context.insert("encrypted.keyName".to_string(), config.key_name.clone());
            volume_context.insert(
                "encrypted.filesystem".to_string(),
                config.filesystem.clone(),
            );
            volume_context.insert("encrypted.backingPath".to_string(), backing_str);
            volume_context.insert("encrypted.mapper".to_string(), mapper);

            if capacity_bytes == 0 {
                capacity_bytes = size_bytes;
            }
        }

        let volume = Volume {
            volume_id: volume_id.clone(),
            capacity_bytes,
            volume_context,
        };

        let stored = StoredVolume {
            volume: volume.clone(),
            parameters: request.parameters,
            path: path.display().to_string(),
            publications: Vec::new(),
            created_at,
            encrypted: stored_encrypted,
        };
        let payload = serde_json::to_string(&stored)
            .map_err(|e| with_context(e, "Failed to serialize volume record"))?;
        CSI_KEYSPACE
            .put(&volume_key(&volume_id), &payload)
            .map_err(|e| with_context(e, "Failed to persist volume record"))?;

        let mut index = read_service_index(&namespace, &service)?;
        if !index.contains(&volume_id) {
            index.push(volume_id.clone());
            write_service_index(&namespace, &service, &index)?;
        }

        Ok(CreateVolumeResponse { volume })
    }

    pub async fn delete_volume(
        &self,
        request: DeleteVolumeRequest,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = volume_key(&request.volume_id);
        let raw = match CSI_KEYSPACE.get(&key) {
            Ok(raw) => raw,
            Err(err) => {
                if is_missing_value_error(err.as_ref()) {
                    return Ok(());
                }
                return Err(err);
            }
        };
        let stored: StoredVolume = serde_json::from_str(&raw)
            .map_err(|e| with_context(e, "Failed to deserialize stored volume"))?;
        if !stored.publications.is_empty() {
            return Err(new_error(format!(
                "Volume '{}' is still published to {:?}",
                request.volume_id, stored.publications
            )));
        }

        let namespace = namespace_from_params(&stored.parameters);
        let service = service_from_params(&stored.parameters)
            .ok_or_else(|| new_error("Stored volume missing service parameter"))?;

        let mut index = read_service_index(&namespace, &service)?;
        index.retain(|entry| entry != &request.volume_id);
        write_service_index(&namespace, &service, &index)?;

        if Path::new(&stored.path).exists() {
            fs::remove_dir_all(&stored.path).map_err(|e| {
                with_context(
                    e,
                    format!("Failed to remove volume directory {}", stored.path),
                )
            })?;
        }
        CSI_KEYSPACE
            .delete(&key)
            .map_err(|e| with_context(e, "Failed to delete volume record"))?;
        Ok(())
    }

    pub async fn node_publish_volume(
        &self,
        request: NodePublishVolumeRequest,
    ) -> Result<NodePublishVolumeResponse, Box<dyn Error + Send + Sync>> {
        let key = volume_key(&request.volume_id);
        let raw = CSI_KEYSPACE
            .get(&key)
            .map_err(|e| with_context(e, format!("Failed to load volume {}", request.volume_id)))?;
        let mut stored: StoredVolume = serde_json::from_str(&raw)
            .map_err(|e| with_context(e, "Failed to deserialize stored volume"))?;

        let publish_path = publication_path(&request.target_path);
        if let Some(parent) = publish_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                with_context(
                    e,
                    format!("Failed to create publication parent {}", parent.display()),
                )
            })?;
        }

        let mut encryption_guard: Option<EncryptionMountGuard> = None;
        if let Some(encrypted) = stored.encrypted.as_ref() {
            let mount_point = Path::new(&stored.path);
            fs::create_dir_all(mount_point).map_err(|e| {
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
            encryption_guard = Some(EncryptionMountGuard::new(
                encrypted.mapper.clone(),
                mount_point.to_path_buf(),
            ));
        }

        if publish_path.exists() {
            let metadata = fs::symlink_metadata(&publish_path).map_err(|e| {
                with_context(
                    e,
                    format!(
                        "Failed to inspect publication path {}",
                        publish_path.display()
                    ),
                )
            })?;
            if metadata.file_type().is_symlink() {
                let target = fs::read_link(&publish_path).map_err(|e| {
                    with_context(
                        e,
                        format!("Failed to read symlink {}", publish_path.display()),
                    )
                })?;
                if target != Path::new(&stored.path) {
                    fs::remove_file(&publish_path).map_err(|e| {
                        with_context(
                            e,
                            format!("Failed to remove stale symlink {}", publish_path.display()),
                        )
                    })?;
                    symlink(&stored.path, &publish_path).map_err(|e| {
                        with_context(
                            e,
                            format!(
                                "Failed to create publication symlink {} -> {}",
                                publish_path.display(),
                                stored.path
                            ),
                        )
                    })?;
                }
            } else if metadata.is_dir() {
                // Directory already exists; assume it is the correct path.
            } else {
                fs::remove_file(&publish_path).map_err(|e| {
                    with_context(
                        e,
                        format!("Failed to remove file {}", publish_path.display()),
                    )
                })?;
                symlink(&stored.path, &publish_path).map_err(|e| {
                    with_context(
                        e,
                        format!(
                            "Failed to create publication symlink {} -> {}",
                            publish_path.display(),
                            stored.path
                        ),
                    )
                })?;
            }
        } else {
            symlink(&stored.path, &publish_path).map_err(|e| {
                with_context(
                    e,
                    format!(
                        "Failed to create publication symlink {} -> {}",
                        publish_path.display(),
                        stored.path
                    ),
                )
            })?;
        }

        if !stored.publications.contains(&request.target_path) {
            stored.publications.push(request.target_path.clone());
            let payload = serde_json::to_string(&stored)
                .map_err(|e| with_context(e, "Failed to serialize updated volume"))?;
            CSI_KEYSPACE
                .put(&key, &payload)
                .map_err(|e| with_context(e, "Failed to persist updated volume"))?;
        }

        if let Some(mut guard) = encryption_guard {
            guard.commit();
        }

        Ok(NodePublishVolumeResponse {
            publish_path: publish_path.display().to_string(),
        })
    }

    pub async fn node_unpublish_volume(
        &self,
        request: NodeUnpublishVolumeRequest,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = volume_key(&request.volume_id);
        let raw = CSI_KEYSPACE
            .get(&key)
            .map_err(|e| with_context(e, format!("Failed to load volume {}", request.volume_id)))?;
        let mut stored: StoredVolume = serde_json::from_str(&raw)
            .map_err(|e| with_context(e, "Failed to deserialize stored volume"))?;

        let publish_path = publication_path(&request.target_path);
        if publish_path.exists() {
            let metadata = fs::symlink_metadata(&publish_path).map_err(|e| {
                with_context(
                    e,
                    format!(
                        "Failed to inspect publication path {}",
                        publish_path.display()
                    ),
                )
            })?;
            if metadata.file_type().is_symlink() {
                fs::remove_file(&publish_path).map_err(|e| {
                    with_context(
                        e,
                        format!("Failed to remove symlink {}", publish_path.display()),
                    )
                })?;
            } else if metadata.is_dir()
                && publish_path
                    .read_dir()
                    .map_err(|e| {
                        with_context(
                            e,
                            format!("Failed to list directory {}", publish_path.display()),
                        )
                    })?
                    .next()
                    .is_none()
            {
                fs::remove_dir(&publish_path).map_err(|e| {
                    with_context(
                        e,
                        format!("Failed to remove directory {}", publish_path.display()),
                    )
                })?;
            }
        }

        stored
            .publications
            .retain(|path| path != &request.target_path);
        let last_reference = stored.publications.is_empty();
        if last_reference {
            if let Some(encrypted) = stored.encrypted.as_ref() {
                let mount_point = Path::new(&stored.path);
                unmount_if_mounted(mount_point)?;
                close_mapper(&encrypted.mapper)?;
            }
        }
        let payload = serde_json::to_string(&stored)
            .map_err(|e| with_context(e, "Failed to serialize updated volume"))?;
        CSI_KEYSPACE
            .put(&key, &payload)
            .map_err(|e| with_context(e, "Failed to persist updated volume"))?;
        Ok(())
    }

    pub async fn create_snapshot(
        &self,
        request: CreateSnapshotRequest,
    ) -> Result<CreateSnapshotResponse, Box<dyn Error + Send + Sync>> {
        let key = volume_key(&request.source_volume_id);
        let raw = CSI_KEYSPACE.get(&key).map_err(|e| {
            with_context(
                e,
                format!("Failed to load volume {}", request.source_volume_id),
            )
        })?;
        let stored: StoredVolume = serde_json::from_str(&raw)
            .map_err(|e| with_context(e, "Failed to deserialize stored volume"))?;

        let snapshot_id = if request.name.is_empty() {
            format!("{}-snapshot", stored.volume.volume_id)
        } else {
            sanitize_name(&request.name)
        };

        let archive_path = snapshot_archive_path(&snapshot_id);
        if let Some(parent) = archive_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                with_context(
                    e,
                    format!("Failed to create snapshot directory {}", parent.display()),
                )
            })?;
        }

        let file = fs::File::create(&archive_path).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to create snapshot archive {}",
                    archive_path.display()
                ),
            )
        })?;
        let mut builder = Builder::new(file);
        append_snapshot_directory(&mut builder, Path::new(&stored.path))
            .map_err(|e| with_context(e, format!("Failed to archive volume {}", stored.path)))?;
        let mut file = builder
            .into_inner()
            .map_err(|e| with_context(e, "Failed to finalize snapshot archive builder"))?;
        file.flush()
            .map_err(|e| with_context(e, "Failed to flush snapshot archive"))?;
        drop(file);

        let size_bytes = fs::metadata(&archive_path)
            .map_err(|e| {
                with_context(
                    e,
                    format!("Failed to stat snapshot {}", archive_path.display()),
                )
            })?
            .len();
        let creation_time = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        let snapshot = Snapshot {
            snapshot_id: snapshot_id.clone(),
            source_volume_id: stored.volume.volume_id.clone(),
            size_bytes,
            ready_to_use: true,
            creation_time,
        };

        let stored_snapshot = StoredSnapshot {
            snapshot: snapshot.clone(),
            archive_path: archive_path.display().to_string(),
        };
        let payload = serde_json::to_string(&stored_snapshot)
            .map_err(|e| with_context(e, "Failed to serialize snapshot record"))?;
        CSI_KEYSPACE
            .put(&snapshot_key(&snapshot_id), &payload)
            .map_err(|e| with_context(e, "Failed to persist snapshot record"))?;

        Ok(CreateSnapshotResponse {
            snapshot,
            archive_path: stored_snapshot.archive_path,
        })
    }

    pub async fn delete_snapshot(
        &self,
        request: DeleteSnapshotRequest,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = snapshot_key(&request.snapshot_id);
        let raw = match CSI_KEYSPACE.get(&key) {
            Ok(raw) => raw,
            Err(err) => {
                if is_missing_value_error(err.as_ref()) {
                    return Ok(());
                }
                return Err(with_context(
                    err,
                    format!("Failed to load snapshot {}", request.snapshot_id),
                ));
            }
        };
        let stored: StoredSnapshot = serde_json::from_str(&raw)
            .map_err(|e| with_context(e, "Failed to deserialize stored snapshot"))?;
        if Path::new(&stored.archive_path).exists() {
            fs::remove_file(&stored.archive_path).map_err(|e| {
                with_context(
                    e,
                    format!("Failed to remove snapshot archive {}", stored.archive_path),
                )
            })?;
        }
        CSI_KEYSPACE
            .delete(&key)
            .map_err(|e| with_context(e, "Failed to delete snapshot record"))?;
        Ok(())
    }

    pub fn load_volume(
        &self,
        volume_id: &str,
    ) -> Result<Option<StoredVolume>, Box<dyn Error + Send + Sync>> {
        let key = volume_key(volume_id);
        match CSI_KEYSPACE.get(&key) {
            Ok(raw) => serde_json::from_str(&raw)
                .map(Some)
                .map_err(|e| with_context(e, "Failed to deserialize stored volume")),
            Err(err) => {
                if is_missing_value_error(err.as_ref()) {
                    Ok(None)
                } else {
                    Err(with_context(
                        err,
                        format!("Failed to load volume {}", volume_id),
                    ))
                }
            }
        }
    }

    pub fn list_service_volumes(
        &self,
        namespace: &str,
        service: &str,
    ) -> Result<Vec<StoredVolume>, Box<dyn Error + Send + Sync>> {
        let ids = read_service_index(namespace, service)?;
        let mut volumes = Vec::new();
        for id in ids {
            if let Some(volume) = self.load_volume(&id)? {
                volumes.push(volume);
            }
        }
        Ok(volumes)
    }

    pub fn load_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<Option<StoredSnapshot>, Box<dyn Error + Send + Sync>> {
        let key = snapshot_key(snapshot_id);
        match CSI_KEYSPACE.get(&key) {
            Ok(raw) => serde_json::from_str(&raw)
                .map(Some)
                .map_err(|e| with_context(e, "Failed to deserialize stored snapshot")),
            Err(err) => {
                if is_missing_value_error(err.as_ref()) {
                    Ok(None)
                } else {
                    Err(with_context(
                        err,
                        format!("Failed to load snapshot {}", snapshot_id),
                    ))
                }
            }
        }
    }

    pub fn restore_from_snapshot(
        &self,
        snapshot_id: &str,
        path: &Path,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if !path.exists() {
            fs::create_dir_all(path).map_err(|e| {
                with_context(
                    e,
                    format!("Failed to create restore directory {}", path.display()),
                )
            })?;
        }
        let stored = self
            .load_snapshot(snapshot_id)?
            .ok_or_else(|| new_error(format!("Snapshot '{}' not found", snapshot_id)))?;
        let file = fs::File::open(&stored.archive_path).map_err(|e| {
            with_context(
                e,
                format!("Failed to open snapshot archive {}", stored.archive_path),
            )
        })?;
        let mut archive = Archive::new(file);
        archive.set_preserve_permissions(true);
        archive.set_preserve_ownerships(true);
        archive.unpack(path).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to unpack snapshot '{}' into {}",
                    snapshot_id,
                    path.display()
                ),
            )
        })?;
        Ok(())
    }
}

struct CreateVolumeEncryptionConfig {
    key_name: String,
    filesystem: String,
}

fn parse_encryption_config(
    parameters: &HashMap<String, String>,
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

struct EncryptionMountGuard {
    mapper: String,
    mount_point: PathBuf,
    committed: bool,
}

impl EncryptionMountGuard {
    fn new(mapper: String, mount_point: PathBuf) -> Self {
        EncryptionMountGuard {
            mapper,
            mount_point,
            committed: false,
        }
    }

    fn commit(&mut self) {
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
impl CsiDriver {
    pub fn publish_root(&self) -> &Path {
        &self.publish_root
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn sanitize_name_normalizes_and_replaces_invalid_chars() {
        assert_eq!(sanitize_name("App.Name"), "app.name");
        assert_eq!(sanitize_name("App Name/One"), "app-name-one");
    }

    #[test]
    fn sanitize_name_trims_leading_and_trailing_hyphens() {
        assert_eq!(sanitize_name("---UPPER---"), "upper");
        assert_eq!(sanitize_name("###"), "");
    }

    #[test]
    fn namespace_from_params_defaults_to_default() {
        let mut params = HashMap::new();
        assert_eq!(namespace_from_params(&params), "default");
        params.insert("namespace".into(), "".into());
        assert_eq!(namespace_from_params(&params), "default");
        params.insert("namespace".into(), "prod".into());
        assert_eq!(namespace_from_params(&params), "prod");
    }

    #[test]
    fn service_and_claim_from_params_propagate_values() {
        let mut params = HashMap::new();
        params.insert("service".into(), "api".into());
        params.insert("claim".into(), "data".into());

        assert_eq!(service_from_params(&params).as_deref(), Some("api"));
        assert_eq!(claim_from_params(&params).as_deref(), Some("data"));

        params.remove("service");
        assert!(service_from_params(&params).is_none());
    }

    #[test]
    fn required_capacity_prefers_required_bytes() {
        let range = CapacityRange {
            required_bytes: Some(1_024),
            limit_bytes: Some(2_048),
        };
        assert_eq!(required_capacity(&Some(range)), 1_024);
        assert_eq!(required_capacity(&None), 0);
        let range = CapacityRange {
            required_bytes: None,
            limit_bytes: Some(4_096),
        };
        assert_eq!(required_capacity(&Some(range)), 4_096);
    }

    #[test]
    fn key_and_path_helpers_produce_expected_locations() {
        assert_eq!(volume_key("vol-1"), "/volumes/vol-1");
        assert_eq!(service_key("ns", "svc"), "/services/ns/svc");
        assert_eq!(snapshot_key("snap"), "/snapshots/snap");
        assert_eq!(
            volume_path("vol-1"),
            PathBuf::from("/var/lib/nanocloud.io/storage/csi/volumes/vol-1")
        );
        assert_eq!(publication_path("/mnt/data"), PathBuf::from("/mnt/data"));
        assert_eq!(
            snapshot_archive_path("snap"),
            PathBuf::from("/var/lib/nanocloud.io/storage/csi/snapshots/snap.tar")
        );
    }
}
