use std::error::Error;
use std::fs;
use std::os::unix::fs::symlink;
use std::path::Path;
use std::time::Instant;

use crate::nanocloud::csi::types::{
    NodePublishVolumeRequest, NodePublishVolumeResponse, NodeUnpublishVolumeRequest,
};
use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::util::security::volume::{close_mapper, unmount_if_mounted};

use super::encryption::mount_encrypted_volume;
use super::metadata::{ensure_volume_exists, persist_volume};
use super::rollback::Rollback;
use super::validation::validate_target_path;
use super::CsiDriver;

impl CsiDriver {
    pub async fn node_publish_volume(
        &self,
        request: NodePublishVolumeRequest,
    ) -> Result<NodePublishVolumeResponse, Box<dyn Error + Send + Sync>> {
        let started = self.op_start("node_publish");
        let result: Result<NodePublishVolumeResponse, Box<dyn Error + Send + Sync>> = async {
            let lock_wait = Instant::now();
            let _lock = self.locks.lock(&request.volume_id).await;
            self.track_lock_wait("node_publish", &request.volume_id, lock_wait);
            let mut stored = ensure_volume_exists(&request.volume_id)?;

            let publish_path = validate_target_path(&request.target_path)?;
            if !publish_path.starts_with(&self.publish_root) {
                return Err(new_error(format!(
                    "Target path {} must be under publish root {}",
                    publish_path.display(),
                    self.publish_root.display()
                )));
            }

            if let Some(parent) = publish_path.parent() {
                fs::create_dir_all(parent).map_err(|e| {
                    with_context(
                        e,
                        format!("Failed to create publication parent {}", parent.display()),
                    )
                })?;
            }

            let mut rollback = Rollback::new();
            let mut encryption_guard: Option<super::encryption::EncryptionMountGuard> = None;

            if let Some(encrypted) = stored.encrypted.as_ref() {
                let mount_point = Path::new(&stored.path);
                let guard = mount_encrypted_volume(encrypted, mount_point)?;
                encryption_guard = Some(guard);
            }

            let mut symlink_created = false;
            let previous = stored.clone();
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
                                format!(
                                    "Failed to remove stale symlink {}",
                                    publish_path.display()
                                ),
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
                        symlink_created = true;
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
                    symlink_created = true;
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
                symlink_created = true;
            }

            if symlink_created {
                let path_clone = publish_path.clone();
                rollback.push(move || {
                    if path_clone.exists() {
                        fs::remove_file(&path_clone).map_err(|e| {
                            with_context(e, format!("Failed to rollback {}", path_clone.display()))
                        })?;
                    }
                    Ok(())
                });
            }

            #[cfg(test)]
            if std::env::var("NANOCLOUD_CSI_TEST_FAIL_PUBLISH_AFTER_SYMLINK").is_ok() {
                return Err(new_error("Injected publish failure"));
            }

            if !stored.publications.contains(&request.target_path) {
                stored.publications.push(request.target_path.clone());
            }
            persist_volume(&stored)?;
            rollback.push(move || persist_volume(&previous));

            if let Some(mut guard) = encryption_guard {
                guard.commit();
            }

            rollback.commit();

            Ok(NodePublishVolumeResponse {
                publish_path: publish_path.display().to_string(),
            })
        }
        .await;

        match result {
            Ok(resp) => {
                self.op_finish("node_publish", started, Ok(()));
                Ok(resp)
            }
            Err(err) => {
                self.op_finish("node_publish", started, Err(format!("{err}")));
                Err(err)
            }
        }
    }

    pub async fn node_unpublish_volume(
        &self,
        request: NodeUnpublishVolumeRequest,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let started = self.op_start("node_unpublish");
        let result: Result<(), Box<dyn Error + Send + Sync>> = async {
            let lock_wait = Instant::now();
            let _lock = self.locks.lock(&request.volume_id).await;
            self.track_lock_wait("node_unpublish", &request.volume_id, lock_wait);
            let mut stored = ensure_volume_exists(&request.volume_id)?;

            let publish_path = validate_target_path(&request.target_path)?;
            if !publish_path.starts_with(&self.publish_root) {
                return Err(new_error(format!(
                    "Target path {} must be under publish root {}",
                    publish_path.display(),
                    self.publish_root.display()
                )));
            }

            let mut rollback = Rollback::new();
            let previous = stored.clone();
            rollback.push(move || persist_volume(&previous));

            let mut removed_symlink = false;
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
                    removed_symlink = true;
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
                    removed_symlink = true;
                }
            }

            if removed_symlink {
                let path_clone = publish_path.clone();
                let stored_path = Path::new(&stored.path).to_path_buf();
                rollback.push(move || {
                    if !path_clone.exists() {
                        symlink(&stored_path, &path_clone).map_err(|e| {
                            with_context(
                                e,
                                format!(
                                    "Failed to recreate publication symlink {} -> {}",
                                    path_clone.display(),
                                    stored_path.display()
                                ),
                            )
                        })?;
                    }
                    Ok(())
                });
            }

            #[cfg(test)]
            if std::env::var("NANOCLOUD_CSI_TEST_FAIL_UNPUBLISH_AFTER_REMOVE").is_ok() {
                return Err(new_error("Injected unpublish failure"));
            }

            stored
                .publications
                .retain(|path| path != &request.target_path);
            let last_reference = stored.publications.is_empty();
            if last_reference {
                if let Some(encrypted) = stored.encrypted.clone() {
                    let mount_point = Path::new(&stored.path).to_path_buf();
                    let enc_clone = encrypted.clone();
                    let mount_point_clone = mount_point.clone();
                    rollback.push(move || {
                        let mut guard = super::encryption::mount_encrypted_volume(
                            &enc_clone,
                            &mount_point_clone,
                        )?;
                        guard.commit();
                        Ok(())
                    });
                    unmount_if_mounted(&mount_point)?;
                    close_mapper(&encrypted.mapper)?;
                }
            }

            persist_volume(&stored)?;
            rollback.commit();
            Ok(())
        }
        .await;

        match result {
            Ok(()) => {
                self.op_finish("node_unpublish", started, Ok(()));
                Ok(())
            }
            Err(err) => {
                self.op_finish("node_unpublish", started, Err(format!("{err}")));
                Err(err)
            }
        }
    }
}
