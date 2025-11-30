use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::time::Instant;

use crate::nanocloud::csi::types::{
    CreateVolumeRequest, CreateVolumeResponse, DeleteVolumeRequest, Volume,
};
use crate::nanocloud::util::error::{new_error, with_context};

use super::encryption::{create_encrypted_backing, parse_encryption_config};
use super::metadata::{
    add_volume_to_service, delete_volume_record, now_rfc3339, persist_volume,
    remove_volume_from_service, StoredEncryptedVolume, StoredVolume,
};
use super::paths::{ensure_storage_roots, volume_path};
use super::rollback::Rollback;
use super::validation::{
    claim_from_params, namespace_from_params, parse_volume_params, required_capacity,
    service_from_params,
};
use super::CsiDriver;

impl CsiDriver {
    pub async fn create_volume(
        &self,
        request: CreateVolumeRequest,
    ) -> Result<CreateVolumeResponse, Box<dyn Error + Send + Sync>> {
        let started = self.op_start("create_volume");
        let result: Result<CreateVolumeResponse, Box<dyn Error + Send + Sync>> = async {
            let identity = parse_volume_params(&request.parameters, &request.name)?;
            let lock_wait = Instant::now();
            let _lock = self.locks.lock(&identity.volume_id).await;
            self.track_lock_wait("create_volume", &identity.volume_id, lock_wait);
            ensure_storage_roots()?;

            let encryption = parse_encryption_config(&request.parameters)?;
            if encryption.is_some() && request.content_source.is_some() {
                return Err(new_error(
                    "Restoring encrypted volumes from existing content is not supported yet",
                ));
            }

            let path = volume_path(&identity.volume_id);
            let mut rollback = Rollback::new();
            if !path.exists() {
                fs::create_dir_all(&path).map_err(|e| {
                    with_context(
                        e,
                        format!("Failed to create volume directory {}", path.display()),
                    )
                })?;
                let path_clone = path.clone();
                rollback.push(move || {
                    if path_clone.exists() {
                        fs::remove_dir_all(&path_clone).map_err(|e| {
                            with_context(e, format!("Failed to rollback {}", path_clone.display()))
                        })?;
                    }
                    Ok(())
                });
            }

            if let Some(content_source) = request.content_source.as_ref() {
                if let Some(snapshot_source) = &content_source.snapshot {
                    self.restore_from_snapshot(&snapshot_source.snapshot_id, &path)?;
                }
            }

            let created_at = now_rfc3339();
            let mut capacity_bytes = required_capacity(&request.capacity_range);
            let mut volume_context = HashMap::new();
            volume_context.insert("path".to_string(), path.display().to_string());
            volume_context.insert("namespace".to_string(), identity.params.namespace.clone());
            volume_context.insert("service".to_string(), identity.params.service.clone());
            volume_context.insert("claim".to_string(), identity.params.claim.clone());

            let mut stored_encrypted: Option<StoredEncryptedVolume> = None;

            if let Some(config) = encryption {
                let stored =
                    create_encrypted_backing(&path, capacity_bytes, &config, &identity.volume_id)?;
                if capacity_bytes == 0 {
                    capacity_bytes = stored.size_bytes;
                }

                volume_context.insert("encrypted".to_string(), "true".to_string());
                volume_context.insert("encrypted.keyName".to_string(), stored.key_name.clone());
                volume_context.insert(
                    "encrypted.filesystem".to_string(),
                    stored.filesystem.clone(),
                );
                volume_context.insert(
                    "encrypted.backingPath".to_string(),
                    stored.backing_path.clone(),
                );
                volume_context.insert("encrypted.mapper".to_string(), stored.mapper.clone());

                let backing = stored.backing_path.clone();
                rollback.push(move || {
                    if Path::new(&backing).exists() {
                        let _ = fs::remove_file(&backing);
                    }
                    Ok(())
                });

                stored_encrypted = Some(stored);
            }

            let volume = Volume {
                volume_id: identity.volume_id.clone(),
                capacity_bytes,
                volume_context,
            };

            let mut stored_parameters = request.parameters.clone();
            stored_parameters.insert("namespace".to_string(), identity.params.namespace.clone());
            stored_parameters.insert("service".to_string(), identity.params.service.clone());
            stored_parameters.insert("claim".to_string(), identity.params.claim.clone());

            let stored = StoredVolume {
                volume: volume.clone(),
                parameters: stored_parameters,
                path: path.display().to_string(),
                publications: Vec::new(),
                created_at,
                encrypted: stored_encrypted,
            };

            persist_volume(&stored)?;
            let volume_id = identity.volume_id.clone();
            rollback.push(move || delete_volume_record(&volume_id));

            add_volume_to_service(
                &identity.params.namespace,
                &identity.params.service,
                &identity.volume_id,
            )?;
            let params_clone = identity.params.clone();
            let volume_id_clone = identity.volume_id.clone();
            rollback.push(move || {
                remove_volume_from_service(
                    &params_clone.namespace,
                    &params_clone.service,
                    &volume_id_clone,
                )
            });

            rollback.commit();
            Ok(CreateVolumeResponse { volume })
        }
        .await;

        match result {
            Ok(response) => {
                self.op_finish("create_volume", started, Ok(()));
                Ok(response)
            }
            Err(err) => {
                self.op_finish("create_volume", started, Err(format!("{err}")));
                Err(err)
            }
        }
    }

    pub async fn delete_volume(
        &self,
        request: DeleteVolumeRequest,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let started = self.op_start("delete_volume");
        let result: Result<(), Box<dyn Error + Send + Sync>> = async {
            let lock_wait = Instant::now();
            let _lock = self.locks.lock(&request.volume_id).await;
            self.track_lock_wait("delete_volume", &request.volume_id, lock_wait);

            let stored = match super::metadata::load_volume(&request.volume_id)? {
                Some(stored) => stored,
                None => return Ok(()),
            };

            if !stored.publications.is_empty() {
                return Err(new_error(format!(
                    "Volume '{}' is still published to {:?}",
                    request.volume_id, stored.publications
                )));
            }

            let namespace = namespace_from_params(&stored.parameters);
            let service = service_from_params(&stored.parameters)
                .ok_or_else(|| new_error("Stored volume missing service parameter"))?;
            let claim = claim_from_params(&stored.parameters)
                .ok_or_else(|| new_error("Stored volume missing claim parameter"))?;

            let mut rollback = Rollback::new();
            let stored_clone = stored.clone();
            rollback.push(move || persist_volume(&stored_clone));

            remove_volume_from_service(&namespace, &service, &request.volume_id)?;
            let ns_clone = namespace.clone();
            let svc_clone = service.clone();
            let vol_clone = request.volume_id.clone();
            rollback.push(move || add_volume_to_service(&ns_clone, &svc_clone, &vol_clone));

            if Path::new(&stored.path).exists() {
                fs::remove_dir_all(&stored.path).map_err(|e| {
                    with_context(
                        e,
                        format!("Failed to remove volume directory {}", stored.path),
                    )
                })?;
            }

            delete_volume_record(&request.volume_id)?;
            rollback.commit();

            // keep claim in scope until here to avoid unused warning
            let _ = claim;
            Ok(())
        }
        .await;

        match result {
            Ok(()) => {
                self.op_finish("delete_volume", started, Ok(()));
                Ok(())
            }
            Err(err) => {
                self.op_finish("delete_volume", started, Err(format!("{err}")));
                Err(err)
            }
        }
    }
}
