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

use crate::nanocloud::api::types::BundleSpec;
use crate::nanocloud::csi::{
    csi_plugin, CreateVolumeRequest, DeleteVolumeRequest, NodePublishVolumeRequest,
    NodeUnpublishVolumeRequest,
};
use crate::nanocloud::engine::Image;
use crate::nanocloud::engine::Profile;
use crate::nanocloud::engine::{
    register_streaming_backup, remove_streaming_backup, streaming_backup_enabled, Snapshot,
};
use crate::nanocloud::k8s::configmap::ConfigMap;
use crate::nanocloud::k8s::pod::{
    ContainerEnvVar, ContainerSpec, HostPathVolumeSource, KeyToPath, Pod, PodSpec, VolumeSpec,
};
use crate::nanocloud::k8s::store as pod_store;
use crate::nanocloud::kubelet::runtime::{
    create_container, exec_in_container, get_container_id_by_name, kill, remove_container,
    setup_container_files, setup_runtime_files, start_container,
};
use crate::nanocloud::kubelet::Kubelet;
use crate::nanocloud::logger::{log_debug, log_info, log_warn};
use crate::nanocloud::observability::metrics::{self, ContainerOperation};
use crate::nanocloud::oci::container_runtime;
use crate::nanocloud::secrets::{KeyspaceSecretStore, StoredSecret};
use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::Config;

pub use crate::nanocloud::kubelet::runtime::resolve_container_id;

use chrono::Local;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::Instant;

type DynError = Box<dyn Error + Send + Sync>;
type MaterializedFile = (PathBuf, Vec<u8>, u32);
type MaterializedFiles = Vec<MaterializedFile>;
type MaterializedResult<T> = Result<T, DynError>;

const CONFIGMAP_VOLUME_ROOT: &str = "/var/lib/nanocloud.io/storage/configmap";
const SECRET_VOLUME_ROOT: &str = "/var/lib/nanocloud.io/storage/secret";

#[derive(Clone, Debug)]
pub struct BackupPlan {
    pub owner: String,
    pub retention: usize,
}

impl BackupPlan {
    pub fn retention(&self) -> usize {
        self.retention.max(1)
    }
}

fn backup_root() -> PathBuf {
    Config::Backup.get_path()
}

pub fn backup_directory(_owner: &str, namespace: Option<&str>, app: &str) -> PathBuf {
    let namespace_component = namespace.filter(|ns| !ns.is_empty()).unwrap_or("default");
    backup_root().join(namespace_component).join(app)
}

pub fn latest_backup_path(
    owner: &str,
    namespace: Option<&str>,
    app: &str,
) -> Result<Option<PathBuf>, Box<dyn Error + Send + Sync>> {
    let dir = backup_directory(owner, namespace, app);
    collect_backup_files(&dir).map(|mut files| files.pop())
}

fn collect_backup_files(dir: &Path) -> Result<Vec<PathBuf>, Box<dyn Error + Send + Sync>> {
    let mut files = Vec::new();
    if !dir.exists() {
        return Ok(files);
    }
    for entry in fs::read_dir(dir).map_err(|err| {
        with_context(
            err,
            format!("Failed to read backup directory '{}'", dir.display()),
        )
    })? {
        let entry = entry.map_err(|err| {
            with_context(
                err,
                format!("Failed to iterate backup directory '{}'", dir.display()),
            )
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|err| {
            with_context(
                err,
                format!("Failed to inspect backup artifact '{}'", path.display()),
            )
        })?;
        if file_type.is_file() {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn prune_backups(dir: &Path, keep: usize) -> Result<(), Box<dyn Error + Send + Sync>> {
    if keep == 0 {
        return Ok(());
    }
    let mut files = collect_backup_files(dir)?;
    let mut removed = 0;
    while files.len() > keep {
        if let Some(path) = files.first().cloned() {
            let display_path = path.display().to_string();
            remove_streaming_backup(&path);
            match fs::remove_file(&path) {
                Ok(()) => {
                    removed += 1;
                    log_info(
                        "container",
                        "Removed expired service backup",
                        &[("path", display_path.as_str())],
                    );
                }
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!("Failed to remove expired backup '{}'", path.display()),
                    ));
                }
            }
            files.remove(0);
        } else {
            break;
        }
    }
    if removed > 0 {
        let kept = keep.to_string();
        log_info(
            "container",
            "Pruned service backups to enforce retention",
            &[("kept", kept.as_str())],
        );
    }
    Ok(())
}

pub struct InstallResult {
    pub pod: Pod,
    pub profile: Profile,
}

pub async fn install(
    namespace: Option<&str>,
    app: &str,
    options: HashMap<String, String>,
    snapshot: Option<&str>,
    force_update: bool,
) -> Result<InstallResult, Box<dyn Error + Send + Sync>> {
    metrics::observe_container_operation(
        namespace,
        app,
        ContainerOperation::Install,
        install_impl(namespace, app, options, snapshot, force_update),
    )
    .await
}

async fn install_impl(
    namespace: Option<&str>,
    app: &str,
    options: HashMap<String, String>,
    snapshot: Option<&str>,
    force_update: bool,
) -> Result<InstallResult, Box<dyn Error + Send + Sync>> {
    // Generate container name
    let container_name = namespace
        .map(|ns| format!("{}-{}", ns, app))
        .unwrap_or_else(|| app.to_string());

    let namespace_value = namespace.filter(|ns| !ns.is_empty()).unwrap_or("default");
    let snapshot_name = snapshot;
    let mut log_fields = vec![
        ("app", app),
        ("container", container_name.as_str()),
        ("namespace", namespace_value),
    ];
    if let Some(snap) = snapshot_name {
        log_fields.push(("snapshot", snap));
    }

    let existing_container_id = get_container_id_by_name(&container_name);
    let runtime = container_runtime();
    let existing_state = existing_container_id
        .as_ref()
        .and_then(|id| runtime.state(id).ok());
    let existing_digest = existing_state
        .as_ref()
        .and_then(|state| state.image_digest.clone());
    let mut existing_pod_manifest = pod_store::load_pod_manifest(namespace, app)
        .map_err(|e| with_context(e, format!("Failed to load pod spec for {container_name}")))?;
    let host_network = existing_pod_manifest
        .as_ref()
        .map(|pod| pod.spec.host_network)
        .unwrap_or(false);
    let mut user_provided_options = !options.is_empty();
    let mut cached_profile: Option<Profile> = None;
    let restore_requested = snapshot_name.is_some();

    // Read snapshot if provided
    let snapshot = snapshot_name
        .map(|snap| {
            Snapshot::new(snap)
                .map_err(|e| with_context(e, format!("Failed to load snapshot {snap}")))
        })
        .transpose()?;

    let mut snapshot_spec_contents: Option<BundleSpec> = None;
    let combined_options = match snapshot {
        Some(ref snap) => {
            let spec = snap
                .read_spec()
                .map_err(|e| with_context(e, "Failed to read bundle spec from snapshot"))?;
            let profile = Profile::from_spec_fields(spec.profile_key.as_deref(), &spec.options)
                .map_err(|e| with_context(e, "Failed to materialize snapshot profile"))?;
            snapshot_spec_contents = Some(spec);
            profile.extend(&options)
        }
        None => options.clone(),
    };

    // // Pull image
    // let manifest = Registry::pull(&format!("registry.nanocloud.io/{}", app)).await?;
    // let oci_image = OciImage::load(&manifest.config.digest)?;

    // Fetch image metadata and load profile
    let image = Image::load(namespace, app, combined_options, force_update)
        .await
        .map_err(|e| with_context(e, format!("Failed to load image metadata for {app}")))?;

    if existing_container_id.is_some() && user_provided_options {
        if let Ok(profile) = Profile::load(namespace, app).await {
            let options_match = options.iter().all(|(key, value)| {
                profile
                    .config
                    .get(key)
                    .map(|stored| stored.as_slice() == value.as_bytes())
                    .unwrap_or(false)
            }) && profile.config.len() == options.len();
            if options_match {
                user_provided_options = false;
                cached_profile = Some(profile);
            }
        }
    }

    let mut pod_manifest = image.to_pod_spec(
        &container_name,
        Some(&format!("registry.nanocloud.io/{}", app)),
    );
    let namespace_string = namespace
        .filter(|ns| !ns.is_empty())
        .map(|ns| ns.to_string());
    pod_manifest.metadata.namespace = namespace_string.clone();
    pod_manifest.metadata.annotations.insert(
        "nanocloud.io/profile-managed".to_string(),
        "true".to_string(),
    );

    let desired_digest = image.oci_manifest.config.digest.clone();
    let should_skip_update = !force_update
        && !user_provided_options
        && !restore_requested
        && existing_container_id.is_some()
        && existing_digest
            .as_deref()
            .map(|digest| digest == desired_digest.as_str())
            .unwrap_or(false);

    if should_skip_update {
        let digest_excerpt = &desired_digest.get(7..19).unwrap_or(&desired_digest);
        let mut skip_fields = vec![
            ("app", app),
            ("container", container_name.as_str()),
            ("namespace", namespace_value),
            ("digest", desired_digest.as_str()),
            ("excerpt", digest_excerpt),
        ];
        if let Some(state) = existing_state.as_ref() {
            if let Some(status) = state.status_detail.as_deref() {
                skip_fields.push(("status", status));
            }
        }
        log_info(
            "container",
            "Container image already up to date",
            &skip_fields,
        );

        let pod = existing_pod_manifest
            .take()
            .unwrap_or_else(|| pod_manifest.clone());
        let profile = match cached_profile.take() {
            Some(profile) => profile,
            None => match Profile::load(namespace, app).await {
                Ok(profile) => profile,
                Err(err) => {
                    let err_text = err.to_string();
                    log_warn(
                        "container",
                        "Falling back to image profile for up-to-date install",
                        &[
                            ("app", app),
                            ("container", container_name.as_str()),
                            ("namespace", namespace_value),
                            ("error", err_text.as_str()),
                        ],
                    );
                    Profile::from_options(&image.config.options).map_err(|e| {
                        with_context(
                            e,
                            "Failed to materialize profile while handling up-to-date install",
                        )
                    })?
                }
            },
        };
        return Ok(InstallResult { pod, profile });
    }

    log_info("container", "Starting container installation", &log_fields);
    if existing_container_id.is_some() {
        let mut diagnostics = vec![
            ("app", app),
            ("container", container_name.as_str()),
            ("namespace", namespace_value),
        ];
        let force_text = force_update.to_string();
        let options_text = user_provided_options.to_string();
        let restore_text = restore_requested.to_string();
        diagnostics.push(("force_update", force_text.as_str()));
        diagnostics.push(("user_provided_options", options_text.as_str()));
        diagnostics.push(("restore_requested", restore_text.as_str()));
        let digest_match = existing_digest
            .as_deref()
            .map(|digest| digest == desired_digest.as_str())
            .unwrap_or(false);
        let digest_text = digest_match.to_string();
        diagnostics.push(("digest_match", digest_text.as_str()));
        if let Some(state) = existing_state.as_ref() {
            if let Some(status) = state.status_detail.as_deref() {
                diagnostics.push(("status", status));
            }
        }
        log_debug(
            "container",
            "Skipping fast-path reinstall guard; proceeding with full reinstall",
            &diagnostics,
        );
    }

    if let Some(container_id) = existing_container_id.as_ref() {
        stop(namespace, app).await.map_err(|e| {
            with_context(
                e,
                format!("Failed to stop existing container '{}'", container_name),
            )
        })?;
        remove_container(&container_name, container_id, host_network).map_err(|e| {
            with_context(
                e,
                format!("Failed to remove existing container '{}'", container_name),
            )
        })?;
        cleanup_configmap_mounts(&container_name);
        Kubelet::shared()
            .forget_pod(namespace, app)
            .await
            .map_err(|e| with_context(e, "Failed to clear previous workload state"))?;
    }

    let mut runtime_pod_spec = pod_manifest.spec.clone();

    prepare_config_map_bindings(namespace_value, &container_name, &mut runtime_pod_spec).map_err(
        |e| {
            with_context(
                e,
                format!("Failed to prepare ConfigMap resources for {container_name}"),
            )
        },
    )?;

    let csi = csi_plugin();

    // Create external bindings
    let binding_map = image.config.bindings.clone();
    for (service, bindings) in binding_map {
        let container_id = get_container_id_by_name(&service).unwrap();
        for binding in bindings {
            let container_id_clone = container_id.clone();
            let binding_args = binding.clone();
            let service_name = service.clone();
            exec_in_container(&container_id_clone, move || {
                let (first, rest) = split_first_rest(&binding_args);
                let binding_display = binding_args.join(" ");
                let status = std::process::Command::new(first)
                    .args(rest)
                    .stdin(Stdio::inherit())
                    .stdout(Stdio::inherit())
                    .stderr(Stdio::inherit())
                    .status()
                    .map_err(|e| {
                        with_context(
                            e,
                            format!("Failed to spawn binding command: {binding_display}"),
                        )
                    })?;
                if status.success() {
                    Ok(())
                } else {
                    let descriptor = status
                        .code()
                        .map(|code| code.to_string())
                        .unwrap_or_else(|| "terminated by signal".to_string());
                    Err(new_error(format!(
                        "Binding command '{}' exited with status {descriptor}",
                        binding_display
                    )))
                }
            })
            .map_err(|e| {
                with_context(
                    e,
                    format!(
                        "Failed to execute binding on dependent service {}",
                        service_name
                    ),
                )
            })?;
        }
    }

    let _main_container = runtime_pod_spec
        .containers
        .first()
        .ok_or_else(|| new_error("Pod spec must contain at least one container"))?;

    let mut volume_host_paths: HashMap<String, String> = HashMap::new();

    // Provision and publish volumes through the CSI plugin (falling back to local storage for legacy specs)
    for volume in runtime_pod_spec.volumes.iter_mut() {
        if let Some(pvc) = volume.persistent_volume_claim.as_ref() {
            let mut parameters = HashMap::new();
            parameters.insert("namespace".to_string(), namespace_value.to_string());
            parameters.insert("service".to_string(), app.to_string());
            parameters.insert("claim".to_string(), pvc.claim_name.clone());
            if let Some(encrypted) = volume.encrypted.as_ref() {
                parameters.insert("encryption.enabled".to_string(), "true".to_string());
                parameters.insert("encryption.key".to_string(), encrypted.key_name.clone());
                if let Some(filesystem) = encrypted.filesystem.as_deref() {
                    parameters.insert("encryption.fs".to_string(), filesystem.to_string());
                }
            }

            let create_response = csi
                .create_volume(CreateVolumeRequest {
                    name: format!("{}-{}", container_name, pvc.claim_name),
                    capacity_range: None,
                    volume_capabilities: Vec::new(),
                    parameters,
                    content_source: None,
                })
                .await
                .map_err(|e| {
                    with_context(
                        e,
                        format!(
                            "Failed to provision volume '{}' for service '{}'",
                            pvc.claim_name, container_name
                        ),
                    )
                })?;

            let publish_root = csi.publish_root();
            let publish_target = format!(
                "{}/{}-{}",
                publish_root.display(),
                container_name,
                pvc.claim_name
            );

            let publish_response = csi
                .node_publish_volume(NodePublishVolumeRequest {
                    volume_id: create_response.volume.volume_id.clone(),
                    target_path: publish_target.clone(),
                    readonly: false,
                    volume_capability: None,
                    volume_context: HashMap::new(),
                })
                .await
                .map_err(|e| {
                    with_context(
                        e,
                        format!(
                            "Failed to publish volume '{}' for service '{}'",
                            pvc.claim_name, container_name
                        ),
                    )
                })?;

            let publish_path = publish_response.publish_path;
            volume.host_path = Some(HostPathVolumeSource {
                path: publish_path.clone(),
                r#type: None,
            });
            volume.persistent_volume_claim = None;
            if let Some(encrypted) = volume.encrypted.as_mut() {
                if let Some(device) = create_response
                    .volume
                    .volume_context
                    .get("encrypted.backingPath")
                {
                    encrypted.device = device.to_string();
                }
                if encrypted.filesystem.is_none() {
                    if let Some(filesystem) = create_response
                        .volume
                        .volume_context
                        .get("encrypted.filesystem")
                    {
                        encrypted.filesystem = Some(filesystem.to_string());
                    }
                }
            }
            volume_host_paths.insert(volume.name.clone(), publish_path);
        } else if volume.empty_dir.is_some() {
            let path = format!(
                "/var/lib/nanocloud.io/storage/volume/{}-{}",
                container_name, volume.name
            );
            fs::create_dir_all(&path).map_err(|e| {
                with_context(
                    e,
                    format!("Failed to prepare legacy volume '{}'", volume.name),
                )
            })?;
            volume.host_path = Some(HostPathVolumeSource {
                path: path.clone(),
                r#type: None,
            });
            volume_host_paths.insert(volume.name.clone(), path);
        }
    }

    pod_manifest.spec = runtime_pod_spec.clone();
    pod_store::save_pod_manifest(namespace, app, &pod_manifest).map_err(|e| {
        with_context(
            e,
            format!("Failed to persist pod spec for {container_name}"),
        )
    })?;

    // Create the container
    let container = create_container(
        &container_name,
        &image.oci_image,
        &image.oci_manifest,
        &runtime_pod_spec,
    )
    .await
    .map_err(|e| with_context(e, format!("Failed to create container {container_name}")))?;

    let container_debug_fields = [
        ("app", app),
        ("container", container_name.as_str()),
        ("namespace", namespace_value),
        ("container_id", container.id.as_str()),
    ];
    log_debug(
        "container",
        "Runtime container created",
        &container_debug_fields,
    );

    let ip = container
        .cni_config
        .as_ref()
        .and_then(|cni| cni.ips.first())
        .and_then(|ip| ip.address.split('/').next().map(str::to_string))
        .or_else(|| {
            runtime_pod_spec
                .containers
                .first()
                .and_then(|container| container.ports.iter().find_map(|port| port.host_ip.clone()))
        })
        .unwrap_or_else(|| "127.0.0.1".to_string());
    setup_container_files(&container.id, &ip)?;

    // Restore snapshot if available
    if let Some(ref snapshot) = snapshot {
        let mut restore_fields = vec![
            ("app", app),
            ("container", container_name.as_str()),
            ("namespace", namespace_value),
        ];
        if let Some(snap) = snapshot_name {
            restore_fields.push(("snapshot", snap));
        }
        log_info("container", "Restoring snapshot data", &restore_fields);
        let snapshot_summary = snapshot.summary().ok().flatten();
        let restore_start = Instant::now();
        snapshot
            .restore(app, &volume_host_paths)
            .await
            .map_err(|e| with_context(e, format!("Failed to restore snapshot for {app}")))?;
        let restore_duration = restore_start.elapsed();
        let (volumes, total_bytes) = snapshot_summary
            .map(|summary| {
                (
                    summary.entries.len(),
                    summary
                        .entries
                        .iter()
                        .map(|entry| entry.size_bytes)
                        .sum::<u64>(),
                )
            })
            .unwrap_or_else(|| (volume_host_paths.len(), 0));
        metrics::record_backup_restore(namespace, app, volumes, total_bytes, restore_duration);
        let duration_text = format!("{:.3}", restore_duration.as_secs_f64());
        let volumes_text = volumes.to_string();
        let bytes_text = total_bytes.to_string();
        let throughput = if restore_duration.as_secs_f64() > 0.0 && total_bytes > 0 {
            (total_bytes as f64 / (1 << 20) as f64) / restore_duration.as_secs_f64()
        } else {
            0.0
        };
        let throughput_text = format!("{throughput:.2}");
        let completed_fields = [
            ("app", app),
            ("container", container_name.as_str()),
            ("namespace", namespace_value),
            ("volumes", volumes_text.as_str()),
            ("bytes", bytes_text.as_str()),
            ("duration_seconds", duration_text.as_str()),
            ("throughput_mib_s", throughput_text.as_str()),
        ];
        log_info("container", "Restored snapshot data", &completed_fields);
    }

    let profile_source = if snapshot.is_some() {
        "snapshot"
    } else {
        "image"
    };
    let profile_fields = [
        ("app", app),
        ("container", container_name.as_str()),
        ("namespace", namespace_value),
        ("profile_source", profile_source),
    ];
    log_info("container", "Loading service profile", &profile_fields);
    // Load or create profile
    let profile = match snapshot_spec_contents.as_ref() {
        Some(spec) => {
            let mut profile = Profile::from_spec_fields(spec.profile_key.as_deref(), &spec.options)
                .map_err(|e| {
                    with_context(
                        e,
                        "Failed to materialize service profile from snapshot specification",
                    )
                })?;
            profile.set_options(&image.config.options);
            profile
        }
        None => Profile::from_options(&image.config.options)
            .map_err(|e| with_context(e, "Failed to build profile from image options"))?,
    };

    let runtime_fields = [
        ("app", app),
        ("container", container_name.as_str()),
        ("namespace", namespace_value),
        ("ip", ip.as_str()),
    ];
    log_info("container", "Configuring runtime files", &runtime_fields);
    let resolv_conf = std::fs::read_to_string("/etc/resolv.conf").ok();
    let app_name = app.to_string();
    let ip_for_runtime = ip.clone();
    let profile_config = profile.config.clone();
    let tls_info = image.config.tls_info.clone();
    let resolv_conf_owned = resolv_conf.clone();
    exec_in_container(&container.id, move || {
        setup_runtime_files(
            &app_name,
            &ip_for_runtime,
            &profile_config,
            &tls_info,
            resolv_conf_owned.as_deref(),
        )
    })
    .map_err(|e| with_context(e, "Failed to configure runtime files"))?;

    Kubelet::shared()
        .track_pod(namespace, app, pod_manifest.clone(), container.id.clone())
        .await
        .map_err(|e| with_context(e, "Failed to register workload with kubelet"))?;

    let completion_fields = [
        ("app", app),
        ("container", container_name.as_str()),
        ("namespace", namespace_value),
        ("ip", ip.as_str()),
    ];
    log_info(
        "container",
        "Container installation complete",
        &completion_fields,
    );
    Ok(InstallResult {
        pod: pod_manifest,
        profile,
    })
}

fn prepare_config_map_bindings(
    namespace: &str,
    container_name: &str,
    pod_spec: &mut PodSpec,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut config_map_cache: HashMap<String, Option<ConfigMap>> = HashMap::new();
    let mut secret_cache: HashMap<String, Option<StoredSecret>> = HashMap::new();
    let secret_store = KeyspaceSecretStore::new();

    for volume in pod_spec.volumes.iter_mut() {
        if volume.config_map.is_some() {
            materialize_config_map_volume(
                namespace,
                container_name,
                volume,
                &mut config_map_cache,
            )?;
        }
        if volume.secret.is_some() {
            materialize_secret_volume(
                namespace,
                container_name,
                volume,
                &mut secret_cache,
                &secret_store,
            )?;
        }
        if volume.projected.is_some() {
            materialize_projected_volume(namespace, container_name, volume, &mut config_map_cache)?;
        }
    }

    for container in pod_spec
        .init_containers
        .iter_mut()
        .chain(pod_spec.containers.iter_mut())
    {
        apply_env_from(
            namespace,
            container,
            &mut config_map_cache,
            &mut secret_cache,
            &secret_store,
        )?;
        resolve_env_value_from(
            namespace,
            container,
            &mut config_map_cache,
            &mut secret_cache,
            &secret_store,
        )?;
    }

    Ok(())
}

#[allow(dead_code)] // Integration tests call through `tests::support::secrets` to exercise this path.
#[doc(hidden)]
/// Internal helper used by integration tests to drive environment resolution without invoking the
/// full container installation flow.
pub fn resolve_env_with_secrets_for_testing(
    namespace: &str,
    container: &mut ContainerSpec,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut config_cache: HashMap<String, Option<ConfigMap>> = HashMap::new();
    let mut secret_cache: HashMap<String, Option<StoredSecret>> = HashMap::new();
    let store = KeyspaceSecretStore::new();
    apply_env_from(
        namespace,
        container,
        &mut config_cache,
        &mut secret_cache,
        &store,
    )?;
    resolve_env_value_from(
        namespace,
        container,
        &mut config_cache,
        &mut secret_cache,
        &store,
    )
}

#[allow(dead_code)] // Integration tests call through `tests::support::secrets` to exercise this path.
#[doc(hidden)]
/// Internal helper used by integration tests to materialize secret volumes without installing a
/// workload.
pub fn materialize_secret_volume_for_testing(
    namespace: &str,
    container_name: &str,
    volume: &mut VolumeSpec,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut cache: HashMap<String, Option<StoredSecret>> = HashMap::new();
    let store = KeyspaceSecretStore::new();
    materialize_secret_volume(namespace, container_name, volume, &mut cache, &store)
}

fn load_config_map_cached(
    namespace: &str,
    name: &str,
    cache: &mut HashMap<String, Option<ConfigMap>>,
) -> Result<Option<ConfigMap>, Box<dyn Error + Send + Sync>> {
    if let Some(entry) = cache.get(name) {
        return Ok(entry.clone());
    }

    let config_map = pod_store::load_config_map(Some(namespace), name)?;
    cache.insert(name.to_string(), config_map.clone());
    Ok(config_map)
}

fn load_secret_cached(
    namespace: &str,
    name: &str,
    cache: &mut HashMap<String, Option<StoredSecret>>,
    store: &KeyspaceSecretStore,
) -> Result<Option<StoredSecret>, Box<dyn Error + Send + Sync>> {
    let cache_key = format!("{namespace}/{name}");
    if let Some(entry) = cache.get(&cache_key) {
        return Ok(entry.clone());
    }

    let secret = store
        .get(namespace, name)
        .map_err(|e| with_context(e, format!("Failed to load secret '{name}' from store")))?;
    cache.insert(cache_key, secret.clone());
    Ok(secret)
}

fn materialized_volume_root(env_var: &str, default_root: &str) -> PathBuf {
    env::var(env_var)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(default_root))
}

fn configmap_volume_path(container_name: &str, volume_name: &str) -> PathBuf {
    materialized_volume_root("NANOCLOUD_CONFIGMAP_VOLUME_ROOT", CONFIGMAP_VOLUME_ROOT)
        .join(container_name)
        .join(volume_name)
}

fn secret_volume_path(container_name: &str, volume_name: &str) -> PathBuf {
    materialized_volume_root("NANOCLOUD_SECRET_VOLUME_ROOT", SECRET_VOLUME_ROOT)
        .join(container_name)
        .join(volume_name)
}

fn reset_volume_dir(path: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    Ok(())
}

fn resolve_mode(value: Option<i32>, default: u32) -> u32 {
    value
        .map(|mode| (mode.max(0) as u32) & 0o777)
        .unwrap_or(default)
}

fn sanitize_relative_path(path: &str) -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    let candidate = PathBuf::from(path);
    if candidate.is_absolute() {
        return Err(new_error(format!(
            "ConfigMap item path '{}' must be relative",
            path
        )));
    }
    if candidate
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(new_error(format!(
            "ConfigMap item path '{}' cannot traverse parent directories",
            path
        )));
    }
    Ok(candidate)
}

fn gather_configmap_files(
    config_map: &ConfigMap,
    items: &[KeyToPath],
    default_mode: u32,
    optional: bool,
) -> MaterializedResult<MaterializedFiles> {
    let mut files: MaterializedFiles = Vec::new();
    if items.is_empty() {
        for (key, value) in config_map.entries() {
            let relative = sanitize_relative_path(&key)?;
            files.push((relative, value, default_mode));
        }
    } else {
        for item in items {
            let relative = sanitize_relative_path(&item.path)?;
            match config_map.get_key_bytes(&item.key) {
                Some(bytes) => {
                    let mode = resolve_mode(item.mode, default_mode);
                    files.push((relative, bytes, mode));
                }
                None if optional => continue,
                None => {
                    let name = config_map
                        .metadata
                        .name
                        .clone()
                        .unwrap_or_else(|| "<unnamed>".to_string());
                    return Err(new_error(format!(
                        "ConfigMap '{}' missing key '{}'",
                        name, item.key
                    )));
                }
            }
        }
    }
    Ok(files)
}

fn write_materialized_files(base: &Path, files: MaterializedFiles) -> MaterializedResult<()> {
    for (relative, data, mode) in files {
        let target = base.join(&relative);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = File::create(&target)?;
        file.write_all(&data)?;
        fs::set_permissions(&target, fs::Permissions::from_mode(mode))?;
    }
    Ok(())
}

fn materialize_config_map_volume(
    namespace: &str,
    container_name: &str,
    volume: &mut VolumeSpec,
    cache: &mut HashMap<String, Option<ConfigMap>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let Some(source) = volume.config_map.as_ref() else {
        return Ok(());
    };

    let configmap_name = source.name.clone().unwrap_or_else(|| volume.name.clone());
    let optional = source.optional.unwrap_or(false);
    let mount_path = configmap_volume_path(container_name, &volume.name);
    reset_volume_dir(&mount_path)?;

    match load_config_map_cached(namespace, &configmap_name, cache)? {
        Some(ref config_map) => {
            let default_mode = resolve_mode(source.default_mode, 0o644);
            let files = gather_configmap_files(config_map, &source.items, default_mode, optional)?;
            write_materialized_files(&mount_path, files)?;
        }
        None if optional => {}
        None => {
            return Err(new_error(format!(
                "ConfigMap '{}' not found in namespace '{}'",
                configmap_name, namespace
            )));
        }
    }

    volume.host_path = Some(HostPathVolumeSource {
        path: mount_path.to_string_lossy().into_owned(),
        r#type: None,
    });
    Ok(())
}

fn materialize_secret_volume(
    namespace: &str,
    container_name: &str,
    volume: &mut VolumeSpec,
    cache: &mut HashMap<String, Option<StoredSecret>>,
    store: &KeyspaceSecretStore,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let Some(secret_source) = volume.secret.as_ref() else {
        return Ok(());
    };

    let secret_name = secret_source.secret_name.trim();
    if secret_name.is_empty() {
        return Err(new_error(format!(
            "Secret volume '{}' must specify secretName",
            volume.name
        )));
    }
    let secret_name = secret_name.to_string();
    let optional = secret_source.optional.unwrap_or(false);
    let mount_path = secret_volume_path(container_name, &volume.name);
    reset_volume_dir(&mount_path)?;

    let secret_record = match load_secret_cached(namespace, &secret_name, cache, store)? {
        Some(record) => record,
        None if optional => {
            if let Err(err) = fs::remove_dir_all(&mount_path) {
                if err.kind() != ErrorKind::NotFound {
                    let path_display = mount_path.display().to_string();
                    let error_text = err.to_string();
                    let warn_fields = [
                        ("path", path_display.as_str()),
                        ("error", error_text.as_str()),
                    ];
                    log_warn(
                        "container",
                        "Failed to clean optional secret volume directory",
                        &warn_fields,
                    );
                }
            }
            return Ok(());
        }
        None => {
            return Err(new_error(format!(
                "Secret '{}' not found in namespace '{}'",
                secret_name, namespace
            )));
        }
    };

    let files = gather_secret_files(
        &secret_name,
        &secret_record.secret.data,
        &secret_source.items,
        optional,
    )?;
    write_materialized_files(&mount_path, files)?;

    volume.host_path = Some(HostPathVolumeSource {
        path: mount_path.to_string_lossy().into_owned(),
        r#type: None,
    });
    volume.secret = None;
    Ok(())
}

fn gather_secret_files(
    secret_name: &str,
    data: &BTreeMap<String, String>,
    items: &HashMap<String, String>,
    optional: bool,
) -> MaterializedResult<MaterializedFiles> {
    let mut files: MaterializedFiles = Vec::new();
    if items.is_empty() {
        for (key, value) in data {
            let relative = sanitize_relative_path(key)?;
            files.push((relative, value.clone().into_bytes(), 0o440));
        }
    } else {
        for (item_key, path) in items {
            let Some(value) = data.get(item_key) else {
                if optional {
                    continue;
                }
                return Err(new_error(format!(
                    "Secret '{}' missing key '{}'",
                    secret_name, item_key
                )));
            };
            let relative = sanitize_relative_path(path)?;
            files.push((relative, value.clone().into_bytes(), 0o440));
        }
    }
    Ok(files)
}

fn materialize_projected_volume(
    namespace: &str,
    container_name: &str,
    volume: &mut VolumeSpec,
    cache: &mut HashMap<String, Option<ConfigMap>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let Some(projected) = volume.projected.as_ref() else {
        return Ok(());
    };

    let mount_path = configmap_volume_path(container_name, &volume.name);
    reset_volume_dir(&mount_path)?;
    let default_mode = resolve_mode(projected.default_mode, 0o644);

    for source in &projected.sources {
        if let Some(config_projection) = source.config_map.as_ref() {
            let configmap_name = config_projection
                .name
                .clone()
                .unwrap_or_else(|| volume.name.clone());
            let optional = config_projection.optional.unwrap_or(false);

            match load_config_map_cached(namespace, &configmap_name, cache)? {
                Some(ref config_map) => {
                    let files = gather_configmap_files(
                        config_map,
                        &config_projection.items,
                        default_mode,
                        optional,
                    )?;
                    if !files.is_empty() {
                        write_materialized_files(&mount_path, files)?;
                    }
                }
                None if optional => continue,
                None => {
                    return Err(new_error(format!(
                        "ConfigMap '{}' not found in namespace '{}'",
                        configmap_name, namespace
                    )));
                }
            }
        }
    }

    volume.host_path = Some(HostPathVolumeSource {
        path: mount_path.to_string_lossy().into_owned(),
        r#type: None,
    });
    Ok(())
}

fn cleanup_configmap_mounts(container_name: &str) {
    let roots = [
        materialized_volume_root("NANOCLOUD_CONFIGMAP_VOLUME_ROOT", CONFIGMAP_VOLUME_ROOT),
        materialized_volume_root("NANOCLOUD_SECRET_VOLUME_ROOT", SECRET_VOLUME_ROOT),
    ];
    for root in roots {
        let path = root.join(container_name);
        if let Err(err) = fs::remove_dir_all(&path) {
            if err.kind() != ErrorKind::NotFound {
                let path_display = path.display().to_string();
                let error_text = err.to_string();
                let warn_fields = [
                    ("path", path_display.as_str()),
                    ("error", error_text.as_str()),
                ];
                log_warn(
                    "container",
                    "Failed to remove materialized volume directory",
                    &warn_fields,
                );
            }
        }
    }
}

fn apply_env_from(
    namespace: &str,
    container: &mut ContainerSpec,
    config_map_cache: &mut HashMap<String, Option<ConfigMap>>,
    secret_cache: &mut HashMap<String, Option<StoredSecret>>,
    secret_store: &KeyspaceSecretStore,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if container.env_from.is_empty() {
        return Ok(());
    }

    let mut existing: HashSet<String> = container.env.iter().map(|env| env.name.clone()).collect();
    let mut injected: Vec<ContainerEnvVar> = Vec::new();

    for source in &container.env_from {
        if let Some(config_map_ref) = source.config_map_ref.as_ref() {
            match load_config_map_cached(namespace, &config_map_ref.name, config_map_cache)? {
                Some(config_map) => {
                    for (key, value) in config_map.entries() {
                        if existing.contains(&key) {
                            continue;
                        }
                        let value_str = String::from_utf8(value).map_err(|_| {
                            new_error(format!(
                                "ConfigMap '{}' key '{}' contains non-UTF8 data",
                                config_map_ref.name, key
                            ))
                        })?;
                        existing.insert(key.clone());
                        injected.push(ContainerEnvVar {
                            name: key,
                            value: Some(value_str),
                            value_from: None,
                        });
                    }
                }
                None if config_map_ref.optional.unwrap_or(false) => continue,
                None => {
                    return Err(new_error(format!(
                        "ConfigMap '{}' not found in namespace '{}'",
                        config_map_ref.name, namespace
                    )));
                }
            }
        }
        if let Some(secret_ref) = source.secret_ref.as_ref() {
            match load_secret_cached(namespace, &secret_ref.name, secret_cache, secret_store)? {
                Some(secret) => {
                    for (key, value) in secret.secret.data.iter() {
                        if existing.contains(key) {
                            continue;
                        }
                        existing.insert(key.clone());
                        injected.push(ContainerEnvVar {
                            name: key.clone(),
                            value: Some(value.clone()),
                            value_from: None,
                        });
                    }
                }
                None if secret_ref.optional.unwrap_or(false) => continue,
                None => {
                    return Err(new_error(format!(
                        "Secret '{}' not found in namespace '{}'",
                        secret_ref.name, namespace
                    )));
                }
            }
        }
    }

    container.env.extend(injected);
    Ok(())
}

fn resolve_env_value_from(
    namespace: &str,
    container: &mut ContainerSpec,
    config_map_cache: &mut HashMap<String, Option<ConfigMap>>,
    secret_cache: &mut HashMap<String, Option<StoredSecret>>,
    secret_store: &KeyspaceSecretStore,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if container.env.is_empty() {
        return Ok(());
    }

    let mut to_remove: Vec<usize> = Vec::new();
    for (index, env_var) in container.env.iter_mut().enumerate() {
        let Some(source) = env_var.value_from.as_ref() else {
            continue;
        };
        if source.config_map_key_ref.is_some() && source.secret_key_ref.is_some() {
            return Err(new_error(format!(
                "Env var '{}' cannot specify both configMapKeyRef and secretKeyRef",
                env_var.name
            )));
        }
        if let Some(key_ref) = source.config_map_key_ref.as_ref() {
            let name = match key_ref.name.as_ref() {
                Some(value) if !value.is_empty() => value.clone(),
                _ if key_ref.optional.unwrap_or(false) => {
                    to_remove.push(index);
                    continue;
                }
                _ => {
                    return Err(new_error(format!(
                        "configMapKeyRef for '{}' must specify a ConfigMap name",
                        env_var.name
                    )));
                }
            };

            let optional = key_ref.optional.unwrap_or(false);
            let config_map = load_config_map_cached(namespace, &name, config_map_cache)?;
            let Some(config_map) = config_map else {
                if optional {
                    to_remove.push(index);
                    continue;
                } else {
                    return Err(new_error(format!(
                        "ConfigMap '{}' not found in namespace '{}'",
                        name, namespace
                    )));
                }
            };

            match config_map.get_key_bytes(&key_ref.key) {
                Some(bytes) => {
                    let value_str = String::from_utf8(bytes).map_err(|_| {
                        new_error(format!(
                            "ConfigMap '{}' key '{}' contains non-UTF8 data",
                            name, key_ref.key
                        ))
                    })?;
                    env_var.value = Some(value_str);
                    env_var.value_from = None;
                }
                None if optional => {
                    to_remove.push(index);
                }
                None => {
                    return Err(new_error(format!(
                        "ConfigMap '{}' missing key '{}' for env '{}'",
                        name, key_ref.key, env_var.name
                    )));
                }
            }
            continue;
        }

        let Some(secret_key_ref) = source.secret_key_ref.as_ref() else {
            continue;
        };

        let name = match secret_key_ref.name.as_ref() {
            Some(value) if !value.is_empty() => value.clone(),
            _ if secret_key_ref.optional.unwrap_or(false) => {
                to_remove.push(index);
                continue;
            }
            _ => {
                return Err(new_error(format!(
                    "secretKeyRef for '{}' must specify a Secret name",
                    env_var.name
                )));
            }
        };

        let optional = secret_key_ref.optional.unwrap_or(false);
        let secret = load_secret_cached(namespace, &name, secret_cache, secret_store)?;
        let Some(secret) = secret else {
            if optional {
                to_remove.push(index);
                continue;
            } else {
                return Err(new_error(format!(
                    "Secret '{}' not found in namespace '{}'",
                    name, namespace
                )));
            }
        };

        match secret.secret.data.get(&secret_key_ref.key) {
            Some(value) => {
                env_var.value = Some(value.clone());
                env_var.value_from = None;
            }
            None if optional => {
                to_remove.push(index);
            }
            None => {
                return Err(new_error(format!(
                    "Secret '{}' missing key '{}' for env '{}'",
                    name, secret_key_ref.key, env_var.name
                )));
            }
        }
    }

    if !to_remove.is_empty() {
        to_remove.sort_unstable();
        to_remove.dedup();
        for index in to_remove.into_iter().rev() {
            container.env.remove(index);
        }
    }

    Ok(())
}

pub async fn uninstall(
    namespace: Option<&str>,
    app: &str,
    plan: BackupPlan,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    metrics::observe_container_operation(
        namespace,
        app,
        ContainerOperation::Uninstall,
        uninstall_impl(namespace, app, plan),
    )
    .await
}

async fn uninstall_impl(
    namespace: Option<&str>,
    app: &str,
    plan: BackupPlan,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let container_name = namespace
        .map(|ns| format!("{}-{}", ns, app))
        .unwrap_or_else(|| app.to_string());
    let namespace_value = namespace.unwrap_or("default");
    let mut uninstall_fields = vec![
        ("app", app),
        ("container", container_name.as_str()),
        ("namespace", namespace_value),
    ];
    uninstall_fields.push(("owner", plan.owner.as_str()));
    let retention_value = plan.retention().to_string();
    uninstall_fields.push(("retention", retention_value.as_str()));
    log_info(
        "container",
        "Starting container uninstall",
        &uninstall_fields,
    );

    let pod_manifest = pod_store::load_pod_manifest(namespace, app)
        .map_err(|e| with_context(e, format!("Failed to load pod spec for {container_name}")))?;
    let host_network = pod_manifest
        .as_ref()
        .map(|workload| workload.spec.host_network)
        .unwrap_or(false);

    // Get id for container
    let container_id = get_container_id_by_name(&container_name)
        .ok_or_else(|| new_error(format!("Service '{}' not found", container_name)))?;

    let csi = csi_plugin();
    let managed_volumes = csi
        .list_service_volumes(namespace_value, app)
        .map_err(|e| {
            with_context(
                e,
                format!("Failed to enumerate volumes for '{container_name}'"),
            )
        })?;

    // Capture a service snapshot before tearing down resources
    let service_dir = backup_directory(&plan.owner, namespace, app);
    fs::create_dir_all(&service_dir).map_err(|err| {
        with_context(
            err,
            format!(
                "Failed to prepare service backup directory '{}'",
                service_dir.display()
            ),
        )
    })?;
    let timestamp = Local::now().format("%Y%m%d-%H%M%S");
    let file_name = namespace.map_or_else(
        || format!("{}-{}.tar", app, timestamp),
        |ns| format!("{}-{}-{}.tar", ns, app, timestamp),
    );
    let snapshot_path = service_dir.join(file_name);
    let snapshot_file_path = snapshot_path
        .to_str()
        .ok_or_else(|| new_error("Backup path contains invalid UTF-8"))?
        .to_string();
    let capture_start = Instant::now();
    let summary = Snapshot::save(namespace, app, None, &snapshot_file_path).await?;
    let capture_duration = capture_start.elapsed();
    let total_bytes: u64 = summary.entries.iter().map(|entry| entry.size_bytes).sum();
    let volumes = summary.entries.len();
    metrics::record_backup_capture(namespace, app, volumes, total_bytes, capture_duration);
    let duration_text = format!("{:.3}", capture_duration.as_secs_f64());
    let bytes_text = total_bytes.to_string();
    let volumes_text = volumes.to_string();
    let throughput = if capture_duration.as_secs_f64() > 0.0 && total_bytes > 0 {
        (total_bytes as f64 / (1 << 20) as f64) / capture_duration.as_secs_f64()
    } else {
        0.0
    };
    let throughput_text = format!("{throughput:.2}");
    log_info(
        "container",
        "Captured service snapshot",
        &[
            ("namespace", namespace_value),
            ("service", app),
            ("volumes", volumes_text.as_str()),
            ("bytes", bytes_text.as_str()),
            ("duration_seconds", duration_text.as_str()),
            ("throughput_mib_s", throughput_text.as_str()),
        ],
    );
    if streaming_backup_enabled() {
        let label = format!("{}/{}", namespace_value, app);
        register_streaming_backup(label, &snapshot_path);
    }
    prune_backups(&service_dir, plan.retention())?;

    // Remove the container
    remove_container(&container_name, &container_id, host_network)?;
    cleanup_configmap_mounts(&container_name);
    Kubelet::shared()
        .forget_pod(namespace, app)
        .await
        .map_err(|e| with_context(e, "Failed to deregister workload"))?;

    // Unpublish and remove managed volumes
    for volume in managed_volumes {
        for target in volume.publications.clone() {
            csi.node_unpublish_volume(NodeUnpublishVolumeRequest {
                volume_id: volume.volume.volume_id.clone(),
                target_path: target.clone(),
            })
            .await
            .map_err(|e| {
                with_context(
                    e,
                    format!(
                        "Failed to unpublish volume '{}' for '{}'",
                        volume.volume.volume_id, container_name
                    ),
                )
            })?;
        }

        csi.delete_volume(DeleteVolumeRequest {
            volume_id: volume.volume.volume_id.clone(),
        })
        .await
        .map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to delete volume '{}' for '{}'",
                    volume.volume.volume_id, container_name
                ),
            )
        })?;
    }
    pod_store::delete_pod_manifest(namespace, app)
        .map_err(|e| with_context(e, format!("Failed to delete pod spec for {container_name}")))?;

    metrics::clear_container(namespace, app);

    // Remove any persisted kubelet desired state so future installs start cleanly.
    Kubelet::shared().clear_desired_state(namespace, app).await;

    Ok(())
}

pub async fn start(namespace: Option<&str>, app: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    metrics::observe_container_operation(
        namespace,
        app,
        ContainerOperation::Start,
        start_impl(namespace, app),
    )
    .await
}

async fn start_impl(
    namespace: Option<&str>,
    app: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let container_name = namespace
        .map(|ns| format!("{}-{}", ns, app))
        .unwrap_or_else(|| app.to_string());

    let container_id = resolve_container_id(namespace, app)?;

    let kubelet = Kubelet::shared();
    kubelet.set_desired_running(namespace, app, true).await;

    start_container(namespace, app, &container_name, &container_id).await?;
    metrics::set_container_ready(namespace, app, true);
    Ok(())
}

pub async fn stop(namespace: Option<&str>, app: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    metrics::observe_container_operation(
        namespace,
        app,
        ContainerOperation::Stop,
        stop_impl(namespace, app),
    )
    .await
}

async fn stop_impl(namespace: Option<&str>, app: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let container_name = namespace
        .map(|ns| format!("{}-{}", ns, app))
        .unwrap_or_else(|| app.to_string());
    let namespace_value = namespace.unwrap_or("default");
    let stop_fields = [
        ("app", app),
        ("container", container_name.as_str()),
        ("namespace", namespace_value),
    ];
    log_info("container", "Stopping container", &stop_fields);

    let kubelet = Kubelet::shared();
    kubelet.set_desired_running(namespace, app, false).await;

    let container_id = get_container_id_by_name(&container_name)
        .ok_or_else(|| new_error(format!("Service '{}' not found", container_name)))?;

    kill(&container_id).await?;
    metrics::set_container_ready(namespace, app, false);
    Ok(())
}

fn split_first_rest(values: &[String]) -> (String, Vec<String>) {
    if values.is_empty() {
        (String::new(), Vec::new())
    } else {
        (values[0].clone(), values[1..].to_vec())
    }
}

// Remove the specified container

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::engine::get_streaming_backup;
    use crate::nanocloud::k8s::configmap::ConfigMap;
    use crate::nanocloud::k8s::pod::{
        ConfigMapEnvSource, ConfigMapKeySelector, ContainerEnvVar, ContainerSpec, EnvFromSource,
        EnvVarSource, KeyToPath, ObjectMeta, SecretEnvSource, SecretKeySelector,
        SecretVolumeSource,
    };
    use crate::nanocloud::secrets::{KeyspaceSecretStore, SecretMaterial, StoredSecret};
    use chrono::Utc;
    use std::collections::{BTreeMap, HashMap};
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::tempdir;

    fn make_config_map(name: &str, data: &[(&str, &str)]) -> ConfigMap {
        let mut config = ConfigMap::new(ObjectMeta {
            name: Some(name.to_string()),
            ..Default::default()
        });
        for (key, value) in data {
            config.data.insert((*key).to_string(), (*value).to_string());
        }
        config
    }

    fn make_secret(name: &str, data: &[(&str, &str)]) -> StoredSecret {
        let mut payload = BTreeMap::new();
        for (key, value) in data {
            payload.insert((*key).to_string(), (*value).to_string());
        }
        StoredSecret {
            secret: SecretMaterial {
                namespace: "default".to_string(),
                name: name.to_string(),
                type_name: "Opaque".to_string(),
                immutable: false,
                data: payload,
                resource_version: None,
            },
            digest: "digest".to_string(),
            created_at: Utc::now(),
        }
    }

    struct EnvOverride {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvOverride {
        fn set(key: &'static str, value: impl AsRef<str>) -> Self {
            let previous = env::var(key).ok();
            env::set_var(key, value.as_ref());
            Self { key, previous }
        }
    }

    impl Drop for EnvOverride {
        fn drop(&mut self) {
            if let Some(prev) = self.previous.as_ref() {
                env::set_var(self.key, prev);
            } else {
                env::remove_var(self.key);
            }
        }
    }

    #[test]
    fn resolve_mode_clamps_and_masks_values() {
        assert_eq!(resolve_mode(Some(0o755), 0o644), 0o755);
        assert_eq!(resolve_mode(Some(-1), 0o644), 0);
        assert_eq!(resolve_mode(Some(0o1777), 0), 0o777);
        assert_eq!(resolve_mode(None, 0o700), 0o700);
    }

    #[test]
    fn sanitize_relative_path_rejects_absolute_and_parent_paths() {
        assert!(sanitize_relative_path("/etc/config").is_err());
        assert!(sanitize_relative_path("..//secret").is_err());
        assert_eq!(
            sanitize_relative_path("valid/path").unwrap(),
            PathBuf::from("valid/path")
        );
    }

    #[test]
    fn gather_configmap_files_handles_full_and_filtered_items() {
        let config = make_config_map("app-config", &[("first", "alpha"), ("second", "beta")]);
        let files = gather_configmap_files(&config, &[], 0o640, false).unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].0, PathBuf::from("first"));
        assert_eq!(files[0].1, b"alpha".to_vec());
        assert_eq!(files[0].2, 0o640);

        let items = vec![KeyToPath {
            key: "second".into(),
            path: "nested/file.txt".into(),
            mode: Some(0o600),
        }];
        let filtered = gather_configmap_files(&config, &items, 0o644, false).unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].0, PathBuf::from("nested/file.txt"));
        assert_eq!(filtered[0].1, b"beta".to_vec());
        assert_eq!(filtered[0].2, 0o600);

        let missing = gather_configmap_files(
            &config,
            &[KeyToPath {
                key: "missing".into(),
                path: "skip".into(),
                mode: None,
            }],
            0o644,
            true,
        )
        .unwrap();
        assert!(missing.is_empty());

        let error = gather_configmap_files(
            &config,
            &[KeyToPath {
                key: "missing".into(),
                path: "skip".into(),
                mode: None,
            }],
            0o644,
            false,
        )
        .expect_err("missing key should error");
        assert!(error.to_string().contains("missing key"));
    }

    #[test]
    fn write_materialized_files_creates_tree_with_permissions() {
        let temp = tempdir().expect("tempdir");
        let files = vec![
            (
                PathBuf::from("subdir/config.txt"),
                b"content".to_vec(),
                0o640,
            ),
            (PathBuf::from("root.txt"), b"root".to_vec(), 0o600),
        ];
        write_materialized_files(temp.path(), files).expect("write files");

        let root_file = temp.path().join("root.txt");
        assert_eq!(fs::read(&root_file).unwrap(), b"root");
        let root_mode = fs::metadata(&root_file).unwrap().permissions().mode() & 0o777;
        assert_eq!(root_mode, 0o600);

        let nested = temp.path().join("subdir/config.txt");
        assert_eq!(fs::read(&nested).unwrap(), b"content");
        let nested_mode = fs::metadata(&nested).unwrap().permissions().mode() & 0o777;
        assert_eq!(nested_mode, 0o640);
    }

    #[test]
    fn prune_backups_removes_stream_registry_entries() {
        let _guard = EnvOverride::set("NANOCLOUD_STREAMING_BACKUP", "1");
        let temp = tempdir().expect("tempdir");
        let dir = temp.path();

        let files = ["a.tar", "b.tar", "c.tar"];
        let mut paths = Vec::new();
        for (index, name) in files.iter().enumerate() {
            let path = dir.join(name);
            fs::write(&path, format!("payload-{index}")).expect("write backup file");
            register_streaming_backup(format!("svc-{name}"), &path);
            paths.push(path);
        }

        prune_backups(dir, 2).expect("prune succeeds");

        assert!(!paths[0].exists());
        assert!(paths[1].exists());
        assert!(paths[2].exists());

        assert!(get_streaming_backup(&paths[0]).is_none());
        assert!(get_streaming_backup(&paths[1]).is_some());
        assert!(get_streaming_backup(&paths[2]).is_some());

        remove_streaming_backup(&paths[1]);
        remove_streaming_backup(&paths[2]);
    }

    #[test]
    fn apply_env_from_injects_missing_variables() {
        let mut container = ContainerSpec {
            env: vec![ContainerEnvVar {
                name: "EXISTING".into(),
                value: Some("preserve".into()),
                value_from: None,
            }],
            env_from: vec![EnvFromSource {
                config_map_ref: Some(ConfigMapEnvSource {
                    name: "app-config".into(),
                    optional: Some(false),
                }),
                secret_ref: None,
            }],
            ..Default::default()
        };

        let config = make_config_map(
            "app-config",
            &[("NEW_VAR", "value"), ("EXISTING", "override")],
        );
        let mut config_cache = HashMap::new();
        config_cache.insert("app-config".to_string(), Some(config));
        let mut secret_cache = HashMap::new();
        let secret_store = KeyspaceSecretStore::new();

        apply_env_from(
            "default",
            &mut container,
            &mut config_cache,
            &mut secret_cache,
            &secret_store,
        )
        .expect("apply env");

        let mut values = container
            .env
            .iter()
            .map(|var| (var.name.clone(), var.value.clone()))
            .collect::<HashMap<_, _>>();

        assert_eq!(values.remove("EXISTING"), Some(Some("preserve".into())));
        assert_eq!(values.remove("NEW_VAR"), Some(Some("value".into())));
    }

    #[test]
    fn apply_env_from_injects_secret_variables() {
        let mut container = ContainerSpec {
            env: vec![],
            env_from: vec![EnvFromSource {
                config_map_ref: None,
                secret_ref: Some(SecretEnvSource {
                    name: "db-creds".into(),
                    optional: Some(false),
                }),
            }],
            ..Default::default()
        };

        let secret = make_secret("db-creds", &[("USERNAME", "admin"), ("PASSWORD", "s3cr3t")]);
        let mut config_cache = HashMap::new();
        let mut secret_cache = HashMap::new();
        secret_cache.insert("default/db-creds".to_string(), Some(secret));
        let secret_store = KeyspaceSecretStore::new();

        apply_env_from(
            "default",
            &mut container,
            &mut config_cache,
            &mut secret_cache,
            &secret_store,
        )
        .expect("apply env");

        let mut values = container
            .env
            .iter()
            .map(|var| (var.name.clone(), var.value.clone()))
            .collect::<HashMap<_, _>>();

        assert_eq!(values.remove("USERNAME"), Some(Some("admin".into())));
        assert_eq!(values.remove("PASSWORD"), Some(Some("s3cr3t".into())));
        assert!(values.is_empty());
    }

    #[test]
    fn resolve_env_value_from_populates_and_prunes_entries() {
        let mut container = ContainerSpec {
            env: vec![
                ContainerEnvVar {
                    name: "CONFIG_VALUE".into(),
                    value: None,
                    value_from: Some(EnvVarSource {
                        config_map_key_ref: Some(ConfigMapKeySelector {
                            key: "setting".into(),
                            name: Some("app-config".into()),
                            optional: Some(false),
                        }),
                        secret_key_ref: None,
                    }),
                },
                ContainerEnvVar {
                    name: "OPTIONAL".into(),
                    value: None,
                    value_from: Some(EnvVarSource {
                        config_map_key_ref: Some(ConfigMapKeySelector {
                            key: "missing".into(),
                            name: Some("app-config".into()),
                            optional: Some(true),
                        }),
                        secret_key_ref: None,
                    }),
                },
            ],
            ..Default::default()
        };

        let config = make_config_map("app-config", &[("setting", "enabled")]);
        let mut config_cache = HashMap::new();
        config_cache.insert("app-config".to_string(), Some(config));
        let mut secret_cache = HashMap::new();
        let secret_store = KeyspaceSecretStore::new();

        resolve_env_value_from(
            "default",
            &mut container,
            &mut config_cache,
            &mut secret_cache,
            &secret_store,
        )
        .expect("resolve env");

        assert_eq!(container.env.len(), 1);
        assert_eq!(container.env[0].name, "CONFIG_VALUE");
        assert_eq!(container.env[0].value.as_deref(), Some("enabled"));
        assert!(container.env[0].value_from.is_none());
    }

    #[test]
    fn resolve_env_value_from_populates_secret_entries() {
        let mut container = ContainerSpec {
            env: vec![ContainerEnvVar {
                name: "DB_PASSWORD".into(),
                value: None,
                value_from: Some(EnvVarSource {
                    config_map_key_ref: None,
                    secret_key_ref: Some(SecretKeySelector {
                        key: "password".into(),
                        name: Some("db-creds".into()),
                        optional: Some(false),
                    }),
                }),
            }],
            ..Default::default()
        };

        let secret = make_secret("db-creds", &[("password", "hunter2")]);
        let mut config_cache = HashMap::new();
        let mut secret_cache = HashMap::new();
        secret_cache.insert("default/db-creds".to_string(), Some(secret));
        let secret_store = KeyspaceSecretStore::new();

        resolve_env_value_from(
            "default",
            &mut container,
            &mut config_cache,
            &mut secret_cache,
            &secret_store,
        )
        .expect("resolve env");

        assert_eq!(container.env.len(), 1);
        assert_eq!(container.env[0].name, "DB_PASSWORD");
        assert_eq!(container.env[0].value.as_deref(), Some("hunter2"));
        assert!(container.env[0].value_from.is_none());
    }

    #[test]
    fn materialize_secret_volume_writes_files() {
        let temp = tempdir().expect("tempdir");
        let secret_root = temp.path().join("secret-root");
        fs::create_dir_all(&secret_root).expect("create secret root");
        let _guard = EnvOverride::set(
            "NANOCLOUD_SECRET_VOLUME_ROOT",
            secret_root.to_string_lossy(),
        );

        let mut volume = VolumeSpec {
            name: "app-secrets".into(),
            secret: Some(SecretVolumeSource {
                secret_name: "db-login".into(),
                items: HashMap::from([("password".into(), "creds/pass.txt".into())]),
                optional: None,
            }),
            ..Default::default()
        };

        let mut cache = HashMap::new();
        cache.insert(
            "default/db-login".to_string(),
            Some(make_secret("db-login", &[("password", "hunter2")])),
        );
        let store = KeyspaceSecretStore::new();

        materialize_secret_volume("default", "svc", &mut volume, &mut cache, &store)
            .expect("materialize secret volume");

        let host_path = volume
            .host_path
            .as_ref()
            .expect("host path set")
            .path
            .clone();
        let file_path = PathBuf::from(&host_path).join("creds/pass.txt");
        let content = fs::read_to_string(&file_path).expect("read secret file");
        assert_eq!(content, "hunter2");
        assert!(volume.secret.is_none());
    }

    #[test]
    fn materialize_secret_volume_optional_missing_secret() {
        let temp = tempdir().expect("tempdir");
        let secret_root = temp.path().join("secret-root");
        fs::create_dir_all(&secret_root).expect("create secret root");
        let _guard = EnvOverride::set(
            "NANOCLOUD_SECRET_VOLUME_ROOT",
            secret_root.to_string_lossy(),
        );

        let mut volume = VolumeSpec {
            name: "app-secrets".into(),
            secret: Some(SecretVolumeSource {
                secret_name: "missing".into(),
                items: HashMap::new(),
                optional: Some(true),
            }),
            ..Default::default()
        };

        let mut cache = HashMap::new();
        let store = KeyspaceSecretStore::new();

        materialize_secret_volume("default", "svc", &mut volume, &mut cache, &store)
            .expect("optional secret volume should not error");

        assert!(volume.host_path.is_none());
    }

    #[test]
    fn split_first_rest_handles_empty_and_nonempty_inputs() {
        let values = vec!["cmd".into(), "--flag".into(), "value".into()];
        let (first, rest) = split_first_rest(&values);
        assert_eq!(first, "cmd");
        assert_eq!(rest, vec!["--flag".to_string(), "value".to_string()]);

        let empty: Vec<String> = Vec::new();
        let (first, rest) = split_first_rest(&empty);
        assert_eq!(first, "");
        assert!(rest.is_empty());
    }

    #[test]
    fn configmap_volume_path_builds_expected_directory() {
        let path = configmap_volume_path("service", "vol");
        assert_eq!(
            path,
            Path::new("/var/lib/nanocloud.io/storage/configmap")
                .join("service")
                .join("vol")
        );
    }
}
