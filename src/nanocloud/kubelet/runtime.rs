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

use crate::nanocloud::cni::{cni_plugin, CniResult};
use crate::nanocloud::engine::log::container_log_dir;
use crate::nanocloud::engine::Profile;
use crate::nanocloud::k8s::pod::{ContainerProbe, PodSpec, ProbeExec};
use crate::nanocloud::k8s::store as pod_store;
use crate::nanocloud::oci::distribution::{load_manifest_from_store, parse_image_reference};
use crate::nanocloud::oci::runtime::{
    container_refs_dir, container_root_path, netns_dir, ContainerStatus, ExecRequest,
};
use crate::nanocloud::oci::{container_runtime, OciImage, OciManifest};
use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::util::security::{EncryptionKey, TlsInfo};

use crate::nanocloud::logger::{log_error, log_info, log_warn};
use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::error::Error;
use std::ffi::{CStr, CString};
use std::io::{self, BufReader, ErrorKind, Write};
use std::net::Ipv4Addr;
use std::os::fd::IntoRawFd;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::ptr;
use std::str;
use std::time::Duration;
use tokio::time::{sleep, Instant};

use nix::fcntl::{open, OFlag};
use nix::sys::stat::Mode;

#[derive(Deserialize, Serialize)]
pub struct Container {
    pub id: String,
    pub name: String,
    pub cni_config: Option<CniResult>,
    pub oci_image: OciImage,
    pub image_manifest: OciManifest,
}

pub async fn create_container(
    container_name: &str,
    oci_image: &OciImage,
    oci_manifest: &OciManifest,
    pod_spec: &PodSpec,
) -> Result<Container, Box<dyn Error + Send + Sync>> {
    // Create and start main container.
    log_info(
        "kubelet",
        "Creating OCI container",
        &[("container", container_name)],
    );

    let container_id = String::from_utf8(EncryptionKey::gen_random_bytes(32, "hex").to_vec())
        .map_err(|_| new_error("Failed to convert random bytes to String"))?;

    let cni_result = if pod_spec.host_network {
        log_info(
            "kubelet",
            "Skipping CNI provisioning for host-networked container",
            &[("container", container_name)],
        );
        None
    } else {
        Some(provision_container_network(
            &container_id,
            container_name,
            pod_spec,
        )?)
    };

    log_info(
        "kubelet",
        "Preparing container root filesystem",
        &[("container", container_name)],
    );
    create_rootfs(&container_id, container_name, oci_manifest)?;

    // Create OCI container
    let bundle_path = container_root_path(&container_id);
    let oci_args: HashMap<String, String> = HashMap::from([(
        "OCI_BUNDLE".to_string(),
        bundle_path.to_string_lossy().to_string(),
    )]);
    let main_container = pod_spec.containers.first().ok_or_else(|| {
        new_error(format!(
            "Pod spec for {} does not define a container",
            container_name
        ))
    })?;

    let runtime = container_runtime();
    let oci_config = runtime.configure_from_spec(
        &container_id,
        container_name,
        main_container,
        &pod_spec.volumes,
        pod_spec.host_network,
    );
    let config_path = bundle_path.join("config.json");
    let config_dir = config_path.parent().ok_or_else(|| {
        new_error(format!(
            "OCI configuration path {} has no parent directory",
            config_path.display()
        ))
    })?;
    std::fs::create_dir_all(config_dir).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to create OCI config directory {}",
                config_dir.display()
            ),
        )
    })?;
    let config_json = serde_json::to_string(&oci_config)
        .map_err(|e| with_context(e, "Failed to serialize OCI runtime config"))?;
    std::fs::File::create(&config_path)
        .map_err(|e| {
            with_context(
                e,
                format!("Failed to create OCI config file {}", config_path.display()),
            )
        })?
        .write_all(config_json.as_bytes())
        .map_err(|e| {
            with_context(
                e,
                format!("Failed to write OCI config file {}", config_path.display()),
            )
        })?;
    runtime
        .create(&container_id, &oci_args, config_json.clone().into_bytes())
        .map_err(|e| {
            with_context(
                e,
                format!("Failed to create OCI container {}", container_id),
            )
        })?;
    log_info(
        "kubelet",
        "OCI container created",
        &[("container", container_name), ("id", container_id.as_str())],
    );

    std::fs::File::create(bundle_path.join("image_digest"))
        .map_err(|e| with_context(e, "Failed to create image digest file"))?
        .write_all(oci_manifest.config.digest.as_bytes())
        .map_err(|e| with_context(e, "Failed to write image digest"))?;

    let container = Container {
        id: container_id.clone(),
        name: container_name.to_string(),
        cni_config: cni_result.clone(),
        oci_image: oci_image.clone(),
        image_manifest: oci_manifest.clone(),
    };

    Ok(container)
}

fn provision_container_network(
    container_id: &str,
    container_name: &str,
    pod_spec: &PodSpec,
) -> Result<CniResult, Box<dyn Error + Send + Sync>> {
    let netns_path = netns_dir().join(container_id);
    let mut cni_args: HashMap<String, String> = HashMap::new();
    cni_args.insert("CNI_COMMAND".to_string(), "ADD".to_string());
    cni_args.insert("CNI_CONTAINERID".to_string(), container_id.to_string());
    cni_args.insert(
        "CNI_NETNS".to_string(),
        netns_path.to_string_lossy().to_string(),
    );
    cni_args.insert("CNI_IFNAME".to_string(), "nanocloud0".to_string());
    cni_args.insert("CNI_PATH".to_string(), "/opt/cni/bin".to_string());

    let port_mappings: Vec<_> = pod_spec
        .containers
        .iter()
        .flat_map(|container| container.ports.iter())
        .filter_map(|port| {
            port.host_port.map(|host_port| {
                let mut mapping = json!({
                    "hostPort": host_port,
                    "containerPort": port.container_port,
                });
                if let Some(protocol) = port.protocol.as_deref() {
                    if !protocol.is_empty() {
                        mapping["protocol"] = json!(protocol.to_lowercase());
                    }
                }
                if let Some(host_ip) = port.host_ip.as_deref() {
                    if !host_ip.trim().is_empty() {
                        mapping["hostIP"] = json!(host_ip);
                    }
                }
                mapping
            })
        })
        .collect();

    let mut cni_config = json!({
        "cniVersion": "1.0.0",
        "name": "nanocloud",
        "type": "bridge",
        "bridge": "nanocloud0",
    });
    if !port_mappings.is_empty() {
        cni_config["runtimeConfig"] = json!({ "portMappings": port_mappings });
    }
    let cni_config = serde_json::to_string(&cni_config)?;
    let plugin = cni_plugin();
    let result = plugin.add(&cni_args, cni_config.clone().into_bytes())?;
    let cni_json = serde_json::to_string(&result)
        .map_err(|e| with_context(e, "Failed to serialize CNI result"))?;
    log_info(
        "kubelet",
        "CNI provisioning result",
        &[("container", container_name), ("result", cni_json.as_str())],
    );
    Ok(result)
}

// Remove the specified container
pub fn remove_container(
    container_name: &str,
    container_id: &str,
    host_network: bool,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // let remove_options = RemoveContainerOptions { force: true, ..Default::default() };
    if !host_network {
        // Remove network interface
        let cni_args: HashMap<String, String> =
            [("CNI_COMMAND", "DEL"), ("CNI_CONTAINERID", container_id)]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
        log_info(
            "kubelet",
            "Removing CNI network interface",
            &[("container_id", container_id)],
        );
        cni_plugin().delete(&cni_args)?;
    } else {
        log_info(
            "kubelet",
            "Skipping CNI teardown for host-networked container",
            &[("container", container_name)],
        );
    }

    log_info(
        "kubelet",
        "Removing OCI container",
        &[("container", container_name), ("id", container_id)],
    );
    container_runtime().delete(container_name, container_id)?;

    log_info(
        "kubelet",
        "OCI container removed",
        &[("container", container_name), ("id", container_id)],
    );
    Ok(())
}

pub async fn start_container(
    namespace: Option<&str>,
    app: &str,
    container_name: &str,
    container_id: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let stored_workload = pod_store::load_pod_manifest(namespace, app)
        .map_err(|e| with_context(e, format!("Failed to load pod spec for {}", container_name)))?;
    let pod_spec = stored_workload.as_ref().map(|workload| &workload.spec);
    let host_network = pod_spec.map(|spec| spec.host_network).unwrap_or(false);
    let container_spec = pod_spec.and_then(|spec| spec.containers.first()).cloned();
    let requires_profile = stored_workload
        .as_ref()
        .map(|workload| {
            workload
                .metadata
                .annotations
                .get("nanocloud.io/profile-managed")
                .map(|value| value == "true")
                .unwrap_or(false)
        })
        .unwrap_or(false);
    let lifecycle_hooks = container_spec
        .as_ref()
        .and_then(|container| container.lifecycle.clone());
    let pre_start_exec = lifecycle_hooks
        .as_ref()
        .and_then(|hooks| hooks.pre_start.clone());
    let post_start_exec = lifecycle_hooks
        .as_ref()
        .and_then(|hooks| hooks.post_start.clone());

    let runtime = container_runtime();

    let mut runtime_config: HashMap<String, Vec<u8>> = if requires_profile {
        let profile = Profile::load(namespace, app).await.map_err(|e| {
            with_context(e, format!("Failed to load profile for {}", container_name))
        })?;
        profile.config.clone()
    } else {
        HashMap::new()
    };

    if !requires_profile {
        if let Some(spec) = container_spec.as_ref() {
            for env in &spec.env {
                if let Some(value) = &env.value {
                    runtime_config
                        .entry(format!("env.{}", env.name))
                        .or_insert_with(|| value.as_bytes().to_vec());
                }
            }
        }
    }

    let tls_info = generate_runtime_tls(app)?;
    let resolv_conf_template = std::fs::read_to_string("/etc/resolv.conf").ok();

    let readiness_probe = container_spec
        .as_ref()
        .and_then(|container| container.readiness_probe.clone())
        .or_else(|| {
            if requires_profile {
                Some(ContainerProbe {
                    exec: Some(ProbeExec {
                        command: vec!["ready.sh".to_string()],
                    }),
                    initial_delay_seconds: Some(5),
                    period_seconds: Some(5),
                    ..ContainerProbe::default()
                })
            } else {
                None
            }
        });

    let mut attempt = 0;
    let creation_timeout = Duration::from_secs(30);
    let mut creation_backoff = Duration::from_millis(100);
    let wait_deadline = Instant::now() + creation_timeout;
    loop {
        attempt += 1;

        let mut state = runtime.state(container_id)?;
        if state.status == ContainerStatus::Stopped {
            log_info(
                "kubelet",
                "Container stopped; recreating init process",
                &[("container", container_name)],
            );
            ensure_rootfs_mounted(
                container_id,
                container_spec
                    .as_ref()
                    .and_then(|container| container.image.as_deref()),
            )
            .map_err(|e| {
                with_context(
                    e,
                    format!("Failed to prepare root filesystem for {}", container_name),
                )
            })?;
            runtime.recreate(container_id)?;
            state = runtime.state(container_id)?;
        }

        match state.status {
            ContainerStatus::Created => {}
            ContainerStatus::Creating | ContainerStatus::Unknown => {
                if Instant::now() >= wait_deadline {
                    return Err(new_error(format!(
                        "Container {} did not reach Created state within {:?} (last observed {:?})",
                        container_name, creation_timeout, state.status
                    )));
                }
                log::debug!(
                    "Waiting for container '{}' to reach Created state (attempt {}, status {:?})",
                    container_name,
                    attempt,
                    state.status
                );
                sleep(creation_backoff).await;
                creation_backoff = (creation_backoff * 2).min(Duration::from_secs(1));
                continue;
            }
            ContainerStatus::Running => {
                log_info(
                    "kubelet",
                    "Container already running; skipping start",
                    &[("container", container_name), ("id", container_id)],
                );
                return Ok(());
            }
            ContainerStatus::Stopped => {
                // handled above; continue to next loop to recreate if needed
                continue;
            }
            ContainerStatus::Paused => {
                return Err(new_error(format!(
                    "Container {} is paused; resume or recreate before starting",
                    container_name
                )));
            }
        }

        let mut reprovisioned_ip: Option<String> = None;
        if !host_network && state.network.ip_addresses.is_empty() {
            if let Some(spec) = pod_spec {
                log_warn(
                    "kubelet",
                    "Container missing network addresses; attempting to reprovision CNI",
                    &[("container", container_name), ("id", container_id)],
                );
                let result = provision_container_network(container_id, container_name, spec)
                    .map_err(|e| {
                        with_context(
                            e,
                            format!(
                                "Failed to reprovision CNI network for container {}",
                                container_name
                            ),
                        )
                    })?;
                reprovisioned_ip = result
                    .ips
                    .first()
                    .and_then(|entry| entry.address.split('/').next().map(str::to_string));
                state = runtime.state(container_id)?;
            } else {
                log_warn(
                    "kubelet",
                    "Pod spec unavailable while reprovisioning container network",
                    &[("container", container_name), ("id", container_id)],
                );
            }
        }

        let ip = state
            .network
            .ip_addresses
            .first()
            .and_then(|addr| addr.split('/').next().map(str::to_string))
            .or_else(|| reprovisioned_ip.clone())
            .or_else(|| {
                if host_network {
                    container_spec
                        .as_ref()
                        .and_then(|container| {
                            container.ports.iter().find_map(|port| port.host_ip.clone())
                        })
                        .or(Some("127.0.0.1".to_string()))
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                new_error(format!(
                    "Container {} does not have an assigned IP address",
                    container_name
                ))
            })?;

        setup_container_files(container_id, &ip).map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to persist network metadata for container '{}'",
                    container_name
                ),
            )
        })?;

        let resolv_conf = resolv_conf_template.as_deref();
        log_info(
            "kubelet",
            "Configuring runtime files",
            &[("container", container_name), ("id", container_id)],
        );
        let app_name = app.to_string();
        let ip_for_runtime = ip.clone();
        let runtime_config_clone = runtime_config.clone();
        let tls_info_clone = tls_info.clone();
        let resolv_conf_owned = resolv_conf.map(|value| value.to_string());
        runtime
            .with_namespace(
                container_id,
                Box::new(move || {
                    let resolv_conf_ref = resolv_conf_owned.as_deref();
                    setup_runtime_files(
                        &app_name,
                        &ip_for_runtime,
                        &runtime_config_clone,
                        &tls_info_clone,
                        resolv_conf_ref,
                    )
                }),
            )
            .map_err(|e| with_context(e, "Failed to setup runtime files"))?;

        let pid = state.pid.ok_or_else(|| {
            new_error(format!(
                "Container {} does not have an init pid",
                container_name
            ))
        })?;
        let pid_str = pid.to_string();
        log_info(
            "kubelet",
            "Container process started",
            &[("container", container_name), ("pid", pid_str.as_str())],
        );

        let log_dir = container_log_dir(container_id);
        std::fs::create_dir_all(&log_dir).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to create container log directory {}",
                    log_dir.display()
                ),
            )
        })?;

        if let Some(exec) = pre_start_exec.as_ref() {
            run_exec_handler(container_id, exec)
                .map_err(|e| with_context(e, "preStart hook failed"))?;
        }

        match runtime.send_start(container_id) {
            Ok(()) => break,
            Err(err) => {
                let missing_start_control = err
                    .downcast_ref::<io::Error>()
                    .map(|io_err| io_err.kind() == ErrorKind::NotFound)
                    .unwrap_or(false);
                if missing_start_control && attempt == 1 {
                    log_warn(
                        "kubelet",
                        "Start control missing while restoring container; recreating init process",
                        &[("container", container_name), ("id", container_id)],
                    );
                    runtime.kill(container_id.to_string()).await?;
                    continue;
                }
                return Err(err);
            }
        }
    }

    if let Some(probe) = readiness_probe {
        runtime.set_status(container_id, ContainerStatus::Creating)?;
        let container_id_owned = container_id.to_string();
        let readiness_runtime = runtime.clone();
        tokio::spawn(async move {
            if let Err(err) = wait_for_readiness_probe(&container_id_owned, &probe).await {
                log_warn(
                    "kubelet",
                    "Readiness probe failed",
                    &[
                        ("container_id", container_id_owned.as_str()),
                        ("error", err.to_string().as_str()),
                    ],
                );
                return;
            }
            if let Err(err) =
                readiness_runtime.set_status(&container_id_owned, ContainerStatus::Running)
            {
                log_error(
                    "kubelet",
                    "Failed to update container state",
                    &[
                        ("container_id", container_id_owned.as_str()),
                        ("error", err.to_string().as_str()),
                    ],
                );
            }
        });
    } else {
        runtime.set_status(container_id, ContainerStatus::Running)?;
    }

    if let Some(exec) = post_start_exec.clone() {
        let container_id_owned = container_id.to_string();
        tokio::spawn(async move {
            if let Err(err) = run_exec_handler(&container_id_owned, &exec) {
                log_warn(
                    "kubelet",
                    "postStart hook failed",
                    &[
                        ("container_id", container_id_owned.as_str()),
                        ("error", err.to_string().as_str()),
                    ],
                );
            }
        });
    }

    Ok(())
}
async fn wait_for_readiness_probe(
    container_id: &str,
    probe: &ContainerProbe,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Some(exec) = &probe.exec {
        wait_for_readiness_command(
            container_id,
            &exec.command,
            probe.initial_delay_seconds,
            probe.period_seconds,
        )
        .await
    } else {
        Ok(())
    }
}

async fn wait_for_readiness_command(
    container_id: &str,
    command: &[String],
    initial_delay_seconds: Option<i32>,
    period_seconds: Option<i32>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if command.is_empty() {
        return Ok(());
    }

    if let Some(delay) = initial_delay_seconds {
        if delay > 0 {
            sleep(Duration::from_secs(delay.max(0) as u64)).await;
        }
    }

    let period = period_seconds.unwrap_or(1).max(1) as u64;

    let runtime = container_runtime();
    loop {
        let cmd: Vec<String> = command.to_owned();
        let result = runtime.exec(container_id, Box::new(move || prepare_null_stdio_exec(cmd)));

        match result {
            Ok(exec_result) => {
                if matches!(
                    exec_result.wait_status,
                    nix::sys::wait::WaitStatus::Exited(_, 0)
                ) {
                    log_readiness_exec_proc_guard(container_id, "succeeded");
                    break;
                }
                log_readiness_exec_proc_guard(container_id, "failed");
                let message =
                    format_wait_status_failure("Readiness command", exec_result.wait_status);
                log_info(
                    "kubelet",
                    "Readiness probe reported not ready",
                    &[("container_id", container_id), ("error", message.as_str())],
                );
                sleep(Duration::from_secs(period)).await;
            }
            Err(err) => {
                log_readiness_exec_proc_guard(container_id, "failed");
                log_info(
                    "kubelet",
                    "Readiness probe reported not ready",
                    &[
                        ("container_id", container_id),
                        ("error", err.to_string().as_str()),
                    ],
                );
                sleep(Duration::from_secs(period)).await;
            }
        }
    }

    Ok(())
}

fn log_readiness_exec_proc_guard(container_id: &str, context: &str) {
    if let Some(mounted) = container_runtime().take_exec_proc_mount_status() {
        debug!(
            "Readiness exec {} for {} (proc guard mounted /proc = {})",
            context, container_id, mounted
        );
    }
}

fn prepare_null_stdio_exec(
    command: Vec<String>,
) -> Result<ExecRequest, Box<dyn Error + Send + Sync>> {
    if command.is_empty() {
        return Err(new_error("Readiness command is empty"));
    }

    redirect_standard_to_null(libc::STDIN_FILENO, true)?;
    redirect_standard_to_null(libc::STDOUT_FILENO, false)?;
    redirect_standard_to_null(libc::STDERR_FILENO, false)?;

    let mut parts = command.into_iter();
    let program = parts
        .next()
        .ok_or_else(|| new_error("Readiness command missing executable"))?;
    let args: Vec<String> = parts.collect();

    Ok(ExecRequest {
        program,
        args,
        env: None,
    })
}

fn redirect_standard_to_null(
    target: i32,
    read_only: bool,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let flags = if read_only {
        OFlag::O_RDONLY
    } else {
        OFlag::O_WRONLY
    };
    let fd = open("/dev/null", flags, Mode::empty())
        .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?
        .into_raw_fd();
    if unsafe { libc::dup2(fd, target) } == -1 {
        let err = io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(Box::new(err));
    }
    if unsafe { libc::close(fd) } == -1 {
        return Err(Box::new(io::Error::last_os_error()));
    }
    Ok(())
}

fn format_wait_status_failure(label: &str, status: nix::sys::wait::WaitStatus) -> String {
    match status {
        nix::sys::wait::WaitStatus::Exited(_, code) => {
            format!("{label} exited with status {code}")
        }
        nix::sys::wait::WaitStatus::Signaled(_, signal, _) => {
            format!("{label} terminated by signal {signal}")
        }
        other => format!("{label} failed with {:?}", other),
    }
}

fn run_exec_handler(
    container_id: &str,
    exec: &ProbeExec,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if exec.command.is_empty() {
        return Ok(());
    }

    let cmd = exec.command.clone();
    let runtime = container_runtime();
    let exec_result = runtime
        .exec(container_id, Box::new(move || prepare_null_stdio_exec(cmd)))
        .map_err(|err| with_context(err, "Failed to execute lifecycle command"))?;

    match exec_result.wait_status {
        nix::sys::wait::WaitStatus::Exited(_, 0) => Ok(()),
        status => Err(new_error(format_wait_status_failure(
            "Lifecycle command",
            status,
        ))),
    }
}
fn create_rootfs(
    id: &str,
    name: &str,
    oci_manifest: &OciManifest,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let refs_dir = container_refs_dir();
    let name_link = refs_dir.join(name);
    if std::fs::exists(&name_link)? {
        return Err(new_error(format!(
            "Container with name {} already exists",
            name
        )));
    }
    std::fs::create_dir_all(&refs_dir).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to create container refs directory {}",
                refs_dir.display()
            ),
        )
    })?;

    let base = container_root_path(id);
    let rootfs = base.join("rootfs");
    let overlay_dir = base.join("overlay");
    let upper = overlay_dir.join("upper");
    let work = overlay_dir.join("work");
    let lower_dirs = oci_manifest
        .layers
        .iter()
        .rev()
        .map(|l| format!("/var/lib/nanocloud.io/image/overlay/{}", &l.digest[7..]))
        .collect::<Vec<_>>();
    let lower = if lower_dirs.is_empty() {
        let fallback = overlay_dir.join("lower");
        std::fs::create_dir_all(&fallback).map_err(|e| {
            with_context(
                e,
                format!("Failed to create fallback lowerdir {}", fallback.display()),
            )
        })?;
        fallback.to_string_lossy().to_string()
    } else {
        lower_dirs.join(":")
    };

    for dir in [&rootfs, &upper, &work] {
        std::fs::create_dir_all(dir).map_err(|e| {
            with_context(
                e,
                format!("Failed to create overlay directory {}", dir.display()),
            )
        })?;
    }

    let source = CString::new("overlay")?;
    let target = CString::new(rootfs.to_string_lossy().to_string())?;
    let fstype = CString::new("overlay")?;
    let options = CString::new(format!(
        "lowerdir={},upperdir={},workdir={},metacopy=on,redirect_dir=on",
        &lower,
        upper.to_string_lossy(),
        work.to_string_lossy()
    ))?;

    nix::mount::mount(
        Some(source.as_c_str()),
        target.as_c_str(),
        Some(fstype.as_c_str()),
        nix::mount::MsFlags::from_bits_truncate(0),
        Some(options.as_c_str()),
    )
    .map_err(|e| with_context(e, format!("Failed to mount overlay for container {}", name)))?;

    if let Err(err) = std::os::unix::fs::symlink(format!("../sha256/{}", id), &name_link) {
        let _ = nix::mount::umount(target.as_c_str());
        return Err(with_context(
            err,
            format!(
                "Failed to create container name symlink {} -> {}",
                name_link.display(),
                id
            ),
        ));
    }

    Ok(())
}

fn ensure_rootfs_mounted(
    container_id: &str,
    image_reference: Option<&str>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let base = container_root_path(container_id);
    let rootfs = base.join("rootfs");
    if rootfs.exists() && is_rootfs_mounted(&rootfs)? {
        return Ok(());
    }

    let manifest = load_manifest_for_container(container_id, image_reference)?;
    let overlay_dir = base.join("overlay");
    let upper = overlay_dir.join("upper");
    let work = overlay_dir.join("work");

    for dir in [&rootfs, &upper, &work] {
        std::fs::create_dir_all(dir).map_err(|e| {
            with_context(
                e,
                format!("Failed to prepare overlay directory {}", dir.display()),
            )
        })?;
    }

    let lower_dirs = overlay_lowerdirs(&manifest)?;
    let lower = if lower_dirs.is_empty() {
        let fallback = overlay_dir.join("lower");
        std::fs::create_dir_all(&fallback).map_err(|e| {
            with_context(
                e,
                format!("Failed to create fallback lowerdir {}", fallback.display()),
            )
        })?;
        fallback.to_string_lossy().to_string()
    } else {
        lower_dirs.join(":")
    };

    let source = CString::new("overlay")?;
    let target = CString::new(rootfs.to_string_lossy().to_string())?;
    let fstype = CString::new("overlay")?;
    let options = CString::new(format!(
        "lowerdir={},upperdir={},workdir={},metacopy=on,redirect_dir=on",
        &lower,
        upper.to_string_lossy(),
        work.to_string_lossy()
    ))?;

    nix::mount::mount(
        Some(source.as_c_str()),
        target.as_c_str(),
        Some(fstype.as_c_str()),
        nix::mount::MsFlags::from_bits_truncate(0),
        Some(options.as_c_str()),
    )
    .map_err(|e| {
        with_context(
            e,
            format!("Failed to remount overlay for container {}", container_id),
        )
    })?;

    debug!(
        "Remounted overlay filesystem for container {} at {}",
        container_id,
        rootfs.display()
    );

    Ok(())
}

fn load_manifest_for_container(
    container_id: &str,
    image_reference: Option<&str>,
) -> Result<OciManifest, Box<dyn Error + Send + Sync>> {
    if let Some(image) = image_reference {
        match parse_image_reference(image) {
            Ok(reference) => match load_manifest_from_store(&reference) {
                Ok(manifest) => return Ok(manifest),
                Err(err) => {
                    debug!(
                        "Failed to load manifest for image {} from cache: {}",
                        image, err
                    );
                }
            },
            Err(err) => {
                debug!("Failed to parse image reference {}: {}", image, err);
            }
        }
    }

    let digest_path = container_root_path(container_id).join("image_digest");
    let raw_digest = std::fs::read_to_string(&digest_path).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to read cached image digest for container {}",
                container_id
            ),
        )
    })?;
    let digest = raw_digest.trim();
    if digest.is_empty() {
        return Err(new_error(format!(
            "Cached image digest missing for container {}",
            container_id
        )));
    }

    find_manifest_by_config_digest(digest)
}

fn find_manifest_by_config_digest(
    digest: &str,
) -> Result<OciManifest, Box<dyn Error + Send + Sync>> {
    let refs_root = Path::new("/var/lib/nanocloud.io/image/refs");
    let mut stack: Vec<PathBuf> = vec![refs_root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(with_context(
                    err,
                    format!("Failed to read manifest references in {}", dir.display()),
                ));
            }
        };
        for entry in entries {
            let entry = entry.map_err(|err| {
                with_context(
                    err,
                    format!("Failed to inspect manifest entry in {}", dir.display()),
                )
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|err| {
                with_context(err, format!("Failed to inspect {}", path.display()))
            })?;
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if file_type.is_file() || file_type.is_symlink() {
                match std::fs::File::open(&path) {
                    Ok(file) => {
                        let reader = BufReader::new(file);
                        if let Ok(manifest) = serde_json::from_reader::<_, OciManifest>(reader) {
                            if manifest.config.digest == digest {
                                return Ok(manifest);
                            }
                        }
                    }
                    Err(err) if err.kind() == ErrorKind::NotFound => continue,
                    Err(err) => {
                        return Err(with_context(
                            err,
                            format!("Failed to open manifest reference {}", path.display()),
                        ));
                    }
                }
            }
        }
    }

    Err(new_error(format!(
        "Cached manifest for config digest {} not found",
        digest
    )))
}

fn overlay_lowerdirs(manifest: &OciManifest) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
    let mut dirs = Vec::new();
    for layer in manifest.layers.iter().rev() {
        if !layer.digest.starts_with("sha256:") || layer.digest.len() != 71 {
            return Err(new_error(format!(
                "Unsupported layer digest format: {}",
                layer.digest
            )));
        }
        dirs.push(format!(
            "/var/lib/nanocloud.io/image/overlay/{}",
            &layer.digest[7..]
        ));
    }
    Ok(dirs)
}

fn is_rootfs_mounted(path: &Path) -> std::io::Result<bool> {
    match std::fs::metadata(path) {
        Ok(meta) => match path.parent() {
            Some(parent) => {
                let parent_meta = std::fs::metadata(parent)?;
                Ok(meta.dev() != parent_meta.dev() || meta.ino() == parent_meta.ino())
            }
            None => Ok(true),
        },
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

pub fn get_container_id_by_name(name: &str) -> Option<String> {
    let link = container_refs_dir().join(name);
    std::fs::read_link(link).ok().and_then(|t| {
        t.components()
            .next_back()
            .and_then(|comp| comp.as_os_str().to_str())
            .map(|s| s.to_string())
    })
}

pub fn resolve_container_id(
    namespace: Option<&str>,
    app: &str,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let container_name = namespace
        .map(|ns| format!("{}-{}", ns, app))
        .unwrap_or_else(|| app.to_string());
    get_container_id_by_name(&container_name)
        .ok_or_else(|| new_error(format!("Service '{}' not found", container_name)))
}

/*
fn get_port_bindings(image_info: &ImageInfo) -> HashMap<String, Option<Vec<PortBinding>>> {
    // Ports should be configured in the form <port_number>/<protocol>, and may optionally be prefixed with <bind_ip_address>:
    image_info.get_ports().iter().map(|(_name, port)| {
        let parts: Vec<&str> = port.split(':').rev().collect::<Vec<&str>>();
        let port = String::from(*parts.get(0).expect("Invalid port"));
        let ip : Option<String> = if let Some(ip) = parts.get(1) { Some((*ip).to_string()) } else { None };
        (port.clone(), Some(vec![PortBinding { host_ip: ip, host_port: Some(port.clone()) }]))
    }).collect()
}

fn get_exposed_ports(image_info: &ImageInfo) -> HashMap<String, HashMap<(), ()>> {
    // Exposed ports should be configured in the form <port_number>/<protocol>
    image_info.get_ports().iter().map(|(_name, port)| {
        (port.clone(), HashMap::new())
    }).collect()
}

fn get_mounts(container_name: &str, image_info: &ImageInfo) -> Vec<Mount> {
    image_info.get_volumes().iter()
        .map(|(key, path)| {
            Mount {
                source: Some(format!("{}-{}", container_name, key)),
                target: Some(path.clone()),
                typ: Some(MountTypeEnum::VOLUME),
                ..Default::default()
            }
        })
        .collect()
}
*/
// Helper function to ensure a volume is created if it doesn't already exist
/*
async fn exec_container(docker: &Docker, container_id: &str, command: Vec<&str>, stdin: Option<Vec<u8>>) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Prepare the exec command to run inside the container
    let exec_options = CreateExecOptions {
        cmd: Some(command),
        attach_stdin: stdin.as_ref().map(|_| true),
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        ..Default::default()
    };

    // Create an exec instance
    let exec_results = docker
        .create_exec(container_id, exec_options)
        .await
        .map_err(|e| format!("Failed to create exec instance: {}", e))?;

    // Start the exec with the given input
    let mut start_exec_results = docker
        .start_exec(&exec_results.id, None)
        .await
        .map_err(|e| format!("Failed to start exec: {}", e))?;

    if let StartExecResults::Attached { ref mut output, ref mut input, .. } = start_exec_results {
        // Write to stdin if input is provided
        if let Some(stdin_data) = stdin {
            input.write_all(&stdin_data)
                .await
                .map_err(|e| format!("Failed to write data to stdin: {}", e))?;
            input.shutdown()
                .await
                .map_err(|e| format!("Failed to close input: {}", e))?;
        }

        stream_logs(output).await?;
    }

    Ok(())
}
*/
// use std::io::Read;
// fn setup_network_files(app: &str, ip: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
//     let contents = format!(
//         "127.0.0.1   localhost
// ::1         localhost
// {ip}   {app}.nanocloud.local {app}
// "
// );

// let file = std::fs::File::create("/etc/hosts")?;
// file.set_permissions(std::fs::Permissions::from_mode(0o644))?;

// let mut writer = std::io::BufWriter::new(file);
// writer.write_all(contents.as_bytes())?;
// writer.flush()?;
//     // // let path = format!("{}/env", &runtime_dir);
//     // // let mut file = std::fs::File::create(&path)?;
//     // // file.write_all(env_vars.join("\n").as_bytes())?;
//     // // std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444))?;
//     let mut file = std::fs::File::open("/etc/hosts")?;
//     let mut contents = String::new();
//     file.read_to_string(&mut contents)?;
//     print!("{}", contents);

//     Ok(())
// }

pub fn setup_container_files(
    container_id: &str,
    ip_address: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let network_dir = container_root_path(container_id).join("network");
    std::fs::create_dir_all(&network_dir).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to create network state directory {}",
                network_dir.display()
            ),
        )
    })?;
    let ip_path = network_dir.join("ip_address");
    let mut file = std::fs::File::create(&ip_path).map_err(|e| {
        with_context(
            e,
            format!("Failed to create IP address file {}", ip_path.display()),
        )
    })?;
    file.write_all(ip_address.as_bytes()).map_err(|e| {
        with_context(
            e,
            format!("Failed to write IP address file {}", ip_path.display()),
        )
    })?;
    file.flush().map_err(|e| {
        with_context(
            e,
            format!("Failed to flush IP address file {}", ip_path.display()),
        )
    })?;

    Ok(())
}

fn ensure_private_mount_namespace() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo")
        .map_err(|e| with_context(e, "Failed to read /proc/self/mountinfo"))?;

    for line in mountinfo.lines() {
        let fields: Vec<&str> = line.split(' ').collect();
        if fields.len() < 7 {
            continue;
        }
        if fields[4] != "/" {
            continue;
        }

        let dash_index = fields
            .iter()
            .position(|field| *field == "-")
            .ok_or_else(|| new_error("Malformed mountinfo entry for root mount"))?;
        let optional = &fields[6..dash_index];

        if optional
            .iter()
            .any(|field| field.starts_with("shared:") || field.starts_with("master:"))
        {
            return Err(new_error(
                "Root mount namespace is shared with the host; refusing to rewrite container /etc/hosts",
            ));
        }

        return Ok(());
    }

    Err(new_error(
        "Unable to locate root mount entry in /proc/self/mountinfo",
    ))
}

pub fn setup_runtime_files(
    app: &str,
    ip: &str,
    config: &HashMap<String, Vec<u8>>,
    tls_info: &TlsInfo,
    resolv_conf: Option<&str>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let runtime = container_runtime();
    let proc_guard_status = runtime.take_exec_proc_mount_status();
    let proc_guard_mounted = proc_guard_status.unwrap_or(false);
    match proc_guard_status {
        Some(true) => debug!("Exec helper mounted /proc before configuring runtime files"),
        Some(false) => debug!("Exec helper verified /proc mount before configuring runtime files"),
        None => debug!(
            "Exec helper did not report /proc guard status; proceeding with runtime files setup"
        ),
    }

    let namespace_check = match ensure_private_mount_namespace() {
        Ok(()) => Ok(()),
        Err(err) if proc_guard_mounted => {
            debug!(
                "Initial mount namespace check failed after /proc guard (retrying): {}",
                err
            );
            ensure_private_mount_namespace()
        }
        Err(err) => Err(err),
    };

    namespace_check.map_err(|e| {
        with_context(
            e,
            "Mount namespace not private; refusing to configure runtime files",
        )
    })?;
    // Create hosts file
    let hosts = format!(
        "127.0.0.1   localhost
::1         localhost
{ip}   {app}.nanocloud.local {app}
"
    );
    let mut file = std::fs::File::create("/etc/hosts")
        .map_err(|e| with_context(e, "Failed to create /etc/hosts"))?;
    file.set_permissions(std::fs::Permissions::from_mode(0o644))
        .map_err(|e| with_context(e, "Failed to set permissions on /etc/hosts"))?;
    file.write_all(hosts.as_bytes())
        .map_err(|e| with_context(e, "Failed to write /etc/hosts"))?;

    if let Some(contents) = resolv_conf {
        let mut resolv = std::fs::File::create("/etc/resolv.conf")
            .map_err(|e| with_context(e, "Failed to create /etc/resolv.conf"))?;
        resolv
            .set_permissions(std::fs::Permissions::from_mode(0o644))
            .map_err(|e| with_context(e, "Failed to set permissions on /etc/resolv.conf"))?;
        resolv
            .write_all(contents.as_bytes())
            .map_err(|e| with_context(e, "Failed to write /etc/resolv.conf"))?;
    }
    // let mut writer = std::io::BufWriter::new(file);
    // writer.write_all(hosts.as_bytes())?;
    // writer.flush()?;

    // setup_network_files(app, &ip)?;
    // let runtime_dir = format!("/var/lib/nanocloud.io/container/sha256/{}/rootfs/var/run/nanocloud.io", container_id);
    let runtime_dir = "/var/run/nanocloud.io";

    // Set environment variables.
    let mut env_vars: Vec<String> = Vec::with_capacity(config.len());
    for (key, value) in config {
        let rendered = String::from_utf8(value.clone()).map_err(|e| {
            with_context(
                e,
                format!("Configuration value '{}' contains invalid UTF-8", key),
            )
        })?;
        env_vars.push(format!(
            "NANOCLOUD_{}_{}='{}'",
            app.to_uppercase(),
            key.to_uppercase().replace('.', "_"),
            rendered
        ));
    }
    let path = format!("{}/env", &runtime_dir);
    let mut file = std::fs::File::create(&path)
        .map_err(|e| with_context(e, format!("Failed to create env file {}", path)))?;
    file.set_permissions(std::fs::Permissions::from_mode(0o444))
        .map_err(|e| with_context(e, format!("Failed to set permissions on {}", path)))?;
    file.write_all(env_vars.join("\n").as_bytes())
        .map_err(|e| with_context(e, format!("Failed to write env file {}", path)))?;

    // Set up TLS certs
    let tls_dir = format!("{}/tls", &runtime_dir);
    std::fs::create_dir_all(&tls_dir)
        .map_err(|e| with_context(e, format!("Failed to create TLS directory {}", tls_dir)))?;
    for (name, content) in [
        ("key.pem", &tls_info.key),
        ("cert.pem", &tls_info.cert),
        ("ca.pem", &tls_info.ca),
    ] {
        let path = format!("{}/{}", &tls_dir, &name);
        let mut file = std::fs::File::create(&path)
            .map_err(|e| with_context(e, format!("Failed to create TLS file {}", path)))?;
        file.set_permissions(std::fs::Permissions::from_mode(0o444))
            .map_err(|e| with_context(e, format!("Failed to set permissions on {}", path)))?;
        file.write_all(content)
            .map_err(|e| with_context(e, format!("Failed to write TLS file {}", path)))?;
    }

    Ok(())
}

pub fn generate_runtime_tls(app: &str) -> Result<TlsInfo, Box<dyn Error + Send + Sync>> {
    let mut san = vec![format!("{}.nanocloud.local", app)];
    if let Ok(ip) = get_local_ip() {
        san.push(ip);
    }
    TlsInfo::create(app, Some(&san))
        .map_err(|e| with_context(e, "Failed to generate runtime TLS assets"))
}

pub async fn kill(container_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    container_runtime().kill(container_id.to_string()).await
}

pub fn exec_in_container<F>(
    container_id: &str,
    action: F,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    F: FnOnce() -> Result<(), Box<dyn Error + Send + Sync>> + Send + 'static,
{
    container_runtime().with_namespace(container_id, Box::new(action))
}

fn get_local_ip() -> Result<String, Box<dyn Error + Send + Sync>> {
    unsafe {
        let mut ifap: *mut libc::ifaddrs = ptr::null_mut();

        if libc::getifaddrs(&mut ifap) != 0 {
            return Err(new_error("Failed to list network interfaces"));
        }

        let mut current = ifap;
        let mut selected_ip: Option<String> = None;

        while !current.is_null() {
            let iface = &*current;

            if !iface.ifa_addr.is_null() && (*iface.ifa_addr).sa_family as i32 == libc::AF_INET {
                let name = CStr::from_ptr(iface.ifa_name).to_str().unwrap_or("");
                if name == "lo" {
                    current = (*current).ifa_next;
                    continue;
                }

                let sockaddr: *const libc::sockaddr_in = iface.ifa_addr as *const libc::sockaddr_in;
                let ip = Ipv4Addr::from((*sockaddr).sin_addr.s_addr.to_be());
                selected_ip = Some(ip.to_string());
                break;
            }

            current = (*current).ifa_next;
        }

        libc::freeifaddrs(ifap);

        selected_ip.ok_or_else(|| new_error("Unable to detect local IP address"))
    }
}
