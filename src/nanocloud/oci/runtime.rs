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

use crate::nanocloud::engine::log::{
    container_log_dir, container_log_path, write_docker_json_logs,
};
use crate::nanocloud::k8s::pod::{ContainerResources, ContainerSpec, VolumeSpec};
use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::security::volume::{
    cleanup_mount_dir, close_mapper, encrypted_host_mount, encrypted_mapper_name,
    ensure_encrypted_volume_root, ensure_luks_device, mount_mapper, open_mapper, read_volume_key,
    unmount_if_mounted,
};

use chrono::{TimeZone, Utc};
use log::{debug, error, info, warn};
use nix::errno::Errno;
use nix::mount::umount2;
use nix::sched::{setns, CloneFlags};
use nix::sys::signal::{kill, Signal};
use nix::sys::stat::fstat;
use nix::unistd::{
    chdir, execvpe, fchown, geteuid, pipe, setgid, setuid, ForkResult, Gid, Pid, Uid,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::ffi::{CStr, CString};
use std::fs::{self, create_dir_all, remove_dir_all, File};
use std::io::{self, BufReader, ErrorKind, Read, Write};
use std::os::fd::{BorrowedFd, OwnedFd};
use std::os::unix::fs::{symlink, MetadataExt, PermissionsExt};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::Builder as TokioRuntimeBuilder;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OciConfig {
    #[serde(rename = "ociVersion")]
    pub oci_version: String,
    pub process: Process,
    pub root: Root,
    pub hostname: String,
    pub mounts: Vec<Mount>,
    pub linux: Linux,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub encrypted_volumes: Vec<EncryptedVolumeMount>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Process {
    pub terminal: bool,
    pub user: User,
    pub args: Vec<String>,
    pub env: Vec<String>,
    pub cwd: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct User {
    pub uid: u32,
    pub gid: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Root {
    pub path: String,
    pub readonly: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Mount {
    pub destination: String,
    #[serde(rename = "type")]
    pub mount_type: String,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EncryptedVolumeMount {
    pub volume: String,
    pub device: String,
    #[serde(rename = "keyName")]
    pub key_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filesystem: Option<String>,
    pub mapper: String,
    #[serde(rename = "hostMount")]
    pub host_mount: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Linux {
    pub namespaces: Vec<Namespace>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<LinuxResources>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LinuxResources {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<LinuxCpu>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<LinuxMemory>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pids: Option<LinuxPids>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LinuxCpu {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shares: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LinuxMemory {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swap: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LinuxPids {
    pub limit: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Namespace {
    #[serde(rename = "type")]
    pub ns_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

pub struct Runtime {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum ContainerStatus {
    Creating,
    Created,
    Running,
    Paused,
    Stopped,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ContainerNetwork {
    pub namespace: Option<String>,
    pub ip_addresses: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ExecRequest {
    pub program: String,
    pub args: Vec<String>,
    pub env: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct ExecResult {
    pub wait_status: nix::sys::wait::WaitStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ContainerState {
    pub id: String,
    pub name: Option<String>,
    pub pid: Option<i32>,
    pub status: ContainerStatus,
    pub status_detail: Option<String>,
    pub bundle: String,
    pub rootfs: String,
    pub config_path: Option<String>,
    pub created_at: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub image_digest: Option<String>,
    pub hostname: Option<String>,
    pub command: Vec<String>,
    pub environment: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub labels: HashMap<String, String>,
    pub runtime: String,
    pub network: ContainerNetwork,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ContainerSummary {
    pub id: String,
    pub name: Option<String>,
    pub status: ContainerStatus,
    pub pid: Option<i32>,
    pub created_at: Option<String>,
    pub image_digest: Option<String>,
    pub hostname: Option<String>,
    pub runtime: String,
    pub ip_addresses: Vec<String>,
}

impl From<ContainerState> for ContainerSummary {
    fn from(state: ContainerState) -> Self {
        ContainerSummary {
            id: state.id,
            name: state.name,
            status: state.status,
            pid: state.pid,
            created_at: state.created_at,
            image_digest: state.image_digest,
            hostname: state.hostname,
            runtime: state.runtime,
            ip_addresses: state.network.ip_addresses,
        }
    }
}

// create – Create a container (without starting it).
// start – Start a created container.
// kill – Send a signal to the container’s init process.
// delete – Delete a container (after it has stopped).
// state – Output the state of a container.
// list – List all containers managed by the runtime.
// exec – Execute a new process in a running container.
// run – Create and start a container in one operation.
// pause – Pause all processes in a container.
// resume – Resume all paused processes in a container.
// spec – Generate a new OCI spec configuration file

const PID_NAMESPACE: u8 = 3;
const INIT_READY: u8 = 4;
const INIT_FAILED: u8 = 5;
const EXEC_STATUS_FAILURE: u8 = 0;
const EXEC_STATUS_SUCCESS: u8 = 1;
const EXEC_PROC_NOT_MOUNTED: u8 = 0;
const EXEC_PROC_MOUNTED: u8 = 1;
const EXEC_PROC_UNKNOWN: u8 = 2;
const CONTAINER_BASE_DIR: &str = "/var/lib/nanocloud.io/container/sha256";

fn container_base_dir() -> PathBuf {
    env::var("NANOCLOUD_CONTAINER_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(CONTAINER_BASE_DIR))
}

pub fn container_root_path(container_id: &str) -> PathBuf {
    container_base_dir().join(container_id)
}

pub fn container_refs_dir() -> PathBuf {
    container_base_dir()
        .parent()
        .map(|parent| parent.join("refs"))
        .unwrap_or_else(|| PathBuf::from("/var/lib/nanocloud.io/container/refs"))
}

pub fn netns_dir() -> PathBuf {
    env::var("NANOCLOUD_NETNS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/var/run/netns"))
}

impl Runtime {
    pub fn configure_from_spec(
        container_id: &str,
        container_name: &str,
        container: &ContainerSpec,
        volumes: &[VolumeSpec],
        host_network: bool,
    ) -> OciConfig {
        let oci_version = "1.2.1".to_string();
        let effective_command = if container.command.is_empty() {
            container.image_command.clone()
        } else {
            container.command.clone()
        };
        let effective_args = if container.args.is_empty() {
            container.image_args.clone()
        } else {
            container.args.clone()
        };
        let (uid, gid) = resolve_process_user(container.user.as_deref());

        let process = Process {
            terminal: true,
            user: User { uid, gid },
            args: effective_command
                .into_iter()
                .chain(effective_args)
                .collect(),
            env: container
                .env
                .iter()
                .map(|env| match &env.value {
                    Some(value) => format!("{}={}", env.name, value),
                    None => env.name.clone(),
                })
                .collect(),
            cwd: container
                .working_dir
                .clone()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "/".to_string()),
        };
        let root = Root {
            path: "rootfs".to_string(),
            readonly: false,
        };
        let hostname = format!("{}.nanocloud.local", container_name);
        let mut mounts: Vec<Mount> = vec![
            Mount {
                destination: "/proc".to_string(),
                mount_type: "proc".to_string(),
                source: "proc".to_string(),
                options: Some(vec![
                    "nosuid".to_string(),
                    "noexec".to_string(),
                    "nodev".to_string(),
                ]),
            },
            Mount {
                destination: "/sys".to_string(),
                mount_type: "sysfs".to_string(),
                source: "sysfs".to_string(),
                options: Some(vec![
                    "nosuid".to_string(),
                    "noexec".to_string(),
                    "nodev".to_string(),
                ]),
            },
            Mount {
                destination: "/dev".to_string(),
                mount_type: "tmpfs".to_string(),
                source: "tmpfs".to_string(),
                options: Some(vec![
                    "nosuid".to_string(),
                    "strictatime".to_string(),
                    "mode=755".to_string(),
                    "size=65536k".to_string(),
                ]),
            },
            Mount {
                destination: "/dev/pts".to_string(),
                mount_type: "devpts".to_string(),
                source: "devpts".to_string(),
                options: Some(vec![
                    "nosuid".to_string(),
                    "noexec".to_string(),
                    "newinstance".to_string(),
                    "ptmxmode=0666".to_string(),
                    "mode=0620".to_string(),
                    "gid=5".to_string(), // tty group
                ]),
            },
            Mount {
                destination: "/dev/shm".to_string(),
                mount_type: "tmpfs".to_string(),
                source: "shm".to_string(),
                options: Some(vec![
                    "nosuid".to_string(),
                    "noexec".to_string(),
                    "nodev".to_string(),
                    "mode=1777".to_string(),
                    "size=65536k".to_string(),
                ]),
            },
            Mount {
                destination: "/dev/mqueue".to_string(),
                mount_type: "mqueue".to_string(),
                source: "mqueue".to_string(),
                options: None,
            },
            Mount {
                destination: "/dev/null".to_string(),
                mount_type: "bind".to_string(),
                source: "/dev/null".to_string(),
                options: Some(vec!["rbind".to_string(), "nosuid".to_string()]),
            },
            Mount {
                destination: "/dev/zero".to_string(),
                mount_type: "bind".to_string(),
                source: "/dev/zero".to_string(),
                options: Some(vec!["rbind".to_string(), "nosuid".to_string()]),
            },
            Mount {
                destination: "/dev/full".to_string(),
                mount_type: "bind".to_string(),
                source: "/dev/full".to_string(),
                options: Some(vec!["rbind".to_string(), "nosuid".to_string()]),
            },
            Mount {
                destination: "/dev/random".to_string(),
                mount_type: "bind".to_string(),
                source: "/dev/random".to_string(),
                options: Some(vec!["rbind".to_string(), "nosuid".to_string()]),
            },
            Mount {
                destination: "/dev/urandom".to_string(),
                mount_type: "bind".to_string(),
                source: "/dev/urandom".to_string(),
                options: Some(vec!["rbind".to_string(), "nosuid".to_string()]),
            },
            Mount {
                destination: "/dev/tty".to_string(),
                mount_type: "bind".to_string(),
                source: "/dev/tty".to_string(),
                options: Some(vec!["rbind".to_string(), "nosuid".to_string()]),
            },
            Mount {
                destination: "/dev/console".to_string(),
                mount_type: "bind".to_string(),
                source: "/dev/console".to_string(),
                options: Some(vec!["rbind".to_string(), "nosuid".to_string()]),
            },
            Mount {
                destination: "/var/run/nanocloud.io".to_string(),
                mount_type: "tmpfs".to_string(),
                source: "tmpfs".to_string(),
                options: Some(vec![
                    "nosuid".to_string(),
                    "nodev".to_string(),
                    "noexec".to_string(),
                    "relatime".to_string(),
                    "size=64m".to_string(),
                ]),
            },
        ];
        let mount_paths: std::collections::HashMap<_, _> = container
            .volume_mounts
            .iter()
            .map(|mount| (mount.name.clone(), mount))
            .collect();
        let mut encrypted_volumes = Vec::new();

        for volume in volumes {
            if let Some(mount) = mount_paths.get(&volume.name) {
                let destination = mount.mount_path.clone();
                if let Some(encrypted) = &volume.encrypted {
                    let mapper = encrypted_mapper_name(container_id, &volume.name);
                    let host_mount_path = encrypted_host_mount(container_id, &volume.name);
                    let host_mount = host_mount_path.to_string_lossy().into_owned();
                    encrypted_volumes.push(EncryptedVolumeMount {
                        volume: volume.name.clone(),
                        device: encrypted.device.clone(),
                        key_name: encrypted.key_name.clone(),
                        filesystem: encrypted.filesystem.clone(),
                        mapper,
                        host_mount: host_mount.clone(),
                    });

                    let mut options = vec!["rbind".to_string()];
                    if mount.read_only.unwrap_or(false) {
                        options.push("ro".to_string());
                    } else {
                        options.push("rw".to_string());
                    }

                    mounts.push(Mount {
                        destination,
                        mount_type: "bind".to_string(),
                        source: host_mount,
                        options: Some(options),
                    });
                    continue;
                }

                let (source, read_only) = if let Some(host_path) = &volume.host_path {
                    (host_path.path.clone(), mount.read_only.unwrap_or(false))
                } else {
                    (
                        format!(
                            "/var/lib/nanocloud.io/storage/volume/{}-{}",
                            container_name, volume.name
                        ),
                        mount.read_only.unwrap_or(false),
                    )
                };

                let mut options = vec!["rbind".to_string()];
                if read_only {
                    options.push("ro".to_string());
                } else {
                    options.push("rw".to_string());
                }

                mounts.push(Mount {
                    destination,
                    mount_type: "bind".to_string(),
                    source,
                    options: Some(options),
                });
            }
        }
        let namespaces = if host_network {
            Vec::new()
        } else {
            let netns_path = netns_dir().join(container_id);
            vec![Namespace {
                ns_type: "network".to_string(),
                path: Some(netns_path.to_string_lossy().to_string()),
            }]
        };
        let resources = container
            .resources
            .as_ref()
            .and_then(map_container_resources);
        let linux = Linux {
            namespaces,
            resources,
        };
        OciConfig {
            oci_version,
            process,
            root,
            hostname,
            mounts,
            linux,
            encrypted_volumes,
        }
    }

    pub fn create<R: Read>(
        container_id: &str,
        env: &HashMap<String, String>,
        input: R,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if !geteuid().is_root() {
            return Err("Must be run as root".into());
        }
        let bundle = env
            .get("OCI_BUNDLE")
            .ok_or("OCI_BUNDLE not set")?
            .to_owned();
        let config: OciConfig =
            serde_json::from_reader(input).map_err(|e| format!("Failed to parse input: {}", e))?;

        Runtime::spawn_container(container_id, &bundle, config)
    }

    fn spawn_container(
        container_id: &str,
        bundle: &str,
        config: OciConfig,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        drop_start_pipe(container_id)?;

        let encrypted_volumes = config.encrypted_volumes.clone();
        let mut encrypted_guards = prepare_encrypted_volumes(&encrypted_volumes)?;

        let has_network_namespace = config
            .linux
            .namespaces
            .iter()
            .any(|ns| ns.ns_type == "network");

        let uid = config.process.user.uid;
        let gid = config.process.user.gid;
        let args: Vec<CString> = config
            .process
            .args
            .iter()
            .map(|s| CString::new(s.as_str()))
            .collect::<Result<_, _>>()?;
        let proc_env: Vec<CString> = config
            .process
            .env
            .iter()
            .map(|s| CString::new(s.as_str()))
            .collect::<Result<_, _>>()?;
        let cwd = config.process.cwd.clone();
        let rootfs = format!("{}/{}", bundle, config.root.path);
        let hostname = config.hostname.clone();

        let (log_out_r, log_out_w) = nix::unistd::pipe()?;
        let (log_err_r, log_err_w) = nix::unistd::pipe()?;

        let (child_read, parent_write) = pipe()?;
        let (parent_read, child_write) = pipe()?;

        match unsafe { nix::unistd::fork() } {
            Ok(ForkResult::Parent { child }) => {
                nix::unistd::close(child_read)?;
                nix::unistd::close(child_write)?;

                let mut buf = [0u8; 5];
                let mut total_read = 0usize;
                while total_read < buf.len() {
                    let read_now = nix::unistd::read(&parent_read, &mut buf[total_read..])?;
                    if read_now == 0 {
                        let _ = nix::sys::wait::waitpid(child, None);
                        return Err(Box::new(io::Error::other(
                            "container bootstrap pipe closed before PID message",
                        )));
                    }
                    total_read += read_now;
                }
                if buf[0] != PID_NAMESPACE {
                    let _ = nix::sys::wait::waitpid(child, None);
                    return Err(Box::new(io::Error::other(format!(
                        "Expected PID_NAMESPACE message type, got: {}",
                        buf[0]
                    ))));
                }

                let pid = u32::from_ne_bytes(buf[1..5].try_into().unwrap());
                let mut status_buf = [0u8; 1];
                let mut status_read = 0usize;
                while status_read < status_buf.len() {
                    let read_now = nix::unistd::read(&parent_read, &mut status_buf[status_read..])?;
                    if read_now == 0 {
                        let _ = nix::sys::wait::waitpid(child, None);
                        return Err(Box::new(io::Error::other(
                            "container bootstrap pipe closed before init status",
                        )));
                    }
                    status_read += read_now;
                }
                match status_buf[0] {
                    INIT_READY => {}
                    INIT_FAILED => {
                        let _ = nix::sys::wait::waitpid(child, None);
                        return Err(Box::new(io::Error::other(
                            "container namespace setup failed",
                        )));
                    }
                    other => {
                        let _ = nix::sys::wait::waitpid(child, None);
                        return Err(Box::new(io::Error::other(format!(
                            "unexpected init status message: {}",
                            other
                        ))));
                    }
                }

                let pid_path = container_root_path(container_id).join("pid");
                if let Some(parent) = pid_path.parent() {
                    create_dir_all(parent)?;
                }
                let mut file = File::create(&pid_path)?;
                file.write_all(pid.to_string().as_bytes())?;
                write_container_status(container_id, &ContainerStatus::Created)?;
                register_start_pipe(container_id, parent_write)?;
                info!("Container started with pid: {}", pid);

                let stdout_file: File = log_out_r.into();
                let stderr_file: File = log_err_r.into();

                drop(log_out_w);
                drop(log_err_w);

                let log_dir = container_log_dir(container_id);
                std::fs::create_dir_all(&log_dir)?;
                let log_path = container_log_path(container_id);

                thread::spawn(move || {
                    let rt = TokioRuntimeBuilder::new_current_thread()
                        .enable_io()
                        .build()
                        .expect("tokio runtime");
                    rt.block_on(async move {
                        let stdout_tokio = tokio::fs::File::from_std(stdout_file);
                        let stderr_tokio = tokio::fs::File::from_std(stderr_file);
                        if let Err(e) =
                            write_docker_json_logs(stdout_tokio, stderr_tokio, &log_path).await
                        {
                            error!("logger error: {e}");
                        }
                    });
                });

                nix::unistd::close(parent_read)?;
                match nix::sys::wait::waitpid(child, None)? {
                    nix::sys::wait::WaitStatus::Exited(_, 0) => {}
                    nix::sys::wait::WaitStatus::Exited(_, code) => {
                        return Err(Box::new(io::Error::other(format!(
                            "intermediate container process exited with status {}",
                            code
                        ))));
                    }
                    nix::sys::wait::WaitStatus::Signaled(_, sig, _) => {
                        return Err(Box::new(io::Error::other(format!(
                            "intermediate container process killed by signal {}",
                            sig
                        ))));
                    }
                    status => {
                        return Err(Box::new(io::Error::other(format!(
                            "unexpected wait status for intermediate process: {:?}",
                            status
                        ))));
                    }
                }
            }
            Ok(ForkResult::Child) => {
                drop(parent_write);
                nix::unistd::close(parent_read)?;
                debug!("Creating new PID namespace");
                nix::sched::unshare(CloneFlags::CLONE_NEWPID)?;

                match unsafe { nix::unistd::fork() } {
                    Ok(ForkResult::Parent { child }) => {
                        let pid = child.as_raw();
                        link_network_namespace(container_id, pid)?;

                        let mut buf = [0; 5];
                        buf[0] = PID_NAMESPACE;
                        buf[1..5].copy_from_slice(&pid.to_ne_bytes());
                        nix::unistd::write(&child_write, &buf)?;
                        nix::unistd::close(child_write)?;
                        nix::unistd::close(child_read)?;

                        drop(log_out_w);
                        drop(log_err_w);

                        debug!("Exiting intermediate process");
                        std::process::exit(0);
                    }
                    Ok(ForkResult::Child) => {
                        let start_reader = child_read;
                        drop(log_out_r);
                        drop(log_err_r);

                        let mut mount_guard = MountGuard::default();

                        let setup_result = (|| -> Result<(), Box<dyn Error + Send + Sync>> {
                            if has_network_namespace {
                                debug!("Entering network namespace");
                                enter_network_namespace(container_id, &config.linux.namespaces)?;
                            } else {
                                debug!("Host network enabled; skipping network namespace entry");
                            }
                            nix::sched::unshare(
                                CloneFlags::CLONE_NEWUTS
                                    | CloneFlags::CLONE_NEWNS
                                    | CloneFlags::CLONE_NEWIPC,
                            )?;
                            do_mount(
                                None,
                                &CString::new("/")?,
                                None,
                                libc::MS_REC | libc::MS_PRIVATE,
                                None,
                            )?;

                            do_mount(
                                Some(&CString::new(rootfs.as_str())?),
                                &CString::new(rootfs.as_str())?,
                                None,
                                libc::MS_BIND | libc::MS_REC,
                                None,
                            )?;
                            mount_guard.register(PathBuf::from(rootfs.clone()));
                            if config.root.readonly {
                                do_mount(
                                    Some(&CString::new(rootfs.as_str())?),
                                    &CString::new(rootfs.as_str())?,
                                    None,
                                    libc::MS_BIND | libc::MS_REMOUNT | libc::MS_RDONLY,
                                    None,
                                )?;
                            }
                            for mount in config.mounts.iter() {
                                let source = CString::new(mount.source.clone())?;
                                let target_path = format!("{}{}", &rootfs, &mount.destination);
                                let target = CString::new(target_path.as_str())?;
                                let fstype = CString::new(mount.mount_type.as_str())?;
                                let flags =
                                    mount.options.as_deref().map(get_mount_flags).unwrap_or(0);
                                let data = mount
                                    .options
                                    .as_deref()
                                    .and_then(get_mount_data)
                                    .map(CString::new)
                                    .transpose()?;
                                do_mount(
                                    Some(&source),
                                    &target,
                                    Some(&fstype),
                                    flags,
                                    data.as_ref(),
                                )?;
                                mount_guard.register(PathBuf::from(target_path.clone()));
                                if config.root.readonly
                                    && mount.mount_type == "bind"
                                    && (flags & libc::MS_RDONLY == 0)
                                {
                                    debug!(
                                        "Remounting bind {} as readonly inside readonly root",
                                        mount.destination
                                    );
                                    do_mount(
                                        Some(&source),
                                        &target,
                                        Some(&fstype),
                                        flags | libc::MS_BIND | libc::MS_REMOUNT | libc::MS_RDONLY,
                                        None,
                                    )?;
                                }
                            }

                            let links = vec![
                                (Path::new(&rootfs).join("dev/fd"), "/proc/self/fd"),
                                (Path::new(&rootfs).join("dev/stdin"), "/proc/self/fd/0"),
                                (Path::new(&rootfs).join("dev/stdout"), "/proc/self/fd/1"),
                                (Path::new(&rootfs).join("dev/stderr"), "/proc/self/fd/2"),
                                (Path::new(&rootfs).join("dev/ptmx"), "/dev/pts/ptmx"),
                            ];
                            for (link, target) in links {
                                if !link.exists() {
                                    std::os::unix::fs::symlink(target, link)?;
                                }
                            }

                            info!("Pivoting root");
                            pivot_rootfs(&rootfs)?;
                            mount_guard.disarm();

                            info!("Setting hostname: {}", hostname);
                            nix::unistd::sethostname(&hostname)?;
                            debug!("Hostname is now set");

                            nix::unistd::dup2_stdout(&log_out_w)?;
                            nix::unistd::dup2_stderr(&log_err_w)?;
                            drop(log_out_w);
                            drop(log_err_w);
                            Ok(())
                        })();

                        let init_status = if setup_result.is_ok() {
                            INIT_READY
                        } else {
                            INIT_FAILED
                        };
                        if let Err(err) = nix::unistd::write(&child_write, &[init_status]) {
                            error!("Failed to report container init status: {}", err);
                            std::mem::drop(mount_guard);
                            exit(1);
                        }

                        if let Err(err) = setup_result {
                            error!("Container namespace setup failed: {}", err);
                            std::mem::drop(mount_guard);
                            exit(1);
                        }

                        if let Err(err) = nix::unistd::close(child_write) {
                            error!("Failed to close init status pipe: {}", err);
                            std::mem::drop(mount_guard);
                            exit(1);
                        }

                        debug!("Waiting for start command");
                        let mut buf = [0u8; 1];
                        nix::unistd::read(&start_reader, &mut buf)
                            .map_err(|e| format!("start pipe read failed: {}", e))?;
                        info!("Start command received");

                        init_process(uid, gid, &cwd, &args, &proc_env)?;

                        exit(0);
                    }
                    Err(err) => {
                        error!("Fork for container init process failed: {}", err);
                        exit(1);
                    }
                }
            }
            Err(err) => {
                error!("Fork for intermediate process failed: {}", err);
                return Err(Box::new(io::Error::other(format!(
                    "Failed to fork intermediate process: {}",
                    err
                ))));
            }
        }
        for guard in &mut encrypted_guards {
            guard.disarm();
        }
        Ok(())
    }

    pub fn recreate(container_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        if !geteuid().is_root() {
            return Err("Must be run as root".into());
        }

        let base = container_root_path(container_id);
        if !base.exists() {
            return Err(format!("Container {} not found", container_id).into());
        }

        let config_path = base.join("config.json");
        let file = File::open(&config_path)?;
        let reader = BufReader::new(file);
        let config: OciConfig = serde_json::from_reader(reader)?;
        let bundle = base.to_string_lossy().to_string();

        Runtime::spawn_container(container_id, &bundle, config)
    }

    pub async fn kill(container_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let pid_path = container_root_path(container_id).join("pid");
        let pid = std::fs::read_to_string(&pid_path)?;
        let timeout = Duration::from_secs(10);

        match terminate_with_timeout(&pid, timeout).await {
            Ok(_) => {
                info!("Container {} stopped successfully", container_id);
                Runtime::set_status(container_id, ContainerStatus::Stopped)?;
                drop_start_pipe(container_id)?;
            }
            Err(e) => {
                error!("Failed to stop container {}: {}", container_id, e);
                std::process::exit(1);
            }
        }

        Ok(())
    }

    pub fn delete(
        container_name: &str,
        container_id: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        drop_start_pipe(container_id)?;
        // TODO: get container name from runtime - we should only need the container_id here
        let root = container_root_path(container_id);
        let config_path = root.join("config.json");
        let encrypted_volumes = match File::open(&config_path) {
            Ok(file) => serde_json::from_reader::<_, OciConfig>(BufReader::new(file))
                .map(|config| config.encrypted_volumes)
                .unwrap_or_default(),
            Err(_) => Vec::new(),
        };

        let rootfs_path = root.join("rootfs");
        match umount2(
            rootfs_path.to_string_lossy().as_ref(),
            nix::mount::MntFlags::MNT_DETACH,
        ) {
            Ok(()) => {}
            Err(err) if err == Errno::EINVAL || err == Errno::ENOENT => {}
            Err(err) => return Err(Box::new(err)),
        }

        teardown_encrypted_volumes(&encrypted_volumes)?;
        std::fs::remove_dir_all(&root)?;
        let refs_path = container_refs_dir().join(container_name);
        std::fs::remove_file(refs_path)?;

        let netns_path = netns_dir().join(container_id);
        if let Err(err) = std::fs::remove_file(&netns_path) {
            if err.kind() != ErrorKind::NotFound {
                warn!(
                    "Failed to remove network namespace link for {}: {}",
                    container_id, err
                );
            }
        }

        Ok(())
    }

    pub fn state(container_id: &str) -> Result<ContainerState, Box<dyn Error + Send + Sync>> {
        let name_map = read_container_name_map()?;
        collect_container_state(container_id, &name_map)
    }

    pub fn list() -> Result<Vec<ContainerSummary>, Box<dyn Error + Send + Sync>> {
        let name_map = read_container_name_map()?;
        let base_dir = container_base_dir();
        let mut results = Vec::new();

        let entries = match fs::read_dir(&base_dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
            Err(err) => return Err(Box::new(err)),
        };

        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let container_id = entry.file_name().to_string_lossy().to_string();
            match collect_container_state(&container_id, &name_map) {
                Ok(state) => results.push(ContainerSummary::from(state)),
                Err(err) => {
                    warn!("Failed to read state for {}: {}", container_id, err);
                }
            }
        }

        results.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(results)
    }

    pub fn send_start(container_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        match take_start_pipe_fd(container_id)? {
            Some(fd) => {
                nix::unistd::write(&fd, &[1]).map_err(|e| {
                    Box::new(io::Error::other(format!(
                        "failed to send start signal: {}",
                        e
                    ))) as Box<dyn Error + Send + Sync>
                })?;
                drop(fd);
                Ok(())
            }
            None => Err(io::Error::new(
                ErrorKind::NotFound,
                format!("start control for {} is not available", container_id),
            )
            .into()),
        }
    }

    pub fn set_status(
        container_id: &str,
        status: ContainerStatus,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        write_container_status(container_id, &status)
    }

    pub fn with_namespace<F>(container_id: &str, f: F) -> Result<(), Box<dyn Error + Send + Sync>>
    where
        F: FnOnce() -> Result<(), Box<dyn Error + Send + Sync>>,
    {
        if !nix::unistd::geteuid().is_root() {
            return Err("Must be run as root".into());
        }

        reset_exec_proc_mount_status();

        let config_path = container_root_path(container_id).join("config.json");
        let file = File::open(&config_path)?;
        let reader = BufReader::new(file);
        let config: OciConfig = serde_json::from_reader(reader)?;

        let (parent_read, child_write) = nix::unistd::pipe()?;

        match unsafe { nix::unistd::fork() } {
            Ok(ForkResult::Parent { child }) => {
                nix::unistd::close(child_write)?;

                let buf = match read_exec_status_bytes(
                    &parent_read,
                    "exec namespace setup pipe closed before status received",
                ) {
                    Ok(buf) => buf,
                    Err(err) => {
                        let _ = nix::sys::wait::waitpid(child, None);
                        return Err(Box::new(err));
                    }
                };
                nix::unistd::close(parent_read)?;

                if buf[0] == EXEC_STATUS_SUCCESS {
                    match buf[1] {
                        EXEC_PROC_MOUNTED => record_exec_proc_mount_status(true),
                        EXEC_PROC_NOT_MOUNTED => record_exec_proc_mount_status(false),
                        EXEC_PROC_UNKNOWN => {}
                        other => {
                            warn!(
                                "Received unexpected proc mount status marker {} for {}",
                                other, container_id
                            );
                        }
                    }
                }

                match nix::sys::wait::waitpid(child, None)? {
                    nix::sys::wait::WaitStatus::Exited(_, 0) => {}
                    nix::sys::wait::WaitStatus::Exited(_, code) => {
                        return Err(Box::new(io::Error::other(format!(
                            "exec helper exited with status {}",
                            code
                        ))));
                    }
                    nix::sys::wait::WaitStatus::Signaled(_, sig, _) => {
                        return Err(Box::new(io::Error::other(format!(
                            "exec helper killed by signal {}",
                            sig
                        ))));
                    }
                    status => {
                        return Err(Box::new(io::Error::other(format!(
                            "unexpected wait status for exec helper: {:?}",
                            status
                        ))));
                    }
                }

                if buf[0] == EXEC_STATUS_FAILURE {
                    return Err(Box::new(io::Error::other(
                        "namespace entry failed for exec action",
                    )));
                }
            }
            Ok(ForkResult::Child) => {
                nix::unistd::close(parent_read)?;

                let (result_read, result_write) = match nix::unistd::pipe() {
                    Ok(pair) => pair,
                    Err(err) => {
                        error!("Failed to create exec namespace status pipe: {}", err);
                        let _ = nix::unistd::write(
                            &child_write,
                            &[EXEC_STATUS_FAILURE, EXEC_PROC_UNKNOWN],
                        );
                        exit(1);
                    }
                };
                let mut action = Some(f);

                let namespace_result = (|| -> Result<(), Box<dyn Error + Send + Sync>> {
                    if config
                        .linux
                        .namespaces
                        .iter()
                        .any(|ns| ns.ns_type == "network")
                    {
                        enter_network_namespace(container_id, &config.linux.namespaces)?;
                    } else {
                        debug!(
                            "Host networking enabled for {}; skipping network namespace entry",
                            container_id
                        );
                    }

                    enter_namespace(container_id, "pid")?;
                    enter_namespace(container_id, "uts")?;
                    enter_namespace(container_id, "ipc")?;
                    enter_namespace(container_id, "mnt")?;
                    Ok(())
                })();

                if let Err(err) = namespace_result {
                    error!(
                        "Exec namespace setup failed inside container {}: {}",
                        container_id, err
                    );
                    let _ = nix::unistd::close(result_read);
                    let _ = nix::unistd::close(result_write);
                    let _ =
                        nix::unistd::write(&child_write, &[EXEC_STATUS_FAILURE, EXEC_PROC_UNKNOWN]);
                    exit(1);
                }

                match unsafe { nix::unistd::fork() } {
                    Ok(ForkResult::Parent { child }) => {
                        let _ = nix::unistd::close(result_write);

                        let mut status_buf = match read_exec_status_bytes(
                            &result_read,
                            "exec namespace status pipe closed before status received",
                        ) {
                            Ok(buf) => buf,
                            Err(err) => {
                                error!(
                                    "Failed to read exec status from namespace child for {}: {}",
                                    container_id, err
                                );
                                [EXEC_STATUS_FAILURE, EXEC_PROC_UNKNOWN]
                            }
                        };
                        let _ = nix::unistd::close(result_read);

                        let wait_status = match nix::sys::wait::waitpid(child, None) {
                            Ok(status) => status,
                            Err(err) => {
                                error!(
                                    "Failed to wait for namespace exec child for {}: {}",
                                    container_id, err
                                );
                                status_buf[0] = EXEC_STATUS_FAILURE;
                                status_buf[1] = EXEC_PROC_UNKNOWN;
                                let _ = nix::unistd::write(&child_write, &status_buf);
                                exit(1);
                            }
                        };

                        let child_success =
                            matches!(wait_status, nix::sys::wait::WaitStatus::Exited(_, 0));
                        if !child_success {
                            status_buf[0] = EXEC_STATUS_FAILURE;
                        }

                        if let Err(err) = nix::unistd::write(&child_write, &status_buf) {
                            error!("Failed to report exec status: {}", err);
                            exit(1);
                        }

                        if !child_success {
                            match wait_status {
                                nix::sys::wait::WaitStatus::Exited(_, code) => {
                                    error!("Namespace exec child exited with status {}", code);
                                }
                                nix::sys::wait::WaitStatus::Signaled(_, sig, _) => {
                                    error!("Namespace exec child killed by signal {}", sig);
                                }
                                other => {
                                    error!(
                                        "Unexpected wait status for namespace exec child: {:?}",
                                        other
                                    );
                                }
                            }
                            exit(1);
                        }

                        if status_buf[0] != EXEC_STATUS_SUCCESS {
                            exit(1);
                        }

                        exit(0);
                    }
                    Ok(ForkResult::Child) => {
                        let _ = nix::unistd::close(result_read);

                        let action_result = (|| -> Result<(), Box<dyn Error + Send + Sync>> {
                            reset_environment(&config.process.env)?;
                            match action.take() {
                                Some(closure) => closure(),
                                None => Err(Box::new(io::Error::other("exec closure missing"))
                                    as Box<dyn Error + Send + Sync>),
                            }
                        })();

                        let status_byte = if action_result.is_ok() {
                            EXEC_STATUS_SUCCESS
                        } else {
                            EXEC_STATUS_FAILURE
                        };
                        let message = [status_byte, EXEC_PROC_UNKNOWN];
                        if let Err(err) = nix::unistd::write(&result_write, &message) {
                            error!("Failed to report exec status: {}", err);
                            exit(1);
                        }
                        let _ = nix::unistd::close(result_write);

                        if let Err(err) = action_result {
                            error!("Exec action failed inside container namespace: {}", err);
                            exit(1);
                        }

                        exit(0);
                    }
                    Err(err) => {
                        error!("Fork for namespace exec child failed: {}", err);
                        let _ = nix::unistd::close(result_read);
                        let _ = nix::unistd::close(result_write);
                        let _ = nix::unistd::write(
                            &child_write,
                            &[EXEC_STATUS_FAILURE, EXEC_PROC_UNKNOWN],
                        );
                        exit(1);
                    }
                }
            }
            Err(err) => {
                error!("Fork for exec helper process failed: {}", err);
                return Err(Box::new(io::Error::other(format!(
                    "Failed to fork exec helper: {}",
                    err
                ))));
            }
        }

        Ok(())
    }

    pub fn exec<F>(
        container_id: &str,
        prepare: F,
    ) -> Result<ExecResult, Box<dyn Error + Send + Sync>>
    where
        F: FnOnce() -> Result<ExecRequest, Box<dyn Error + Send + Sync>>,
    {
        if !nix::unistd::geteuid().is_root() {
            return Err("Must be run as root".into());
        }

        reset_exec_proc_mount_status();

        let config_path = container_root_path(container_id).join("config.json");
        let file = File::open(&config_path)?;
        let reader = BufReader::new(file);
        let config: OciConfig = serde_json::from_reader(reader)?;

        let (parent_read, child_write) = nix::unistd::pipe()?;
        let mut prepare_opt = Some(prepare);

        match unsafe { nix::unistd::fork() } {
            Ok(ForkResult::Parent { child }) => {
                drop(child_write);
                let status_buf = match read_exec_status_bytes(
                    &parent_read,
                    "exec namespace setup pipe closed before status received",
                ) {
                    Ok(buf) => buf,
                    Err(err) => {
                        drop(parent_read);
                        let _ = nix::sys::wait::waitpid(child, None);
                        return Err(Box::new(err));
                    }
                };
                drop(parent_read);

                match status_buf[1] {
                    EXEC_PROC_MOUNTED => {
                        record_exec_proc_mount_status(true);
                    }
                    EXEC_PROC_NOT_MOUNTED => {
                        record_exec_proc_mount_status(false);
                    }
                    EXEC_PROC_UNKNOWN => {}
                    other => {
                        warn!(
                            "Received unexpected proc mount status marker {} for {}",
                            other, container_id
                        );
                    }
                }

                if status_buf[0] == EXEC_STATUS_FAILURE {
                    let _ = nix::sys::wait::waitpid(child, None);
                    return Err(Box::new(io::Error::other(
                        "namespace entry failed for exec action",
                    )));
                }

                let wait_status = nix::sys::wait::waitpid(child, None)?;
                Ok(ExecResult { wait_status })
            }
            Ok(ForkResult::Child) => {
                drop(parent_read);
                let child_write_fd = child_write;

                let namespace_result = (|| -> Result<(), Box<dyn Error + Send + Sync>> {
                    if config
                        .linux
                        .namespaces
                        .iter()
                        .any(|ns| ns.ns_type == "network")
                    {
                        enter_network_namespace(container_id, &config.linux.namespaces)?;
                    } else {
                        debug!(
                            "Host networking enabled for {}; skipping network namespace entry",
                            container_id
                        );
                    }

                    enter_namespace(container_id, "pid")?;
                    enter_namespace(container_id, "uts")?;
                    enter_namespace(container_id, "ipc")?;
                    enter_namespace(container_id, "mnt")?;
                    Ok(())
                })();

                if let Err(err) = namespace_result {
                    error!(
                        "Exec namespace setup failed inside container {}: {}",
                        container_id, err
                    );
                    let _ = nix::unistd::write(
                        &child_write_fd,
                        &[EXEC_STATUS_FAILURE, EXEC_PROC_UNKNOWN],
                    );
                    exit(1);
                }

                if let Err(err) = reset_environment(&config.process.env) {
                    error!(
                        "Failed to apply container environment before exec in {}: {}",
                        container_id, err
                    );
                    let _ = nix::unistd::write(
                        &child_write_fd,
                        &[EXEC_STATUS_FAILURE, EXEC_PROC_UNKNOWN],
                    );
                    exit(1);
                }

                let exec_request = match prepare_opt.take() {
                    Some(prepare_fn) => match prepare_fn() {
                        Ok(req) => req,
                        Err(err) => {
                            error!(
                                "Exec preparation failed inside container {}: {}",
                                container_id, err
                            );
                            let _ = nix::unistd::write(
                                child_write_fd,
                                &[EXEC_STATUS_FAILURE, EXEC_PROC_UNKNOWN],
                            );
                            exit(1);
                        }
                    },
                    None => {
                        error!(
                            "Exec preparation closure missing for container {}",
                            container_id
                        );
                        let _ = nix::unistd::write(
                            &child_write_fd,
                            &[EXEC_STATUS_FAILURE, EXEC_PROC_UNKNOWN],
                        );
                        exit(1);
                    }
                };

                let ExecRequest {
                    program,
                    args,
                    env: env_override,
                } = exec_request;

                let env_vars = if let Some(env_values) = env_override {
                    if let Err(err) = reset_environment(&env_values) {
                        error!(
                            "Failed to apply exec-specific environment inside {}: {}",
                            container_id, err
                        );
                        let _ = nix::unistd::write(
                            child_write_fd,
                            &[EXEC_STATUS_FAILURE, EXEC_PROC_UNKNOWN],
                        );
                        exit(1);
                    }
                    env_values
                } else {
                    config.process.env.clone()
                };
                let exec_uid = config.process.user.uid;
                let exec_gid = config.process.user.gid;
                let exec_cwd = config.process.cwd.clone();

                let program_cstring = match CString::new(program.as_str()) {
                    Ok(value) => value,
                    Err(err) => {
                        error!(
                            "Exec command contains invalid bytes for {}: {}",
                            container_id, err
                        );
                        let _ = nix::unistd::write(
                            child_write_fd,
                            &[EXEC_STATUS_FAILURE, EXEC_PROC_UNKNOWN],
                        );
                        exit(1);
                    }
                };

                let mut arg_cstrings = Vec::with_capacity(1 + args.len());
                arg_cstrings.push(program_cstring.clone());
                for arg in args {
                    match CString::new(arg.as_str()) {
                        Ok(value) => arg_cstrings.push(value),
                        Err(err) => {
                            error!(
                                "Exec argument contains invalid bytes for {}: {}",
                                container_id, err
                            );
                            let _ = nix::unistd::write(
                                child_write_fd,
                                &[EXEC_STATUS_FAILURE, EXEC_PROC_UNKNOWN],
                            );
                            exit(1);
                        }
                    }
                }
                let arg_ptrs: Vec<&CStr> =
                    arg_cstrings.iter().map(|value| value.as_c_str()).collect();

                let mut env_cstrings = Vec::with_capacity(env_vars.len());
                for entry in &env_vars {
                    match CString::new(entry.as_str()) {
                        Ok(value) => env_cstrings.push(value),
                        Err(err) => {
                            error!(
                                "Exec environment value contains invalid bytes for {}: {}",
                                container_id, err
                            );
                            let _ = nix::unistd::write(
                                child_write_fd,
                                &[EXEC_STATUS_FAILURE, EXEC_PROC_UNKNOWN],
                            );
                            exit(1);
                        }
                    }
                }
                let env_ptrs: Vec<&CStr> =
                    env_cstrings.iter().map(|value| value.as_c_str()).collect();

                match unsafe { nix::unistd::fork() } {
                    Ok(ForkResult::Parent { child }) => {
                        let success_message = [EXEC_STATUS_SUCCESS, EXEC_PROC_UNKNOWN];
                        if let Err(err) = nix::unistd::write(&child_write_fd, &success_message) {
                            error!(
                                "Failed to report namespace success for {}: {}",
                                container_id, err
                            );
                            exit(1);
                        }
                        drop(child_write_fd);

                        match nix::sys::wait::waitpid(child, None) {
                            Ok(nix::sys::wait::WaitStatus::Exited(_, code)) => unsafe {
                                libc::_exit(code);
                            },
                            Ok(nix::sys::wait::WaitStatus::Signaled(_, sig, _)) => unsafe {
                                libc::_exit(128 + sig as i32);
                            },
                            Ok(other) => {
                                error!(
                                    "Unexpected wait status for exec child in {}: {:?}",
                                    container_id, other
                                );
                                unsafe {
                                    libc::_exit(1);
                                }
                            }
                            Err(err) => {
                                error!(
                                    "Failed to wait for exec child in {}: {}",
                                    container_id, err
                                );
                                unsafe {
                                    libc::_exit(1);
                                }
                            }
                        }
                    }
                    Ok(ForkResult::Child) => {
                        drop(child_write_fd);
                        if let Err(err) = adjust_stdio_ownership(exec_uid, exec_gid) {
                            error!(
                                "Failed to adjust exec stdio ownership inside {}: {}",
                                container_id, err
                            );
                            unsafe {
                                libc::_exit(1);
                            }
                        }
                        if let Err(err) = setgid(Gid::from_raw(exec_gid)) {
                            error!(
                                "Failed to setgid to {} inside container {}: {}",
                                exec_gid, container_id, err
                            );
                            unsafe {
                                libc::_exit(1);
                            }
                        }
                        if let Err(err) = setuid(Uid::from_raw(exec_uid)) {
                            error!(
                                "Failed to setuid to {} inside container {}: {}",
                                exec_uid, container_id, err
                            );
                            unsafe {
                                libc::_exit(1);
                            }
                        }
                        if unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 1, 0, 0, 0) } != 0 {
                            let err = io::Error::last_os_error();
                            error!(
                                "Failed to mark exec process dumpable inside {}: {}",
                                container_id, err
                            );
                            unsafe {
                                libc::_exit(1);
                            }
                        }
                        if let Err(err) = chdir(exec_cwd.as_str()) {
                            error!(
                                "Failed to change directory to {} inside container {}: {}",
                                exec_cwd, container_id, err
                            );
                            unsafe {
                                libc::_exit(1);
                            }
                        }
                        match nix::unistd::execvpe(arg_ptrs[0], &arg_ptrs, &env_ptrs) {
                            Ok(_) => unreachable!("execvpe should not return on success"),
                            Err(err) => {
                                error!("Execvpe failed inside container {}: {}", container_id, err);
                                unsafe {
                                    libc::_exit(127);
                                }
                            }
                        }
                    }
                    Err(err) => {
                        error!("Fork for exec command failed in {}: {}", container_id, err);
                        let _ = nix::unistd::write(
                            &child_write_fd,
                            &[EXEC_STATUS_FAILURE, EXEC_PROC_UNKNOWN],
                        );
                        drop(child_write_fd);
                        exit(1);
                    }
                }
            }
            Err(err) => {
                error!("Fork for exec helper process failed: {}", err);
                Err(Box::new(io::Error::other(format!(
                    "Failed to fork exec helper: {}",
                    err
                ))))
            }
        }
    }

    pub fn take_exec_proc_mount_status() -> Option<bool> {
        take_exec_proc_mount_status_internal()
    }
}

fn read_exec_status_bytes(fd: &OwnedFd, eof_message: &str) -> io::Result<[u8; 2]> {
    let mut buf = [0u8; 2];
    let mut total_read = 0usize;
    while total_read < buf.len() {
        match nix::unistd::read(fd, &mut buf[total_read..]) {
            Ok(0) => {
                return Err(io::Error::other(eof_message.to_owned()));
            }
            Ok(n) => total_read += n,
            Err(err) => {
                return Err(io::Error::from(err));
            }
        }
    }
    Ok(buf)
}

fn reset_environment(entries: &[String]) -> Result<(), Box<dyn Error + Send + Sync>> {
    let existing: Vec<String> = env::vars().map(|(key, _)| key).collect();
    for key in existing {
        env::remove_var(key);
    }

    for entry in entries {
        if entry.is_empty() {
            continue;
        }
        if let Some((key, value)) = entry.split_once('=') {
            if !key.is_empty() {
                env::set_var(key, value);
            }
        } else {
            env::set_var(entry, "");
        }
    }

    Ok(())
}

fn map_container_resources(spec: &ContainerResources) -> Option<LinuxResources> {
    let cpu = if spec.cpu_shares.is_some() || spec.cpu_quota.is_some() || spec.cpu_period.is_some()
    {
        Some(LinuxCpu {
            shares: spec.cpu_shares,
            quota: spec.cpu_quota,
            period: spec.cpu_period,
        })
    } else {
        None
    };

    let memory = if spec.memory_limit.is_some() || spec.memory_swap.is_some() {
        Some(LinuxMemory {
            limit: spec.memory_limit,
            swap: spec.memory_swap,
        })
    } else {
        None
    };

    let pids = spec.pids_limit.map(|limit| LinuxPids { limit });

    if cpu.is_none() && memory.is_none() && pids.is_none() {
        None
    } else {
        Some(LinuxResources { cpu, memory, pids })
    }
}

fn resolve_process_user(user_spec: Option<&str>) -> (u32, u32) {
    const DEFAULT_UID: u32 = 1000;
    const DEFAULT_GID: u32 = 1000;

    let spec = match user_spec {
        Some(value) => value.trim(),
        None => return (DEFAULT_UID, DEFAULT_GID),
    };

    if spec.is_empty() {
        return (DEFAULT_UID, DEFAULT_GID);
    }

    if spec.eq_ignore_ascii_case("root") {
        return (0, 0);
    }

    let mut parts = spec.splitn(2, ':');
    let user_part = parts.next().unwrap_or("");
    let group_part = parts.next();

    let mut resolved_uid = DEFAULT_UID;
    let mut fallback_gid = DEFAULT_GID;

    let user_lookup = if !user_part.is_empty() {
        resolve_user_component(user_part)
    } else {
        None
    };

    if let Some((uid, primary_gid)) = user_lookup {
        resolved_uid = uid;
        if let Some(gid) = primary_gid {
            fallback_gid = gid;
        }
    }

    let resolved_gid = match group_part {
        Some(group) if !group.is_empty() => resolve_group_component(group).unwrap_or(fallback_gid),
        _ => user_lookup.and_then(|(_, gid)| gid).unwrap_or(fallback_gid),
    };

    (resolved_uid, resolved_gid)
}

fn resolve_user_component(value: &str) -> Option<(u32, Option<u32>)> {
    if let Ok(uid) = value.parse::<u32>() {
        return Some((uid, None));
    }

    match nix::unistd::User::from_name(value) {
        Ok(Some(user)) => Some((user.uid.as_raw(), Some(user.gid.as_raw()))),
        Ok(None) | Err(_) => None,
    }
}

fn resolve_group_component(value: &str) -> Option<u32> {
    if let Ok(gid) = value.parse::<u32>() {
        return Some(gid);
    }

    match nix::unistd::Group::from_name(value) {
        Ok(Some(group)) => Some(group.gid.as_raw()),
        Ok(None) | Err(_) => None,
    }
}

fn read_container_name_map() -> io::Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    let refs_dir = container_refs_dir();
    let entries = match fs::read_dir(&refs_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(map),
        Err(err) => return Err(err),
    };

    for entry_result in entries {
        let entry = match entry_result {
            Ok(entry) => entry,
            Err(_) => continue,
        };

        let entry_path = entry.path();
        let metadata = match fs::symlink_metadata(&entry_path) {
            Ok(meta) => meta,
            Err(_) => continue,
        };

        if !metadata.file_type().is_symlink() {
            continue;
        }

        if let Ok(target) = fs::read_link(&entry_path) {
            if let Some(id_os) = target.file_name() {
                if let Some(id) = id_os.to_str() {
                    map.insert(
                        id.to_string(),
                        entry.file_name().to_string_lossy().to_string(),
                    );
                }
            }
        }
    }

    Ok(map)
}

fn collect_container_state(
    container_id: &str,
    name_map: &HashMap<String, String>,
) -> Result<ContainerState, Box<dyn Error + Send + Sync>> {
    let base = container_root_path(container_id);
    if !base.exists() {
        return Err(format!("Container {} not found", container_id).into());
    }

    let config_path = base.join("config.json");
    let config = match File::open(&config_path) {
        Ok(file) => Some(serde_json::from_reader::<_, OciConfig>(BufReader::new(
            file,
        ))?),
        Err(err) if err.kind() == ErrorKind::NotFound => None,
        Err(err) => return Err(Box::new(err)),
    };

    let pid_path = base.join("pid");
    let pid = read_pid(&pid_path)?;

    let recorded_status = read_container_status(&base);
    let (status, status_detail) = determine_status(pid, config.is_some(), recorded_status);

    let created_at = fs::metadata(&pid_path)
        .ok()
        .and_then(|meta| metadata_to_rfc3339(&meta))
        .or_else(|| {
            fs::metadata(&base)
                .ok()
                .and_then(|meta| metadata_to_rfc3339(&meta))
        });

    let image_digest = fs::read_to_string(base.join("image_digest"))
        .ok()
        .map(|s| s.trim().to_string());

    let hostname = config.as_ref().map(|cfg| cfg.hostname.clone());
    let command = config
        .as_ref()
        .map(|cfg| cfg.process.args.clone())
        .unwrap_or_default();
    let environment = config
        .as_ref()
        .map(|cfg| parse_env(&cfg.process.env))
        .unwrap_or_default();

    let network = read_network_state(&base, container_id);

    Ok(ContainerState {
        id: container_id.to_string(),
        name: name_map.get(container_id).cloned(),
        pid,
        status,
        status_detail,
        bundle: base.to_string_lossy().to_string(),
        rootfs: base.join("rootfs").to_string_lossy().to_string(),
        config_path: config
            .as_ref()
            .map(|_| config_path.to_string_lossy().to_string()),
        created_at,
        started_at: None,
        finished_at: None,
        image_digest,
        hostname,
        command,
        environment,
        annotations: HashMap::new(),
        labels: HashMap::new(),
        runtime: "nanocloud".to_string(),
        network,
    })
}

fn read_pid(pid_path: &Path) -> Result<Option<i32>, Box<dyn Error + Send + Sync>> {
    if !pid_path.exists() {
        return Ok(None);
    }

    let raw = fs::read_to_string(pid_path)?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let pid = trimmed
        .parse::<i32>()
        .map_err(|e| format!("invalid pid in {}: {}", pid_path.display(), e))?;

    Ok(Some(pid))
}

fn determine_status(
    pid: Option<i32>,
    has_bundle: bool,
    recorded: Option<ContainerStatus>,
) -> (ContainerStatus, Option<String>) {
    let probed = match pid {
        Some(pid_num) => match is_gone_or_zombie(Pid::from_raw(pid_num)) {
            Ok(true) => (ContainerStatus::Stopped, None),
            Ok(false) => (ContainerStatus::Running, None),
            Err(e) => (
                ContainerStatus::Unknown,
                Some(format!("status probe failed: {}", e)),
            ),
        },
        None => {
            if has_bundle {
                (ContainerStatus::Created, None)
            } else {
                (
                    ContainerStatus::Unknown,
                    Some("bundle metadata missing".to_string()),
                )
            }
        }
    };

    match recorded {
        Some(ContainerStatus::Created) => match probed.0 {
            ContainerStatus::Stopped | ContainerStatus::Unknown => probed,
            _ => (ContainerStatus::Created, None),
        },
        Some(ContainerStatus::Running) => match probed.0 {
            ContainerStatus::Stopped | ContainerStatus::Unknown => probed,
            _ => (ContainerStatus::Running, None),
        },
        Some(ContainerStatus::Stopped) => (ContainerStatus::Stopped, None),
        Some(ContainerStatus::Paused) => (ContainerStatus::Paused, None),
        Some(ContainerStatus::Creating) => match probed.0 {
            ContainerStatus::Stopped | ContainerStatus::Unknown => probed,
            _ => (ContainerStatus::Creating, None),
        },
        Some(ContainerStatus::Unknown) => probed,
        None => probed,
    }
}

fn metadata_to_rfc3339(meta: &fs::Metadata) -> Option<String> {
    let secs = meta.ctime();
    let nanos = meta.ctime_nsec();
    let nanos = if nanos < 0 { 0 } else { nanos as u32 };
    Utc.timestamp_opt(secs, nanos)
        .single()
        .map(|dt| dt.to_rfc3339())
}

fn parse_env(entries: &[String]) -> HashMap<String, String> {
    let mut env_map = HashMap::new();
    for entry in entries {
        if entry.is_empty() {
            continue;
        }
        if let Some((key, value)) = entry.split_once('=') {
            env_map.insert(key.to_string(), value.to_string());
        } else {
            env_map.insert(entry.to_string(), String::new());
        }
    }
    env_map
}

fn read_network_state(base: &Path, container_id: &str) -> ContainerNetwork {
    let namespace_path = netns_dir().join(container_id);
    let namespace = namespace_path
        .exists()
        .then(|| namespace_path.to_string_lossy().to_string());

    let ip_path = base.join("network").join("ip_address");
    let mut ip_addresses = Vec::new();
    if let Ok(contents) = fs::read_to_string(ip_path) {
        for line in contents.lines() {
            let addr = line.trim();
            if !addr.is_empty() {
                ip_addresses.push(addr.to_string());
            }
        }
    }

    ContainerNetwork {
        namespace,
        ip_addresses,
    }
}

fn write_container_status(
    container_id: &str,
    status: &ContainerStatus,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let base = container_root_path(container_id);
    fs::create_dir_all(&base)?;
    let status_path = base.join("status");
    let payload = serde_json::to_string(status)?;
    fs::write(status_path, payload)?;
    Ok(())
}

fn read_container_status(base: &Path) -> Option<ContainerStatus> {
    let status_path = base.join("status");
    let raw = fs::read_to_string(status_path).ok()?;
    serde_json::from_str(&raw).ok()
}

static START_PIPES: OnceLock<Mutex<HashMap<String, OwnedFd>>> = OnceLock::new();
static EXEC_PROC_MOUNT_STATUS: OnceLock<Mutex<Option<bool>>> = OnceLock::new();

fn start_pipe_registry() -> &'static Mutex<HashMap<String, OwnedFd>> {
    START_PIPES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn exec_proc_mount_status_registry() -> &'static Mutex<Option<bool>> {
    EXEC_PROC_MOUNT_STATUS.get_or_init(|| Mutex::new(None))
}

fn reset_exec_proc_mount_status() {
    match exec_proc_mount_status_registry().lock() {
        Ok(mut guard) => *guard = None,
        Err(poisoned) => {
            *poisoned.into_inner() = None;
        }
    }
}

fn record_exec_proc_mount_status(value: bool) {
    match exec_proc_mount_status_registry().lock() {
        Ok(mut guard) => *guard = Some(value),
        Err(poisoned) => {
            *poisoned.into_inner() = Some(value);
        }
    }
}

fn take_exec_proc_mount_status_internal() -> Option<bool> {
    match exec_proc_mount_status_registry().lock() {
        Ok(mut guard) => guard.take(),
        Err(poisoned) => poisoned.into_inner().take(),
    }
}

fn register_start_pipe(
    container_id: &str,
    fd: OwnedFd,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut guard = start_pipe_registry().lock().map_err(|_| {
        Box::new(io::Error::other("start pipe registry poisoned")) as Box<dyn Error + Send + Sync>
    })?;
    if let Some(existing) = guard.insert(container_id.to_string(), fd) {
        drop(existing);
    }
    Ok(())
}

fn take_start_pipe_fd(container_id: &str) -> Result<Option<OwnedFd>, Box<dyn Error + Send + Sync>> {
    let fd = start_pipe_registry()
        .lock()
        .map_err(|_| {
            Box::new(io::Error::other("start pipe registry poisoned"))
                as Box<dyn Error + Send + Sync>
        })?
        .remove(container_id);
    Ok(fd)
}

fn drop_start_pipe(container_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Some(fd) = take_start_pipe_fd(container_id)? {
        drop(fd);
    }
    Ok(())
}

fn get_mount_flags(options: &[String]) -> u64 {
    options.iter().fold(0, |mut flags, opt| {
        match opt.as_str() {
            "ro" => flags |= libc::MS_RDONLY,
            "nosuid" => flags |= libc::MS_NOSUID,
            "noexec" => flags |= libc::MS_NOEXEC,
            "nodev" => flags |= libc::MS_NODEV,
            "dirsync" => flags |= libc::MS_DIRSYNC,
            "remount" => flags |= libc::MS_REMOUNT,
            "mand" => flags |= libc::MS_MANDLOCK,
            "bind" => flags |= libc::MS_BIND,
            "rbind" => flags |= libc::MS_BIND | libc::MS_REC,
            "move" => flags |= libc::MS_MOVE,
            "private" => flags |= libc::MS_PRIVATE,
            "shared" => flags |= libc::MS_SHARED,
            "slave" => flags |= libc::MS_SLAVE,
            "unbindable" => flags |= libc::MS_UNBINDABLE,
            "rec" => flags |= libc::MS_REC,
            "rprivate" => flags |= libc::MS_PRIVATE | libc::MS_REC,
            "rshared" => flags |= libc::MS_SHARED | libc::MS_REC,
            "rslave" => flags |= libc::MS_SLAVE | libc::MS_REC,
            "runbindable" => flags |= libc::MS_UNBINDABLE | libc::MS_REC,
            "strictatime" => flags |= libc::MS_STRICTATIME,
            "noatime" => flags |= libc::MS_NOATIME,
            "relatime" => flags |= libc::MS_RELATIME,
            "lazytime" => flags |= libc::MS_LAZYTIME,
            _ => {}
        }
        flags
    })
}

fn get_mount_data(options: &[String]) -> Option<String> {
    let keywords = [
        // devpts
        "newinstance",
        // tmpfs
        "size",
        "mode",
        "uid",
        "gid",
        "nr_inodes",
        "nr_blocks",
        "inode64",
        // cgroup subsystems
        "cpu",
        "memory",
        "cpuacct",
        "blkio",
        "devices",
        "freezer",
        "net_cls",
        "perf_event",
        "pids",
        "rdma",
        "hugetlb",
        "net_prio",
        // overlay
        "lowerdir",
        "upperdir",
        "workdir",
        // proc
        "hidepid",
    ];

    let v = options
        .iter()
        .filter(|s| s.contains('=') || keywords.contains(&s.as_str()))
        .cloned()
        .collect::<Vec<String>>();
    (!v.is_empty()).then(|| v.join(","))
}

#[doc(hidden)]
#[allow(dead_code)]
pub fn ensure_proc_mounted_for_testing(
    config: &OciConfig,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    match File::open("/proc/self/mountinfo") {
        Ok(_) => return Ok(false),
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(Box::new(err)),
    }

    let proc_mount = config
        .mounts
        .iter()
        .find(|mount| mount.destination == "/proc")
        .ok_or_else(|| {
            Box::new(io::Error::new(
                ErrorKind::NotFound,
                "OciConfig missing /proc mount entry",
            )) as Box<dyn Error + Send + Sync>
        })?;

    let target = CString::new(proc_mount.destination.as_str()).map_err(|err| {
        Box::new(io::Error::new(
            ErrorKind::InvalidInput,
            format!("invalid /proc destination: {}", err),
        )) as Box<dyn Error + Send + Sync>
    })?;
    let fstype = CString::new(proc_mount.mount_type.as_str()).map_err(|err| {
        Box::new(io::Error::new(
            ErrorKind::InvalidInput,
            format!("invalid /proc mount type: {}", err),
        )) as Box<dyn Error + Send + Sync>
    })?;
    let source = CString::new(proc_mount.source.as_str()).map_err(|err| {
        Box::new(io::Error::new(
            ErrorKind::InvalidInput,
            format!("invalid /proc source: {}", err),
        )) as Box<dyn Error + Send + Sync>
    })?;

    let options = proc_mount.options.clone().unwrap_or_default();
    let data_cstring = if let Some(data) = get_mount_data(&options) {
        Some(CString::new(data).map_err(|err| {
            Box::new(io::Error::new(
                ErrorKind::InvalidInput,
                format!("invalid /proc mount data: {}", err),
            )) as Box<dyn Error + Send + Sync>
        })?)
    } else {
        None
    };

    do_mount(
        Some(&source),
        &target,
        Some(&fstype),
        get_mount_flags(&options),
        data_cstring.as_ref(),
    )?;

    File::open("/proc/self/mountinfo").map_err(|err| {
        Box::new(io::Error::other(format!(
            "failed to read mountinfo after mounting /proc: {}",
            err
        ))) as Box<dyn Error + Send + Sync>
    })?;

    Ok(true)
}

fn enter_namespace(container_id: &str, ns: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let pid_path = container_root_path(container_id).join("pid");
    let pid = read_pid(&pid_path)?
        .ok_or_else(|| {
            io::Error::other(format!(
                "PID file missing or empty for container {}",
                container_id
            ))
        })
        .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
    let path = format!("/proc/{}/ns/{}", pid, ns);
    let file = File::open(path)?;
    let fd = unsafe { BorrowedFd::borrow_raw(file.as_raw_fd()) };
    setns(fd, CloneFlags::empty()).map_err(|e| format!("Can't set namespace: {:?}", e))?;
    Ok(())
}

fn enter_namespace_from_path(path: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let file = File::open(path).map_err(|err| {
        with_context(
            err,
            format!("Failed to open network namespace path {}", path),
        )
    })?;
    let fd = unsafe { BorrowedFd::borrow_raw(file.as_raw_fd()) };
    setns(fd, CloneFlags::empty())
        .map_err(|e| with_context(e, format!("Can't set network namespace from {}", path)))
}

fn enter_network_namespace(
    container_id: &str,
    namespaces: &[Namespace],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut attempt_messages = Vec::new();

    if let Some(config_path) = namespaces
        .iter()
        .find(|ns| ns.ns_type == "network")
        .and_then(|ns| ns.path.as_deref())
    {
        if let Err(err) = enter_namespace_from_path(config_path) {
            attempt_messages.push(format!("{}: {}", config_path, err));
        } else {
            return Ok(());
        }
    }

    let path = netns_dir().join(container_id);
    const RETRY_LIMIT: usize = 50;
    const RETRY_DELAY: Duration = Duration::from_millis(10);

    let mut last_error: Option<std::io::Error> = None;
    for _ in 0..RETRY_LIMIT {
        match File::open(&path) {
            Ok(file) => {
                let fd = unsafe { BorrowedFd::borrow_raw(file.as_raw_fd()) };
                setns(fd, CloneFlags::empty())
                    .map_err(|e| with_context(e, "Can't set network namespace"))?;
                return Ok(());
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                last_error = Some(err);
                thread::sleep(RETRY_DELAY);
            }
            Err(err) => {
                attempt_messages.push(format!("{}: {}", path.display(), err));
                let summary = attempt_messages.join(" | ");
                return Err(with_context(
                    err,
                    format!(
                        "Failed to enter network namespace for {} ({})",
                        container_id, summary
                    ),
                ));
            }
        }
    }

    let netns_err = last_error.unwrap_or_else(|| {
        std::io::Error::new(
            ErrorKind::NotFound,
            format!("network namespace link {} missing", path.display()),
        )
    });
    attempt_messages.push(format!(
        "{} after {} attempts: {}",
        path.display(),
        RETRY_LIMIT,
        netns_err
    ));

    let pid_path = container_root_path(container_id).join("pid");
    let pid = match read_pid(&pid_path)? {
        Some(pid) => pid,
        None => {
            let summary = attempt_messages.join(" | ");
            return Err(with_context(
                netns_err,
                format!(
                    "Failed to enter network namespace for {}; pid not recorded at {} ({})",
                    container_id,
                    pid_path.display(),
                    summary
                ),
            ));
        }
    };

    let fallback_path = format!("/proc/{}/ns/net", pid);
    match enter_namespace_from_path(&fallback_path) {
        Ok(()) => Ok(()),
        Err(err) => {
            attempt_messages.push(format!("{}: {}", fallback_path, err));
            let summary = attempt_messages.join(" | ");
            Err(with_context(
                err,
                format!(
                    "Failed to enter network namespace for {} ({})",
                    container_id, summary
                ),
            ))
        }
    }
}

fn link_network_namespace(
    container_id: &str,
    pid: i32,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Ensure the standard netns directory exists
    let netns_dir = netns_dir();
    create_dir_all(&netns_dir)?;

    let netns_src = format!("/proc/{}/ns/net", pid);
    let netns_link = netns_dir.join(container_id);

    match std::fs::symlink_metadata(&netns_link) {
        Ok(meta) => {
            if meta.file_type().is_dir() {
                return Err(format!(
                    "netns link path {} already exists as a directory",
                    netns_link.display()
                )
                .into());
            }

            if meta.file_type().is_symlink() {
                if let Ok(existing) = std::fs::read_link(&netns_link) {
                    if existing == PathBuf::from(&netns_src) {
                        return Ok(());
                    }
                }
                std::fs::remove_file(&netns_link)?;
            } else {
                // The namespace already has a persistent nsfs entry (created by ip netns add).
                // Reuse the existing path so we do not attempt to replace an active mount.
                return Ok(());
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(Box::new(err)),
    }

    symlink(netns_src, &netns_link)?;

    Ok(())
}

struct MountGuard {
    mounts: Vec<PathBuf>,
    armed: bool,
}

impl Default for MountGuard {
    fn default() -> Self {
        Self {
            mounts: Vec::new(),
            armed: true,
        }
    }
}

impl MountGuard {
    fn register<P>(&mut self, target: P)
    where
        P: AsRef<Path>,
    {
        if self.armed {
            self.mounts.push(target.as_ref().to_path_buf());
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
        self.mounts.clear();
    }
}

impl Drop for MountGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        for target in self.mounts.iter().rev() {
            if let Err(err) = umount2(target, nix::mount::MntFlags::MNT_DETACH) {
                debug!(
                    "Failed to unmount {} during cleanup: {}",
                    target.display(),
                    err
                );
            }
        }
    }
}

struct EncryptedVolumeGuard {
    mapper: String,
    host_mount: PathBuf,
    armed: bool,
}

impl EncryptedVolumeGuard {
    fn new(mapper: String, host_mount: PathBuf) -> Self {
        Self {
            mapper,
            host_mount,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for EncryptedVolumeGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        if let Err(err) = unmount_if_mounted(&self.host_mount) {
            warn!(
                "Failed to unmount encrypted volume mount {}: {}",
                self.host_mount.display(),
                err
            );
        }
        if let Err(err) = close_mapper(&self.mapper) {
            warn!("Failed to close encrypted mapper {}: {}", self.mapper, err);
        }
    }
}

fn do_mount(
    source: Option<&CString>,
    target: &CString,
    fstype: Option<&CString>,
    flags: u64,
    data: Option<&CString>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if log::log_enabled!(log::Level::Debug) {
        debug!(
            "Creating mount for source: {:?} target: {:?} fstype: {:?} flags: {} data: {:?}",
            source.map(|s| s.to_string_lossy()),
            target.to_string_lossy(),
            fstype.map(|f| f.to_string_lossy()),
            flags,
            data.map(|d| d.to_string_lossy())
        );
    }

    // let mut source_is_dir = true;
    let mut source_mode: Option<u32> = None;
    if let (Some(source), Some(fstype)) = (source, fstype) {
        if fstype.to_string_lossy() == "bind" {
            let source_path = source.to_string_lossy().to_string();
            if !Path::new(&source_path).exists() {
                debug!("Creating missing source directory: {}", &source_path);
                std::fs::create_dir_all(&source_path)?;
            }
        }

        let source_path = source.to_string_lossy().to_string();
        let source_path_obj = Path::new(&source_path);
        if source_path_obj.exists() && !source_path_obj.is_dir() {
            let src_metadata = std::fs::metadata(source_path.clone())?;
            source_mode = Some(src_metadata.mode() & 0o7777);
        }

        if source_path_obj.exists() && is_nanocloud_managed_path(source_path_obj) {
            let uid = Some(nix::unistd::Uid::from_raw(1000));
            let gid = Some(nix::unistd::Gid::from_raw(1000));
            nix::unistd::chown(source_path_obj, uid, gid)?;
        }
    }

    let target_path = target.to_string_lossy().to_string();
    if !Path::new(&target_path).exists() {
        if let Some(source_mode) = source_mode {
            debug!("Creating missing target file: {}", &target_path);
            std::fs::File::create(&target_path)?;
            std::fs::set_permissions(&target_path, std::fs::Permissions::from_mode(source_mode))?;
        } else {
            debug!("Creating missing target directory: {}", &target_path);
            std::fs::create_dir_all(&target_path)?;
        }
    }

    nix::mount::mount(
        source.map(|s| s.as_c_str()),
        target.as_c_str(),
        fstype.map(|s| s.as_c_str()),
        nix::mount::MsFlags::from_bits_truncate(flags),
        data.map(|s| s.as_c_str()),
    )
    .map_err(|e| format!("Mount failed for {:?}: {:?}", source, e))?;

    Ok(())
}

fn prepare_encrypted_volumes(
    volumes: &[EncryptedVolumeMount],
) -> Result<Vec<EncryptedVolumeGuard>, Box<dyn Error + Send + Sync>> {
    let mut guards = Vec::new();
    if volumes.is_empty() {
        return Ok(guards);
    }

    ensure_encrypted_volume_root()?;

    for volume in volumes {
        let key_bytes = read_volume_key(&volume.key_name)?;

        ensure_luks_device(&volume.device, &key_bytes)?;
        open_mapper(&volume.device, &volume.mapper, &key_bytes)?;

        let host_mount = PathBuf::from(&volume.host_mount);
        fs::create_dir_all(&host_mount).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to create encrypted volume mount directory {}",
                    host_mount.display()
                ),
            )
        })?;

        let filesystem = volume.filesystem.as_deref().unwrap_or("ext4");
        mount_mapper(&volume.mapper, &host_mount, filesystem)?;

        guards.push(EncryptedVolumeGuard::new(volume.mapper.clone(), host_mount));
    }

    Ok(guards)
}

fn teardown_encrypted_volumes(
    volumes: &[EncryptedVolumeMount],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    for volume in volumes {
        let host_mount = Path::new(&volume.host_mount);
        unmount_if_mounted(host_mount)?;
        cleanup_mount_dir(host_mount);
        close_mapper(&volume.mapper)?;
    }
    Ok(())
}

fn is_nanocloud_managed_path(path: &Path) -> bool {
    let container_root = container_base_dir();
    if path.starts_with(&container_root) {
        return true;
    }

    if let Ok(storage_root) = env::var("NANOCLOUD_STORAGE_ROOT") {
        let storage_path = PathBuf::from(storage_root);
        if path.starts_with(&storage_path) {
            return true;
        }
    }

    path.starts_with(Path::new("/var/lib/nanocloud.io"))
}

fn pivot_rootfs(new_root: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let new_root_path = Path::new(new_root);
    if !is_mount_point(new_root_path)? {
        return Err(format!("new root {} is not a mount point", new_root).into());
    }

    let put_old_path = format!("{}/.old_root", new_root);
    create_dir_all(&put_old_path)?;

    match nix::unistd::pivot_root(new_root_path, Path::new(&put_old_path)) {
        Ok(()) => {
            chdir("/")?;
            umount2("/.old_root", nix::mount::MntFlags::MNT_DETACH)?;
            remove_dir_all("/.old_root")?;
            Ok(())
        }
        Err(err) if matches!(err, Errno::EXDEV | Errno::EINVAL) => {
            remove_dir_all(&put_old_path).ok();
            warn!("pivot_root unsupported, falling back to MS_MOVE: {}", err);
            fallback_move_root(new_root_path)
        }
        Err(err) => {
            remove_dir_all(&put_old_path).ok();
            Err(format!("pivot_root failed for {}: {}", new_root, err).into())
        }
    }
}

fn adjust_stdio_ownership(uid: u32, gid: u32) -> Result<(), Box<dyn Error + Send + Sync>> {
    for fd_raw in [libc::STDIN_FILENO, libc::STDOUT_FILENO, libc::STDERR_FILENO] {
        let stats = fstat(unsafe { BorrowedFd::borrow_raw(fd_raw) })
            .map_err(|err| Box::new(io::Error::from(err)) as Box<dyn Error + Send + Sync>)?;
        if stats.st_uid == uid && stats.st_gid == gid {
            continue;
        }
        match fchown(
            unsafe { BorrowedFd::borrow_raw(fd_raw) },
            Some(Uid::from_raw(uid)),
            Some(Gid::from_raw(gid)),
        ) {
            Ok(()) => {}
            Err(Errno::EPERM) | Err(Errno::EROFS) => continue,
            Err(other) => {
                return Err(Box::new(io::Error::from(other)) as Box<dyn Error + Send + Sync>);
            }
        }
    }
    Ok(())
}

fn init_process(
    uid: u32,
    gid: u32,
    cwd: &str,
    args: &[CString],
    env: &[CString],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    adjust_stdio_ownership(uid, gid)?;
    debug!("Setting uid={}, gid={}", uid, gid);
    setgid(Gid::from_raw(gid))?;
    setuid(Uid::from_raw(uid))?;
    if unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 1, 0, 0, 0) } != 0 {
        return Err(Box::new(io::Error::last_os_error()));
    }

    debug!("Changing directory to {}", cwd);
    chdir(cwd)?;

    let prog = args.first().ok_or("Container program not provided")?;
    info!("Starting container");
    execvpe(prog, args, env)?;

    debug!("Exiting container");
    Ok(())
}

fn fallback_move_root(new_root: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
    chdir(new_root)?;
    let dot = CString::new(".")?;
    let slash = CString::new("/")?;
    let dot_cstr = dot.as_c_str();
    let slash_cstr = slash.as_c_str();

    nix::mount::mount(
        Some(dot_cstr),
        dot_cstr,
        Option::<&CStr>::None,
        nix::mount::MsFlags::MS_REC | nix::mount::MsFlags::MS_BIND,
        Option::<&[u8]>::None,
    )
    .map_err(|e| Box::<dyn Error + Send + Sync>::from(format!("failed to bind new root: {}", e)))?;

    nix::mount::mount(
        Some(dot_cstr),
        slash_cstr,
        Option::<&CStr>::None,
        nix::mount::MsFlags::MS_MOVE,
        Option::<&[u8]>::None,
    )
    .map_err(|e| Box::<dyn Error + Send + Sync>::from(format!("failed to move new root: {}", e)))?;

    chdir("/")?;
    Ok(())
}

fn is_mount_point(path: &Path) -> io::Result<bool> {
    let meta = fs::metadata(path)?;
    match path.parent() {
        Some(parent) => {
            let parent_meta = fs::metadata(parent)?;
            Ok(meta.dev() != parent_meta.dev() || meta.ino() == parent_meta.ino())
        }
        None => Ok(true),
    }
}

/// Terminate a process given its PID string:
/// 1) Send SIGTERM
/// 2) Wait up to `timeout`
/// 3) If still present, send SIGKILL
/// 4) Brief grace period to confirm exit
///
/// Notes:
/// - Works for arbitrary PIDs (not just your children).
/// - Treats zombies as "terminated".
pub async fn terminate_with_timeout(pid_str: &str, timeout: Duration) -> Result<(), String> {
    let pid_num: i32 = pid_str
        .parse()
        .map_err(|e| format!("Invalid PID '{}': {}", pid_str, e))?;
    let pid = Pid::from_raw(pid_num);

    // Step 1: send SIGTERM
    match kill(pid, Signal::SIGTERM) {
        Ok(()) => { /* delivered */ }
        Err(Errno::ESRCH) => return Ok(()), // already gone
        Err(Errno::EPERM) => {
            return Err(format!("No permission to send SIGTERM to PID {}", pid_num));
        }
        Err(e) => return Err(format!("Failed to send SIGTERM to {}: {}", pid_num, e)),
    }

    // Step 2: wait until gone or zombie, up to timeout
    if wait_until_gone_or_zombie(pid, timeout).await? {
        return Ok(());
    }

    // Step 3: escalate to SIGKILL
    match kill(pid, Signal::SIGKILL) {
        Ok(()) => { /* delivered */ }
        Err(Errno::ESRCH) => return Ok(()), // disappeared during escalation
        Err(Errno::EPERM) => {
            return Err(format!("No permission to send SIGKILL to PID {}", pid_num));
        }
        Err(e) => return Err(format!("Failed to send SIGKILL to {}: {}", pid_num, e)),
    }

    // Step 4: short grace period after SIGKILL
    let _ = wait_until_gone_or_zombie(pid, Duration::from_millis(500)).await?;
    Ok(())
}

/// Poll until the process is gone or a zombie, or the timeout elapses.
/// Returns Ok(true) if gone/zombie, Ok(false) on timeout.
async fn wait_until_gone_or_zombie(pid: Pid, timeout: Duration) -> Result<bool, String> {
    let deadline = Instant::now() + timeout;
    loop {
        match is_gone_or_zombie(pid) {
            Ok(true) => return Ok(true),
            Ok(false) => { /* still present */ }
            Err(e) => return Err(format!("liveness check failed: {}", e)),
        }

        if Instant::now() >= deadline {
            return Ok(false);
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Returns true if the process no longer exists, or exists but is a zombie.
/// Uses `/proc/<pid>/stat`. This avoids needing `kill(pid, 0)` or an extra `libc` dependency.
fn is_gone_or_zombie(pid: Pid) -> io::Result<bool> {
    let stat_path = format!("/proc/{}/stat", pid.as_raw());

    // If /proc/<pid>/stat is missing, the process is gone.
    let stat = match fs::read_to_string(&stat_path) {
        Ok(s) => s,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(e) => {
            // On systems with hidepid or permission restrictions, we may not be able to read stat.
            // Be conservative: if we cannot confirm gone/zombie, report "still present".
            return Err(e);
        }
    };

    // Parse the process state character from /proc/<pid>/stat
    Ok(extract_state_from_stat(&stat) == Some('Z'))
}

/// Extract the state character from `/proc/<pid>/stat` content.
/// Format: pid (comm) state ...  We find the closing ')' of comm, then the next token is the state.
fn extract_state_from_stat(stat: &str) -> Option<char> {
    let rparen = stat.rfind(')')?;
    let after = &stat[rparen + 1..];
    after.split_whitespace().next()?.chars().next()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::pod::{
        ContainerResources, ContainerSpec, EncryptedVolumeSource, HostPathVolumeSource,
        VolumeMount, VolumeSpec,
    };
    use tempfile::tempdir;

    fn base_container() -> ContainerSpec {
        ContainerSpec {
            command: vec!["/bin/sleep".to_string()],
            args: vec!["1".to_string()],
            ..Default::default()
        }
    }

    #[test]
    fn configure_sets_network_namespace_by_default() {
        let container = base_container();
        let config = Runtime::configure_from_spec("abc123", "demo", &container, &[], false);

        assert_eq!(config.linux.namespaces.len(), 1);
        let ns = config.linux.namespaces.first().unwrap();
        assert_eq!(ns.ns_type, "network");
        let expected_path = netns_dir().join("abc123");
        assert_eq!(
            ns.path.as_deref(),
            Some(expected_path.to_string_lossy().as_ref())
        );
    }

    #[test]
    fn configure_skips_network_namespace_with_host_network() {
        let container = base_container();
        let config = Runtime::configure_from_spec("abc123", "demo", &container, &[], true);

        assert!(config.linux.namespaces.is_empty());
    }

    #[test]
    fn configure_populates_core_isolation_mounts() {
        let container = base_container();
        let config = Runtime::configure_from_spec("abc123", "demo", &container, &[], false);

        let proc_mount = config
            .mounts
            .iter()
            .find(|mount| mount.destination == "/proc")
            .expect("/proc mount missing");
        assert_eq!(proc_mount.mount_type, "proc");
        let proc_options: Vec<&str> = proc_mount
            .options
            .as_ref()
            .expect("/proc options missing")
            .iter()
            .map(|s| s.as_str())
            .collect();
        assert!(proc_options.contains(&"nosuid"));
        assert!(proc_options.contains(&"noexec"));
        assert!(proc_options.contains(&"nodev"));

        let dev_mount = config
            .mounts
            .iter()
            .find(|mount| mount.destination == "/dev")
            .expect("/dev mount missing");
        assert_eq!(dev_mount.mount_type, "tmpfs");
        let dev_options: Vec<&str> = dev_mount
            .options
            .as_ref()
            .expect("/dev options missing")
            .iter()
            .map(|s| s.as_str())
            .collect();
        assert!(dev_options.contains(&"nosuid"));
        assert!(dev_options.contains(&"mode=755"));
    }

    #[test]
    fn link_network_namespace_reuses_existing_nsfs_entry() {
        let tmp = tempdir().expect("tempdir");
        let original = std::env::var("NANOCLOUD_NETNS_DIR").ok();
        std::env::set_var("NANOCLOUD_NETNS_DIR", tmp.path());

        let netns_name = "ns-test";
        let netns_path = tmp.path().join(netns_name);
        std::fs::write(&netns_path, b"dummy").expect("create placeholder netns file");

        link_network_namespace(netns_name, 12345).expect("link should succeed");

        let meta = std::fs::symlink_metadata(&netns_path).expect("metadata");
        assert!(
            !meta.file_type().is_symlink(),
            "existing nsfs entry must be left untouched"
        );

        if let Some(value) = original {
            std::env::set_var("NANOCLOUD_NETNS_DIR", value);
        } else {
            std::env::remove_var("NANOCLOUD_NETNS_DIR");
        }
    }

    #[test]
    fn configure_respects_readonly_volume_mounts() {
        let mut container = base_container();
        container.volume_mounts = vec![VolumeMount {
            name: "data".to_string(),
            mount_path: "/data".to_string(),
            read_only: Some(true),
        }];

        let volumes = vec![VolumeSpec {
            name: "data".to_string(),
            host_path: Some(HostPathVolumeSource {
                path: "/srv/data".to_string(),
                r#type: None,
            }),
            ..Default::default()
        }];

        let config = Runtime::configure_from_spec("abc123", "demo", &container, &volumes, false);
        let mount = config
            .mounts
            .iter()
            .find(|m| m.destination == "/data")
            .expect("bind mount missing");
        assert_eq!(mount.source, "/srv/data");
        let options: Vec<&str> = mount
            .options
            .as_ref()
            .expect("bind mount options missing")
            .iter()
            .map(|s| s.as_str())
            .collect();
        assert!(options.contains(&"rbind"));
        assert!(options.contains(&"ro"));
        assert!(!options.contains(&"rw"));
    }

    #[test]
    fn configure_records_encrypted_volume_metadata() {
        let mut container = base_container();
        container.volume_mounts = vec![VolumeMount {
            name: "secure-data".to_string(),
            mount_path: "/secure".to_string(),
            read_only: Some(false),
        }];

        let volumes = vec![VolumeSpec {
            name: "secure-data".to_string(),
            encrypted: Some(EncryptedVolumeSource {
                device: "/dev/sdb".to_string(),
                key_name: "demo-volume".to_string(),
                filesystem: Some("ext4".to_string()),
            }),
            ..Default::default()
        }];

        let config = Runtime::configure_from_spec("abc123", "demo", &container, &volumes, false);
        let mount = config
            .mounts
            .iter()
            .find(|m| m.destination == "/secure")
            .expect("encrypted bind mount missing");
        assert_eq!(mount.mount_type, "bind");
        let root = crate::nanocloud::util::security::volume::encrypted_volume_root();
        assert!(mount.source.starts_with(root.to_string_lossy().as_ref()));
        assert!(mount.source.contains("abc123"));

        let metadata = config
            .encrypted_volumes
            .first()
            .expect("encrypted volume metadata missing");
        assert_eq!(metadata.volume, "secure-data");
        assert_eq!(metadata.device, "/dev/sdb");
        assert_eq!(metadata.key_name, "demo-volume");
        assert_eq!(metadata.filesystem.as_deref(), Some("ext4"));
        assert!(metadata.mapper.contains("secure-data"));
        let root = crate::nanocloud::util::security::volume::encrypted_volume_root();
        assert!(metadata
            .host_mount
            .starts_with(root.to_string_lossy().as_ref()));
    }

    #[test]
    fn resolve_process_user_supports_numeric_components() {
        assert_eq!(resolve_process_user(Some("1001:2000")), (1001, 2000));
    }

    #[test]
    fn resolve_process_user_resolves_names() {
        // root is guaranteed to exist on unix systems
        assert_eq!(resolve_process_user(Some("root")), (0, 0));
        assert_eq!(resolve_process_user(Some("root:root")), (0, 0));
    }

    #[test]
    fn configure_applies_linux_resources() {
        let mut container = base_container();
        container.resources = Some(ContainerResources {
            cpu_shares: Some(512),
            cpu_quota: Some(200_000),
            cpu_period: Some(100_000),
            memory_limit: Some(268_435_456),
            memory_swap: Some(536_870_912),
            pids_limit: Some(128),
        });

        let config = Runtime::configure_from_spec("abc123", "demo", &container, &[], false);
        let resources = config
            .linux
            .resources
            .expect("resources should be populated");

        let cpu = resources.cpu.expect("cpu resources missing");
        assert_eq!(cpu.shares, Some(512));
        assert_eq!(cpu.quota, Some(200_000));
        assert_eq!(cpu.period, Some(100_000));

        let memory = resources.memory.expect("memory resources missing");
        assert_eq!(memory.limit, Some(268_435_456));
        assert_eq!(memory.swap, Some(536_870_912));

        let pids = resources.pids.expect("pids resources missing");
        assert_eq!(pids.limit, 128);
    }

    #[test]
    fn map_container_resources_returns_none_when_spec_is_empty() {
        assert!(map_container_resources(&ContainerResources::default()).is_none());
    }

    #[test]
    fn map_container_resources_preserves_partial_values() {
        let spec = ContainerResources {
            pids_limit: Some(32),
            ..Default::default()
        };

        let resources = map_container_resources(&spec).expect("should map");
        assert!(resources.cpu.is_none());
        assert!(resources.memory.is_none());
        assert_eq!(resources.pids.expect("pids entry").limit, 32);
    }

    #[test]
    fn parse_env_handles_missing_values() {
        let entries = vec!["KEY=value".to_string(), "FLAG".to_string(), "".to_string()];

        let env_map = parse_env(&entries);
        assert_eq!(env_map.get("KEY").map(String::as_str), Some("value"));
        assert_eq!(env_map.get("FLAG").map(String::as_str), Some(""));
        assert!(!env_map.contains_key(""));
    }

    #[test]
    fn extract_state_from_stat_parses_state_character() {
        let stat = "123 (sleep) S 1 2 3";
        assert_eq!(extract_state_from_stat(stat), Some('S'));
        assert_eq!(extract_state_from_stat("corrupted"), None);
    }
}
