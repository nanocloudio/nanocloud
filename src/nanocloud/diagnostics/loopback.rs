use crate::nanocloud::cni::cni_plugin;
use crate::nanocloud::csi::{
    csi_plugin, CreateVolumeRequest, DeleteVolumeRequest, NodePublishVolumeRequest,
    NodeUnpublishVolumeRequest,
};
use crate::nanocloud::oci::runtime::netns_dir;
use crate::nanocloud::oci::Registry;
use chrono::Utc;
use log::warn;
use nix::sched::{setns, CloneFlags};
use rand::{distributions::Alphanumeric, Rng};
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::task;
use tokio::time::timeout;

/// Default diagnostics image published alongside Nanocloud releases.
pub const DEFAULT_LOOPBACK_IMAGE: &str = "dockyard.nanocloud.io/diagnostics/loopback:latest";
pub const DEFAULT_LOOPBACK_TIMEOUT: Duration = Duration::from_secs(90);

const PROBE_NAMESPACE: &str = "diagnostics";
const PROBE_SERVICE: &str = "loopback-probe";
const CLAIM_PREFIX: &str = "loopback";
const LOOPBACK_TARGET_BASE: &str = "/mnt/nanocloud-loopback";
const LOG_DIR_ENV: &str = "NANOCLOUD_DIAGNOSTICS_LOG_DIR";
const DEFAULT_LOG_DIR: &str = "/var/log/nanocloud/diagnostics";

/// Configuration for the loopback probe.
#[derive(Clone, Debug)]
pub struct LoopbackProbeConfig {
    pub image: String,
    pub timeout: Duration,
}

impl Default for LoopbackProbeConfig {
    fn default() -> Self {
        Self {
            image: DEFAULT_LOOPBACK_IMAGE.to_string(),
            timeout: DEFAULT_LOOPBACK_TIMEOUT,
        }
    }
}

/// Structured result returned by the probe.
#[derive(Clone, Debug)]
pub struct LoopbackProbeResult {
    pub dns_ok: bool,
    pub volumes_ok: bool,
    pub duration: Duration,
    pub log_path: Option<PathBuf>,
    pub notes: Vec<String>,
    pub skipped: bool,
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum LoopbackProbeError {
    Timeout,
    Failed(String),
}

impl Display for LoopbackProbeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LoopbackProbeError::Timeout => {
                write!(f, "loopback probe exceeded the configured timeout")
            }
            LoopbackProbeError::Failed(reason) => write!(f, "loopback probe failed: {}", reason),
        }
    }
}

impl Error for LoopbackProbeError {}

struct LoopbackResources {
    container_id: Option<String>,
    volume: Option<VolumeResource>,
}

impl LoopbackResources {
    fn new() -> Self {
        Self {
            container_id: None,
            volume: None,
        }
    }

    fn set_container(&mut self, id: String) {
        self.container_id = Some(id);
    }

    fn init_volume(&mut self, volume_id: String, target_path: String) {
        self.volume = Some(VolumeResource {
            volume_id,
            target_path,
            published: false,
        });
    }

    fn mark_volume_published(&mut self) {
        if let Some(volume) = self.volume.as_mut() {
            volume.published = true;
        }
    }

    async fn cleanup(&mut self) -> Result<(), LoopbackProbeError> {
        if let Some(volume) = self.volume.take() {
            let plugin = csi_plugin();
            if volume.published {
                plugin
                    .node_unpublish_volume(NodeUnpublishVolumeRequest {
                        volume_id: volume.volume_id.clone(),
                        target_path: volume.target_path.clone(),
                    })
                    .await
                    .map_err(|err| {
                        LoopbackProbeError::Failed(format!(
                            "Failed to unpublish diagnostics volume: {}",
                            err
                        ))
                    })?;
            }
            plugin
                .delete_volume(DeleteVolumeRequest {
                    volume_id: volume.volume_id,
                })
                .await
                .map_err(|err| {
                    LoopbackProbeError::Failed(format!(
                        "Failed to delete diagnostics volume: {}",
                        err
                    ))
                })?;
        }

        if let Some(container_id) = self.container_id.take() {
            let plugin = cni_plugin();
            let env: HashMap<String, String> = [
                ("CNI_COMMAND".to_string(), "DEL".to_string()),
                ("CNI_CONTAINERID".to_string(), container_id),
            ]
            .into_iter()
            .collect();
            plugin.delete(&env).map_err(|err| {
                LoopbackProbeError::Failed(format!("Failed to tear down CNI network: {}", err))
            })?;
        }

        Ok(())
    }
}

struct VolumeResource {
    volume_id: String,
    target_path: String,
    published: bool,
}

struct NetworkAttachment {
    netns_path: PathBuf,
    assigned_ip: Option<String>,
}

struct NamespaceGuard {
    original: File,
}

impl NamespaceGuard {
    fn enter(path: &Path) -> Result<Self, LoopbackProbeError> {
        let original = File::open("/proc/self/ns/net").map_err(|err| {
            LoopbackProbeError::Failed(format!("Failed to open current network namespace: {}", err))
        })?;
        let target = File::open(path).map_err(|err| {
            LoopbackProbeError::Failed(format!(
                "Failed to open target namespace {}: {}",
                path.display(),
                err
            ))
        })?;
        setns(&target, CloneFlags::CLONE_NEWNET).map_err(|err| {
            LoopbackProbeError::Failed(format!(
                "Failed to enter namespace {}: {}",
                path.display(),
                err
            ))
        })?;
        Ok(Self { original })
    }
}

impl Drop for NamespaceGuard {
    fn drop(&mut self) {
        if let Err(err) = setns(&self.original, CloneFlags::CLONE_NEWNET) {
            warn!(
                "Failed to restore original network namespace after loopback probe: {}",
                err
            );
        }
    }
}

pub async fn run_loopback_probe(
    config: LoopbackProbeConfig,
) -> Result<LoopbackProbeResult, LoopbackProbeError> {
    let mut resources = LoopbackResources::new();
    let start = Instant::now();
    let probe_future = run_probe_steps(&config, &mut resources);
    let timed = timeout(config.timeout, probe_future).await;
    let cleanup_result = resources.cleanup().await;

    match timed {
        Ok(Ok(mut result)) => {
            cleanup_result?;
            result.duration = start.elapsed();
            Ok(result)
        }
        Ok(Err(err)) => {
            if let Err(cleanup_err) = cleanup_result {
                warn!(
                    "Loopback cleanup encountered an error after failure: {}",
                    cleanup_err
                );
            }
            Err(err)
        }
        Err(_) => {
            cleanup_result?;
            Err(LoopbackProbeError::Timeout)
        }
    }
}

async fn run_probe_steps(
    config: &LoopbackProbeConfig,
    resources: &mut LoopbackResources,
) -> Result<LoopbackProbeResult, LoopbackProbeError> {
    let mut notes = Vec::new();
    Registry::pull(&config.image, true).await.map_err(|err| {
        LoopbackProbeError::Failed(format!(
            "Failed to pull diagnostics image {}: {}",
            config.image, err
        ))
    })?;
    notes.push(format!("Pulled diagnostics image {}", config.image));

    let container_id = random_container_id();
    let attach = attach_network(&container_id)?;
    resources.set_container(container_id.clone());
    if let Some(ip) = attach.assigned_ip.as_deref() {
        notes.push(format!("Attached diagnostics namespace with IP {}", ip));
    }

    let publish_path = prepare_volume(resources, &container_id).await?;
    notes.push(format!(
        "Mounted diagnostics volume at {}",
        publish_path.display()
    ));

    let (dns_ok, mut dns_notes) = check_dns_in_namespace(&attach.netns_path).await?;
    notes.append(&mut dns_notes);
    let (volumes_ok, mut volume_notes) = verify_volume_mount(&publish_path).await;
    notes.append(&mut volume_notes);

    let log_path = match persist_probe_log(&container_id, dns_ok, volumes_ok, &notes).await {
        Ok(path) => Some(path),
        Err(err) => {
            notes.push(format!("Failed to write probe log: {}", err));
            None
        }
    };

    Ok(LoopbackProbeResult {
        dns_ok,
        volumes_ok,
        duration: Duration::from_secs(0),
        log_path,
        notes,
        skipped: false,
    })
}

fn random_container_id() -> String {
    let suffix: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect();
    format!("loopback-{}", suffix.to_lowercase())
}

fn attach_network(container_id: &str) -> Result<NetworkAttachment, LoopbackProbeError> {
    let netns_path = netns_dir().join(container_id);
    let mut env = HashMap::new();
    env.insert("CNI_COMMAND".to_string(), "ADD".to_string());
    env.insert("CNI_CONTAINERID".to_string(), container_id.to_string());
    env.insert(
        "CNI_NETNS".to_string(),
        netns_path.to_string_lossy().to_string(),
    );
    env.insert("CNI_IFNAME".to_string(), "nanocloud0".to_string());
    env.insert("CNI_PATH".to_string(), "/opt/cni/bin".to_string());

    let config = json!({
        "cniVersion": "1.0.0",
        "name": "nanocloud",
        "type": "bridge",
        "bridge": "nanocloud0",
    })
    .to_string();

    let plugin = cni_plugin();
    let result = plugin.add(&env, config.into_bytes()).map_err(|err| {
        LoopbackProbeError::Failed(format!(
            "Failed to configure diagnostics CNI attachment: {}",
            err
        ))
    })?;
    Ok(NetworkAttachment {
        netns_path,
        assigned_ip: result.ips.first().map(|ip| ip.address.clone()),
    })
}

async fn prepare_volume(
    resources: &mut LoopbackResources,
    container_id: &str,
) -> Result<PathBuf, LoopbackProbeError> {
    let plugin = csi_plugin();
    let mut parameters = HashMap::new();
    parameters.insert("namespace".to_string(), PROBE_NAMESPACE.to_string());
    parameters.insert("service".to_string(), PROBE_SERVICE.to_string());
    parameters.insert(
        "claim".to_string(),
        format!("{}-{}", CLAIM_PREFIX, container_id),
    );

    let request = CreateVolumeRequest {
        name: format!("{}-volume", container_id),
        capacity_range: None,
        volume_capabilities: Vec::new(),
        parameters,
        content_source: None,
    };

    let response = plugin.create_volume(request).await.map_err(|err| {
        LoopbackProbeError::Failed(format!("Failed to create diagnostics volume: {}", err))
    })?;

    let target_path = format!("{}/{}", LOOPBACK_TARGET_BASE, container_id);
    resources.init_volume(response.volume.volume_id.clone(), target_path.clone());

    let publish = plugin
        .node_publish_volume(NodePublishVolumeRequest {
            volume_id: response.volume.volume_id,
            target_path,
            readonly: false,
            volume_capability: None,
            volume_context: HashMap::new(),
        })
        .await
        .map_err(|err| {
            LoopbackProbeError::Failed(format!("Failed to publish diagnostics volume: {}", err))
        })?;
    resources.mark_volume_published();
    Ok(PathBuf::from(publish.publish_path))
}

async fn check_dns_in_namespace(
    netns_path: &Path,
) -> Result<(bool, Vec<String>), LoopbackProbeError> {
    let servers = discover_nameservers();
    let mut notes = Vec::new();
    if servers.is_empty() {
        notes.push("No nameservers found in /etc/resolv.conf".to_string());
        return Ok((false, notes));
    }

    let netns = netns_path.to_path_buf();
    task::spawn_blocking(move || run_dns_checks(&netns, &servers))
        .await
        .map_err(|err| LoopbackProbeError::Failed(format!("DNS check task failed: {}", err)))?
}

fn run_dns_checks(
    netns_path: &Path,
    servers: &[String],
) -> Result<(bool, Vec<String>), LoopbackProbeError> {
    let _guard = NamespaceGuard::enter(netns_path)?;
    let mut notes = Vec::new();
    for server in servers {
        match try_connect(server) {
            Ok(()) => {
                notes.push(format!("DNS server {} reachable (tcp/53)", server));
                return Ok((true, notes));
            }
            Err(reason) => notes.push(reason),
        }
    }
    Ok((false, notes))
}

fn try_connect(server: &str) -> Result<(), String> {
    let ip: IpAddr = server
        .parse()
        .map_err(|err| format!("Failed to parse nameserver {}: {}", server, err))?;
    let addr = SocketAddr::new(ip, 53);
    TcpStream::connect_timeout(&addr, Duration::from_secs(3))
        .map_err(|err| format!("DNS server {} unreachable on tcp/53: {}", server, err))?;
    Ok(())
}

async fn verify_volume_mount(publish_path: &Path) -> (bool, Vec<String>) {
    let mut notes = Vec::new();
    if fs::metadata(publish_path).await.is_err() {
        notes.push(format!(
            "Diagnostics volume path {} missing",
            publish_path.display()
        ));
        return (false, notes);
    }

    let sentinel = publish_path.join("probe.txt");
    let marker = format!("nanocloud-loopback-{}", Utc::now().to_rfc3339());
    match fs::write(&sentinel, marker.as_bytes()).await {
        Ok(()) => {}
        Err(err) => {
            notes.push(format!(
                "Failed to write sentinel {}: {}",
                sentinel.display(),
                err
            ));
            return (false, notes);
        }
    }

    match fs::read_to_string(&sentinel).await {
        Ok(contents) if contents == marker => notes.push(format!(
            "Volume round-trip succeeded for {}",
            sentinel.display()
        )),
        Ok(contents) => {
            notes.push(format!(
                "Sentinel mismatch ({} bytes) at {}",
                contents.len(),
                sentinel.display()
            ));
            return (false, notes);
        }
        Err(err) => {
            notes.push(format!(
                "Failed to read sentinel {}: {}",
                sentinel.display(),
                err
            ));
            return (false, notes);
        }
    }

    notes.push(format!(
        "Diagnostics volume {} is writable (sentinel stored)",
        publish_path.display()
    ));
    (true, notes)
}

async fn persist_probe_log(
    container_id: &str,
    dns_ok: bool,
    volumes_ok: bool,
    notes: &[String],
) -> Result<PathBuf, std::io::Error> {
    let log_dir = diagnostics_log_dir();
    fs::create_dir_all(&log_dir).await?;
    let log_path = log_dir.join(format!("loopback-{}.log", container_id));
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .await?;
    let entry = format!(
        "[{}] dns_ok={} volumes_ok={} notes={}\n",
        Utc::now().to_rfc3339(),
        dns_ok,
        volumes_ok,
        notes.join(" | "),
    );
    file.write_all(entry.as_bytes()).await?;
    Ok(log_path)
}

fn diagnostics_log_dir() -> PathBuf {
    env::var(LOG_DIR_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_LOG_DIR))
}

fn discover_nameservers() -> Vec<String> {
    let mut dns_servers = Vec::new();
    if let Ok(file) = File::open("/etc/resolv.conf") {
        let reader = BufReader::new(file);
        for line in reader.lines().map_while(Result::ok) {
            if line.starts_with("nameserver") {
                if let Some(ip) = line.split_whitespace().nth(1) {
                    dns_servers.push(ip.to_string());
                }
            }
        }
    }
    dns_servers
}
