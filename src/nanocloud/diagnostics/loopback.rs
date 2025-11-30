use crate::nanocloud::cni::cni_plugin;
use crate::nanocloud::cni::provider::CniPlugin;
use crate::nanocloud::csi::CsiPlugin;
use crate::nanocloud::csi::{
    csi_plugin, CreateVolumeRequest, DeleteVolumeRequest, NodePublishVolumeRequest,
    NodeUnpublishVolumeRequest,
};
use crate::nanocloud::oci::runtime::netns_dir;
use crate::nanocloud::oci::Registry;
use chrono::Utc;
use futures_util::future::BoxFuture;
use log::{debug, info, warn};
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
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::task;
use tokio::time;
use tokio_util::sync::CancellationToken;

/// Default diagnostics image published alongside Nanocloud releases.
pub const DEFAULT_LOOPBACK_IMAGE: &str = "dockyard.nanocloud.io/diagnostics/loopback:latest";
pub const DEFAULT_LOOPBACK_TIMEOUT: Duration = Duration::from_secs(90);

const DEFAULT_PULL_TIMEOUT: Duration = Duration::from_secs(25);
const DEFAULT_CNI_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_CSI_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_DNS_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_VOLUME_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_LOG_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);

const PROBE_NAMESPACE: &str = "diagnostics";
const PROBE_SERVICE: &str = "loopback-probe";
const CLAIM_PREFIX: &str = "loopback";
const LOOPBACK_TARGET_BASE: &str = "/mnt/nanocloud-loopback";
const LOG_DIR_ENV: &str = "NANOCLOUD_DIAGNOSTICS_LOG_DIR";
const TARGET_BASE_ENV: &str = "NANOCLOUD_LOOPBACK_TARGET_DIR";
const DEFAULT_LOG_DIR: &str = "/var/log/nanocloud/diagnostics";

static SERIALIZATION_GUARD: OnceLock<Arc<Mutex<()>>> = OnceLock::new();

/// Configuration for the loopback probe.
#[derive(Clone, Debug)]
pub struct LoopbackProbeConfig {
    pub image: String,
    pub timeout: Duration,
    pub phase_timeouts: Option<LoopbackPhaseTimeouts>,
    pub serialize_probes: bool,
    pub log_dir: Option<PathBuf>,
    pub target_base: Option<PathBuf>,
}

impl Default for LoopbackProbeConfig {
    fn default() -> Self {
        Self {
            image: DEFAULT_LOOPBACK_IMAGE.to_string(),
            timeout: DEFAULT_LOOPBACK_TIMEOUT,
            phase_timeouts: None,
            serialize_probes: false,
            log_dir: None,
            target_base: None,
        }
    }
}

impl LoopbackProbeConfig {
    fn validate(&self) -> Result<(), LoopbackProbeError> {
        if self.image.trim().is_empty() {
            return Err(LoopbackProbeError::Failed(
                "diagnostics image must be specified".to_string(),
            ));
        }
        if self.timeout.is_zero() {
            return Err(LoopbackProbeError::Failed(
                "loopback timeout must be greater than zero".to_string(),
            ));
        }
        if let Some(phases) = &self.phase_timeouts {
            for (label, value) in [
                ("pull", phases.pull),
                ("cni", phases.cni),
                ("csi", phases.csi),
                ("dns", phases.dns),
                ("volume", phases.volume),
                ("log", phases.log),
                ("cleanup", phases.cleanup),
            ] {
                if value.is_zero() {
                    return Err(LoopbackProbeError::Failed(format!(
                        "phase timeout {} must be greater than zero",
                        label
                    )));
                }
                if value > self.timeout {
                    return Err(LoopbackProbeError::Failed(format!(
                        "phase timeout {} ({:?}) exceeds overall timeout {:?}",
                        label, value, self.timeout
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Per-phase timeout configuration.
#[derive(Clone, Debug)]
pub struct LoopbackPhaseTimeouts {
    pub pull: Duration,
    pub cni: Duration,
    pub csi: Duration,
    pub dns: Duration,
    pub volume: Duration,
    pub log: Duration,
    pub cleanup: Duration,
}

impl Default for LoopbackPhaseTimeouts {
    fn default() -> Self {
        Self {
            pull: DEFAULT_PULL_TIMEOUT,
            cni: DEFAULT_CNI_TIMEOUT,
            csi: DEFAULT_CSI_TIMEOUT,
            dns: DEFAULT_DNS_TIMEOUT,
            volume: DEFAULT_VOLUME_TIMEOUT,
            log: DEFAULT_LOG_TIMEOUT,
            cleanup: DEFAULT_CLEANUP_TIMEOUT,
        }
    }
}

impl LoopbackPhaseTimeouts {
    fn resolved(overall: Duration, overrides: Option<Self>) -> Self {
        let mut resolved = overrides.unwrap_or_default();
        resolved.clamp(overall);
        resolved
    }

    fn clamp(&mut self, overall: Duration) {
        for timeout in [
            &mut self.pull,
            &mut self.cni,
            &mut self.csi,
            &mut self.dns,
            &mut self.volume,
            &mut self.log,
            &mut self.cleanup,
        ] {
            if *timeout > overall {
                *timeout = overall;
            }
        }
    }

    fn for_phase(&self, phase: ProbePhase) -> Duration {
        match phase {
            ProbePhase::PullImage => self.pull,
            ProbePhase::AttachNetwork => self.cni,
            ProbePhase::PrepareVolume => self.csi,
            ProbePhase::DnsCheck => self.dns,
            ProbePhase::VolumeCheck => self.volume,
            ProbePhase::PersistLog => self.log,
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

impl LoopbackProbeError {
    fn with_context(self, phase: ProbePhase) -> Self {
        match self {
            LoopbackProbeError::Failed(reason) => {
                LoopbackProbeError::Failed(format!("{}: {}", phase.label(), reason))
            }
            other => other,
        }
    }
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

/// OCI interface used by the probe, abstracted for tests.
pub trait OciClient: Send + Sync {
    fn pull<'a>(&'a self, image: &'a str) -> BoxFuture<'a, Result<(), LoopbackProbeError>>;
}

/// DNS check interface, injectable for tests.
pub trait DnsProbe: Send + Sync {
    fn check<'a>(
        &'a self,
        netns_path: PathBuf,
        cancel: CancellationToken,
    ) -> BoxFuture<'a, Result<(bool, Vec<String>), LoopbackProbeError>>;
}

/// Metrics hook for probe phases and cleanup.
pub trait LoopbackMetrics: Send + Sync {
    fn phase_start(&self, _phase: &'static str) {}
    fn phase_end(&self, _phase: &'static str, _elapsed: Duration, _outcome: Result<(), &str>) {}
    fn cleanup_retry(&self, _resource: &'static str, _attempt: usize) {}
}

#[derive(Default)]
struct NoopLoopbackMetrics;

impl LoopbackMetrics for NoopLoopbackMetrics {}

#[derive(Default)]
struct DefaultOciClient;

impl OciClient for DefaultOciClient {
    fn pull<'a>(&'a self, image: &'a str) -> BoxFuture<'a, Result<(), LoopbackProbeError>> {
        Box::pin(async move {
            Registry::pull(image, true)
                .await
                .map(|_| ())
                .map_err(|err| {
                    LoopbackProbeError::Failed(format!(
                        "Failed to pull diagnostics image {}: {}",
                        image, err
                    ))
                })
        })
    }
}

#[derive(Default)]
struct DefaultDnsProbe;

impl DnsProbe for DefaultDnsProbe {
    fn check<'a>(
        &'a self,
        netns_path: PathBuf,
        cancel: CancellationToken,
    ) -> BoxFuture<'a, Result<(bool, Vec<String>), LoopbackProbeError>> {
        Box::pin(check_dns_in_namespace(netns_path, cancel))
    }
}

/// Dependencies used by the probe, injectable for tests.
#[derive(Clone)]
pub struct LoopbackProbeDeps {
    pub cni: Arc<dyn CniPlugin>,
    pub csi: Arc<dyn CsiPlugin>,
    pub oci: Arc<dyn OciClient>,
    pub dns: Arc<dyn DnsProbe>,
    pub metrics: Arc<dyn LoopbackMetrics>,
}

impl Default for LoopbackProbeDeps {
    fn default() -> Self {
        Self {
            cni: cni_plugin(),
            csi: csi_plugin(),
            oci: Arc::new(DefaultOciClient),
            dns: Arc::new(DefaultDnsProbe),
            metrics: Arc::new(NoopLoopbackMetrics),
        }
    }
}

#[derive(Clone, Debug)]
struct ProbeRunId {
    raw: String,
}

impl ProbeRunId {
    fn new() -> Self {
        let suffix: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();
        let ts = Utc::now().format("%Y%m%d%H%M%S");
        Self {
            raw: format!("loopback-{}-{}", ts, suffix.to_lowercase()),
        }
    }

    fn container_id(&self) -> String {
        self.raw.clone()
    }

    fn volume_name(&self) -> String {
        format!("{}-volume", self.raw)
    }

    fn claim_name(&self) -> String {
        format!("{}-{}", CLAIM_PREFIX, self.raw)
    }

    fn target_path(&self, base: &Path) -> PathBuf {
        base.join(&self.raw)
    }

    fn log_dir(&self, base: &Path) -> PathBuf {
        base.join(&self.raw)
    }
}

struct CleanupReport {
    notes: Vec<String>,
}

impl CleanupReport {
    fn new() -> Self {
        Self { notes: Vec::new() }
    }

    fn push(&mut self, note: impl Into<String>) {
        self.notes.push(note.into());
    }
}

async fn retry_with_backoff<F, Fut, T, E>(
    mut op: F,
    attempts: usize,
    base_delay: Duration,
    resource: &'static str,
    metrics: &Arc<dyn LoopbackMetrics>,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(_err) if attempt + 1 < attempts => {
                attempt += 1;
                metrics.cleanup_retry(resource, attempt);
                time::sleep(base_delay.saturating_mul(attempt as u32)).await;
                continue;
            }
            Err(err) => return Err(err),
        }
    }
}

struct VolumeResource {
    volume_id: String,
    target_path: PathBuf,
    published: bool,
}

struct NetworkResource {
    container_id: String,
    netns_path: PathBuf,
}

struct ResourceRegistry {
    network: Option<NetworkResource>,
    volume: Option<VolumeResource>,
    log_dir: Option<PathBuf>,
}

impl ResourceRegistry {
    fn new() -> Self {
        Self {
            network: None,
            volume: None,
            log_dir: None,
        }
    }

    fn register_network(&mut self, container_id: String, netns_path: PathBuf) {
        self.network = Some(NetworkResource {
            container_id,
            netns_path,
        });
    }

    fn register_volume(&mut self, volume_id: String, target_path: PathBuf) {
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

    fn register_log_dir(&mut self, log_dir: PathBuf) {
        self.log_dir = Some(log_dir);
    }

    async fn cleanup(
        &mut self,
        deps: &LoopbackProbeDeps,
        timeout: Duration,
        cancel: &CancellationToken,
        metrics: &Arc<dyn LoopbackMetrics>,
    ) -> CleanupReport {
        let mut report = CleanupReport::new();

        if cancel.is_cancelled() {
            report.push("Cleanup continuing after cancellation");
        }

        if let Some(volume) = self.volume.take() {
            if cancel.is_cancelled() {
                report
                    .push("Cleanup proceeding without additional time budget for volume teardown");
            }
            if volume.published {
                let csi = deps.csi.clone();
                let target_path = volume.target_path.clone();
                let publish_id = volume.volume_id.clone();
                let result = time::timeout(
                    timeout,
                    retry_with_backoff(
                        || {
                            let csi = csi.clone();
                            let target_path = target_path.clone();
                            let publish_id = publish_id.clone();
                            async move {
                                csi.node_unpublish_volume(NodeUnpublishVolumeRequest {
                                    volume_id: publish_id.clone(),
                                    target_path: target_path.to_string_lossy().to_string(),
                                })
                                .await
                            }
                        },
                        3,
                        Duration::from_millis(100),
                        "csi_unpublish",
                        metrics,
                    ),
                )
                .await;
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => report.push(format!(
                        "Cleanup: failed to unpublish volume {}: {}",
                        volume.volume_id, err
                    )),
                    Err(_) => report.push(format!(
                        "Cleanup: timeout while unpublishing volume {}",
                        volume.volume_id
                    )),
                }
            }

            let csi = deps.csi.clone();
            let delete_id = volume.volume_id.clone();
            let result = time::timeout(
                timeout,
                retry_with_backoff(
                    || {
                        let csi = csi.clone();
                        let delete_id = delete_id.clone();
                        async move {
                            csi.delete_volume(DeleteVolumeRequest {
                                volume_id: delete_id.clone(),
                            })
                            .await
                        }
                    },
                    3,
                    Duration::from_millis(100),
                    "csi_delete",
                    metrics,
                ),
            )
            .await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => report.push(format!(
                    "Cleanup: failed to delete diagnostics volume {}: {}",
                    volume.volume_id, err
                )),
                Err(_) => report.push(format!(
                    "Cleanup: timeout while deleting volume {}",
                    volume.volume_id
                )),
            }

            if let Err(err) = fs::remove_dir_all(&volume.target_path).await {
                report.push(format!(
                    "Cleanup: failed to remove volume target {}: {}",
                    volume.target_path.display(),
                    err
                ));
            }
        }

        if let Some(network) = self.network.take() {
            let cni = deps.cni.clone();
            let netns_path = network.netns_path.clone();
            let env = build_cni_env(&network.container_id, &netns_path, "DEL");
            let delete_result = time::timeout(
                timeout,
                retry_with_backoff(
                    || {
                        let cni = cni.clone();
                        let env = env.clone();
                        task::spawn_blocking(move || cni.delete(&env))
                    },
                    3,
                    Duration::from_millis(50),
                    "cni_delete",
                    metrics,
                ),
            )
            .await;
            match delete_result {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(err))) => report.push(format!(
                    "Cleanup: failed to tear down CNI network for {}: {}",
                    network.container_id, err
                )),
                Ok(Err(join_err)) => report.push(format!(
                    "Cleanup: task error while tearing down CNI network for {}: {}",
                    network.container_id, join_err
                )),
                Err(_) => report.push(format!(
                    "Cleanup: timeout while tearing down network for {}",
                    network.container_id
                )),
            }

            if let Err(err) = fs::remove_file(&netns_path).await {
                if err.kind() != std::io::ErrorKind::NotFound {
                    report.push(format!(
                        "Cleanup: failed to remove netns path {}: {}",
                        netns_path.display(),
                        err
                    ));
                }
            }
        }

        if let Some(log_dir) = self.log_dir.take() {
            report.push(format!("Logs retained at {}", log_dir.display()));
        }

        report
    }
}

struct NetworkAttachment {
    container_id: String,
    netns_path: PathBuf,
    assigned_ip: Option<String>,
}

struct VolumePublishResult {
    publish_path: PathBuf,
    target_path: PathBuf,
    volume_id: String,
}

/// Guard that restores the original namespace on drop.
///
/// # Safety
/// - Calls to `NamespaceGuard::enter` must remain on the same thread until the guard is dropped.
/// - The guard must not be moved across threads because it carries the original namespace file.
/// - Dropping the guard restores the original namespace even if the probe panics while inside the target namespace.
#[must_use = "Namespace restoration is performed when the guard is dropped"]
struct NamespaceGuard {
    original: File,
}

impl NamespaceGuard {
    fn enter(path: &Path) -> Result<Self, LoopbackProbeError> {
        debug!("Entering namespace {}", path.display());
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
        } else {
            debug!("Restored original network namespace");
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ProbePhase {
    PullImage,
    AttachNetwork,
    PrepareVolume,
    DnsCheck,
    VolumeCheck,
    PersistLog,
}

impl ProbePhase {
    fn label(&self) -> &'static str {
        match self {
            ProbePhase::PullImage => "pull image",
            ProbePhase::AttachNetwork => "attach network",
            ProbePhase::PrepareVolume => "prepare volume",
            ProbePhase::DnsCheck => "dns check",
            ProbePhase::VolumeCheck => "volume check",
            ProbePhase::PersistLog => "persist log",
        }
    }
}

struct ProbeRunner {
    config: LoopbackProbeConfig,
    deps: LoopbackProbeDeps,
    run_id: ProbeRunId,
    resources: ResourceRegistry,
    timeouts: LoopbackPhaseTimeouts,
    cancel: CancellationToken,
    started_at: Instant,
    target_base: PathBuf,
}

impl ProbeRunner {
    fn new(config: LoopbackProbeConfig, deps: LoopbackProbeDeps) -> Self {
        let timeouts =
            LoopbackPhaseTimeouts::resolved(config.timeout, config.phase_timeouts.clone());
        let target_base = loopback_target_base(config.target_base.as_deref());
        Self {
            config,
            deps,
            run_id: ProbeRunId::new(),
            resources: ResourceRegistry::new(),
            timeouts,
            cancel: CancellationToken::new(),
            started_at: Instant::now(),
            target_base,
        }
    }

    async fn run(&mut self) -> Result<LoopbackProbeResult, LoopbackProbeError> {
        let _serialization_guard = self.acquire_serialization_guard().await?;
        let outcome = self.execute().await;
        let cleanup_timeout = {
            let budget = self.timeouts.cleanup.min(self.remaining_budget());
            if budget.is_zero() {
                Duration::from_secs(1)
            } else {
                budget
            }
        };
        let cleanup_report = self
            .resources
            .cleanup(
                &self.deps,
                cleanup_timeout,
                &self.cancel,
                &self.deps.metrics,
            )
            .await;

        match outcome {
            Ok(mut result) => {
                result.notes.extend(cleanup_report.notes);
                result.duration = self.started_at.elapsed();
                Ok(result)
            }
            Err(err) => {
                for note in cleanup_report.notes {
                    warn!("{}", note);
                }
                Err(err)
            }
        }
    }

    async fn execute(&mut self) -> Result<LoopbackProbeResult, LoopbackProbeError> {
        let mut notes = Vec::new();
        let image = self.config.image.clone();
        let oci = self.deps.oci.clone();
        self.run_phase(ProbePhase::PullImage, async move { oci.pull(&image).await })
            .await?;
        notes.push(format!("Pulled diagnostics image {}", self.config.image));

        let run_id = self.run_id.clone();
        let deps = self.deps.clone();
        let network = self
            .run_phase(ProbePhase::AttachNetwork, async move {
                attach_network(&run_id, &deps).await
            })
            .await?;
        self.resources
            .register_network(network.container_id.clone(), network.netns_path.clone());
        if let Some(ip) = network.assigned_ip.as_deref() {
            notes.push(format!("Attached diagnostics namespace with IP {}", ip));
        }
        let netns_for_dns = network.netns_path.clone();

        let run_id = self.run_id.clone();
        let deps = self.deps.clone();
        let target_base = self.target_base.clone();
        let publish_path = self
            .run_phase(ProbePhase::PrepareVolume, async move {
                prepare_volume(&run_id, &deps, &target_base).await
            })
            .await?;
        self.resources.register_volume(
            publish_path.volume_id.clone(),
            publish_path.target_path.clone(),
        );
        self.resources.mark_volume_published();
        let publish_path = publish_path.publish_path;
        notes.push(format!(
            "Mounted diagnostics volume at {}",
            publish_path.display()
        ));

        let cancel = self.cancel.clone();
        let dns = self.deps.dns.clone();
        let (dns_ok, mut dns_notes) = self
            .run_phase(ProbePhase::DnsCheck, async move {
                dns.check(netns_for_dns, cancel).await
            })
            .await?;
        notes.append(&mut dns_notes);

        let (volumes_ok, mut volume_notes) = self
            .run_phase(
                ProbePhase::VolumeCheck,
                verify_volume_mount(publish_path.clone()),
            )
            .await?;
        notes.append(&mut volume_notes);

        let log_override = self.config.log_dir.clone();
        let run_id = self.run_id.clone();
        let notes_snapshot = notes.clone();
        let log_path = match self
            .run_phase(
                ProbePhase::PersistLog,
                persist_probe_log(
                    run_id,
                    log_override.as_deref(),
                    dns_ok,
                    volumes_ok,
                    notes_snapshot,
                ),
            )
            .await
        {
            Ok((path, log_dir)) => {
                self.resources.register_log_dir(log_dir);
                Some(path)
            }
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

    async fn run_phase<F, T>(&mut self, phase: ProbePhase, fut: F) -> Result<T, LoopbackProbeError>
    where
        F: std::future::Future<Output = Result<T, LoopbackProbeError>> + Send,
        T: Send,
    {
        if self.cancel.is_cancelled() {
            return Err(LoopbackProbeError::Timeout);
        }
        let remaining = self.remaining_budget();
        let timeout = self.timeouts.for_phase(phase).min(remaining);
        if timeout.is_zero() {
            self.cancel.cancel();
            return Err(LoopbackProbeError::Timeout);
        }
        let started = Instant::now();
        self.deps.metrics.phase_start(phase.label());
        info!("loopback probe: starting {} phase", phase.label());
        match time::timeout(timeout, fut).await {
            Ok(result) => {
                let elapsed = started.elapsed();
                match result {
                    Ok(value) => {
                        self.deps.metrics.phase_end(phase.label(), elapsed, Ok(()));
                        info!(
                            "loopback probe: completed {} phase in {:?}",
                            phase.label(),
                            elapsed
                        );
                        Ok(value)
                    }
                    Err(err) => {
                        self.deps
                            .metrics
                            .phase_end(phase.label(), elapsed, Err("error"));
                        Err(err.with_context(phase))
                    }
                }
            }
            Err(_) => {
                self.cancel.cancel();
                self.deps
                    .metrics
                    .phase_end(phase.label(), timeout, Err("timeout"));
                warn!(
                    "loopback probe: {} phase timed out after {:?}",
                    phase.label(),
                    timeout
                );
                Err(LoopbackProbeError::Timeout)
            }
        }
    }

    fn remaining_budget(&self) -> Duration {
        self.config
            .timeout
            .saturating_sub(self.started_at.elapsed())
    }

    async fn acquire_serialization_guard(
        &self,
    ) -> Result<Option<tokio::sync::OwnedMutexGuard<()>>, LoopbackProbeError> {
        if !self.config.serialize_probes {
            return Ok(None);
        }
        let mutex = SERIALIZATION_GUARD
            .get_or_init(|| Arc::new(Mutex::new(())))
            .clone();
        let remaining = self.remaining_budget();
        let guard = time::timeout(remaining, mutex.lock_owned())
            .await
            .map_err(|_| LoopbackProbeError::Timeout)?;
        Ok(Some(guard))
    }
}

pub async fn run_loopback_probe(
    config: LoopbackProbeConfig,
) -> Result<LoopbackProbeResult, LoopbackProbeError> {
    run_loopback_probe_with(config, LoopbackProbeDeps::default()).await
}

pub async fn run_loopback_probe_with(
    config: LoopbackProbeConfig,
    deps: LoopbackProbeDeps,
) -> Result<LoopbackProbeResult, LoopbackProbeError> {
    config.validate()?;
    let mut runner = ProbeRunner::new(config, deps);
    runner.run().await
}

async fn attach_network(
    run_id: &ProbeRunId,
    deps: &LoopbackProbeDeps,
) -> Result<NetworkAttachment, LoopbackProbeError> {
    let container_id = run_id.container_id();
    let netns_path = netns_dir().join(&container_id);
    let env = build_cni_env(&container_id, &netns_path, "ADD");

    let config = json!({
        "cniVersion": "1.0.0",
        "name": "nanocloud",
        "type": "bridge",
        "bridge": "nanocloud0",
    })
    .to_string();

    let cni = deps.cni.clone();
    let result = task::spawn_blocking(move || cni.add(&env, config.into_bytes()))
        .await
        .map_err(|err| {
            LoopbackProbeError::Failed(format!(
                "CNI attach task failed for {}: {}",
                container_id, err
            ))
        })?
        .map_err(|err| {
            LoopbackProbeError::Failed(format!(
                "Failed to configure diagnostics CNI attachment: {}",
                err
            ))
        })?;

    Ok(NetworkAttachment {
        container_id,
        netns_path,
        assigned_ip: result.ips.first().map(|ip| ip.address.clone()),
    })
}

async fn prepare_volume(
    run_id: &ProbeRunId,
    deps: &LoopbackProbeDeps,
    target_base: &Path,
) -> Result<VolumePublishResult, LoopbackProbeError> {
    let plugin = deps.csi.clone();
    let target_path = run_id.target_path(target_base);
    let mut parameters = HashMap::new();
    parameters.insert("namespace".to_string(), PROBE_NAMESPACE.to_string());
    parameters.insert("service".to_string(), PROBE_SERVICE.to_string());
    parameters.insert("claim".to_string(), run_id.claim_name());

    let request = CreateVolumeRequest {
        name: run_id.volume_name(),
        capacity_range: None,
        volume_capabilities: Vec::new(),
        parameters,
        content_source: None,
    };

    let response = plugin
        .create_volume(request)
        .await
        .map_err(|err| LoopbackProbeError::Failed(format!("Failed to create volume: {}", err)))?;

    let volume_id = response.volume.volume_id.clone();
    let publish = plugin
        .node_publish_volume(NodePublishVolumeRequest {
            volume_id: response.volume.volume_id,
            target_path: target_path.to_string_lossy().to_string(),
            readonly: false,
            volume_capability: None,
            volume_context: HashMap::new(),
        })
        .await
        .map_err(|err| LoopbackProbeError::Failed(format!("Failed to publish volume: {}", err)))?;

    Ok(VolumePublishResult {
        publish_path: PathBuf::from(publish.publish_path),
        target_path,
        volume_id,
    })
}

async fn check_dns_in_namespace(
    netns_path: PathBuf,
    cancel: CancellationToken,
) -> Result<(bool, Vec<String>), LoopbackProbeError> {
    let servers = discover_nameservers().await;
    let mut notes = Vec::new();
    if servers.is_empty() {
        notes.push("No nameservers found in /etc/resolv.conf".to_string());
        return Ok((false, notes));
    }

    let result = task::spawn_blocking(move || {
        if cancel.is_cancelled() {
            return Err(LoopbackProbeError::Timeout);
        }
        run_dns_checks(&netns_path, &servers)
    })
    .await
    .map_err(|err| LoopbackProbeError::Failed(format!("DNS check task failed: {}", err)))??;
    Ok(result)
}

async fn verify_volume_mount(
    publish_path: PathBuf,
) -> Result<(bool, Vec<String>), LoopbackProbeError> {
    let mut notes = Vec::new();
    if fs::metadata(&publish_path).await.is_err() {
        notes.push(format!(
            "Diagnostics volume path {} missing",
            publish_path.display()
        ));
        return Ok((false, notes));
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
            return Ok((false, notes));
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
            return Ok((false, notes));
        }
        Err(err) => {
            notes.push(format!(
                "Failed to read sentinel {}: {}",
                sentinel.display(),
                err
            ));
            return Ok((false, notes));
        }
    }

    notes.push(format!(
        "Diagnostics volume {} is writable (sentinel stored)",
        publish_path.display()
    ));
    Ok((true, notes))
}

async fn persist_probe_log(
    run_id: ProbeRunId,
    log_dir_override: Option<&Path>,
    dns_ok: bool,
    volumes_ok: bool,
    notes: Vec<String>,
) -> Result<(PathBuf, PathBuf), LoopbackProbeError> {
    let base = diagnostics_log_dir(log_dir_override);
    let log_dir = run_id.log_dir(&base);
    fs::create_dir_all(&log_dir).await.map_err(|err| {
        LoopbackProbeError::Failed(format!(
            "Failed to create log directory {}: {}",
            log_dir.display(),
            err
        ))
    })?;

    let log_path = log_dir.join("loopback.log");
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .await
        .map_err(|err| LoopbackProbeError::Failed(format!("Failed to open log file: {}", err)))?;
    let entry = format!(
        "[{}] dns_ok={} volumes_ok={} notes={}\n",
        Utc::now().to_rfc3339(),
        dns_ok,
        volumes_ok,
        notes.join(" | "),
    );
    file.write_all(entry.as_bytes())
        .await
        .map_err(|err| LoopbackProbeError::Failed(format!("Failed to write log: {}", err)))?;
    file.flush()
        .await
        .map_err(|err| LoopbackProbeError::Failed(format!("Failed to flush log: {}", err)))?;
    Ok((log_path, log_dir))
}

fn build_cni_env(container_id: &str, netns_path: &Path, command: &str) -> HashMap<String, String> {
    let mut env = HashMap::new();
    env.insert("CNI_COMMAND".to_string(), command.to_string());
    env.insert("CNI_CONTAINERID".to_string(), container_id.to_string());
    env.insert(
        "CNI_NETNS".to_string(),
        netns_path.to_string_lossy().to_string(),
    );
    env.insert("CNI_IFNAME".to_string(), "nanocloud0".to_string());
    env.insert("CNI_PATH".to_string(), "/opt/cni/bin".to_string());
    env
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

async fn discover_nameservers() -> Vec<String> {
    task::spawn_blocking(|| {
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
    })
    .await
    .unwrap_or_else(|err| {
        warn!(
            "Loopback probe: failed to read /etc/resolv.conf in blocking task: {}",
            err
        );
        Vec::new()
    })
}

fn diagnostics_log_dir(configured: Option<&Path>) -> PathBuf {
    if let Some(explicit) = configured {
        return explicit.to_path_buf();
    }
    env::var(LOG_DIR_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_LOG_DIR))
}

fn loopback_target_base(configured: Option<&Path>) -> PathBuf {
    if let Some(explicit) = configured {
        return explicit.to_path_buf();
    }
    env::var(TARGET_BASE_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(LOOPBACK_TARGET_BASE))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::cni::{CniReconciliationReport, CniResult};
    use crate::nanocloud::csi::{
        CreateSnapshotRequest, CreateSnapshotResponse, CreateVolumeRequest, CreateVolumeResponse,
        CsiFuture, CsiVolume, DeleteSnapshotRequest, DeleteVolumeRequest, NodePublishVolumeRequest,
        NodePublishVolumeResponse, NodeUnpublishVolumeRequest, StoredVolume,
    };
    use std::sync::{Arc as StdArc, Mutex as StdMutex};
    use tempfile::TempDir;
    use tokio::time::sleep;

    type DynResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

    #[derive(Default)]
    struct MockOci {
        pulls: StdMutex<Vec<String>>,
    }

    impl OciClient for MockOci {
        fn pull<'a>(&'a self, image: &'a str) -> BoxFuture<'a, Result<(), LoopbackProbeError>> {
            let image = image.to_string();
            Box::pin(async move {
                if let Ok(mut guard) = self.pulls.lock() {
                    guard.push(image);
                }
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct MockDns;

    impl DnsProbe for MockDns {
        fn check<'a>(
            &'a self,
            _netns_path: PathBuf,
            _cancel: CancellationToken,
        ) -> BoxFuture<'a, Result<(bool, Vec<String>), LoopbackProbeError>> {
            Box::pin(async { Ok((true, vec!["mock dns ok".to_string()])) })
        }
    }

    #[derive(Default)]
    struct MockCni {
        netns_paths: StdMutex<Vec<PathBuf>>,
    }

    impl MockCni {
        fn last_netns(&self) -> Option<PathBuf> {
            self.netns_paths
                .lock()
                .ok()
                .and_then(|paths| paths.last().cloned())
        }
    }

    impl CniPlugin for MockCni {
        fn reconcile_cni_artifacts(&self) -> DynResult<CniReconciliationReport> {
            Ok(CniReconciliationReport::default())
        }

        fn bridge(&self, _name: &str, _cidr: &str) -> DynResult<()> {
            Ok(())
        }

        fn add(&self, env: &HashMap<String, String>, _config: Vec<u8>) -> DynResult<CniResult> {
            let netns = env
                .get("CNI_NETNS")
                .map(PathBuf::from)
                .ok_or_else(|| "missing CNI_NETNS".to_string())?;
            if let Some(parent) = netns.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|err| format!("failed to create netns dir: {}", err))?;
            }
            std::fs::write(&netns, b"test-ns").map_err(|err| err.to_string())?;
            if let Ok(mut guard) = self.netns_paths.lock() {
                guard.push(netns.clone());
            }
            Ok(CniResult {
                cni_version: "1.0.0".to_string(),
                interfaces: Vec::new(),
                ips: Vec::new(),
                routes: Vec::new(),
            })
        }

        fn delete(&self, env: &HashMap<String, String>) -> DynResult<()> {
            if let Some(netns) = env.get("CNI_NETNS") {
                let _ = std::fs::remove_file(netns);
            }
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct MockCsi {
        published: StdArc<StdMutex<Vec<String>>>,
    }

    impl CsiPlugin for MockCsi {
        fn publish_root(&self) -> PathBuf {
            PathBuf::from("/tmp/mock")
        }

        fn create_volume(&self, request: CreateVolumeRequest) -> CsiFuture<CreateVolumeResponse> {
            Box::pin(async move {
                Ok(CreateVolumeResponse {
                    volume: CsiVolume {
                        volume_id: format!("vol-{}", request.name),
                        capacity_bytes: 1,
                        volume_context: HashMap::new(),
                    },
                })
            })
        }

        fn delete_volume(&self, _request: DeleteVolumeRequest) -> CsiFuture<()> {
            Box::pin(async move { Ok(()) })
        }

        fn node_publish_volume(
            &self,
            request: NodePublishVolumeRequest,
        ) -> CsiFuture<NodePublishVolumeResponse> {
            let published = self.published.clone();
            Box::pin(async move {
                let target = PathBuf::from(&request.target_path);
                std::fs::create_dir_all(&target)
                    .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
                if let Ok(mut guard) = published.lock() {
                    guard.push(request.volume_id.clone());
                }
                Ok(NodePublishVolumeResponse {
                    publish_path: request.target_path,
                })
            })
        }

        fn node_unpublish_volume(&self, _request: NodeUnpublishVolumeRequest) -> CsiFuture<()> {
            Box::pin(async move { Ok(()) })
        }

        fn create_snapshot(
            &self,
            _request: CreateSnapshotRequest,
        ) -> CsiFuture<CreateSnapshotResponse> {
            Box::pin(async move {
                Err(Box::<dyn Error + Send + Sync>::from(std::io::Error::other(
                    "not implemented",
                )))
            })
        }

        fn delete_snapshot(&self, _request: DeleteSnapshotRequest) -> CsiFuture<()> {
            Box::pin(async move { Ok(()) })
        }

        fn list_service_volumes(
            &self,
            _namespace: &str,
            _service: &str,
        ) -> DynResult<Vec<StoredVolume>> {
            Ok(Vec::new())
        }
    }

    #[derive(Clone)]
    struct SlowCsi {
        delay: Duration,
    }

    impl CsiPlugin for SlowCsi {
        fn publish_root(&self) -> PathBuf {
            PathBuf::from("/tmp/mock")
        }

        fn create_volume(&self, _request: CreateVolumeRequest) -> CsiFuture<CreateVolumeResponse> {
            let delay = self.delay;
            Box::pin(async move {
                sleep(delay).await;
                Ok(CreateVolumeResponse {
                    volume: CsiVolume {
                        volume_id: "slow-vol".to_string(),
                        capacity_bytes: 1,
                        volume_context: HashMap::new(),
                    },
                })
            })
        }

        fn delete_volume(&self, _request: DeleteVolumeRequest) -> CsiFuture<()> {
            Box::pin(async move { Ok(()) })
        }

        fn node_publish_volume(
            &self,
            request: NodePublishVolumeRequest,
        ) -> CsiFuture<NodePublishVolumeResponse> {
            let target = request.target_path.clone();
            Box::pin(async move {
                std::fs::create_dir_all(&target)
                    .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
                Ok(NodePublishVolumeResponse {
                    publish_path: target,
                })
            })
        }

        fn node_unpublish_volume(&self, _request: NodeUnpublishVolumeRequest) -> CsiFuture<()> {
            Box::pin(async move { Ok(()) })
        }

        fn create_snapshot(
            &self,
            _request: CreateSnapshotRequest,
        ) -> CsiFuture<CreateSnapshotResponse> {
            Box::pin(async move {
                Err(Box::<dyn Error + Send + Sync>::from(std::io::Error::other(
                    "not implemented",
                )))
            })
        }

        fn delete_snapshot(&self, _request: DeleteSnapshotRequest) -> CsiFuture<()> {
            Box::pin(async move { Ok(()) })
        }

        fn list_service_volumes(
            &self,
            _namespace: &str,
            _service: &str,
        ) -> DynResult<Vec<StoredVolume>> {
            Ok(Vec::new())
        }
    }

    struct EnvGuard {
        key: &'static str,
        original: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let original = env::var(key).ok();
            env::set_var(key, value);
            Self { key, original }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(value) = &self.original {
                env::set_var(self.key, value);
            } else {
                env::remove_var(self.key);
            }
        }
    }

    #[derive(Default, Clone)]
    struct RecordingMetrics {
        starts: StdArc<StdMutex<Vec<&'static str>>>,
    }

    impl LoopbackMetrics for RecordingMetrics {
        fn phase_start(&self, phase: &'static str) {
            if let Ok(mut guard) = self.starts.lock() {
                guard.push(phase);
            }
        }
    }

    #[derive(Default)]
    struct FailingCni;

    impl CniPlugin for FailingCni {
        fn reconcile_cni_artifacts(&self) -> DynResult<CniReconciliationReport> {
            Ok(CniReconciliationReport::default())
        }

        fn bridge(&self, _name: &str, _cidr: &str) -> DynResult<()> {
            Ok(())
        }

        fn add(&self, _env: &HashMap<String, String>, _config: Vec<u8>) -> DynResult<CniResult> {
            Err("cni add failed".into())
        }

        fn delete(&self, _env: &HashMap<String, String>) -> DynResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn loopback_probe_succeeds_with_mocks() {
        let temp = TempDir::new().expect("tempdir");
        let netns_dir = temp.path().join("netns");
        let _netns_guard =
            EnvGuard::set("NANOCLOUD_NETNS_DIR", netns_dir.to_string_lossy().as_ref());

        let cni = StdArc::new(MockCni::default());
        let deps = LoopbackProbeDeps {
            cni: cni.clone(),
            csi: StdArc::new(MockCsi::default()),
            oci: StdArc::new(MockOci::default()),
            dns: StdArc::new(MockDns),
            metrics: StdArc::new(NoopLoopbackMetrics),
        };

        let target_base = temp.path().join("targets");
        let log_dir = temp.path().join("logs");
        let config = LoopbackProbeConfig {
            image: "mock/image:tag".to_string(),
            timeout: Duration::from_secs(5),
            target_base: Some(target_base.clone()),
            log_dir: Some(log_dir.clone()),
            ..Default::default()
        };

        let result = run_loopback_probe_with(config, deps)
            .await
            .expect("probe succeeds");
        assert!(result.dns_ok);
        assert!(result.volumes_ok);
        assert!(result.notes.iter().any(|note| note.contains("mock dns ok")));

        let log_path = result.log_path.expect("log path");
        assert!(log_path.exists());
        let run_dir = log_path.parent().expect("run dir");
        let run_id = run_dir
            .file_name()
            .and_then(|name| name.to_str())
            .expect("run id");

        assert!(
            !target_base.join(run_id).exists(),
            "volume target should be cleaned up"
        );
    }

    #[tokio::test]
    async fn loopback_probe_times_out_during_volume_and_cleans_up_netns() {
        let temp = TempDir::new().expect("tempdir");
        let netns_dir = temp.path().join("netns");
        let _netns_guard =
            EnvGuard::set("NANOCLOUD_NETNS_DIR", netns_dir.to_string_lossy().as_ref());

        let cni = StdArc::new(MockCni::default());
        let deps = LoopbackProbeDeps {
            cni: cni.clone(),
            csi: StdArc::new(SlowCsi {
                delay: Duration::from_millis(100),
            }),
            oci: StdArc::new(MockOci::default()),
            dns: StdArc::new(MockDns),
            metrics: StdArc::new(NoopLoopbackMetrics),
        };

        let target_base = temp.path().join("targets");
        let log_dir = temp.path().join("logs");
        let timeouts = LoopbackPhaseTimeouts {
            pull: Duration::from_millis(5),
            cni: Duration::from_millis(10),
            csi: Duration::from_millis(10),
            dns: Duration::from_millis(5),
            volume: Duration::from_millis(5),
            log: Duration::from_millis(5),
            cleanup: Duration::from_millis(5),
        };
        let config = LoopbackProbeConfig {
            timeout: Duration::from_millis(40),
            phase_timeouts: Some(timeouts),
            target_base: Some(target_base),
            log_dir: Some(log_dir),
            ..Default::default()
        };

        let result = run_loopback_probe_with(config, deps).await;
        assert!(matches!(result, Err(LoopbackProbeError::Timeout)));

        if let Some(netns) = cni.last_netns() {
            assert!(
                !netns.exists(),
                "netns path should be removed after cleanup even on timeout"
            );
        }
    }

    #[tokio::test]
    async fn loopback_probe_fails_on_cni_error() {
        let config = LoopbackProbeConfig {
            timeout: Duration::from_secs(1),
            ..Default::default()
        };
        let deps = LoopbackProbeDeps {
            cni: StdArc::new(FailingCni),
            csi: StdArc::new(MockCsi::default()),
            oci: StdArc::new(MockOci::default()),
            dns: StdArc::new(MockDns),
            metrics: StdArc::new(NoopLoopbackMetrics),
        };
        let result = run_loopback_probe_with(config, deps).await;
        assert!(matches!(result, Err(LoopbackProbeError::Failed(_))));
    }

    #[tokio::test]
    async fn loopback_probe_unique_names_concurrent() {
        let temp = TempDir::new().expect("tempdir");
        let netns_dir = temp.path().join("netns");
        let _netns_guard =
            EnvGuard::set("NANOCLOUD_NETNS_DIR", netns_dir.to_string_lossy().as_ref());

        let cni = StdArc::new(MockCni::default());
        let deps = LoopbackProbeDeps {
            cni: cni.clone(),
            csi: StdArc::new(MockCsi::default()),
            oci: StdArc::new(MockOci::default()),
            dns: StdArc::new(MockDns),
            metrics: StdArc::new(NoopLoopbackMetrics),
        };
        let config = LoopbackProbeConfig {
            target_base: Some(temp.path().join("targets")),
            log_dir: Some(temp.path().join("logs")),
            ..Default::default()
        };

        let (a, b) = tokio::join!(
            run_loopback_probe_with(config.clone(), deps.clone()),
            run_loopback_probe_with(config, deps),
        );

        let first = a.expect("first run ok");
        let second = b.expect("second run ok");
        assert_ne!(first.log_path, second.log_path);
    }

    #[tokio::test]
    async fn loopback_probe_serializes_when_enabled() {
        let temp = TempDir::new().expect("tempdir");
        let netns_dir = temp.path().join("netns");
        let _netns_guard =
            EnvGuard::set("NANOCLOUD_NETNS_DIR", netns_dir.to_string_lossy().as_ref());

        #[derive(Default)]
        struct SlowCni {
            delay: Duration,
        }
        impl CniPlugin for SlowCni {
            fn reconcile_cni_artifacts(&self) -> DynResult<CniReconciliationReport> {
                Ok(CniReconciliationReport::default())
            }
            fn bridge(&self, _name: &str, _cidr: &str) -> DynResult<()> {
                Ok(())
            }
            fn add(
                &self,
                _env: &HashMap<String, String>,
                _config: Vec<u8>,
            ) -> DynResult<CniResult> {
                std::thread::sleep(self.delay);
                Ok(CniResult {
                    cni_version: "1.0.0".into(),
                    interfaces: Vec::new(),
                    ips: Vec::new(),
                    routes: Vec::new(),
                })
            }
            fn delete(&self, _env: &HashMap<String, String>) -> DynResult<()> {
                Ok(())
            }
        }

        let metrics = StdArc::new(RecordingMetrics::default());
        let deps = LoopbackProbeDeps {
            cni: StdArc::new(SlowCni {
                delay: Duration::from_millis(150),
            }),
            csi: StdArc::new(MockCsi::default()),
            oci: StdArc::new(MockOci::default()),
            dns: StdArc::new(MockDns),
            metrics: metrics.clone(),
        };
        let config = LoopbackProbeConfig {
            serialize_probes: true,
            log_dir: Some(temp.path().join("logs")),
            target_base: Some(temp.path().join("targets")),
            timeout: Duration::from_secs(5),
            ..Default::default()
        };

        let (a, b) = tokio::join!(
            run_loopback_probe_with(config.clone(), deps.clone()),
            run_loopback_probe_with(config, deps),
        );
        a.expect("first run ok");
        b.expect("second run ok");

        let starts = metrics.starts.lock().unwrap().clone();
        let pull_count = starts
            .iter()
            .filter(|phase| **phase == ProbePhase::PullImage.label())
            .count();
        assert_eq!(pull_count, 2, "both runs recorded pull phase start");
    }

    #[tokio::test]
    async fn namespace_guard_enters_and_restores() {
        match NamespaceGuard::enter(Path::new("/proc/self/ns/net")) {
            Ok(guard) => drop(guard),
            Err(LoopbackProbeError::Failed(msg)) if msg.contains("EPERM") => {
                // Skip when lacking permission to enter namespaces.
            }
            Err(err) => panic!("unexpected namespace error: {}", err),
        }
    }

    #[test]
    fn probe_run_id_is_unique_and_formatted() {
        let ids: Vec<_> = (0..5).map(|_| ProbeRunId::new()).collect();
        let unique: std::collections::HashSet<_> = ids.iter().map(|id| id.raw.clone()).collect();
        assert_eq!(unique.len(), ids.len());
        for id in ids {
            assert!(id.raw.starts_with("loopback-"));
            assert!(id.raw.len() > "loopback-".len());
        }
    }

    #[test]
    fn config_validation_rejects_invalid_values() {
        let mut config = LoopbackProbeConfig {
            image: "".into(),
            ..Default::default()
        };
        assert!(config.validate().is_err());

        config.image = "img:tag".into();
        config.timeout = Duration::from_secs(0);
        assert!(config.validate().is_err());

        config.timeout = Duration::from_secs(5);
        config.phase_timeouts = Some(LoopbackPhaseTimeouts {
            pull: Duration::from_secs(10),
            ..Default::default()
        });
        assert!(config.validate().is_err());
    }
}

#[cfg(feature = "diagnostics-bench")]
pub mod bench {
    use super::*;
    use crate::nanocloud::cni::{CniReconciliationReport, CniResult};
    use crate::nanocloud::csi::{
        CreateSnapshotRequest, CreateSnapshotResponse, CreateVolumeRequest, CreateVolumeResponse,
        CsiFuture, CsiVolume, DeleteSnapshotRequest, DeleteVolumeRequest, NodePublishVolumeRequest,
        NodePublishVolumeResponse, NodeUnpublishVolumeRequest, StoredVolume,
    };
    use std::sync::Arc as StdArc;
    use tempfile::tempdir;

    #[derive(Default)]
    #[allow(dead_code)]
    struct BenchCni;

    impl CniPlugin for BenchCni {
        fn reconcile_cni_artifacts(
            &self,
        ) -> Result<CniReconciliationReport, Box<dyn Error + Send + Sync>> {
            Ok(CniReconciliationReport::default())
        }
        fn bridge(&self, _name: &str, _cidr: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
            Ok(())
        }
        fn add(
            &self,
            _env: &HashMap<String, String>,
            _config: Vec<u8>,
        ) -> Result<CniResult, Box<dyn Error + Send + Sync>> {
            Ok(CniResult {
                cni_version: "1.0.0".into(),
                interfaces: Vec::new(),
                ips: Vec::new(),
                routes: Vec::new(),
            })
        }
        fn delete(
            &self,
            _env: &HashMap<String, String>,
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            Ok(())
        }
    }

    #[derive(Default)]
    #[allow(dead_code)]
    struct BenchCsi;

    impl CsiPlugin for BenchCsi {
        fn publish_root(&self) -> PathBuf {
            PathBuf::from("/tmp/mock")
        }
        fn create_volume(&self, request: CreateVolumeRequest) -> CsiFuture<CreateVolumeResponse> {
            Box::pin(async move {
                Ok(CreateVolumeResponse {
                    volume: CsiVolume {
                        volume_id: format!("bench-{}", request.name),
                        capacity_bytes: 1,
                        volume_context: HashMap::new(),
                    },
                })
            })
        }
        fn delete_volume(&self, _request: DeleteVolumeRequest) -> CsiFuture<()> {
            Box::pin(async move { Ok(()) })
        }
        fn node_publish_volume(
            &self,
            request: NodePublishVolumeRequest,
        ) -> CsiFuture<NodePublishVolumeResponse> {
            Box::pin(async move {
                std::fs::create_dir_all(&request.target_path)
                    .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
                Ok(NodePublishVolumeResponse {
                    publish_path: request.target_path,
                })
            })
        }
        fn node_unpublish_volume(&self, _request: NodeUnpublishVolumeRequest) -> CsiFuture<()> {
            Box::pin(async move { Ok(()) })
        }
        fn create_snapshot(
            &self,
            _request: CreateSnapshotRequest,
        ) -> CsiFuture<CreateSnapshotResponse> {
            Box::pin(async move {
                Err(Box::<dyn Error + Send + Sync>::from(std::io::Error::other(
                    "not implemented",
                )))
            })
        }
        fn delete_snapshot(&self, _request: DeleteSnapshotRequest) -> CsiFuture<()> {
            Box::pin(async move { Ok(()) })
        }
        fn list_service_volumes(
            &self,
            _namespace: &str,
            _service: &str,
        ) -> Result<Vec<StoredVolume>, Box<dyn Error + Send + Sync>> {
            Ok(Vec::new())
        }
    }

    /// Lightweight perf harness to exercise the probe with mocks.
    #[allow(dead_code)]
    pub async fn run_mock_bench(iterations: usize) -> Result<Duration, LoopbackProbeError> {
        let temp = tempdir().expect("bench temp");
        let deps = LoopbackProbeDeps {
            cni: StdArc::new(BenchCni),
            csi: StdArc::new(BenchCsi),
            oci: StdArc::new(DefaultOciClient),
            dns: StdArc::new(DefaultDnsProbe),
            metrics: StdArc::new(NoopLoopbackMetrics),
        };
        let config = LoopbackProbeConfig {
            log_dir: Some(temp.path().join("logs")),
            target_base: Some(temp.path().join("targets")),
            ..Default::default()
        };
        let start = Instant::now();
        for _ in 0..iterations {
            run_loopback_probe_with(config.clone(), deps.clone()).await?;
        }
        Ok(start.elapsed())
    }
}
