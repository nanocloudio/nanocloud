use std::env;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use chrono::Utc;
use tokio::fs;
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Default diagnostics image published alongside Nanocloud releases.
pub const DEFAULT_LOOPBACK_IMAGE: &str = "dockyard.nanocloud.io/diagnostics/loopback:latest";
pub const DEFAULT_LOOPBACK_TIMEOUT: Duration = Duration::from_secs(90);

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

pub async fn run_loopback_probe(
    config: LoopbackProbeConfig,
) -> Result<LoopbackProbeResult, LoopbackProbeError> {
    let start = Instant::now();
    let (dns_ok, mut notes) = check_dns_servers(config.timeout).await;
    if dns_ok {
        notes.push("Resolved at least one nameserver via tcp/53".to_string());
    }
    let (volumes_ok, log_path, volume_notes) = check_volume_probe().await;
    notes.extend(volume_notes);
    Ok(LoopbackProbeResult {
        dns_ok,
        volumes_ok,
        duration: start.elapsed(),
        log_path,
        notes,
        skipped: false,
    })
}

async fn check_dns_servers(timeout_budget: Duration) -> (bool, Vec<String>) {
    let servers = discover_nameservers();
    let mut notes = Vec::new();
    if servers.is_empty() {
        notes.push("No nameservers found in /etc/resolv.conf".to_string());
        return (false, notes);
    }

    let per_attempt = timeout_budget
        .min(Duration::from_secs(5))
        .max(Duration::from_secs(1));

    for server in servers {
        let target = format!("{}:53", server);
        match timeout(per_attempt, TcpStream::connect(&target)).await {
            Ok(Ok(stream)) => {
                drop(stream);
                notes.push(format!("DNS server {} reachable (tcp/53)", server));
                return (true, notes);
            }
            Ok(Err(err)) => notes.push(format!(
                "DNS server {} reported a connection error: {}",
                server, err
            )),
            Err(_) => notes.push(format!(
                "DNS server {} timed out after {:?}",
                server, per_attempt
            )),
        }
    }

    (false, notes)
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

async fn check_volume_probe() -> (bool, Option<PathBuf>, Vec<String>) {
    let mut notes = Vec::new();
    let mount_dir = env::temp_dir().join("nanocloud-loopback");
    if let Err(err) = fs::create_dir_all(&mount_dir).await {
        notes.push(format!(
            "Failed to prepare {}: {}",
            mount_dir.display(),
            err
        ));
        return (false, None, notes);
    }

    let test_file = mount_dir.join("volume-check.txt");
    let marker = format!("nanocloud-loopback-{}", Utc::now().to_rfc3339());
    if let Err(err) = fs::write(&test_file, marker.as_bytes()).await {
        notes.push(format!("Failed to write {}: {}", test_file.display(), err));
        return (false, None, notes);
    }

    match fs::read_to_string(&test_file).await {
        Ok(contents) if contents == marker => {
            notes.push(format!(
                "Successfully wrote and read {} bytes at {}",
                contents.len(),
                test_file.display()
            ));
            let log_file = mount_dir.join("loopback.log");
            let log_entry = format!(
                "[{}] volume check succeeded; wrote {} bytes to {}\n",
                Utc::now().to_rfc3339(),
                contents.len(),
                test_file.display()
            );
            if let Err(err) = fs::write(&log_file, log_entry).await {
                notes.push(format!(
                    "Failed to write loopback log file {}: {}",
                    log_file.display(),
                    err
                ));
                (true, None, notes)
            } else {
                (true, Some(log_file), notes)
            }
        }
        Ok(contents) => {
            notes.push(format!(
                "Volume check read unexpected payload ({} bytes)",
                contents.len()
            ));
            (false, None, notes)
        }
        Err(err) => {
            notes.push(format!("Failed to read {}: {}", test_file.display(), err));
            (false, None, notes)
        }
    }
}
