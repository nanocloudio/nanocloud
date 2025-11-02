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

use crate::nanocloud::logger::LogFormat;
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

/// Parse a key=value argument into a tuple, validating the format.
fn parse_key_val(s: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
        Err(format!(
            "Invalid option '{}'. Must be in key=value format.",
            s
        ))
    } else {
        Ok((parts[0].to_string(), parts[1].to_string()))
    }
}

/// A CLI tool for managing a Nanocloud.
#[derive(Parser)]
#[command(name = env!("CARGO_PKG_NAME"))]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = env!("CARGO_PKG_DESCRIPTION"))]
pub struct NanoCtl {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Set up your new Nanocloud
    Setup(SetupArgs),

    /// Use the Nanocloud CA to issue a certificate
    Ca(CaArgs),

    /// Install a service in your Nanocloud
    Install(InstallArgs),

    /// Uninstall a service leaving volumes intact
    Uninstall(UninstallArgs),

    /// Restore volumes from a snapshot artifact
    Restore(RestoreArgs),

    /// Start a service
    Start(StartArgs),

    /// Stop a service
    Stop(StartArgs),

    /// Restart a service
    Restart(StartArgs),

    /// Show the logs for a service
    Logs(LogsArgs),

    /// Execute a command in a pod
    Exec(ExecArgs),

    /// Show the status of a service or services
    Status(StatusArgs),

    /// Reconcile host-side CNI artifacts and report findings
    Diagnostics(DiagnosticsArgs),

    /// Inspect NetworkPolicy state and debugging details
    Policy(PolicyArgs),

    /// Follow controller events
    Events(EventsArgs),

    /// Manage configured devices
    Device(DeviceArgs),

    /// Run the Nanocloud HTTP server
    Server(ServerArgs),

    /// Generate a kubeconfig for Nanocloud access
    Config(KubeConfigArgs),

    /// Generate a single-use token
    Token(TokenArgs),

    /// Manage encrypted volumes on the host
    Volume(VolumeArgs),

    /// Manage bundle manifests
    Bundle(BundleArgs),
}

#[derive(Args)]
pub struct SetupArgs {
    /// Repair an existing Nanocloud setup
    #[arg(long)]
    pub repair: bool,
}

#[derive(Args)]
pub struct CaArgs {
    /// The common_name for the certificate
    #[arg()]
    pub common_name: String,

    /// A comma-separated list of subject alternate names
    #[arg(short, long)]
    pub additional: Option<String>,

    /// Print the equivalent curl command without performing the request
    #[arg(long)]
    pub curl: bool,
}

#[derive(Args)]
pub struct InstallArgs {
    /// The name of the service to install
    #[arg()]
    pub service: String,

    /// The namespace of the service (defaults to "default")
    #[arg(short, long)]
    pub namespace: Option<String>,

    /// Additional options in key=value format
    #[arg(short, long, value_parser = parse_key_val)]
    pub option: Vec<(String, String)>,

    /// Restore from backup file
    #[arg(short, long)]
    pub snapshot: Option<String>,

    /// Print the equivalent curl command without performing the request
    #[arg(long)]
    pub curl: bool,

    /// Force pulling the service image even if it exists locally
    #[arg(long)]
    pub update: bool,

    /// Generate encrypted volume keys before installation (repeat per volume)
    #[arg(long = "volume-key", value_name = "VOLUME")]
    pub volume_keys: Vec<String>,
}

#[derive(Args)]
pub struct UninstallArgs {
    /// The name of the service to uninstall
    #[arg()]
    pub service: String,

    /// The namespace of the service (defaults to "default")
    #[arg(short, long)]
    pub namespace: Option<String>,

    /// Download the latest backup tarball to this path after uninstall completes
    #[arg(short, long, value_name = "PATH")]
    pub snapshot: Option<String>,

    /// Print the equivalent curl command without performing the request
    #[arg(long)]
    pub curl: bool,
}

#[derive(Args)]
pub struct RestoreArgs {
    /// Path to the snapshot artifact tarball
    #[arg(value_name = "SNAPSHOT")]
    pub artifact: String,

    /// Map a volume claim to the host path that should be repopulated (repeatable)
    #[arg(
        short = 'm',
        long = "map",
        value_name = "CLAIM=PATH",
        value_parser = parse_key_val
    )]
    pub mappings: Vec<(String, String)>,

    /// Logical service name used for logging
    #[arg(short, long)]
    pub service: Option<String>,
}

#[derive(Args)]
pub struct StartArgs {
    /// The name of the service to start
    #[arg()]
    pub service: String,

    /// The namespace of the service (defaults to "default")
    #[arg(short, long)]
    pub namespace: Option<String>,

    /// Print the equivalent curl command without performing the request
    #[arg(long)]
    pub curl: bool,
}

#[derive(Args)]
pub struct LogsArgs {
    /// The name of the service to stream logs for
    #[arg()]
    pub service: String,

    /// The namespace of the service (defaults to "default")
    #[arg(short, long)]
    pub namespace: Option<String>,

    /// Continue streaming logs until cancelled
    #[arg(short, long)]
    pub follow: bool,

    /// Print the equivalent curl command without performing the request
    #[arg(long)]
    pub curl: bool,
}

#[derive(Args)]
pub struct ExecArgs {
    /// The namespace of the pod (defaults to "default")
    #[arg(short, long)]
    pub namespace: Option<String>,

    /// The container within the pod
    #[arg(short, long)]
    pub container: Option<String>,

    /// Pass stdin to the container. Matches kubectl's `-i/--stdin`.
    #[arg(short = 'i', long = "stdin", help = "Pass stdin to the container")]
    pub stdin: bool,

    /// Allocate a TTY for the command. Pair with `--stdin` for interactive shells.
    #[arg(
        short = 't',
        long = "tty",
        help = "Allocate a TTY for the container (use with --stdin for interactive sessions)"
    )]
    pub tty: bool,

    /// The pod to execute the command in
    #[arg()]
    pub pod: String,

    /// The command to run inside the container (provide after `--` to mirror kubectl)
    #[arg(
        value_name = "COMMAND",
        num_args = 1..,
        trailing_var_arg = true,
        allow_hyphen_values = true
    )]
    pub command: Vec<String>,
}

#[derive(Args)]
pub struct StatusArgs {
    /// The name of the service to show (omit to list all)
    #[arg()]
    pub service: Option<String>,

    /// The namespace of the service (defaults to "default")
    #[arg(short, long)]
    pub namespace: Option<String>,

    /// Show pods instead of services
    #[arg(long, conflicts_with = "service")]
    pub pods: bool,

    /// Print the equivalent curl command without performing the request
    #[arg(long)]
    pub curl: bool,
}

/// Run host-side diagnostics and cleanup for Nanocloud CNI artifacts
#[derive(Args)]
pub struct DiagnosticsArgs {
    /// Run the loopback probe (set --no-loopback to skip)
    #[arg(long = "loopback", default_value_t = true)]
    pub loopback: bool,

    /// Skip the loopback probe even when enabled by default
    #[arg(long = "no-loopback")]
    pub no_loopback: bool,

    /// Override the diagnostics image used for the loopback probe
    #[arg(long = "loopback-image", value_name = "IMAGE")]
    pub loopback_image: Option<String>,

    /// Override the loopback probe timeout (e.g. 60s, 2m, 500ms)
    #[arg(long = "loopback-timeout", value_name = "DURATION")]
    pub loopback_timeout: Option<String>,
}

#[derive(Args)]
pub struct EventsArgs {
    /// Only include events from this namespace
    #[arg(long)]
    pub namespace: Option<String>,

    /// Filter events to a single bundle/service
    #[arg(long)]
    pub bundle: Option<String>,

    /// Maximum number of events to print from the initial list
    #[arg(long, value_name = "COUNT")]
    pub limit: Option<u32>,

    /// Only include events newer than this RFC3339 timestamp or duration (e.g. 30m, 6h)
    #[arg(long, value_name = "DURATION|RFC3339")]
    pub since: Option<String>,

    /// Continue watching for new events until interrupted
    #[arg(long)]
    pub follow: bool,

    /// Filter by event level (Normal or Warning)
    #[arg(long, value_enum)]
    pub level: Option<EventLevelArg>,

    /// Filter by reason (repeatable)
    #[arg(long = "reason", value_name = "REASON")]
    pub reasons: Vec<String>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum EventLevelArg {
    Normal,
    Warning,
}

#[derive(Args)]
pub struct PolicyArgs {
    #[command(subcommand)]
    pub command: PolicyCommands,
}

#[derive(Subcommand)]
pub enum PolicyCommands {
    /// Show the resolved NetworkPolicy programming
    Debug,
}

#[derive(Args)]
pub struct ServerArgs {
    /// Address to bind the HTTPS server (e.g. 0.0.0.0:6443)
    #[arg(long, default_value = "127.0.0.1:6443")]
    pub listen: String,

    /// Format to use when emitting server logs
    #[arg(
        long = "log-format",
        value_enum,
        env = "NANOCLOUD_LOG_FORMAT",
        default_value = "text"
    )]
    pub log_format: LogFormatArg,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum LogFormatArg {
    Text,
    Json,
}

impl From<LogFormatArg> for LogFormat {
    fn from(value: LogFormatArg) -> Self {
        match value {
            LogFormatArg::Text => LogFormat::Text,
            LogFormatArg::Json => LogFormat::Json,
        }
    }
}

#[derive(Args)]
pub struct KubeConfigArgs {
    /// The user identity to embed in the kubeconfig and CSR subject
    #[arg(long, default_value = "admin")]
    pub user: String,

    /// The Kubernetes API server endpoint
    #[arg(long, default_value = "https://127.0.0.1:6443")]
    pub server: String,

    /// Hostname[:port] to contact when kubeconfig credentials are unavailable
    #[arg(long, default_value = "localhost:6443")]
    pub host: String,

    /// The logical cluster name referenced by contexts
    #[arg(long, default_value = "nanocloud")]
    pub cluster: String,

    /// Single-use token or URL produced by `nanocloud token`
    #[arg(long)]
    pub token: Option<String>,

    /// Print the equivalent curl command without performing the request
    #[arg(long)]
    pub curl: bool,
}

#[derive(Args)]
pub struct TokenArgs {
    /// The user identity to embed in the token grant
    #[arg(long, default_value = "admin")]
    pub user: String,

    /// Optional logical cluster name included in the grant payload
    #[arg(long)]
    pub cluster: Option<String>,

    /// Print the equivalent curl command without performing the request
    #[arg(long)]
    pub curl: bool,

    /// Render an ANSI QR code along with the token URL
    #[arg(long)]
    pub qr: bool,
}

#[derive(Args)]
pub struct DeviceArgs {
    #[command(subcommand)]
    pub command: DeviceCommands,
}

#[derive(Subcommand)]
pub enum DeviceCommands {
    /// List devices in a namespace
    List(DeviceListArgs),

    /// Describe a device
    Get(DeviceGetArgs),

    /// Register a new device record
    Create(DeviceCreateArgs),

    /// Remove a device record
    Delete(DeviceDeleteArgs),

    /// Issue a certificate for a device CSR
    IssueCertificate(DeviceIssueCertificateArgs),
}

#[derive(Args)]
pub struct DeviceListArgs {
    /// Namespace to scope the listing (defaults to "default")
    #[arg(short, long)]
    pub namespace: Option<String>,
}

#[derive(Args)]
pub struct DeviceGetArgs {
    /// Device record name (e.g. device-<hash>)
    #[arg()]
    pub name: String,

    /// Namespace containing the device (defaults to "default")
    #[arg(short, long)]
    pub namespace: Option<String>,
}

#[derive(Args)]
pub struct DeviceCreateArgs {
    /// Device identity hash
    #[arg()]
    pub hash: String,

    /// Namespace to create the device in (defaults to "default")
    #[arg(short, long)]
    pub namespace: Option<String>,

    /// Human-readable description
    #[arg(short, long)]
    pub description: Option<String>,
}

#[derive(Args)]
pub struct DeviceDeleteArgs {
    /// Device record name (e.g. device-<hash>)
    #[arg()]
    pub name: String,

    /// Namespace containing the device (defaults to "default")
    #[arg(short, long)]
    pub namespace: Option<String>,
}

#[derive(Args)]
pub struct DeviceIssueCertificateArgs {
    /// Path to the CSR (PEM format) to sign
    #[arg(value_name = "CSR")]
    pub csr_path: PathBuf,

    /// Namespace owning the device entry (defaults to "default")
    #[arg(short, long)]
    pub namespace: Option<String>,
}

#[derive(Args)]
pub struct VolumeArgs {
    #[command(subcommand)]
    pub command: VolumeCommands,
}

#[derive(Subcommand)]
pub enum VolumeCommands {
    /// Unlock an encrypted volume on the host
    Unlock(VolumeUnlockArgs),

    /// Lock (unmount and close) an encrypted volume
    Lock(VolumeLockArgs),
}

#[derive(Args)]
pub struct VolumeUnlockArgs {
    /// Path to the block device (loop device or disk) backing the encrypted volume
    #[arg(long)]
    pub device: String,

    /// Name of the secure-assets volume key to use
    #[arg(long = "key")]
    pub key_name: String,

    /// Container identifier used to derive mapper/mount defaults
    #[arg(long)]
    pub container: Option<String>,

    /// Volume name used to derive mapper/mount defaults
    #[arg(long)]
    pub volume: Option<String>,

    /// Override the dm-crypt mapper name
    #[arg(long)]
    pub mapper: Option<String>,

    /// Override the mount point on the host
    #[arg(long = "mount")]
    pub mount_path: Option<String>,

    /// Filesystem to mount once unlocked
    #[arg(long, default_value = "ext4")]
    pub filesystem: String,
}

#[derive(Args)]
pub struct VolumeLockArgs {
    /// Container identifier used to derive mapper/mount defaults
    #[arg(long)]
    pub container: Option<String>,

    /// Volume name used to derive mapper/mount defaults
    #[arg(long)]
    pub volume: Option<String>,

    /// dm-crypt mapper name to close
    #[arg(long)]
    pub mapper: Option<String>,

    /// Mounted path to unmount and clean up
    #[arg(long = "mount")]
    pub mount_path: Option<String>,
}

#[derive(Args)]
pub struct BundleArgs {
    #[command(subcommand)]
    pub command: BundleCommands,
}

#[derive(Subcommand)]
pub enum BundleCommands {
    /// Apply a manifest fragment to an existing bundle
    Apply(BundleApplyArgs),

    /// Export a bundle profile artifact for backups or reinstall
    Export(BundleExportArgs),
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum BundleApplyFormat {
    Json,
    Yaml,
}

impl BundleApplyFormat {
    pub fn content_type(self) -> &'static str {
        match self {
            BundleApplyFormat::Json => "application/apply-patch+json",
            BundleApplyFormat::Yaml => "application/apply-patch+yaml",
        }
    }
}

#[derive(Args)]
pub struct BundleApplyArgs {
    /// Bundle/service name to update
    #[arg()]
    pub service: String,

    /// Namespace of the bundle (defaults to \"default\")
    #[arg(short, long)]
    pub namespace: Option<String>,

    /// Path to the manifest fragment
    #[arg(
        short = 'f',
        long = "file",
        value_name = "PATH",
        conflicts_with = "stdin"
    )]
    pub file: Option<PathBuf>,

    /// Read the manifest fragment from stdin
    #[arg(long, conflicts_with = "file")]
    pub stdin: bool,

    /// Explicitly set the manifest format (inferred from extension when omitted)
    #[arg(long, value_enum)]
    pub format: Option<BundleApplyFormat>,

    /// Field manager recorded for the apply operation
    #[arg(long = "field-manager", default_value = "cli.nanocloud/v1")]
    pub field_manager: String,

    /// Override existing field managers on conflicting paths
    #[arg(long = "force")]
    pub force: bool,

    /// Validate only without persisting changes
    #[arg(long = "dry-run")]
    pub dry_run: bool,

    /// Print the equivalent curl command instead of applying
    #[arg(long)]
    pub curl: bool,
}

#[derive(Args, Debug)]
pub struct BundleExportArgs {
    /// Bundle/service name to export
    #[arg()]
    pub service: String,

    /// Namespace of the bundle (defaults to "default")
    #[arg(short, long)]
    pub namespace: Option<String>,

    /// Write the export artifact to this path
    #[arg(
        short = 'o',
        long = "output",
        value_name = "PATH",
        conflicts_with = "stdout"
    )]
    pub output: Option<PathBuf>,

    /// Stream the export artifact to stdout instead of writing a file
    #[arg(long, conflicts_with = "output")]
    pub stdout: bool,

    /// Include encrypted secrets in the export when server policy allows it
    #[arg(long = "include-secrets")]
    pub include_secrets: bool,

    /// Print the equivalent curl command without performing the request
    #[arg(long)]
    pub curl: bool,
}
