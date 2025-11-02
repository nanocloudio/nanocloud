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

use clap::{Args, Parser, Subcommand};

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

    /// Run the Nanocloud HTTP server
    Server(ServerArgs),

    /// Generate a kubeconfig for Nanocloud access
    Config(KubeConfigArgs),

    /// Generate a single-use token
    Token(TokenArgs),

    /// Manage encrypted volumes on the host
    Volume(VolumeArgs),
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
pub struct DiagnosticsArgs {}

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
