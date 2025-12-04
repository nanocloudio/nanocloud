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

use crate::nanocloud::network::config::{NetworkErrorClass, NetworkInstrumentation};
use crate::nanocloud::observability::metrics::{self, PolicyOperation};
use crate::nanocloud::util::error::{new_error, with_context};

use hex;
use log::{debug, error, info, Level};
use sha1::{Digest, Sha1};
use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::fmt;
use std::io::Write;
use std::net::IpAddr;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};

const BASE_CHAIN: &str = "NCLD-NP";
const NFT_FAMILY: &str = "inet";
const NFT_TABLE: &str = "nanocloud";

type AnyError = Box<dyn Error + Send + Sync>;

/// Configuration for policy programming. Uses environment defaults and validates
/// paths so we fail fast before touching nftables.
#[derive(Debug, Clone)]
pub struct PolicyConfig {
    pub nft_binary: String,
    pub record_path: Option<PathBuf>,
    pub instrumentation: NetworkInstrumentation,
}

impl PolicyConfig {
    /// Loads configuration from environment variables.
    ///
    /// - `NANOCLOUD_NFT` optionally points to the nftables binary.
    /// - `NANOCLOUD_NFT_RECORD`/`NANOCLOUD_IPTABLES_RECORD` record commands
    ///   instead of executing them (used for tests/dry-runs).
    /// - See `NetworkInstrumentation::from_env` for logging/metrics knobs.
    pub fn from_env() -> PolicyResult<Self> {
        let instrumentation = NetworkInstrumentation::from_env()
            .map_err(|err| PolicyError::validation("network instrumentation", err))?;
        let record_path = env::var("NANOCLOUD_NFT_RECORD")
            .ok()
            .or_else(|| env::var("NANOCLOUD_IPTABLES_RECORD").ok())
            .map(PathBuf::from);
        let nft_binary = env::var("NANOCLOUD_NFT").unwrap_or_else(|_| "nft".to_string());
        let config = Self {
            nft_binary,
            record_path,
            instrumentation,
        };
        config.validate()?;
        Ok(config)
    }

    /// Validates required fields and file paths.
    pub fn validate(&self) -> PolicyResult<()> {
        if self.nft_binary.trim().is_empty() {
            return Err(PolicyError::validation(
                "NANOCLOUD_NFT",
                "nft binary path must not be empty",
            ));
        }

        if let Some(path) = self.record_path.as_ref() {
            if let Some(parent) = path.parent() {
                if !parent.exists() {
                    return Err(PolicyError::validation(
                        path.display().to_string(),
                        "record path parent directory does not exist",
                    ));
                }
                if !parent.is_dir() {
                    return Err(PolicyError::validation(
                        path.display().to_string(),
                        "record path parent is not a directory",
                    ));
                }
            }
        }

        Ok(())
    }
}

/// Structured errors for policy programming operations.
#[derive(Debug)]
pub enum PolicyError {
    Validation { target: String, reason: String },
    Command { command: String, source: AnyError },
    Io { context: String, source: AnyError },
}

impl PolicyError {
    fn validation(target: impl Into<String>, reason: impl Into<String>) -> Self {
        PolicyError::Validation {
            target: target.into(),
            reason: reason.into(),
        }
    }

    fn command(command: impl Into<String>, source: AnyError) -> Self {
        PolicyError::Command {
            command: command.into(),
            source,
        }
    }

    fn io(context: impl Into<String>, source: AnyError) -> Self {
        PolicyError::Io {
            context: context.into(),
            source,
        }
    }

    /// Returns a coarse classification for logging/metrics.
    pub fn classification(&self) -> NetworkErrorClass {
        match self {
            PolicyError::Validation { .. } => NetworkErrorClass::Validation,
            PolicyError::Command { .. } => NetworkErrorClass::Command,
            PolicyError::Io { .. } => NetworkErrorClass::Io,
        }
    }
}

impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PolicyError::Validation { target, reason } => {
                write!(f, "invalid network policy input for {}: {}", target, reason)
            }
            PolicyError::Command { command, source } => {
                write!(f, "policy command `{}` failed: {}", command, source)
            }
            PolicyError::Io { context, source } => write!(f, "{}: {}", context, source),
        }
    }
}

impl Error for PolicyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            PolicyError::Validation { .. } => None,
            PolicyError::Command { source, .. } => Some(source.as_ref()),
            PolicyError::Io { source, .. } => Some(source.as_ref()),
        }
    }
}

/// Convenient alias for policy operation results.
pub type PolicyResult<T> = Result<T, PolicyError>;

/// Validates an IP address string and returns the parsed address.
fn validate_ip(address: &str) -> PolicyResult<IpAddr> {
    address.parse::<IpAddr>().map_err(|err| {
        PolicyError::validation(address.to_string(), format!("invalid IP address: {}", err))
    })
}

/// Validates that a CIDR block is well-formed.
fn validate_cidr(cidr: &str) -> PolicyResult<()> {
    if !cidr.contains('/') {
        validate_ip(cidr)?;
        return Ok(());
    }

    let mut parts = cidr.split('/');
    let ip_part = parts.next().unwrap_or_default();
    let mask_part = parts.next().unwrap_or_default();
    if parts.next().is_some() || ip_part.is_empty() || mask_part.is_empty() {
        return Err(PolicyError::validation(
            cidr.to_string(),
            "CIDR must be formatted as <ip> or <ip>/<mask>",
        ));
    }
    let addr = validate_ip(ip_part)?;
    let mask: u8 = mask_part.parse().map_err(|err| {
        PolicyError::validation(cidr.to_string(), format!("invalid CIDR mask: {}", err))
    })?;
    let max_mask = match addr {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    };
    if mask > max_mask {
        return Err(PolicyError::validation(
            cidr.to_string(),
            format!("mask must be <= {}", max_mask),
        ));
    }
    Ok(())
}

fn validate_protocol(protocol: &str) -> PolicyResult<()> {
    let normalized = protocol.to_ascii_lowercase();
    let allowed = ["tcp", "udp", "sctp", "icmp", "icmpv6"];
    if allowed.contains(&normalized.as_str()) {
        Ok(())
    } else {
        Err(PolicyError::validation(
            protocol.to_string(),
            "protocol must be one of tcp, udp, sctp, icmp, icmpv6",
        ))
    }
}

fn validate_policy_rule(chain: &PolicyChain, rule: &PolicyRule) -> PolicyResult<()> {
    if let Some(cidr) = rule.cidr.as_deref() {
        validate_cidr(cidr)?;
    }
    if let Some(protocol) = rule.protocol.as_deref() {
        validate_protocol(protocol)?;
    }
    if let Some(port) = rule.port {
        if port == 0 {
            return Err(PolicyError::validation(
                chain.name.clone(),
                "port must be greater than zero",
            ));
        }
    }
    Ok(())
}

fn validate_chain(chain: &PolicyChain) -> PolicyResult<()> {
    if chain.namespace.is_empty() || chain.pod.is_empty() {
        return Err(PolicyError::validation(
            chain.name.clone(),
            "namespace and pod must not be empty",
        ));
    }
    validate_ip(&chain.pod_ip)?;
    for rule in &chain.rules {
        validate_policy_rule(chain, rule)?;
    }
    Ok(())
}

fn validate_chains(chains: &[PolicyChain]) -> PolicyResult<()> {
    for chain in chains {
        validate_chain(chain)?;
    }
    Ok(())
}

fn namespace_hint(chains: &[PolicyChain]) -> String {
    let mut namespaces: HashSet<String> = HashSet::new();
    for chain in chains {
        namespaces.insert(chain.namespace.clone());
    }
    if namespaces.is_empty() {
        "none".to_string()
    } else if namespaces.len() == 1 {
        namespaces
            .into_iter()
            .next()
            .unwrap_or_else(|| "unknown".to_string())
    } else {
        format!("{} namespaces", namespaces.len())
    }
}

fn record_policy_failure(
    namespace: &str,
    pod: &str,
    instrumentation: &NetworkInstrumentation,
    err: &PolicyError,
) {
    if instrumentation.metrics_enabled {
        metrics::record_policy_error_classification(
            Some(namespace),
            pod,
            err.classification().as_str(),
        );
    }
    if instrumentation.should_log(Level::Error) {
        error!(
            "policy sync failed namespace={} pod={} classification={} error={}",
            namespace,
            pod,
            err.classification().as_str(),
            err
        );
    }
}

/// Traffic direction for a network policy chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PolicyDirection {
    Ingress,
    Egress,
}

impl fmt::Display for PolicyDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PolicyDirection::Ingress => write!(f, "ingress"),
            PolicyDirection::Egress => write!(f, "egress"),
        }
    }
}

impl PolicyDirection {
    /// Returns the lowercase textual representation of the direction.
    pub fn as_str(&self) -> &'static str {
        match self {
            PolicyDirection::Ingress => "ingress",
            PolicyDirection::Egress => "egress",
        }
    }
}

/// A single allow rule within a pod policy chain.
#[derive(Debug, Clone)]
pub struct PolicyRule {
    pub cidr: Option<String>,
    pub protocol: Option<String>,
    pub port: Option<u16>,
}

impl PolicyRule {
    /// Creates a rule that matches any CIDR/protocol/port.
    pub fn any() -> Self {
        Self {
            cidr: None,
            protocol: None,
            port: None,
        }
    }
}

/// A rendered nftables chain for a pod and direction.
#[derive(Debug, Clone)]
pub struct PolicyChain {
    pub name: String,
    pub namespace: String,
    pub pod: String,
    pub direction: PolicyDirection,
    pub pod_ip: String,
    pub rules: Vec<PolicyRule>,
}

impl PolicyChain {
    /// Constructs a new chain with the derived name for the namespace/pod/direction.
    pub fn new(
        namespace: &str,
        pod: &str,
        pod_ip: &str,
        direction: PolicyDirection,
        rules: Vec<PolicyRule>,
    ) -> Self {
        let name = chain_name(namespace, pod, direction);
        Self {
            name,
            namespace: namespace.to_string(),
            pod: pod.to_string(),
            direction,
            pod_ip: pod_ip.to_string(),
            rules,
        }
    }
}

/// Deterministically generates a chain name for the namespace/pod/direction.
pub fn chain_name(namespace: &str, pod: &str, direction: PolicyDirection) -> String {
    let mut hasher = Sha1::new();
    hasher.update(namespace.as_bytes());
    hasher.update(b"/");
    hasher.update(pod.as_bytes());
    match direction {
        PolicyDirection::Ingress => hasher.update(b"ingress"),
        PolicyDirection::Egress => hasher.update(b"egress"),
    }
    let digest = hex::encode(hasher.finalize());
    let suffix = &digest[..12];
    let prefix = match direction {
        PolicyDirection::Ingress => "NCLD-NPI",
        PolicyDirection::Egress => "NCLD-NPE",
    };
    format!("{}{}", prefix, suffix).to_uppercase()
}

#[derive(Debug, Clone)]
struct CommandRunner {
    config: PolicyConfig,
}

impl CommandRunner {
    fn new(config: PolicyConfig) -> Self {
        Self { config }
    }

    fn instrumentation(&self) -> &NetworkInstrumentation {
        &self.config.instrumentation
    }

    fn record_path(&self) -> Option<String> {
        env::var("NANOCLOUD_NFT_RECORD")
            .ok()
            .or_else(|| env::var("NANOCLOUD_IPTABLES_RECORD").ok())
            .or_else(|| {
                self.config
                    .record_path
                    .as_ref()
                    .map(|p| p.to_string_lossy().into())
            })
    }

    fn nft_binary(&self) -> String {
        env::var("NANOCLOUD_NFT").unwrap_or_else(|_| self.config.nft_binary.clone())
    }

    fn run<I, S>(&self, args: I) -> PolicyResult<bool>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let args_vec: Vec<String> = args
            .into_iter()
            .map(|segment| segment.as_ref().to_string())
            .collect();
        let binary = self.nft_binary();
        let command_line = format!("{} {}", binary, args_vec.join(" "));
        let record_path = self.record_path();
        if let Some(record) = record_path.as_ref() {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(record)
                .map_err(|e| PolicyError::io("Failed to open nftables record log", Box::new(e)))?;
            writeln!(file, "{}", command_line)
                .map_err(|e| PolicyError::io("Failed to write nftables record", Box::new(e)))?;
            return Ok(true);
        }

        let status = Command::new(&binary)
            .args(&args_vec)
            .status()
            .map_err(|e| {
                PolicyError::command(
                    command_line.clone(),
                    with_context(e, format!("Failed to execute {}", binary)),
                )
            })?;
        Ok(status.success())
    }

    fn health_check(&self) -> PolicyResult<()> {
        if let Some(record_path) = self.record_path() {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(record_path)
                .map_err(|e| PolicyError::io("Failed to open nftables record log", Box::new(e)))?;
            writeln!(file, "{} --version", self.nft_binary())
                .map_err(|e| PolicyError::io("Failed to write nftables record", Box::new(e)))?;
            return Ok(());
        }

        let binary = self.nft_binary();
        let command_line = format!("{} --version", binary);
        let status = Command::new(&binary)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|e| {
                PolicyError::command(
                    command_line.clone(),
                    with_context(e, format!("Failed to execute {}", binary)),
                )
            })?;
        if status.success() {
            Ok(())
        } else {
            let descriptor = status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "terminated by signal".to_string());
            Err(PolicyError::command(
                command_line,
                with_context(
                    new_error(format!("nftables exited with status {}", descriptor)),
                    "nftables unavailable",
                ),
            ))
        }
    }

    fn ensure_table(&self) -> PolicyResult<()> {
        let _ = self.run(["add", "table", NFT_FAMILY, NFT_TABLE])?;
        Ok(())
    }

    fn ensure_chain(&self, chain: &str) -> PolicyResult<()> {
        if !self.run(["add", "chain", NFT_FAMILY, NFT_TABLE, chain])? {
            self.run(["flush", "chain", NFT_FAMILY, NFT_TABLE, chain])?;
        }
        Ok(())
    }

    fn clear_chain(&self, chain: &str) -> PolicyResult<()> {
        let _ = self.run(["flush", "chain", NFT_FAMILY, NFT_TABLE, chain])?;
        Ok(())
    }

    fn delete_chain(&self, chain: &str) -> PolicyResult<()> {
        let _ = self.run(["delete", "chain", NFT_FAMILY, NFT_TABLE, chain])?;
        Ok(())
    }

    fn ensure_base_chain(&self) -> PolicyResult<()> {
        self.ensure_table()?;
        let definition = "{ type filter hook forward priority 0; policy accept; }";
        if !self.run([
            "add", "chain", NFT_FAMILY, NFT_TABLE, BASE_CHAIN, definition,
        ])? {
            self.run(["flush", "chain", NFT_FAMILY, NFT_TABLE, BASE_CHAIN])?;
        }
        Ok(())
    }

    fn append_base_jump(&self, chain: &PolicyChain) -> PolicyResult<()> {
        let mut args = vec![
            "add".to_string(),
            "rule".to_string(),
            NFT_FAMILY.to_string(),
            NFT_TABLE.to_string(),
            BASE_CHAIN.to_string(),
        ];
        match chain.direction {
            PolicyDirection::Ingress => {
                args.push("ip".to_string());
                args.push("daddr".to_string());
            }
            PolicyDirection::Egress => {
                args.push("ip".to_string());
                args.push("saddr".to_string());
            }
        }
        args.push(chain.pod_ip.clone());
        args.push("counter".to_string());
        args.push("jump".to_string());
        args.push(chain.name.clone());
        self.run(args.iter().map(|s| s.as_str()))?;
        Ok(())
    }

    fn append_allow_rule(&self, chain: &PolicyChain, rule: &PolicyRule) -> PolicyResult<()> {
        let mut args = vec![
            "add".to_string(),
            "rule".to_string(),
            NFT_FAMILY.to_string(),
            NFT_TABLE.to_string(),
            chain.name.clone(),
        ];

        if let Some(cidr) = rule.cidr.as_deref() {
            match chain.direction {
                PolicyDirection::Ingress => {
                    args.push("ip".to_string());
                    args.push("saddr".to_string());
                }
                PolicyDirection::Egress => {
                    args.push("ip".to_string());
                    args.push("daddr".to_string());
                }
            }
            args.push(cidr.to_string());
        }

        let mut protocol = rule.protocol.clone().map(|p| p.to_lowercase());
        if rule.port.is_some() && protocol.is_none() {
            protocol = Some("tcp".to_string());
        }

        if let Some(proto) = protocol.as_deref() {
            args.push(proto.to_string());
        }

        if let Some(port) = rule.port {
            if protocol.is_none() {
                args.push("tcp".to_string());
            }
            args.push("dport".to_string());
            args.push(port.to_string());
        }

        args.push("counter".to_string());
        args.push("return".to_string());
        self.run(args.iter().map(|s| s.as_str()))?;
        Ok(())
    }

    fn append_drop(&self, chain: &PolicyChain) -> PolicyResult<()> {
        self.run([
            "add",
            "rule",
            NFT_FAMILY,
            NFT_TABLE,
            &chain.name,
            "counter",
            "drop",
        ])?;
        Ok(())
    }
}

/// Programs nftables chains for pod network policies.
///
/// Requires nftables access (typically `CAP_NET_ADMIN`); set
/// `NANOCLOUD_NFT_RECORD` to a writable file to run in dry-run mode.
pub struct PolicyProgrammer {
    runner: CommandRunner,
    installed_chains: Mutex<HashSet<String>>,
}

impl PolicyProgrammer {
    /// Returns a shared, lazily-initialised programmer.
    pub fn shared() -> PolicyResult<&'static PolicyProgrammer> {
        static INSTANCE: OnceLock<PolicyProgrammer> = OnceLock::new();
        if let Some(instance) = INSTANCE.get() {
            return Ok(instance);
        }
        let instance = PolicyProgrammer::try_new()?;
        let _ = INSTANCE.set(instance);
        Ok(INSTANCE.get().expect("policy programmer initialised"))
    }

    /// Builds a new programmer using the current environment configuration.
    #[cfg(test)]
    pub fn new_from_env() -> PolicyResult<Self> {
        Self::try_new()
    }

    fn try_new() -> PolicyResult<Self> {
        let config = PolicyConfig::from_env()?;
        Ok(PolicyProgrammer {
            runner: CommandRunner::new(config),
            installed_chains: Mutex::new(HashSet::new()),
        })
    }

    /// Applies the provided policy chains, replacing any previously installed chains.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use nanocloud::nanocloud::network::policy::{
    ///     PolicyChain, PolicyDirection, PolicyProgrammer, PolicyRule,
    /// };
    ///
    /// # fn demo() -> Result<(), nanocloud::nanocloud::network::policy::PolicyError> {
    /// let chains = vec![PolicyChain::new(
    ///     "default",
    ///     "web-0",
    ///     "10.0.0.12",
    ///     PolicyDirection::Ingress,
    ///     vec![PolicyRule {
    ///         cidr: Some("10.0.0.0/24".into()),
    ///         protocol: Some("tcp".into()),
    ///         port: Some(80),
    ///     }],
    /// )];
    /// PolicyProgrammer::shared()?.sync(&chains)?;
    /// # Ok(())
    /// # }
    /// # demo().unwrap();
    /// ```
    pub fn sync(&self, chains: &[PolicyChain]) -> PolicyResult<()> {
        let (namespace_label, pod_label) = match chains.split_first() {
            None => ("batch", "none"),
            Some((first, [])) => (first.namespace.as_str(), first.pod.as_str()),
            _ => ("batch", "batch"),
        };

        let instrumentation = self.runner.instrumentation();
        if let Err(err) = validate_chains(chains) {
            record_policy_failure(namespace_label, pod_label, instrumentation, &err);
            return Err(err);
        }
        let action = || self.sync_inner(chains, instrumentation);
        let result = if instrumentation.metrics_enabled {
            metrics::observe_policy_operation(
                Some(namespace_label),
                pod_label,
                PolicyOperation::Sync,
                action,
            )
        } else {
            action()
        };

        if let Err(ref err) = result {
            record_policy_failure(namespace_label, pod_label, instrumentation, err);
        }

        result
    }

    fn sync_inner(
        &self,
        chains: &[PolicyChain],
        instrumentation: &NetworkInstrumentation,
    ) -> PolicyResult<()> {
        if instrumentation.should_log(Level::Info) {
            info!(
                "Syncing policy chains count={} namespaces={}",
                chains.len(),
                namespace_hint(chains)
            );
        }
        self.runner.health_check()?;
        self.runner.ensure_base_chain()?;
        self.runner.clear_chain(BASE_CHAIN)?;

        let desired_names: HashSet<String> =
            chains.iter().map(|chain| chain.name.clone()).collect();

        let mut installed = self.installed_chains.lock().expect("policy lock poisoned");
        for name in installed.difference(&desired_names) {
            if instrumentation.should_log(Level::Debug) {
                debug!("Removing stale policy chain {}", name);
            }
            self.runner.delete_chain(name)?;
        }

        for chain in chains {
            if instrumentation.should_log(Level::Debug) {
                debug!(
                    "Programming policy chain {} namespace={} pod={} direction={} rules={}",
                    chain.name,
                    chain.namespace,
                    chain.pod,
                    chain.direction,
                    chain.rules.len()
                );
            }
            self.runner.ensure_chain(&chain.name)?;
            self.runner.clear_chain(&chain.name)?;
            for rule in &chain.rules {
                self.runner.append_allow_rule(chain, rule)?;
            }
            self.runner.append_drop(chain)?;
        }

        for chain in chains {
            self.runner.append_base_jump(chain)?;
        }

        *installed = desired_names;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::observability::metrics;
    use crate::nanocloud::test_support::keyspace_lock;
    use logtest::Logger;
    use serial_test::serial;
    use std::collections::HashSet;
    use std::env;
    use std::fs;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use tempfile::tempdir;

    fn restore_env(key: &str, previous: Option<String>) {
        if let Some(value) = previous {
            env::set_var(key, value);
        } else {
            env::remove_var(key);
        }
    }

    #[test]
    #[serial]
    fn sync_programs_ingress_rules() {
        let _guard = keyspace_lock().lock();
        let dir = tempdir().expect("tempdir");
        let log_path = dir.path().join("nft.log");
        let previous_record = env::var("NANOCLOUD_NFT_RECORD").ok();
        let previous_binary = env::var("NANOCLOUD_NFT").ok();
        env::set_var("NANOCLOUD_NFT_RECORD", &log_path);
        env::set_var("NANOCLOUD_NFT", "/usr/sbin/nft");

        let programmer = PolicyProgrammer {
            runner: CommandRunner::new(PolicyConfig::from_env().expect("policy config")),
            installed_chains: Mutex::new(HashSet::new()),
        };

        let chain = PolicyChain::new(
            "default",
            "web-0",
            "10.203.0.10",
            PolicyDirection::Ingress,
            vec![PolicyRule {
                cidr: Some("10.1.0.0/24".to_string()),
                protocol: Some("tcp".to_string()),
                port: Some(80),
            }],
        );

        programmer.sync(&[chain]).expect("sync policy");

        let log = fs::read_to_string(&log_path).expect("read nft log");
        assert!(
            log.contains("nft add rule inet nanocloud NCLD-NP ip daddr 10.203.0.10 counter jump"),
            "expected base jump in log: {log}"
        );
        assert!(
            log.contains("tcp dport 80 counter return"),
            "expected port match in log: {log}"
        );
        assert!(
            log.contains("counter drop"),
            "expected drop rule in log: {log}"
        );

        programmer.sync(&[]).expect("sync empty policy set");

        let updated = fs::read_to_string(&log_path).expect("read updated nft log");
        assert!(
            updated.contains("delete chain inet nanocloud"),
            "expected chain deletion in log: {updated}"
        );

        restore_env("NANOCLOUD_NFT_RECORD", previous_record);
        restore_env("NANOCLOUD_NFT", previous_binary);
    }

    #[test]
    #[serial]
    fn sync_programs_egress_rules_with_default_tcp() {
        let _guard = keyspace_lock().lock();
        let dir = tempdir().expect("tempdir");
        let log_path = dir.path().join("nft-egress.log");
        let previous_record = env::var("NANOCLOUD_NFT_RECORD").ok();
        let previous_binary = env::var("NANOCLOUD_NFT").ok();
        env::set_var("NANOCLOUD_NFT_RECORD", &log_path);
        env::set_var("NANOCLOUD_NFT", "/usr/sbin/nft");

        let programmer = PolicyProgrammer {
            runner: CommandRunner::new(PolicyConfig::from_env().expect("policy config")),
            installed_chains: Mutex::new(HashSet::new()),
        };

        let chain = PolicyChain::new(
            "default",
            "worker-0",
            "10.203.0.11",
            PolicyDirection::Egress,
            vec![PolicyRule {
                cidr: Some("0.0.0.0/0".to_string()),
                protocol: None,
                port: Some(53),
            }],
        );

        programmer.sync(&[chain]).expect("sync egress policy");

        let log = fs::read_to_string(&log_path).expect("read nft log");
        assert!(
            log.contains("ip saddr 10.203.0.11"),
            "expected egress base jump: {log}"
        );
        assert!(
            log.contains("tcp dport 53 counter return"),
            "expected default tcp port match: {log}"
        );

        restore_env("NANOCLOUD_NFT_RECORD", previous_record);
        restore_env("NANOCLOUD_NFT", previous_binary);
    }

    #[test]
    #[serial]
    fn sync_rejects_invalid_cidr() {
        let _guard = keyspace_lock().lock();
        let dir = tempdir().expect("tempdir");
        let log_path = dir.path().join("nft-invalid.log");
        let previous_record = env::var("NANOCLOUD_NFT_RECORD").ok();
        let previous_binary = env::var("NANOCLOUD_NFT").ok();
        env::set_var("NANOCLOUD_NFT_RECORD", &log_path);
        env::set_var("NANOCLOUD_NFT", "/usr/sbin/nft");

        let programmer = PolicyProgrammer {
            runner: CommandRunner::new(PolicyConfig::from_env().expect("policy config")),
            installed_chains: Mutex::new(HashSet::new()),
        };

        let chain = PolicyChain::new(
            "default",
            "web-1",
            "10.203.0.12",
            PolicyDirection::Ingress,
            vec![PolicyRule {
                cidr: Some("10.1.0.0/40".to_string()),
                protocol: Some("tcp".to_string()),
                port: Some(80),
            }],
        );

        let result = programmer.sync(&[chain]);
        assert!(
            matches!(result, Err(PolicyError::Validation { .. })),
            "expected validation error"
        );

        restore_env("NANOCLOUD_NFT_RECORD", previous_record);
        restore_env("NANOCLOUD_NFT", previous_binary);
    }

    #[test]
    #[serial]
    fn policy_logs_and_records_classification_metrics() {
        let _guard = keyspace_lock().lock();
        let dir = tempdir().expect("tempdir");
        let log_path = dir.path().join("nft-classification.log");
        let previous_record = env::var("NANOCLOUD_NFT_RECORD").ok();
        let previous_binary = env::var("NANOCLOUD_NFT").ok();
        env::set_var("NANOCLOUD_NFT_RECORD", &log_path);
        env::set_var("NANOCLOUD_NFT", "/usr/sbin/nft");

        let mut logger = Logger::start();

        let programmer = PolicyProgrammer::new_from_env().expect("policy programmer");
        let chain = PolicyChain::new(
            "default",
            "bad-pod",
            "10.203.0.200",
            PolicyDirection::Ingress,
            vec![PolicyRule {
                cidr: Some("not-a-cidr".into()),
                protocol: Some("tcp".into()),
                port: Some(8080),
            }],
        );

        let result = programmer.sync(&[chain]);
        assert!(
            result.is_err(),
            "expected validation failure to trigger classification hooks"
        );

        let metrics_text =
            String::from_utf8(metrics::gather().expect("gather metrics")).expect("utf8 metrics");
        assert!(
            metrics_text.contains("policy_error_classifications_total")
                && metrics_text.contains("classification=\"validation\""),
            "expected policy error classification metric: {metrics_text}"
        );
        assert!(
            logger.any(|record| record
                .args()
                .to_string()
                .contains("classification=validation")),
            "expected validation classification log message"
        );

        restore_env("NANOCLOUD_NFT_RECORD", previous_record);
        restore_env("NANOCLOUD_NFT", previous_binary);
    }

    #[test]
    #[serial]
    fn policy_sync_is_thread_safe() {
        let _guard = keyspace_lock().lock();
        let dir = tempdir().expect("tempdir");
        let log_path = dir.path().join("nft-concurrent.log");
        let previous_record = env::var("NANOCLOUD_NFT_RECORD").ok();
        let previous_binary = env::var("NANOCLOUD_NFT").ok();
        env::set_var("NANOCLOUD_NFT_RECORD", &log_path);
        env::set_var("NANOCLOUD_NFT", "/usr/sbin/nft");

        let programmer = Arc::new(PolicyProgrammer {
            runner: CommandRunner::new(PolicyConfig::from_env().expect("policy config")),
            installed_chains: Mutex::new(HashSet::new()),
        });

        let mut handles = Vec::new();
        for idx in 0..3 {
            let prog = programmer.clone();
            handles.push(thread::spawn(move || {
                let pod_ip = format!("10.203.0.{}", 20 + idx);
                let chain = PolicyChain::new(
                    "default",
                    &format!("pod-{idx}"),
                    &pod_ip,
                    PolicyDirection::Ingress,
                    vec![PolicyRule::any()],
                );
                prog.sync(&[chain])
            }));
        }

        for handle in handles {
            handle
                .join()
                .expect("thread join")
                .expect("policy sync should succeed");
        }

        let log = fs::read_to_string(&log_path).expect("read nft log");
        assert!(
            log.contains("NCLD-NP"),
            "expected base chain programming in log: {log}"
        );

        restore_env("NANOCLOUD_NFT_RECORD", previous_record);
        restore_env("NANOCLOUD_NFT", previous_binary);
    }
}
