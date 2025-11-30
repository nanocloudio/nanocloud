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

use crate::nanocloud::oci::runtime::{container_root_path, netns_dir};
use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::util::{is_missing_value_error, Keyspace};
use crate::nanocloud::Config;

use crate::nanocloud::logger::log_info;
use nix::unistd::geteuid;
use serde::{Deserialize, Serialize};
use serde_json::{self, Value};
use std::collections::{HashMap, HashSet};
use std::env;
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::{ErrorKind, Read};
use std::net::Ipv4Addr;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::Path;
use std::path::PathBuf;
use std::process::{Command, ExitStatus, Output};
use std::time::Instant;

mod add;
mod bridge;
pub(crate) mod nat;
mod reconcile;

use add::{add_with_runner, delete_with_runner};
use bridge::bridge_with_runner;

use self::nat::runtime_mapping_to_rule;

type DynError = Box<dyn Error + Send + Sync>;
type DynResult<T> = Result<T, DynError>;
type NatRule = (u64, Option<String>);

pub trait CommandRunner: Send + Sync + 'static {
    fn status(
        &self,
        program: &str,
        args: &[OsString],
        context: &str,
    ) -> Result<ExitStatus, DynError>;

    fn output(&self, program: &str, args: &[OsString], context: &str) -> Result<Output, DynError>;
}

#[derive(Clone, Default)]
pub struct SystemCommandRunner;

impl CommandRunner for SystemCommandRunner {
    fn status(
        &self,
        program: &str,
        args: &[OsString],
        context: &str,
    ) -> Result<ExitStatus, DynError> {
        let mut cmd = Command::new(program);
        cmd.args(args);
        cmd.status()
            .map_err(|e| with_context(e, context.to_string()))
    }

    fn output(&self, program: &str, args: &[OsString], context: &str) -> Result<Output, DynError> {
        let mut cmd = Command::new(program);
        cmd.args(args);
        cmd.output()
            .map_err(|e| with_context(e, context.to_string()))
    }
}

pub(crate) fn run_status(
    runner: &dyn CommandRunner,
    program: &str,
    args: &[OsString],
    context: impl Into<String>,
) -> DynResult<ExitStatus> {
    runner.status(program, args, &context.into())
}

pub(crate) fn run_output(
    runner: &dyn CommandRunner,
    program: &str,
    args: &[OsString],
    context: impl Into<String>,
) -> DynResult<Output> {
    runner.output(program, args, &context.into())
}

pub(crate) fn args(tokens: &[&str]) -> Vec<OsString> {
    tokens.iter().map(OsString::from).collect()
}

#[derive(Default)]
pub(crate) struct ArgsBuilder {
    args: Vec<OsString>,
}

impl ArgsBuilder {
    pub(crate) fn push(&mut self, token: impl AsRef<OsStr>) -> &mut Self {
        self.args.push(token.as_ref().to_os_string());
        self
    }

    pub(crate) fn extend(&mut self, tokens: &[&str]) -> &mut Self {
        for token in tokens {
            self.args.push(OsString::from(*token));
        }
        self
    }

    pub(crate) fn into_vec(self) -> Vec<OsString> {
        self.args
    }
}

pub(crate) fn validate_token(value: &str, field: &str) -> DynResult<()> {
    let is_safe = value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':' | '/'));
    if is_safe {
        Ok(())
    } else {
        Err(new_error(format!(
            "{} contains invalid characters: {}",
            field, value
        )))
    }
}

pub(crate) fn validate_port_forward_rule(
    rule: &PortForwardRule,
    bridge_name: Option<&str>,
    comment: &str,
) -> DynResult<()> {
    validate_token(&rule.protocol, "protocol")?;
    rule.container_ip
        .parse::<Ipv4Addr>()
        .map_err(|e| with_context(e, "Port-forward container_ip is invalid"))?;
    if let Some(host_ip) = &rule.host_ip {
        host_ip
            .parse::<Ipv4Addr>()
            .map_err(|e| with_context(e, "Port-forward host_ip is invalid"))?;
        validate_token(host_ip, "host_ip")?;
    }
    if let Some(bridge) = bridge_name {
        validate_token(bridge, "bridge name")?;
    }
    validate_token(comment, "nft comment")?;
    Ok(())
}

#[derive(Deserialize, Serialize)]
pub struct CniConfig {
    #[serde(rename = "cniVersion")]
    pub cni_version: String,
    pub name: String,
    #[serde(rename = "type")]
    pub plugin_type: String,
    #[serde(default)]
    pub bridge: Option<String>,
    #[serde(default)]
    pub ipam: Option<IpamConfig>,
    #[serde(rename = "runtimeConfig", default)]
    pub runtime_config: Option<RuntimeConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct IpamConfig {
    #[serde(default)]
    pub subnet: Option<String>,
    #[serde(default)]
    pub gateway: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct RuntimeConfig {
    #[serde(rename = "portMappings", default)]
    pub port_mappings: Vec<RuntimePortMapping>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RuntimePortMapping {
    #[serde(rename = "hostPort")]
    pub host_port: u16,
    #[serde(rename = "containerPort")]
    pub container_port: u16,
    #[serde(rename = "protocol", default)]
    pub protocol: Option<String>,
    #[serde(rename = "hostIP", default)]
    pub host_ip: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CniResult {
    pub cni_version: String,
    pub interfaces: Vec<Interface>,
    pub ips: Vec<Ip>,
    pub routes: Vec<Route>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Interface {
    pub name: String,
    pub mac: String,
    pub sandbox: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Ip {
    pub version: String,
    pub address: String,
    pub gateway: String,
    pub interface: u8,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Route {
    pub dst: String,
    pub gw: String,
}

pub struct SubnetIterator {
    current: u32,
    end: u32,
    gateway: u32,
}

impl SubnetIterator {
    fn new(subnet: &Subnet) -> Self {
        let gateway = u32::from(subnet.gateway);
        let mut current = subnet.first_host;
        if current == gateway {
            current = current.saturating_add(1);
        }

        SubnetIterator {
            current,
            end: subnet.last_host,
            gateway,
        }
    }
}

impl Iterator for SubnetIterator {
    type Item = Ipv4Addr;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current <= self.end {
            let candidate = self.current;
            self.current = self.current.saturating_add(1);
            if candidate == self.gateway {
                continue;
            }
            return Some(Ipv4Addr::from(candidate));
        }

        None
    }
}

#[derive(Clone, Debug)]
pub struct Subnet {
    pub network: Ipv4Addr,
    pub mask: u8,
    pub gateway: Ipv4Addr,
    first_host: u32,
    last_host: u32,
}

impl Subnet {
    fn new(
        cidr: &str,
        gateway_override: Option<&str>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let (network_str, netmask_str) = cidr
            .split_once('/')
            .ok_or_else(|| new_error(format!("Invalid CIDR format: {cidr}")))?;
        let parsed_ip = network_str
            .parse::<Ipv4Addr>()
            .map_err(|e| with_context(e, format!("Invalid network address in CIDR {cidr}")))?;
        let mask = netmask_str
            .parse::<u8>()
            .map_err(|e| with_context(e, format!("Invalid prefix length in CIDR {cidr}")))?;
        if mask > 32 {
            return Err(new_error(format!(
                "Prefix length must be within 0-32: {mask}"
            )));
        }

        let mask_bits = prefix_to_mask(mask);
        let network = u32::from(parsed_ip) & mask_bits;
        let broadcast = network | !mask_bits;

        let first_host = network
            .checked_add(1)
            .ok_or_else(|| new_error(format!("CIDR {cidr} has no usable host addresses")))?;
        let last_host = broadcast
            .checked_sub(1)
            .ok_or_else(|| new_error(format!("CIDR {cidr} has no usable host addresses")))?;
        if first_host > last_host {
            return Err(new_error(format!(
                "CIDR {cidr} has no usable host addresses"
            )));
        }

        let gateway = if let Some(gateway_str) = gateway_override {
            let gateway_ip = gateway_str.parse::<Ipv4Addr>().map_err(|e| {
                with_context(
                    e,
                    format!("Invalid gateway address override: {gateway_str}"),
                )
            })?;
            let gateway_u32 = u32::from(gateway_ip);
            if gateway_u32 < first_host || gateway_u32 > last_host {
                return Err(new_error(format!(
                    "Gateway {} is outside the usable host range of {}",
                    gateway_str, cidr
                )));
            }
            gateway_u32
        } else {
            first_host
        };

        if gateway == first_host && first_host == last_host {
            return Err(new_error(format!(
                "Gateway configuration for {cidr} leaves no addresses for allocation"
            )));
        }

        Ok(Subnet {
            network: Ipv4Addr::from(network),
            mask,
            gateway: Ipv4Addr::from(gateway),
            first_host,
            last_host,
        })
    }

    pub fn iter(&self) -> SubnetIterator {
        SubnetIterator::new(self)
    }
}

fn prefix_to_mask(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        (!0u32) << (32 - prefix)
    }
}

pub(crate) fn delete_link_if_exists(
    runner: &dyn CommandRunner,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let result = run_status(
        runner,
        "ip",
        &args(&["link", "show", name]),
        format!("Failed to inspect link {}", name),
    )?;
    if !result.success() {
        return Ok(());
    }

    ensure_success(
        run_status(
            runner,
            "ip",
            &args(&["link", "delete", name]),
            format!("Failed to delete existing link {}", name),
        )?,
        &format!("Failed to delete existing link {}", name),
    )
}

pub(crate) fn validate_cni_version(version: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    if SUPPORTED_CNI_VERSIONS.contains(&version) {
        Ok(())
    } else {
        Err(new_error(format!(
            "Unsupported cniVersion '{}'; supported versions: {}",
            version,
            SUPPORTED_CNI_VERSIONS.join(", ")
        )))
    }
}

pub(crate) fn validate_interface_name(
    name: &str,
    context: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if name.is_empty() {
        return Err(new_error(format!("{} must not be empty", context)));
    }
    if name.len() > 15 {
        return Err(new_error(format!(
            "{} '{}' exceeds the 15 character Linux interface limit",
            context, name
        )));
    }
    if !name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        return Err(new_error(format!(
            "{} '{}' contains invalid characters; only alphanumeric, '-' and '_' are allowed",
            context, name
        )));
    }

    Ok(())
}

pub(crate) fn ensure_binary_available(
    name: &str,
    purpose: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if find_in_path(name).is_some() {
        return Ok(());
    }

    log_info(
        "cni",
        "Required executable missing",
        &[("binary", name), ("purpose", purpose)],
    );

    Err(new_error(format!(
        "Required executable '{}' for {} not found in PATH; install it or update PATH",
        name, purpose
    )))
}

fn find_in_path(name: &str) -> Option<PathBuf> {
    if Path::new(name).components().count() > 1 {
        return Path::new(name).canonicalize().ok();
    }

    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths).find_map(|dir| {
            let candidate = dir.join(name);
            if !candidate.is_file() {
                return None;
            }

            match candidate.metadata() {
                Ok(metadata) if metadata.permissions().mode() & 0o111 != 0 => Some(candidate),
                _ => None,
            }
        })
    })
}

pub(crate) fn ensure_privileged(operation: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    if is_root() || allow_unprivileged_cni() {
        return Ok(());
    }

    Err(new_error(format!(
        "{} requires root privileges; rerun as root or set NANOCLOUD_CNI_ALLOW_UNPRIVILEGED=1 to override",
        operation
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::io::Cursor;
    use std::sync::{Mutex, OnceLock};
    use std::{fs, os::unix::process::ExitStatusExt};
    use tempfile::tempdir;
    use crate::nanocloud::cni::network::nat::configure_port_forwards;

    #[test]
    fn subnet_iterator_skips_gateway_and_broadcast() {
        let subnet = Subnet::new("10.0.0.0/29", None).unwrap();
        let assigned: Vec<Ipv4Addr> = subnet.iter().collect();

        assert_eq!(assigned.first().unwrap(), &Ipv4Addr::new(10, 0, 0, 2));
        assert_eq!(assigned.last().unwrap(), &Ipv4Addr::new(10, 0, 0, 6));
        assert!(!assigned.contains(&subnet.gateway));
        assert!(!assigned.iter().any(|ip| ip.octets() == [10, 0, 0, 7]));
    }

    #[test]
    fn subnet_iterator_handles_high_gateway() {
        let subnet = Subnet::new("10.0.0.0/29", Some("10.0.0.6")).unwrap();
        let assigned: Vec<Ipv4Addr> = subnet.iter().collect();

        assert!(assigned.contains(&Ipv4Addr::new(10, 0, 0, 1)));
        assert!(assigned.contains(&Ipv4Addr::new(10, 0, 0, 5)));
        assert!(!assigned.contains(&subnet.gateway));
    }

    #[test]
    fn subnet_rejects_gateway_outside_range() {
        let error = Subnet::new("10.0.0.0/24", Some("10.0.1.1")).unwrap_err();
        assert!(error.to_string().contains("outside the usable host range"));
    }

    #[test]
    fn subnet_rejects_prefix_without_hosts() {
        assert!(Subnet::new("10.0.0.0/31", None).is_err());
        assert!(Subnet::new("10.0.0.0/32", None).is_err());
    }

    #[test]
    fn cni_version_validation_accepts_known_values() {
        assert!(validate_cni_version("1.0.0").is_ok());
        assert!(validate_cni_version("0.4.0").is_ok());
    }

    #[test]
    fn cni_version_validation_rejects_unknown_values() {
        assert!(validate_cni_version("0.3.1").is_err());
    }

    #[test]
    fn interface_name_validation_rejects_invalid_names() {
        assert!(validate_interface_name("", "ifname").is_err());
        assert!(validate_interface_name("interface-with-very-long-name", "ifname").is_err());
        assert!(validate_interface_name("bad$name", "ifname").is_err());
        assert!(validate_interface_name("good_name-0", "ifname").is_ok());
    }

    #[test]
    fn prefix_to_mask_maps_prefix_lengths() {
        assert_eq!(prefix_to_mask(0), 0);
        assert_eq!(prefix_to_mask(24), 0xFFFFFF00);
        assert_eq!(prefix_to_mask(32), u32::MAX);
    }

    #[test]
    fn subnet_default_gateway_is_first_host() {
        let subnet = Subnet::new("192.168.1.0/30", None).expect("valid subnet");
        assert_eq!(subnet.gateway, Ipv4Addr::new(192, 168, 1, 1));
        let allocated: Vec<Ipv4Addr> = subnet.iter().collect();
        assert_eq!(allocated, vec![Ipv4Addr::new(192, 168, 1, 2)]);
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn reset_cni_keyspace() {
        let root = Config::Keyspace.get_path().join("cni");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn allow_unprivileged_cni_respects_environment_flag() {
        let _guard = env_lock().lock().unwrap();
        const VAR: &str = "NANOCLOUD_CNI_ALLOW_UNPRIVILEGED";
        let original = std::env::var(VAR).ok();

        std::env::remove_var(VAR);
        assert!(!allow_unprivileged_cni());

        std::env::set_var(VAR, "1");
        assert!(allow_unprivileged_cni());

        std::env::set_var(VAR, "0");
        assert!(!allow_unprivileged_cni());

        if let Some(value) = original {
            std::env::set_var(VAR, value);
        } else {
            std::env::remove_var(VAR);
        }
    }

    #[test]
    fn path_helpers_format_consistently() {
        assert_eq!(allocation_path("abc"), "/allocations/abc");
        assert_eq!(ip_pool_path("10.0.0.2"), "/ip-pool/10.0.0.2");
        assert_eq!(port_forward_path("container"), "/port-forwards/container");
    }

    #[test]
    fn read_keyspace_values_collects_entries_and_warnings() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();

        let valid_dir = root.join("valid");
        std::fs::create_dir(&valid_dir).expect("create valid dir");
        std::fs::write(valid_dir.join("_value_"), "value\n").expect("write value");

        let missing_value_dir = root.join("missing");
        std::fs::create_dir(&missing_value_dir).expect("create missing dir");

        let file_entry = root.join("file");
        std::fs::write(&file_entry, "").expect("write stray file");

        let mut warnings = Vec::new();
        let entries = read_keyspace_values(root, &mut warnings).expect("read keyspace");

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "valid");
        assert_eq!(entries[0].1, "value");

        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("missing value file"));
    }

    #[test]
    fn runtime_mapping_rejects_invalid_host_ip() {
        let mapping = RuntimePortMapping {
            host_port: 8080,
            container_port: 80,
            protocol: Some("tcp".to_string()),
            host_ip: Some("not-an-ip".to_string()),
        };

        assert!(nat::runtime_mapping_to_rule(&mapping, Ipv4Addr::new(10, 0, 0, 2)).is_none());
    }

    #[test]
    fn port_forward_rule_validation_rejects_unsafe_comment() {
        let rule = PortForwardRule {
            host_ip: None,
            host_port: 8080,
            container_ip: "10.0.0.2".to_string(),
            container_port: 80,
            protocol: "tcp".to_string(),
        };
        let err = validate_port_forward_rule(&rule, Some("nanobr0"), "bad\"comment");

        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("invalid characters"));
    }

    #[test]
    fn strict_reconciliation_returns_error_when_cleanup_fails() {
        let container_id = "strict-reconcile-test";
        let keyspace_root = Config::Keyspace.get_path().join("cni");
        let _ = fs::remove_dir_all(&keyspace_root);

        CNI_KEYSPACE
            .put(&allocation_path(container_id), "10.1.0.2 strictveth 0")
            .expect("write allocation");
        CNI_KEYSPACE
            .put(&ip_pool_path("10.1.0.2"), container_id)
            .expect("write ip pool");
        CNI_KEYSPACE
            .put(&port_forward_path(container_id), "[]")
            .expect("write port forward");

        let runner = FakeCommandRunner::default()
            .with_output(
                "ip",
                &["netns", "list"],
                FakeCommandRunner::output(0, "", ""),
            )
            .with_output(
                "ip",
                &["link", "show", "type", "veth"],
                FakeCommandRunner::output(0, "", ""),
            )
            .with_status("ip", &["link", "show", "strictveth"], 1)
            .with_output(
                "nft",
                &["-j", "list", "chain", "ip", "nat", "PREROUTING"],
                FakeCommandRunner::output(1, "", "nft fail"),
            )
            .with_output(
                "nft",
                &["-j", "list", "chain", "ip", "nat", "OUTPUT"],
                FakeCommandRunner::output(1, "", "nft fail"),
            );

        let result = reconcile::reconcile(&runner, true);

        assert!(result.is_err());

        let _ = fs::remove_dir_all(&keyspace_root);
    }

    #[derive(Default)]
    struct FakeCommandRunner {
        responses: Mutex<VecDeque<CommandResponse>>,
        calls: Mutex<Vec<(String, Vec<String>)>>,
    }

    enum CommandResponse {
        Status(Result<ExitStatus, DynError>),
        Output(Result<Output, DynError>),
    }

    impl FakeCommandRunner {
        fn output(code: i32, stdout: &str, stderr: &str) -> Output {
            Output {
                status: ExitStatus::from_raw(code << 8),
                stdout: stdout.as_bytes().to_vec(),
                stderr: stderr.as_bytes().to_vec(),
            }
        }

        fn with_status(self, _program: &str, _args: &[&str], code: i32) -> Self {
            self.responses
                .lock()
                .unwrap()
                .push_back(CommandResponse::Status(Ok(ExitStatus::from_raw(code << 8))));
            self
        }

        fn with_output(self, _program: &str, _args: &[&str], output: Output) -> Self {
            self.responses
                .lock()
                .unwrap()
                .push_back(CommandResponse::Output(Ok(output)));
            self
        }

        fn calls(&self) -> Vec<(String, Vec<String>)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl CommandRunner for FakeCommandRunner {
        fn status(
            &self,
            program: &str,
            args: &[OsString],
            context: &str,
        ) -> Result<ExitStatus, DynError> {
            let mut responses = self.responses.lock().unwrap();
            let mut calls = self.calls.lock().unwrap();
            calls.push((
                program.to_string(),
                args.iter()
                    .map(|arg| arg.to_string_lossy().into_owned())
                    .collect(),
            ));
            match responses.pop_front() {
                Some(CommandResponse::Status(res)) => res,
                Some(CommandResponse::Output(_)) => Err(new_error(format!(
                    "Unexpected output response for {context}"
                ))),
                None => Err(new_error("No response configured")),
            }
        }

        fn output(
            &self,
            program: &str,
            args: &[OsString],
            context: &str,
        ) -> Result<Output, DynError> {
            let mut responses = self.responses.lock().unwrap();
            let mut calls = self.calls.lock().unwrap();
            calls.push((
                program.to_string(),
                args.iter()
                    .map(|arg| arg.to_string_lossy().into_owned())
                    .collect(),
            ));
            match responses.pop_front() {
                Some(CommandResponse::Output(res)) => res,
                Some(CommandResponse::Status(_)) => Err(new_error(format!(
                    "Unexpected status response for {context}"
                ))),
                None => Err(new_error("No response configured")),
            }
        }
    }

    #[test]
    fn add_with_runner_records_allocation_and_calls_commands() {
        reset_cni_keyspace();
        let _env_guard = env_lock().lock().unwrap();
        let temp = tempdir().expect("tempdir");
        let netns_dir = temp.path().join("netns");
        fs::create_dir_all(&netns_dir).expect("netns dir");
        let netns_path = netns_dir.join("container");
        fs::write(&netns_path, "").expect("netns placeholder");
        let original_netns = std::env::var("NANOCLOUD_NETNS_DIR").ok();
        std::env::set_var("NANOCLOUD_NETNS_DIR", &netns_dir);

        let container_id = "container";
        let host_if = host_interface_name(container_id);
        let _peer_if = peer_interface_name(&host_if);

        let runner = FakeCommandRunner::default()
            .with_status("ip", &[], 1)
            .with_status("ip", &[], 0)
            .with_status("ip", &[], 0)
            .with_status("ip", &[], 0)
            .with_status("ip", &[], 0)
            .with_output(
                "ip",
                &[],
                FakeCommandRunner::output(0, "aa:bb:cc:dd:ee:ff\n", ""),
            )
            .with_status("ip", &[], 0)
            .with_status("ip", &[], 0)
            .with_status("ip", &[], 0)
            .with_status("ip", &[], 0)
            .with_status("ip", &[], 0)
            .with_output("nft", &[], FakeCommandRunner::output(0, "{\"nftables\":[]}", ""))
            .with_output("nft", &[], FakeCommandRunner::output(0, "{\"nftables\":[]}", ""));

        let mut env: HashMap<String, String> = HashMap::new();
        env.insert("CNI_COMMAND".into(), "ADD".into());
        env.insert("CNI_CONTAINERID".into(), container_id.into());
        env.insert("CNI_NETNS".into(), netns_path.to_string_lossy().into());
        env.insert("CNI_IFNAME".into(), "eth0".into());
        env.insert("CNI_PATH".into(), "/opt/cni/bin".into());

        let config = r#"{"cniVersion":"1.0.0","name":"test","type":"nanocloud"}"#;
        let result = add_with_runner(&runner, &env, Cursor::new(config)).expect("add ok");

        assert_eq!(result.interfaces[0].name, "eth0");
        assert_eq!(result.ips[0].address, "172.20.0.2/16");
        let allocation = CNI_KEYSPACE
            .get(&allocation_path(container_id))
            .expect("allocation written");
        assert!(allocation.contains("172.20.0.2"));
        assert!(allocation.contains(&host_if));
        assert_eq!(
            CNI_KEYSPACE
                .get(&ip_pool_path("172.20.0.2"))
                .expect("ip pool entry"),
            container_id
        );

        let calls = runner.calls();
        assert!(calls
            .iter()
            .any(|(program, args)| program == "nft" && args.contains(&"PREROUTING".to_string())));
        assert!(calls
            .iter()
            .any(|(program, args)| program == "nft" && args.contains(&"OUTPUT".to_string())));

        if let Some(original) = original_netns {
            std::env::set_var("NANOCLOUD_NETNS_DIR", original);
        } else {
            std::env::remove_var("NANOCLOUD_NETNS_DIR");
        }
        reset_cni_keyspace();
    }

    #[test]
    fn add_failure_rolls_back_ip_pool_entry() {
        reset_cni_keyspace();
        let _env_guard = env_lock().lock().unwrap();
        let temp = tempdir().expect("tempdir");
        let netns_dir = temp.path().join("netns");
        fs::create_dir_all(&netns_dir).expect("netns dir");
        let netns_path = netns_dir.join("fail-container");
        fs::write(&netns_path, "").expect("netns placeholder");
        let original_netns = std::env::var("NANOCLOUD_NETNS_DIR").ok();
        std::env::set_var("NANOCLOUD_NETNS_DIR", &netns_dir);

        let container_id = "fail-container";
        let host_if = host_interface_name(container_id);

        let runner = FakeCommandRunner::default()
            .with_status("ip", &[], 1)
            .with_status("ip", &[], 0)
            .with_status("ip", &[], 0)
            .with_status("ip", &[], 0)
            .with_status("ip", &[], 0)
            .with_output(
                "ip",
                &[],
                FakeCommandRunner::output(0, "aa:bb:cc:dd:ee:ff\n", ""),
            )
            .with_status("ip", &[], 1)
            .with_status("ip", &[], 1);

        let err = add::add(
            &runner,
            container_id,
            &netns_path.to_string_lossy(),
            "eth0",
            "nanocloud0",
            Subnet::new("172.20.0.0/16", None).expect("subnet"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("rename"));

        assert!(CNI_KEYSPACE
            .get(&ip_pool_path("172.20.0.2"))
            .is_err());
        assert!(CNI_KEYSPACE
            .get(&allocation_path(container_id))
            .is_err());

        let calls = runner.calls();
        assert!(calls
            .iter()
            .any(|(program, args)| program == "ip" && args.iter().any(|arg| arg == &host_if)));

        if let Some(original) = original_netns {
            std::env::set_var("NANOCLOUD_NETNS_DIR", original);
        } else {
            std::env::remove_var("NANOCLOUD_NETNS_DIR");
        }
        reset_cni_keyspace();
    }

    #[test]
    fn delete_with_runner_cleans_all_records() {
        reset_cni_keyspace();
        let container_id = "del-test";
        let host_if = host_interface_name(container_id);
        CNI_KEYSPACE
            .put(&allocation_path(container_id), &format!("10.0.0.2 {} 1", host_if))
            .expect("alloc");
        CNI_KEYSPACE
            .put(&ip_pool_path("10.0.0.2"), container_id)
            .expect("ip pool");
        CNI_KEYSPACE
            .put(&port_forward_path(container_id), "[]")
            .expect("pf entry");

        let runner = FakeCommandRunner::default()
            .with_status("ip", &[], 0)
            .with_status("ip", &[], 0)
            .with_status("ip", &[], 0)
            .with_output("nft", &[], FakeCommandRunner::output(0, "{\"nftables\":[]}", ""))
            .with_output("nft", &[], FakeCommandRunner::output(0, "{\"nftables\":[]}", ""));

        let mut env: HashMap<String, String> = HashMap::new();
        env.insert("CNI_COMMAND".into(), "DEL".into());
        env.insert("CNI_CONTAINERID".into(), container_id.into());

        delete_with_runner(&runner, &env).expect("delete ok");

        assert!(CNI_KEYSPACE
            .get(&allocation_path(container_id))
            .is_err());
        assert!(CNI_KEYSPACE
            .get(&ip_pool_path("10.0.0.2"))
            .is_err());
        assert!(CNI_KEYSPACE
            .get(&port_forward_path(container_id))
            .is_err());

        let calls = runner.calls();
        assert!(calls
            .iter()
            .any(|(program, args)| program == "ip" && args.iter().any(|arg| arg == &host_if)));
        assert!(calls
            .iter()
            .any(|(program, args)| program == "nft" && args.contains(&"PREROUTING".to_string())));
    }

    #[test]
    fn port_forward_builder_generates_expected_rules() {
        reset_cni_keyspace();
        let container_id = "pf-test";
        let rule = PortForwardRule {
            host_ip: Some("192.168.1.10".to_string()),
            host_port: 8080,
            container_ip: "10.0.0.2".to_string(),
            container_port: 80,
            protocol: "tcp".to_string(),
        };
        let runner = FakeCommandRunner::default()
            .with_output("nft", &[], FakeCommandRunner::output(0, "{\"nftables\":[]}", ""))
            .with_output("nft", &[], FakeCommandRunner::output(0, "{\"nftables\":[]}", ""))
            .with_output("nft", &[], FakeCommandRunner::output(0, "", ""))
            .with_output("nft", &[], FakeCommandRunner::output(0, "", ""))
            .with_output("nft", &[], FakeCommandRunner::output(0, "", ""))
            .with_status("nft", &[], 0)
            .with_status("nft", &[], 0);

        configure_port_forwards(&runner, container_id, "br0", vec![rule]).expect("port forward ok");

        let calls = runner.calls();
        let prerouting = calls
            .iter()
            .find(|(program, args)| {
                program == "nft"
                    && args.contains(&"PREROUTING".to_string())
                    && args.contains(&"dnat".to_string())
            })
            .expect("prerouting rule");
        assert!(prerouting.1.contains(&"192.168.1.10".to_string()));
        assert!(prerouting.1.contains(&"br0".to_string()));
        assert!(prerouting
            .1
            .iter()
            .any(|arg| arg == "10.0.0.2:80"));

        let stored = CNI_KEYSPACE
            .get(&port_forward_path(container_id))
            .expect("port forward record");
        assert!(stored.contains("prerouting_comment"));
        reset_cni_keyspace();
    }

    #[test]
    fn port_forward_validation_rejects_unsafe_input() {
        reset_cni_keyspace();
        let container_id = "bad\"id";
        let rule = PortForwardRule {
            host_ip: None,
            host_port: 8080,
            container_ip: "10.0.0.2".to_string(),
            container_port: 80,
            protocol: "tcp".to_string(),
        };
        let runner = FakeCommandRunner::default()
            .with_output("nft", &[], FakeCommandRunner::output(0, "{\"nftables\":[]}", ""))
            .with_output("nft", &[], FakeCommandRunner::output(0, "{\"nftables\":[]}", ""))
            .with_output("nft", &[], FakeCommandRunner::output(0, "", ""))
            .with_output("nft", &[], FakeCommandRunner::output(0, "", ""))
            .with_output("nft", &[], FakeCommandRunner::output(0, "", ""));

        let err = configure_port_forwards(&runner, container_id, "br0", vec![rule]).unwrap_err();
        assert!(err.to_string().contains("invalid characters"));
        assert!(CNI_KEYSPACE
            .get(&port_forward_path(container_id))
            .is_err());
        reset_cni_keyspace();
    }

    #[test]
    fn list_nat_chain_rules_surfaces_nft_failure() {
        let runner = FakeCommandRunner::default().with_output(
            "nft",
            &[],
            FakeCommandRunner::output(1, "", "boom"),
        );

        let err = nat::list_nat_chain_rules(&runner, "PREROUTING").unwrap_err();
        assert!(err.to_string().contains("PREROUTING"));
        assert!(err.to_string().contains("boom"));
    }

    #[test]
    fn reconciliation_reports_missing_resources() {
        reset_cni_keyspace();
        CNI_KEYSPACE
            .put(&allocation_path("stale"), "10.1.0.2 staleveth 0")
            .expect("alloc");
        CNI_KEYSPACE
            .put(&ip_pool_path("10.1.0.2"), "stale")
            .expect("ip pool");
        CNI_KEYSPACE
            .put(&port_forward_path("stale"), "")
            .expect("port forward");

        let runner = FakeCommandRunner::default()
            .with_output("ip", &[], FakeCommandRunner::output(0, "", ""))
            .with_output("ip", &[], FakeCommandRunner::output(0, "", ""))
            .with_output("nft", &[], FakeCommandRunner::output(0, "{\"nftables\":[]}", ""))
            .with_output("nft", &[], FakeCommandRunner::output(0, "{\"nftables\":[]}", ""))
            .with_status("ip", &[], 1);

        let report = reconcile::reconcile(&runner, false).expect("reconcile ok");
        assert_eq!(report.stale_containers.len(), 1);
        let cleanup = &report.stale_containers[0];
        assert_eq!(cleanup.released_ips, vec!["10.1.0.2".to_string()]);
        assert!(!cleanup.host_interface_removed);
        assert!(report
            .warnings
            .iter()
            .any(|w| w.contains("Port-forward record")));
        assert!(report.errors.is_empty());
        reset_cni_keyspace();
    }

    #[test]
    fn bridge_with_runner_writes_sysctls_and_commands() {
        let _env_guard = env_lock().lock().unwrap();
        let temp = tempdir().expect("tempdir");
        let sysctl_root = temp.path().join("sysctl");
        fs::create_dir_all(sysctl_root.join("net/ipv4/conf/all")).expect("sysctl all");
        fs::create_dir_all(sysctl_root.join("net/ipv4/conf/brtest")).expect("sysctl br");
        fs::create_dir_all(sysctl_root.join("net/ipv4")).expect("sysctl ipv4");
        let original_sysctl = std::env::var("NANOCLOUD_SYSCTL_ROOT").ok();
        std::env::set_var("NANOCLOUD_SYSCTL_ROOT", &sysctl_root);

        let runner = FakeCommandRunner::default()
            .with_output("ip", &[], FakeCommandRunner::output(1, "", ""))
            .with_status("ip", &[], 0)
            .with_status("ip", &[], 0)
            .with_status("ip", &[], 0)
            .with_output("nft", &[], FakeCommandRunner::output(0, "", ""))
            .with_output("nft", &[], FakeCommandRunner::output(0, "", ""))
            .with_output("nft", &[], FakeCommandRunner::output(0, "", ""))
            .with_status("nft", &[], 0)
            .with_status("nft", &[], 0);

        bridge_with_runner(&runner, "brtest", "10.1.0.1/30").expect("bridge ok");

        assert_eq!(
            fs::read_to_string(sysctl_root.join("net/ipv4/conf/all/route_localnet"))
                .expect("sysctl all")
                .trim(),
            "1"
        );
        assert_eq!(
            fs::read_to_string(sysctl_root.join("net/ipv4/conf/brtest/route_localnet"))
                .expect("sysctl br")
                .trim(),
            "1"
        );
        assert_eq!(
            fs::read_to_string(sysctl_root.join("net/ipv4/ip_forward"))
                .expect("sysctl ip_forward")
                .trim(),
            "1"
        );

        let calls = runner.calls();
        assert!(calls
            .iter()
            .any(|(program, args)| program == "ip" && args.contains(&"link".to_string())));
        assert!(calls
            .iter()
            .any(|(program, args)| program == "nft" && args.contains(&"POSTROUTING".to_string())));

        if let Some(original) = original_sysctl {
            std::env::set_var("NANOCLOUD_SYSCTL_ROOT", original);
        } else {
            std::env::remove_var("NANOCLOUD_SYSCTL_ROOT");
        }
    }

    #[test]
    fn bridge_with_runner_rolls_back_on_failure() {
        let runner = FakeCommandRunner::default()
            .with_output("ip", &[], FakeCommandRunner::output(1, "", ""))
            .with_status("ip", &[], 0)
            .with_status("ip", &[], 1)
            .with_status("ip", &[], 0);

        let err = bridge_with_runner(&runner, "brfail", "10.2.0.1/30").unwrap_err();
        assert!(err.to_string().contains("address"));
        let calls = runner.calls();
        assert!(calls
            .iter()
            .any(|(program, args)| program == "ip" && args.contains(&"delete".to_string())));
    }

    #[test]
    fn reconcile_best_effort_collects_errors_without_failing() {
        reset_cni_keyspace();
        CNI_KEYSPACE
            .put(&allocation_path("err"), "10.2.0.2 vetherr 0")
            .expect("alloc");
        CNI_KEYSPACE
            .put(&ip_pool_path("10.2.0.2"), "err")
            .expect("ip pool");

        let runner = FakeCommandRunner::default()
            .with_output("ip", &[], FakeCommandRunner::output(0, "", ""))
            .with_output("ip", &[], FakeCommandRunner::output(0, "", ""))
            .with_output(
                "nft",
                &[],
                FakeCommandRunner::output(1, "", "nft unavailable"),
            )
            .with_output(
                "nft",
                &[],
                FakeCommandRunner::output(1, "", "nft unavailable"),
            )
            .with_status("ip", &[], 1);

        let report = reconcile::reconcile(&runner, false).expect("best effort ok");
        assert!(report.errors.iter().any(|e| e.contains("nft")));
        assert_eq!(report.stale_containers.len(), 1);

        reset_cni_keyspace();
        CNI_KEYSPACE
            .put(&allocation_path("err"), "10.2.0.2 vetherr 0")
            .expect("alloc");
        CNI_KEYSPACE
            .put(&ip_pool_path("10.2.0.2"), "err")
            .expect("ip pool");

        let runner_strict = FakeCommandRunner::default()
            .with_output("ip", &[], FakeCommandRunner::output(0, "", ""))
            .with_output("ip", &[], FakeCommandRunner::output(0, "", ""))
            .with_output(
                "nft",
                &[],
                FakeCommandRunner::output(1, "", "nft unavailable"),
            )
            .with_output(
                "nft",
                &[],
                FakeCommandRunner::output(1, "", "nft unavailable"),
            )
            .with_status("ip", &[], 1);

        let strict_err = reconcile::reconcile(&runner_strict, true).unwrap_err();
        assert!(strict_err.to_string().contains("strict mode"));
        reset_cni_keyspace();
    }
}

#[derive(Debug)]
pub struct IpAssignment {
    pub addr: Ipv4Addr,
    pub mac: String,
    pub subnet: Subnet,
    pub host_if: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortForwardRule {
    pub host_ip: Option<String>,
    pub host_port: u16,
    pub container_ip: String,
    pub container_port: u16,
    pub protocol: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredPortForward {
    pub rule: PortForwardRule,
    pub prerouting_comment: String,
    pub output_comment: String,
}

#[derive(Debug, Clone)]
pub struct NftRuleCleanup {
    pub chain: String,
    pub comment: String,
}

#[derive(Debug, Clone)]
pub struct CniContainerCleanup {
    pub container_id: String,
    pub released_ips: Vec<String>,
    pub removed_allocation: bool,
    pub host_interface: Option<String>,
    pub host_interface_was_present: bool,
    pub host_interface_removed: bool,
    pub had_port_forward_entry: bool,
    pub port_forward_entry_removed: bool,
    pub removed_nat_rules: Vec<NftRuleCleanup>,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct CniReconciliationReport {
    pub stale_containers: Vec<CniContainerCleanup>,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct AllocationRecord {
    ip: Option<String>,
    host_if: Option<String>,
}

/// Facade for CNI lifecycle operations (bridge setup, ADD/DEL, reconciliation).
///
/// All operations assume root privileges (or `NANOCLOUD_CNI_ALLOW_UNPRIVILEGED=1` for tests)
/// and require the `ip` and `nft` binaries to be available on `PATH`.
/// Keyspace records:
/// - `/allocations/<container>` → space-separated string `"<ip> <host_if> <netns_created_flag>"`.
/// - `/ip-pool/<ip>` → container id owning the address.
/// - `/port-forwards/<container>` → JSON array of `StoredPortForward` entries.
pub struct Network {}

pub(crate) const CNI_KEYSPACE: Keyspace = Keyspace::new("cni");
pub(crate) const ALLOCATIONS_PREFIX: &str = "/allocations";
pub(crate) const IP_POOL_PREFIX: &str = "/ip-pool";
pub(crate) const PORT_FORWARDS_PREFIX: &str = "/port-forwards";
const SUPPORTED_CNI_VERSIONS: &[&str] = &["1.0.0", "0.4.0"];

pub(crate) fn read_keyspace_values(
    root: &Path,
    warnings: &mut Vec<String>,
) -> Result<Vec<(String, String)>, Box<dyn Error + Send + Sync>> {
    const VALUE_FILE_NAME: &str = "_value_";

    let mut entries = Vec::new();
    match fs::read_dir(root) {
        Ok(read_dir) => {
            for entry_result in read_dir {
                let entry = entry_result.map_err(|e| {
                    with_context(
                        e,
                        format!("Failed to iterate keyspace directory '{}'", root.display()),
                    )
                })?;
                let file_type = entry.file_type().map_err(|e| {
                    with_context(
                        e,
                        format!(
                            "Failed to inspect keyspace entry '{}'",
                            entry.path().display()
                        ),
                    )
                })?;
                if !file_type.is_dir() {
                    continue;
                }
                let name = match entry.file_name().into_string() {
                    Ok(name) => name,
                    Err(_) => {
                        warnings.push(format!(
                            "Encountered non-UTF8 keyspace entry under '{}'; skipping",
                            root.display()
                        ));
                        continue;
                    }
                };
                let value_path = entry.path().join(VALUE_FILE_NAME);
                if !value_path.exists() {
                    warnings.push(format!(
                        "Keyspace entry '{}' is missing value file '{}'",
                        name,
                        value_path.display()
                    ));
                    continue;
                }
                match fs::read_to_string(&value_path) {
                    Ok(contents) => entries.push((name, contents.trim().to_string())),
                    Err(err) => warnings.push(format!(
                        "Failed to read value file '{}': {}",
                        value_path.display(),
                        err
                    )),
                }
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => {
            return Err(with_context(
                err,
                format!("Failed to read keyspace directory '{}'", root.display()),
            ));
        }
    }

    Ok(entries)
}

pub(crate) fn list_network_namespaces(
    runner: &dyn CommandRunner,
) -> Result<HashSet<String>, Box<dyn Error + Send + Sync>> {
    let output = run_output(
        runner,
        "ip",
        &args(&["netns", "list"]),
        "Failed to run 'ip netns list'",
    )?;
    ensure_success(output.status, "Failed to list network namespaces")?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut namespaces = HashSet::new();
    for line in stdout.lines() {
        let name = line.split_whitespace().next().unwrap_or("").trim();
        if !name.is_empty() {
            namespaces.insert(name.to_string());
        }
    }
    Ok(namespaces)
}

pub(crate) fn list_veth_interfaces(
    runner: &dyn CommandRunner,
) -> Result<HashSet<String>, Box<dyn Error + Send + Sync>> {
    let output = run_output(
        runner,
        "ip",
        &args(&["link", "show", "type", "veth"]),
        "Failed to inspect veth interfaces",
    )?;
    ensure_success(output.status, "Failed to list veth interfaces")?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut interfaces = HashSet::new();
    for line in stdout.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            continue;
        }
        if let Some((_, rest)) = line.split_once(':') {
            if let Some(name_token) = rest.split_whitespace().next() {
                let name = name_token.split('@').next().unwrap_or(name_token);
                if !name.is_empty() {
                    interfaces.insert(name.to_string());
                }
            }
        }
    }
    Ok(interfaces)
}

pub(crate) fn allow_unprivileged_cni() -> bool {
    env::var("NANOCLOUD_CNI_ALLOW_UNPRIVILEGED")
        .map(|value| value != "0")
        .unwrap_or(false)
}

pub(crate) fn allocation_path(container_id: &str) -> String {
    format!("{}/{}", ALLOCATIONS_PREFIX, container_id)
}

pub(crate) fn ip_pool_path(ip: &str) -> String {
    format!("{}/{}", IP_POOL_PREFIX, ip)
}

pub(crate) fn port_forward_path(container_id: &str) -> String {
    format!("{}/{}", PORT_FORWARDS_PREFIX, container_id)
}

#[derive(Debug, Clone)]
struct AddEnv {
    container_id: String,
    netns: String,
    ifname: String,
}

fn parse_add_env(env: &HashMap<String, String>) -> DynResult<AddEnv> {
    let command = env
        .get("CNI_COMMAND")
        .ok_or_else(|| new_error("CNI_COMMAND not set"))?;
    if command.as_str() != "ADD" {
        return Err(new_error("CNI_COMMAND must be set to ADD"));
    }
    let container_id = env
        .get("CNI_CONTAINERID")
        .ok_or_else(|| new_error("CNI_CONTAINERID not set"))?
        .to_owned();
    let netns = env
        .get("CNI_NETNS")
        .ok_or_else(|| new_error("CNI_NETNS not set"))?
        .to_owned();
    let ifname = env
        .get("CNI_IFNAME")
        .ok_or_else(|| new_error("CNI_IFNAME not set"))?
        .to_owned();
    validate_interface_name(&ifname, "CNI_IFNAME")?;
    let _path = env
        .get("CNI_PATH")
        .ok_or_else(|| new_error("CNI_PATH not set"))?;

    Ok(AddEnv {
        container_id,
        netns,
        ifname,
    })
}

fn parse_cni_config<R: Read>(input: R) -> DynResult<CniConfig> {
    let config: CniConfig = serde_json::from_reader(input)
        .map_err(|e| with_context(e, "Failed to parse CNI configuration"))?;
    validate_cni_version(&config.cni_version)?;
    Ok(config)
}

fn desired_network(config: &CniConfig) -> DynResult<(String, Subnet)> {
    let bridge_name = config.bridge.as_deref().unwrap_or("nanocloud0").to_string();
    let subnet_cidr = config
        .ipam
        .as_ref()
        .and_then(|ipam| ipam.subnet.as_deref())
        .unwrap_or("172.20.0.0/16");
    let gateway_override = config
        .ipam
        .as_ref()
        .and_then(|ipam| ipam.gateway.as_deref());
    let subnet = Subnet::new(subnet_cidr, gateway_override)?;

    Ok((bridge_name, subnet))
}

fn build_port_forward_rules(config: &CniConfig, container_ip: Ipv4Addr) -> Vec<PortForwardRule> {
    config
        .runtime_config
        .as_ref()
        .map(|runtime| {
            runtime
                .port_mappings
                .iter()
                .filter_map(|mapping| runtime_mapping_to_rule(mapping, container_ip))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

impl Network {
    pub fn reconcile_cni_artifacts() -> Result<CniReconciliationReport, Box<dyn Error + Send + Sync>>
    {
        Self::reconcile_cni_artifacts_with_mode(false)
    }

    pub fn reconcile_cni_artifacts_with_mode(
        strict: bool,
    ) -> Result<CniReconciliationReport, Box<dyn Error + Send + Sync>> {
        ensure_binary_available("ip", "network namespace and link inspection")?;
        ensure_binary_available("nft", "nat rule inspection and cleanup")?;
        ensure_privileged("CNI reconciliation")?;
        let runner = SystemCommandRunner;
        reconcile::reconcile(&runner, strict)
    }

    pub fn bridge(name: &str, cidr: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        ensure_binary_available("ip", "bridge reconciliation")?;
        ensure_binary_available("nft", "bridge NAT rule reconciliation")?;
        ensure_privileged("bridge reconciliation")?;
        validate_interface_name(name, "bridge name")?;
        let runner = SystemCommandRunner;
        bridge_with_runner(&runner, name, cidr)
    }

    pub fn add<R: Read>(
        env: &HashMap<String, String>,
        input: R,
    ) -> Result<CniResult, Box<dyn Error + Send + Sync>> {
        ensure_binary_available("ip", "CNI add")?;
        ensure_binary_available("nft", "CNI add port forwarding")?;
        ensure_privileged("CNI ADD")?;
        let runner = SystemCommandRunner;
        add_with_runner(&runner, env, input)
    }

    pub fn delete(env: &HashMap<String, String>) -> Result<(), Box<dyn Error + Send + Sync>> {
        ensure_binary_available("ip", "CNI delete")?;
        ensure_binary_available("nft", "CNI delete port forwarding")?;
        ensure_privileged("CNI DEL")?;
        let runner = SystemCommandRunner;
        delete_with_runner(&runner, env)
    }

}

pub(crate) fn ensure_sysctl_value(
    key: &str,
    desired: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let path = sysctl_root().join(key.replace('.', "/"));

    if let Ok(current) = fs::read_to_string(&path) {
        if current.trim() == desired {
            return Ok(());
        }
    }

    fs::write(&path, desired)
        .map_err(|e| with_context(e, format!("Failed to write sysctl {}", key)))?;
    Ok(())
}

fn sysctl_root() -> PathBuf {
    env::var("NANOCLOUD_SYSCTL_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/proc/sys"))
}

pub(crate) fn ensure_success(
    status: ExitStatus,
    context: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if status.success() {
        Ok(())
    } else {
        let descriptor = status
            .code()
            .map(|code| code.to_string())
            .unwrap_or_else(|| "terminated by signal".to_string());
        Err(new_error(format!(
            "{} (exit status: {})",
            context, descriptor
        )))
    }
}

fn is_root() -> bool {
    geteuid().as_raw() == 0
}

pub(crate) fn host_interface_name(container_id: &str) -> String {
    const PREFIX: &str = "veth";
    const MAX_LEN: usize = 15;
    let remaining = MAX_LEN.saturating_sub(PREFIX.len());
    let suffix = container_id.chars().take(remaining).collect::<String>();
    format!("{}{}", PREFIX, suffix)
}

pub(crate) fn peer_interface_name(host_if: &str) -> String {
    const MAX_LEN: usize = 15;
    if host_if.len() >= MAX_LEN {
        format!("{}p", &host_if[..(MAX_LEN - 1)])
    } else {
        format!("{}p", host_if)
    }
}
