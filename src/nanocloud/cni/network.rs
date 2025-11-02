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
use std::fs;
use std::io::{ErrorKind, Read};
use std::net::Ipv4Addr;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::Path;
use std::path::PathBuf;
use std::process::{Command, ExitStatus};

type DynError = Box<dyn Error + Send + Sync>;
type DynResult<T> = Result<T, DynError>;
type NatRule = (u64, Option<String>);

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

fn delete_link_if_exists(name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let result = Command::new("ip")
        .args(["link", "show", name])
        .status()
        .map_err(|e| with_context(e, format!("Failed to inspect link {}", name)))?;
    if !result.success() {
        return Ok(());
    }

    ensure_success(
        command_status(
            {
                let mut cmd = Command::new("ip");
                cmd.args(["link", "delete", name]);
                cmd
            },
            format!("Failed to delete existing link {}", name),
        )?,
        &format!("Failed to delete existing link {}", name),
    )
}

fn validate_cni_version(version: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
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

fn validate_interface_name(name: &str, context: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
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

fn ensure_binary_available(name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    if find_in_path(name).is_some() {
        return Ok(());
    }

    Err(new_error(format!(
        "Required executable '{}' not found in PATH",
        name
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};
    use tempfile::tempdir;

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
}

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
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct CniReconciliationReport {
    pub stale_containers: Vec<CniContainerCleanup>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct AllocationRecord {
    ip: Option<String>,
    host_if: Option<String>,
}

pub struct Network {}

const CNI_KEYSPACE: Keyspace = Keyspace::new("cni");
const ALLOCATIONS_PREFIX: &str = "/allocations";
const IP_POOL_PREFIX: &str = "/ip-pool";
const PORT_FORWARDS_PREFIX: &str = "/port-forwards";
const SUPPORTED_CNI_VERSIONS: &[&str] = &["1.0.0", "0.4.0"];

fn read_keyspace_values(
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

fn list_network_namespaces() -> Result<HashSet<String>, Box<dyn Error + Send + Sync>> {
    let output = Command::new("ip")
        .args(["netns", "list"])
        .output()
        .map_err(|e| with_context(e, "Failed to run 'ip netns list'"))?;
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

fn list_veth_interfaces() -> Result<HashSet<String>, Box<dyn Error + Send + Sync>> {
    let output = Command::new("ip")
        .args(["link", "show", "type", "veth"])
        .output()
        .map_err(|e| with_context(e, "Failed to inspect veth interfaces"))?;
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

fn allow_unprivileged_cni() -> bool {
    env::var("NANOCLOUD_CNI_ALLOW_UNPRIVILEGED")
        .map(|value| value != "0")
        .unwrap_or(false)
}

fn allocation_path(container_id: &str) -> String {
    format!("{}/{}", ALLOCATIONS_PREFIX, container_id)
}

fn ip_pool_path(ip: &str) -> String {
    format!("{}/{}", IP_POOL_PREFIX, ip)
}

fn port_forward_path(container_id: &str) -> String {
    format!("{}/{}", PORT_FORWARDS_PREFIX, container_id)
}

impl Network {
    pub fn reconcile_cni_artifacts() -> Result<CniReconciliationReport, Box<dyn Error + Send + Sync>>
    {
        ensure_binary_available("ip")?;
        ensure_binary_available("nft")?;

        let keyspace_root = Config::Keyspace.get_path().join("cni");
        let allocations_root = keyspace_root.join(ALLOCATIONS_PREFIX.trim_start_matches('/'));
        let ip_pool_root = keyspace_root.join(IP_POOL_PREFIX.trim_start_matches('/'));
        let port_forwards_root = keyspace_root.join(PORT_FORWARDS_PREFIX.trim_start_matches('/'));

        let netns_names = list_network_namespaces()?;
        let veth_interfaces = list_veth_interfaces()?;

        let mut report = CniReconciliationReport::default();

        let allocation_entries = read_keyspace_values(&allocations_root, &mut report.warnings)?;
        let ip_pool_entries = read_keyspace_values(&ip_pool_root, &mut report.warnings)?;
        let port_forward_entries = read_keyspace_values(&port_forwards_root, &mut report.warnings)?;

        let mut allocations: HashMap<String, AllocationRecord> = HashMap::new();
        for (container_id, raw) in allocation_entries {
            let mut record = AllocationRecord::default();
            let mut parts = raw.split_whitespace();
            record.ip = parts
                .next()
                .filter(|value| !value.is_empty())
                .map(|value| value.to_string());
            if record.ip.is_none() {
                report.warnings.push(format!(
                    "Allocation record for '{}' is missing an IP address",
                    container_id
                ));
            }
            record.host_if = parts
                .next()
                .filter(|value| !value.is_empty())
                .map(|value| value.to_string());
            allocations.insert(container_id, record);
        }

        let mut ip_pool_by_container: HashMap<String, Vec<String>> = HashMap::new();
        for (ip, container_id) in ip_pool_entries {
            if container_id.is_empty() {
                report
                    .warnings
                    .push(format!("IP pool entry '{}' has an empty container id", ip));
                continue;
            }
            ip_pool_by_container
                .entry(container_id.clone())
                .or_default()
                .push(ip);
        }

        let mut port_forward_containers: HashSet<String> = HashSet::new();
        for (container_id, value) in port_forward_entries {
            if value.is_empty() {
                report.warnings.push(format!(
                    "Port-forward record for '{}' is empty; continuing cleanup",
                    container_id
                ));
            }
            port_forward_containers.insert(container_id);
        }

        let mut container_ids: HashSet<String> = HashSet::new();
        container_ids.extend(allocations.keys().cloned());
        container_ids.extend(ip_pool_by_container.keys().cloned());
        container_ids.extend(port_forward_containers.iter().cloned());

        let stale_ids: Vec<String> = container_ids
            .into_iter()
            .filter(|id| !netns_names.contains(id))
            .collect();

        for container_id in stale_ids {
            let allocation = allocations.remove(&container_id);
            let had_port_forward_entry = port_forward_containers.remove(&container_id);
            let mut cleanup = CniContainerCleanup {
                container_id: container_id.clone(),
                released_ips: Vec::new(),
                removed_allocation: false,
                host_interface: allocation
                    .as_ref()
                    .and_then(|record| record.host_if.clone()),
                host_interface_was_present: false,
                host_interface_removed: false,
                had_port_forward_entry,
                port_forward_entry_removed: false,
                removed_nat_rules: Vec::new(),
                errors: Vec::new(),
            };

            let mut ips = ip_pool_by_container
                .remove(&container_id)
                .unwrap_or_default();
            if let Some(record) = allocation.as_ref() {
                if let Some(ip) = record.ip.as_ref() {
                    ips.push(ip.clone());
                }
            }

            let mut seen_ips = HashSet::new();
            let mut deduped_ips = Vec::new();
            for ip in ips {
                if seen_ips.insert(ip.clone()) {
                    deduped_ips.push(ip);
                }
            }
            cleanup.released_ips = deduped_ips.clone();

            for ip in &deduped_ips {
                if let Err(err) = CNI_KEYSPACE.delete(&ip_pool_path(ip)) {
                    if !is_missing_value_error(err.as_ref()) {
                        cleanup
                            .errors
                            .push(format!("Failed to delete ip-pool entry '{}': {}", ip, err));
                    }
                }
            }

            let host_interface_name = cleanup
                .host_interface
                .clone()
                .unwrap_or_else(|| host_interface_name(&container_id));
            cleanup.host_interface = Some(host_interface_name.clone());
            cleanup.host_interface_was_present = veth_interfaces.contains(&host_interface_name);
            if let Err(err) = delete_link_if_exists(&host_interface_name) {
                cleanup.errors.push(format!(
                    "Failed to delete veth interface '{}': {}",
                    host_interface_name, err
                ));
            } else if cleanup.host_interface_was_present {
                cleanup.host_interface_removed = true;
            }

            if allocation.is_some() {
                match CNI_KEYSPACE.delete(&allocation_path(&container_id)) {
                    Ok(_) => {
                        cleanup.removed_allocation = true;
                    }
                    Err(err) => {
                        if !is_missing_value_error(err.as_ref()) {
                            cleanup
                                .errors
                                .push(format!("Failed to delete allocation record: {}", err));
                        }
                    }
                }
            }

            let prefix = format!("nanocloud-{}-", container_id);
            for chain in ["PREROUTING", "OUTPUT"] {
                match list_nat_chain_rules(chain) {
                    Ok(rules) => {
                        for (handle, comment_opt) in rules {
                            if let Some(comment) = comment_opt {
                                if comment.starts_with(&prefix) {
                                    match delete_rule_by_handle(chain, handle) {
                                        Ok(()) => cleanup.removed_nat_rules.push(NftRuleCleanup {
                                            chain: chain.to_string(),
                                            comment,
                                        }),
                                        Err(err) => cleanup.errors.push(format!(
                                            "Failed to delete nft rule '{}' in {}: {}",
                                            comment, chain, err
                                        )),
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => cleanup
                        .errors
                        .push(format!("Failed to inspect nft {} chain: {}", chain, err)),
                }
            }

            match CNI_KEYSPACE.delete(&port_forward_path(&container_id)) {
                Ok(_) => {
                    cleanup.port_forward_entry_removed = true;
                }
                Err(err) => {
                    if !is_missing_value_error(err.as_ref()) {
                        cleanup
                            .errors
                            .push(format!("Failed to delete port-forward record: {}", err));
                    }
                }
            }

            report.stale_containers.push(cleanup);
        }

        Ok(report)
    }

    pub fn bridge(name: &str, cidr: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        ensure_binary_available("ip")?;
        ensure_binary_available("nft")?;
        validate_interface_name(name, "bridge name")?;

        // Interpret the provided CIDR as the desired bridge address plus prefix
        // and derive the network portion from it so the gateway is assigned correctly.
        let (gateway_str, prefix_str) = cidr
            .split_once('/')
            .ok_or_else(|| new_error(format!("Invalid CIDR format: {cidr}")))?;
        let gateway_str = gateway_str.trim();
        let prefix_str = prefix_str.trim();

        let gateway_ip: Ipv4Addr = gateway_str
            .parse()
            .map_err(|e| with_context(e, format!("Invalid gateway address: {gateway_str}")))?;
        let prefix: u8 = prefix_str
            .parse()
            .map_err(|e| with_context(e, format!("Invalid prefix length: {prefix_str}")))?;

        let mask = (!0u32) << (32 - prefix);
        let network_u32 = u32::from(gateway_ip) & mask;
        let network_ip = Ipv4Addr::from(network_u32);
        let network_cidr = format!("{}/{}", network_ip, prefix);

        let subnet = Subnet::new(&network_cidr, Some(gateway_str))?;

        // Create a bridge interface if it doesn't already exist
        let bridge_inspect = Command::new("ip")
            .args(["link", "show", name])
            .output()
            .map_err(|e| with_context(e, format!("Failed to inspect bridge {name}")))?;
        if !bridge_inspect.status.success() {
            let status = command_status(
                {
                    let mut cmd = Command::new("ip");
                    cmd.args(["link", "add", name, "type", "bridge"]);
                    cmd
                },
                format!("Failed to run bridge creation for {name}"),
            )?;
            ensure_success(status, &format!("Failed to create bridge '{name}'"))?;
        }
        let addr_status = command_status(
            {
                let mut cmd = Command::new("ip");
                cmd.args([
                    "addr",
                    "replace",
                    &format!("{}/{}", subnet.gateway, subnet.mask),
                    "dev",
                    name,
                ]);
                cmd
            },
            format!("Failed to run address configuration for bridge {name}"),
        )?;
        ensure_success(
            addr_status,
            &format!("Failed to configure address on bridge '{name}'"),
        )?;
        let link_up_status = command_status(
            {
                let mut cmd = Command::new("ip");
                cmd.args(["link", "set", name, "up"]);
                cmd
            },
            format!("Failed to run link-up for bridge {name}"),
        )?;
        ensure_success(
            link_up_status,
            &format!("Failed to bring bridge '{name}' up"),
        )?;

        // Ensure the NAT table exists
        let nat_table_exists = Command::new("nft")
            .args(["list", "table", "ip", "nat"])
            .output()
            .map_err(|e| with_context(e, "Failed to inspect nft nat table"))?
            .status
            .success();
        if !nat_table_exists {
            let status = command_status(
                {
                    let mut cmd = Command::new("nft");
                    cmd.args(["add", "table", "ip", "nat"]);
                    cmd
                },
                "Failed to create nft nat table",
            )?;
            ensure_success(status, "Failed to ensure nft nat table exists")?;
        }

        // Ensure the postrouting chain exists
        let postrouting_chain_exists = Command::new("nft")
            .args(["list", "chain", "ip", "nat", "POSTROUTING"])
            .output()
            .map_err(|e| with_context(e, "Failed to inspect nft POSTROUTING chain"))?
            .status
            .success();
        if !postrouting_chain_exists {
            let status = command_status(
                {
                    let mut cmd = Command::new("nft");
                    cmd.args([
                        "add",
                        "chain",
                        "ip",
                        "nat",
                        "POSTROUTING",
                        "{",
                        "type",
                        "nat",
                        "hook",
                        "postrouting",
                        "priority",
                        "100",
                        ";",
                        "}",
                    ]);
                    cmd
                },
                "Failed to create nft POSTROUTING chain",
            )?;
            ensure_success(status, "Failed to ensure nft POSTROUTING chain exists")?;
        }

        // Add masquerade rule if it doesn't already exist
        let ruleset = Command::new("nft")
            .args(["list", "ruleset"])
            .output()
            .map_err(|e| with_context(e, "Failed to list nft ruleset"))?;
        let ruleset_str = String::from_utf8_lossy(&ruleset.stdout);
        if !ruleset_str.contains(&format!(
            "saddr {}/{} oifname != \"{}\" masquerade",
            subnet.network, subnet.mask, name
        )) {
            let status = command_status(
                {
                    let mut cmd = Command::new("nft");
                    cmd.args([
                        "add",
                        "rule",
                        "ip",
                        "nat",
                        "POSTROUTING",
                        "ip",
                        "saddr",
                        &format!("{}/{}", subnet.network, subnet.mask),
                        "oifname",
                        "!=",
                        name,
                        "masquerade",
                    ]);
                    cmd
                },
                "Failed to create masquerade rule",
            )?;
            ensure_success(status, "Failed to ensure masquerade rule exists")?;
        }

        let hairpin_rule_snippet = format!("fib saddr type local oifname \"{}\"", name);
        if !ruleset_str.contains(&hairpin_rule_snippet) {
            let mut cmd = Command::new("nft");
            cmd.args([
                "add",
                "rule",
                "ip",
                "nat",
                "POSTROUTING",
                "fib",
                "saddr",
                "type",
                "local",
                "oifname",
            ]);
            cmd.arg(name);
            cmd.args(["counter", "masquerade"]);
            let status = command_status(cmd, "Failed to create hairpin masquerade rule")?;
            ensure_success(status, "Failed to ensure hairpin masquerade rule exists")?;
        }

        ensure_sysctl_value("net.ipv4.conf.all.route_localnet", "1")?;
        ensure_sysctl_value(&format!("net.ipv4.conf.{}.route_localnet", name), "1")?;
        ensure_sysctl_value("net.ipv4.ip_forward", "1")?;

        Ok(())
    }

    pub fn add<R: Read>(
        env: &HashMap<String, String>,
        input: R,
    ) -> Result<CniResult, Box<dyn Error + Send + Sync>> {
        ensure_binary_available("ip")?;
        ensure_binary_available("nft")?;
        if !is_root() && !allow_unprivileged_cni() {
            return Err(new_error("Must be run as root"));
        }
        let command = env
            .get("CNI_COMMAND")
            .ok_or_else(|| new_error("CNI_COMMAND not set"))?;
        let container_id = env
            .get("CNI_CONTAINERID")
            .ok_or_else(|| new_error("CNI_CONTAINERID not set"))?;
        let netns = env
            .get("CNI_NETNS")
            .ok_or_else(|| new_error("CNI_NETNS not set"))?;
        let ifname = env
            .get("CNI_IFNAME")
            .ok_or_else(|| new_error("CNI_IFNAME not set"))?;
        validate_interface_name(ifname, "CNI_IFNAME")?;
        let _path = env
            .get("CNI_PATH")
            .ok_or_else(|| new_error("CNI_PATH not set"))?;
        let config: CniConfig = serde_json::from_reader(input)
            .map_err(|e| with_context(e, "Failed to parse CNI configuration"))?;
        validate_cni_version(&config.cni_version)?;

        if command.as_str() != "ADD" {
            return Err(new_error("CNI_COMMAND must be set to ADD"));
        }
        let bridge_name = config.bridge.as_deref().unwrap_or("nanocloud0");
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

        let (ip, netns_created) = add(container_id, netns, ifname, bridge_name, subnet.clone())?;
        let port_forward_rules = config
            .runtime_config
            .as_ref()
            .map(|runtime| {
                runtime
                    .port_mappings
                    .iter()
                    .filter_map(|mapping| runtime_mapping_to_rule(mapping, ip.addr))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let result = CniResult {
            cni_version: config.cni_version.clone(),
            interfaces: vec![Interface {
                name: ifname.to_string(),
                mac: ip.mac.to_string(),
                sandbox: netns.to_string(),
            }],
            ips: vec![Ip {
                version: "4".to_string(),
                address: format!("{}/{}", ip.addr, ip.subnet.mask),
                gateway: ip.subnet.gateway.to_string(),
                interface: 0,
            }],
            routes: vec![Route {
                dst: "0.0.0.0/0".to_string(),
                gw: ip.subnet.gateway.to_string(),
            }],
        };
        let json = serde_json::to_string(&result)
            .map_err(|e| with_context(e, "Failed to serialize CNI result"))?;
        log_info("cni", "CNI operation result", &[("json", json.as_str())]);

        let allocation_record = format!(
            "{} {} {}",
            ip.addr,
            ip.host_if,
            if netns_created { "1" } else { "0" }
        );
        CNI_KEYSPACE
            .put(&allocation_path(container_id), &allocation_record)
            .map_err(|e| with_context(e, "Failed to record IP allocation"))?;

        configure_port_forwards(container_id, bridge_name, port_forward_rules)?;

        Ok(result)
    }

    pub fn delete(env: &HashMap<String, String>) -> Result<(), Box<dyn Error + Send + Sync>> {
        ensure_binary_available("ip")?;
        ensure_binary_available("nft")?;
        if !is_root() && !allow_unprivileged_cni() {
            return Err(new_error("Must be run as root"));
        }
        let command = env
            .get("CNI_COMMAND")
            .ok_or_else(|| new_error("CNI_COMMAND not set"))?;
        let container_id = env
            .get("CNI_CONTAINERID")
            .ok_or_else(|| new_error("CNI_CONTAINERID not set"))?;

        if command.as_str() != "DEL" {
            return Err(new_error("CNI_COMMAND must be set to DEL"));
        }
        delete(container_id)?;
        clear_port_forwards(container_id)
    }
}

fn add(
    container_id: &str,
    netns_path: &str,
    ifname: &str,
    bridge_name: &str,
    subnet: Subnet,
) -> Result<(IpAssignment, bool), Box<dyn Error + Send + Sync>> {
    let netns_created = ensure_namespace(container_id, netns_path)?;
    let _netns_guard = NetnsLink::attach(container_id, netns_path)?;

    // Create veth pair on host
    let veth_host = host_interface_name(container_id);
    let veth_peer = peer_interface_name(&veth_host);
    delete_link_if_exists(&veth_host)?;
    let status = command_status(
        {
            let mut cmd = Command::new("ip");
            cmd.args([
                "link", "add", &veth_host, "type", "veth", "peer", "name", &veth_peer,
            ]);
            cmd
        },
        "Failed to run veth pair creation command",
    )?;
    ensure_success(status, "Failed to create veth pair")?;

    // Move the container end into the netns
    let status = command_status(
        {
            let mut cmd = Command::new("ip");
            cmd.args(["link", "set", &veth_peer, "netns", container_id]);
            cmd
        },
        "Failed to run veth namespace attach command",
    )?;
    ensure_success(status, "Failed to move veth peer into container namespace")?;

    // Attach host to bridge and bring up
    let status = command_status(
        {
            let mut cmd = Command::new("ip");
            cmd.args(["link", "set", &veth_host, "master", bridge_name]);
            cmd
        },
        "Failed to run bridge attach command",
    )?;
    ensure_success(status, "Failed to connect veth host interface to bridge")?;
    let status = command_status(
        {
            let mut cmd = Command::new("ip");
            cmd.args(["link", "set", &veth_host, "up"]);
            cmd
        },
        "Failed to run veth host interface up command",
    )?;
    ensure_success(status, "Failed to bring veth host interface up")?;

    // Retrieve MAC address and allocate IP address
    let mac_output = Command::new("ip")
        .args([
            "netns",
            "exec",
            container_id,
            "cat",
            &format!("/sys/class/net/{}/address", &veth_peer),
        ])
        .output()
        .map_err(|e| with_context(e, "Failed to execute MAC address read command"))?;
    ensure_success(
        mac_output.status,
        "Failed to read MAC address from container namespace",
    )?;
    let mac: String = String::from_utf8(mac_output.stdout)
        .map_err(|e| with_context(e, "Failed to decode MAC address"))?
        .trim()
        .to_owned();
    let ip = CNI_KEYSPACE
        .put_first_fit(
            IP_POOL_PREFIX,
            container_id,
            |s| {
                s.parse::<Ipv4Addr>()
                    .map_err(|e| with_context(e, format!("Failed to parse IP address {}", s)))
            },
            subnet.iter(),
        )
        .and_then(|ip| {
            ip.parse::<Ipv4Addr>()
                .map(|addr| IpAssignment {
                    addr,
                    mac,
                    subnet: subnet.clone(),
                    host_if: veth_host.clone(),
                })
                .map_err(|_| new_error("Invalid IP address allocated"))
        })?;

    // Finish configuration inside container netns
    let status = command_status(
        {
            let mut cmd = Command::new("ip");
            cmd.args([
                "netns",
                "exec",
                container_id,
                "ip",
                "link",
                "set",
                &veth_peer,
                "name",
                ifname,
            ]);
            cmd
        },
        "Failed to run veth rename inside container namespace",
    )?;
    ensure_success(
        status,
        "Failed to rename veth peer inside container namespace",
    )?;
    let status = command_status(
        {
            let mut cmd = Command::new("ip");
            cmd.args([
                "netns",
                "exec",
                container_id,
                "ip",
                "link",
                "set",
                "lo",
                "up",
            ]);
            cmd
        },
        "Failed to run loopback up command inside container namespace",
    )?;
    ensure_success(
        status,
        "Failed to bring loopback interface up inside container namespace",
    )?;
    let status = command_status(
        {
            let mut cmd = Command::new("ip");
            cmd.args([
                "netns",
                "exec",
                container_id,
                "ip",
                "link",
                "set",
                ifname,
                "up",
            ]);
            cmd
        },
        "Failed to run container interface up command",
    )?;
    ensure_success(status, "Failed to bring container interface up")?;
    let status = command_status(
        {
            let mut cmd = Command::new("ip");
            cmd.args([
                "netns",
                "exec",
                container_id,
                "ip",
                "addr",
                "add",
                &format!("{}/{}", ip.addr, ip.subnet.mask),
                "dev",
                ifname,
            ]);
            cmd
        },
        "Failed to run IP assignment inside container namespace",
    )?;
    ensure_success(
        status,
        "Failed to assign IP address inside container namespace",
    )?;
    let status = command_status(
        {
            let mut cmd = Command::new("ip");
            cmd.args([
                "netns",
                "exec",
                container_id,
                "ip",
                "route",
                "add",
                "default",
                "via",
                &ip.subnet.gateway.to_string(),
            ]);
            cmd
        },
        "Failed to run default route configuration inside container namespace",
    )?;
    ensure_success(
        status,
        "Failed to configure default route inside container namespace",
    )?;

    Ok((ip, netns_created))
}

fn runtime_mapping_to_rule(
    mapping: &RuntimePortMapping,
    container_ip: Ipv4Addr,
) -> Option<PortForwardRule> {
    if mapping.host_port == 0 || mapping.container_port == 0 {
        return None;
    }

    let protocol = mapping.protocol.as_deref().unwrap_or("tcp").to_lowercase();
    if protocol != "tcp" && protocol != "udp" {
        return None;
    }

    let host_ip = mapping
        .host_ip
        .as_ref()
        .map(|ip| ip.trim())
        .filter(|ip| !ip.is_empty())
        .map(|ip| if ip == "0.0.0.0" { "" } else { ip })
        .map(|ip| ip.to_string())
        .filter(|ip| !ip.is_empty());

    Some(PortForwardRule {
        host_ip,
        host_port: mapping.host_port,
        container_ip: container_ip.to_string(),
        container_port: mapping.container_port,
        protocol,
    })
}

fn configure_port_forwards(
    container_id: &str,
    bridge_name: &str,
    rules: Vec<PortForwardRule>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    clear_port_forwards(container_id)?;
    if rules.is_empty() {
        return Ok(());
    }

    ensure_nat_table()?;
    ensure_nat_chain("PREROUTING", "prerouting", -100)?;
    ensure_nat_chain("OUTPUT", "output", -100)?;

    let mut applied: Vec<StoredPortForward> = Vec::new();
    for (index, rule) in rules.into_iter().enumerate() {
        match add_port_forward_rule(container_id, bridge_name, index, &rule) {
            Ok(entry) => applied.push(entry),
            Err(err) => {
                for entry in applied.iter().rev() {
                    let _ = remove_port_forward_entry(entry);
                }
                return Err(err);
            }
        }
    }

    let payload = serde_json::to_string(&applied)
        .map_err(|e| with_context(e, "Failed to serialize port forward state"))?;
    CNI_KEYSPACE
        .put(&port_forward_path(container_id), &payload)
        .map_err(|e| with_context(e, "Failed to persist port forward state"))?;

    Ok(())
}

fn add_port_forward_rule(
    container_id: &str,
    bridge_name: &str,
    index: usize,
    rule: &PortForwardRule,
) -> Result<StoredPortForward, Box<dyn Error + Send + Sync>> {
    let base_comment = format!("nanocloud-{}-{}", container_id, index);
    let prerouting_comment = format!("{}-pr", base_comment);
    let output_comment = format!("{}-out", base_comment);

    apply_nat_rule(
        container_id,
        "PREROUTING",
        Some(bridge_name),
        rule,
        &prerouting_comment,
    )?;
    apply_nat_rule(container_id, "OUTPUT", None, rule, &output_comment)?;

    Ok(StoredPortForward {
        rule: rule.clone(),
        prerouting_comment,
        output_comment,
    })
}

fn apply_nat_rule(
    container_id: &str,
    chain: &str,
    bridge_name: Option<&str>,
    rule: &PortForwardRule,
    comment: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut args = vec![
        "add".to_string(),
        "rule".to_string(),
        "ip".to_string(),
        "nat".to_string(),
        chain.to_string(),
    ];

    if let Some(host_ip) = &rule.host_ip {
        args.push("ip".to_string());
        args.push("daddr".to_string());
        args.push(host_ip.clone());
    } else {
        args.push("fib".to_string());
        args.push("daddr".to_string());
        args.push("type".to_string());
        args.push("local".to_string());
    }

    if let Some(bridge) = bridge_name {
        args.push("iifname".to_string());
        args.push("!=".to_string());
        args.push(format!("\"{}\"", bridge));
    }

    args.push(rule.protocol.clone());
    args.push("dport".to_string());
    args.push(rule.host_port.to_string());

    args.push("dnat".to_string());
    args.push("to".to_string());
    args.push(format!("{}:{}", rule.container_ip, rule.container_port));

    args.push("comment".to_string());
    args.push(format!("\"{}\"", comment));

    let status = command_status(
        {
            let mut cmd = Command::new("nft");
            cmd.args(args.iter().map(|s| s.as_str()));
            cmd
        },
        format!(
            "Failed to add {} nat rule for container {}",
            chain, container_id
        ),
    )?;
    ensure_success(
        status,
        &format!(
            "Failed to add {} nat rule for container {}",
            chain, container_id
        ),
    )?;

    Ok(())
}

fn clear_port_forwards(container_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = port_forward_path(container_id);
    let stored = CNI_KEYSPACE.get(&key).ok();
    let mut stored_err: Option<Box<dyn Error + Send + Sync>> = None;

    if let Some(stored) = stored {
        match serde_json::from_str::<Vec<StoredPortForward>>(&stored) {
            Ok(entries) => {
                for entry in &entries {
                    remove_port_forward_entry(entry)?;
                }
            }
            Err(err) => {
                stored_err = Some(with_context(
                    err,
                    "Failed to parse stored port forward state",
                ));
            }
        }
        if let Err(err) = CNI_KEYSPACE.delete(&key) {
            stored_err.get_or_insert(with_context(err, "Failed to delete port forward record"));
        }
    }

    let prefix = format!("nanocloud-{}-", container_id);
    delete_rules_by_comment_prefix("PREROUTING", &prefix)?;
    delete_rules_by_comment_prefix("OUTPUT", &prefix)?;

    if let Some(err) = stored_err {
        return Err(err);
    }

    Ok(())
}

fn remove_port_forward_entry(
    entry: &StoredPortForward,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    delete_rule_by_comment("PREROUTING", &entry.prerouting_comment)?;
    delete_rule_by_comment("OUTPUT", &entry.output_comment)?;
    Ok(())
}

fn delete_rule_by_comment(chain: &str, comment: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Some(handle) =
        list_nat_chain_rules(chain)?
            .into_iter()
            .find_map(|(handle, rule_comment)| {
                if rule_comment.as_deref() == Some(comment) {
                    Some(handle)
                } else {
                    None
                }
            })
    {
        delete_rule_by_handle(chain, handle)?;
    }

    Ok(())
}

fn delete_rules_by_comment_prefix(chain: &str, prefix: &str) -> DynResult<()> {
    let handles: Vec<u64> = list_nat_chain_rules(chain)?
        .into_iter()
        .filter_map(|(handle, comment)| {
            comment
                .as_deref()
                .filter(|value| value.starts_with(prefix))
                .map(|_| handle)
        })
        .collect();

    for handle in handles {
        delete_rule_by_handle(chain, handle)?;
    }

    Ok(())
}

fn list_nat_chain_rules(chain: &str) -> DynResult<Vec<NatRule>> {
    let output = Command::new("nft")
        .args(["-j", "list", "chain", "ip", "nat", chain])
        .output()
        .map_err(|e| with_context(e, format!("Failed to list nft chain {}", chain)))?;

    if !output.status.success() {
        return Ok(Vec::new());
    }

    let data: Value = serde_json::from_slice(&output.stdout)
        .map_err(|e| with_context(e, "Failed to parse nft ruleset JSON"))?;
    let nftables = data
        .get("nftables")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut rules = Vec::new();
    for entry in nftables {
        if let Some(rule_obj) = entry.get("rule") {
            if let Some(handle) = rule_obj.get("handle").and_then(Value::as_u64) {
                let comment = rule_obj
                    .get("comment")
                    .and_then(Value::as_str)
                    .map(|value| value.to_string());
                rules.push((handle, comment));
            }
            continue;
        }

        if let Some(chain_obj) = entry.get("chain") {
            if let Some(rule_array) = chain_obj.get("rules").and_then(Value::as_array) {
                for rule in rule_array {
                    if let Some(handle) = rule.get("handle").and_then(Value::as_u64) {
                        let comment = rule
                            .get("comment")
                            .and_then(Value::as_str)
                            .map(|value| value.to_string());
                        rules.push((handle, comment));
                    }
                }
            }
        }
    }

    Ok(rules)
}

fn delete_rule_by_handle(chain: &str, handle: u64) -> Result<(), Box<dyn Error + Send + Sync>> {
    ensure_success(
        command_status(
            {
                let mut cmd = Command::new("nft");
                cmd.args([
                    "delete",
                    "rule",
                    "ip",
                    "nat",
                    chain,
                    "handle",
                    &handle.to_string(),
                ]);
                cmd
            },
            format!(
                "Failed to execute delete for {} nat rule handle {}",
                chain, handle
            ),
        )?,
        &format!("Failed to delete {} nat rule with handle {}", chain, handle),
    )?;

    Ok(())
}

fn ensure_nat_table() -> Result<(), Box<dyn Error + Send + Sync>> {
    let nat_table_exists = Command::new("nft")
        .args(["list", "table", "ip", "nat"])
        .output()
        .map_err(|e| with_context(e, "Failed to check nft nat table"))?
        .status
        .success();
    if !nat_table_exists {
        ensure_success(
            command_status(
                {
                    let mut cmd = Command::new("nft");
                    cmd.args(["add", "table", "ip", "nat"]);
                    cmd
                },
                "Failed to create nft nat table",
            )?,
            "Failed to ensure nft nat table exists",
        )?;
    }
    Ok(())
}

fn ensure_nat_chain(
    chain: &str,
    hook: &str,
    priority: i32,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let chain_exists = Command::new("nft")
        .args(["list", "chain", "ip", "nat", chain])
        .output()
        .map_err(|e| with_context(e, format!("Failed to inspect nft chain {}", chain)))?
        .status
        .success();
    if !chain_exists {
        ensure_success(
            command_status(
                {
                    let mut cmd = Command::new("nft");
                    cmd.args([
                        "add",
                        "chain",
                        "ip",
                        "nat",
                        chain,
                        "{",
                        "type",
                        "nat",
                        "hook",
                        hook,
                        "priority",
                        &priority.to_string(),
                        ";",
                        "}",
                    ]);
                    cmd
                },
                format!("Failed to create nft chain {}", chain),
            )?,
            &format!("Failed to ensure nft {} chain exists", chain),
        )?;
    }
    Ok(())
}

fn ensure_sysctl_value(key: &str, desired: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let path = format!("/proc/sys/{}", key.replace('.', "/"));

    if let Ok(current) = fs::read_to_string(&path) {
        if current.trim() == desired {
            return Ok(());
        }
    }

    fs::write(&path, desired)
        .map_err(|e| with_context(e, format!("Failed to write sysctl {}", key)))?;
    Ok(())
}

fn command_status(
    mut cmd: Command,
    context: impl Into<String>,
) -> Result<ExitStatus, Box<dyn Error + Send + Sync>> {
    let context = context.into();
    cmd.status().map_err(|e| with_context(e, context.clone()))
}

fn ensure_success(status: ExitStatus, context: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
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

fn ensure_namespace(
    container_id: &str,
    netns_path: &str,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let path = Path::new(netns_path);
    if path.exists() {
        return Ok(false);
    }

    let run_dir = netns_dir();
    if path.starts_with(&run_dir) {
        fs::create_dir_all(&run_dir).map_err(|e| {
            with_context(
                e,
                format!("Failed to create netns run directory {}", run_dir.display()),
            )
        })?;
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| new_error(format!("Invalid network namespace path: {}", netns_path)))?;

        if name != container_id {
            return Err(new_error(format!(
                "Network namespace {} does not exist and name does not match container id",
                netns_path
            )));
        }

        ensure_success(
            command_status(
                {
                    let mut cmd = Command::new("ip");
                    cmd.args(["netns", "add", name]);
                    cmd
                },
                format!("Failed to execute netns add for {}", name),
            )?,
            &format!("Failed to create network namespace {}", name),
        )?;

        if path.exists() {
            return Ok(true);
        }
    }

    Err(new_error(format!(
        "Network namespace {} does not exist",
        netns_path
    )))
}

fn delete(container_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let allocation_key = allocation_path(container_id);
    let allocation = CNI_KEYSPACE.get(&allocation_key).ok();
    if let Some(allocation) = allocation {
        let mut parts = allocation.split_whitespace();
        let ip_address = parts.next().unwrap_or("").to_string();
        let host_if = parts.next().map(|s| s.to_string());
        let netns_flag = parts.next().map(|flag| flag == "1").unwrap_or(false);
        let link_name = host_if.unwrap_or_else(|| host_interface_name(container_id));
        let _ = delete_link_if_exists(&link_name);
        if !ip_address.is_empty() {
            let _ = CNI_KEYSPACE.delete(&ip_pool_path(&ip_address));
        }
        let _ = CNI_KEYSPACE.delete(&allocation_key);
        if netns_flag {
            let _ = Command::new("ip")
                .args(["netns", "delete", container_id])
                .status();
        }
    } else {
        let ip_path = container_root_path(container_id)
            .join("network")
            .join("ip_address");
        if let Ok(mut file) = fs::File::open(&ip_path) {
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).map_err(|e| {
                with_context(
                    e,
                    format!("Failed to read legacy IP allocation {}", ip_path.display()),
                )
            })?;
            let ip_address = String::from_utf8_lossy(&buffer).trim().to_string();
            if !ip_address.is_empty() {
                let _ = CNI_KEYSPACE.delete(&ip_pool_path(&ip_address));
            }
        }
        let _ = delete_link_if_exists(&host_interface_name(container_id));
        let _ = Command::new("ip")
            .args(["netns", "delete", container_id])
            .status();
    }

    Ok(())
}

fn is_root() -> bool {
    geteuid().as_raw() == 0
}

fn host_interface_name(container_id: &str) -> String {
    const PREFIX: &str = "veth";
    const MAX_LEN: usize = 15;
    let remaining = MAX_LEN.saturating_sub(PREFIX.len());
    let suffix = container_id.chars().take(remaining).collect::<String>();
    format!("{}{}", PREFIX, suffix)
}

fn peer_interface_name(host_if: &str) -> String {
    const MAX_LEN: usize = 15;
    if host_if.len() >= MAX_LEN {
        format!("{}p", &host_if[..(MAX_LEN - 1)])
    } else {
        format!("{}p", host_if)
    }
}

struct NetnsLink {
    path: std::path::PathBuf,
    created: bool,
}

impl NetnsLink {
    fn attach(name: &str, netns_path: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let netns = std::path::Path::new(netns_path);
        if !netns.exists() {
            return Err(new_error(format!(
                "Network namespace {} does not exist",
                netns_path
            )));
        }
        let run_dir = netns_dir();
        fs::create_dir_all(&run_dir).map_err(|e| {
            with_context(
                e,
                format!("Failed to create netns run directory {}", run_dir.display()),
            )
        })?;
        let link_path = run_dir.join(name);
        if netns == link_path {
            return Ok(NetnsLink {
                path: link_path,
                created: false,
            });
        }
        let mut created = false;
        match fs::read_link(&link_path) {
            Ok(existing) if existing == netns => {}
            Ok(_) => {
                fs::remove_file(&link_path).map_err(|e| {
                    with_context(
                        e,
                        format!(
                            "Failed to remove existing netns link {}",
                            link_path.display()
                        ),
                    )
                })?;
                symlink(netns, &link_path).map_err(|e| {
                    with_context(
                        e,
                        format!(
                            "Failed to create netns symlink {} -> {}",
                            link_path.display(),
                            netns.display()
                        ),
                    )
                })?;
                created = true;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                symlink(netns, &link_path).map_err(|e| {
                    with_context(
                        e,
                        format!(
                            "Failed to create netns symlink {} -> {}",
                            link_path.display(),
                            netns.display()
                        ),
                    )
                })?;
                created = true;
            }
            Err(err) => {
                return Err(with_context(
                    err,
                    format!("Failed to inspect netns link {}", link_path.display()),
                ))
            }
        }
        Ok(NetnsLink {
            path: link_path,
            created,
        })
    }
}

impl Drop for NetnsLink {
    fn drop(&mut self) {
        if self.created {
            let _ = fs::remove_file(&self.path);
        }
    }
}
