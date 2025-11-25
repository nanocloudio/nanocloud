/*
 * Copyright (C) 2025 The Nanocloud Authors
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

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DnsConfig {
    pub cluster_domain: String,
    pub listen_address: IpAddr,
    pub listen_port: u16,
    pub default_ttl_seconds: u32,
    pub upstream_servers: Vec<SocketAddr>,
    pub max_udp_payload_size: u16,
}

#[derive(Debug)]
pub enum DnsConfigError {
    InvalidDomain(String),
}

impl Display for DnsConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DnsConfigError::InvalidDomain(reason) => {
                write!(f, "Invalid cluster domain: {}", reason)
            }
        }
    }
}

impl Error for DnsConfigError {}

impl DnsConfig {
    pub fn new(
        cluster_domain: String,
        listen_address: IpAddr,
        listen_port: u16,
        default_ttl_seconds: u32,
        upstream_servers: Vec<SocketAddr>,
        max_udp_payload_size: u16,
    ) -> Result<Self, DnsConfigError> {
        let normalized_domain = normalize_domain(&cluster_domain)?;
        Ok(Self {
            cluster_domain: normalized_domain,
            listen_address,
            listen_port,
            default_ttl_seconds,
            upstream_servers,
            max_udp_payload_size,
        })
    }

    pub fn zone_root(&self) -> String {
        format!("svc.{}", self.cluster_domain)
    }

    pub fn zone_root_fqdn(&self) -> String {
        format!("{}.", self.zone_root())
    }

    pub fn cluster_domain_fqdn(&self) -> String {
        format!("{}.", self.cluster_domain)
    }

    pub fn ns_name(&self) -> String {
        format!("ns1.{}", self.zone_root_fqdn())
    }
}

impl Default for DnsConfig {
    fn default() -> Self {
        DnsConfig::new(
            "cluster.local".to_string(),
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            53,
            30,
            Vec::new(),
            512,
        )
        .expect("default DNS config must be valid")
    }
}

fn normalize_domain(domain: &str) -> Result<String, DnsConfigError> {
    let trimmed = domain.trim_end_matches('.').trim();
    if trimmed.is_empty() {
        return Err(DnsConfigError::InvalidDomain(
            "cluster domain must not be empty".to_string(),
        ));
    }
    for label in trimmed.split('.') {
        if !is_dns_label(label) {
            return Err(DnsConfigError::InvalidDomain(format!(
                "label '{}' is not DNS compliant",
                label
            )));
        }
    }
    Ok(trimmed.to_ascii_lowercase())
}

fn is_dns_label(value: &str) -> bool {
    if value.is_empty() || value.len() > 63 {
        return false;
    }
    let bytes = value.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() || !bytes[value.len() - 1].is_ascii_alphanumeric() {
        return false;
    }
    bytes
        .iter()
        .all(|c| c.is_ascii_alphanumeric() || *c == b'-')
}
