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
use std::time::Duration;

pub const DEFAULT_HANDLER_CONCURRENCY: usize = 128;
pub const DEFAULT_RATE_LIMIT_BURST: u32 = 50;
pub const DEFAULT_RESPONSE_CACHE_TTL_SECONDS: u64 = 2;
pub const DEFAULT_RESPONSE_CACHE_MAX_ENTRIES: usize = 256;
pub const DEFAULT_NORMALIZED_CACHE_TTL_SECONDS: u64 = 60;
pub const DEFAULT_UPSTREAM_TIMEOUT: Duration = Duration::from_secs(2);
pub const DEFAULT_UPSTREAM_RETRIES: usize = 1;
pub const DEFAULT_LISTENER_BACKOFF_MS: u64 = 250;
pub const DEFAULT_BUFFER_POOL_SIZE: usize = 512;
pub const DEFAULT_MIN_DNS_PACKET_LEN: usize = 12;

/// DNS server configuration used by the control plane DNS service.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DnsConfig {
    /// Cluster DNS suffix without the `svc.` prefix (e.g. `cluster.local`).
    pub cluster_domain: String,
    /// Address to bind DNS listeners.
    pub listen_address: IpAddr,
    /// Port to bind DNS listeners.
    pub listen_port: u16,
    /// Default TTL applied to answers when the registry omits per-service TTLs.
    pub default_ttl_seconds: u32,
    /// Upstream resolvers for out-of-zone queries; empty means REFUSED.
    pub upstream_servers: Vec<SocketAddr>,
    /// Maximum UDP payload size to emit/accept.
    pub max_udp_payload_size: u16,
    /// Optional semaphore limit for concurrent query handlers (0 disables).
    pub handler_concurrency: usize,
    /// Optional per-client token-bucket rate limit (queries per second).
    pub rate_limit_per_second: Option<u32>,
    /// Token-bucket burst size for rate limiting.
    pub rate_limit_burst: u32,
    /// TTL for cached responses in seconds (0 disables).
    pub response_cache_ttl_seconds: u64,
    /// Maximum number of cached responses (0 disables).
    pub response_cache_max_entries: usize,
    /// TTL for normalized-name cache in seconds.
    pub normalized_cache_ttl_seconds: u64,
    /// Timeout for upstream forwarding attempts.
    pub upstream_timeout: Duration,
    /// Retry attempts for upstream forwarding.
    pub upstream_retries: usize,
    /// Initial listener restart backoff in milliseconds.
    pub listener_backoff_ms: u64,
    /// Buffer pool size for UDP receive path.
    pub buffer_pool_size: usize,
    /// Minimum DNS packet length accepted before parsing.
    pub min_dns_packet_len: usize,
}

#[derive(Debug)]
pub enum DnsConfigError {
    InvalidDomain(String),
    InvalidValue(String),
}

impl Display for DnsConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DnsConfigError::InvalidDomain(reason) => {
                write!(f, "Invalid cluster domain: {}", reason)
            }
            DnsConfigError::InvalidValue(reason) => {
                write!(f, "Invalid DNS configuration: {}", reason)
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
        let config = Self {
            cluster_domain: normalized_domain,
            listen_address,
            listen_port,
            default_ttl_seconds,
            upstream_servers,
            max_udp_payload_size,
            handler_concurrency: DEFAULT_HANDLER_CONCURRENCY,
            rate_limit_per_second: None,
            rate_limit_burst: DEFAULT_RATE_LIMIT_BURST,
            response_cache_ttl_seconds: DEFAULT_RESPONSE_CACHE_TTL_SECONDS,
            response_cache_max_entries: DEFAULT_RESPONSE_CACHE_MAX_ENTRIES,
            normalized_cache_ttl_seconds: DEFAULT_NORMALIZED_CACHE_TTL_SECONDS,
            upstream_timeout: DEFAULT_UPSTREAM_TIMEOUT,
            upstream_retries: DEFAULT_UPSTREAM_RETRIES,
            listener_backoff_ms: DEFAULT_LISTENER_BACKOFF_MS,
            buffer_pool_size: DEFAULT_BUFFER_POOL_SIZE,
            min_dns_packet_len: DEFAULT_MIN_DNS_PACKET_LEN,
        };
        config.validate()?;
        Ok(config)
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

    pub fn validate(&self) -> Result<(), DnsConfigError> {
        if let Some(limit) = self.rate_limit_per_second {
            if limit == 0 {
                return Err(DnsConfigError::InvalidValue(
                    "rate limit per second must be greater than zero".to_string(),
                ));
            }
        }
        if self.rate_limit_burst == 0 {
            return Err(DnsConfigError::InvalidValue(
                "rate limit burst must be at least 1".to_string(),
            ));
        }
        if self.min_dns_packet_len < DEFAULT_MIN_DNS_PACKET_LEN {
            return Err(DnsConfigError::InvalidValue(format!(
                "minimum DNS packet length must be at least {} bytes",
                DEFAULT_MIN_DNS_PACKET_LEN
            )));
        }
        if (self.max_udp_payload_size as usize) < self.min_dns_packet_len {
            return Err(DnsConfigError::InvalidValue(
                "minimum DNS packet length cannot exceed max UDP payload size".to_string(),
            ));
        }
        if self.upstream_timeout.is_zero() {
            return Err(DnsConfigError::InvalidValue(
                "upstream timeout must be greater than zero".to_string(),
            ));
        }
        if self.listener_backoff_ms == 0 {
            return Err(DnsConfigError::InvalidValue(
                "listener backoff must be at least 1ms".to_string(),
            ));
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    #![allow(clippy::field_reassign_with_default)]
    use super::*;

    #[test]
    fn rejects_too_small_min_packet_len() {
        let mut config = DnsConfig::default();
        config.min_dns_packet_len = 8;
        assert!(matches!(
            config.validate(),
            Err(DnsConfigError::InvalidValue(_))
        ));
    }

    #[test]
    fn rejects_zero_rate_limit() {
        let mut config = DnsConfig::default();
        config.rate_limit_per_second = Some(0);
        assert!(matches!(
            config.validate(),
            Err(DnsConfigError::InvalidValue(_))
        ));
    }
}
