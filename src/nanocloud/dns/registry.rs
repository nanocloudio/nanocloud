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

#![allow(dead_code)]

use crate::nanocloud::logger::log_info;
use crate::nanocloud::util::error::new_error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum DnsProtocol {
    Tcp,
    Udp,
}

impl DnsProtocol {
    pub fn as_str(&self) -> &'static str {
        match self {
            DnsProtocol::Tcp => "tcp",
            DnsProtocol::Udp => "udp",
        }
    }
}

impl FromStr for DnsProtocol {
    type Err = RegistryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "tcp" => Ok(DnsProtocol::Tcp),
            "udp" => Ok(DnsProtocol::Udp),
            _ => Err(RegistryError::InvalidInput(format!(
                "Unsupported protocol '{}'",
                s
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct EndpointId(u64);

impl EndpointId {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for EndpointId {
    fn from(value: u64) -> Self {
        EndpointId(value)
    }
}

#[derive(Clone, Debug)]
pub struct ServicePortDescription {
    pub name: String,
    pub protocol: DnsProtocol,
    pub port: u16,
    pub target_port: Option<u16>,
}

#[derive(Clone, Debug)]
pub struct ServiceDescription {
    pub name: String,
    pub namespace: String,
    pub cluster_ip: Option<IpAddr>,
    pub ports: Vec<ServicePortDescription>,
    pub ttl_seconds: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct EndpointDescription {
    pub service_name: String,
    pub namespace: String,
    pub ip: IpAddr,
    pub hostname: String,
    pub ready: bool,
    pub port_overrides: HashMap<String, u16>,
}

#[derive(Clone, Debug, Default)]
pub struct EndpointPatch {
    pub ip: Option<IpAddr>,
    pub hostname: Option<String>,
    pub ready: Option<bool>,
    pub port_overrides: Option<HashMap<String, u16>>,
}

#[derive(Clone, Debug)]
pub struct EndpointSnapshot {
    pub id: Option<EndpointId>,
    pub description: EndpointDescription,
}

#[derive(Clone, Debug, Default)]
pub struct ClusterDnsSnapshot {
    pub services: Vec<ServiceDescription>,
    pub endpoints: Vec<EndpointSnapshot>,
}

#[derive(Debug)]
pub enum RegistryError {
    InvalidInput(String),
    NotFound(String),
    Persistence(Box<dyn Error + Send + Sync>),
}

impl Display for RegistryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryError::InvalidInput(reason) => write!(f, "Invalid input: {}", reason),
            RegistryError::NotFound(reason) => write!(f, "Not found: {}", reason),
            RegistryError::Persistence(err) => write!(f, "{}", err),
        }
    }
}

impl Error for RegistryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            RegistryError::Persistence(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct ServiceKey {
    namespace: String,
    name: String,
}

impl ServiceKey {
    fn new(namespace: impl AsRef<str>, name: impl AsRef<str>) -> Result<Self, RegistryError> {
        Ok(Self {
            namespace: normalize_label(namespace.as_ref(), "namespace")?,
            name: normalize_label(name.as_ref(), "service name")?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PortKey {
    pub name: String,
    pub protocol: DnsProtocol,
}

#[derive(Clone, Debug)]
pub struct ServicePort {
    pub name: String,
    pub protocol: DnsProtocol,
    pub port: u16,
    pub target_port: Option<u16>,
}

#[derive(Clone, Debug)]
pub struct EndpointRecord {
    pub id: EndpointId,
    pub ip: IpAddr,
    pub hostname: String,
    pub service_name: String,
    pub namespace: String,
    pub port_overrides: HashMap<String, u16>,
    pub ready: bool,
}

#[derive(Clone, Debug)]
pub struct ServiceSnapshot {
    pub name: String,
    pub namespace: String,
    pub cluster_ip: Option<IpAddr>,
    pub ports: HashMap<PortKey, ServicePort>,
    pub ttl_seconds: Option<u32>,
    pub endpoints: Vec<EndpointRecord>,
}

#[derive(Clone, Debug, Default)]
pub struct RegistrySnapshot {
    services: HashMap<ServiceKey, ServiceSnapshot>,
    generation: u64,
}

impl RegistrySnapshot {
    pub(crate) fn service(&self, namespace: &str, name: &str) -> Option<&ServiceSnapshot> {
        let key = ServiceKey::new(namespace, name).ok()?;
        self.services.get(&key)
    }

    pub fn services(&self) -> Vec<ServiceSnapshot> {
        self.services.values().cloned().collect()
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}

#[derive(Clone, Debug)]
struct ServiceState {
    name: String,
    namespace: String,
    cluster_ip: Option<IpAddr>,
    ports: HashMap<PortKey, ServicePort>,
    ttl_seconds: Option<u32>,
    endpoints: HashMap<EndpointId, EndpointRecord>,
}

#[derive(Default)]
struct RegistryState {
    services: HashMap<ServiceKey, ServiceState>,
    endpoint_index: HashMap<EndpointId, ServiceKey>,
}

#[derive(Clone)]
pub struct DnsRegistry {
    state: Arc<RwLock<RegistryState>>,
    next_endpoint_id: Arc<AtomicU64>,
    snapshot: Arc<RwLock<Arc<RegistrySnapshot>>>,
    snapshot_generation: Arc<AtomicU64>,
}

impl Default for DnsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DnsRegistry {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(RegistryState::default())),
            next_endpoint_id: Arc::new(AtomicU64::new(1)),
            snapshot: Arc::new(RwLock::new(Arc::new(RegistrySnapshot::default()))),
            snapshot_generation: Arc::new(AtomicU64::new(1)),
        }
    }

    fn rebuild_snapshot_from_state(&self, state: &RegistryState) {
        let generation = self.snapshot_generation.fetch_add(1, Ordering::SeqCst);
        let mut services = HashMap::new();
        for (key, svc) in state.services.iter() {
            let snapshot = ServiceSnapshot {
                name: svc.name.clone(),
                namespace: svc.namespace.clone(),
                cluster_ip: svc.cluster_ip,
                ports: svc.ports.clone(),
                ttl_seconds: svc.ttl_seconds,
                endpoints: svc.endpoints.values().cloned().collect(),
            };
            services.insert(key.clone(), snapshot);
        }
        let snapshot = Arc::new(RegistrySnapshot {
            services,
            generation,
        });
        let mut guard = self
            .snapshot
            .write()
            .expect("DNS registry snapshot lock poisoned");
        *guard = snapshot;
    }

    pub fn register_service(&self, description: ServiceDescription) -> Result<(), RegistryError> {
        self.upsert_service(description)
    }

    pub fn update_service(&self, description: ServiceDescription) -> Result<(), RegistryError> {
        self.upsert_service(description)
    }

    pub fn remove_service(&self, namespace: &str, name: &str) -> Result<bool, RegistryError> {
        let key = ServiceKey::new(namespace, name)?;
        let mut guard = self
            .state
            .write()
            .map_err(|_| RegistryError::Persistence(new_error("DNS registry lock poisoned")))?;
        if let Some(entry) = guard.services.remove(&key) {
            for endpoint_id in entry.endpoints.keys() {
                guard.endpoint_index.remove(endpoint_id);
            }
            log_info(
                "dns-registry",
                "Removed service",
                &[
                    ("namespace", key.namespace.as_str()),
                    ("service", key.name.as_str()),
                ],
            );
            self.rebuild_snapshot_from_state(&guard);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn register_endpoint(
        &self,
        description: EndpointDescription,
    ) -> Result<EndpointId, RegistryError> {
        let mut guard = self
            .state
            .write()
            .map_err(|_| RegistryError::Persistence(new_error("DNS registry lock poisoned")))?;
        let key = ServiceKey::new(&description.namespace, &description.service_name)?;
        let hostname = normalize_label(&description.hostname, "endpoint hostname")?;
        let hostname_for_log = hostname.clone();
        let port_overrides = normalize_overrides(description.port_overrides)?;
        let ready = description.ready;
        let ip = description.ip;

        let existing_id = {
            let Some(service) = guard.services.get_mut(&key) else {
                return Err(RegistryError::NotFound(format!(
                    "Service {}/{} not found for endpoint",
                    key.namespace, key.name
                )));
            };
            service
                .endpoints
                .iter_mut()
                .find(|(_, endpoint)| endpoint.hostname == hostname && endpoint.ip == ip)
                .map(|(existing_id, existing)| {
                    existing.ready = ready;
                    existing.port_overrides = port_overrides.clone();
                    *existing_id
                })
        };

        if let Some(existing_id) = existing_id {
            guard.endpoint_index.insert(existing_id, key);
            self.rebuild_snapshot_from_state(&guard);
            return Ok(existing_id);
        }

        let Some(service) = guard.services.get_mut(&key) else {
            return Err(RegistryError::NotFound(format!(
                "Service {}/{} not found for endpoint",
                key.namespace, key.name
            )));
        };
        let id = self.allocate_endpoint_id();
        let key_for_log = key.clone();
        let record = EndpointRecord {
            id,
            ip,
            hostname,
            service_name: key.name.clone(),
            namespace: key.namespace.clone(),
            port_overrides,
            ready,
        };

        service.endpoints.insert(id, record);
        guard.endpoint_index.insert(id, key_for_log.clone());
        log_info(
            "dns-registry",
            "Registered endpoint",
            &[
                ("namespace", key.namespace.as_str()),
                ("service", key.name.as_str()),
                ("hostname", hostname_for_log.as_str()),
            ],
        );
        self.rebuild_snapshot_from_state(&guard);
        Ok(id)
    }

    pub fn update_endpoint(
        &self,
        endpoint_id: EndpointId,
        patch: EndpointPatch,
    ) -> Result<(), RegistryError> {
        let mut guard = self
            .state
            .write()
            .map_err(|_| RegistryError::Persistence(new_error("DNS registry lock poisoned")))?;
        let service_key = guard
            .endpoint_index
            .get(&endpoint_id)
            .cloned()
            .ok_or_else(|| {
                RegistryError::NotFound(format!(
                    "Endpoint '{}' not found in registry",
                    endpoint_id.as_u64()
                ))
            })?;
        let Some(service) = guard.services.get_mut(&service_key) else {
            return Err(RegistryError::NotFound(format!(
                "Service {}/{} missing for endpoint {}",
                service_key.namespace,
                service_key.name,
                endpoint_id.as_u64()
            )));
        };
        let Some(endpoint) = service.endpoints.get_mut(&endpoint_id) else {
            return Err(RegistryError::NotFound(format!(
                "Endpoint '{}' missing from service {}/{}",
                endpoint_id.as_u64(),
                service_key.namespace,
                service_key.name
            )));
        };

        if let Some(ip) = patch.ip {
            endpoint.ip = ip;
        }
        if let Some(hostname) = patch.hostname {
            endpoint.hostname = normalize_label(&hostname, "endpoint hostname")?;
        }
        if let Some(ready) = patch.ready {
            endpoint.ready = ready;
        }
        if let Some(overrides) = patch.port_overrides {
            endpoint.port_overrides = normalize_overrides(overrides)?;
        }
        self.rebuild_snapshot_from_state(&guard);
        Ok(())
    }

    pub fn remove_endpoint(&self, endpoint_id: EndpointId) -> Result<bool, RegistryError> {
        let mut guard = self
            .state
            .write()
            .map_err(|_| RegistryError::Persistence(new_error("DNS registry lock poisoned")))?;
        let Some(service_key) = guard.endpoint_index.remove(&endpoint_id) else {
            return Ok(false);
        };
        if let Some(service) = guard.services.get_mut(&service_key) {
            let removed = service.endpoints.remove(&endpoint_id).is_some();
            if removed {
                let endpoint_id_text = endpoint_id.as_u64().to_string();
                log_info(
                    "dns-registry",
                    "Removed endpoint",
                    &[
                        ("namespace", service_key.namespace.as_str()),
                        ("service", service_key.name.as_str()),
                        ("endpoint_id", endpoint_id_text.as_str()),
                    ],
                );
                self.rebuild_snapshot_from_state(&guard);
            }
            Ok(removed)
        } else {
            Ok(false)
        }
    }

    pub fn apply_snapshot(&self, snapshot: ClusterDnsSnapshot) -> Result<(), RegistryError> {
        let mut next_id = 1u64;
        let mut new_state = RegistryState::default();

        for service in snapshot.services {
            let key = ServiceKey::new(&service.namespace, &service.name)?;
            let state = ServiceState {
                name: key.name.clone(),
                namespace: key.namespace.clone(),
                cluster_ip: service.cluster_ip,
                ports: build_ports(service.ports)?,
                ttl_seconds: service.ttl_seconds,
                endpoints: HashMap::new(),
            };
            new_state.services.insert(key, state);
        }

        for endpoint in snapshot.endpoints {
            let description = endpoint.description;
            let key = ServiceKey::new(&description.namespace, &description.service_name)?;
            let Some(service) = new_state.services.get_mut(&key) else {
                return Err(RegistryError::NotFound(format!(
                    "Service {}/{} missing in snapshot for endpoint",
                    key.namespace, key.name
                )));
            };
            let id = endpoint.id.unwrap_or_else(|| {
                let value = next_id;
                next_id += 1;
                EndpointId(value)
            });
            next_id = next_id.max(id.as_u64() + 1);
            let record = EndpointRecord {
                id,
                ip: description.ip,
                hostname: normalize_label(&description.hostname, "endpoint hostname")?,
                service_name: key.name.clone(),
                namespace: key.namespace.clone(),
                port_overrides: normalize_overrides(description.port_overrides)?,
                ready: description.ready,
            };
            service.endpoints.insert(id, record);
            new_state.endpoint_index.insert(id, key.clone());
        }

        let mut guard = self
            .state
            .write()
            .map_err(|_| RegistryError::Persistence(new_error("DNS registry lock poisoned")))?;
        *guard = new_state;
        self.next_endpoint_id.store(next_id, Ordering::SeqCst);
        self.rebuild_snapshot_from_state(&guard);
        let services_count = guard.services.len().to_string();
        let next_id_text = next_id.to_string();
        log_info(
            "dns-registry",
            "Applied DNS snapshot",
            &[
                ("services", services_count.as_str()),
                ("next_endpoint_id", next_id_text.as_str()),
            ],
        );
        Ok(())
    }

    pub fn snapshot(&self) -> RegistrySnapshot {
        self.shared_snapshot().as_ref().clone()
    }

    pub fn shared_snapshot(&self) -> Arc<RegistrySnapshot> {
        self.snapshot
            .read()
            .expect("DNS registry snapshot lock poisoned")
            .clone()
    }

    fn upsert_service(&self, description: ServiceDescription) -> Result<(), RegistryError> {
        let key = ServiceKey::new(&description.namespace, &description.name)?;
        let mut guard = self
            .state
            .write()
            .map_err(|_| RegistryError::Persistence(new_error("DNS registry lock poisoned")))?;
        let ports = build_ports(description.ports)?;
        let ttl_seconds = description.ttl_seconds;
        let cluster_ip = description.cluster_ip;
        let entry = guard
            .services
            .entry(key.clone())
            .or_insert_with(|| ServiceState {
                name: key.name.clone(),
                namespace: key.namespace.clone(),
                cluster_ip,
                ports: ports.clone(),
                ttl_seconds,
                endpoints: HashMap::new(),
            });
        entry.cluster_ip = cluster_ip;
        entry.ports = ports;
        entry.ttl_seconds = ttl_seconds;
        let cluster_ip_text = cluster_ip
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| "headless".to_string());
        log_info(
            "dns-registry",
            "Service registered",
            &[
                ("namespace", key.namespace.as_str()),
                ("service", key.name.as_str()),
                ("cluster_ip", cluster_ip_text.as_str()),
            ],
        );
        self.rebuild_snapshot_from_state(&guard);
        Ok(())
    }

    fn allocate_endpoint_id(&self) -> EndpointId {
        EndpointId(self.next_endpoint_id.fetch_add(1, Ordering::SeqCst))
    }
}

fn build_ports(
    ports: Vec<ServicePortDescription>,
) -> Result<HashMap<PortKey, ServicePort>, RegistryError> {
    let mut result = HashMap::new();
    for port in ports {
        let name = normalize_label(&port.name, "port name")?;
        let key = PortKey {
            name: name.clone(),
            protocol: port.protocol,
        };
        if result.contains_key(&key) {
            return Err(RegistryError::InvalidInput(format!(
                "Duplicate port '{}' for protocol {}",
                name,
                port.protocol.as_str()
            )));
        }
        result.insert(
            key,
            ServicePort {
                name,
                protocol: port.protocol,
                port: port.port,
                target_port: port.target_port,
            },
        );
    }
    Ok(result)
}

fn normalize_overrides(
    overrides: HashMap<String, u16>,
) -> Result<HashMap<String, u16>, RegistryError> {
    let mut normalized = HashMap::new();
    for (name, port) in overrides {
        let key = normalize_label(&name, "port override name")?;
        normalized.insert(key, port);
    }
    Ok(normalized)
}

fn normalize_label(value: &str, field: &str) -> Result<String, RegistryError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(RegistryError::InvalidInput(format!(
            "{} must not be empty",
            field
        )));
    }
    let lower = trimmed.to_ascii_lowercase();
    if !is_dns_label(&lower) {
        return Err(RegistryError::InvalidInput(format!(
            "{} '{}' is not DNS compatible",
            field, value
        )));
    }
    Ok(lower)
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
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn sample_service() -> ServiceDescription {
        ServiceDescription {
            name: "web".to_string(),
            namespace: "default".to_string(),
            cluster_ip: Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
            ports: vec![ServicePortDescription {
                name: "http".to_string(),
                protocol: DnsProtocol::Tcp,
                port: 80,
                target_port: Some(8080),
            }],
            ttl_seconds: Some(60),
        }
    }

    fn sample_endpoint(service_name: &str) -> EndpointDescription {
        EndpointDescription {
            service_name: service_name.to_string(),
            namespace: "default".to_string(),
            ip: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            hostname: "pod-1".to_string(),
            ready: true,
            port_overrides: HashMap::new(),
        }
    }

    #[test]
    fn registers_and_updates_services() {
        let registry = DnsRegistry::new();
        registry.register_service(sample_service()).unwrap();
        registry
            .register_service(ServiceDescription {
                ttl_seconds: Some(30),
                ..sample_service()
            })
            .unwrap();
        let snapshot = registry.snapshot();
        let service = snapshot.service("default", "web").unwrap();
        assert_eq!(service.ttl_seconds, Some(30));
        assert_eq!(
            service.cluster_ip,
            Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)))
        );
    }

    #[test]
    fn registers_endpoints_idempotently() {
        let registry = DnsRegistry::new();
        registry.register_service(sample_service()).unwrap();
        let endpoint = sample_endpoint("web");
        let first = registry.register_endpoint(endpoint.clone()).unwrap();
        let second = registry.register_endpoint(endpoint).unwrap();
        assert_eq!(first, second);
        let snapshot = registry.snapshot();
        let service = snapshot.service("default", "web").unwrap();
        assert_eq!(service.endpoints.len(), 1);
    }

    #[test]
    fn apply_snapshot_replaces_state() {
        let registry = DnsRegistry::new();
        let endpoint = sample_endpoint("web");
        let snapshot = ClusterDnsSnapshot {
            services: vec![sample_service()],
            endpoints: vec![EndpointSnapshot {
                id: Some(EndpointId(42)),
                description: endpoint,
            }],
        };
        registry.apply_snapshot(snapshot).unwrap();
        let snapshot = registry.snapshot();
        let service = snapshot.service("default", "web").unwrap();
        assert_eq!(service.endpoints.len(), 1);
        assert_eq!(service.endpoints[0].id.as_u64(), 42);
    }
}
