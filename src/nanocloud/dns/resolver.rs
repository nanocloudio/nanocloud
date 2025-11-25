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

use super::config::DnsConfig;
use super::registry::{DnsProtocol, DnsRegistry, PortKey, RegistrySnapshot, ServiceSnapshot};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryType {
    A,
    AAAA,
    SRV,
    NS,
    SOA,
    Other(u16),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DnsQuestion {
    pub name: String,
    pub qtype: QueryType,
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseCode {
    NoError,
    NxDomain,
    Refused,
    ServFail,
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DnsRecord {
    A {
        name: String,
        address: Ipv4Addr,
        ttl: u32,
    },
    AAAA {
        name: String,
        address: Ipv6Addr,
        ttl: u32,
    },
    Srv {
        name: String,
        priority: u16,
        weight: u16,
        port: u16,
        target: String,
        ttl: u32,
    },
    Ns {
        name: String,
        host: String,
        ttl: u32,
    },
    Soa {
        name: String,
        mname: String,
        rname: String,
        serial: u32,
        refresh: u32,
        retry: u32,
        expire: u32,
        minimum: u32,
        ttl: u32,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DnsResponse {
    pub code: ResponseCode,
    pub answers: Vec<DnsRecord>,
    pub authorities: Vec<DnsRecord>,
    pub additionals: Vec<DnsRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Resolution {
    Answer(DnsResponse),
    Forward(Vec<SocketAddr>),
}

#[derive(Clone)]
pub struct DnsResolver {
    config: DnsConfig,
    registry: Arc<DnsRegistry>,
}

impl DnsResolver {
    pub fn new(config: DnsConfig, registry: Arc<DnsRegistry>) -> Self {
        Self { config, registry }
    }

    pub fn resolve(&self, question: &DnsQuestion) -> Resolution {
        let snapshot = self.registry.snapshot();
        resolve_question(&self.config, &snapshot, question)
    }
}

enum ParsedName {
    ZoneRoot,
    Service {
        namespace: String,
        service: String,
    },
    Endpoint {
        namespace: String,
        service: String,
        hostname: String,
    },
    #[allow(clippy::upper_case_acronyms)]
    Srv {
        namespace: String,
        service: String,
        port_name: String,
        protocol: DnsProtocol,
    },
    Unknown,
}

fn resolve_question(
    config: &DnsConfig,
    snapshot: &RegistrySnapshot,
    question: &DnsQuestion,
) -> Resolution {
    let normalized_name = normalize_question_name(&question.name);
    if !is_in_zone(config, &normalized_name) {
        if config.upstream_servers.is_empty() {
            return Resolution::Answer(DnsResponse {
                code: ResponseCode::Refused,
                answers: Vec::new(),
                authorities: Vec::new(),
                additionals: Vec::new(),
            });
        }
        return Resolution::Forward(config.upstream_servers.clone());
    }

    let parsed = parse_in_zone_name(config, &normalized_name, &question.qtype);
    match parsed {
        ParsedName::ZoneRoot => resolve_zone_root(config, question),
        ParsedName::Service { namespace, service } => {
            resolve_service(config, snapshot, question, namespace, service)
        }
        ParsedName::Endpoint {
            namespace,
            service,
            hostname,
        } => resolve_endpoint_name(config, snapshot, question, namespace, service, hostname),
        ParsedName::Srv {
            namespace,
            service,
            port_name,
            protocol,
        } => resolve_srv(
            config, snapshot, question, namespace, service, port_name, protocol,
        ),
        ParsedName::Unknown => Resolution::Answer(empty_response(config, ResponseCode::NxDomain)),
    }
}

fn resolve_zone_root(config: &DnsConfig, question: &DnsQuestion) -> Resolution {
    match question.qtype {
        QueryType::NS => Resolution::Answer(DnsResponse {
            code: ResponseCode::NoError,
            answers: vec![zone_ns_record(config)],
            authorities: Vec::new(),
            additionals: Vec::new(),
        }),
        QueryType::SOA => Resolution::Answer(DnsResponse {
            code: ResponseCode::NoError,
            answers: vec![zone_soa_record(config)],
            authorities: Vec::new(),
            additionals: Vec::new(),
        }),
        _ => Resolution::Answer(empty_response(config, ResponseCode::NoError)),
    }
}

fn resolve_service(
    config: &DnsConfig,
    snapshot: &RegistrySnapshot,
    question: &DnsQuestion,
    namespace: String,
    service: String,
) -> Resolution {
    let Some(service_snapshot) = snapshot.service(&namespace, &service) else {
        return Resolution::Answer(empty_response(config, ResponseCode::NxDomain));
    };
    match question.qtype {
        QueryType::A => Resolution::Answer(resolve_service_address(
            config,
            service_snapshot,
            IpFamily::V4,
        )),
        QueryType::AAAA => Resolution::Answer(resolve_service_address(
            config,
            service_snapshot,
            IpFamily::V6,
        )),
        QueryType::SRV => Resolution::Answer(empty_response(config, ResponseCode::NxDomain)),
        _ => Resolution::Answer(empty_response(config, ResponseCode::NoError)),
    }
}

fn resolve_endpoint_name(
    config: &DnsConfig,
    snapshot: &RegistrySnapshot,
    question: &DnsQuestion,
    namespace: String,
    service: String,
    hostname: String,
) -> Resolution {
    let Some(service_snapshot) = snapshot.service(&namespace, &service) else {
        return Resolution::Answer(empty_response(config, ResponseCode::NxDomain));
    };
    let endpoint = service_snapshot
        .endpoints
        .iter()
        .find(|ep| ep.hostname == hostname && ep.ready);

    match (endpoint, &question.qtype) {
        (None, _) => Resolution::Answer(empty_response(config, ResponseCode::NxDomain)),
        (Some(ep), QueryType::A) => match ep.ip {
            IpAddr::V4(ip) => Resolution::Answer(answer_with_records(
                ResponseCode::NoError,
                vec![DnsRecord::A {
                    name: endpoint_fqdn(config, &namespace, &service, &hostname),
                    address: ip,
                    ttl: ttl_for(service_snapshot, config),
                }],
                zone_authority(config),
            )),
            IpAddr::V6(_) => Resolution::Answer(answer_with_records(
                ResponseCode::NoError,
                Vec::new(),
                zone_authority(config),
            )),
        },
        (Some(ep), QueryType::AAAA) => match ep.ip {
            IpAddr::V6(ip) => Resolution::Answer(answer_with_records(
                ResponseCode::NoError,
                vec![DnsRecord::AAAA {
                    name: endpoint_fqdn(config, &namespace, &service, &hostname),
                    address: ip,
                    ttl: ttl_for(service_snapshot, config),
                }],
                zone_authority(config),
            )),
            IpAddr::V4(_) => Resolution::Answer(answer_with_records(
                ResponseCode::NoError,
                Vec::new(),
                zone_authority(config),
            )),
        },
        _ => Resolution::Answer(empty_response(config, ResponseCode::NoError)),
    }
}

fn resolve_srv(
    config: &DnsConfig,
    snapshot: &RegistrySnapshot,
    question: &DnsQuestion,
    namespace: String,
    service: String,
    port_name: String,
    protocol: DnsProtocol,
) -> Resolution {
    if !matches!(question.qtype, QueryType::SRV) {
        return Resolution::Answer(empty_response(config, ResponseCode::NoError));
    }
    let Some(service_snapshot) = snapshot.service(&namespace, &service) else {
        return Resolution::Answer(empty_response(config, ResponseCode::NxDomain));
    };
    let Some(port) = find_port(service_snapshot, &port_name, protocol) else {
        return Resolution::Answer(empty_response(config, ResponseCode::NxDomain));
    };
    let ttl = ttl_for(service_snapshot, config);

    if service_snapshot.cluster_ip.is_some() {
        let target = service_fqdn(config, &namespace, &service);
        let port_value = port.target_port.unwrap_or(port.port);
        let record = DnsRecord::Srv {
            name: srv_fqdn(config, &namespace, &service, &port_name, protocol),
            priority: 10,
            weight: 100,
            port: port_value,
            target,
            ttl,
        };
        return Resolution::Answer(answer_with_records(
            ResponseCode::NoError,
            vec![record],
            zone_authority(config),
        ));
    }

    let mut records = Vec::new();
    for endpoint in service_snapshot.endpoints.iter().filter(|ep| ep.ready) {
        let target = endpoint_fqdn(config, &namespace, &service, &endpoint.hostname);
        let port_value = endpoint
            .port_overrides
            .get(&port.name)
            .copied()
            .or(port.target_port)
            .unwrap_or(port.port);
        records.push(DnsRecord::Srv {
            name: srv_fqdn(config, &namespace, &service, &port_name, protocol),
            priority: 10,
            weight: 100,
            port: port_value,
            target,
            ttl,
        });
    }

    Resolution::Answer(answer_with_records(
        ResponseCode::NoError,
        records,
        zone_authority(config),
    ))
}

fn resolve_service_address(
    config: &DnsConfig,
    service: &ServiceSnapshot,
    family: IpFamily,
) -> DnsResponse {
    match (service.cluster_ip, family) {
        (Some(IpAddr::V4(ip)), IpFamily::V4) => DnsResponse {
            code: ResponseCode::NoError,
            answers: vec![DnsRecord::A {
                name: service_fqdn(config, &service.namespace, &service.name),
                address: ip,
                ttl: ttl_for(service, config),
            }],
            authorities: zone_authority(config),
            additionals: Vec::new(),
        },
        (Some(IpAddr::V6(ip)), IpFamily::V6) => DnsResponse {
            code: ResponseCode::NoError,
            answers: vec![DnsRecord::AAAA {
                name: service_fqdn(config, &service.namespace, &service.name),
                address: ip,
                ttl: ttl_for(service, config),
            }],
            authorities: zone_authority(config),
            additionals: Vec::new(),
        },
        (None, _) => resolve_headless_endpoints(config, service, family),
        _ => empty_response(config, ResponseCode::NoError),
    }
}

fn resolve_headless_endpoints(
    config: &DnsConfig,
    service: &ServiceSnapshot,
    family: IpFamily,
) -> DnsResponse {
    let ttl = ttl_for(service, config);
    let mut answers = Vec::new();
    for endpoint in service.endpoints.iter().filter(|ep| ep.ready) {
        match (family, endpoint.ip) {
            (IpFamily::V4, IpAddr::V4(ip)) => answers.push(DnsRecord::A {
                name: service_fqdn(config, &service.namespace, &service.name),
                address: ip,
                ttl,
            }),
            (IpFamily::V6, IpAddr::V6(ip)) => answers.push(DnsRecord::AAAA {
                name: service_fqdn(config, &service.namespace, &service.name),
                address: ip,
                ttl,
            }),
            _ => {}
        }
    }

    DnsResponse {
        code: ResponseCode::NoError,
        answers,
        authorities: zone_authority(config),
        additionals: Vec::new(),
    }
}

fn find_port<'a>(
    service: &'a ServiceSnapshot,
    name: &str,
    protocol: DnsProtocol,
) -> Option<&'a super::registry::ServicePort> {
    service.ports.get(&PortKey {
        name: name.to_string(),
        protocol,
    })
}

fn zone_authority(config: &DnsConfig) -> Vec<DnsRecord> {
    vec![zone_ns_record(config), zone_soa_record(config)]
}

fn zone_ns_record(config: &DnsConfig) -> DnsRecord {
    DnsRecord::Ns {
        name: config.zone_root_fqdn(),
        host: config.ns_name(),
        ttl: config.default_ttl_seconds,
    }
}

fn zone_soa_record(config: &DnsConfig) -> DnsRecord {
    DnsRecord::Soa {
        name: config.zone_root_fqdn(),
        mname: config.ns_name(),
        rname: format!("hostmaster.{}", config.cluster_domain_fqdn()),
        serial: 1,
        refresh: 300,
        retry: 120,
        expire: 1_800,
        minimum: config.default_ttl_seconds,
        ttl: config.default_ttl_seconds,
    }
}

fn answer_with_records(
    code: ResponseCode,
    answers: Vec<DnsRecord>,
    authorities: Vec<DnsRecord>,
) -> DnsResponse {
    DnsResponse {
        code,
        answers,
        authorities,
        additionals: Vec::new(),
    }
}

fn empty_response(config: &DnsConfig, code: ResponseCode) -> DnsResponse {
    DnsResponse {
        code,
        answers: Vec::new(),
        authorities: if matches!(code, ResponseCode::NxDomain | ResponseCode::NoError) {
            zone_authority(config)
        } else {
            Vec::new()
        },
        additionals: Vec::new(),
    }
}

fn ttl_for(service: &ServiceSnapshot, config: &DnsConfig) -> u32 {
    service.ttl_seconds.unwrap_or(config.default_ttl_seconds)
}

fn normalize_question_name(name: &str) -> String {
    let trimmed = name.trim_end_matches('.');
    trimmed.to_ascii_lowercase()
}

fn is_in_zone(config: &DnsConfig, name: &str) -> bool {
    let zone = config.zone_root();
    name == zone || name.ends_with(&format!(".{}", zone))
}

fn parse_in_zone_name(config: &DnsConfig, name: &str, qtype: &QueryType) -> ParsedName {
    let zone = config.zone_root();
    let remaining = if name == zone {
        ""
    } else if let Some(stripped) = name.strip_suffix(&format!(".{}", zone)) {
        stripped
    } else {
        return ParsedName::Unknown;
    };

    if remaining.is_empty() {
        return ParsedName::ZoneRoot;
    }

    let labels: Vec<&str> = remaining.split('.').collect();
    match labels.len() {
        2 => ParsedName::Service {
            service: labels[0].to_string(),
            namespace: labels[1].to_string(),
        },
        3 => ParsedName::Endpoint {
            hostname: labels[0].to_string(),
            service: labels[1].to_string(),
            namespace: labels[2].to_string(),
        },
        4 if matches!(qtype, QueryType::SRV) => {
            let port = labels[0].strip_prefix('_');
            let proto = labels[1].strip_prefix('_');
            if let (Some(port_name), Some(proto)) = (port, proto) {
                if let Ok(protocol) = DnsProtocol::from_str(proto) {
                    return ParsedName::Srv {
                        namespace: labels[3].to_string(),
                        service: labels[2].to_string(),
                        port_name: port_name.to_string(),
                        protocol,
                    };
                }
            }
            ParsedName::Unknown
        }
        _ => ParsedName::Unknown,
    }
}

fn service_fqdn(config: &DnsConfig, namespace: &str, service: &str) -> String {
    format!("{}.{}.{}.", service, namespace, config.zone_root())
}

fn endpoint_fqdn(config: &DnsConfig, namespace: &str, service: &str, hostname: &str) -> String {
    format!(
        "{}.{}.{}.{}.",
        hostname,
        service,
        namespace,
        config.zone_root()
    )
}

fn srv_fqdn(
    config: &DnsConfig,
    namespace: &str,
    service: &str,
    port_name: &str,
    protocol: DnsProtocol,
) -> String {
    format!(
        "_{}._{}.{}.{}.{}.",
        port_name,
        protocol.as_str(),
        service,
        namespace,
        config.zone_root()
    )
}

#[derive(Clone, Copy)]
enum IpFamily {
    V4,
    V6,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::dns::registry::{
        DnsProtocol, DnsRegistry, EndpointDescription, ServiceDescription, ServicePortDescription,
    };
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;

    fn build_resolver() -> (Arc<DnsRegistry>, DnsResolver) {
        let registry = Arc::new(DnsRegistry::new());
        let resolver = DnsResolver::new(DnsConfig::default(), Arc::clone(&registry));
        (registry, resolver)
    }

    fn cluster_service() -> ServiceDescription {
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

    fn headless_service() -> ServiceDescription {
        ServiceDescription {
            cluster_ip: None,
            ..cluster_service()
        }
    }

    fn endpoint(hostname: &str, ip: IpAddr) -> EndpointDescription {
        EndpointDescription {
            service_name: "web".to_string(),
            namespace: "default".to_string(),
            ip,
            hostname: hostname.to_string(),
            ready: true,
            port_overrides: HashMap::new(),
        }
    }

    #[test]
    fn forwards_out_of_zone_when_upstream_present() {
        let (registry, _) = build_resolver();
        registry.register_service(cluster_service()).unwrap();
        let config = DnsConfig {
            upstream_servers: vec!["1.1.1.1:53".parse().unwrap()],
            ..DnsConfig::default()
        };
        let resolver = DnsResolver::new(config, registry);
        let response = resolver.resolve(&DnsQuestion {
            name: "example.com.".to_string(),
            qtype: QueryType::A,
        });
        assert!(matches!(response, Resolution::Forward(_)));
    }

    #[test]
    fn resolves_cluster_ip_a_record() {
        let (registry, resolver) = build_resolver();
        registry.register_service(cluster_service()).unwrap();
        let response = resolver.resolve(&DnsQuestion {
            name: "web.default.svc.cluster.local.".to_string(),
            qtype: QueryType::A,
        });
        match response {
            Resolution::Answer(resp) => {
                assert_eq!(resp.code, ResponseCode::NoError);
                assert_eq!(resp.answers.len(), 1);
                match &resp.answers[0] {
                    DnsRecord::A { address, ttl, .. } => {
                        assert_eq!(*address, Ipv4Addr::new(10, 0, 0, 1));
                        assert_eq!(*ttl, 60);
                    }
                    _ => panic!("unexpected answer type"),
                }
            }
            _ => panic!("expected answer"),
        }
    }

    #[test]
    fn resolves_headless_endpoints() {
        let (registry, resolver) = build_resolver();
        registry.register_service(headless_service()).unwrap();
        registry
            .register_endpoint(endpoint("pod-1", IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))))
            .unwrap();
        registry
            .register_endpoint(endpoint("pod-2", IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3))))
            .unwrap();

        let response = resolver.resolve(&DnsQuestion {
            name: "web.default.svc.cluster.local.".to_string(),
            qtype: QueryType::A,
        });
        match response {
            Resolution::Answer(resp) => {
                assert_eq!(resp.answers.len(), 2);
            }
            _ => panic!("expected answer"),
        }
    }

    #[test]
    fn resolves_endpoint_hostname_for_headless() {
        let (registry, resolver) = build_resolver();
        registry.register_service(headless_service()).unwrap();
        registry
            .register_endpoint(endpoint("pod-1", IpAddr::V4(Ipv4Addr::new(10, 0, 0, 4))))
            .unwrap();

        let response = resolver.resolve(&DnsQuestion {
            name: "pod-1.web.default.svc.cluster.local.".to_string(),
            qtype: QueryType::A,
        });
        match response {
            Resolution::Answer(resp) => {
                assert_eq!(resp.code, ResponseCode::NoError);
                assert_eq!(resp.answers.len(), 1);
            }
            _ => panic!("expected answer"),
        }
    }

    #[test]
    fn resolves_srv_records_for_cluster_ip() {
        let (registry, resolver) = build_resolver();
        registry.register_service(cluster_service()).unwrap();
        let response = resolver.resolve(&DnsQuestion {
            name: "_http._tcp.web.default.svc.cluster.local.".to_string(),
            qtype: QueryType::SRV,
        });
        match response {
            Resolution::Answer(resp) => {
                assert_eq!(resp.answers.len(), 1);
                match &resp.answers[0] {
                    DnsRecord::Srv { port, target, .. } => {
                        assert_eq!(*port, 8080);
                        assert_eq!(target, "web.default.svc.cluster.local.");
                    }
                    _ => panic!("unexpected record"),
                }
            }
            _ => panic!("expected answer"),
        }
    }

    #[test]
    fn resolves_srv_records_for_headless() {
        let (registry, resolver) = build_resolver();
        registry.register_service(headless_service()).unwrap();
        registry
            .register_endpoint(endpoint("pod-1", IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5))))
            .unwrap();
        let response = resolver.resolve(&DnsQuestion {
            name: "_http._tcp.web.default.svc.cluster.local.".to_string(),
            qtype: QueryType::SRV,
        });
        match response {
            Resolution::Answer(resp) => {
                assert_eq!(resp.answers.len(), 1);
                match &resp.answers[0] {
                    DnsRecord::Srv { target, .. } => {
                        assert_eq!(target, "pod-1.web.default.svc.cluster.local.");
                    }
                    _ => panic!("unexpected record"),
                }
            }
            _ => panic!("expected answer"),
        }
    }

    #[test]
    fn returns_nxdomain_for_unknown_service() {
        let (_, resolver) = build_resolver();
        let response = resolver.resolve(&DnsQuestion {
            name: "missing.default.svc.cluster.local.".to_string(),
            qtype: QueryType::A,
        });
        match response {
            Resolution::Answer(resp) => assert_eq!(resp.code, ResponseCode::NxDomain),
            _ => panic!("expected answer"),
        }
    }

    #[test]
    fn applies_default_ttl_when_missing() {
        let (registry, resolver) = build_resolver();
        let mut svc = headless_service();
        svc.ttl_seconds = None;
        registry.register_service(svc).unwrap();
        registry
            .register_endpoint(endpoint("pod-1", IpAddr::V4(Ipv4Addr::new(10, 0, 0, 6))))
            .unwrap();
        let response = resolver.resolve(&DnsQuestion {
            name: "pod-1.web.default.svc.cluster.local.".to_string(),
            qtype: QueryType::A,
        });
        match response {
            Resolution::Answer(resp) => match &resp.answers[0] {
                DnsRecord::A { ttl, .. } => {
                    assert_eq!(*ttl, DnsConfig::default().default_ttl_seconds)
                }
                _ => panic!("unexpected record"),
            },
            _ => panic!("expected answer"),
        }
    }

    #[test]
    fn refuses_out_of_zone_without_upstream() {
        let (_, resolver) = build_resolver();
        let response = resolver.resolve(&DnsQuestion {
            name: "example.com.".to_string(),
            qtype: QueryType::A,
        });
        match response {
            Resolution::Answer(resp) => assert_eq!(resp.code, ResponseCode::Refused),
            _ => panic!("expected direct answer"),
        }
    }
}
