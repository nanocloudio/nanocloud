use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

use crate::nanocloud::controller::runtime::ControllerRuntime;
use crate::nanocloud::dns::DnsService;

pub async fn dump_registry() -> impl IntoResponse {
    let Some(dns) = ControllerRuntime::shared().dependency::<DnsService>() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let snapshot = dns.registry().snapshot();
    let mut services = Vec::new();
    for svc in snapshot.services() {
        services.push(DebugService::from_snapshot(&svc));
    }
    Json(DnsRegistryDebug { services }).into_response()
}

#[derive(Serialize)]
struct DnsRegistryDebug {
    services: Vec<DebugService>,
}

#[derive(Serialize)]
struct DebugService {
    name: String,
    namespace: String,
    cluster_ip: Option<String>,
    ttl_seconds: Option<u32>,
    ports: Vec<DebugPort>,
    endpoints: Vec<DebugEndpoint>,
}

#[derive(Serialize)]
struct DebugPort {
    name: String,
    protocol: String,
    port: u16,
    target_port: Option<u16>,
}

#[derive(Serialize)]
struct DebugEndpoint {
    id: u64,
    ip: String,
    hostname: String,
    ready: bool,
    port_overrides: Vec<(String, u16)>,
}

impl DebugService {
    fn from_snapshot(snapshot: &crate::nanocloud::dns::registry::ServiceSnapshot) -> Self {
        let ports = snapshot
            .ports
            .values()
            .map(|port| DebugPort {
                name: port.name.clone(),
                protocol: port.protocol.as_str().to_string(),
                port: port.port,
                target_port: port.target_port,
            })
            .collect();
        let endpoints = snapshot
            .endpoints
            .iter()
            .map(|ep| DebugEndpoint {
                id: ep.id.as_u64(),
                ip: ep.ip.to_string(),
                hostname: ep.hostname.clone(),
                ready: ep.ready,
                port_overrides: ep
                    .port_overrides
                    .iter()
                    .map(|(k, v)| (k.clone(), *v))
                    .collect(),
            })
            .collect();
        Self {
            name: snapshot.name.clone(),
            namespace: snapshot.namespace.clone(),
            cluster_ip: snapshot.cluster_ip.map(|ip| ip.to_string()),
            ttl_seconds: snapshot.ttl_seconds,
            ports,
            endpoints,
        }
    }
}
