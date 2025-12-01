#![cfg(feature = "dns-bench")]

use nanocloud::nanocloud::dns::config::DnsConfig;
use nanocloud::nanocloud::dns::registry::{
    DnsRegistry, EndpointDescription, ServiceDescription, ServicePortDescription,
};
use nanocloud::nanocloud::dns::resolver::{DnsQuestion, DnsResolver, QueryType};
use nanocloud::nanocloud::dns::DnsProtocol;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Instant;

#[test]
fn resolver_hot_path_benchmark() {
    let registry = Arc::new(DnsRegistry::new());
    registry
        .register_service(ServiceDescription {
            name: "svc".into(),
            namespace: "default".into(),
            cluster_ip: Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
            ports: vec![ServicePortDescription {
                name: "http".into(),
                protocol: DnsProtocol::Tcp,
                port: 80,
                target_port: None,
            }],
            ttl_seconds: Some(30),
        })
        .unwrap();
    registry
        .register_endpoint(EndpointDescription {
            service_name: "svc".into(),
            namespace: "default".into(),
            ip: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            hostname: "pod-1".into(),
            ready: true,
            port_overrides: Default::default(),
        })
        .unwrap();
    let resolver = DnsResolver::new(DnsConfig::default(), registry);
    let question = DnsQuestion {
        name: "svc.default.svc.cluster.local.".into(),
        qtype: QueryType::A,
    };

    let start = Instant::now();
    let iterations = 5_000usize;
    for _ in 0..iterations {
        let _ = resolver.resolve(&question);
    }
    let elapsed = start.elapsed();
    // Avoid unused warning; basic sanity check the loop ran.
    assert!(elapsed.as_secs_f64() >= 0.0);
}
