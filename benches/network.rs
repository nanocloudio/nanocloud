#![cfg(feature = "network-bench")]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use nanocloud::nanocloud::k8s::endpoints::{EndpointAddress, EndpointSubset, Endpoints};
use nanocloud::nanocloud::k8s::pod::ObjectMeta;
use nanocloud::nanocloud::k8s::service::{Service, ServicePort, ServiceSpec, ServiceStatus};
use nanocloud::nanocloud::network::policy::{
    PolicyChain, PolicyDirection, PolicyProgrammer, PolicyRule,
};
use nanocloud::nanocloud::network::proxy;
use std::env;
use tempfile::tempdir;

fn bench_policy_sync(c: &mut Criterion) {
    let dir = tempdir().expect("tempdir");
    let log_path = dir.path().join("nft-bench.log");
    env::set_var("NANOCLOUD_NFT_RECORD", &log_path);
    env::set_var("NANOCLOUD_NFT", "/usr/sbin/nft");

    let programmer = PolicyProgrammer::shared().expect("policy programmer");
    let chain = PolicyChain::new(
        "default",
        "bench-pod",
        "10.203.1.10",
        PolicyDirection::Ingress,
        vec![PolicyRule {
            cidr: Some("10.1.0.0/24".into()),
            protocol: Some("tcp".into()),
            port: Some(8080),
        }],
    );

    programmer.sync(&[]).ok();

    c.bench_function("policy_sync_single_chain", |b| {
        b.iter(|| programmer.sync(black_box(std::slice::from_ref(&chain))))
    });
}

fn bench_proxy_program(c: &mut Criterion) {
    let dir = tempdir().expect("tempdir");
    let log_path = dir.path().join("iptables-bench.log");
    env::set_var("NANOCLOUD_IPTABLES_RECORD", &log_path);
    env::set_var("NANOCLOUD_IPTABLES", "/sbin/iptables");

    let service = bench_service();
    let endpoints = bench_endpoints();

    c.bench_function("proxy_program_service", |b| {
        b.iter(|| proxy::program_service(black_box(&service), black_box(&endpoints)))
    });
}

fn bench_service() -> Service {
    Service {
        metadata: ObjectMeta {
            name: Some("svc".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: ServiceSpec {
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                port: 80,
                target_port: Some(8080),
                protocol: Some("TCP".to_string()),
            }],
            ..Default::default()
        },
        status: Some(ServiceStatus {
            cluster_ip: Some("10.203.0.12".to_string()),
        }),
        ..Default::default()
    }
}

fn bench_endpoints() -> Endpoints {
    Endpoints {
        metadata: ObjectMeta {
            name: Some("svc".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        subsets: vec![EndpointSubset {
            addresses: vec![
                EndpointAddress {
                    ip: "10.1.0.30".to_string(),
                },
                EndpointAddress {
                    ip: "10.1.0.31".to_string(),
                },
            ],
            ports: Vec::new(),
        }],
        ..Default::default()
    }
}

criterion_group!(network, bench_policy_sync, bench_proxy_program);
criterion_main!(network);
