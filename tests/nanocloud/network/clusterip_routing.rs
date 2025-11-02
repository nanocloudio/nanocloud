use std::env;

use nanocloud::nanocloud::k8s::endpoints::{EndpointAddress, EndpointSubset, Endpoints};
use nanocloud::nanocloud::k8s::pod::ObjectMeta;
use nanocloud::nanocloud::k8s::service::{Service, ServicePort, ServiceStatus};
use nanocloud::nanocloud::network::proxy;
use nanocloud::nanocloud::observability::metrics;
use tempfile::tempdir;

fn sample_service() -> Service {
    Service {
        metadata: ObjectMeta {
            name: Some("web".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: nanocloud::nanocloud::k8s::service::ServiceSpec {
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                port: 80,
                target_port: Some(8080),
                protocol: Some("TCP".to_string()),
            }],
            ..Default::default()
        },
        status: Some(ServiceStatus {
            cluster_ip: Some("10.203.0.40".to_string()),
        }),
        ..Default::default()
    }
}

fn sample_endpoints() -> Endpoints {
    Endpoints {
        metadata: ObjectMeta {
            name: Some("web".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        subsets: vec![EndpointSubset {
            addresses: vec![
                EndpointAddress {
                    ip: "10.1.0.21".to_string(),
                },
                EndpointAddress {
                    ip: "10.1.0.22".to_string(),
                },
            ],
            ports: Vec::new(),
        }],
        ..Default::default()
    }
}

#[test]
fn cluster_ip_programming_records_iptables_commands() {
    let dir = tempdir().expect("tempdir");
    let log_path = dir.path().join("iptables.log");
    env::set_var("NANOCLOUD_IPTABLES_RECORD", &log_path);
    env::set_var("NANOCLOUD_IPTABLES", "/sbin/iptables");

    let service = sample_service();
    let endpoints = sample_endpoints();

    proxy::program_service(&service, &endpoints).expect("program service");
    proxy::remove_service(&service).expect("remove service");

    let log = std::fs::read_to_string(&log_path).expect("read iptables log");
    assert!(
        log.contains("-A NCLD-SERVICES"),
        "primary chain should be programmed: {log}"
    );
    assert!(
        log.contains("DNAT --to-destination 10.1.0.21:8080"),
        "first endpoint DNAT missing: {log}"
    );
    assert!(
        log.contains("DNAT --to-destination 10.1.0.22:8080"),
        "second endpoint DNAT missing: {log}"
    );
    assert!(
        log.contains("--probability 0.500000"),
        "random load-balancing not configured: {log}"
    );
    assert!(
        log.contains("-D NCLD-SERVICES"),
        "service rule removal missing: {log}"
    );

    let metrics_body = metrics::gather().expect("gather metrics");
    let metrics_text = String::from_utf8(metrics_body).expect("metrics utf8");
    assert!(
        metrics_text.contains("nanocloud_proxy_operations_total"),
        "proxy metrics missing: {metrics_text}"
    );
    assert!(
        metrics_text.contains("operation=\"program\""),
        "program metric missing: {metrics_text}"
    );
    assert!(
        metrics_text.contains("operation=\"remove\""),
        "remove metric missing: {metrics_text}"
    );

    env::remove_var("NANOCLOUD_IPTABLES_RECORD");
    env::remove_var("NANOCLOUD_IPTABLES");
}
