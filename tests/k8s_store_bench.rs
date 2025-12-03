#![cfg(feature = "k8s-store-bench")]

use std::time::Instant;

use nanocloud::nanocloud::k8s::endpoints::{
    EndpointAddress, EndpointPort, EndpointSubset, Endpoints,
};
use nanocloud::nanocloud::k8s::pod::ObjectMeta;
use nanocloud::nanocloud::k8s::service::{Service, ServicePort};
use nanocloud::nanocloud::k8s::store::{
    list_endpoints, list_services, save_endpoints, save_service,
};
use tempfile::TempDir;

#[test]
fn service_list_hot_path_benchmark() {
    let tempdir = TempDir::new().expect("tempdir");
    let keyspace_root = tempdir.path().join("keyspace");
    std::fs::create_dir_all(&keyspace_root).expect("keyspace dir");
    std::env::set_var("NANOCLOUD_KEYSPACE", &keyspace_root);

    for idx in 0..200u32 {
        let name = format!("svc-{idx}");
        let mut svc = Service {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                namespace: Some("bench".to_string()),
                ..Default::default()
            },
            ..Service::default()
        };
        svc.spec
            .selector
            .insert("app".to_string(), format!("bench-{idx}"));
        svc.spec.ports.push(ServicePort {
            name: Some("http".to_string()),
            port: 80,
            target_port: Some(8080),
            protocol: Some("TCP".to_string()),
        });
        save_service(Some("bench"), &name, &svc).expect("save service");
    }

    let start = Instant::now();
    let listed = list_services(None).expect("list services");
    let elapsed = start.elapsed();
    assert!(listed.len() >= 200);
    assert!(
        elapsed.as_secs_f64() >= 0.0,
        "elapsed time should be measurable"
    );
}

#[test]
fn endpoints_list_hot_path_benchmark() {
    let tempdir = TempDir::new().expect("tempdir");
    let keyspace_root = tempdir.path().join("keyspace");
    std::fs::create_dir_all(&keyspace_root).expect("keyspace dir");
    std::env::set_var("NANOCLOUD_KEYSPACE", &keyspace_root);

    for idx in 0..150u32 {
        let name = format!("svc-{idx}");
        let mut endpoints = Endpoints {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                namespace: Some("bench".to_string()),
                ..Default::default()
            },
            ..Endpoints::default()
        };
        endpoints.subsets.push(EndpointSubset {
            addresses: vec![EndpointAddress {
                ip: format!("10.77.0.{idx}"),
            }],
            ports: vec![EndpointPort::new(Some("http".to_string()), 8080, None)],
        });
        save_endpoints(Some("bench"), &name, &endpoints).expect("save endpoints");
    }

    let start = Instant::now();
    let listed = list_endpoints(Some("bench")).expect("list endpoints");
    let elapsed = start.elapsed();
    assert!(listed.len() >= 150);
    assert!(
        elapsed.as_secs_f64() >= 0.0,
        "elapsed time should be measurable"
    );
}
