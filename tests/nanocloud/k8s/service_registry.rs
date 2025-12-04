use std::env;
use std::sync::MutexGuard;

use nanocloud::nanocloud::k8s::endpoints::{
    EndpointAddress, EndpointPort, EndpointSubset, Endpoints, EndpointsRegistry,
};
use nanocloud::nanocloud::k8s::pod::ObjectMeta;
use nanocloud::nanocloud::k8s::service::{Service, ServicePort};
use nanocloud::nanocloud::k8s::service_registry::ServiceRegistry;
use nanocloud::nanocloud::k8s::store::{
    endpoints_cache_metrics, list_endpoints, list_services, service_cache_metrics,
};
use nanocloud::nanocloud::test_support::keyspace_lock;
use serial_test::serial;
use tempfile::TempDir;

struct TestEnv {
    _guard: MutexGuard<'static, ()>,
    _tempdir: TempDir,
    previous_keyspace: Option<String>,
    previous_record: Option<String>,
    previous_service_cache: Option<String>,
    previous_endpoints_cache: Option<String>,
}

impl TestEnv {
    fn new(enable_service_cache: bool, enable_endpoints_cache: bool) -> Self {
        let guard = keyspace_lock().lock();
        let tempdir = TempDir::new().expect("tempdir");
        let keyspace_root = tempdir.path().join("keyspace");
        std::fs::create_dir_all(&keyspace_root).expect("keyspace dir");

        let previous_keyspace = env::var("NANOCLOUD_KEYSPACE").ok();
        let previous_record = env::var("NANOCLOUD_IPTABLES_RECORD").ok();
        let previous_service_cache = env::var("NANOCLOUD_K8S_CACHE_SERVICES").ok();
        let previous_endpoints_cache = env::var("NANOCLOUD_K8S_CACHE_ENDPOINTS").ok();

        env::set_var("NANOCLOUD_KEYSPACE", &keyspace_root);
        env::set_var(
            "NANOCLOUD_IPTABLES_RECORD",
            tempdir.path().join("iptables.log"),
        );
        if enable_service_cache {
            env::set_var("NANOCLOUD_K8S_CACHE_SERVICES", "true");
        } else {
            env::remove_var("NANOCLOUD_K8S_CACHE_SERVICES");
        }
        if enable_endpoints_cache {
            env::set_var("NANOCLOUD_K8S_CACHE_ENDPOINTS", "true");
        } else {
            env::remove_var("NANOCLOUD_K8S_CACHE_ENDPOINTS");
        }

        TestEnv {
            _guard: guard,
            _tempdir: tempdir,
            previous_keyspace,
            previous_record,
            previous_service_cache,
            previous_endpoints_cache,
        }
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        match self.previous_keyspace.as_ref() {
            Some(value) => env::set_var("NANOCLOUD_KEYSPACE", value),
            None => env::remove_var("NANOCLOUD_KEYSPACE"),
        }
        match self.previous_record.as_ref() {
            Some(value) => env::set_var("NANOCLOUD_IPTABLES_RECORD", value),
            None => env::remove_var("NANOCLOUD_IPTABLES_RECORD"),
        }
        match self.previous_service_cache.as_ref() {
            Some(value) => env::set_var("NANOCLOUD_K8S_CACHE_SERVICES", value),
            None => env::remove_var("NANOCLOUD_K8S_CACHE_SERVICES"),
        }
        match self.previous_endpoints_cache.as_ref() {
            Some(value) => env::set_var("NANOCLOUD_K8S_CACHE_ENDPOINTS", value),
            None => env::remove_var("NANOCLOUD_K8S_CACHE_ENDPOINTS"),
        }
    }
}

fn service_with(name: &str, namespace: &str, port: u16) -> Service {
    let mut svc = Service::default();
    svc.metadata.name = Some(name.to_string());
    svc.metadata.namespace = Some(namespace.to_string());
    svc.spec
        .selector
        .insert("app".to_string(), name.to_string());
    svc.spec.ports.push(ServicePort {
        name: Some("http".to_string()),
        port,
        target_port: Some(port + 1000),
        protocol: Some("TCP".to_string()),
    });
    svc
}

#[tokio::test]
#[serial]
async fn service_registry_paginates_and_caches_results() {
    let _env = TestEnv::new(true, true);
    let registry = ServiceRegistry::shared();

    let created_a = registry
        .create("ns-a", service_with("svc-a", "ns-a", 80))
        .expect("service a created");
    let rv_a = created_a
        .metadata
        .resource_version
        .clone()
        .unwrap_or_else(|| "0".to_string());
    registry
        .create("ns-b", service_with("svc-b", "ns-b", 81))
        .expect("service b created");

    let first_page = registry
        .list_paginated(None, None, Some(1), None)
        .expect("first page");
    assert_eq!(first_page.items.len(), 1);
    assert!(
        first_page.remaining >= 1,
        "remaining items should reflect second page"
    );
    let cursor = first_page.next_cursor.expect("continue token");
    let second_page = registry
        .list_paginated(None, None, Some(1), Some(&cursor))
        .expect("second page");
    assert_eq!(second_page.items.len(), 1);

    let since = registry.list_since(None, rv_a.parse::<u64>().ok());
    assert!(
        since
            .iter()
            .any(|svc| svc.metadata.name.as_deref() == Some("svc-b")),
        "newer service should appear when filtering by resourceVersion"
    );

    let _ = list_services(None).expect("list services populates cache");
    let _ = list_services(None).expect("cache hit");
    let cache_metrics = service_cache_metrics();
    assert!(
        cache_metrics.enabled,
        "service cache should be enabled for test"
    );
    assert!(
        cache_metrics.hits >= 1,
        "expected at least one cache hit after repeat list"
    );

    let _ = registry.delete("ns-a", "svc-a");
    let _ = registry.delete("ns-b", "svc-b");
}

#[tokio::test]
#[serial]
async fn endpoints_registry_versions_and_cache_hits() {
    let _env = TestEnv::new(true, true);
    let registry = EndpointsRegistry::shared();

    let mut endpoints = Endpoints {
        metadata: ObjectMeta {
            name: Some("svc-cache".to_string()),
            namespace: Some("dns".to_string()),
            ..Default::default()
        },
        ..Endpoints::default()
    };
    endpoints.subsets.push(EndpointSubset {
        addresses: vec![EndpointAddress {
            ip: "10.1.0.2".to_string(),
        }],
        ports: vec![EndpointPort::new(Some("http".to_string()), 80, None)],
    });

    let created = registry
        .upsert(endpoints.clone())
        .expect("upsert endpoints");
    let initial_rv = created
        .metadata
        .resource_version
        .clone()
        .unwrap_or_default();

    let listed = registry
        .list_paginated(Some("dns"), None, Some(1), None)
        .expect("paginated list");
    assert_eq!(listed.items.len(), 1);

    let _ = list_endpoints(Some("dns")).expect("populate endpoints cache");
    let _ = list_endpoints(Some("dns")).expect("cache hit");
    let cache_metrics = endpoints_cache_metrics();
    assert!(cache_metrics.enabled);
    assert!(
        cache_metrics.hits >= 1,
        "expected cache hits after repeat endpoints list"
    );

    let removed = registry
        .remove("dns", "svc-cache")
        .expect("remove endpoints");
    assert_ne!(
        removed.metadata.resource_version.as_deref(),
        Some(initial_rv.as_str()),
        "resourceVersion should change when removing endpoints"
    );
}
