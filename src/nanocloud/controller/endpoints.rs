use crate::nanocloud::controller::runtime::ControllerRuntime;
use crate::nanocloud::controller::watch::{ControllerWatchEvent, ControllerWatchManager};
use crate::nanocloud::dns::{
    DnsProtocol, DnsService, EndpointDescription, EndpointId, ServiceDescription,
    ServicePortDescription,
};
use crate::nanocloud::k8s::endpoints::{
    EndpointAddress, EndpointPort, EndpointSubset, Endpoints, EndpointsRegistry,
};
use crate::nanocloud::k8s::pod::{ObjectMeta, PodStatus};
use crate::nanocloud::k8s::service::{Service, ServicePort};
use crate::nanocloud::k8s::service_registry::ServiceRegistry;
use crate::nanocloud::kubelet::Kubelet;
use crate::nanocloud::logger::{log_debug, log_warn};
use crate::nanocloud::network::proxy;
use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::util::KeyspaceEventType;
use serde_json;

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::net::IpAddr;
use std::str::FromStr;
use tokio::task::JoinHandle;

const COMPONENT: &str = "endpoints-controller";
const POD_PREFIX: &str = "/pods";
const SERVICE_PREFIX: &str = "/services";

pub fn spawn() -> JoinHandle<()> {
    tokio::spawn(async move {
        bootstrap_services().await;
        bootstrap_pod_dns().await;
        let manager = ControllerWatchManager::shared();
        let mut pod_watch = manager.subscribe(POD_PREFIX, None);
        let mut service_watch = manager.subscribe(SERVICE_PREFIX, None);

        loop {
            tokio::select! {
                event = pod_watch.recv() => {
                    match event {
                        Some(evt) => handle_pod_event(evt).await,
                        None => break,
                    }
                }
                event = service_watch.recv() => {
                    match event {
                        Some(evt) => handle_service_event(evt).await,
                        None => break,
                    }
                }
            }
        }
    })
}

async fn bootstrap_services() {
    let registry = ServiceRegistry::shared();
    let services = registry.list(None);
    for service in services {
        if let Err(err) = reconcile_service(&service).await {
            log_warn(
                COMPONENT,
                "Failed to reconcile endpoints during bootstrap",
                &[
                    (
                        "service",
                        service
                            .metadata
                            .name
                            .as_deref()
                            .unwrap_or("<unnamed-service>"),
                    ),
                    (
                        "namespace",
                        service.metadata.namespace.as_deref().unwrap_or("default"),
                    ),
                    ("error", err.to_string().as_str()),
                ],
            );
        }
    }
}

async fn bootstrap_pod_dns() {
    let kubelet = Kubelet::shared();
    match kubelet.list_pods(None).await {
        Ok(pods) => {
            let namespaces: HashSet<String> = pods
                .iter()
                .filter_map(|pod| {
                    pod.metadata
                        .namespace
                        .clone()
                        .or_else(|| Some("default".to_string()))
                })
                .collect();
            for namespace in namespaces {
                reconcile_pod_headless_services(&namespace).await;
            }
        }
        Err(err) => {
            log_warn(
                COMPONENT,
                "Failed to list pods during DNS bootstrap",
                &[("error", err.to_string().as_str())],
            );
        }
    };
}

async fn handle_pod_event(event: ControllerWatchEvent) {
    if let Some((namespace, _)) = parse_key(POD_PREFIX, event.key.as_str()) {
        reconcile_services_in_namespace(&namespace).await;
        reconcile_pod_headless_services(&namespace).await;
    }
}

async fn handle_service_event(event: ControllerWatchEvent) {
    if matches!(event.event_type, KeyspaceEventType::Deleted) {
        if let Some((namespace, name)) = parse_key(SERVICE_PREFIX, event.key.as_str()) {
            tokio::spawn(remove_dns_service(namespace, name));
        }
        return;
    }
    if let Some(service) = load_service_from_event(&event) {
        if let Err(err) = reconcile_service(&service).await {
            log_warn(
                COMPONENT,
                "Failed to reconcile service endpoints",
                &[
                    (
                        "service",
                        service
                            .metadata
                            .name
                            .as_deref()
                            .unwrap_or("<unnamed-service>"),
                    ),
                    (
                        "namespace",
                        service.metadata.namespace.as_deref().unwrap_or("default"),
                    ),
                    ("error", err.to_string().as_str()),
                ],
            );
        }
    }
}

async fn reconcile_services_in_namespace(namespace: &str) {
    let registry = ServiceRegistry::shared();
    let services = registry.list(Some(namespace));
    for service in services {
        if let Err(err) = reconcile_service(&service).await {
            log_debug(
                COMPONENT,
                "Endpoint reconciliation after pod update failed",
                &[
                    (
                        "service",
                        service
                            .metadata
                            .name
                            .as_deref()
                            .unwrap_or("<unnamed-service>"),
                    ),
                    ("namespace", namespace),
                    ("error", err.to_string().as_str()),
                ],
            );
        }
    }
}

fn parse_key(prefix: &str, key: &str) -> Option<(String, String)> {
    let parts = key
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }
    if parts[0] != prefix.trim_start_matches('/') {
        return None;
    }
    let namespace = parts[1].to_string();
    let name = parts[2].to_string();
    Some((namespace, name))
}

fn load_service_from_event(event: &ControllerWatchEvent) -> Option<Service> {
    if let Some(value) = event.value.as_ref() {
        if let Ok(service) = serde_json::from_str::<Service>(value) {
            return Some(service);
        }
    }
    if let Some((namespace, name)) = parse_key(SERVICE_PREFIX, event.key.as_str()) {
        return ServiceRegistry::shared().get(&namespace, &name);
    }
    None
}

pub async fn reconcile_service(service: &Service) -> Result<(), Box<dyn Error + Send + Sync>> {
    let name = service.metadata.name.as_deref().ok_or_else(|| {
        new_error("Service metadata.name is required for endpoints reconciliation")
    })?;
    let namespace = service.metadata.namespace.as_deref().unwrap_or("default");

    let selector = &service.spec.selector;
    let kubelet = Kubelet::shared();
    let candidate_pods = kubelet
        .list_pods(Some(namespace))
        .await
        .map_err(|err| with_context(err, "Failed to list pods for DNS reconciliation"))?;
    let mut addresses = Vec::new();
    let mut dns_endpoints = Vec::new();

    for pod in candidate_pods.iter() {
        if !matches_selector(selector, &pod.metadata.labels) {
            continue;
        }
        if let Some(status) = pod.status.as_ref() {
            if let Some(ip) = status.pod_ip.as_deref() {
                addresses.push(EndpointAddress { ip: ip.to_string() });
                let hostname = pod.metadata.name.clone().unwrap_or_default();
                let ready = pod_ready(status);
                dns_endpoints.push(DnsEndpoint {
                    ip: ip.to_string(),
                    hostname,
                    ready,
                });
            }
        }
    }

    let ports = map_service_ports(&service.spec.ports);
    let subsets = if addresses.is_empty() {
        Vec::new()
    } else {
        vec![EndpointSubset { addresses, ports }]
    };

    let metadata = ObjectMeta {
        name: Some(name.to_string()),
        namespace: Some(namespace.to_string()),
        labels: service.metadata.labels.clone(),
        annotations: service.metadata.annotations.clone(),
        ..Default::default()
    };

    let endpoints = Endpoints {
        metadata,
        subsets,
        ..Default::default()
    };

    let registry = EndpointsRegistry::shared();
    registry
        .upsert(endpoints.clone())
        .map_err(|err| with_context(err, "Failed to persist endpoints"))?;
    if has_cluster_ip(service) {
        proxy::program_service(service, &endpoints)?;
    }
    tokio::spawn(update_dns_records(service.clone(), dns_endpoints));
    Ok(())
}

pub fn remove_service(service: &Service) -> Result<(), Box<dyn Error + Send + Sync>> {
    let name = service
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| new_error("Service metadata.name missing"))?;
    let namespace = service.metadata.namespace.as_deref().unwrap_or("default");
    EndpointsRegistry::shared()
        .remove(namespace, name)
        .map_err(|err| with_context(err, "Failed to delete service endpoints"))?;
    if has_cluster_ip(service) {
        proxy::remove_service(service)?;
    }
    tokio::spawn(remove_dns_service(namespace.to_string(), name.to_string()));
    Ok(())
}

fn matches_selector(selector: &HashMap<String, String>, labels: &HashMap<String, String>) -> bool {
    if selector.is_empty() {
        return false;
    }
    selector
        .iter()
        .all(|(key, value)| labels.get(key).map(|v| v == value).unwrap_or(false))
}

fn map_service_ports(ports: &[ServicePort]) -> Vec<EndpointPort> {
    if ports.is_empty() {
        return Vec::new();
    }
    ports
        .iter()
        .map(|port| {
            EndpointPort::new(
                port.name.clone(),
                port.port,
                port.protocol.clone().or_else(|| Some("TCP".to_string())),
            )
        })
        .collect()
}

fn has_cluster_ip(service: &Service) -> bool {
    service
        .status
        .as_ref()
        .and_then(|status| status.cluster_ip.as_ref())
        .map(|ip| !ip.is_empty())
        .unwrap_or(false)
}

#[derive(Clone)]
struct DnsEndpoint {
    ip: String,
    hostname: String,
    ready: bool,
}

fn pod_ready(status: &PodStatus) -> bool {
    if status.conditions.is_empty() {
        return true;
    }
    status
        .conditions
        .iter()
        .any(|cond| cond.condition_type == "Ready" && cond.status == "True")
}

async fn update_dns_records(service: Service, endpoints: Vec<DnsEndpoint>) {
    let Some(dns) = ControllerRuntime::shared().dependency::<DnsService>() else {
        return;
    };
    let registry = dns.registry();
    let namespace = service.metadata.namespace.as_deref().unwrap_or("default");
    let name = match service.metadata.name.as_deref() {
        Some(value) => value,
        None => return,
    };

    let cluster_ip = service
        .status
        .as_ref()
        .and_then(|status| status.cluster_ip.as_ref())
        .or(service.spec.cluster_ip.as_ref())
        .and_then(|ip| IpAddr::from_str(ip).ok());

    let ports: Vec<ServicePortDescription> = service
        .spec
        .ports
        .iter()
        .filter_map(|port| {
            port.name.as_ref().map(|name| ServicePortDescription {
                name: name.clone(),
                protocol: map_protocol(port.protocol.as_deref()),
                port: port.port,
                target_port: port.target_port,
            })
        })
        .collect();

    let desc = ServiceDescription {
        name: name.to_string(),
        namespace: namespace.to_string(),
        cluster_ip,
        ports,
        ttl_seconds: None,
    };

    if let Err(err) = registry.register_service(desc) {
        log_warn(
            COMPONENT,
            "Failed to register DNS service",
            &[
                ("service", name),
                ("namespace", namespace),
                ("error", err.to_string().as_str()),
            ],
        );
        return;
    }

    let mut keep: HashSet<EndpointId> = HashSet::new();

    for endpoint in endpoints {
        if let Ok(ip) = IpAddr::from_str(&endpoint.ip) {
            let desc = EndpointDescription {
                service_name: name.to_string(),
                namespace: namespace.to_string(),
                ip,
                hostname: endpoint.hostname,
                ready: endpoint.ready,
                port_overrides: HashMap::new(),
            };
            match registry.register_endpoint(desc) {
                Ok(id) => {
                    keep.insert(id);
                }
                Err(err) => log_warn(
                    COMPONENT,
                    "Failed to register DNS endpoint",
                    &[
                        ("service", name),
                        ("namespace", namespace),
                        ("error", err.to_string().as_str()),
                    ],
                ),
            }
        }
    }

    if let Some(existing) = registry.snapshot().service(namespace, name) {
        for endpoint in existing.endpoints.iter() {
            if !keep.contains(&endpoint.id) {
                let _ = registry.remove_endpoint(endpoint.id);
            }
        }
    }
}

async fn reconcile_pod_headless_services(namespace: &str) {
    let Some(dns) = ControllerRuntime::shared().dependency::<DnsService>() else {
        return;
    };
    let kubelet = Kubelet::shared();
    let pods = match kubelet.list_pods(Some(namespace)).await {
        Ok(pods) => pods,
        Err(err) => {
            log_warn(
                COMPONENT,
                "Failed to list pods for DNS reconciliation",
                &[("error", err.to_string().as_str())],
            );
            return;
        }
    };

    let registry = dns.registry();
    for pod in pods.into_iter() {
        let Some(status) = pod.status.as_ref() else {
            continue;
        };
        let Some(ip) = status.pod_ip.as_ref() else {
            continue;
        };
        let name = pod.metadata.name.clone().unwrap_or_default();
        let ip_parsed = match IpAddr::from_str(ip) {
            Ok(addr) => addr,
            Err(_) => continue,
        };
        let _ = registry.register_service(ServiceDescription {
            name: name.clone(),
            namespace: namespace.to_string(),
            cluster_ip: None,
            ports: Vec::new(),
            ttl_seconds: None,
        });
        let endpoint = EndpointDescription {
            service_name: name.clone(),
            namespace: namespace.to_string(),
            ip: ip_parsed,
            hostname: name.clone(),
            ready: pod_ready(status),
            port_overrides: HashMap::new(),
        };
        let _ = registry.register_endpoint(endpoint);
    }
}

async fn remove_dns_service(namespace: String, name: String) {
    let Some(dns) = ControllerRuntime::shared().dependency::<DnsService>() else {
        return;
    };
    let _ = dns.registry().remove_service(&namespace, &name);
}

fn map_protocol(value: Option<&str>) -> DnsProtocol {
    match value.map(|v| v.to_ascii_lowercase()) {
        Some(ref proto) if proto == "udp" => DnsProtocol::Udp,
        _ => DnsProtocol::Tcp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::test_support::keyspace_lock;
    use std::env;
    use std::fs;
    use std::fs::File;
    use std::path::Path;
    use std::sync::MutexGuard;
    use tokio::runtime::Runtime;

    struct TestEnv {
        dir: tempfile::TempDir,
        previous_keyspace: Option<String>,
        previous_lock_file: Option<String>,
        _guard: MutexGuard<'static, ()>,
    }

    impl TestEnv {
        fn new() -> Self {
            let guard = keyspace_lock().lock();
            let dir = tempfile::tempdir().expect("create tempdir");
            let keyspace_dir = dir.path().join("keyspace");
            fs::create_dir_all(&keyspace_dir).expect("keyspace dir");

            let lock_dir = dir.path().join("lock");
            fs::create_dir_all(&lock_dir).expect("lock dir");
            let lock_file = lock_dir.join("nanocloud.lock");
            File::create(&lock_file).expect("lock file");

            let previous_keyspace = env::var("NANOCLOUD_KEYSPACE").ok();
            env::set_var("NANOCLOUD_KEYSPACE", &keyspace_dir);

            let previous_lock_file = env::var("NANOCLOUD_LOCK_FILE").ok();
            env::set_var("NANOCLOUD_LOCK_FILE", &lock_file);

            Self {
                dir,
                previous_keyspace,
                previous_lock_file,
                _guard: guard,
            }
        }

        fn root(&self) -> &Path {
            self.dir.path()
        }
    }

    impl Drop for TestEnv {
        fn drop(&mut self) {
            match self.previous_keyspace.as_ref() {
                Some(value) => env::set_var("NANOCLOUD_KEYSPACE", value),
                None => env::remove_var("NANOCLOUD_KEYSPACE"),
            }
            match self.previous_lock_file.as_ref() {
                Some(value) => env::set_var("NANOCLOUD_LOCK_FILE", value),
                None => env::remove_var("NANOCLOUD_LOCK_FILE"),
            }
        }
    }

    #[test]
    fn matches_selector_respects_labels() {
        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "demo".to_string());
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "demo".to_string());
        assert!(matches_selector(&selector, &labels));
        labels.insert("env".to_string(), "prod".to_string());
        assert!(matches_selector(&selector, &labels));
        labels.insert("app".to_string(), "other".to_string());
        assert!(!matches_selector(&selector, &labels));
    }

    #[test]
    fn map_service_ports_defaults_protocol() {
        let ports = vec![ServicePort {
            name: Some("http".to_string()),
            port: 80,
            target_port: Some(8080),
            protocol: None,
        }];
        let mapped = map_service_ports(&ports);
        assert_eq!(mapped.len(), 1);
        assert_eq!(mapped[0].protocol.as_deref(), Some("TCP"));
    }

    #[test]
    fn reconcile_without_matches_clears_endpoints() {
        let env = TestEnv::new();
        let log_path = env.root().join("iptables.log");
        env::set_var("NANOCLOUD_IPTABLES_RECORD", &log_path);
        let mut service = Service::default();
        service.metadata.name.replace("demo-service".to_string());
        service.metadata.namespace.replace("default".to_string());
        service
            .spec
            .selector
            .insert("app".to_string(), "demo".to_string());

        let rt = Runtime::new().expect("runtime");
        rt.block_on(reconcile_service(&service)).expect("reconcile");
        let key_path = env
            .root()
            .join("keyspace")
            .join("k8s")
            .join("endpoints")
            .join("default")
            .join("demo-service")
            .join("_value_");
        assert!(key_path.exists());
        env::remove_var("NANOCLOUD_IPTABLES_RECORD");
    }
}
