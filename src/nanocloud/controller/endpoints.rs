use crate::nanocloud::k8s::endpoints::{EndpointAddress, EndpointPort, EndpointSubset, Endpoints};
use crate::nanocloud::k8s::pod::ObjectMeta;
use crate::nanocloud::k8s::service::{Service, ServicePort};
use crate::nanocloud::k8s::store;
use crate::nanocloud::network::proxy;
use crate::nanocloud::util::error::{new_error, with_context};

use std::collections::HashMap;
use std::error::Error;

pub fn reconcile_service(service: &Service) -> Result<(), Box<dyn Error + Send + Sync>> {
    let name = service.metadata.name.as_deref().ok_or_else(|| {
        new_error("Service metadata.name is required for endpoints reconciliation")
    })?;
    let namespace = service.metadata.namespace.as_deref().unwrap_or("default");

    let selector = &service.spec.selector;
    let candidate_pods = store::list_pod_manifests()?;
    let mut addresses = Vec::new();

    for pod in candidate_pods.iter() {
        if pod
            .workload
            .metadata
            .namespace
            .as_deref()
            .unwrap_or("default")
            != namespace
        {
            continue;
        }
        if !matches_selector(selector, &pod.workload.metadata.labels) {
            continue;
        }
        if let Some(status) = pod.workload.status.as_ref() {
            if let Some(ip) = status.pod_ip.as_deref() {
                addresses.push(EndpointAddress { ip: ip.to_string() });
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

    store::save_endpoints(Some(namespace), name, &endpoints)?;
    if has_cluster_ip(service) {
        proxy::program_service(service, &endpoints)?;
    }
    Ok(())
}

pub fn remove_service(service: &Service) -> Result<(), Box<dyn Error + Send + Sync>> {
    let name = service
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| new_error("Service metadata.name missing"))?;
    let namespace = service.metadata.namespace.as_deref().unwrap_or("default");
    store::delete_endpoints(Some(namespace), name)
        .map_err(|err| with_context(err, "Failed to delete service endpoints"))?;
    if has_cluster_ip(service) {
        proxy::remove_service(service)?;
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::test_support::keyspace_lock;
    use std::env;
    use std::fs;
    use std::fs::File;
    use std::path::Path;
    use std::sync::MutexGuard;

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

        reconcile_service(&service).expect("reconcile");
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
