#![allow(dead_code)]

use crate::nanocloud::controller::endpoints;
use crate::nanocloud::k8s::service::{Service, ServiceStatus};
use crate::nanocloud::k8s::store;
use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::util::Keyspace;

use std::collections::HashMap;
use std::error::Error;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

const CLUSTER_IP_KEYSPACE: Keyspace = Keyspace::new("network");
const ALLOC_SERVICE_PREFIX: &str = "/clusterip/services";
const ALLOC_IP_PREFIX: &str = "/clusterip/ips";
const NEXT_IP_KEY: &str = "/clusterip/state/next";

#[derive(Debug, Clone, Copy)]
struct ClusterIpRange {
    start: u32,
    end: u32,
}

impl ClusterIpRange {
    fn default() -> Self {
        let start = Ipv4Addr::new(10, 203, 0, 1);
        let end = Ipv4Addr::new(10, 203, 255, 254);
        ClusterIpRange {
            start: ipv4_to_u32(&start),
            end: ipv4_to_u32(&end),
        }
    }

    fn contains(&self, ip: u32) -> bool {
        ip >= self.start && ip <= self.end
    }
}

fn ipv4_to_u32(addr: &Ipv4Addr) -> u32 {
    u32::from_be_bytes(addr.octets())
}

fn u32_to_ipv4(value: u32) -> Ipv4Addr {
    Ipv4Addr::from(value.to_be_bytes())
}

struct ClusterIpAllocator {
    range: ClusterIpRange,
    mutex: Mutex<()>,
}

impl ClusterIpAllocator {
    fn new(range: ClusterIpRange) -> Self {
        Self {
            range,
            mutex: Mutex::new(()),
        }
    }

    fn service_key(namespace: &str, name: &str) -> String {
        format!("{}/{}/{}", ALLOC_SERVICE_PREFIX, namespace, name)
    }

    fn ip_key(ip: &str) -> String {
        format!("{}/{}", ALLOC_IP_PREFIX, ip)
    }

    fn allocate(
        &self,
        namespace: &str,
        name: &str,
        requested: Option<&str>,
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        let _guard = self.mutex.lock().expect("allocator mutex poisoned");
        let service_key = Self::service_key(namespace, name);
        if CLUSTER_IP_KEYSPACE.get(&service_key).is_ok() {
            return Err(new_error(format!(
                "Service '{namespace}/{name}' already has a ClusterIP"
            )));
        }

        if let Some(requested_ip) = requested {
            self.reserve_specific(namespace, name, requested_ip)
        } else {
            self.allocate_next(namespace, name)
        }
    }

    fn reserve_specific(
        &self,
        namespace: &str,
        name: &str,
        requested: &str,
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        let ip_int = parse_ipv4(requested).map_err(|err| {
            with_context(err, format!("Requested ClusterIP '{requested}' is invalid"))
        })?;
        if !self.range.contains(ip_int) {
            return Err(new_error(format!(
                "Requested ClusterIP '{requested}' is outside the allocator range"
            )));
        }
        let ip_key = Self::ip_key(requested);
        match CLUSTER_IP_KEYSPACE.get(&ip_key) {
            Ok(_) => {
                return Err(new_error(format!(
                    "Requested ClusterIP '{requested}' is already allocated"
                )))
            }
            Err(err) => {
                if !err.to_string().contains("No such file or directory")
                    && !err.to_string().contains("not found")
                {
                    return Err(err);
                }
            }
        }
        let service_key = Self::service_key(namespace, name);
        CLUSTER_IP_KEYSPACE.put(&service_key, requested)?;
        CLUSTER_IP_KEYSPACE.put(&ip_key, &format!("{namespace}/{name}"))?;
        Ok(requested.to_string())
    }

    fn allocate_next(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        let mut current = match CLUSTER_IP_KEYSPACE.get(NEXT_IP_KEY) {
            Ok(value) => value.parse::<u32>().unwrap_or(self.range.start),
            Err(_) => self.range.start,
        };

        let mut attempts = 0usize;
        let max_attempts = (self.range.end - self.range.start + 1) as usize;
        loop {
            if attempts >= max_attempts {
                return Err(new_error(
                    "Exhausted ClusterIP allocation range for services",
                ));
            }
            if !self.range.contains(current) {
                current = self.range.start;
            }
            let candidate = u32_to_ipv4(current).to_string();
            let ip_key = Self::ip_key(&candidate);
            let available = match CLUSTER_IP_KEYSPACE.get(&ip_key) {
                Ok(_) => false,
                Err(err) => {
                    err.to_string().contains("No such file or directory")
                        || err.to_string().contains("not found")
                }
            };
            if available {
                let service_key = Self::service_key(namespace, name);
                CLUSTER_IP_KEYSPACE.put(&service_key, &candidate)?;
                CLUSTER_IP_KEYSPACE.put(&ip_key, &format!("{namespace}/{name}"))?;
                let next = current.saturating_add(1);
                CLUSTER_IP_KEYSPACE.put(NEXT_IP_KEY, &next.to_string())?;
                return Ok(candidate);
            }
            current = current.saturating_add(1);
            attempts += 1;
        }
    }

    fn release(&self, namespace: &str, name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let _guard = self.mutex.lock().expect("allocator mutex poisoned");
        let service_key = Self::service_key(namespace, name);
        let ip = match CLUSTER_IP_KEYSPACE.get(&service_key) {
            Ok(value) => value,
            Err(_) => return Ok(()),
        };
        CLUSTER_IP_KEYSPACE.delete(&service_key)?;
        let ip_key = Self::ip_key(ip.trim());
        let _ = CLUSTER_IP_KEYSPACE.delete(&ip_key);
        Ok(())
    }
}

fn parse_ipv4(value: &str) -> Result<u32, Box<dyn Error + Send + Sync>> {
    let addr: Ipv4Addr = value
        .parse()
        .map_err(|e| with_context(e, format!("Failed to parse IPv4 address '{value}'")))?;
    Ok(ipv4_to_u32(&addr))
}

pub struct ServiceRegistry {
    services: RwLock<HashMap<String, Service>>,
    resource_counter: AtomicU64,
    allocator: ClusterIpAllocator,
}

impl ServiceRegistry {
    pub fn shared() -> Arc<Self> {
        static REGISTRY: OnceLock<Arc<ServiceRegistry>> = OnceLock::new();
        REGISTRY
            .get_or_init(|| {
                let allocator = ClusterIpAllocator::new(ClusterIpRange::default());
                let (initial, counter) = load_initial_services();
                Arc::new(ServiceRegistry {
                    services: RwLock::new(initial),
                    resource_counter: AtomicU64::new(counter.max(1)),
                    allocator,
                })
            })
            .clone()
    }

    pub fn list(&self, namespace: Option<&str>) -> Vec<Service> {
        let guard = self.services.read().expect("service registry poisoned");
        guard
            .values()
            .filter(|service| match namespace {
                Some(ns) => service.metadata.namespace.as_deref().unwrap_or("default") == ns,
                None => true,
            })
            .cloned()
            .collect()
    }

    pub fn get(&self, namespace: &str, name: &str) -> Option<Service> {
        let key = service_key(namespace, name);
        let guard = self.services.read().expect("service registry poisoned");
        guard.get(&key).cloned()
    }

    pub fn create(
        &self,
        namespace: &str,
        mut service: Service,
    ) -> Result<Service, Box<dyn Error + Send + Sync>> {
        if service.api_version.is_empty() {
            service.api_version = "nanocloud.io/v1".to_string();
        }
        if service.kind.is_empty() {
            service.kind = "Service".to_string();
        }
        let identity = ensure_metadata(namespace, &mut service)?;
        let key = service_key(&identity.namespace, &identity.name);

        let cluster_ip = self.allocator.allocate(
            &identity.namespace,
            &identity.name,
            service
                .spec
                .cluster_ip
                .as_deref()
                .filter(|ip| !ip.is_empty()),
        )?;

        service.spec.cluster_ip = Some(cluster_ip.clone());
        service.status = Some(ServiceStatus {
            cluster_ip: Some(cluster_ip),
        });
        service.metadata.resource_version = Some(self.next_resource_version());

        {
            let mut guard = self.services.write().expect("service registry poisoned");
            if guard.contains_key(&key) {
                return Err(new_error(format!(
                    "Service '{}/{}' already exists",
                    identity.namespace, identity.name
                )));
            }
            guard.insert(key.clone(), service.clone());
        }

        store::save_service(Some(&identity.namespace), &identity.name, &service)?;
        endpoints::reconcile_service(&service)?;
        Ok(service)
    }

    pub fn delete(&self, namespace: &str, name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let normalized = if namespace.is_empty() {
            "default"
        } else {
            namespace
        };
        let key = service_key(normalized, name);
        let removed = {
            let mut guard = self.services.write().expect("service registry poisoned");
            guard.remove(&key)
        };
        if let Some(service) = removed {
            self.allocator.release(normalized, name)?;
            store::delete_service(Some(normalized), name)?;
            endpoints::remove_service(&service)?;
        }
        Ok(())
    }

    fn next_resource_version(&self) -> String {
        self.resource_counter
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1)
            .to_string()
    }
}

fn service_key(namespace: &str, name: &str) -> String {
    let normalized = if namespace.is_empty() {
        "default"
    } else {
        namespace
    };
    format!("{}/{}", normalized, name)
}

struct ServiceIdentity {
    namespace: String,
    name: String,
}

fn ensure_metadata(
    namespace: &str,
    service: &mut Service,
) -> Result<ServiceIdentity, Box<dyn Error + Send + Sync>> {
    let namespace_value = if let Some(ns) = service.metadata.namespace.as_deref() {
        if ns.trim().is_empty() {
            "default".to_string()
        } else {
            ns.trim().to_string()
        }
    } else if namespace.trim().is_empty() {
        "default".to_string()
    } else {
        namespace.trim().to_string()
    };
    service.metadata.namespace = Some(namespace_value.clone());

    let name_value = match service.metadata.name.as_deref() {
        Some(name) if !name.trim().is_empty() => name.trim().to_string(),
        _ => service
            .metadata
            .name
            .get_or_insert_with(|| {
                service
                    .spec
                    .selector
                    .get("app")
                    .cloned()
                    .unwrap_or_else(|| "service".to_string())
            })
            .clone(),
    };
    if name_value.trim().is_empty() {
        return Err(new_error("Service metadata.name is required"));
    }
    service.metadata.name = Some(name_value.clone());

    Ok(ServiceIdentity {
        namespace: namespace_value,
        name: name_value,
    })
}

fn load_initial_services() -> (HashMap<String, Service>, u64) {
    match store::list_services(None) {
        Ok(services) => {
            let mut map = HashMap::new();
            let mut counter = 1u64;
            for service in services {
                if let Some(namespace) = service.metadata.namespace.as_deref() {
                    if let Some(name) = service.metadata.name.as_deref() {
                        let key = service_key(namespace, name);
                        if let Some(rv) = service
                            .metadata
                            .resource_version
                            .as_deref()
                            .and_then(|value| value.parse::<u64>().ok())
                        {
                            counter = counter.max(rv);
                        }
                        map.insert(key, service);
                    }
                }
            }
            (map, counter)
        }
        Err(err) => {
            let message = err.to_string();
            log::warn!(
                target: "service-registry",
                "Failed to load services from store: {}",
                message
            );
            (HashMap::new(), 1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::service::ServicePort;
    use crate::nanocloud::test_support::keyspace_lock;
    use std::env;
    use std::sync::MutexGuard;
    use tempfile::tempdir;

    fn setup_env() -> (tempfile::TempDir, EnvCleanup) {
        let lock = keyspace_lock().lock();
        let dir = tempdir().expect("tempdir");
        let keyspace_dir = dir.path().join("keyspace");
        std::fs::create_dir_all(&keyspace_dir).expect("keyspace dir");
        let previous = env::var("NANOCLOUD_KEYSPACE").ok();
        env::set_var("NANOCLOUD_KEYSPACE", &keyspace_dir);
        let iptables_log = dir.path().join("iptables.log");
        let previous_iptables = env::var("NANOCLOUD_IPTABLES_RECORD").ok();
        env::set_var("NANOCLOUD_IPTABLES_RECORD", &iptables_log);
        (
            dir,
            EnvCleanup {
                _lock: lock,
                previous_keyspace: previous,
                previous_iptables,
            },
        )
    }

    struct EnvCleanup {
        _lock: MutexGuard<'static, ()>,
        previous_keyspace: Option<String>,
        previous_iptables: Option<String>,
    }

    impl Drop for EnvCleanup {
        fn drop(&mut self) {
            match self.previous_keyspace.as_ref() {
                Some(value) => env::set_var("NANOCLOUD_KEYSPACE", value),
                None => env::remove_var("NANOCLOUD_KEYSPACE"),
            }
            match self.previous_iptables.as_ref() {
                Some(value) => env::set_var("NANOCLOUD_IPTABLES_RECORD", value),
                None => env::remove_var("NANOCLOUD_IPTABLES_RECORD"),
            }
        }
    }

    #[test]
    fn allocator_rejects_duplicate_ip() {
        let (_dir, _cleanup) = setup_env();
        let range = ClusterIpRange::default();
        let allocator = ClusterIpAllocator::new(range);
        allocator
            .allocate("default", "svc-a", Some("10.203.0.20"))
            .expect("first allocation");
        let err = allocator
            .allocate("default", "svc-b", Some("10.203.0.20"))
            .expect_err("duplicate ip should fail");
        assert!(
            err.to_string().contains("already allocated"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn allocator_wraps_range() {
        let (_dir, _cleanup) = setup_env();
        let range = ClusterIpRange {
            start: ipv4_to_u32(&Ipv4Addr::new(10, 203, 0, 1)),
            end: ipv4_to_u32(&Ipv4Addr::new(10, 203, 0, 3)),
        };
        let allocator = ClusterIpAllocator::new(range);
        allocator.allocate("default", "svc-a", None).unwrap();
        allocator.allocate("default", "svc-b", None).unwrap();
        allocator.allocate("default", "svc-c", None).unwrap();
        let err = allocator.allocate("default", "svc-d", None).unwrap_err();
        assert!(
            err.to_string()
                .contains("Exhausted ClusterIP allocation range"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn registry_assigns_cluster_ip() {
        let (_dir, _cleanup) = setup_env();
        let registry = ServiceRegistry::shared();
        let mut service = Service::default();
        service.spec.ports.push(ServicePort {
            name: Some("http".to_string()),
            port: 80,
            target_port: Some(8080),
            protocol: Some("TCP".to_string()),
        });
        service
            .spec
            .selector
            .insert("app".to_string(), "demo".to_string());

        let created = registry
            .create("default", service)
            .expect("service created");
        assert!(
            created
                .spec
                .cluster_ip
                .as_deref()
                .unwrap()
                .starts_with("10.203."),
            "cluster ip not assigned"
        );
    }
}
