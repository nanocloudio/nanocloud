#![allow(dead_code)]

use serde::{Deserialize, Serialize};

use super::pod::{ListMeta, ObjectMeta};
use super::store::{self, normalize_namespace, paginate_entries, ListCursor, PaginatedResult};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EndpointAddress {
    pub ip: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EndpointPort {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub port: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
}

impl EndpointPort {
    pub fn new(name: Option<String>, port: u16, protocol: Option<String>) -> Self {
        EndpointPort {
            name,
            port,
            protocol,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EndpointSubset {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub addresses: Vec<EndpointAddress>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<EndpointPort>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
/// Endpoints resource generated from Service selectors.
///
/// When persisted, `metadata.name`/`namespace` should match the owning Service.
/// Empty subsets represent a Service with no ready backends.
pub struct Endpoints {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subsets: Vec<EndpointSubset>,
}

impl Default for Endpoints {
    fn default() -> Self {
        Endpoints {
            api_version: "nanocloud.io/v1".to_string(),
            kind: "Endpoints".to_string(),
            metadata: ObjectMeta::default(),
            subsets: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EndpointsList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ListMeta,
    pub items: Vec<Endpoints>,
}

impl Default for EndpointsList {
    fn default() -> Self {
        EndpointsList {
            api_version: "nanocloud.io/v1".to_string(),
            kind: "EndpointsList".to_string(),
            metadata: ListMeta::default(),
            items: Vec::new(),
        }
    }
}

const WATCH_BUFFER_SIZE: usize = 32;

#[derive(Debug)]
pub enum EndpointsError {
    NotFound(String),
    Persistence(Box<dyn Error + Send + Sync>),
}

impl Display for EndpointsError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            EndpointsError::NotFound(msg) => f.write_str(msg),
            EndpointsError::Persistence(err) => write!(f, "{}", err),
        }
    }
}

impl Error for EndpointsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            EndpointsError::Persistence(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

impl EndpointsError {
    pub fn persistence(err: Box<dyn Error + Send + Sync>) -> Self {
        EndpointsError::Persistence(err)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct EndpointsWatchEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub object: Endpoints,
}

#[derive(Clone, Hash, Eq, PartialEq)]
enum WatchScope {
    Cluster,
    Namespace(String),
    Endpoints(String),
}

pub struct EndpointsRegistry {
    endpoints: RwLock<HashMap<String, Endpoints>>,
    watchers: RwLock<HashMap<WatchScope, broadcast::Sender<EndpointsWatchEvent>>>,
    resource_counter: AtomicU64,
}

impl EndpointsRegistry {
    pub fn shared() -> Arc<Self> {
        static REGISTRY: OnceLock<Arc<EndpointsRegistry>> = OnceLock::new();
        REGISTRY
            .get_or_init(|| {
                let (entries, counter) = load_initial_endpoints();
                Arc::new(EndpointsRegistry {
                    endpoints: RwLock::new(entries),
                    watchers: RwLock::new(HashMap::new()),
                    resource_counter: AtomicU64::new(counter.max(1)),
                })
            })
            .clone()
    }

    pub fn current_resource_version(&self) -> String {
        let current = self.resource_counter.load(Ordering::SeqCst);
        current.saturating_sub(1).to_string()
    }

    pub fn list_paginated(
        &self,
        namespace: Option<&str>,
        resource_version: Option<u64>,
        limit: Option<u32>,
        cursor: Option<&ListCursor>,
    ) -> Result<PaginatedResult<Endpoints>, EndpointsError> {
        let entries = self.collect_entries(namespace, resource_version);
        paginate_entries(entries, cursor, limit)
            .map_err(|err| EndpointsError::Persistence(Box::new(err)))
    }

    pub fn list_since(
        &self,
        namespace: Option<&str>,
        resource_version: Option<u64>,
    ) -> Vec<Endpoints> {
        self.collect_entries(namespace, resource_version)
            .into_iter()
            .map(|(_, ep, _)| ep)
            .collect()
    }

    pub fn get(&self, namespace: &str, name: &str) -> Option<Endpoints> {
        let key = endpoints_key(namespace, name);
        let guard = self.endpoints.read().expect("endpoints registry poisoned");
        guard.get(&key).cloned()
    }

    pub fn upsert(&self, mut endpoints: Endpoints) -> Result<Endpoints, EndpointsError> {
        let namespace = endpoints
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let name = endpoints
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| "endpoints".to_string());
        endpoints
            .metadata
            .ensure_common_fields(Some(&namespace), Some(&name));
        endpoints.metadata.resource_version = Some(self.next_resource_version());

        let key = endpoints_key(&namespace, &name);
        let (event_type, previous) = {
            let mut guard = self.endpoints.write().expect("endpoints registry poisoned");
            let existing = guard.get(&key).cloned();
            let event_type = if existing.is_some() {
                "MODIFIED".to_string()
            } else {
                "ADDED".to_string()
            };
            guard.insert(key.clone(), endpoints.clone());
            (event_type, existing)
        };

        let persist = store::save_endpoints(Some(&namespace), &name, &endpoints);
        if let Err(err) = persist {
            let mut guard = self.endpoints.write().expect("endpoints registry poisoned");
            if let Some(previous) = previous {
                guard.insert(key, previous);
            } else {
                guard.remove(&key);
            }
            return Err(EndpointsError::persistence(err));
        }

        self.notify_watchers(
            &namespace,
            &name,
            EndpointsWatchEvent {
                event_type,
                object: endpoints.clone(),
            },
        );
        Ok(endpoints)
    }

    pub fn remove(&self, namespace: &str, name: &str) -> Result<Endpoints, EndpointsError> {
        let key = endpoints_key(namespace, name);
        let removed = {
            let mut guard = self.endpoints.write().expect("endpoints registry poisoned");
            guard.remove(&key)
        };
        let Some(mut endpoints) = removed else {
            return Err(EndpointsError::NotFound(format!(
                "Endpoints '{}/{}' not found",
                normalize_namespace(Some(namespace)),
                name
            )));
        };
        endpoints.metadata.resource_version = Some(self.next_resource_version());
        store::delete_endpoints(Some(namespace), name).map_err(EndpointsError::persistence)?;
        self.notify_watchers(
            namespace,
            name,
            EndpointsWatchEvent {
                event_type: "DELETED".to_string(),
                object: endpoints.clone(),
            },
        );
        Ok(endpoints)
    }

    pub fn watch_cluster(&self) -> broadcast::Receiver<EndpointsWatchEvent> {
        self.ensure_watcher(WatchScope::Cluster)
    }

    pub fn watch_namespace(&self, namespace: &str) -> broadcast::Receiver<EndpointsWatchEvent> {
        let ns = normalize_namespace(Some(namespace));
        self.ensure_watcher(WatchScope::Namespace(ns))
    }

    pub fn watch_endpoints(
        &self,
        namespace: &str,
        name: &str,
    ) -> broadcast::Receiver<EndpointsWatchEvent> {
        let ns = normalize_namespace(Some(namespace));
        self.ensure_watcher(WatchScope::Endpoints(endpoints_key(&ns, name)))
    }

    fn ensure_watcher(&self, scope: WatchScope) -> broadcast::Receiver<EndpointsWatchEvent> {
        let mut guard = self
            .watchers
            .write()
            .expect("endpoints watcher map poisoned");
        guard
            .entry(scope.clone())
            .or_insert_with(|| broadcast::channel(WATCH_BUFFER_SIZE).0)
            .subscribe()
    }

    fn notify_watchers(&self, namespace: &str, name: &str, event: EndpointsWatchEvent) {
        let guard = self
            .watchers
            .read()
            .expect("endpoints watcher map poisoned");
        let ns = normalize_namespace(Some(namespace));
        let scope_cluster = WatchScope::Cluster;
        let scope_namespace = WatchScope::Namespace(ns.clone());
        let scope_endpoints = WatchScope::Endpoints(endpoints_key(&ns, name));

        for scope in [scope_cluster, scope_namespace, scope_endpoints] {
            if let Some(sender) = guard.get(&scope) {
                let _ = sender.send(event.clone());
            }
        }
    }

    pub fn collect_entries(
        &self,
        namespace: Option<&str>,
        resource_version: Option<u64>,
    ) -> Vec<(String, Endpoints, Option<String>)> {
        let namespace_filter = namespace.map(|ns| normalize_namespace(Some(ns)));
        let guard = self.endpoints.read().expect("endpoints registry poisoned");
        guard
            .iter()
            .filter_map(|(key, endpoints)| {
                if let Some(target_ns) = namespace_filter.as_ref() {
                    if endpoints
                        .metadata
                        .namespace
                        .as_deref()
                        .map(|ns| normalize_namespace(Some(ns)))
                        .as_deref()
                        != Some(target_ns.as_str())
                    {
                        return None;
                    }
                }
                if let Some(threshold) = resource_version {
                    let current = endpoints
                        .metadata
                        .resource_version
                        .as_deref()
                        .and_then(|rv| rv.parse::<u64>().ok());
                    if current.map(|rv| rv <= threshold).unwrap_or(false) {
                        return None;
                    }
                }
                Some((
                    key.clone(),
                    endpoints.clone(),
                    endpoints.metadata.resource_version.clone(),
                ))
            })
            .collect()
    }

    fn next_resource_version(&self) -> String {
        self.resource_counter
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1)
            .to_string()
    }
}

fn endpoints_key(namespace: &str, name: &str) -> String {
    let normalized = normalize_namespace(Some(namespace));
    format!("{}/{}", normalized, name)
}

fn load_initial_endpoints() -> (HashMap<String, Endpoints>, u64) {
    match store::list_endpoints(None) {
        Ok(endpoints) => {
            let mut map = HashMap::new();
            let mut counter = 1u64;
            for entry in endpoints {
                if let (Some(ns), Some(name)) = (
                    entry.metadata.namespace.as_deref(),
                    entry.metadata.name.as_deref(),
                ) {
                    let key = endpoints_key(ns, name);
                    if let Some(rv) = entry
                        .metadata
                        .resource_version
                        .as_deref()
                        .and_then(|value| value.parse::<u64>().ok())
                    {
                        counter = counter.max(rv);
                    }
                    map.insert(key, entry);
                }
            }
            (map, counter)
        }
        Err(err) => {
            log::warn!(
                target: "endpoints-registry",
                "Failed to load endpoints from store: {}",
                err
            );
            (HashMap::new(), 1)
        }
    }
}
