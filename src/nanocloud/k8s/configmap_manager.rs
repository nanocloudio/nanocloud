/*
 * Copyright (C) 2024 The Nanocloud Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use super::configmap::ConfigMap;
use super::store::{
    delete_config_map, list_config_maps, normalize_namespace, paginate_entries, save_config_map,
    ListCursor, PaginatedResult,
};

use serde::Serialize;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::{broadcast, RwLock};

const WATCH_BUFFER_SIZE: usize = 32;

#[derive(Debug)]
pub enum ConfigMapError {
    AlreadyExists(String),
    NotFound(String),
    Invalid(String),
    Conflict(String),
    Persistence(Box<dyn Error + Send + Sync>),
}

impl Display for ConfigMapError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigMapError::AlreadyExists(msg)
            | ConfigMapError::NotFound(msg)
            | ConfigMapError::Invalid(msg)
            | ConfigMapError::Conflict(msg) => f.write_str(msg),
            ConfigMapError::Persistence(err) => write!(f, "{}", err),
        }
    }
}

impl Error for ConfigMapError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ConfigMapError::Persistence(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

impl ConfigMapError {
    pub fn persistence_box(err: Box<dyn Error + Send + Sync>) -> Self {
        Self::Persistence(err)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ConfigMapWatchEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub object: ConfigMap,
}

#[derive(Clone, Hash, Eq, PartialEq)]
enum WatchScope {
    Cluster,
    Namespace(String),
    ConfigMap(String),
}

fn configmap_key(namespace: &str, name: &str) -> String {
    format!("{}/{}", normalize_namespace(Some(namespace)), name)
}

fn ensure_namespace(namespace: &str, config_map: &mut ConfigMap) -> String {
    let ns = config_map
        .metadata
        .namespace
        .clone()
        .filter(|ns| !ns.is_empty())
        .unwrap_or_else(|| namespace.to_string());
    config_map.metadata.namespace = Some(ns.clone());
    ns
}

fn ensure_name(name: &str, config_map: &mut ConfigMap) -> Result<String, ConfigMapError> {
    let current = config_map
        .metadata
        .name
        .clone()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| name.to_string());
    if current != name {
        return Err(ConfigMapError::Invalid(format!(
            "metadata.name '{}' does not match request name '{}'",
            current, name
        )));
    }
    config_map.metadata.name = Some(current.clone());
    Ok(current)
}

fn normalize_key(
    namespace: &str,
    name: &str,
    metadata: &mut ConfigMap,
) -> Result<String, ConfigMapError> {
    let ns = ensure_namespace(namespace, metadata);
    let name = ensure_name(name, metadata)?;
    Ok(configmap_key(&ns, &name))
}

fn normalize_key_new(namespace: &str, metadata: &mut ConfigMap) -> Result<String, ConfigMapError> {
    let ns = ensure_namespace(namespace, metadata);
    let name = metadata
        .metadata
        .name
        .clone()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ConfigMapError::Invalid("metadata.name is required".to_string()))?;
    Ok(configmap_key(&ns, &name))
}

pub struct ConfigMapRegistry {
    maps: RwLock<HashMap<String, ConfigMap>>,
    watchers: RwLock<HashMap<WatchScope, broadcast::Sender<ConfigMapWatchEvent>>>,
    resource_counter: AtomicU64,
}

static REGISTRY: OnceLock<Arc<ConfigMapRegistry>> = OnceLock::new();

impl ConfigMapRegistry {
    pub fn shared() -> Arc<Self> {
        REGISTRY
            .get_or_init(|| {
                let (maps, counter) = load_initial_configmaps();
                Arc::new(ConfigMapRegistry {
                    maps: RwLock::new(maps),
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

    pub async fn list_since(
        &self,
        namespace: Option<&str>,
        resource_version: Option<u64>,
    ) -> Vec<ConfigMap> {
        self.list_paginated(namespace, resource_version, None, None)
            .await
            .map(|page| page.items)
            .unwrap_or_default()
    }

    pub async fn list_paginated(
        &self,
        namespace: Option<&str>,
        resource_version: Option<u64>,
        limit: Option<u32>,
        cursor: Option<&ListCursor>,
    ) -> Result<PaginatedResult<ConfigMap>, ConfigMapError> {
        let entries = self.collect_entries(namespace, resource_version).await;

        paginate_entries(entries, cursor, limit)
            .map_err(|err| ConfigMapError::Invalid(err.to_string()))
    }

    pub async fn collect_entries(
        &self,
        namespace: Option<&str>,
        resource_version: Option<u64>,
    ) -> Vec<(String, ConfigMap, Option<String>)> {
        let maps = self.maps.read().await;
        let namespace_filter = namespace.map(|ns| normalize_namespace(Some(ns)));
        let mut entries = Vec::new();
        for (key, config_map) in maps.iter() {
            if let Some(filter) = &namespace_filter {
                if !key.starts_with(&format!("{}/", filter)) {
                    continue;
                }
            }

            if resource_version.is_some_and(|threshold| {
                config_map
                    .metadata
                    .resource_version
                    .as_deref()
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(|current| current <= threshold)
                    .unwrap_or(false)
            }) {
                continue;
            }

            entries.push((
                key.clone(),
                config_map.clone(),
                config_map.metadata.resource_version.clone(),
            ));
        }
        entries
    }

    pub async fn get(&self, namespace: &str, name: &str) -> Option<ConfigMap> {
        let key = configmap_key(namespace, name);
        self.maps.read().await.get(&key).cloned()
    }

    pub async fn create(
        &self,
        namespace: &str,
        mut payload: ConfigMap,
    ) -> Result<ConfigMap, ConfigMapError> {
        if payload.metadata.resource_version.is_some() {
            return Err(ConfigMapError::Invalid(
                "resourceVersion must not be set on create".to_string(),
            ));
        }
        let key = normalize_key_new(namespace, &mut payload)?;

        {
            let maps = self.maps.read().await;
            if maps.contains_key(&key) {
                return Err(ConfigMapError::AlreadyExists(format!(
                    "ConfigMap '{}' already exists",
                    payload.metadata.name.clone().unwrap()
                )));
            }
        }

        let resource_version = self.next_resource_version();
        payload.metadata.resource_version = Some(resource_version.clone());

        let stored = payload.clone();
        save_config_map(
            payload.metadata.namespace.as_deref(),
            payload.metadata.name.as_deref().unwrap(),
            &stored,
        )
        .map_err(ConfigMapError::persistence_box)?;

        {
            let mut maps = self.maps.write().await;
            maps.insert(key.clone(), stored.clone());
        }

        self.broadcast(&stored, "ADDED").await;
        Ok(stored)
    }

    pub async fn replace(
        &self,
        namespace: &str,
        name: &str,
        mut payload: ConfigMap,
    ) -> Result<ConfigMap, ConfigMapError> {
        let key = normalize_key(namespace, name, &mut payload)?;

        let existing = {
            let maps = self.maps.read().await;
            maps.get(&key).cloned()
        };

        let Some(existing) = existing else {
            return Err(ConfigMapError::NotFound(format!(
                "ConfigMap '{}' not found",
                name
            )));
        };

        if existing.immutable.unwrap_or(false)
            && (existing.data != payload.data || existing.binary_data != payload.binary_data)
        {
            return Err(ConfigMapError::Conflict(format!(
                "ConfigMap '{}' is immutable",
                name
            )));
        }

        if let Some(resource_version) = payload.metadata.resource_version.as_deref() {
            if existing.metadata.resource_version.as_deref().unwrap_or("") != resource_version {
                return Err(ConfigMapError::Conflict(
                    "resourceVersion does not match current ConfigMap".to_string(),
                ));
            }
        }

        let resource_version = self.next_resource_version();
        payload.metadata.resource_version = Some(resource_version.clone());

        let stored = payload.clone();
        save_config_map(
            stored.metadata.namespace.as_deref(),
            stored.metadata.name.as_deref().unwrap(),
            &stored,
        )
        .map_err(ConfigMapError::persistence_box)?;

        {
            let mut maps = self.maps.write().await;
            maps.insert(key.clone(), stored.clone());
        }

        self.broadcast(&stored, "MODIFIED").await;
        Ok(stored)
    }

    pub async fn delete(&self, namespace: &str, name: &str) -> Result<ConfigMap, ConfigMapError> {
        let key = configmap_key(namespace, name);

        let removed = {
            let mut maps = self.maps.write().await;
            maps.remove(&key)
        };

        let Some(config_map) = removed else {
            return Err(ConfigMapError::NotFound(format!(
                "ConfigMap '{}' not found",
                name
            )));
        };

        delete_config_map(
            config_map.metadata.namespace.as_deref(),
            config_map.metadata.name.as_deref().unwrap(),
        )
        .map_err(ConfigMapError::persistence_box)?;

        self.broadcast(&config_map, "DELETED").await;
        Ok(config_map)
    }

    pub async fn watch_cluster(&self) -> broadcast::Receiver<ConfigMapWatchEvent> {
        self.ensure_watch(WatchScope::Cluster).await
    }

    pub async fn watch_namespace(
        &self,
        namespace: &str,
    ) -> broadcast::Receiver<ConfigMapWatchEvent> {
        let scope = WatchScope::Namespace(normalize_namespace(Some(namespace)));
        self.ensure_watch(scope).await
    }

    pub async fn watch_config_map(
        &self,
        namespace: &str,
        name: &str,
    ) -> broadcast::Receiver<ConfigMapWatchEvent> {
        let scope = WatchScope::ConfigMap(configmap_key(namespace, name));
        self.ensure_watch(scope).await
    }

    fn next_resource_version(&self) -> String {
        self.resource_counter
            .fetch_add(1, Ordering::SeqCst)
            .to_string()
    }

    async fn ensure_watch(&self, scope: WatchScope) -> broadcast::Receiver<ConfigMapWatchEvent> {
        let mut watchers = self.watchers.write().await;
        watchers
            .entry(scope.clone())
            .or_insert_with(|| broadcast::channel(WATCH_BUFFER_SIZE).0)
            .subscribe()
    }

    async fn broadcast(&self, config_map: &ConfigMap, event_type: &str) {
        let event = ConfigMapWatchEvent {
            event_type: event_type.to_string(),
            object: config_map.clone(),
        };

        let namespace = config_map
            .metadata
            .namespace
            .as_deref()
            .map(|ns| normalize_namespace(Some(ns)))
            .unwrap_or_else(|| "default".to_string());
        let key = configmap_key(&namespace, config_map.metadata.name.as_deref().unwrap());

        let watchers = self.watchers.read().await;
        let mut targets: Vec<broadcast::Sender<ConfigMapWatchEvent>> = Vec::new();

        if let Some(sender) = watchers.get(&WatchScope::Cluster) {
            targets.push(sender.clone());
        }
        if let Some(sender) = watchers.get(&WatchScope::Namespace(namespace.clone())) {
            targets.push(sender.clone());
        }
        if let Some(sender) = watchers.get(&WatchScope::ConfigMap(key)) {
            targets.push(sender.clone());
        }
        drop(watchers);

        for sender in targets {
            let _ = sender.send(event.clone());
        }
    }
}

fn load_initial_configmaps() -> (HashMap<String, ConfigMap>, u64) {
    let mut maps = HashMap::new();
    let mut counter: u64 = 1;

    match list_config_maps(None) {
        Ok(existing) => {
            for mut config_map in existing.into_iter() {
                let Some(name) = config_map.metadata.name.clone() else {
                    continue;
                };
                let namespace = config_map
                    .metadata
                    .namespace
                    .clone()
                    .filter(|ns| !ns.is_empty())
                    .unwrap_or_else(|| "default".to_string());

                let key = configmap_key(&namespace, &name);

                match config_map
                    .metadata
                    .resource_version
                    .as_deref()
                    .and_then(|rv| rv.parse::<u64>().ok())
                {
                    Some(value) => {
                        counter = counter.max(value.saturating_add(1));
                    }
                    None => {
                        let rv = counter.to_string();
                        config_map.metadata.resource_version = Some(rv.clone());
                        counter = counter.saturating_add(1);
                        if let Err(err) =
                            save_config_map(Some(namespace.as_str()), &name, &config_map)
                        {
                            eprintln!(
                                "Failed to persist ConfigMap '{}' during initialization: {}",
                                key, err
                            );
                        }
                    }
                }

                maps.insert(key, config_map);
            }
        }
        Err(err) => {
            eprintln!("Failed to load persisted ConfigMaps: {err}");
        }
    }

    (maps, counter)
}
