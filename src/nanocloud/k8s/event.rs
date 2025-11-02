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

use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, RwLock};

use super::pod::{ListMeta, ObjectMeta};
use super::store::normalize_namespace;

const DEFAULT_EVENT_RETENTION: usize = 1024;
const WATCH_BUFFER_SIZE: usize = 64;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ObjectReference {
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    #[serde(rename = "resourceVersion", skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
    #[serde(rename = "fieldPath", skip_serializing_if = "Option::is_none")]
    pub field_path: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EventSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EventSeries {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<i32>,
    #[serde(rename = "lastObservedTime", skip_serializing_if = "Option::is_none")]
    pub last_observed_time: Option<String>,
}

/// Minimal representation of Kubernetes core/v1 Event.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Event {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    #[serde(rename = "involvedObject")]
    pub involved_object: ObjectReference,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    #[serde(rename = "firstTimestamp", skip_serializing_if = "Option::is_none")]
    pub first_timestamp: Option<String>,
    #[serde(rename = "lastTimestamp", skip_serializing_if = "Option::is_none")]
    pub last_timestamp: Option<String>,
    #[serde(rename = "eventTime", skip_serializing_if = "Option::is_none")]
    pub event_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<i32>,
    #[serde(rename = "reportingComponent", skip_serializing_if = "Option::is_none")]
    pub reporting_component: Option<String>,
    #[serde(rename = "reportingInstance", skip_serializing_if = "Option::is_none")]
    pub reporting_instance: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related: Option<ObjectReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub series: Option<EventSeries>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<EventSource>,
    #[serde(rename = "deprecatedSource", skip_serializing_if = "Option::is_none")]
    pub deprecated_source: Option<EventSource>,
    #[serde(
        rename = "deprecatedFirstTimestamp",
        skip_serializing_if = "Option::is_none"
    )]
    pub deprecated_first_timestamp: Option<String>,
    #[serde(
        rename = "deprecatedLastTimestamp",
        skip_serializing_if = "Option::is_none"
    )]
    pub deprecated_last_timestamp: Option<String>,
    #[serde(rename = "deprecatedCount", skip_serializing_if = "Option::is_none")]
    pub deprecated_count: Option<i32>,
}

#[derive(Clone, Debug, Serialize)]
pub struct EventWatchEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub object: Event,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EventList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ListMeta,
    pub items: Vec<Event>,
}

impl EventList {
    pub fn new(items: Vec<Event>, metadata: ListMeta) -> Self {
        Self {
            api_version: "v1".to_string(),
            kind: "EventList".to_string(),
            metadata,
            items,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum EventScope {
    Cluster,
    Namespace(String),
}

struct EventStore {
    records: VecDeque<Event>,
    capacity: usize,
}

impl EventStore {
    fn new(capacity: usize) -> Self {
        Self {
            records: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn push(&mut self, event: Event) {
        if self.records.len() == self.capacity {
            self.records.pop_front();
        }
        self.records.push_back(event);
    }

    fn iter(&self) -> impl Iterator<Item = &Event> {
        self.records.iter()
    }
}

pub struct EventRegistry {
    store: RwLock<EventStore>,
    watchers: RwLock<HashMap<EventScope, broadcast::Sender<EventWatchEvent>>>,
    resource_counter: AtomicU64,
}

impl EventRegistry {
    pub fn shared() -> Arc<Self> {
        static INSTANCE: OnceLock<Arc<EventRegistry>> = OnceLock::new();
        INSTANCE
            .get_or_init(|| {
                Arc::new(Self {
                    store: RwLock::new(EventStore::new(DEFAULT_EVENT_RETENTION)),
                    watchers: RwLock::new(HashMap::new()),
                    resource_counter: AtomicU64::new(1),
                })
            })
            .clone()
    }

    pub fn current_resource_version(&self) -> String {
        let value = self.resource_counter.load(Ordering::SeqCst);
        value.saturating_sub(1).to_string()
    }

    pub async fn record(&self, mut event: Event) -> Event {
        let resource_version = self.resource_counter.fetch_add(1, Ordering::SeqCst);
        event.metadata.resource_version = Some(resource_version.to_string());
        event.metadata.name = Some(format!("event-{}", resource_version));

        if let Some(namespace) = event.metadata.namespace.as_ref() {
            let normalized = normalize_namespace(Some(namespace));
            event.metadata.namespace = Some(normalized);
        } else if let Some(namespace) = event.involved_object.namespace.as_ref() {
            event
                .metadata
                .namespace
                .get_or_insert_with(|| normalize_namespace(Some(namespace)));
        } else {
            event
                .metadata
                .namespace
                .get_or_insert_with(|| "default".to_string());
        }

        if event.api_version.is_empty() {
            event.api_version = "v1".to_string();
        }

        if event.kind.is_empty() {
            event.kind = "Event".to_string();
        }

        if event.count.is_none() {
            event.count = Some(1);
        }

        let namespace = event
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        let cloned = event.clone();
        {
            let mut store = self.store.write().await;
            store.push(event);
        }

        self.broadcast(&namespace, cloned.clone()).await;
        cloned
    }

    pub async fn list_since(
        &self,
        namespace: Option<&str>,
        resource_version: Option<u64>,
    ) -> Vec<Event> {
        let namespace = namespace.map(|ns| normalize_namespace(Some(ns)));
        let store = self.store.read().await;
        let mut items: Vec<Event> = store
            .iter()
            .filter(|event| {
                let event_namespace = event.metadata.namespace.as_deref().unwrap_or("default");
                if namespace
                    .as_ref()
                    .map(|expected| expected.as_str() != event_namespace)
                    .unwrap_or(false)
                {
                    return false;
                }

                if let Some(threshold) = resource_version {
                    let current = event
                        .metadata
                        .resource_version
                        .as_deref()
                        .and_then(|value| value.parse::<u64>().ok())
                        .unwrap_or(0);
                    current > threshold
                } else {
                    true
                }
            })
            .cloned()
            .collect();

        items.sort_by_key(|event| {
            event
                .metadata
                .resource_version
                .as_deref()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0)
        });
        items
    }

    pub async fn collect_entries(
        &self,
        namespace: Option<&str>,
        resource_version: Option<u64>,
    ) -> Vec<(String, Event, Option<String>)> {
        let namespace = namespace.map(|ns| normalize_namespace(Some(ns)));
        let store = self.store.read().await;
        let mut entries = Vec::new();
        for event in store.iter() {
            let event_namespace = event
                .metadata
                .namespace
                .as_deref()
                .unwrap_or("default")
                .to_string();
            if namespace
                .as_ref()
                .map(|expected| expected.as_str() != event_namespace.as_str())
                .unwrap_or(false)
            {
                continue;
            }

            if resource_version.is_some_and(|threshold| {
                event
                    .metadata
                    .resource_version
                    .as_deref()
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(|current| current <= threshold)
                    .unwrap_or(false)
            }) {
                continue;
            }

            let name = event
                .metadata
                .name
                .as_deref()
                .unwrap_or("unnamed")
                .to_string();
            let key = format!("{}/{}", event_namespace, name);
            entries.push((key, event.clone(), event.metadata.resource_version.clone()));
        }
        entries
    }

    pub async fn watch_cluster(&self) -> broadcast::Receiver<EventWatchEvent> {
        self.ensure_watch(EventScope::Cluster).await
    }

    pub async fn watch_namespace(&self, namespace: &str) -> broadcast::Receiver<EventWatchEvent> {
        let scope = EventScope::Namespace(normalize_namespace(Some(namespace)));
        self.ensure_watch(scope).await
    }

    async fn ensure_watch(&self, scope: EventScope) -> broadcast::Receiver<EventWatchEvent> {
        let mut watchers = self.watchers.write().await;
        watchers
            .entry(scope)
            .or_insert_with(|| broadcast::channel(WATCH_BUFFER_SIZE).0)
            .subscribe()
    }

    async fn broadcast(&self, namespace: &str, event: Event) {
        let mut targets: Vec<broadcast::Sender<EventWatchEvent>> = Vec::new();
        {
            let watchers = self.watchers.read().await;
            if let Some(sender) = watchers.get(&EventScope::Cluster) {
                targets.push(sender.clone());
            }
            if let Some(sender) = watchers.get(&EventScope::Namespace(namespace.to_string())) {
                targets.push(sender.clone());
            }
        }

        if targets.is_empty() {
            return;
        }

        let payload = EventWatchEvent {
            event_type: "ADDED".to_string(),
            object: event,
        };

        for sender in targets {
            let _ = sender.send(payload.clone());
        }
    }
}

#[cfg(test)]
impl EventRegistry {
    pub async fn clear(&self) {
        {
            let mut store = self.store.write().await;
            store.records.clear();
        }
        self.resource_counter.store(1, Ordering::SeqCst);
        let mut watchers = self.watchers.write().await;
        watchers.clear();
    }
}
