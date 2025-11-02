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

use crate::nanocloud::controller::reconcile::ReconcileContext;
use crate::nanocloud::k8s::pod::Pod;
use crate::nanocloud::k8s::store::normalize_namespace;
use serde::{Deserialize, Serialize};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock as StdRwLock};
use tokio::sync::broadcast;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;

const DEFAULT_QUEUE_CAPACITY: usize = 256;

/// Generic Kubernetes-style watch event.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WatchEvent<T> {
    #[serde(rename = "type")]
    pub event_type: String,
    pub object: T,
}

/// Controller manager runtime that offers informers, a work queue, and shared dependencies.
pub struct ControllerRuntime {
    dependencies: DependencyRegistry,
    work_queue: WorkQueue<ControllerWorkItem>,
    pods: PodInformer,
}

impl ControllerRuntime {
    pub fn shared() -> Arc<Self> {
        use std::sync::OnceLock;
        static INSTANCE: OnceLock<Arc<ControllerRuntime>> = OnceLock::new();
        INSTANCE.get_or_init(ControllerRuntime::new).clone()
    }

    /// Creates a standalone controller runtime with default queue sizing.
    pub fn new() -> Arc<Self> {
        Arc::new(ControllerRuntime::with_capacity(DEFAULT_QUEUE_CAPACITY))
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            dependencies: DependencyRegistry::new(),
            work_queue: WorkQueue::new(capacity),
            pods: PodInformer::new(),
        }
    }

    pub fn register_dependency<T>(&self, dependency: Arc<T>) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.dependencies.insert(dependency)
    }

    pub fn dependency<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.dependencies.get::<T>()
    }

    pub fn pods(&self) -> &PodInformer {
        &self.pods
    }

    pub fn context(&self) -> ReconcileContext<'_> {
        ReconcileContext::new(self)
    }

    pub fn work_queue(&self) -> WorkQueue<ControllerWorkItem> {
        self.work_queue.clone()
    }

    pub fn spawn_executor<H, Fut>(&self, handler: H) -> JoinHandle<()>
    where
        H: Fn(ControllerWorkItem) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let queue = self.work_queue.clone();
        let handler = Arc::new(handler);
        tokio::spawn(async move {
            while let Some(item) = queue.next().await {
                let fut = (handler.as_ref())(item);
                fut.await;
            }
        })
    }
}

#[derive(Default)]
struct DependencyRegistry {
    values: StdRwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl DependencyRegistry {
    fn new() -> Self {
        Self {
            values: StdRwLock::new(HashMap::new()),
        }
    }

    fn insert<T>(&self, dependency: Arc<T>) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        let mut guard = self.values.write().expect("dependency registry poisoned");
        let existing = guard.insert(
            TypeId::of::<T>(),
            dependency.clone() as Arc<dyn Any + Send + Sync>,
        );
        existing.and_then(|arc| arc.downcast::<T>().ok())
    }

    fn get<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        let guard = self.values.read().expect("dependency registry poisoned");
        guard
            .get(&TypeId::of::<T>())
            .and_then(|arc| arc.clone().downcast::<T>().ok())
    }
}

#[derive(Clone)]
pub struct WorkQueue<T> {
    inner: Arc<WorkQueueInner<T>>,
}

struct WorkQueueInner<T> {
    sender: mpsc::Sender<T>,
    receiver: Mutex<mpsc::Receiver<T>>,
}

impl<T> WorkQueue<T>
where
    T: Send + 'static,
{
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        Self {
            inner: Arc::new(WorkQueueInner {
                sender,
                receiver: Mutex::new(receiver),
            }),
        }
    }

    pub async fn enqueue(&self, item: T) -> Result<(), mpsc::error::SendError<T>> {
        self.inner.sender.send(item).await
    }

    pub async fn next(&self) -> Option<T> {
        let mut guard = self.inner.receiver.lock().await;
        guard.recv().await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ControllerTarget {
    ReplicaSet {
        namespace: Option<String>,
        name: String,
    },
    StatefulSet {
        namespace: Option<String>,
        name: String,
    },
    Deployment {
        namespace: Option<String>,
        name: String,
    },
    DaemonSet {
        namespace: Option<String>,
        name: String,
    },
}

impl ControllerTarget {
    #[allow(dead_code)]
    pub fn namespace(&self) -> Option<&str> {
        match self {
            ControllerTarget::ReplicaSet { namespace, .. }
            | ControllerTarget::StatefulSet { namespace, .. }
            | ControllerTarget::Deployment { namespace, .. }
            | ControllerTarget::DaemonSet { namespace, .. } => namespace.as_deref(),
        }
    }

    #[allow(dead_code)]
    pub fn name(&self) -> &str {
        match self {
            ControllerTarget::ReplicaSet { name, .. }
            | ControllerTarget::StatefulSet { name, .. }
            | ControllerTarget::Deployment { name, .. }
            | ControllerTarget::DaemonSet { name, .. } => name,
        }
    }
}

impl fmt::Display for ControllerTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ControllerTarget::ReplicaSet { namespace, name } => {
                write!(
                    f,
                    "ReplicaSet/{}/{}",
                    normalize_namespace(namespace.as_deref()),
                    name
                )
            }
            ControllerTarget::StatefulSet { namespace, name } => {
                write!(
                    f,
                    "StatefulSet/{}/{}",
                    normalize_namespace(namespace.as_deref()),
                    name
                )
            }
            ControllerTarget::Deployment { namespace, name } => {
                write!(
                    f,
                    "Deployment/{}/{}",
                    normalize_namespace(namespace.as_deref()),
                    name
                )
            }
            ControllerTarget::DaemonSet { namespace, name } => {
                write!(
                    f,
                    "DaemonSet/{}/{}",
                    normalize_namespace(namespace.as_deref()),
                    name
                )
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ControllerWorkItem {
    pub target: ControllerTarget,
}

impl ControllerWorkItem {
    #[allow(dead_code)]
    pub fn replicaset(namespace: Option<&str>, name: &str) -> Self {
        Self {
            target: ControllerTarget::ReplicaSet {
                namespace: namespace.map(|ns| ns.to_string()),
                name: name.to_string(),
            },
        }
    }

    pub fn statefulset(namespace: Option<&str>, name: &str) -> Self {
        Self {
            target: ControllerTarget::StatefulSet {
                namespace: namespace.map(|ns| ns.to_string()),
                name: name.to_string(),
            },
        }
    }

    #[allow(dead_code)]
    pub fn deployment(namespace: Option<&str>, name: &str) -> Self {
        Self {
            target: ControllerTarget::Deployment {
                namespace: namespace.map(|ns| ns.to_string()),
                name: name.to_string(),
            },
        }
    }

    #[allow(dead_code)]
    pub fn daemonset(namespace: Option<&str>, name: &str) -> Self {
        Self {
            target: ControllerTarget::DaemonSet {
                namespace: namespace.map(|ns| ns.to_string()),
                name: name.to_string(),
            },
        }
    }
}

#[derive(Clone)]
pub struct PodInformer {
    inner: Arc<PodInformerInner>,
}

impl PodInformer {
    fn new() -> Self {
        Self {
            inner: Arc::new(PodInformerInner {
                cache: RwLock::new(HashMap::new()),
                watchers: RwLock::new(HashMap::new()),
            }),
        }
    }

    pub async fn publish(&self, event_type: &str, pod: Pod) {
        let Some(key) = InformerKey::from_pod(&pod) else {
            return;
        };

        {
            let mut cache = self.inner.cache.write().await;
            if event_type.eq_ignore_ascii_case("DELETED") {
                cache.remove(&key);
            } else {
                cache.insert(key.clone(), pod.clone());
            }
        }

        self.inner.broadcast(key, event_type, pod).await;
    }

    pub async fn watch_namespace(
        &self,
        namespace: Option<&str>,
    ) -> broadcast::Receiver<WatchEvent<Pod>> {
        let scope = PodScope::Namespace(normalize_namespace(namespace));
        self.inner.ensure_watch(scope).await
    }

    pub async fn watch_pod(
        &self,
        namespace: Option<&str>,
        name: &str,
    ) -> broadcast::Receiver<WatchEvent<Pod>> {
        let key = InformerKey::new(namespace, name);
        let scope = PodScope::Pod(key.clone());
        self.inner.ensure_watch(scope).await
    }

    pub async fn watch_cluster(&self) -> broadcast::Receiver<WatchEvent<Pod>> {
        self.inner.ensure_watch(PodScope::Cluster).await
    }

    #[allow(dead_code)]
    pub async fn get(&self, namespace: Option<&str>, name: &str) -> Option<Pod> {
        let cache = self.inner.cache.read().await;
        cache.get(&InformerKey::new(namespace, name)).cloned()
    }

    #[allow(dead_code)]
    pub async fn list(&self, namespace: Option<&str>) -> Vec<Pod> {
        let namespace_filter = namespace.map(|ns| normalize_namespace(Some(ns)));
        let cache = self.inner.cache.read().await;
        cache
            .iter()
            .filter_map(|(key, pod)| {
                if namespace_filter
                    .as_ref()
                    .map(|target| key.namespace() == target.as_str())
                    .unwrap_or(true)
                {
                    Some(pod.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

struct PodInformerInner {
    cache: RwLock<HashMap<InformerKey, Pod>>,
    watchers: RwLock<HashMap<PodScope, broadcast::Sender<WatchEvent<Pod>>>>,
}

impl PodInformerInner {
    async fn ensure_watch(&self, scope: PodScope) -> broadcast::Receiver<WatchEvent<Pod>> {
        let mut watchers = self.watchers.write().await;
        watchers
            .entry(scope)
            .or_insert_with(|| {
                let (tx, _rx) = broadcast::channel(128);
                tx
            })
            .subscribe()
    }

    async fn broadcast(&self, key: InformerKey, event_type: &str, pod: Pod) {
        let event = WatchEvent {
            event_type: event_type.to_string(),
            object: pod,
        };

        let watchers = self.watchers.read().await;
        if let Some(sender) = watchers.get(&PodScope::Cluster) {
            let _ = sender.send(event.clone());
        }
        if let Some(sender) = watchers.get(&PodScope::Namespace(key.namespace().to_string())) {
            let _ = sender.send(event.clone());
        }
        if let Some(sender) = watchers.get(&PodScope::Pod(key.clone())) {
            let _ = sender.send(event);
        }
    }
}

#[derive(Clone, Debug, Eq)]
struct InformerKey {
    namespace: String,
    name: String,
}

impl InformerKey {
    fn new(namespace: Option<&str>, name: &str) -> Self {
        Self {
            namespace: normalize_namespace(namespace),
            name: name.to_string(),
        }
    }

    fn from_pod(pod: &Pod) -> Option<Self> {
        let name = pod.metadata.name.as_deref()?;
        Some(Self::new(pod.metadata.namespace.as_deref(), name))
    }

    fn namespace(&self) -> &str {
        &self.namespace
    }
}

impl PartialEq for InformerKey {
    fn eq(&self, other: &Self) -> bool {
        self.namespace == other.namespace && self.name == other.name
    }
}

impl Hash for InformerKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.namespace.hash(state);
        self.name.hash(state);
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum PodScope {
    Cluster,
    Namespace(String),
    Pod(InformerKey),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::pod::{ContainerSpec, PodSpec};
    use tokio::time::{timeout, Duration};

    fn sample_pod(namespace: Option<&str>, name: &str) -> Pod {
        Pod {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            metadata: crate::nanocloud::k8s::pod::ObjectMeta {
                name: Some(name.to_string()),
                namespace: namespace.map(|ns| ns.to_string()),
                ..Default::default()
            },
            spec: PodSpec {
                containers: vec![ContainerSpec {
                    name: "main".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            status: None,
        }
    }

    #[tokio::test]
    async fn dependency_round_trip() {
        let runtime = ControllerRuntime::new();
        let value = Arc::new(String::from("dependency"));
        assert!(runtime.register_dependency(value.clone()).is_none());
        let retrieved = runtime
            .dependency::<String>()
            .expect("dependency should be present");
        assert_eq!(retrieved.as_str(), "dependency");
    }

    #[tokio::test]
    async fn work_queue_orders_items() {
        let queue: WorkQueue<u32> = WorkQueue::new(4);
        queue.enqueue(1).await.expect("enqueue 1");
        queue.enqueue(2).await.expect("enqueue 2");
        queue.enqueue(3).await.expect("enqueue 3");

        assert_eq!(queue.next().await, Some(1));
        assert_eq!(queue.next().await, Some(2));
        assert_eq!(queue.next().await, Some(3));
    }

    #[tokio::test]
    async fn pod_informer_broadcasts_events() {
        let informer = PodInformer::new();
        let mut cluster = informer.watch_cluster().await;
        let mut namespace = informer.watch_namespace(Some("default")).await;
        let mut pod_scope = informer.watch_pod(Some("default"), "demo").await;

        let pod = sample_pod(Some("default"), "demo");
        informer.publish("ADDED", pod.clone()).await;

        let event = timeout(Duration::from_secs(1), cluster.recv())
            .await
            .expect("cluster event timeout")
            .expect("cluster event");
        assert_eq!(event.event_type, "ADDED");
        assert_eq!(event.object.metadata.name.as_deref(), Some("demo"));

        let event = timeout(Duration::from_secs(1), namespace.recv())
            .await
            .expect("namespace event timeout")
            .expect("namespace event");
        assert_eq!(event.event_type, "ADDED");
        assert_eq!(event.object.metadata.name.as_deref(), Some("demo"));

        let event = timeout(Duration::from_secs(1), pod_scope.recv())
            .await
            .expect("pod event timeout")
            .expect("pod event");
        assert_eq!(event.event_type, "ADDED");
        assert_eq!(event.object.metadata.name.as_deref(), Some("demo"));

        informer.publish("DELETED", pod.clone()).await;
        let event = timeout(Duration::from_secs(1), pod_scope.recv())
            .await
            .expect("pod delete timeout")
            .expect("pod delete event");
        assert_eq!(event.event_type, "DELETED");
    }
}
