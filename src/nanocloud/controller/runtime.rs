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
use crate::nanocloud::logger::{log_error, log_warn};
use crate::nanocloud::observability::metrics;
use serde::{Deserialize, Serialize};
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, RwLock as StdRwLock};
use tokio::sync::broadcast;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const DEFAULT_QUEUE_CAPACITY: usize = 256;
const DEFAULT_QUEUE_WARN_AT: usize = 192;
const COMPONENT: &str = "controller-runtime";

pub type HandlerResult = Result<(), Box<dyn Error + Send + Sync>>;
type HandlerErrorHook = Arc<dyn Fn(&ControllerWorkItem, &str) + Send + Sync>;

#[derive(Clone, Copy, Debug)]
/// Snapshot of pending queue items used for observability hooks.
pub struct QueueDepth {
    pub queued: usize,
    pub capacity: usize,
}

type QueueDepthHook = Arc<dyn Fn(QueueDepth) + Send + Sync>;

#[derive(Clone, Default)]
/// Optional callbacks used to surface dispatcher metrics and failures.
pub struct DispatcherHooks {
    pub queue_depth: Option<QueueDepthHook>,
    pub handler_error: Option<HandlerErrorHook>,
}

#[derive(Clone)]
/// Configuration for dispatcher queue sizing and observability.
pub struct ControllerRuntimeConfig {
    pub queue_capacity: usize,
    pub queue_warn_at: usize,
    pub hooks: DispatcherHooks,
}

impl Default for ControllerRuntimeConfig {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            queue_warn_at: DEFAULT_QUEUE_WARN_AT,
            hooks: DispatcherHooks::default(),
        }
    }
}

impl ControllerRuntimeConfig {
    #[allow(dead_code)]
    /// Overrides the bounded queue capacity while keeping backpressure warnings aligned.
    pub fn with_queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity.max(1);
        self.queue_warn_at = capacity.saturating_sub(capacity / 4).max(1);
        self
    }
}

#[allow(dead_code)]
#[derive(Clone)]
/// Handle returned by `spawn_executor` that can initiate shutdown and await completion.
pub struct DispatcherHandle {
    shutdown: CancellationToken,
    queue: KeyedWorkQueue<ControllerWorkItem>,
    join: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl DispatcherHandle {
    fn new(
        queue: KeyedWorkQueue<ControllerWorkItem>,
        join: JoinHandle<()>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            shutdown,
            queue,
            join: Arc::new(Mutex::new(Some(join))),
        }
    }

    #[allow(dead_code)]
    /// Signals dispatcher shutdown and closes the work queue.
    pub fn shutdown(&self) {
        self.queue.close();
        self.shutdown.cancel();
    }

    #[allow(dead_code)]
    /// Awaits dispatcher completion, draining any remaining work.
    pub async fn join(&self) {
        if let Some(handle) = self.join.lock().await.take() {
            let _ = handle.await;
        }
    }
}

#[derive(Debug)]
/// Error raised when a dependency lookup fails.
pub struct DependencyError {
    type_name: &'static str,
}

impl DependencyError {
    fn missing(type_name: &'static str) -> Self {
        Self { type_name }
    }
}

impl fmt::Display for DependencyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "missing dependency: {}", self.type_name)
    }
}

impl Error for DependencyError {}

#[derive(Debug)]
/// Errors emitted when initializing or starting the controller runtime.
pub enum ControllerRuntimeError {
    MissingDependencies(Vec<&'static str>),
}

impl fmt::Display for ControllerRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ControllerRuntimeError::MissingDependencies(types) => {
                write!(f, "missing dependencies: {}", types.join(", "))
            }
        }
    }
}

impl Error for ControllerRuntimeError {}

/// Generic Kubernetes-style watch event.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WatchEvent<T> {
    #[serde(rename = "type")]
    pub event_type: String,
    pub object: T,
}

/// Controller manager runtime that offers informers, a work queue, and shared dependencies.
/// Dispatcher tasks can be shut down and awaited via the returned `DispatcherHandle`.
pub struct ControllerRuntime {
    dependencies: DependencyRegistry,
    required_dependencies: StdRwLock<HashMap<TypeId, &'static str>>,
    work_queue: KeyedWorkQueue<ControllerWorkItem>,
    pods: PodInformer,
    handlers: Arc<StdRwLock<Arc<[ExecutorHandler]>>>,
    dispatcher: StdMutex<Option<DispatcherHandle>>,
    hooks: Arc<StdRwLock<DispatcherHooks>>,
}

impl ControllerRuntime {
    pub fn shared() -> Arc<Self> {
        static INSTANCE: OnceLock<Arc<ControllerRuntime>> = OnceLock::new();
        INSTANCE.get_or_init(ControllerRuntime::new).clone()
    }

    /// Creates a standalone controller runtime with default queue sizing.
    pub fn new() -> Arc<Self> {
        Arc::new(ControllerRuntime::with_config(
            ControllerRuntimeConfig::default(),
        ))
    }

    /// Builds a runtime with custom queue sizing and hooks. Useful for isolated tests.
    pub fn with_config(config: ControllerRuntimeConfig) -> Self {
        let hooks = Arc::new(StdRwLock::new(config.hooks.clone()));
        let warn_at = config.queue_warn_at;
        let queue_capacity = config.queue_capacity;
        let depth_hooks: QueueDepthHook = {
            let hooks = Arc::clone(&hooks);
            Arc::new(move |depth: QueueDepth| {
                if depth.queued >= warn_at {
                    let metadata = [
                        ("queued".to_string(), depth.queued.to_string()),
                        ("capacity".to_string(), depth.capacity.to_string()),
                    ];
                    let metadata_refs = [
                        (metadata[0].0.as_str(), metadata[0].1.as_str()),
                        (metadata[1].0.as_str(), metadata[1].1.as_str()),
                    ];
                    log_warn(
                        COMPONENT,
                        "controller work queue nearing capacity",
                        &metadata_refs,
                    );
                }

                if let Some(callback) = hooks
                    .read()
                    .expect("dispatcher hooks poisoned")
                    .queue_depth
                    .as_ref()
                {
                    callback(depth);
                }
            })
        };

        Self {
            dependencies: DependencyRegistry::new(),
            required_dependencies: StdRwLock::new(HashMap::new()),
            work_queue: KeyedWorkQueue::with_options(queue_capacity, Some(depth_hooks)),
            pods: PodInformer::new(),
            handlers: Arc::new(StdRwLock::new(Arc::from([]))),
            dispatcher: StdMutex::new(None),
            hooks,
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

    pub fn require_dependency<T>(&self) -> Result<Arc<T>, DependencyError>
    where
        T: Send + Sync + 'static,
    {
        self.dependencies
            .get::<T>()
            .ok_or_else(|| DependencyError::missing(std::any::type_name::<T>()))
    }

    /// Declares that `T` must be registered before dispatchers start, enabling eager validation.
    pub fn declare_required_dependency<T>(&self)
    where
        T: Send + Sync + 'static,
    {
        let mut required = self
            .required_dependencies
            .write()
            .expect("required dependency registry poisoned");
        required.insert(TypeId::of::<T>(), std::any::type_name::<T>());
    }

    fn validate_required_dependencies(&self) -> Result<(), ControllerRuntimeError> {
        let required = self
            .required_dependencies
            .read()
            .expect("required dependency registry poisoned");
        if required.is_empty() {
            return Ok(());
        }

        let missing: Vec<&'static str> = required
            .iter()
            .filter_map(|(id, name)| {
                if self.dependencies.contains(*id) {
                    None
                } else {
                    Some(*name)
                }
            })
            .collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(ControllerRuntimeError::MissingDependencies(missing))
        }
    }

    /// Replaces dispatcher hooks, allowing observers to receive queue depth or error notifications.
    #[allow(dead_code)]
    pub fn set_dispatcher_hooks(&self, hooks: DispatcherHooks) {
        let mut guard = self.hooks.write().expect("dispatcher hooks poisoned");
        *guard = hooks;
    }

    pub fn pods(&self) -> &PodInformer {
        &self.pods
    }

    pub fn context(&self) -> ReconcileContext<'_> {
        ReconcileContext::new(self)
    }

    pub fn work_queue(&self) -> KeyedWorkQueue<ControllerWorkItem> {
        self.work_queue.clone()
    }

    /// Registers a new handler and starts the dispatcher if needed, returning a joinable handle.
    ///
    /// Handlers should return `HandlerResult` to propagate failures into the dispatcher hooks.
    pub fn spawn_executor<H, Fut>(
        &self,
        handler: H,
    ) -> Result<DispatcherHandle, ControllerRuntimeError>
    where
        H: Fn(ControllerWorkItem) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = HandlerResult> + Send + 'static,
    {
        {
            let mut guard = self.handlers.write().expect("handler registry poisoned");
            let mut handlers: Vec<ExecutorHandler> = guard.iter().cloned().collect();
            handlers.push(Arc::new(move |item| Box::pin(handler(item))));
            *guard = Arc::from(handlers.into_boxed_slice());
        }
        self.ensure_dispatcher()
    }

    fn ensure_dispatcher(&self) -> Result<DispatcherHandle, ControllerRuntimeError> {
        if let Some(handle) = self
            .dispatcher
            .lock()
            .expect("dispatcher lock poisoned")
            .as_ref()
        {
            return Ok(handle.clone());
        }

        self.validate_required_dependencies()?;

        let queue = self.work_queue.clone();
        let queue_for_task = queue.clone();
        let handlers = Arc::clone(&self.handlers);
        let hooks = Arc::clone(&self.hooks);
        let shutdown = CancellationToken::new();
        let shutdown_for_task = shutdown.clone();
        let join = tokio::spawn(async move {
            run_dispatch_loop(queue_for_task, handlers, hooks, shutdown_for_task).await;
        });

        let mut guard = self.dispatcher.lock().expect("dispatcher lock poisoned");
        let handle = DispatcherHandle::new(queue, join, shutdown);
        *guard = Some(handle.clone());
        Ok(handle)
    }
}

#[derive(Default)]
struct DependencyRegistry {
    values: StdRwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

type ExecutorHandler = Arc<
    dyn Fn(ControllerWorkItem) -> Pin<Box<dyn Future<Output = HandlerResult> + Send>> + Send + Sync,
>;

async fn run_dispatch_loop(
    queue: KeyedWorkQueue<ControllerWorkItem>,
    handlers: Arc<StdRwLock<Arc<[ExecutorHandler]>>>,
    hooks: Arc<StdRwLock<DispatcherHooks>>,
    shutdown: CancellationToken,
) {
    let mut shutdown_triggered = false;

    loop {
        tokio::select! {
            _ = shutdown.cancelled(), if !shutdown_triggered => {
                shutdown_triggered = true;
                queue.close();
                continue;
            }
            maybe_item = queue.next() => {
                let Some(item) = maybe_item else { break; };
                let listeners = {
                    let guard = handlers.read().expect("handler registry poisoned");
                    guard.clone()
                };
                for handler in listeners.iter() {
                    if let Err(err) = handler(item.clone()).await {
                        let error_text = err.to_string();
                        let target_label = item.target.to_string();
                        metrics::record_controller_handler_error(target_label.as_str());
                        if let Some(callback) = hooks
                            .read()
                            .expect("dispatcher hooks poisoned")
                            .handler_error
                            .as_ref()
                        {
                            callback(&item, &error_text);
                        }

                        let metadata = [
                            ("target".to_string(), item.target.to_string()),
                            ("error".to_string(), error_text),
                        ];
                        let metadata_refs = [
                            (metadata[0].0.as_str(), metadata[0].1.as_str()),
                            (metadata[1].0.as_str(), metadata[1].1.as_str()),
                        ];
                        log_error(COMPONENT, "controller handler failed", &metadata_refs);
                    }
                }
            }
        }
    }
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

    fn contains(&self, type_id: TypeId) -> bool {
        let guard = self.values.read().expect("dependency registry poisoned");
        guard.contains_key(&type_id)
    }
}

#[derive(Clone)]
pub struct WorkQueue<T> {
    inner: Arc<WorkQueueInner<T>>,
}

struct WorkQueueInner<T> {
    sender: Mutex<mpsc::Sender<T>>,
    receiver: Mutex<mpsc::Receiver<T>>,
    pending: AtomicUsize,
    capacity: usize,
    depth_hook: QueueDepthHook,
    shutdown: CancellationToken,
}

impl<T> WorkQueue<T>
where
    T: Send + 'static,
{
    #[allow(dead_code)]
    pub fn new(capacity: usize) -> Self {
        Self::with_hook(capacity, None)
    }

    pub fn with_hook(capacity: usize, depth_hook: Option<QueueDepthHook>) -> Self {
        let capacity = capacity.max(1);
        let (sender, receiver) = mpsc::channel(capacity);
        Self {
            inner: Arc::new(WorkQueueInner {
                sender: Mutex::new(sender),
                receiver: Mutex::new(receiver),
                pending: AtomicUsize::new(0),
                capacity,
                depth_hook: depth_hook.unwrap_or_else(|| Arc::new(|_| {})),
                shutdown: CancellationToken::new(),
            }),
        }
    }

    pub async fn enqueue(&self, item: T) -> Result<(), mpsc::error::SendError<T>> {
        let sender = { self.inner.sender.lock().await.clone() };
        sender.send(item).await?;
        let pending = self.inner.pending.fetch_add(1, Ordering::SeqCst) + 1;
        self.inner.observe_depth(pending);
        Ok(())
    }

    pub async fn next(&self) -> Option<T> {
        let mut guard = self.inner.receiver.lock().await;
        let shutdown = self.inner.shutdown.clone();
        let pending = self.inner.pending.load(Ordering::SeqCst);
        tokio::select! {
            _ = shutdown.cancelled(), if pending == 0 => None,
            item = guard.recv() => {
                drop(guard);
                if let Some(value) = item {
                    let previous = self.inner.pending.fetch_sub(1, Ordering::SeqCst);
                    let pending = previous.saturating_sub(1);
                    self.inner.observe_depth(pending);
                    Some(value)
                } else {
                    None
                }
            }
        }
    }

    pub fn close(&self) {
        self.inner.shutdown.cancel();
    }

    #[allow(dead_code)]
    pub fn depth(&self) -> QueueDepth {
        QueueDepth {
            queued: self.inner.pending.load(Ordering::SeqCst),
            capacity: self.inner.capacity,
        }
    }
}

impl<T> WorkQueueInner<T> {
    fn observe_depth(&self, pending: usize) {
        metrics::set_controller_dispatcher_queue_depth(pending as i64);
        (self.depth_hook)(QueueDepth {
            queued: pending,
            capacity: self.capacity,
        });
    }
}

/// Work queue that de-duplicates items by key to avoid flooding reconciliations.
#[derive(Clone)]
pub struct KeyedWorkQueue<T>
where
    T: Clone + Eq + Hash + Send + 'static,
{
    inner: Arc<KeyedWorkQueueInner<T>>,
}

struct KeyedWorkQueueInner<T>
where
    T: Clone + Eq + Hash + Send + 'static,
{
    queue: WorkQueue<T>,
    in_flight: Mutex<HashSet<T>>,
}

impl<T> KeyedWorkQueue<T>
where
    T: Clone + Eq + Hash + Send + 'static,
{
    #[allow(dead_code)]
    pub fn new(capacity: usize) -> Self {
        Self::with_options(capacity, None)
    }

    pub fn with_options(capacity: usize, depth_hook: Option<QueueDepthHook>) -> Self {
        Self {
            inner: Arc::new(KeyedWorkQueueInner {
                queue: WorkQueue::with_hook(capacity, depth_hook),
                in_flight: Mutex::new(HashSet::new()),
            }),
        }
    }

    /// Enqueues the item if it is not already pending; returns true when the item
    /// was enqueued and false when it was coalesced with an existing entry.
    pub async fn enqueue(&self, item: T) -> Result<bool, mpsc::error::SendError<T>> {
        {
            let mut guard = self.inner.in_flight.lock().await;
            if !guard.insert(item.clone()) {
                return Ok(false);
            }
        }

        if let Err(err) = self.inner.queue.enqueue(item.clone()).await {
            let mut guard = self.inner.in_flight.lock().await;
            guard.remove(&item);
            return Err(err);
        }

        Ok(true)
    }

    pub async fn next(&self) -> Option<T> {
        let item = self.inner.queue.next().await?;
        let mut guard = self.inner.in_flight.lock().await;
        guard.remove(&item);
        Some(item)
    }

    pub fn close(&self) {
        self.inner.queue.close();
    }

    #[allow(dead_code)]
    pub fn depth(&self) -> QueueDepth {
        self.inner.queue.depth()
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
    Bundle {
        namespace: Option<String>,
        name: String,
    },
    VolumeSnapshot {
        namespace: Option<String>,
        name: String,
    },
    NetworkPolicy {
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
            | ControllerTarget::DaemonSet { namespace, .. }
            | ControllerTarget::Bundle { namespace, .. }
            | ControllerTarget::VolumeSnapshot { namespace, .. }
            | ControllerTarget::NetworkPolicy { namespace, .. } => namespace.as_deref(),
        }
    }

    #[allow(dead_code)]
    pub fn name(&self) -> &str {
        match self {
            ControllerTarget::ReplicaSet { name, .. }
            | ControllerTarget::StatefulSet { name, .. }
            | ControllerTarget::Deployment { name, .. }
            | ControllerTarget::DaemonSet { name, .. }
            | ControllerTarget::Bundle { name, .. }
            | ControllerTarget::VolumeSnapshot { name, .. }
            | ControllerTarget::NetworkPolicy { name, .. } => name,
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
            ControllerTarget::Bundle { namespace, name } => {
                write!(
                    f,
                    "Bundle/{}/{}",
                    normalize_namespace(namespace.as_deref()),
                    name
                )
            }
            ControllerTarget::VolumeSnapshot { namespace, name } => {
                write!(
                    f,
                    "VolumeSnapshot/{}/{}",
                    normalize_namespace(namespace.as_deref()),
                    name
                )
            }
            ControllerTarget::NetworkPolicy { namespace, name } => {
                write!(
                    f,
                    "NetworkPolicy/{}/{}",
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

    pub fn bundle(namespace: Option<&str>, name: &str) -> Self {
        Self {
            target: ControllerTarget::Bundle {
                namespace: namespace.map(|ns| ns.to_string()),
                name: name.to_string(),
            },
        }
    }

    pub fn volume_snapshot(namespace: Option<&str>, name: &str) -> Self {
        Self {
            target: ControllerTarget::VolumeSnapshot {
                namespace: namespace.map(|ns| ns.to_string()),
                name: name.to_string(),
            },
        }
    }

    pub fn network_policy(namespace: Option<&str>, name: &str) -> Self {
        Self {
            target: ControllerTarget::NetworkPolicy {
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
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
    async fn keyed_queue_coalesces_duplicates() {
        let queue: KeyedWorkQueue<&'static str> = KeyedWorkQueue::new(4);
        let first = queue.enqueue("a").await.expect("enqueue a");
        let second = queue.enqueue("a").await.expect("enqueue duplicate");
        assert!(first, "first enqueue should insert");
        assert!(
            !second,
            "duplicate enqueue should be coalesced and return false"
        );

        assert_eq!(queue.next().await, Some("a"));
        // Once drained, the same key can be enqueued again.
        let third = queue.enqueue("a").await.expect("enqueue after drain");
        assert!(third, "key should be accepted after drain");
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

    #[tokio::test]
    async fn dispatcher_shutdown_drains_queue() {
        let runtime = Arc::new(ControllerRuntime::with_config(
            ControllerRuntimeConfig::default().with_queue_capacity(8),
        ));
        let handled = Arc::new(AtomicUsize::new(0));
        let handler_count = handled.clone();
        let handle = runtime
            .spawn_executor(move |_item| {
                let handler_count = handler_count.clone();
                async move {
                    handler_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .expect("dispatcher handle");

        for idx in 0..4 {
            let item = ControllerWorkItem::statefulset(Some("default"), &format!("demo-{idx}"));
            assert!(runtime.work_queue().enqueue(item).await.expect("enqueue"));
        }

        timeout(Duration::from_secs(1), async {
            while handled.load(Ordering::SeqCst) < 4 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("dispatcher should drain");

        handle.shutdown();
        handle.join().await;
        assert_eq!(
            runtime.work_queue().depth().queued,
            0,
            "queue should be empty after shutdown"
        );
    }

    #[tokio::test]
    async fn handler_error_is_reported_and_dispatch_continues() {
        let runtime = Arc::new(ControllerRuntime::with_config(
            ControllerRuntimeConfig::default().with_queue_capacity(4),
        ));
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let depth_events: Arc<Mutex<Vec<QueueDepth>>> = Arc::new(Mutex::new(Vec::new()));

        runtime.set_dispatcher_hooks(DispatcherHooks {
            queue_depth: Some({
                let depth_events = depth_events.clone();
                Arc::new(move |depth| {
                    depth_events.lock().unwrap().push(depth);
                })
            }),
            handler_error: Some({
                let errors = errors.clone();
                Arc::new(move |item, err| {
                    errors
                        .lock()
                        .unwrap()
                        .push(format!("{}:{}", item.target, err));
                })
            }),
        });

        let success_count = Arc::new(AtomicUsize::new(0));
        let success_clone = success_count.clone();
        let _ = runtime
            .spawn_executor(move |_item| {
                let success_clone = success_clone.clone();
                async move {
                    success_clone.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .expect("dispatcher handle");

        let _ = runtime
            .spawn_executor(|item| async move {
                let message = format!("boom:{}", item.target);
                Err(message.into())
            })
            .expect("error handler");

        let item = ControllerWorkItem::deployment(Some("default"), "demo");
        runtime
            .work_queue()
            .enqueue(item.clone())
            .await
            .expect("enqueue");

        timeout(Duration::from_secs(1), async {
            while success_count.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("success handler should run");

        let captured_errors = errors.lock().unwrap();
        assert!(
            captured_errors
                .iter()
                .any(|err| err.contains("Deployment/")),
            "handler error hook should capture target"
        );
        drop(captured_errors);

        assert!(
            !depth_events.lock().unwrap().is_empty(),
            "queue depth hook should be invoked"
        );
    }

    #[tokio::test]
    async fn handler_snapshot_updates_across_dispatches() {
        let runtime = Arc::new(ControllerRuntime::with_config(
            ControllerRuntimeConfig::default().with_queue_capacity(4),
        ));
        let first_hits = Arc::new(AtomicUsize::new(0));
        let second_hits = Arc::new(AtomicUsize::new(0));

        let first_clone = first_hits.clone();
        runtime
            .spawn_executor(move |_item| {
                let first_clone = first_clone.clone();
                async move {
                    first_clone.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .expect("first handler");

        let item1 = ControllerWorkItem::bundle(Some("default"), "one");
        runtime
            .work_queue()
            .enqueue(item1)
            .await
            .expect("enqueue 1");

        timeout(Duration::from_secs(1), async {
            while first_hits.load(Ordering::SeqCst) < 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("first handler should process first item");

        let second_clone = second_hits.clone();
        runtime
            .spawn_executor(move |_item| {
                let second_clone = second_clone.clone();
                async move {
                    second_clone.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .expect("second handler");

        let item2 = ControllerWorkItem::bundle(Some("default"), "two");
        runtime
            .work_queue()
            .enqueue(item2)
            .await
            .expect("enqueue 2");

        timeout(Duration::from_secs(1), async {
            while second_hits.load(Ordering::SeqCst) < 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("second handler should observe later items");

        assert_eq!(first_hits.load(Ordering::SeqCst), 2);
        assert_eq!(second_hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dependency_validation_reports_missing() {
        let runtime = ControllerRuntime::new();
        assert!(
            runtime.require_dependency::<String>().is_err(),
            "missing dependency should error"
        );

        runtime.declare_required_dependency::<String>();
        let result = runtime.spawn_executor(|_item| async { Ok(()) });
        assert!(
            result.is_err(),
            "dispatcher startup should fail when required dependency absent"
        );

        assert!(
            runtime.dependency::<String>().is_none(),
            "optional lookup should remain None"
        );
    }
}
