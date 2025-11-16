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

use crate::nanocloud::controller::reconcile::DependencyHandle;
use crate::nanocloud::controller::replicaset::{
    ReplicaSetDesiredState, ReplicaSetError, ReplicaSetPodAction, ReplicaSetPodDesiredState,
};
use crate::nanocloud::controller::runtime::{ControllerRuntime, ControllerTarget, WatchEvent};
use crate::nanocloud::controller::scheduling::{
    KubeletExecutor, KubeletSchedulingBridge, ReplicaSetScheduler,
};
use crate::nanocloud::k8s::pod::{
    ContainerStatus as PodContainerStatus, Pod, PodCondition, PodStatus,
};
use crate::nanocloud::k8s::statefulset::StatefulSet;
use crate::nanocloud::k8s::store::{self, paginate_entries, ListCursor, PaginatedResult};
use crate::nanocloud::kubelet::runtime;
use crate::nanocloud::logger::{log_error, log_info, log_warn};
use crate::nanocloud::observability::{metrics, tracing};
use crate::nanocloud::oci::runtime::{ContainerState, ContainerStatus as RuntimeStatus};
use crate::nanocloud::oci::{container_runtime, OciImage, Registry};
use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::util::{is_missing_value_error, Keyspace};

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{
    broadcast,
    mpsc::{self, UnboundedSender},
    Mutex, RwLock,
};
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration, Instant};

struct PodRegistration {
    namespace: Option<String>,
    name: String,
    workload: RwLock<WorkloadManifest>,
    container_id: Mutex<Option<String>>,
    last_pod: RwLock<Option<Pod>>,
    restart_count: AtomicU32,
    backoff: Mutex<RestartBackoff>,
    desired_running: AtomicBool,
    monitor: Mutex<Option<JoinHandle<()>>>,
}

impl PodRegistration {
    fn container_name(&self) -> String {
        match self.namespace.as_deref() {
            Some(ns) if !ns.is_empty() => format!("{}-{}", ns, self.name),
            _ => self.name.clone(),
        }
    }
}

#[derive(Clone)]
enum WorkloadManifest {
    StatefulSet(StatefulSet),
    Pod(Pod),
}

impl WorkloadManifest {
    fn pod_template(&self) -> Pod {
        match self {
            WorkloadManifest::StatefulSet(workload) => {
                let mut metadata = workload.spec.template.metadata.clone();
                metadata.name = workload.metadata.name.clone();
                metadata.namespace = workload.metadata.namespace.clone();
                Pod {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    metadata,
                    spec: workload.spec.template.spec.clone(),
                    status: None,
                }
            }
            WorkloadManifest::Pod(pod) => {
                let mut template = pod.clone();
                template.status = None;
                template
            }
        }
    }

    fn primary_container_name(&self) -> Option<String> {
        match self {
            WorkloadManifest::StatefulSet(workload) => workload
                .spec
                .template
                .spec
                .containers
                .first()
                .map(|container| container.name.clone()),
            WorkloadManifest::Pod(pod) => pod
                .spec
                .containers
                .first()
                .map(|container| container.name.clone()),
        }
    }
}

#[derive(Debug)]
struct ReplicaPlanWork {
    namespace: Option<String>,
    statefulset: String,
    plan: ReplicaSetDesiredState,
}

#[derive(Clone, Debug)]
pub struct RestartBackoff {
    attempt: u32,
    next_attempt: Instant,
    last_error: Option<String>,
}

impl Default for RestartBackoff {
    fn default() -> Self {
        Self {
            attempt: 0,
            next_attempt: Instant::now(),
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RestartBackoffState {
    attempt: u32,
    next_allowed_at_ms: Option<u64>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DesiredStateRecord {
    running: bool,
}

impl RestartBackoff {
    pub fn should_retry(&self, now: Instant) -> bool {
        now >= self.next_attempt
    }

    pub fn on_success(&mut self, now: Instant) {
        self.attempt = 0;
        self.next_attempt = now;
        self.last_error = None;
    }

    pub fn on_failure(&mut self, now: Instant, message: String) {
        self.attempt = self.attempt.saturating_add(1);
        let exponent = self.attempt.saturating_sub(1).min(5);
        let delay = Duration::from_secs(1u64 << exponent);
        self.next_attempt = now + delay;
        self.last_error = Some(message);
    }

    fn to_state(&self) -> RestartBackoffState {
        let now = Instant::now();
        let remaining = if self.should_retry(now) {
            Duration::from_secs(0)
        } else {
            self.next_attempt.duration_since(now)
        };
        let next_allowed_at_ms = SystemTime::now()
            .checked_add(remaining)
            .and_then(|ts| ts.duration_since(UNIX_EPOCH).ok())
            .map(|dur| dur.as_millis() as u64);

        RestartBackoffState {
            attempt: self.attempt,
            next_allowed_at_ms,
            last_error: self.last_error.clone(),
        }
    }

    fn from_state(state: RestartBackoffState) -> Self {
        let now = Instant::now();
        let next_attempt = state
            .next_allowed_at_ms
            .map(|millis| {
                let target = UNIX_EPOCH + Duration::from_millis(millis);
                let now_system = SystemTime::now();
                let wait = target
                    .duration_since(now_system)
                    .unwrap_or(Duration::from_secs(0));
                now + wait
            })
            .unwrap_or(now);

        RestartBackoff {
            attempt: state.attempt,
            next_attempt,
            last_error: state.last_error,
        }
    }
}

pub struct Kubelet {
    runtime: Arc<ControllerRuntime>,
    pods: RwLock<HashMap<String, Arc<PodRegistration>>>,
    resource_counter: AtomicU64,
    plan_tx: UnboundedSender<ReplicaPlanWork>,
}

static INSTANCE: OnceLock<Arc<Kubelet>> = OnceLock::new();
const KUBELET_KEYSPACE: Keyspace = Keyspace::new("kubelet");

impl Kubelet {
    pub fn shared() -> Arc<Kubelet> {
        INSTANCE
            .get_or_init(|| {
                let runtime = ControllerRuntime::shared();
                let (plan_tx, mut plan_rx) = mpsc::unbounded_channel();
                let kubelet = Arc::new(Kubelet {
                    runtime: Arc::clone(&runtime),
                    pods: RwLock::new(HashMap::new()),
                    resource_counter: AtomicU64::new(1),
                    plan_tx: plan_tx.clone(),
                });
                let worker = Arc::clone(&kubelet);
                tokio::spawn(async move {
                    while let Some(work) = plan_rx.recv().await {
                        let namespace_label = work
                            .namespace
                            .clone()
                            .unwrap_or_else(|| "default".to_string());
                        let namespace_for_log = namespace_label.clone();
                        let set_name = work.statefulset.clone();
                        let span_label = format!("{}/{}", namespace_label, set_name);
                        let result = tracing::with_span(
                            "kubelet",
                            span_label,
                            worker.process_replica_plan(work),
                        )
                        .await;
                        if let Err(err) = result {
                            let error_text = err.to_string();
                            log_error(
                                "kubelet",
                                "Failed to process ReplicaSet plan",
                                &[
                                    ("statefulset", set_name.as_str()),
                                    ("namespace", namespace_for_log.as_str()),
                                    ("error", error_text.as_str()),
                                ],
                            );
                        }
                    }
                });
                let _ = runtime.register_dependency(Arc::clone(&kubelet));

                let executor: Arc<dyn KubeletExecutor> = kubelet.clone();
                let bridge = Arc::new(KubeletSchedulingBridge::new(executor));
                let scheduler_trait: Arc<dyn ReplicaSetScheduler> = bridge.clone();
                let scheduler_handle = Arc::new(DependencyHandle::new(scheduler_trait));
                let _ = runtime.register_dependency::<DependencyHandle<dyn ReplicaSetScheduler>>(
                    scheduler_handle,
                );
                kubelet
            })
            .clone()
    }

    pub fn current_resource_version(&self) -> String {
        let current = self.resource_counter.load(Ordering::SeqCst);
        current.saturating_sub(1).to_string()
    }

    pub async fn track_stateful_set(
        self: &Arc<Self>,
        namespace: Option<&str>,
        name: &str,
        workload: StatefulSet,
        container_id: String,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.track_workload(
            namespace,
            name,
            WorkloadManifest::StatefulSet(workload),
            container_id,
        )
        .await
    }

    pub async fn track_pod(
        self: &Arc<Self>,
        namespace: Option<&str>,
        name: &str,
        workload: Pod,
        container_id: String,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.track_workload(
            namespace,
            name,
            WorkloadManifest::Pod(workload),
            container_id,
        )
        .await
    }

    pub async fn delete_pod(
        self: &Arc<Self>,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let manifest = store::load_pod_manifest(namespace, name)?;
        let host_network = manifest
            .as_ref()
            .map(|pod| pod.spec.host_network)
            .unwrap_or(false);

        let container_alias = self.container_alias_by_name(namespace, name);
        if let Some(container_id) = runtime::get_container_id_by_name(&container_alias) {
            runtime::remove_container(&container_alias, &container_id, host_network)?;
        }

        self.forget_pod(namespace, name).await?;

        if let Err(err) = store::delete_pod_manifest(namespace, name) {
            if !is_missing_value_error(err.as_ref()) {
                return Err(err);
            }
        }

        metrics::clear_container(namespace, name);
        self.clear_desired_state(namespace, name).await;

        Ok(())
    }

    async fn track_workload(
        self: &Arc<Self>,
        namespace: Option<&str>,
        name: &str,
        manifest: WorkloadManifest,
        container_id: String,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = pod_key(namespace, name);
        let registration = {
            let mut pods_guard = self.pods.write().await;
            if let Some(existing) = pods_guard.get(&key) {
                existing.clone()
            } else {
                let reg = Arc::new(PodRegistration {
                    namespace: namespace
                        .filter(|ns| !ns.is_empty())
                        .map(|ns| ns.to_string()),
                    name: name.to_string(),
                    workload: RwLock::new(manifest.clone()),
                    container_id: Mutex::new(None),
                    last_pod: RwLock::new(None),
                    restart_count: AtomicU32::new(0),
                    backoff: Mutex::new(RestartBackoff::default()),
                    desired_running: AtomicBool::new(true),
                    monitor: Mutex::new(None),
                });
                pods_guard.insert(key, reg.clone());
                reg
            }
        };

        {
            let mut cid = registration.container_id.lock().await;
            *cid = Some(container_id);
        }
        {
            let mut spec = registration.workload.write().await;
            *spec = manifest;
        }
        let desired = load_desired_state(namespace, name)?.unwrap_or(true);
        registration
            .desired_running
            .store(desired, Ordering::SeqCst);

        if let Some(backoff_state) = load_backoff_state(namespace, name)? {
            let mut guard = registration.backoff.lock().await;
            *guard = backoff_state;
        }

        let previous_status = registration
            .last_pod
            .read()
            .await
            .as_ref()
            .and_then(|pod| pod.status.as_ref().cloned());
        let had_previous = previous_status.is_some();

        self.ensure_monitor(registration.clone()).await;
        let initial_event = if had_previous { "MODIFIED" } else { "ADDED" };
        let _ = self
            .refresh_pod(&registration, initial_event, previous_status)
            .await?;

        self.update_pod_gauges().await;
        Ok(())
    }

    pub async fn forget_pod(
        &self,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = pod_key(namespace, name);
        let registration = {
            let mut pods = self.pods.write().await;
            pods.remove(&key)
        };

        if let Some(reg) = registration {
            if let Some(handle) = reg.monitor.lock().await.take() {
                handle.abort();
            }
            if let Some(mut pod) = reg.last_pod.write().await.take() {
                pod.metadata.resource_version = Some(self.next_resource_version());
                self.runtime.pods().publish("DELETED", pod).await;
            }
        }

        if let Err(err) = clear_desired_state(namespace, name) {
            let err_str = err.to_string();
            log_error(
                "kubelet",
                "Failed to clear desired state",
                &[("pod", key.as_str()), ("error", err_str.as_str())],
            );
        }
        if let Err(err) = clear_backoff_state(namespace, name) {
            let err_str = err.to_string();
            log_error(
                "kubelet",
                "Failed to clear restart backoff",
                &[("pod", key.as_str()), ("error", err_str.as_str())],
            );
        }

        self.update_pod_gauges().await;
        Ok(())
    }

    pub async fn watch_namespace(
        &self,
        namespace: Option<&str>,
    ) -> broadcast::Receiver<WatchEvent<Pod>> {
        self.runtime.pods().watch_namespace(namespace).await
    }

    pub async fn watch_pod(
        &self,
        namespace: Option<&str>,
        name: &str,
    ) -> broadcast::Receiver<WatchEvent<Pod>> {
        self.runtime.pods().watch_pod(namespace, name).await
    }

    pub async fn watch_cluster(&self) -> broadcast::Receiver<WatchEvent<Pod>> {
        self.runtime.pods().watch_cluster().await
    }

    pub async fn restore_state(self: &Arc<Self>) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut restored: HashSet<String> = HashSet::new();

        let pod_records = store::list_pod_manifests()?;
        for record in pod_records {
            let store::StoredPod {
                namespace,
                name,
                mut workload,
            } = record;
            let namespace = namespace.filter(|ns| !ns.is_empty());
            let namespace_ref = namespace.as_deref();
            let namespace_display = normalize_namespace(namespace_ref);
            let desired_running = match load_desired_state(namespace_ref, &name) {
                Ok(record) => record.unwrap_or(true),
                Err(err) => {
                    let error_text = err.to_string();
                    log_warn(
                        "kubelet",
                        "Failed to load desired state record; assuming running",
                        &[
                            ("namespace", namespace_display.as_str()),
                            ("service", name.as_str()),
                            ("error", error_text.as_str()),
                        ],
                    );
                    true
                }
            };
            let container_name = match namespace_ref {
                Some(ns) => format!("{}-{}", ns, name.as_str()),
                None => name.clone(),
            };
            let container_id = match runtime::get_container_id_by_name(&container_name) {
                Some(id) => id,
                None => {
                    log_warn(
                        "kubelet",
                        "Skipping restoration for missing container reference",
                        &[
                            ("namespace", namespace_display.as_str()),
                            ("service", name.as_str()),
                        ],
                    );
                    continue;
                }
            };

            if workload.metadata.name.is_none() {
                workload.metadata.name = Some(name.clone());
            }
            if workload.metadata.namespace.is_none() {
                workload.metadata.namespace = namespace.clone();
            }

            if let Err(err) = self
                .track_pod(namespace_ref, &name, workload.clone(), container_id.clone())
                .await
            {
                let error_text = err.to_string();
                log_warn(
                    "kubelet",
                    "Failed to re-register workload",
                    &[
                        ("namespace", namespace_display.as_str()),
                        ("service", name.as_str()),
                        ("container_id", container_id.as_str()),
                        ("error", error_text.as_str()),
                    ],
                );
                continue;
            }

            if desired_running {
                if let Err(err) = self.ensure_running_on_restore(namespace_ref, &name).await {
                    let error_text = err.to_string();
                    log_warn(
                        "kubelet",
                        "Failed to restore desired running state",
                        &[
                            ("namespace", namespace_display.as_str()),
                            ("service", name.as_str()),
                            ("error", error_text.as_str()),
                        ],
                    );
                }
            }

            log_info(
                "kubelet",
                "Restored workload registration",
                &[
                    ("namespace", namespace_display.as_str()),
                    ("service", name.as_str()),
                    ("container_id", container_id.as_str()),
                ],
            );

            restored.insert(format!("{}/{}", namespace_display.as_str(), name.as_str()));
        }

        let workloads = store::list_stateful_sets()?;
        for record in workloads {
            let store::StoredStatefulSet {
                namespace,
                name,
                workload,
            } = record;
            let namespace = namespace.filter(|ns| !ns.is_empty());
            let namespace_ref = namespace.as_deref();
            let namespace_display = normalize_namespace(namespace_ref);
            let restore_key = format!("{}/{}", namespace_display.as_str(), name.as_str());
            if restored.contains(&restore_key) {
                continue;
            }
            let desired_running = match load_desired_state(namespace_ref, &name) {
                Ok(record) => record.unwrap_or(true),
                Err(err) => {
                    let error_text = err.to_string();
                    log_warn(
                        "kubelet",
                        "Failed to load desired state record; assuming running",
                        &[
                            ("namespace", namespace_display.as_str()),
                            ("service", name.as_str()),
                            ("error", error_text.as_str()),
                        ],
                    );
                    true
                }
            };
            let container_name = match namespace_ref {
                Some(ns) => format!("{}-{}", ns, name.as_str()),
                None => name.clone(),
            };
            let container_id = match runtime::get_container_id_by_name(&container_name) {
                Some(id) => id,
                None => {
                    log_warn(
                        "kubelet",
                        "Skipping restoration for missing container reference",
                        &[
                            ("namespace", namespace_display.as_str()),
                            ("service", name.as_str()),
                        ],
                    );
                    continue;
                }
            };

            if let Err(err) = self
                .track_stateful_set(namespace_ref, &name, workload, container_id.clone())
                .await
            {
                let error_text = err.to_string();
                log_warn(
                    "kubelet",
                    "Failed to re-register workload",
                    &[
                        ("namespace", namespace_display.as_str()),
                        ("service", name.as_str()),
                        ("container_id", container_id.as_str()),
                        ("error", error_text.as_str()),
                    ],
                );
                continue;
            }

            if desired_running {
                if let Err(err) = self.ensure_running_on_restore(namespace_ref, &name).await {
                    let error_text = err.to_string();
                    log_warn(
                        "kubelet",
                        "Failed to restore desired running state",
                        &[
                            ("namespace", namespace_display.as_str()),
                            ("service", name.as_str()),
                            ("error", error_text.as_str()),
                        ],
                    );
                }
            }

            log_info(
                "kubelet",
                "Restored workload registration",
                &[
                    ("namespace", namespace_display.as_str()),
                    ("service", name.as_str()),
                    ("container_id", container_id.as_str()),
                ],
            );

            restored.insert(restore_key);
        }

        Ok(())
    }

    async fn ensure_running_on_restore(
        &self,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = pod_key(namespace, name);
        let registration = {
            let pods = self.pods.read().await;
            pods.get(&key).cloned()
        };
        let Some(registration) = registration else {
            return Ok(());
        };

        if !registration.desired_running.load(Ordering::SeqCst) {
            return Ok(());
        }

        let container_id = {
            let guard = registration.container_id.lock().await;
            guard.clone()
        };
        let Some(container_id) = container_id else {
            return Ok(());
        };

        let runtime = container_runtime();
        let state = runtime.state(&container_id)?;
        if matches!(
            state.status,
            RuntimeStatus::Running | RuntimeStatus::Paused | RuntimeStatus::Creating
        ) {
            return Ok(());
        }

        self.try_restart(&registration, &container_id).await?;
        Ok(())
    }

    pub async fn set_desired_running(&self, namespace: Option<&str>, name: &str, running: bool) {
        let key = pod_key(namespace, name);
        if let Some(entry) = {
            let pods = self.pods.read().await;
            pods.get(&key).cloned()
        } {
            entry.desired_running.store(running, Ordering::SeqCst);
        }
        if let Err(err) = persist_desired_state(namespace, name, running) {
            let err_str = err.to_string();
            log_error(
                "kubelet",
                "Failed to persist desired state",
                &[("pod", key.as_str()), ("error", err_str.as_str())],
            );
        }
    }

    pub async fn clear_desired_state(&self, namespace: Option<&str>, name: &str) {
        let key = pod_key(namespace, name);
        if let Err(err) = clear_desired_state(namespace, name) {
            let err_str = err.to_string();
            log_error(
                "kubelet",
                "Failed to clear desired state",
                &[("pod", key.as_str()), ("error", err_str.as_str())],
            );
        }
    }

    pub async fn get_pod(
        &self,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Pod>, Box<dyn Error + Send + Sync>> {
        let key = pod_key(namespace, name);
        let registration = {
            let pods = self.pods.read().await;
            pods.get(&key).cloned()
        };
        if let Some(registration) = registration {
            self.load_pod_snapshot(&registration).await
        } else {
            Ok(None)
        }
    }

    pub async fn list_pods(
        &self,
        namespace: Option<&str>,
    ) -> Result<Vec<Pod>, Box<dyn Error + Send + Sync>> {
        self.list_pods_paginated(namespace, None, None, None)
            .await
            .map(|page| page.items)
    }

    pub async fn list_pods_since(
        &self,
        namespace: Option<&str>,
        resource_version: Option<u64>,
    ) -> Result<Vec<Pod>, Box<dyn Error + Send + Sync>> {
        self.list_pods_paginated(namespace, resource_version, None, None)
            .await
            .map(|page| page.items)
    }

    pub async fn list_pods_paginated(
        &self,
        namespace: Option<&str>,
        resource_version: Option<u64>,
        limit: Option<u32>,
        cursor: Option<&ListCursor>,
    ) -> Result<PaginatedResult<Pod>, Box<dyn Error + Send + Sync>> {
        let entries = self
            .collect_pod_entries(namespace, resource_version)
            .await?;

        paginate_entries(entries, cursor, limit)
            .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)
    }

    pub async fn collect_pod_entries(
        &self,
        namespace: Option<&str>,
        resource_version: Option<u64>,
    ) -> Result<Vec<(String, Pod, Option<String>)>, Box<dyn Error + Send + Sync>> {
        let registrations = self.registrations_for_namespace(namespace).await;
        let mut entries = Vec::new();
        for registration in registrations {
            if let Some(pod) = self.load_pod_snapshot(&registration).await? {
                if Self::resource_version_is_newer(&pod, resource_version) {
                    let key = pod_key(registration.namespace.as_deref(), &registration.name);
                    let resource_version = pod.metadata.resource_version.clone();
                    entries.push((key, pod, resource_version));
                }
            }
        }
        Ok(entries)
    }

    async fn registrations_for_namespace(
        &self,
        namespace: Option<&str>,
    ) -> Vec<Arc<PodRegistration>> {
        let namespace_filter = namespace.map(|ns| normalize_namespace(Some(ns)));
        let pods = self.pods.read().await;
        pods.values()
            .filter(|entry| {
                namespace_filter
                    .as_ref()
                    .map(|target| normalize_namespace(entry.namespace.as_deref()) == *target)
                    .unwrap_or(true)
            })
            .cloned()
            .collect()
    }

    fn resource_version_is_newer(pod: &Pod, since: Option<u64>) -> bool {
        since
            .map(|threshold| {
                pod.metadata
                    .resource_version
                    .as_deref()
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(|current| current > threshold)
                    .unwrap_or(true)
            })
            .unwrap_or(true)
    }

    async fn ensure_monitor(self: &Arc<Self>, registration: Arc<PodRegistration>) {
        let mut monitor = registration.monitor.lock().await;
        if monitor.is_some() {
            return;
        }
        let kubelet = Arc::clone(self);
        let entry = Arc::clone(&registration);
        *monitor = Some(tokio::spawn(
            async move { kubelet.monitor_loop(entry).await },
        ));
    }

    async fn update_pod_gauges(&self) {
        let pods_guard = self.pods.read().await;
        if pods_guard.is_empty() {
            metrics::set_pod_gauges(&[]);
            return;
        }
        let mut counts: HashMap<String, i64> = HashMap::new();
        for registration in pods_guard.values() {
            let namespace = registration
                .namespace
                .as_deref()
                .filter(|ns| !ns.is_empty())
                .unwrap_or("default");
            *counts.entry(namespace.to_string()).or_insert(0) += 1;
        }
        let entries: Vec<(String, i64)> = counts.into_iter().collect();
        metrics::set_pod_gauges(&entries);
    }

    async fn load_pod_snapshot(
        &self,
        registration: &Arc<PodRegistration>,
    ) -> Result<Option<Pod>, Box<dyn Error + Send + Sync>> {
        {
            let guard = registration.last_pod.read().await;
            if let Some(pod) = guard.clone() {
                return Ok(Some(pod));
            }
        }
        let previous_status = registration
            .last_pod
            .read()
            .await
            .as_ref()
            .and_then(|pod| pod.status.as_ref().cloned());
        self.refresh_pod(registration, "ADDED", previous_status)
            .await
    }

    async fn monitor_loop(self: Arc<Self>, registration: Arc<PodRegistration>) {
        let mut ticker = interval(Duration::from_secs(2));
        loop {
            ticker.tick().await;
            if let Err(err) = self
                .refresh_pod(&registration, "MODIFIED", None)
                .await
                .map(|_| ())
            {
                let err_str = err.to_string();
                log_warn(
                    "kubelet",
                    "Failed to refresh pod snapshot",
                    &[
                        ("pod", registration.container_name().as_str()),
                        ("error", err_str.as_str()),
                    ],
                );
            }
        }
    }

    async fn refresh_pod(
        &self,
        registration: &Arc<PodRegistration>,
        initial_event: &str,
        previous_status: Option<PodStatus>,
    ) -> Result<Option<Pod>, Box<dyn Error + Send + Sync>> {
        let container_id = {
            let cid_guard = registration.container_id.lock().await;
            cid_guard.clone()
        };

        let container_id = match container_id {
            Some(id) => id,
            None => return Ok(None),
        };

        let runtime = container_runtime();
        let mut state = runtime.state(&container_id)?;

        if matches!(
            state.status,
            RuntimeStatus::Stopped | RuntimeStatus::Unknown
        ) {
            self.try_restart(registration, &container_id).await?;
            state = runtime.state(&container_id)?;
        }

        let previous_snapshot = registration.last_pod.read().await.clone();
        let status_for_build = match previous_status {
            Some(status) => Some(status),
            None => previous_snapshot
                .as_ref()
                .and_then(|pod| pod.status.as_ref().cloned()),
        };
        let event_type = if previous_snapshot.is_some() {
            "MODIFIED"
        } else {
            initial_event
        };

        let mut pod = self
            .build_pod(registration, &state, status_for_build)
            .await?;

        if let Some(previous) = previous_snapshot.as_ref() {
            if !has_pod_status_changed(previous.status.as_ref(), pod.status.as_ref()) {
                return Ok(previous_snapshot);
            }
        }

        pod.metadata.resource_version = Some(self.next_resource_version());
        registration.last_pod.write().await.replace(pod.clone());
        self.runtime.pods().publish(event_type, pod.clone()).await;
        Ok(Some(pod))
    }

    async fn try_restart(
        &self,
        registration: &Arc<PodRegistration>,
        container_id: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if !registration.desired_running.load(Ordering::SeqCst) {
            return Ok(());
        }
        let namespace = registration.namespace.as_deref();
        let app = registration.name.as_str();
        let container_name = registration.container_name();

        let now = Instant::now();
        {
            let guard = registration.backoff.lock().await;
            if !guard.should_retry(now) {
                return Ok(());
            }
        }

        let result = runtime::start_container(namespace, app, &container_name, container_id).await;

        let mut backoff = registration.backoff.lock().await;
        match result {
            Ok(()) => {
                backoff.on_success(Instant::now());
                if let Err(err) = clear_backoff_state(namespace, app) {
                    let err_str = err.to_string();
                    log_error(
                        "kubelet",
                        "Failed to clear restart backoff",
                        &[
                            ("container", container_name.as_str()),
                            ("error", err_str.as_str()),
                        ],
                    );
                }
                registration.restart_count.fetch_add(1, Ordering::SeqCst);
                metrics::record_restart(namespace, app, "auto");
                Ok(())
            }
            Err(err) => {
                let message = err.to_string();
                backoff.on_failure(Instant::now(), message);
                if let Err(err) = persist_backoff_state(namespace, app, &backoff) {
                    let err_str = err.to_string();
                    log_error(
                        "kubelet",
                        "Failed to persist restart backoff",
                        &[
                            ("container", container_name.as_str()),
                            ("error", err_str.as_str()),
                        ],
                    );
                }
                Err(err)
            }
        }
    }

    async fn build_pod(
        &self,
        registration: &Arc<PodRegistration>,
        state: &ContainerState,
        previous_status: Option<PodStatus>,
    ) -> Result<Pod, Box<dyn Error + Send + Sync>> {
        let workload_manifest = registration.workload.read().await.clone();
        let mut pod = workload_manifest.pod_template();

        let namespace = normalize_namespace(registration.namespace.as_deref());
        pod.metadata.namespace = Some(namespace.clone());
        pod.metadata.name = Some(registration.name.clone());

        let mut status = PodStatus::default();
        status.phase = Some(match state.status {
            RuntimeStatus::Running => "Running".to_string(),
            RuntimeStatus::Created | RuntimeStatus::Creating => "Pending".to_string(),
            RuntimeStatus::Paused => "Running".to_string(),
            RuntimeStatus::Stopped => "Failed".to_string(),
            RuntimeStatus::Unknown => "Unknown".to_string(),
        });
        status.pod_ip = state
            .network
            .ip_addresses
            .first()
            .map(|addr| addr.split('/').next().unwrap_or(addr).to_string());
        status.start_time = state.started_at.clone();

        let readiness_status = match state.status {
            RuntimeStatus::Running | RuntimeStatus::Paused => "True",
            RuntimeStatus::Unknown => "Unknown",
            _ => "False",
        };
        let container_ready = readiness_status == "True";
        let failure_message = {
            let guard = registration.backoff.lock().await;
            guard.last_error.clone()
        };

        let mut previous_conditions = HashMap::new();
        if let Some(prev_status) = previous_status.as_ref() {
            for condition in &prev_status.conditions {
                previous_conditions.insert(condition.condition_type.clone(), condition.clone());
            }
        }

        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        let mut conditions = Vec::new();
        conditions.push(build_condition(
            "PodScheduled",
            "True",
            None,
            None,
            previous_conditions.get("PodScheduled"),
            &now,
        ));

        let (ready_reason_key, ready_message_owned): (Option<&'static str>, Option<String>) =
            match (readiness_status, failure_message) {
                ("True", _) => (None, None),
                (_, Some(message)) => (Some("ContainerStartFailure"), Some(message)),
                ("Unknown", None) => (
                    Some("Unknown"),
                    Some("Container readiness is unknown".to_string()),
                ),
                (_, None) => (
                    Some("ContainersNotReady"),
                    Some("Containers are not yet ready".to_string()),
                ),
            };
        let ready_message = ready_message_owned.as_deref();

        conditions.push(build_condition(
            "Ready",
            readiness_status,
            ready_reason_key,
            ready_message,
            previous_conditions.get("Ready"),
            &now,
        ));
        conditions.push(build_condition(
            "ContainersReady",
            readiness_status,
            ready_reason_key,
            ready_message,
            previous_conditions.get("ContainersReady"),
            &now,
        ));

        status.conditions = conditions;

        let container_status = PodContainerStatus {
            name: workload_manifest
                .primary_container_name()
                .unwrap_or_else(|| registration.name.clone()),
            restart_count: registration.restart_count.load(Ordering::SeqCst),
            ready: container_ready,
            image: state.image_digest.clone(),
            image_id: state.image_digest.clone(),
            state: pod_state(
                &state.status,
                state.started_at.as_deref(),
                state.finished_at.as_deref(),
            ),
        };
        status.container_statuses.push(container_status);
        pod.status = Some(status);

        Ok(pod)
    }

    fn next_resource_version(&self) -> String {
        let value = self.resource_counter.fetch_add(1, Ordering::SeqCst);
        value.to_string()
    }

    async fn process_replica_plan(
        self: &Arc<Self>,
        work: ReplicaPlanWork,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let ReplicaPlanWork {
            namespace,
            statefulset,
            plan,
        } = work;

        let namespace_ref = namespace.as_deref();
        let workload = match store::load(namespace_ref, statefulset.as_str()) {
            Ok(Some(workload)) => workload,
            Ok(None) => {
                log_warn(
                    "kubelet",
                    "Skipping ReplicaSet plan for missing StatefulSet",
                    &[
                        ("namespace", namespace_ref.unwrap_or("default")),
                        ("statefulset", statefulset.as_str()),
                    ],
                );
                return Ok(());
            }
            Err(err) => {
                return Err(with_context(
                    err,
                    format!(
                        "Failed to load StatefulSet {}/{} from store",
                        namespace_ref.unwrap_or("default"),
                        statefulset
                    ),
                ));
            }
        };

        for deletion in plan.deletions.iter() {
            if let Err(err) = self.remove_statefulset_pod(namespace_ref, deletion).await {
                let error_text = err.to_string();
                log_error(
                    "kubelet",
                    "Failed to remove StatefulSet pod during plan application",
                    &[
                        ("namespace", namespace_ref.unwrap_or("default")),
                        ("statefulset", statefulset.as_str()),
                        ("pod", deletion.as_str()),
                        ("error", error_text.as_str()),
                    ],
                );
            }
        }

        for pod_plan in plan.pods.iter() {
            if let Err(err) = self
                .ensure_statefulset_pod(namespace_ref, statefulset.as_str(), &workload, pod_plan)
                .await
            {
                let error_text = err.to_string();
                log_error(
                    "kubelet",
                    "Failed to apply StatefulSet pod plan",
                    &[
                        ("namespace", namespace_ref.unwrap_or("default")),
                        ("statefulset", statefulset.as_str()),
                        ("pod", pod_plan.identity.name.as_str()),
                        ("action", format!("{:?}", pod_plan.action).as_str()),
                        ("error", error_text.as_str()),
                    ],
                );
            }
        }

        Ok(())
    }

    async fn ensure_statefulset_pod(
        self: &Arc<Self>,
        namespace: Option<&str>,
        statefulset_name: &str,
        workload: &StatefulSet,
        pod_plan: &ReplicaSetPodDesiredState,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if matches!(pod_plan.action, ReplicaSetPodAction::Update) {
            self.remove_statefulset_pod(namespace, pod_plan.identity.name.as_str())
                .await?;
        } else if matches!(pod_plan.action, ReplicaSetPodAction::Retain) {
            if let Some(existing_id) =
                runtime::get_container_id_by_name(&self.container_alias(namespace, pod_plan))
            {
                let manifest =
                    match store::load_pod_manifest(namespace, pod_plan.identity.name.as_str()) {
                        Ok(Some(pod)) => pod,
                        Ok(None) => {
                            let mut template = workload.spec.template.clone();
                            template.metadata.name = Some(pod_plan.identity.name.clone());
                            template.metadata.namespace = Some(normalize_namespace(namespace));
                            Pod {
                                api_version: "v1".to_string(),
                                kind: "Pod".to_string(),
                                metadata: template.metadata,
                                spec: template.spec,
                                status: None,
                            }
                        }
                        Err(err) => {
                            return Err(with_context(
                                err,
                                format!(
                                    "Failed to load manifest for StatefulSet pod '{}'",
                                    pod_plan.identity.name
                                ),
                            ));
                        }
                    };
                store::save_pod_manifest(namespace, pod_plan.identity.name.as_str(), &manifest)?;
                self.track_pod(
                    namespace,
                    pod_plan.identity.name.as_str(),
                    manifest,
                    existing_id,
                )
                .await?;
            }
            return Ok(());
        }

        let namespace_label = normalize_namespace(namespace);
        let pod_name = pod_plan.identity.name.clone();
        let container_alias = self.container_alias(namespace, pod_plan);

        let mut runtime_pod_spec = workload.spec.template.spec.clone();
        let primary_container = runtime_pod_spec
            .containers
            .first_mut()
            .ok_or_else(|| new_error("StatefulSet template must provide at least one container"))?;

        let image_reference = primary_container.image.clone().ok_or_else(|| {
            new_error("StatefulSet container specification requires an image reference")
        })?;

        let manifest = Registry::pull(&image_reference, false)
            .await
            .map_err(|err| {
                with_context(err, format!("Failed to pull image {}", image_reference))
            })?;
        let oci_image = OciImage::load(&manifest.config.digest)?;

        primary_container.image_command = oci_image.config.entrypoint.clone().unwrap_or_default();
        primary_container.image_args = oci_image.config.cmd.clone().unwrap_or_default();
        primary_container.working_dir = primary_container
            .working_dir
            .clone()
            .or_else(|| oci_image.config.working_dir.clone());

        let mut metadata = workload.spec.template.metadata.clone();
        metadata.name = Some(pod_name.clone());
        metadata.namespace = Some(namespace_label.clone());
        metadata.labels = pod_plan.identity.labels.clone();
        metadata.annotations = pod_plan.identity.annotations.clone();
        metadata.resource_version = Some(self.next_resource_version());

        let pod_manifest = Pod {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            metadata,
            spec: runtime_pod_spec.clone(),
            status: None,
        };

        store::save_pod_manifest(namespace, pod_name.as_str(), &pod_manifest)?;

        if let Some(existing_id) = runtime::get_container_id_by_name(&container_alias) {
            // Container already exists; ensure tracking is in place.
            self.track_pod(namespace, pod_name.as_str(), pod_manifest, existing_id)
                .await?;
            return Ok(());
        }

        let container =
            runtime::create_container(&container_alias, &oci_image, &manifest, &runtime_pod_spec)
                .await
                .map_err(|err| {
                    with_context(
                        err,
                        format!(
                            "Failed to create container '{}' for StatefulSet '{}/{}'",
                            container_alias, namespace_label, statefulset_name
                        ),
                    )
                })?;

        runtime::start_container(
            namespace,
            pod_name.as_str(),
            &container_alias,
            &container.id,
        )
        .await
        .map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to start container '{}' for StatefulSet '{}/{}'",
                    container_alias, namespace_label, statefulset_name
                ),
            )
        })?;

        self.track_pod(
            namespace,
            pod_name.as_str(),
            pod_manifest,
            container.id.clone(),
        )
        .await?;
        self.set_desired_running(namespace, pod_name.as_str(), true)
            .await;

        log_info(
            "kubelet",
            "Started StatefulSet pod",
            &[
                ("namespace", namespace_label.as_str()),
                ("statefulset", statefulset_name),
                ("pod", pod_name.as_str()),
                ("container_id", container.id.as_str()),
            ],
        );

        Ok(())
    }

    async fn remove_statefulset_pod(
        self: &Arc<Self>,
        namespace: Option<&str>,
        pod_name: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let manifest = match store::load_pod_manifest(namespace, pod_name) {
            Ok(value) => value,
            Err(err) => {
                log_warn(
                    "kubelet",
                    "Failed to load pod manifest while removing StatefulSet pod",
                    &[
                        ("namespace", normalize_namespace(namespace).as_str()),
                        ("pod", pod_name),
                        ("error", err.to_string().as_str()),
                    ],
                );
                None
            }
        };
        let host_network = manifest
            .as_ref()
            .map(|pod| pod.spec.host_network)
            .unwrap_or(false);

        let container_alias = self.container_alias_by_name(namespace, pod_name);
        if let Some(container_id) = runtime::get_container_id_by_name(&container_alias) {
            if let Err(err) =
                runtime::remove_container(&container_alias, &container_id, host_network)
            {
                log_warn(
                    "kubelet",
                    "Failed to remove StatefulSet container during deletion",
                    &[
                        ("pod", pod_name),
                        ("container_id", container_id.as_str()),
                        ("error", err.to_string().as_str()),
                    ],
                );
            }
        }

        let _ = self.forget_pod(namespace, pod_name).await;

        if let Err(err) = store::delete_pod_manifest(namespace, pod_name) {
            if !is_missing_value_error(err.as_ref()) {
                return Err(err);
            }
        }

        Ok(())
    }

    fn container_alias(
        &self,
        namespace: Option<&str>,
        pod_plan: &ReplicaSetPodDesiredState,
    ) -> String {
        self.container_alias_by_name(namespace, pod_plan.identity.name.as_str())
    }

    fn container_alias_by_name(&self, namespace: Option<&str>, pod_name: &str) -> String {
        let namespace_label = normalize_namespace(namespace);
        if namespace_label.is_empty() {
            pod_name.to_string()
        } else {
            format!("{}-{}", namespace_label, pod_name)
        }
    }
}

impl KubeletExecutor for Kubelet {
    fn apply_replica_set(
        &self,
        target: &ControllerTarget,
        plan: &ReplicaSetDesiredState,
    ) -> Result<(), ReplicaSetError> {
        let ControllerTarget::ReplicaSet { namespace, name } = target else {
            return Err(ReplicaSetError::Dependency(
                "kubelet executor received non-ReplicaSet target".to_string(),
            ));
        };
        let statefulset_name = plan
            .owner
            .as_ref()
            .map(|owner| owner.name.clone())
            .unwrap_or_else(|| name.clone());
        let work = ReplicaPlanWork {
            namespace: namespace.clone(),
            statefulset: statefulset_name,
            plan: plan.clone(),
        };
        self.plan_tx
            .send(work)
            .map_err(|err| ReplicaSetError::Dependency(err.to_string()))?;
        Ok(())
    }
}

fn normalize_namespace(namespace: Option<&str>) -> String {
    namespace
        .filter(|ns| !ns.is_empty())
        .unwrap_or("default")
        .to_string()
}

fn has_pod_status_changed(previous: Option<&PodStatus>, current: Option<&PodStatus>) -> bool {
    match (previous, current) {
        (Some(prev), Some(curr)) => prev != curr,
        (None, None) => false,
        _ => true,
    }
}

fn pod_key(namespace: Option<&str>, name: &str) -> String {
    format!("{}/{}", normalize_namespace(namespace), name)
}

fn backoff_key(namespace: Option<&str>, name: &str) -> String {
    format!("/backoff/{}/{}", normalize_namespace(namespace), name)
}

fn desired_state_key(namespace: Option<&str>, name: &str) -> String {
    format!("/desired/{}/{}", normalize_namespace(namespace), name)
}

fn load_backoff_state(
    namespace: Option<&str>,
    name: &str,
) -> Result<Option<RestartBackoff>, Box<dyn Error + Send + Sync>> {
    let key = backoff_key(namespace, name);
    match KUBELET_KEYSPACE.get(&key) {
        Ok(raw) => {
            let state: RestartBackoffState = serde_json::from_str(&raw)?;
            Ok(Some(RestartBackoff::from_state(state)))
        }
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(None)
            } else {
                Err(err)
            }
        }
    }
}

fn persist_backoff_state(
    namespace: Option<&str>,
    name: &str,
    backoff: &RestartBackoff,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = backoff_key(namespace, name);
    let payload = serde_json::to_string(&backoff.to_state())?;
    KUBELET_KEYSPACE.put(&key, &payload)
}

fn clear_backoff_state(
    namespace: Option<&str>,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = backoff_key(namespace, name);
    match KUBELET_KEYSPACE.delete(&key) {
        Ok(()) => Ok(()),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(())
            } else {
                Err(err)
            }
        }
    }
}

fn persist_desired_state(
    namespace: Option<&str>,
    name: &str,
    running: bool,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = desired_state_key(namespace, name);
    let payload = serde_json::to_string(&DesiredStateRecord { running })
        .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
    KUBELET_KEYSPACE.put(&key, &payload)
}

fn load_desired_state(
    namespace: Option<&str>,
    name: &str,
) -> Result<Option<bool>, Box<dyn Error + Send + Sync>> {
    let key = desired_state_key(namespace, name);
    match KUBELET_KEYSPACE.get(&key) {
        Ok(raw) => {
            let record: DesiredStateRecord = serde_json::from_str(&raw)
                .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
            Ok(Some(record.running))
        }
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(None)
            } else {
                Err(err)
            }
        }
    }
}

fn clear_desired_state(
    namespace: Option<&str>,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = desired_state_key(namespace, name);
    match KUBELET_KEYSPACE.delete(&key) {
        Ok(()) => Ok(()),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(())
            } else {
                Err(err)
            }
        }
    }
}

fn pod_state(
    status: &RuntimeStatus,
    started_at: Option<&str>,
    finished_at: Option<&str>,
) -> Option<HashMap<String, serde_json::Value>> {
    use serde_json::json;

    let mut state = HashMap::new();
    match status {
        RuntimeStatus::Running => {
            let mut running = HashMap::new();
            if let Some(started) = started_at {
                running.insert("startedAt".to_string(), json!(started));
            }
            state.insert("running".to_string(), json!(running));
        }
        RuntimeStatus::Stopped => {
            let mut terminated = HashMap::new();
            if let Some(finished) = finished_at {
                terminated.insert("finishedAt".to_string(), json!(finished));
            }
            state.insert("terminated".to_string(), json!(terminated));
        }
        _ => {}
    }

    if state.is_empty() {
        None
    } else {
        Some(state)
    }
}

fn build_condition(
    condition_type: &str,
    status: &str,
    reason: Option<&str>,
    message: Option<&str>,
    previous: Option<&PodCondition>,
    now: &str,
) -> PodCondition {
    let last_transition_time = match previous {
        Some(prev) if prev.status == status => prev.last_transition_time.clone(),
        _ => Some(now.to_string()),
    };

    PodCondition {
        condition_type: condition_type.to_string(),
        status: status.to_string(),
        last_transition_time,
        reason: reason.map(|value| value.to_string()),
        message: message.map(|value| value.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::pod::PodStatus;
    use serde_json::Value;

    #[test]
    fn restart_backoff_exponential_delays_cap_at_thirty_two_seconds() {
        let mut backoff = RestartBackoff::default();
        let now = Instant::now();

        let mut expected_delay = 1;
        for attempt in 1..=6 {
            let message = format!("attempt-{attempt}");
            backoff.on_failure(now, message.clone());
            assert_eq!(backoff.attempt, attempt);
            assert_eq!(backoff.last_error.as_deref(), Some(message.as_str()));
            assert!(
                !backoff.should_retry(now),
                "should not retry immediately after failure {attempt}"
            );
            assert!(
                backoff.should_retry(now + Duration::from_secs(expected_delay)),
                "retry should be allowed after {expected_delay}s for attempt {attempt}"
            );
            expected_delay = (expected_delay * 2).min(32);
        }

        // Additional failures stay capped at 32 seconds
        backoff.on_failure(now, "capped".into());
        assert_eq!(backoff.attempt, 7);
        assert!(
            backoff.should_retry(now + Duration::from_secs(32)),
            "retry window should remain capped at 32s"
        );
    }

    #[test]
    fn restart_backoff_state_round_trip_preserves_fields() {
        let mut backoff = RestartBackoff::default();
        let now = Instant::now();
        backoff.on_failure(now, "error".into());

        let state = backoff.to_state();
        assert_eq!(state.attempt, 1);
        assert_eq!(state.last_error.as_deref(), Some("error"));
        assert!(state.next_allowed_at_ms.is_some());

        let restored = RestartBackoff::from_state(state);
        assert_eq!(restored.attempt, 1);
        assert_eq!(restored.last_error.as_deref(), Some("error"));
        assert!(
            !restored.should_retry(Instant::now()),
            "restored backoff should respect pending delay"
        );
    }

    #[test]
    fn restart_backoff_success_resets_attempts_and_error() {
        let mut backoff = RestartBackoff::default();
        let now = Instant::now();
        backoff.on_failure(now, "boom".into());
        assert_eq!(backoff.attempt, 1);
        assert!(backoff.last_error.is_some());

        backoff.on_success(Instant::now());
        assert_eq!(backoff.attempt, 0);
        assert!(backoff.last_error.is_none());
        assert!(
            backoff.should_retry(Instant::now()),
            "successful restart should allow immediate retries"
        );
    }

    #[test]
    fn normalize_namespace_handles_empty_values() {
        assert_eq!(normalize_namespace(Some("prod")), "prod");
        assert_eq!(normalize_namespace(Some("")), "default");
        assert_eq!(normalize_namespace(None), "default");
    }

    #[test]
    fn has_pod_status_changed_detects_differences() {
        let mut status = PodStatus::default();
        assert!(!has_pod_status_changed(None, None));
        assert!(has_pod_status_changed(None, Some(&status)));

        let other = PodStatus {
            phase: Some("Running".into()),
            ..Default::default()
        };
        assert!(has_pod_status_changed(Some(&status), Some(&other)));

        status.phase = Some("Running".into());
        assert!(!has_pod_status_changed(Some(&status), Some(&other)));
    }

    #[test]
    fn pod_key_helpers_expand_namespace() {
        assert_eq!(pod_key(Some("ns"), "pod"), "ns/pod");
        assert_eq!(pod_key(None, "pod"), "default/pod");
        assert_eq!(backoff_key(Some("ns"), "pod"), "/backoff/ns/pod");
        assert_eq!(desired_state_key(Some("ns"), "pod"), "/desired/ns/pod");
    }

    #[test]
    fn pod_state_maps_running_and_stopped_variants() {
        let running = pod_state(&RuntimeStatus::Running, Some("2024-01-01T00:00:00Z"), None)
            .expect("running state");
        assert!(running.contains_key("running"));
        assert_eq!(
            running
                .get("running")
                .and_then(Value::as_object)
                .and_then(|map| map.get("startedAt"))
                .and_then(Value::as_str),
            Some("2024-01-01T00:00:00Z")
        );

        let stopped = pod_state(&RuntimeStatus::Stopped, None, Some("2024-01-02T00:00:00Z"))
            .expect("stopped state");
        assert!(stopped.contains_key("terminated"));
        assert_eq!(
            stopped
                .get("terminated")
                .and_then(Value::as_object)
                .and_then(|map| map.get("finishedAt"))
                .and_then(Value::as_str),
            Some("2024-01-02T00:00:00Z")
        );

        assert!(pod_state(&RuntimeStatus::Creating, None, None).is_none());
    }

    #[test]
    fn build_condition_reuses_transition_time_when_status_unchanged() {
        let previous = PodCondition {
            condition_type: "Ready".into(),
            status: "True".into(),
            last_transition_time: Some("2024-01-01T00:00:00Z".into()),
            reason: Some("Ok".into()),
            message: None,
        };

        let now = "2024-01-02T00:00:00Z";
        let same_status = build_condition("Ready", "True", Some("Ok"), None, Some(&previous), now);
        assert_eq!(
            same_status.last_transition_time.as_deref(),
            Some("2024-01-01T00:00:00Z")
        );

        let new_status = build_condition(
            "Ready",
            "False",
            Some("Error"),
            Some("Failed"),
            Some(&previous),
            now,
        );
        assert_eq!(new_status.last_transition_time.as_deref(), Some(now));
        assert_eq!(new_status.reason.as_deref(), Some("Error"));
        assert_eq!(new_status.message.as_deref(), Some("Failed"));
    }
}
