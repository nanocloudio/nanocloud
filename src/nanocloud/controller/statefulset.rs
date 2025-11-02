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

use crate::nanocloud::controller::delete::DeletionPropagation;
use crate::nanocloud::controller::reconcile::{
    DependencyHandle, ReconcileContext, ReconcileData, Reconciler, WorkloadFetcher,
};
use crate::nanocloud::controller::replicaset::{
    short_revision_hash, AppliedReplicaSetStrategy, ReplicaSetController, ReplicaSetDesiredState,
    ReplicaSetOwnerRef, ReplicaSetPodAction, ReplicaSetPodDesiredState, ReplicaSetPodStatus,
    ANNOTATION_POD_NAME, ANNOTATION_POD_ORDINAL, LABEL_POD_ORDINAL, LABEL_POD_TEMPLATE_HASH,
    LABEL_REPLICASET_NAME, LABEL_STATEFULSET_NAME,
};
use crate::nanocloud::controller::runtime::{
    ControllerRuntime, ControllerTarget, ControllerWorkItem,
};
use crate::nanocloud::controller::scheduling::ReplicaSetScheduler;
use crate::nanocloud::controller::watch::{ControllerWatchEvent, ControllerWatchManager};
use crate::nanocloud::k8s::pod::Pod;
use crate::nanocloud::k8s::statefulset::{
    PodTemplateSpec, StatefulSet, StatefulSetCondition, StatefulSetSpec, StatefulSetStatus,
    StatefulSetUpdateStrategy,
};
use crate::nanocloud::k8s::store::{self, list_stateful_sets_for, normalize_namespace};
use crate::nanocloud::kubelet::Kubelet;
use crate::nanocloud::logger::{log_debug, log_error, log_info, log_warn};
use crate::nanocloud::observability::metrics::{self, ControllerReconcileResult};
use crate::nanocloud::util::{is_missing_value_error, Keyspace, KeyspaceEventType};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use tokio::runtime::{Handle, Runtime};
use tokio::task::{block_in_place, JoinHandle};

const CONTROLLER_KEYSPACE: Keyspace = Keyspace::new("controllers");
const STATEFULSET_PREFIX: &str = "/statefulsets";
const DEFAULT_MAX_IN_FLIGHT: usize = 1;
const COMPONENT: &str = "statefulset-controller";

pub type StatefulSetFetcher =
    dyn WorkloadFetcher<StatefulSetSpec, Vec<ReplicaSetPodStatus>, StatefulSetError>;

/// Persisted desired state for a StatefulSet reconciliation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatefulSetDesiredState {
    pub revision: String,
    #[serde(default)]
    pub replicaset_name: String,
    pub replicas: u32,
    pub bounded_concurrency: u32,
    pub replica_plan: ReplicaSetDesiredState,
    #[serde(default)]
    pub status: StatefulSetStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ready_ordinals: Vec<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub revision_history: Vec<StatefulSetRevisionRecord>,
    #[serde(skip)]
    pub pruned_replicasets: Vec<String>,
}

/// Stored metadata for a historical StatefulSet revision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatefulSetRevisionRecord {
    pub revision: String,
    #[serde(default)]
    pub replicaset: String,
}

const MAX_REVISION_HISTORY: usize = 10;

/// StatefulSet controller that orchestrates per-ordinal reconciliation by delegating
/// pod management to the ReplicaSet controller with bounded concurrency.
pub struct StatefulSetController {
    name: String,
    namespace: Option<String>,
}

impl StatefulSetController {
    pub fn new(name: impl Into<String>, namespace: Option<String>) -> Self {
        let name = name.into();
        assert!(
            !name.is_empty(),
            "StatefulSetController requires a non-empty StatefulSet name"
        );
        Self { name, namespace }
    }

    /// Loads the desired state previously persisted for this StatefulSet.
    pub fn desired_state(&self) -> Result<Option<StatefulSetDesiredState>, StatefulSetError> {
        let key = self.state_key();
        match CONTROLLER_KEYSPACE.get(&key) {
            Ok(raw) => {
                let desired =
                    serde_json::from_str(&raw).map_err(StatefulSetError::Serialization)?;
                Ok(Some(desired))
            }
            Err(err) => {
                if is_missing_value_error(err.as_ref()) {
                    Ok(None)
                } else {
                    Err(StatefulSetError::Persistence(err))
                }
            }
        }
    }

    /// Computes the bounded ReplicaSet plan for the supplied spec and observed pods.
    pub fn reconcile(
        &self,
        spec: &StatefulSetSpec,
        observed: &[ReplicaSetPodStatus],
    ) -> Result<StatefulSetDesiredState, StatefulSetError> {
        let replicas = if spec.replicas < 0 {
            0
        } else {
            spec.replicas as u32
        };

        let revision = compute_revision(&spec.template)?;
        let template_hash = short_revision_hash(&revision);

        let mut template_metadata = spec.template.metadata.clone();
        template_metadata
            .labels
            .insert(LABEL_POD_TEMPLATE_HASH.to_string(), template_hash.clone());
        template_metadata
            .labels
            .insert(LABEL_STATEFULSET_NAME.to_string(), self.name.clone());

        let previous_state = self.desired_state()?;
        let mut revision_history = previous_state
            .as_ref()
            .map(|state| state.revision_history.clone())
            .unwrap_or_default();
        let mut pruned_replicasets: Vec<String> = Vec::new();

        let mut replicaset_name = derive_replicaset_name(&self.name, &revision);

        if let Some(prev_state) = previous_state.as_ref() {
            if prev_state.revision != revision {
                if let Some(position) = revision_history
                    .iter()
                    .position(|record| record.revision == revision)
                {
                    let rollback_record = revision_history.remove(position);
                    if !rollback_record.replicaset.is_empty() {
                        replicaset_name = rollback_record.replicaset.clone();
                    }
                    let previous_record = StatefulSetRevisionRecord {
                        revision: prev_state.revision.clone(),
                        replicaset: prev_state.replicaset_name.clone(),
                    };
                    if !previous_record.revision.is_empty() && previous_record.revision != revision
                    {
                        revision_history
                            .retain(|record| record.revision != previous_record.revision);
                        revision_history.insert(0, previous_record);
                    }
                } else {
                    let previous_record = StatefulSetRevisionRecord {
                        revision: prev_state.revision.clone(),
                        replicaset: prev_state.replicaset_name.clone(),
                    };
                    if !previous_record.revision.is_empty() {
                        revision_history
                            .retain(|record| record.revision != previous_record.revision);
                        revision_history.insert(0, previous_record);
                    }
                }
            } else if !prev_state.replicaset_name.is_empty() {
                replicaset_name = prev_state.replicaset_name.clone();
            }
        }

        if replicaset_name.is_empty() {
            replicaset_name = derive_replicaset_name(&self.name, &revision);
        }

        if revision_history.len() > MAX_REVISION_HISTORY {
            let overflow = revision_history.split_off(MAX_REVISION_HISTORY);
            for record in overflow {
                if !record.replicaset.is_empty() {
                    pruned_replicasets.push(record.replicaset);
                }
            }
        }

        let replicaset = ReplicaSetController::new_with_pod_prefix(
            replicaset_name.clone(),
            self.namespace.clone(),
            template_metadata,
            self.name.clone(),
        );
        let previously_ready: HashSet<u32> = previous_state
            .as_ref()
            .map(|state| state.ready_ordinals.iter().copied().collect())
            .unwrap_or_default();
        let (mut replica_plan, ready_ordinals) = build_replica_plan(
            &replicaset,
            &self.name,
            replicas,
            &revision,
            observed,
            &previously_ready,
            &spec.update_strategy,
        )?;
        replica_plan.owner = Some(ReplicaSetOwnerRef {
            kind: "StatefulSet".to_string(),
            name: self.name.clone(),
        });
        let ready_replicas = ready_ordinals.len() as i32;
        let current_replicas = observed
            .iter()
            .filter(|pod| parse_ordinal(&self.name, &pod.name).is_some())
            .count() as i32;
        let pending_creates = replica_plan
            .pods
            .iter()
            .filter(|pod| matches!(pod.action, ReplicaSetPodAction::Create))
            .count();
        let pending_updates = replica_plan
            .pods
            .iter()
            .filter(|pod| matches!(pod.action, ReplicaSetPodAction::Update))
            .count();
        let pending_deletes = replica_plan.deletions.len();
        let progressing = pending_creates + pending_updates > 0
            || pending_deletes > 0
            || ready_replicas as u32 != replicas;

        let mut conditions = Vec::new();
        let progress_reason = if progressing {
            if pending_creates + pending_updates > 0 || pending_deletes > 0 {
                "Reconciling"
            } else {
                "WaitingForReadiness"
            }
        } else {
            "Reconciled"
        };
        let progress_message = format!(
            "ready={ready_replicas} current={current_replicas} target={} pending_create={} pending_update={} pending_delete={}",
            replicas,
            pending_creates,
            pending_updates,
            pending_deletes
        );
        conditions.push(StatefulSetCondition {
            condition_type: "Progressing".to_string(),
            status: if progressing {
                "True".to_string()
            } else {
                "False".to_string()
            },
            reason: Some(progress_reason.to_string()),
            message: Some(progress_message),
        });
        let ready_reason = if !progressing && ready_replicas as u32 == replicas {
            "AllReplicasReady"
        } else {
            "InsufficientReplicas"
        };
        conditions.push(StatefulSetCondition {
            condition_type: "Ready".to_string(),
            status: if !progressing && ready_replicas as u32 == replicas {
                "True".to_string()
            } else {
                "False".to_string()
            },
            reason: Some(ready_reason.to_string()),
            message: Some(format!("{} of {} replicas ready", ready_replicas, replicas)),
        });

        let status = StatefulSetStatus {
            ready_replicas: Some(ready_replicas),
            current_replicas: Some(current_replicas),
            conditions,
        };

        Ok(StatefulSetDesiredState {
            revision,
            replicaset_name,
            replicas,
            bounded_concurrency: DEFAULT_MAX_IN_FLIGHT as u32,
            replica_plan,
            status,
            ready_ordinals,
            revision_history,
            pruned_replicasets,
        })
    }

    fn persist_state(&self, state: &StatefulSetDesiredState) -> Result<(), StatefulSetError> {
        let key = self.state_key();
        let payload =
            serde_json::to_string_pretty(state).map_err(StatefulSetError::Serialization)?;
        CONTROLLER_KEYSPACE
            .put(&key, &payload)
            .map_err(StatefulSetError::Persistence)?;

        let namespace_label = normalize_namespace(self.namespace.as_deref());
        let replica_str = state.replicas.to_string();
        let ready_str = state.ready_ordinals.len().to_string();
        let replicaset_label = if state.replicaset_name.is_empty() {
            self.name.as_str()
        } else {
            state.replicaset_name.as_str()
        };
        log_debug(
            "statefulset-controller",
            "persisted desired state",
            &[
                ("namespace", namespace_label.as_str()),
                ("statefulset", self.name.as_str()),
                ("replicaset", replicaset_label),
                ("key", key.as_str()),
                ("revision", state.revision.as_str()),
                ("replicas", replica_str.as_str()),
                ("ready_ordinals", ready_str.as_str()),
            ],
        );

        Ok(())
    }

    fn state_key(&self) -> String {
        let ns = normalize_namespace(self.namespace.as_deref());
        format!("{}/{}/{}", STATEFULSET_PREFIX, ns, self.name)
    }

    pub fn clear_state(&self) -> Result<(), StatefulSetError> {
        let key = self.state_key();
        match CONTROLLER_KEYSPACE.delete(&key) {
            Ok(()) => Ok(()),
            Err(err) => {
                if is_missing_value_error(err.as_ref()) {
                    Ok(())
                } else {
                    Err(StatefulSetError::Persistence(err))
                }
            }
        }
    }

    pub fn delete_with_propagation(
        &self,
        propagation: DeletionPropagation,
    ) -> Result<(), StatefulSetError> {
        let state = self.desired_state()?;
        if propagation.cascades() {
            self.prune_dependents(state.as_ref())?;
        }
        self.clear_state()?;
        Ok(())
    }

    fn prune_dependents(
        &self,
        state: Option<&StatefulSetDesiredState>,
    ) -> Result<(), StatefulSetError> {
        let Some(state) = state else {
            return Ok(());
        };

        let mut replicasets: HashSet<String> = HashSet::new();
        if state.replicaset_name.is_empty() {
            replicasets.insert(self.name.clone());
        } else {
            replicasets.insert(state.replicaset_name.clone());
        }
        for record in state.revision_history.iter() {
            if !record.replicaset.is_empty() {
                replicasets.insert(record.replicaset.clone());
            }
        }

        for replicaset in replicasets {
            ReplicaSetController::clear_state(self.namespace.as_deref(), replicaset.as_str())
                .map_err(StatefulSetError::ReplicaSet)?;
            self.remove_replicaset_pods(replicaset.as_str())?;
        }

        Ok(())
    }

    fn remove_replicaset_pods(&self, replicaset: &str) -> Result<(), StatefulSetError> {
        let namespace_label = normalize_namespace(self.namespace.as_deref());
        let pods = store::list_pod_manifests().map_err(StatefulSetError::Persistence)?;

        for stored in pods {
            let stored_ns = normalize_namespace(stored.namespace.as_deref());
            if stored_ns != namespace_label {
                continue;
            }

            let Some(label) = stored
                .workload
                .metadata
                .labels
                .get(LABEL_REPLICASET_NAME)
                .cloned()
            else {
                continue;
            };

            if label != replicaset {
                continue;
            }

            store::delete_pod_manifest(stored.namespace.as_deref(), stored.name.as_str())
                .map_err(StatefulSetError::Persistence)?;
        }

        Ok(())
    }
}

impl Reconciler for StatefulSetController {
    type Desired = StatefulSetSpec;
    type Observed = Vec<ReplicaSetPodStatus>;
    type Plan = StatefulSetDesiredState;
    type Error = StatefulSetError;

    fn kind(&self) -> &'static str {
        "StatefulSet"
    }

    fn fetch(
        &self,
        ctx: &ReconcileContext,
        target: &ControllerTarget,
    ) -> Result<Option<ReconcileData<Self::Desired, Self::Observed>>, Self::Error> {
        let fetcher = ctx
            .dependency::<DependencyHandle<StatefulSetFetcher>>()
            .ok_or_else(|| {
                StatefulSetError::Dependency(
                    "StatefulSet fetcher dependency not registered with runtime".to_string(),
                )
            })?
            .get();

        let ControllerTarget::StatefulSet { .. } = target else {
            return Err(StatefulSetError::Dependency(
                "StatefulSet reconciler received non-StatefulSet target".to_string(),
            ));
        };

        let desired = match fetcher.desired(target)? {
            Some(spec) => spec,
            None => return Ok(None),
        };
        let observed = fetcher.observed(target)?;
        Ok(Some(ReconcileData { desired, observed }))
    }

    fn diff(
        &self,
        target: &ControllerTarget,
        desired: &Self::Desired,
        observed: &Self::Observed,
    ) -> Result<Self::Plan, Self::Error> {
        let ControllerTarget::StatefulSet { .. } = target else {
            return Err(StatefulSetError::Dependency(
                "StatefulSet reconciler received non-StatefulSet target".to_string(),
            ));
        };
        self.reconcile(desired, observed.as_slice())
    }

    fn apply(
        &self,
        ctx: &ReconcileContext,
        target: &ControllerTarget,
        plan: Self::Plan,
    ) -> Result<(), Self::Error> {
        let ControllerTarget::StatefulSet { namespace, name } = target else {
            return Err(StatefulSetError::Dependency(
                "StatefulSet reconciler received non-StatefulSet target".to_string(),
            ));
        };

        self.persist_state(&plan)?;

        let scheduler = ctx
            .dependency::<DependencyHandle<dyn ReplicaSetScheduler>>()
            .ok_or_else(|| {
                StatefulSetError::Dependency(
                    "ReplicaSet scheduler dependency not registered with runtime".to_string(),
                )
            })?
            .get();

        let replicaset_target_name = if plan.replicaset_name.is_empty() {
            name.clone()
        } else {
            plan.replicaset_name.clone()
        };

        let replica_target = ControllerTarget::ReplicaSet {
            namespace: namespace.clone(),
            name: replicaset_target_name.clone(),
        };

        scheduler
            .schedule(&replica_target, &plan.replica_plan)
            .map_err(StatefulSetError::ReplicaSet)?;

        let namespace_ref = namespace.as_deref();
        let name_ref = name.as_str();
        let ready_count = plan.status.ready_replicas.unwrap_or(0);
        let current_count = plan.status.current_replicas.unwrap_or(0);
        let pending_creates = plan
            .replica_plan
            .pods
            .iter()
            .filter(|pod| matches!(pod.action, ReplicaSetPodAction::Create))
            .count();
        let pending_updates = plan
            .replica_plan
            .pods
            .iter()
            .filter(|pod| matches!(pod.action, ReplicaSetPodAction::Update))
            .count();
        let pending_deletes = plan.replica_plan.deletions.len();
        let progressing = pending_creates + pending_updates > 0
            || pending_deletes > 0
            || ready_count as u32 != plan.replicas;

        metrics::record_statefulset_status(
            namespace_ref,
            name_ref,
            ready_count,
            current_count,
            progressing,
        );

        let namespace_label = normalize_namespace(namespace_ref);
        let ready_str = ready_count.to_string();
        let current_str = current_count.to_string();
        let target_str = plan.replicas.to_string();
        let pending_create_str = pending_creates.to_string();
        let pending_update_str = pending_updates.to_string();
        let pending_delete_str = pending_deletes.to_string();

        log_info(
            "statefulset-controller",
            "applied reconciliation plan",
            &[
                ("namespace", namespace_label.as_str()),
                ("statefulset", name_ref),
                ("replicaset", replicaset_target_name.as_str()),
                ("ready", ready_str.as_str()),
                ("current", current_str.as_str()),
                ("target", target_str.as_str()),
                ("pending_create", pending_create_str.as_str()),
                ("pending_update", pending_update_str.as_str()),
                ("pending_delete", pending_delete_str.as_str()),
                ("progressing", if progressing { "true" } else { "false" }),
            ],
        );

        match store::load(namespace_ref, name_ref) {
            Ok(Some(mut workload)) => {
                let status_changed = workload.status.as_ref() != Some(&plan.status);
                if status_changed {
                    workload.status = Some(plan.status.clone());
                    store::save(namespace_ref, name_ref, &workload)
                        .map_err(StatefulSetError::Persistence)?;
                }
                log_debug(
                    "statefulset-controller",
                    "persisted workload status update",
                    &[
                        ("namespace", namespace_label.as_str()),
                        ("statefulset", name_ref),
                        ("ready", ready_str.as_str()),
                        ("current", current_str.as_str()),
                        ("target", target_str.as_str()),
                        (
                            "status_changed",
                            if status_changed { "true" } else { "false" },
                        ),
                    ],
                );
            }
            Ok(None) => {
                log_warn(
                    "statefulset-controller",
                    "skipping status update for missing StatefulSet",
                    &[
                        ("namespace", namespace_label.as_str()),
                        ("statefulset", name_ref),
                    ],
                );
            }
            Err(err) => return Err(StatefulSetError::Persistence(err)),
        }

        for replicaset in &plan.pruned_replicasets {
            if let Err(err) = ReplicaSetController::clear_state(namespace_ref, replicaset.as_str())
            {
                let error_text = err.to_string();
                log_warn(
                    "statefulset-controller",
                    "Failed to prune superseded ReplicaSet state",
                    &[
                        ("namespace", namespace_label.as_str()),
                        ("statefulset", name_ref),
                        ("replicaset", replicaset.as_str()),
                        ("error", error_text.as_str()),
                    ],
                );
            }
        }

        Ok(())
    }
}

fn build_replica_plan(
    controller: &ReplicaSetController,
    statefulset_name: &str,
    desired_replicas: u32,
    desired_revision: &str,
    observed: &[ReplicaSetPodStatus],
    previously_ready: &HashSet<u32>,
    update_strategy: &StatefulSetUpdateStrategy,
) -> Result<(ReplicaSetDesiredState, Vec<u32>), StatefulSetError> {
    let mut observed_by_ordinal: HashMap<u32, ReplicaSetPodStatus> = HashMap::new();
    let mut deletion_candidates: Vec<(u32, String)> = Vec::new();
    let mut ready_ordinals: BTreeSet<u32> = BTreeSet::new();

    for pod in observed {
        if let Some(ordinal) = parse_ordinal(statefulset_name, &pod.name) {
            if ordinal < desired_replicas {
                if pod.ready {
                    ready_ordinals.insert(ordinal);
                }
                observed_by_ordinal
                    .entry(ordinal)
                    .or_insert_with(|| pod.clone());
            } else {
                deletion_candidates.push((ordinal, pod.name.clone()));
            }
        }
    }

    let mut pods: Vec<ReplicaSetPodDesiredState> = Vec::new();
    let mut in_flight = 0usize;
    let partition = if update_strategy.is_on_delete() {
        desired_replicas
    } else {
        update_strategy.partition().min(desired_replicas)
    };

    for ordinal in 0..desired_replicas {
        let identity = controller.pod_identity(ordinal);
        let observed_pod = observed_by_ordinal.get(&ordinal);
        let needs_create = observed_pod.is_none();
        let was_ready = previously_ready.contains(&ordinal);
        let is_ready = observed_pod.map(|pod| pod.ready).unwrap_or(false);

        let predecessors_ready = predecessors_ready(
            ordinal,
            desired_revision,
            &observed_by_ordinal,
            update_strategy,
            partition,
        );
        let blocked = !predecessors_ready || in_flight >= DEFAULT_MAX_IN_FLIGHT;

        let mut action = ReplicaSetPodAction::Retain;
        let mut target_revision = observed_pod
            .map(|pod| pod.revision.clone())
            .unwrap_or_else(|| desired_revision.to_string());

        if needs_create {
            if !blocked {
                action = ReplicaSetPodAction::Create;
                target_revision = desired_revision.to_string();
                in_flight += 1;
            } else {
                target_revision = desired_revision.to_string();
            }
        } else if let Some(pod) = observed_pod {
            let update_allowed = !update_strategy.is_on_delete() && ordinal >= partition;
            let needs_version_update = pod.revision != desired_revision && update_allowed;
            let needs_restart = was_ready && !is_ready && !needs_create;

            if needs_version_update || needs_restart {
                if !blocked {
                    action = ReplicaSetPodAction::Update;
                    target_revision = if needs_version_update {
                        desired_revision.to_string()
                    } else {
                        pod.revision.clone()
                    };
                    in_flight += 1;
                } else {
                    target_revision = pod.revision.clone();
                }
            } else {
                target_revision = pod.revision.clone();
            }
        }

        pods.push(ReplicaSetPodDesiredState {
            ordinal,
            identity,
            revision: target_revision,
            action,
        });
    }

    deletion_candidates.sort_by(|a, b| b.0.cmp(&a.0));
    let deletions: Vec<String> = deletion_candidates
        .into_iter()
        .take(DEFAULT_MAX_IN_FLIGHT)
        .map(|(_, name)| name)
        .collect();

    Ok((
        ReplicaSetDesiredState {
            revision: desired_revision.to_string(),
            strategy: AppliedReplicaSetStrategy {
                strategy: "RollingUpdate".to_string(),
                max_unavailable: Some(DEFAULT_MAX_IN_FLIGHT as u32),
            },
            pods,
            deletions,
            owner: None,
        },
        ready_ordinals.into_iter().collect(),
    ))
}

fn predecessors_ready(
    ordinal: u32,
    desired_revision: &str,
    observed: &HashMap<u32, ReplicaSetPodStatus>,
    strategy: &StatefulSetUpdateStrategy,
    partition: u32,
) -> bool {
    if ordinal == 0 {
        return true;
    }

    (0..ordinal).all(|idx| {
        observed
            .get(&idx)
            .map(|pod| {
                if !strategy.is_on_delete() && idx >= partition {
                    pod.ready && pod.revision == desired_revision
                } else {
                    pod.ready
                }
            })
            .unwrap_or(false)
    })
}

fn parse_ordinal(statefulset_name: &str, pod_name: &str) -> Option<u32> {
    let prefix = format!("{}-", statefulset_name);
    pod_name.strip_prefix(&prefix)?.parse().ok()
}

fn derive_replicaset_name(workload_name: &str, revision: &str) -> String {
    const MAX_OBJECT_NAME_LEN: usize = 63;
    let hash = short_revision_hash(revision);
    let suffix_len = hash.len() + 1; // hyphen + hash
    let max_prefix_len = MAX_OBJECT_NAME_LEN.saturating_sub(suffix_len);

    let mut prefix = if max_prefix_len > 0 && workload_name.len() > max_prefix_len {
        let truncated = &workload_name[..max_prefix_len];
        let trimmed = truncated.trim_end_matches('-');
        if trimmed.is_empty() {
            truncated.to_string()
        } else {
            trimmed.to_string()
        }
    } else {
        workload_name.to_string()
    };

    if prefix.is_empty() {
        prefix = workload_name.chars().take(max_prefix_len.max(1)).collect();
    }

    format!("{}-{}", prefix, hash)
}

fn compute_revision(template: &PodTemplateSpec) -> Result<String, StatefulSetError> {
    let payload = serde_json::to_vec(template).map_err(StatefulSetError::Serialization)?;
    let digest = Sha1::digest(&payload);
    Ok(format!("{:x}", digest))
}

/// Errors raised by the StatefulSet controller.
#[derive(Debug)]
pub enum StatefulSetError {
    Persistence(Box<dyn Error + Send + Sync>),
    Serialization(serde_json::Error),
    Dependency(String),
    ReplicaSet(crate::nanocloud::controller::replicaset::ReplicaSetError),
}

impl Display for StatefulSetError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StatefulSetError::Persistence(err) => {
                write!(f, "StatefulSet persistence error: {}", err)
            }
            StatefulSetError::Serialization(err) => {
                write!(f, "StatefulSet serialization error: {}", err)
            }
            StatefulSetError::Dependency(message) => {
                write!(f, "StatefulSet dependency error: {}", message)
            }
            StatefulSetError::ReplicaSet(err) => {
                write!(f, "StatefulSet ReplicaSet error: {}", err)
            }
        }
    }
}

impl Error for StatefulSetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            StatefulSetError::Persistence(err) => Some(err.as_ref()),
            StatefulSetError::Serialization(err) => Some(err),
            StatefulSetError::Dependency(_) => None,
            StatefulSetError::ReplicaSet(err) => Some(err),
        }
    }
}

pub fn spawn() -> JoinHandle<()> {
    tokio::spawn(async move {
        let runtime = ControllerRuntime::shared();
        let kubelet = Kubelet::shared();

        register_fetcher(&runtime, kubelet);
        start_statefulset_executor(&runtime);
        bootstrap_existing_statefulsets(&runtime).await;
        watch_statefulset_events(runtime).await;
    })
}

fn register_fetcher(runtime: &Arc<ControllerRuntime>, kubelet: Arc<Kubelet>) {
    let fetcher: Arc<StatefulSetFetcher> = Arc::new(KeyspaceStatefulSetFetcher::new(kubelet));
    let handle = Arc::new(DependencyHandle::new(fetcher));
    let _ = runtime.register_dependency::<DependencyHandle<StatefulSetFetcher>>(handle);
}

fn start_statefulset_executor(runtime: &Arc<ControllerRuntime>) {
    let exec_runtime = Arc::clone(runtime);
    runtime.spawn_executor(move |item| {
        let runtime = Arc::clone(&exec_runtime);
        async move {
            if let ControllerTarget::StatefulSet { namespace, name } = &item.target {
                let controller = StatefulSetController::new(name.clone(), namespace.clone());
                let reconcile_result =
                    controller.reconcile_and_apply(&runtime.context(), &item.target);
                if reconcile_result.is_ok() {
                    metrics::record_controller_reconcile(
                        "statefulset",
                        ControllerReconcileResult::Success,
                    );
                } else {
                    metrics::record_controller_reconcile(
                        "statefulset",
                        ControllerReconcileResult::Error,
                    );
                }
                if let Err(err) = reconcile_result {
                    let error_text = err.to_string();
                    log_error(
                        COMPONENT,
                        "StatefulSet reconciliation failed",
                        &[
                            (
                                "namespace",
                                namespace.as_ref().map(String::as_str).unwrap_or("default"),
                            ),
                            ("statefulset", name.as_str()),
                            ("error", error_text.as_str()),
                        ],
                    );
                }
            }
        }
    });
}

async fn bootstrap_existing_statefulsets(runtime: &Arc<ControllerRuntime>) {
    match tokio::task::spawn_blocking(|| list_stateful_sets_for(None)).await {
        Ok(Ok(statefulsets)) => {
            for set in statefulsets {
                if let Some(name) = set.metadata.name.clone() {
                    enqueue_statefulset(runtime, set.metadata.namespace.clone(), name).await;
                }
            }
        }
        Ok(Err(err)) => {
            let error_text = err.to_string();
            log_error(
                COMPONENT,
                "Failed to enumerate existing StatefulSets",
                &[("error", error_text.as_str())],
            );
        }
        Err(err) => {
            let error_text = err.to_string();
            log_error(
                COMPONENT,
                "StatefulSet bootstrap task failed",
                &[("error", error_text.as_str())],
            );
        }
    }
}

async fn watch_statefulset_events(runtime: Arc<ControllerRuntime>) {
    let manager = ControllerWatchManager::shared();
    let mut subscription = manager.subscribe(STATEFULSET_PREFIX, None);

    while let Some(event) = subscription.recv().await {
        handle_watch_event(&runtime, event).await;
    }
}

async fn handle_watch_event(runtime: &Arc<ControllerRuntime>, event: ControllerWatchEvent) {
    let Some((namespace, name)) = parse_statefulset_key(event.key.as_str()) else {
        log_warn(
            COMPONENT,
            "Received unparseable StatefulSet keyspace event",
            &[("key", event.key.as_str())],
        );
        return;
    };

    match event.event_type {
        KeyspaceEventType::Added | KeyspaceEventType::Modified => {
            enqueue_statefulset(runtime, namespace, name).await;
        }
        KeyspaceEventType::Deleted => {
            cleanup_controller_state(namespace, &name);
        }
    }
}

fn cleanup_controller_state(namespace: Option<String>, name: &str) {
    let controller = StatefulSetController::new(name.to_string(), namespace.clone());
    if let Err(err) = controller.clear_state() {
        let error_text = err.to_string();
        log_warn(
            COMPONENT,
            "Failed to clear StatefulSet controller state",
            &[
                ("namespace", namespace.as_deref().unwrap_or("default")),
                ("statefulset", name),
                ("error", error_text.as_str()),
            ],
        );
    }
}

async fn enqueue_statefulset(
    runtime: &Arc<ControllerRuntime>,
    namespace: Option<String>,
    name: String,
) {
    let item = ControllerWorkItem::statefulset(namespace.as_deref(), name.as_str());
    if let Err(err) = runtime.work_queue().enqueue(item).await {
        let error_text = err.to_string();
        log_warn(
            COMPONENT,
            "Failed to enqueue StatefulSet reconciliation",
            &[
                ("namespace", namespace.as_deref().unwrap_or("default")),
                ("statefulset", name.as_str()),
                ("error", error_text.as_str()),
            ],
        );
    }
}

fn parse_statefulset_key(key: &str) -> Option<(Option<String>, String)> {
    let parts: Vec<&str> = key
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    if parts.len() != 3 || parts[0] != STATEFULSET_PREFIX.trim_start_matches('/') {
        return None;
    }
    let namespace = if parts[1].eq_ignore_ascii_case("default") {
        Some("default".to_string())
    } else {
        Some(parts[1].to_string())
    };
    let name = parts[2].to_string();
    Some((namespace, name))
}

struct KeyspaceStatefulSetFetcher {
    kubelet: Arc<Kubelet>,
}

impl KeyspaceStatefulSetFetcher {
    fn new(kubelet: Arc<Kubelet>) -> Self {
        Self { kubelet }
    }
}

impl WorkloadFetcher<StatefulSetSpec, Vec<ReplicaSetPodStatus>, StatefulSetError>
    for KeyspaceStatefulSetFetcher
{
    fn desired(
        &self,
        target: &ControllerTarget,
    ) -> Result<Option<StatefulSetSpec>, StatefulSetError> {
        let ControllerTarget::StatefulSet { namespace, name } = target else {
            return Err(StatefulSetError::Dependency(
                "StatefulSet fetcher received non-StatefulSet target".to_string(),
            ));
        };

        let workload: Option<StatefulSet> =
            store::load(namespace.as_deref(), name).map_err(StatefulSetError::Persistence)?;

        Ok(workload.map(|set| set.spec))
    }

    fn observed(
        &self,
        target: &ControllerTarget,
    ) -> Result<Vec<ReplicaSetPodStatus>, StatefulSetError> {
        let ControllerTarget::StatefulSet { namespace, name } = target else {
            return Err(StatefulSetError::Dependency(
                "StatefulSet fetcher received non-StatefulSet target".to_string(),
            ));
        };

        let namespace_clone = namespace.clone();
        let name_clone = name.clone();
        let kubelet = Arc::clone(&self.kubelet);

        let future =
            async move { collect_observed_statuses(kubelet, namespace_clone, name_clone).await };

        if let Ok(handle) = Handle::try_current() {
            block_in_place(|| handle.block_on(future))
        } else {
            let runtime = Runtime::new().map_err(|err| {
                StatefulSetError::Dependency(format!(
                    "failed to create Tokio runtime for fetcher: {err}"
                ))
            })?;
            runtime.block_on(future)
        }
    }
}

async fn collect_observed_statuses(
    kubelet: Arc<Kubelet>,
    namespace: Option<String>,
    set_name: String,
) -> Result<Vec<ReplicaSetPodStatus>, StatefulSetError> {
    let pods = kubelet
        .list_pods(namespace.as_deref())
        .await
        .map_err(StatefulSetError::Persistence)?;

    let mut statuses = Vec::new();
    for pod in pods {
        let matches_statefulset = pod
            .metadata
            .labels
            .get(LABEL_STATEFULSET_NAME)
            .map(|value| value == &set_name)
            .unwrap_or(false);
        let matches_legacy = pod
            .metadata
            .labels
            .get(LABEL_REPLICASET_NAME)
            .map(|value| value == &set_name)
            .unwrap_or(false);
        if !matches_statefulset && !matches_legacy {
            continue;
        }

        let pod_name = match pod.metadata.name.clone() {
            Some(name) => name,
            None => continue,
        };

        let revision = pod_revision(&pod)?;
        let ready = pod_ready(&pod);

        statuses.push(ReplicaSetPodStatus {
            name: pod_name,
            revision,
            ready,
        });
    }

    Ok(statuses)
}

fn pod_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| {
            status
                .conditions
                .iter()
                .find(|condition| condition.condition_type == "Ready")
        })
        .map(|condition| condition.status == "True")
        .unwrap_or(false)
}

fn pod_revision(pod: &Pod) -> Result<String, StatefulSetError> {
    let mut metadata = pod.metadata.clone();
    metadata.name = None;
    metadata.namespace = None;
    metadata.resource_version = None;
    metadata.labels.remove(LABEL_REPLICASET_NAME);
    metadata.labels.remove(LABEL_POD_ORDINAL);
    metadata.labels.remove(LABEL_POD_TEMPLATE_HASH);
    metadata.labels.remove(LABEL_STATEFULSET_NAME);
    metadata.annotations.remove(ANNOTATION_POD_NAME);
    metadata.annotations.remove(ANNOTATION_POD_ORDINAL);

    let mut spec = pod.spec.clone();
    spec.node_name = None;

    let template = PodTemplateSpec { metadata, spec };
    let payload = serde_json::to_vec(&template).map_err(StatefulSetError::Serialization)?;
    Ok(format!("{:x}", Sha1::digest(&payload)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::controller::reconcile::DependencyHandle;
    use crate::nanocloud::controller::runtime::{ControllerRuntime, ControllerTarget};
    use crate::nanocloud::controller::scheduling::ReplicaSetScheduler;
    use crate::nanocloud::k8s::pod::{ContainerSpec, ObjectMeta, PodSpec};
    use crate::nanocloud::k8s::statefulset::{
        LabelSelector, PodTemplateSpec, StatefulSet, StatefulSetRollingUpdate,
        StatefulSetUpdateStrategyType,
    };
    use crate::nanocloud::k8s::store;
    use crate::nanocloud::test_support;
    use serial_test::serial;
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::sync::{Arc, Mutex, MutexGuard};
    use tempfile::TempDir;

    fn sample_template() -> PodTemplateSpec {
        PodTemplateSpec {
            metadata: Default::default(),
            spec: PodSpec {
                containers: vec![ContainerSpec {
                    name: "worker".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        }
    }

    fn sample_selector() -> LabelSelector {
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "worker".to_string());
        LabelSelector {
            match_labels: labels,
        }
    }

    fn sample_spec(replicas: i32) -> StatefulSetSpec {
        StatefulSetSpec {
            service_name: "worker".to_string(),
            replicas,
            selector: sample_selector(),
            template: sample_template(),
            update_strategy: Default::default(),
            volume_claim_templates: Vec::new(),
        }
    }

    struct EnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvGuard {
        fn set_path(key: &'static str, value: &std::path::Path) -> Self {
            let previous = env::var(key).ok();
            env::set_var(key, value);
            Self { key, previous }
        }

        fn set_str(key: &'static str, value: &str) -> Self {
            let previous = env::var(key).ok();
            env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(value) = self.previous.as_ref() {
                env::set_var(self.key, value);
            } else {
                env::remove_var(self.key);
            }
        }
    }

    struct TestContext {
        dir: TempDir,
        _keyspace: EnvGuard,
        _lock: EnvGuard,
    }

    impl TestContext {
        fn new() -> Self {
            let dir = tempfile::tempdir().expect("tempdir");
            let keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", dir.path());
            let lock_path = dir.path().join("lockfile");
            let lock_guard =
                EnvGuard::set_str("NANOCLOUD_LOCK_FILE", lock_path.to_string_lossy().as_ref());
            Self {
                dir,
                _keyspace: keyspace_guard,
                _lock: lock_guard,
            }
        }
    }

    fn acquire_test_lock() -> MutexGuard<'static, ()> {
        test_support::keyspace_lock().lock()
    }

    fn controller_with_env() -> (StatefulSetController, TestContext, MutexGuard<'static, ()>) {
        let guard = acquire_test_lock();
        let ctx = TestContext::new();
        let controller = StatefulSetController::new("worker", Some("default".into()));
        (controller, ctx, guard)
    }

    fn get_condition<'a>(
        state: &'a StatefulSetDesiredState,
        condition_type: &str,
    ) -> &'a crate::nanocloud::k8s::statefulset::StatefulSetCondition {
        state
            .status
            .conditions
            .iter()
            .find(|cond| cond.condition_type == condition_type)
            .expect("condition present")
    }

    #[test]
    fn reconcile_limits_creation_to_single_ordinal() {
        let (controller, _ctx, _guard) = controller_with_env();
        let spec = sample_spec(3);
        let first = controller.reconcile(&spec, &[]).expect("reconcile");

        let expected_rs = derive_replicaset_name("worker", &first.revision);
        assert_eq!(first.replicaset_name, expected_rs);
        assert_eq!(first.replica_plan.pods.len(), 3);
        assert!(matches!(
            &first.replica_plan.owner,
            Some(owner) if owner.kind == "StatefulSet" && owner.name == "worker"
        ));
        let first_labels = &first.replica_plan.pods[0].identity.labels;
        assert_eq!(
            first_labels.get(LABEL_STATEFULSET_NAME).map(String::as_str),
            Some("worker")
        );
        assert!(matches!(
            first.replica_plan.pods[0].action,
            ReplicaSetPodAction::Create
        ));
        assert!(first.replica_plan.pods[1..]
            .iter()
            .all(|pod| matches!(pod.action, ReplicaSetPodAction::Retain)));
        assert_eq!(first.status.ready_replicas, Some(0));
        assert_eq!(first.status.current_replicas, Some(0));
        assert_eq!(get_condition(&first, "Progressing").status, "True");
        assert_eq!(get_condition(&first, "Ready").status, "False");

        let observed = vec![ReplicaSetPodStatus {
            name: "worker-0".to_string(),
            revision: first.revision.clone(),
            ready: true,
        }];
        let second = controller.reconcile(&spec, &observed).expect("reconcile");

        assert!(matches!(
            second.replica_plan.pods[1].action,
            ReplicaSetPodAction::Create
        ));
        assert!(second.replica_plan.pods[2..]
            .iter()
            .all(|pod| matches!(pod.action, ReplicaSetPodAction::Retain)));
        assert_eq!(second.status.ready_replicas, Some(1));
        assert_eq!(second.status.current_replicas, Some(1));
        assert_eq!(get_condition(&second, "Progressing").status, "True");
        assert_eq!(get_condition(&second, "Ready").status, "False");
    }

    #[test]
    fn reconcile_updates_one_ordinal_at_a_time() {
        let (controller, _ctx, _guard) = controller_with_env();
        let original = sample_spec(2);
        let mut updated = original.clone();
        updated
            .template
            .metadata
            .annotations
            .insert("version".to_string(), "v2".to_string());

        let original_revision = compute_revision(&original.template).expect("revision");
        let observed = vec![
            ReplicaSetPodStatus {
                name: "worker-0".to_string(),
                revision: original_revision.clone(),
                ready: true,
            },
            ReplicaSetPodStatus {
                name: "worker-1".to_string(),
                revision: original_revision.clone(),
                ready: true,
            },
        ];

        let plan = controller
            .reconcile(&updated, &observed)
            .expect("reconcile");
        assert!(matches!(
            plan.replica_plan.pods[0].action,
            ReplicaSetPodAction::Update
        ));
        assert!(matches!(
            plan.replica_plan.pods[1].action,
            ReplicaSetPodAction::Retain
        ));
        assert_eq!(plan.status.ready_replicas, Some(2));
        assert_eq!(plan.status.current_replicas, Some(2));
        assert_eq!(get_condition(&plan, "Progressing").status, "True");
        assert_eq!(get_condition(&plan, "Ready").status, "False");
    }

    #[test]
    fn rolling_update_respects_partition() {
        let (controller, _ctx, _guard) = controller_with_env();
        let original = sample_spec(3);
        let original_revision = compute_revision(&original.template).expect("revision");

        let mut updated = original.clone();
        updated
            .template
            .metadata
            .annotations
            .insert("version".to_string(), "v2".to_string());
        updated.update_strategy.rolling_update =
            Some(StatefulSetRollingUpdate { partition: Some(1) });
        let updated_revision = compute_revision(&updated.template).expect("revision");

        let observed: Vec<ReplicaSetPodStatus> = (0..3)
            .map(|ordinal| ReplicaSetPodStatus {
                name: format!("worker-{ordinal}"),
                revision: original_revision.clone(),
                ready: true,
            })
            .collect();

        let plan = controller
            .reconcile(&updated, &observed)
            .expect("reconcile partitioned update");

        assert_eq!(plan.replica_plan.revision, updated_revision);
        assert!(matches!(
            plan.replica_plan.pods[0].action,
            ReplicaSetPodAction::Retain
        ));
        assert_eq!(plan.replica_plan.pods[0].revision, original_revision);
        assert!(matches!(
            plan.replica_plan.pods[1].action,
            ReplicaSetPodAction::Update
        ));
        assert_eq!(
            plan.replica_plan.pods[1].revision,
            plan.replica_plan.revision
        );
        assert_eq!(plan.status.ready_replicas, Some(3));
        assert_eq!(get_condition(&plan, "Progressing").status, "True");
        assert_eq!(get_condition(&plan, "Ready").status, "False");
    }

    #[test]
    fn on_delete_skips_automatic_updates() {
        let (controller, _ctx, _guard) = controller_with_env();
        let original = sample_spec(2);
        let original_revision = compute_revision(&original.template).expect("revision");

        let mut on_delete_spec = original.clone();
        on_delete_spec
            .template
            .metadata
            .annotations
            .insert("version".to_string(), "v2".to_string());
        on_delete_spec.update_strategy.r#type = StatefulSetUpdateStrategyType::OnDelete;
        on_delete_spec.update_strategy.rolling_update = None;
        let updated_revision = compute_revision(&on_delete_spec.template).expect("revision");

        let observed = vec![
            ReplicaSetPodStatus {
                name: "worker-0".to_string(),
                revision: original_revision.clone(),
                ready: true,
            },
            ReplicaSetPodStatus {
                name: "worker-1".to_string(),
                revision: original_revision.clone(),
                ready: true,
            },
        ];

        let plan = controller
            .reconcile(&on_delete_spec, &observed)
            .expect("reconcile ondelete");

        assert_eq!(plan.replica_plan.revision, updated_revision);
        assert!(plan
            .replica_plan
            .pods
            .iter()
            .all(|pod| matches!(pod.action, ReplicaSetPodAction::Retain)));
        for pod in &plan.replica_plan.pods {
            assert_eq!(pod.revision, original_revision);
        }
        assert!(plan.replica_plan.deletions.is_empty());
        assert_eq!(plan.status.ready_replicas, Some(2));
        assert_eq!(get_condition(&plan, "Progressing").status, "False");
        assert_eq!(get_condition(&plan, "Ready").status, "True");
    }

    #[test]
    fn reconcile_scales_down_highest_ordinal_first() {
        let (controller, _ctx, _guard) = controller_with_env();
        let spec = sample_spec(1);
        let revision = compute_revision(&spec.template).expect("revision");
        let observed = vec![
            ReplicaSetPodStatus {
                name: "worker-0".to_string(),
                revision: revision.clone(),
                ready: true,
            },
            ReplicaSetPodStatus {
                name: "worker-1".to_string(),
                revision: revision.clone(),
                ready: true,
            },
            ReplicaSetPodStatus {
                name: "worker-2".to_string(),
                revision,
                ready: true,
            },
        ];

        let plan = controller.reconcile(&spec, &observed).expect("reconcile");
        assert_eq!(plan.replica_plan.deletions, vec!["worker-2".to_string()]);
        assert_eq!(plan.status.ready_replicas, Some(1));
        assert_eq!(plan.status.current_replicas, Some(3));
        assert_eq!(get_condition(&plan, "Progressing").status, "True");
        assert_eq!(get_condition(&plan, "Ready").status, "False");
    }

    #[test]
    fn reconcile_tracks_revision_history() {
        let (controller, _ctx, _guard) = controller_with_env();
        let spec = sample_spec(1);

        let initial = controller.reconcile(&spec, &[]).expect("initial reconcile");
        assert!(initial.revision_history.is_empty());
        controller
            .persist_state(&initial)
            .expect("persist initial state");

        let mut updated = spec.clone();
        updated
            .template
            .metadata
            .annotations
            .insert("version".to_string(), "v2".to_string());
        let observed = vec![ReplicaSetPodStatus {
            name: "worker-0".to_string(),
            revision: initial.revision.clone(),
            ready: true,
        }];

        let plan = controller
            .reconcile(&updated, &observed)
            .expect("reconcile updated spec");
        assert_eq!(plan.revision_history.len(), 1);
        assert_eq!(plan.revision_history[0].revision, initial.revision);
        assert_eq!(plan.revision_history[0].replicaset, initial.replicaset_name);
        assert!(plan.pruned_replicasets.is_empty());
    }

    #[test]
    fn reconcile_promotes_previous_revision_on_rollback() {
        let (controller, _ctx, _guard) = controller_with_env();
        let original = sample_spec(1);

        let baseline = controller.reconcile(&original, &[]).expect("baseline");
        controller
            .persist_state(&baseline)
            .expect("persist baseline");

        let mut upgraded = original.clone();
        upgraded
            .template
            .metadata
            .annotations
            .insert("version".to_string(), "v2".to_string());
        let observed_baseline = vec![ReplicaSetPodStatus {
            name: "worker-0".to_string(),
            revision: baseline.revision.clone(),
            ready: true,
        }];
        let rollout = controller
            .reconcile(&upgraded, &observed_baseline)
            .expect("rollout");
        controller.persist_state(&rollout).expect("persist rollout");

        let observed_rollout = vec![ReplicaSetPodStatus {
            name: "worker-0".to_string(),
            revision: rollout.revision.clone(),
            ready: true,
        }];
        let rollback = controller
            .reconcile(&original, &observed_rollout)
            .expect("rollback");

        assert_eq!(rollback.revision, baseline.revision);
        assert_eq!(rollback.replicaset_name, baseline.replicaset_name);
        assert_eq!(
            rollback
                .revision_history
                .first()
                .map(|record| record.revision.clone()),
            Some(rollout.revision.clone())
        );
        assert!(rollback.pruned_replicasets.is_empty());
        assert!(matches!(
            rollback.replica_plan.pods[0].action,
            ReplicaSetPodAction::Update
        ));
        assert_eq!(rollback.replica_plan.pods[0].revision, baseline.revision);
    }

    #[test]
    fn reconcile_prunes_revision_history_beyond_limit() {
        let (controller, _ctx, _guard) = controller_with_env();
        let mut spec = sample_spec(1);

        let mut plan = controller.reconcile(&spec, &[]).expect("initial reconcile");
        controller.persist_state(&plan).expect("persist initial");
        let mut previous_revision = plan.revision.clone();

        for idx in 0..(MAX_REVISION_HISTORY + 2) {
            spec.template
                .metadata
                .annotations
                .insert("version".to_string(), format!("rev-{idx}"));
            let observed = vec![ReplicaSetPodStatus {
                name: "worker-0".to_string(),
                revision: previous_revision.clone(),
                ready: true,
            }];
            plan = controller
                .reconcile(&spec, &observed)
                .expect("reconcile new revision");
            controller.persist_state(&plan).expect("persist revision");
            previous_revision = plan.revision.clone();
        }

        assert!(
            plan.revision_history.len() <= MAX_REVISION_HISTORY,
            "history length exceeds configured maximum"
        );
        assert!(
            !plan.pruned_replicasets.is_empty(),
            "expected pruned ReplicaSets when history limit exceeded"
        );
    }

    #[derive(Default)]
    struct RecordingScheduler {
        last: Mutex<Option<ReplicaSetDesiredState>>,
    }

    impl ReplicaSetScheduler for RecordingScheduler {
        fn schedule(
            &self,
            _: &ControllerTarget,
            plan: &ReplicaSetDesiredState,
        ) -> Result<(), crate::nanocloud::controller::replicaset::ReplicaSetError> {
            let mut guard = self.last.lock().expect("scheduler lock");
            guard.replace(plan.clone());
            Ok(())
        }
    }

    #[test]
    fn apply_persists_state_and_schedules_plan() {
        let (controller, _ctx, _guard) = controller_with_env();
        let runtime = ControllerRuntime::new();

        let scheduler_impl = Arc::new(RecordingScheduler::default());
        let scheduler_trait: Arc<dyn ReplicaSetScheduler> = scheduler_impl.clone();
        let scheduler_handle = Arc::new(DependencyHandle::new(scheduler_trait));
        let _ = runtime
            .register_dependency::<DependencyHandle<dyn ReplicaSetScheduler>>(scheduler_handle);

        let ctx = runtime.context();
        let target = ControllerTarget::StatefulSet {
            namespace: Some("default".to_string()),
            name: "worker".to_string(),
        };

        let spec = sample_spec(1);
        let plan = controller
            .reconcile(&spec, &[])
            .expect("statefulset reconcile");
        controller
            .apply(&ctx, &target, plan.clone())
            .expect("apply statefulset");

        let actual_root = crate::nanocloud::Config::Keyspace.get_path();

        let desired_path = actual_root
            .join("controllers")
            .join("statefulsets")
            .join("default")
            .join("worker")
            .join("_value_");
        let stored: StatefulSetDesiredState =
            serde_json::from_str(&fs::read_to_string(&desired_path).expect("read desired state"))
                .expect("parse desired state");
        assert_eq!(stored, plan, "desired state persisted");

        let recorded = scheduler_impl
            .last
            .lock()
            .expect("scheduler record")
            .clone()
            .expect("plan recorded");
        assert_eq!(recorded.revision, plan.replica_plan.revision);
    }

    #[test]
    fn reconcile_recreates_evicted_pod_after_ready() {
        let (controller, _ctx, _guard) = controller_with_env();
        let runtime = ControllerRuntime::new();

        let scheduler_impl = Arc::new(RecordingScheduler::default());
        let scheduler_trait: Arc<dyn ReplicaSetScheduler> = scheduler_impl.clone();
        let scheduler_handle = Arc::new(DependencyHandle::new(scheduler_trait));
        let _ = runtime
            .register_dependency::<DependencyHandle<dyn ReplicaSetScheduler>>(scheduler_handle);

        let ctx = runtime.context();
        let target = ControllerTarget::StatefulSet {
            namespace: Some("default".to_string()),
            name: "worker".to_string(),
        };

        let spec = sample_spec(1);
        let revision = compute_revision(&spec.template).expect("revision");
        let ready_status = vec![ReplicaSetPodStatus {
            name: "worker-0".to_string(),
            revision: revision.clone(),
            ready: true,
        }];

        let plan = controller
            .reconcile(&spec, &ready_status)
            .expect("initial reconcile");
        assert_eq!(plan.ready_ordinals, vec![0]);
        assert_eq!(plan.status.ready_replicas, Some(1));
        assert_eq!(plan.status.current_replicas, Some(1));
        assert_eq!(get_condition(&plan, "Progressing").status, "False");
        controller
            .apply(&ctx, &target, plan)
            .expect("persist ready state");

        let controller = StatefulSetController::new("worker", Some("default".into()));
        let recovery = controller
            .reconcile(&spec, &[])
            .expect("recover missing pod");
        assert!(matches!(
            recovery.replica_plan.pods[0].action,
            ReplicaSetPodAction::Create
        ));
        assert!(recovery.ready_ordinals.is_empty());
        assert_eq!(recovery.status.ready_replicas, Some(0));
        assert_eq!(recovery.status.current_replicas, Some(0));
        assert_eq!(get_condition(&recovery, "Progressing").status, "True");
        assert_eq!(get_condition(&recovery, "Ready").status, "False");
    }

    #[test]
    fn reconcile_restarts_regressed_pod_after_restart() {
        let (controller, _ctx, _guard) = controller_with_env();
        let runtime = ControllerRuntime::new();

        let scheduler_impl = Arc::new(RecordingScheduler::default());
        let scheduler_trait: Arc<dyn ReplicaSetScheduler> = scheduler_impl.clone();
        let scheduler_handle = Arc::new(DependencyHandle::new(scheduler_trait));
        let _ = runtime
            .register_dependency::<DependencyHandle<dyn ReplicaSetScheduler>>(scheduler_handle);

        let ctx = runtime.context();
        let target = ControllerTarget::StatefulSet {
            namespace: Some("default".to_string()),
            name: "worker".to_string(),
        };

        let spec = sample_spec(1);
        let revision = compute_revision(&spec.template).expect("revision");
        let ready_status = vec![ReplicaSetPodStatus {
            name: "worker-0".to_string(),
            revision: revision.clone(),
            ready: true,
        }];

        let plan = controller
            .reconcile(&spec, &ready_status)
            .expect("initial reconcile");
        assert_eq!(plan.status.ready_replicas, Some(1));
        assert_eq!(plan.status.current_replicas, Some(1));
        assert_eq!(get_condition(&plan, "Progressing").status, "False");
        assert_eq!(get_condition(&plan, "Ready").status, "True");
        controller
            .apply(&ctx, &target, plan)
            .expect("persist ready state");

        let degraded_status = vec![ReplicaSetPodStatus {
            name: "worker-0".to_string(),
            revision,
            ready: false,
        }];

        let controller = StatefulSetController::new("worker", Some("default".into()));
        let recovery = controller
            .reconcile(&spec, &degraded_status)
            .expect("recover regressed pod");
        assert!(matches!(
            recovery.replica_plan.pods[0].action,
            ReplicaSetPodAction::Update
        ));
        assert!(recovery.ready_ordinals.is_empty());
        assert_eq!(recovery.status.ready_replicas, Some(0));
        assert_eq!(recovery.status.current_replicas, Some(1));
        assert_eq!(get_condition(&recovery, "Progressing").status, "True");
        assert_eq!(get_condition(&recovery, "Ready").status, "False");
    }

    #[test]
    #[serial]
    fn apply_updates_statefulset_status_in_store() {
        let (controller, test_ctx, _guard) = controller_with_env();
        let runtime = ControllerRuntime::new();

        let scheduler_impl = Arc::new(RecordingScheduler::default());
        let scheduler_trait: Arc<dyn ReplicaSetScheduler> = scheduler_impl.clone();
        let scheduler_handle = Arc::new(DependencyHandle::new(scheduler_trait));
        let _ = runtime
            .register_dependency::<DependencyHandle<dyn ReplicaSetScheduler>>(scheduler_handle);

        let ctx = runtime.context();
        let target = ControllerTarget::StatefulSet {
            namespace: Some("default".to_string()),
            name: "worker".to_string(),
        };

        let spec = sample_spec(1);
        let keyspace_root = test_ctx.dir.path().join("keyspace-store");
        std::fs::create_dir_all(&keyspace_root).expect("create temp keyspace dir");
        let _override = EnvGuard::set_path("NANOCLOUD_KEYSPACE", &keyspace_root);
        let metadata = ObjectMeta {
            name: Some("worker".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        };
        let workload = StatefulSet::new(metadata, spec.clone());
        store::save(Some("default"), "worker", &workload).expect("persist workload");
        let current_keyspace = crate::nanocloud::Config::Keyspace.get_path();

        let revision = compute_revision(&spec.template).expect("revision");
        let observed = vec![ReplicaSetPodStatus {
            name: "worker-0".to_string(),
            revision,
            ready: true,
        }];

        let plan = controller.reconcile(&spec, &observed).expect("reconcile");
        controller
            .apply(&ctx, &target, plan.clone())
            .expect("apply statefulset");

        let stored_path = current_keyspace
            .join("k8s")
            .join("statefulsets")
            .join("default")
            .join("worker")
            .join("_value_");
        let stored: StatefulSet = serde_json::from_str(
            &fs::read_to_string(&stored_path).expect("read statefulset workload"),
        )
        .expect("parse statefulset workload");
        assert_eq!(stored.status, Some(plan.status));
    }
}
