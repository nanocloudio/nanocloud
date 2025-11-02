#![allow(dead_code)]

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
    short_revision_hash, ReplicaSetController, ReplicaSetError, ReplicaSetPodStatus,
    ReplicaSetUpdateStrategy, LABEL_REPLICASET_NAME,
};
use crate::nanocloud::controller::rollout::{plan_rollout, RolloutPlan, RolloutPolicy};
use crate::nanocloud::controller::runtime::ControllerTarget;
use crate::nanocloud::k8s::deployment::{
    DeploymentSpec, DeploymentStrategyType, RollingUpdateDeployment,
};
use crate::nanocloud::k8s::store::{self, normalize_namespace};
use crate::nanocloud::logger::{log_debug, log_info};
use crate::nanocloud::util::{is_missing_value_error, Keyspace};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};

const CONTROLLER_KEYSPACE: Keyspace = Keyspace::new("controllers");
const DEPLOYMENT_PREFIX: &str = "/deployments";
const DEFAULT_HISTORY_LIMIT: usize = 10;

pub type DeploymentFetcher =
    dyn WorkloadFetcher<DeploymentSpec, DeploymentObservedState, DeploymentError>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeploymentDesiredState {
    pub revision: String,
    pub replicas: u32,
    pub max_surge: u32,
    pub max_unavailable: u32,
    pub active_replicaset: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replica_sets: Vec<DeploymentReplicaSetPlan>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub revision_history: Vec<DeploymentRevisionRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pruned_replicasets: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeploymentReplicaSetPlan {
    pub name: String,
    pub revision: String,
    pub target_replicas: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available_replicas: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeploymentRevisionRecord {
    pub revision: String,
    #[serde(default)]
    pub replicaset: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeploymentObservedState {
    pub pods: Vec<ReplicaSetPodStatus>,
    pub replica_sets: Vec<DeploymentReplicaSetStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentReplicaSetStatus {
    pub name: String,
    pub revision: String,
    pub replicas: u32,
    pub ready_replicas: u32,
    pub available_replicas: u32,
}

#[derive(Clone, Copy)]
struct RollingUpdateParams {
    max_surge: u32,
    max_unavailable: u32,
}

struct DeploymentRolloutPolicy {
    params: RollingUpdateParams,
}

impl RolloutPolicy for DeploymentRolloutPolicy {
    fn max_unavailable(&self, _desired_replicas: u32, _strategy: &ReplicaSetUpdateStrategy) -> u32 {
        self.params.max_unavailable
    }

    fn max_surge(&self, _desired_replicas: u32, _strategy: &ReplicaSetUpdateStrategy) -> u32 {
        self.params.max_surge
    }
}

pub struct DeploymentController {
    name: String,
    namespace: Option<String>,
}

impl DeploymentController {
    pub fn new(name: impl Into<String>, namespace: Option<String>) -> Self {
        let name = name.into();
        assert!(
            !name.is_empty(),
            "DeploymentController requires a non-empty Deployment name"
        );
        DeploymentController { name, namespace }
    }

    pub fn desired_state(&self) -> Result<Option<DeploymentDesiredState>, DeploymentError> {
        let key = self.state_key();
        match CONTROLLER_KEYSPACE.get(&key) {
            Ok(raw) => {
                let desired = serde_json::from_str(&raw).map_err(DeploymentError::Serialization)?;
                Ok(Some(desired))
            }
            Err(err) => {
                if is_missing_value_error(err.as_ref()) {
                    Ok(None)
                } else {
                    Err(DeploymentError::Persistence(err))
                }
            }
        }
    }

    pub fn reconcile(
        &self,
        spec: &DeploymentSpec,
        observed: &DeploymentObservedState,
    ) -> Result<DeploymentDesiredState, DeploymentError> {
        let desired_replicas = if spec.replicas < 0 {
            0
        } else {
            spec.replicas as u32
        };

        let revision = compute_revision(&spec.template)?;
        let mut active_replicaset = derive_replicaset_name(&self.name, &revision);

        let previous_state = self.desired_state()?;
        let mut revision_history = previous_state
            .as_ref()
            .map(|state| state.revision_history.clone())
            .unwrap_or_default();
        let mut pruned_replicasets: Vec<String> = Vec::new();

        if let Some(previous) = previous_state.as_ref() {
            if previous.revision == revision {
                active_replicaset = previous.active_replicaset.clone();
            } else {
                if let Some(position) = revision_history
                    .iter()
                    .position(|record| record.revision == revision)
                {
                    let record = revision_history.remove(position);
                    if !record.replicaset.is_empty() {
                        active_replicaset = record.replicaset.clone();
                    }
                }
                if !previous.revision.is_empty() {
                    let prior = DeploymentRevisionRecord {
                        revision: previous.revision.clone(),
                        replicaset: previous.active_replicaset.clone(),
                    };
                    revision_history.retain(|record| record.revision != prior.revision);
                    revision_history.insert(0, prior);
                }
            }
        }

        if active_replicaset.is_empty() {
            active_replicaset = derive_replicaset_name(&self.name, &revision);
        }

        let history_limit = normalize_history_limit(spec.revision_history_limit);
        if revision_history.len() > history_limit {
            let overflow = revision_history.split_off(history_limit);
            for record in overflow {
                if !record.replicaset.is_empty() {
                    pruned_replicasets.push(record.replicaset);
                }
            }
        }

        let params = resolve_strategy(&spec.strategy, desired_replicas)?;
        let policy = DeploymentRolloutPolicy { params };

        let strategy = ReplicaSetUpdateStrategy::RollingUpdate {
            max_unavailable: Some(params.max_unavailable),
        };

        let observed_map = build_observed_map(observed, desired_replicas);
        let rollout_plan = plan_rollout(
            desired_replicas,
            revision.as_str(),
            &strategy,
            &observed_map,
            &policy,
        );
        let replica_sets = build_replica_plan(
            &self.name,
            &revision,
            &active_replicaset,
            observed,
            &rollout_plan,
        );

        let desired_state = DeploymentDesiredState {
            revision,
            replicas: desired_replicas,
            max_surge: params.max_surge,
            max_unavailable: params.max_unavailable,
            active_replicaset,
            replica_sets,
            revision_history,
            pruned_replicasets,
        };

        self.persist_state(&desired_state)?;
        Ok(desired_state)
    }

    fn persist_state(&self, state: &DeploymentDesiredState) -> Result<(), DeploymentError> {
        let key = self.state_key();
        let payload =
            serde_json::to_string_pretty(state).map_err(DeploymentError::Serialization)?;
        CONTROLLER_KEYSPACE
            .put(&key, &payload)
            .map_err(DeploymentError::Persistence)?;

        let namespace_label = normalize_namespace(self.namespace.as_deref());
        let replica_str = state.replicas.to_string();
        let replicasets = state.replica_sets.len().to_string();

        log_debug(
            "deployment-controller",
            "persisted desired state",
            &[
                ("namespace", namespace_label.as_str()),
                ("deployment", self.name.as_str()),
                ("replicas", replica_str.as_str()),
                ("replica_sets", replicasets.as_str()),
                ("revision", state.revision.as_str()),
            ],
        );

        Ok(())
    }

    fn state_key(&self) -> String {
        let ns = normalize_namespace(self.namespace.as_deref());
        format!("{}/{}/{}", DEPLOYMENT_PREFIX, ns, self.name)
    }

    pub fn clear_state(&self) -> Result<(), DeploymentError> {
        let key = self.state_key();
        match CONTROLLER_KEYSPACE.delete(&key) {
            Ok(()) => Ok(()),
            Err(err) => {
                if is_missing_value_error(err.as_ref()) {
                    Ok(())
                } else {
                    Err(DeploymentError::Persistence(err))
                }
            }
        }
    }

    pub fn delete_with_propagation(
        &self,
        propagation: DeletionPropagation,
    ) -> Result<(), DeploymentError> {
        let state = self.desired_state()?;
        if propagation.cascades() {
            self.prune_dependents(state.as_ref())?;
        }
        self.clear_state()?;
        Ok(())
    }

    fn prune_dependents(
        &self,
        state: Option<&DeploymentDesiredState>,
    ) -> Result<(), DeploymentError> {
        let Some(state) = state else {
            return Ok(());
        };

        let mut replicasets: HashSet<String> = HashSet::new();
        if !state.active_replicaset.is_empty() {
            replicasets.insert(state.active_replicaset.clone());
        }
        for plan in state.replica_sets.iter() {
            if !plan.name.is_empty() {
                replicasets.insert(plan.name.clone());
            }
        }
        for record in state.revision_history.iter() {
            if !record.replicaset.is_empty() {
                replicasets.insert(record.replicaset.clone());
            }
        }
        for pruned in state.pruned_replicasets.iter() {
            if !pruned.is_empty() {
                replicasets.insert(pruned.clone());
            }
        }

        for replicaset in replicasets {
            ReplicaSetController::clear_state(self.namespace.as_deref(), replicaset.as_str())
                .map_err(DeploymentError::ReplicaSet)?;
            self.remove_replicaset_pods(replicaset.as_str())?;
        }

        Ok(())
    }

    fn remove_replicaset_pods(&self, replicaset: &str) -> Result<(), DeploymentError> {
        let namespace_label = normalize_namespace(self.namespace.as_deref());
        let pods = store::list_pod_manifests().map_err(DeploymentError::Persistence)?;

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
                .map_err(DeploymentError::Persistence)?;
        }

        Ok(())
    }
}

fn build_replica_plan(
    deployment_name: &str,
    desired_revision: &str,
    active_replicaset: &str,
    observed: &DeploymentObservedState,
    plan: &RolloutPlan,
) -> Vec<DeploymentReplicaSetPlan> {
    let mut replicas_by_revision: HashMap<String, u32> = HashMap::new();
    for pod in plan.pods.iter() {
        *replicas_by_revision
            .entry(pod.revision.clone())
            .or_default() += 1;
    }

    let mut observed_by_revision: HashMap<String, &DeploymentReplicaSetStatus> = HashMap::new();
    for status in observed.replica_sets.iter() {
        observed_by_revision.insert(status.revision.clone(), status);
    }

    let mut plans: Vec<DeploymentReplicaSetPlan> = Vec::new();
    let mut covered: HashSet<String> = HashSet::new();

    for (revision, replicas) in replicas_by_revision.into_iter() {
        let name = if revision == desired_revision {
            active_replicaset.to_string()
        } else if let Some(status) = observed_by_revision.get(&revision) {
            status.name.clone()
        } else {
            derive_replicaset_name(deployment_name, &revision)
        };

        let available = observed_by_revision
            .get(&revision)
            .map(|status| status.available_replicas);

        plans.push(DeploymentReplicaSetPlan {
            name: name.clone(),
            revision: revision.clone(),
            target_replicas: replicas,
            available_replicas: available,
        });
        covered.insert(name);
    }

    for status in observed.replica_sets.iter() {
        if covered.contains(&status.name) {
            continue;
        }
        plans.push(DeploymentReplicaSetPlan {
            name: status.name.clone(),
            revision: status.revision.clone(),
            target_replicas: 0,
            available_replicas: Some(status.available_replicas),
        });
    }

    plans.sort_by(|a, b| a.name.cmp(&b.name));
    plans
}

fn build_observed_map(
    observed: &DeploymentObservedState,
    desired_replicas: u32,
) -> HashMap<u32, ReplicaSetPodStatus> {
    let mut pods = observed.pods.clone();
    pods.sort_by(|a, b| a.name.cmp(&b.name));

    let mut map = HashMap::new();
    for (idx, pod) in pods.into_iter().enumerate() {
        if idx >= desired_replicas as usize {
            break;
        }
        map.insert(idx as u32, pod);
    }
    map
}

fn normalize_history_limit(limit: Option<i32>) -> usize {
    match limit {
        Some(value) if value > 0 => value as usize,
        _ => DEFAULT_HISTORY_LIMIT,
    }
}

fn resolve_strategy(
    strategy: &crate::nanocloud::k8s::deployment::DeploymentStrategy,
    desired_replicas: u32,
) -> Result<RollingUpdateParams, DeploymentError> {
    match strategy.r#type {
        DeploymentStrategyType::Recreate => Ok(RollingUpdateParams {
            max_surge: 0,
            max_unavailable: desired_replicas,
        }),
        DeploymentStrategyType::RollingUpdate => {
            let rolling = strategy
                .rolling_update
                .clone()
                .unwrap_or(RollingUpdateDeployment {
                    max_unavailable: None,
                    max_surge: None,
                });
            let max_unavailable = rolling
                .max_unavailable
                .as_ref()
                .map(|value| {
                    value.resolve(
                        desired_replicas,
                        if desired_replicas == 0 { 0 } else { 1 },
                        "maxUnavailable",
                    )
                })
                .transpose()
                .map_err(DeploymentError::InvalidStrategy)?
                .unwrap_or(if desired_replicas == 0 { 0 } else { 1 });
            let max_surge = rolling
                .max_surge
                .as_ref()
                .map(|value| value.resolve(desired_replicas, 1, "maxSurge"))
                .transpose()
                .map_err(DeploymentError::InvalidStrategy)?
                .unwrap_or(1);
            Ok(RollingUpdateParams {
                max_surge,
                max_unavailable,
            })
        }
    }
}

fn derive_replicaset_name(workload_name: &str, revision: &str) -> String {
    const MAX_OBJECT_NAME_LEN: usize = 63;
    let hash = short_revision_hash(revision);
    let suffix_len = hash.len() + 1;
    let max_prefix_len = MAX_OBJECT_NAME_LEN.saturating_sub(suffix_len);
    let prefix = workload_name
        .chars()
        .take(max_prefix_len)
        .collect::<String>();
    if prefix.is_empty() {
        hash
    } else {
        format!("{prefix}-{hash}")
    }
}

fn compute_revision(
    template: &crate::nanocloud::k8s::statefulset::PodTemplateSpec,
) -> Result<String, DeploymentError> {
    let payload = serde_json::to_vec(template).map_err(DeploymentError::Serialization)?;
    let digest = Sha1::digest(&payload);
    Ok(format!("{:x}", digest))
}

impl Reconciler for DeploymentController {
    type Desired = DeploymentSpec;
    type Observed = DeploymentObservedState;
    type Plan = DeploymentDesiredState;
    type Error = DeploymentError;

    fn kind(&self) -> &'static str {
        "Deployment"
    }

    fn fetch(
        &self,
        ctx: &ReconcileContext,
        target: &ControllerTarget,
    ) -> Result<Option<ReconcileData<Self::Desired, Self::Observed>>, Self::Error> {
        let fetcher = ctx
            .dependency::<DependencyHandle<DeploymentFetcher>>()
            .ok_or_else(|| {
                DeploymentError::Dependency(
                    "Deployment fetcher dependency not registered with runtime".to_string(),
                )
            })?
            .get();

        let ControllerTarget::Deployment { .. } = target else {
            return Err(DeploymentError::Dependency(
                "Deployment reconciler received non-Deployment target".to_string(),
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
        let ControllerTarget::Deployment { .. } = target else {
            return Err(DeploymentError::Dependency(
                "Deployment reconciler received non-Deployment target".to_string(),
            ));
        };
        self.reconcile(desired, observed)
    }

    fn apply(
        &self,
        _ctx: &ReconcileContext,
        target: &ControllerTarget,
        plan: Self::Plan,
    ) -> Result<(), Self::Error> {
        let ControllerTarget::Deployment { namespace, name } = target else {
            return Err(DeploymentError::Dependency(
                "Deployment reconciler received non-Deployment target".to_string(),
            ));
        };
        self.persist_state(&plan)?;

        let namespace_label = normalize_namespace(namespace.as_deref());
        let replicas_str = plan.replicas.to_string();
        log_info(
            "deployment-controller",
            "applied reconciliation plan",
            &[
                ("namespace", namespace_label.as_str()),
                ("deployment", name.as_str()),
                ("replicas", replicas_str.as_str()),
                ("revision", plan.revision.as_str()),
            ],
        );

        Ok(())
    }
}

#[derive(Debug)]
pub enum DeploymentError {
    Persistence(Box<dyn Error + Send + Sync>),
    Serialization(serde_json::Error),
    InvalidStrategy(String),
    Dependency(String),
    ReplicaSet(ReplicaSetError),
}

impl Display for DeploymentError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DeploymentError::Persistence(err) => {
                write!(f, "Deployment persistence error: {}", err)
            }
            DeploymentError::Serialization(err) => {
                write!(f, "Deployment serialization error: {}", err)
            }
            DeploymentError::InvalidStrategy(message) => {
                write!(f, "Deployment strategy error: {}", message)
            }
            DeploymentError::Dependency(message) => {
                write!(f, "Deployment dependency error: {}", message)
            }
            DeploymentError::ReplicaSet(err) => {
                write!(f, "Deployment ReplicaSet error: {}", err)
            }
        }
    }
}

impl Error for DeploymentError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DeploymentError::Persistence(err) => Some(err.as_ref()),
            DeploymentError::Serialization(err) => Some(err),
            DeploymentError::InvalidStrategy(_)
            | DeploymentError::Dependency(_)
            | DeploymentError::ReplicaSet(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::deployment::{
        DeploymentStrategy, DeploymentStrategyType, RollingUpdateDeployment, RollingUpdateValue,
    };
    use crate::nanocloud::k8s::pod::{ContainerSpec, ObjectMeta as PodObjectMeta, PodSpec};
    use crate::nanocloud::k8s::statefulset::PodTemplateSpec;
    use crate::nanocloud::test_support;
    use std::env;
    use std::sync::MutexGuard;
    use tempfile::TempDir;

    fn keyspace_guard() -> MutexGuard<'static, ()> {
        test_support::keyspace_lock().lock()
    }

    fn sample_template() -> PodTemplateSpec {
        PodTemplateSpec {
            metadata: PodObjectMeta::default(),
            spec: PodSpec {
                containers: vec![ContainerSpec {
                    name: "web".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
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
        _dir: TempDir,
        _keyspace: EnvGuard,
        _lock: EnvGuard,
    }

    impl TestContext {
        fn new() -> Self {
            let dir = tempfile::tempdir().expect("tempdir");
            let keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", dir.path());
            let lock_path = dir.path().join("lockfile");
            let lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", lock_path.as_path());
            Self {
                _dir: dir,
                _keyspace: keyspace_guard,
                _lock: lock_guard,
            }
        }
    }

    #[test]
    fn reconcile_new_deployment_creates_initial_plan() {
        let _lock = keyspace_guard();
        let _ctx = TestContext::new();
        let controller = DeploymentController::new("web", Some("default".into()));

        let spec = DeploymentSpec {
            replicas: 3,
            selector: Default::default(),
            template: sample_template(),
            strategy: DeploymentStrategy::default(),
            revision_history_limit: Some(2),
        };

        let state = controller
            .reconcile(&spec, &DeploymentObservedState::default())
            .expect("reconcile");

        assert_eq!(state.replicas, 3);
        assert_eq!(state.replica_sets.len(), 1);
        let plan = &state.replica_sets[0];
        assert_eq!(plan.target_replicas, 3);
        assert_eq!(plan.revision, state.revision);
    }

    #[test]
    fn reconcile_preserves_revision_history_with_limit() {
        let _lock = keyspace_guard();
        let _ctx = TestContext::new();
        let controller = DeploymentController::new("api", Some("default".into()));
        let mut spec = DeploymentSpec {
            replicas: 2,
            selector: Default::default(),
            template: sample_template(),
            strategy: DeploymentStrategy::default(),
            revision_history_limit: Some(1),
        };

        let first_state = controller
            .reconcile(&spec, &DeploymentObservedState::default())
            .expect("initial reconcile");

        spec.template
            .metadata
            .annotations
            .insert("version".to_string(), "v2".to_string());

        let observed = DeploymentObservedState {
            pods: vec![
                ReplicaSetPodStatus {
                    name: "api-0".to_string(),
                    revision: first_state.revision.clone(),
                    ready: true,
                },
                ReplicaSetPodStatus {
                    name: "api-1".to_string(),
                    revision: first_state.revision.clone(),
                    ready: true,
                },
            ],
            replica_sets: vec![DeploymentReplicaSetStatus {
                name: first_state.active_replicaset.clone(),
                revision: first_state.revision.clone(),
                replicas: 2,
                ready_replicas: 2,
                available_replicas: 2,
            }],
        };

        let second_state = controller
            .reconcile(&spec, &observed)
            .expect("second reconcile");

        assert_eq!(second_state.revision_history.len(), 1);
        assert_eq!(
            second_state.revision_history[0].revision,
            first_state.revision
        );
    }

    #[test]
    fn resolve_strategy_parses_percentage_values() {
        let params = resolve_strategy(
            &DeploymentStrategy {
                r#type: DeploymentStrategyType::RollingUpdate,
                rolling_update: Some(RollingUpdateDeployment {
                    max_unavailable: Some(RollingUpdateValue::String("50%".into())),
                    max_surge: Some(RollingUpdateValue::String("25%".into())),
                }),
            },
            4,
        )
        .expect("strategy");

        assert_eq!(params.max_unavailable, 2);
        assert_eq!(params.max_surge, 1);
    }
}
