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
    ReplicaSetPodAction, ReplicaSetPodStatus, ReplicaSetUpdateStrategy,
};
use crate::nanocloud::controller::rollout::{plan_rollout, RolloutPlan, RolloutPolicy};
use crate::nanocloud::controller::runtime::ControllerTarget;
use crate::nanocloud::k8s::daemonset::{
    DaemonSetSpec, DaemonSetUpdateStrategy, DaemonSetUpdateStrategyType,
};
use crate::nanocloud::k8s::statefulset::PodTemplateSpec;
use crate::nanocloud::k8s::store::{self, normalize_namespace};
use crate::nanocloud::logger::log_info;
use crate::nanocloud::util::{is_missing_value_error, Keyspace};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};

const CONTROLLER_KEYSPACE: Keyspace = Keyspace::new("controllers");
const DAEMONSET_PREFIX: &str = "/daemonsets";

pub type DaemonSetFetcher =
    dyn WorkloadFetcher<DaemonSetSpec, DaemonSetObservedState, DaemonSetError>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DaemonSetDesiredState {
    pub revision: String,
    pub max_unavailable: u32,
    pub nodes: Vec<DaemonSetNodePlan>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unavailable_nodes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DaemonSetNodePlan {
    pub node: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_name: Option<String>,
    pub revision: String,
    pub action: ReplicaSetPodAction,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DaemonSetObservedState {
    pub nodes: Vec<DaemonSetNodeStatus>,
    pub pods: Vec<DaemonSetPodStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonSetNodeStatus {
    pub name: String,
    pub ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonSetPodStatus {
    pub name: String,
    pub node: String,
    pub revision: String,
    pub ready: bool,
}

#[derive(Clone)]
struct DaemonSetRolloutPolicy {
    max_unavailable: u32,
    allow_updates: bool,
    nodes: Vec<String>,
}

impl RolloutPolicy for DaemonSetRolloutPolicy {
    fn max_unavailable(&self, _desired_replicas: u32, _strategy: &ReplicaSetUpdateStrategy) -> u32 {
        self.max_unavailable
    }

    fn can_update(&self, _ordinal: u32, pod: &ReplicaSetPodStatus) -> bool {
        if !self.allow_updates {
            return false;
        }
        pod.ready
    }

    fn preferred_node(&self, ordinal: u32) -> Option<String> {
        self.nodes.get(ordinal as usize).cloned()
    }
}

pub struct DaemonSetController {
    name: String,
    namespace: Option<String>,
}

impl DaemonSetController {
    pub fn new(name: impl Into<String>, namespace: Option<String>) -> Self {
        let name = name.into();
        assert!(
            !name.is_empty(),
            "DaemonSetController requires a non-empty DaemonSet name"
        );
        DaemonSetController { name, namespace }
    }

    pub fn desired_state(&self) -> Result<Option<DaemonSetDesiredState>, DaemonSetError> {
        let key = self.state_key();
        match CONTROLLER_KEYSPACE.get(&key) {
            Ok(raw) => {
                let desired = serde_json::from_str(&raw).map_err(DaemonSetError::Serialization)?;
                Ok(Some(desired))
            }
            Err(err) => {
                if is_missing_value_error(err.as_ref()) {
                    Ok(None)
                } else {
                    Err(DaemonSetError::Persistence(err))
                }
            }
        }
    }

    pub fn reconcile(
        &self,
        spec: &DaemonSetSpec,
        observed: &DaemonSetObservedState,
    ) -> Result<DaemonSetDesiredState, DaemonSetError> {
        let revision = compute_revision(&spec.template)?;
        let params = resolve_strategy(&spec.update_strategy, observed)?;

        let nodes = desired_nodes(observed);
        let policy = DaemonSetRolloutPolicy {
            max_unavailable: params.max_unavailable,
            allow_updates: params.allow_updates,
            nodes: nodes.clone(),
        };

        let observed_map = build_observed_map(&nodes, observed);
        let strategy = match spec.update_strategy.r#type {
            DaemonSetUpdateStrategyType::OnDelete => ReplicaSetUpdateStrategy::OnDelete,
            DaemonSetUpdateStrategyType::RollingUpdate => ReplicaSetUpdateStrategy::RollingUpdate {
                max_unavailable: Some(params.max_unavailable),
            },
        };

        let rollout_plan = plan_rollout(
            nodes.len() as u32,
            revision.as_str(),
            &strategy,
            &observed_map,
            &policy,
        );
        let node_plan = build_node_plan(&nodes, observed, &rollout_plan);
        let unavailable_nodes = compute_unavailable_nodes(&nodes, observed);

        let desired_state = DaemonSetDesiredState {
            revision,
            max_unavailable: params.max_unavailable,
            nodes: node_plan,
            unavailable_nodes,
        };

        self.persist_state(&desired_state)?;
        Ok(desired_state)
    }

    fn persist_state(&self, state: &DaemonSetDesiredState) -> Result<(), DaemonSetError> {
        let key = self.state_key();
        let payload = serde_json::to_string_pretty(state).map_err(DaemonSetError::Serialization)?;
        CONTROLLER_KEYSPACE
            .put(&key, &payload)
            .map_err(DaemonSetError::Persistence)?;

        let namespace_label = normalize_namespace(self.namespace.as_deref());
        let node_count = state.nodes.len().to_string();
        log_info(
            "daemonset-controller",
            "persisted desired state",
            &[
                ("namespace", namespace_label.as_str()),
                ("daemonset", self.name.as_str()),
                ("nodes", node_count.as_str()),
                ("revision", state.revision.as_str()),
            ],
        );

        Ok(())
    }

    fn state_key(&self) -> String {
        let ns = normalize_namespace(self.namespace.as_deref());
        format!("{}/{}/{}", DAEMONSET_PREFIX, ns, self.name)
    }

    pub fn clear_state(&self) -> Result<(), DaemonSetError> {
        let key = self.state_key();
        match CONTROLLER_KEYSPACE.delete(&key) {
            Ok(()) => Ok(()),
            Err(err) => {
                if is_missing_value_error(err.as_ref()) {
                    Ok(())
                } else {
                    Err(DaemonSetError::Persistence(err))
                }
            }
        }
    }

    pub fn delete_with_propagation(
        &self,
        propagation: DeletionPropagation,
    ) -> Result<(), DaemonSetError> {
        let state = self.desired_state()?;
        if propagation.cascades() {
            self.remove_managed_pods(state.as_ref())?;
        }
        self.clear_state()?;
        Ok(())
    }

    fn remove_managed_pods(
        &self,
        state: Option<&DaemonSetDesiredState>,
    ) -> Result<(), DaemonSetError> {
        let Some(state) = state else {
            return Ok(());
        };

        let mut seen: HashSet<String> = HashSet::new();
        for plan in state.nodes.iter() {
            if let Some(pod_name) = plan.pod_name.as_ref() {
                if seen.insert(pod_name.clone()) {
                    store::delete_pod_manifest(self.namespace.as_deref(), pod_name)
                        .map_err(DaemonSetError::Persistence)?;
                }
            }
        }

        Ok(())
    }
}

fn desired_nodes(observed: &DaemonSetObservedState) -> Vec<String> {
    let mut nodes: Vec<String> = observed
        .nodes
        .iter()
        .map(|node| node.name.clone())
        .collect();
    let mut additional: Vec<String> = observed.pods.iter().map(|pod| pod.node.clone()).collect();
    nodes.append(&mut additional);
    let mut unique: HashSet<String> = HashSet::new();
    nodes.retain(|node| unique.insert(node.clone()));
    nodes.sort();
    nodes
}

fn build_observed_map(
    nodes: &[String],
    observed: &DaemonSetObservedState,
) -> HashMap<u32, ReplicaSetPodStatus> {
    let mut pods_by_node: HashMap<&str, &DaemonSetPodStatus> = HashMap::new();
    for pod in observed.pods.iter() {
        pods_by_node.insert(pod.node.as_str(), pod);
    }

    let mut map = HashMap::new();
    for (idx, node) in nodes.iter().enumerate() {
        if let Some(pod) = pods_by_node.get(node.as_str()) {
            map.insert(
                idx as u32,
                ReplicaSetPodStatus {
                    name: pod.name.clone(),
                    revision: pod.revision.clone(),
                    ready: pod.ready,
                },
            );
        }
    }
    map
}

fn build_node_plan(
    nodes: &[String],
    observed: &DaemonSetObservedState,
    plan: &RolloutPlan,
) -> Vec<DaemonSetNodePlan> {
    let mut pods_by_node: HashMap<&str, &DaemonSetPodStatus> = HashMap::new();
    for pod in observed.pods.iter() {
        pods_by_node.insert(pod.node.as_str(), pod);
    }

    let mut node_plan = Vec::with_capacity(nodes.len());
    for pod_plan in plan.pods.iter() {
        let target_node = pod_plan
            .preferred_node
            .as_ref()
            .cloned()
            .or_else(|| nodes.get(pod_plan.ordinal as usize).cloned());
        let Some(node_name) = target_node else {
            continue;
        };

        let existing = pods_by_node.get(node_name.as_str());
        node_plan.push(DaemonSetNodePlan {
            node: node_name,
            pod_name: existing.map(|pod| pod.name.clone()),
            revision: pod_plan.revision.clone(),
            action: pod_plan.action.clone(),
        });
    }

    node_plan
}

fn compute_unavailable_nodes(nodes: &[String], observed: &DaemonSetObservedState) -> Vec<String> {
    let ready_nodes: HashSet<&str> = observed
        .nodes
        .iter()
        .filter(|node| node.ready)
        .map(|node| node.name.as_str())
        .collect();
    let ready_pods: HashSet<&str> = observed
        .pods
        .iter()
        .filter(|pod| pod.ready)
        .map(|pod| pod.node.as_str())
        .collect();

    let mut unavailable = Vec::new();
    for node in nodes.iter() {
        let ready = ready_nodes.contains(node.as_str()) && ready_pods.contains(node.as_str());
        if !ready {
            unavailable.push(node.clone());
        }
    }

    unavailable
}

#[derive(Clone, Copy)]
struct StrategyParams {
    max_unavailable: u32,
    allow_updates: bool,
}

fn resolve_strategy(
    strategy: &DaemonSetUpdateStrategy,
    observed: &DaemonSetObservedState,
) -> Result<StrategyParams, DaemonSetError> {
    let node_count = desired_nodes(observed).len() as u32;
    match strategy.r#type {
        DaemonSetUpdateStrategyType::OnDelete => Ok(StrategyParams {
            max_unavailable: 0,
            allow_updates: false,
        }),
        DaemonSetUpdateStrategyType::RollingUpdate => {
            let rolling = strategy.rolling_update.clone().unwrap_or_default();
            let max_unavailable = rolling
                .max_unavailable
                .as_ref()
                .map(|value| value.resolve(node_count.max(1), 1, "maxUnavailable"))
                .transpose()
                .map_err(DaemonSetError::InvalidStrategy)?
                .unwrap_or(1);
            Ok(StrategyParams {
                max_unavailable,
                allow_updates: true,
            })
        }
    }
}

fn compute_revision(template: &PodTemplateSpec) -> Result<String, DaemonSetError> {
    let payload = serde_json::to_vec(template).map_err(DaemonSetError::Serialization)?;
    let digest = Sha1::digest(&payload);
    Ok(format!("{:x}", digest))
}

impl Reconciler for DaemonSetController {
    type Desired = DaemonSetSpec;
    type Observed = DaemonSetObservedState;
    type Plan = DaemonSetDesiredState;
    type Error = DaemonSetError;

    fn kind(&self) -> &'static str {
        "DaemonSet"
    }

    fn fetch(
        &self,
        ctx: &ReconcileContext,
        target: &ControllerTarget,
    ) -> Result<Option<ReconcileData<Self::Desired, Self::Observed>>, Self::Error> {
        let fetcher = ctx
            .dependency::<DependencyHandle<DaemonSetFetcher>>()
            .ok_or_else(|| {
                DaemonSetError::Dependency(
                    "DaemonSet fetcher dependency not registered with runtime".to_string(),
                )
            })?
            .get();

        let ControllerTarget::DaemonSet { .. } = target else {
            return Err(DaemonSetError::Dependency(
                "DaemonSet reconciler received non-DaemonSet target".to_string(),
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
        let ControllerTarget::DaemonSet { .. } = target else {
            return Err(DaemonSetError::Dependency(
                "DaemonSet reconciler received non-DaemonSet target".to_string(),
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
        let ControllerTarget::DaemonSet { .. } = target else {
            return Err(DaemonSetError::Dependency(
                "DaemonSet reconciler received non-DaemonSet target".to_string(),
            ));
        };
        self.persist_state(&plan)
    }
}

#[derive(Debug)]
pub enum DaemonSetError {
    Persistence(Box<dyn Error + Send + Sync>),
    Serialization(serde_json::Error),
    InvalidStrategy(String),
    Dependency(String),
}

impl Display for DaemonSetError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DaemonSetError::Persistence(err) => write!(f, "DaemonSet persistence error: {}", err),
            DaemonSetError::Serialization(err) => {
                write!(f, "DaemonSet serialization error: {}", err)
            }
            DaemonSetError::InvalidStrategy(message) => {
                write!(f, "DaemonSet strategy error: {}", message)
            }
            DaemonSetError::Dependency(message) => {
                write!(f, "DaemonSet dependency error: {}", message)
            }
        }
    }
}

impl Error for DaemonSetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DaemonSetError::Persistence(err) => Some(err.as_ref()),
            DaemonSetError::Serialization(err) => Some(err),
            DaemonSetError::InvalidStrategy(_) | DaemonSetError::Dependency(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::daemonset::{
        DaemonSetRollingUpdate, DaemonSetUpdateStrategy, DaemonSetUpdateStrategyType,
    };
    use crate::nanocloud::k8s::deployment::RollingUpdateValue;
    use crate::nanocloud::k8s::pod::{ContainerSpec, ObjectMeta as PodObjectMeta, PodSpec};
    use crate::nanocloud::k8s::statefulset::PodTemplateSpec;
    use crate::nanocloud::test_support;
    use std::env;
    use std::sync::MutexGuard;
    use tempfile::TempDir;

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

    fn keyspace_guard() -> MutexGuard<'static, ()> {
        test_support::keyspace_lock().lock()
    }

    fn sample_template() -> PodTemplateSpec {
        PodTemplateSpec {
            metadata: PodObjectMeta::default(),
            spec: PodSpec {
                containers: vec![ContainerSpec {
                    name: "ds".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        }
    }

    #[test]
    fn reconcile_creates_pods_for_each_node() {
        let _lock = keyspace_guard();
        let _ctx = TestContext::new();
        let controller = DaemonSetController::new("agent", Some("default".into()));

        let spec = DaemonSetSpec {
            selector: Default::default(),
            template: sample_template(),
            update_strategy: DaemonSetUpdateStrategy::default(),
        };

        let observed = DaemonSetObservedState {
            nodes: vec![
                DaemonSetNodeStatus {
                    name: "node-a".to_string(),
                    ready: true,
                },
                DaemonSetNodeStatus {
                    name: "node-b".to_string(),
                    ready: true,
                },
            ],
            pods: Vec::new(),
        };

        let state = controller.reconcile(&spec, &observed).expect("reconcile");
        assert_eq!(state.nodes.len(), 2);
        assert!(state
            .nodes
            .iter()
            .all(|plan| matches!(plan.action, ReplicaSetPodAction::Create)));
    }

    #[test]
    fn reconcile_rollout_respects_max_unavailable() {
        let _lock = keyspace_guard();
        let _ctx = TestContext::new();
        let controller = DaemonSetController::new("agent", Some("default".into()));

        let mut spec = DaemonSetSpec {
            selector: Default::default(),
            template: sample_template(),
            update_strategy: DaemonSetUpdateStrategy {
                r#type: DaemonSetUpdateStrategyType::RollingUpdate,
                rolling_update: Some(DaemonSetRollingUpdate {
                    max_unavailable: Some(RollingUpdateValue::Int(1)),
                }),
            },
        };

        let observed = DaemonSetObservedState {
            nodes: vec![
                DaemonSetNodeStatus {
                    name: "node-a".to_string(),
                    ready: true,
                },
                DaemonSetNodeStatus {
                    name: "node-b".to_string(),
                    ready: true,
                },
            ],
            pods: vec![
                DaemonSetPodStatus {
                    name: "agent-node-a".to_string(),
                    node: "node-a".to_string(),
                    revision: "old".to_string(),
                    ready: true,
                },
                DaemonSetPodStatus {
                    name: "agent-node-b".to_string(),
                    node: "node-b".to_string(),
                    revision: "old".to_string(),
                    ready: true,
                },
            ],
        };

        let initial = controller.reconcile(&spec, &observed).expect("initial");

        spec.template
            .metadata
            .annotations
            .insert("version".to_string(), "v2".to_string());

        let rollout = controller.reconcile(&spec, &observed).expect("rollout");
        let updates = rollout
            .nodes
            .iter()
            .filter(|plan| matches!(plan.action, ReplicaSetPodAction::Update))
            .count();
        assert_eq!(updates, 1);
        assert_eq!(rollout.max_unavailable, 1);
        assert_ne!(rollout.revision, initial.revision);
    }

    #[test]
    fn build_node_plan_honors_preferred_node() {
        let nodes = vec!["node-a".to_string(), "node-b".to_string()];
        let observed = DaemonSetObservedState {
            nodes: vec![
                DaemonSetNodeStatus {
                    name: "node-a".to_string(),
                    ready: true,
                },
                DaemonSetNodeStatus {
                    name: "node-b".to_string(),
                    ready: true,
                },
            ],
            pods: Vec::new(),
        };
        let plan = RolloutPlan {
            pods: vec![crate::nanocloud::controller::rollout::RolloutPodPlan {
                ordinal: 0,
                revision: "rev-1".to_string(),
                action: ReplicaSetPodAction::Create,
                preferred_node: Some("node-b".to_string()),
            }],
        };

        let assignments = build_node_plan(&nodes, &observed, &plan);
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].node, "node-b");
    }

    #[test]
    fn on_delete_strategy_disables_automatic_updates() {
        let _lock = keyspace_guard();
        let _ctx = TestContext::new();
        let controller = DaemonSetController::new("agent", Some("default".into()));

        let mut spec = DaemonSetSpec {
            selector: Default::default(),
            template: sample_template(),
            update_strategy: DaemonSetUpdateStrategy {
                r#type: DaemonSetUpdateStrategyType::OnDelete,
                rolling_update: None,
            },
        };

        let observed = DaemonSetObservedState {
            nodes: vec![DaemonSetNodeStatus {
                name: "node-a".to_string(),
                ready: true,
            }],
            pods: vec![DaemonSetPodStatus {
                name: "agent-node-a".to_string(),
                node: "node-a".to_string(),
                revision: "old".to_string(),
                ready: true,
            }],
        };

        let initial = controller.reconcile(&spec, &observed).expect("initial");
        spec.template
            .metadata
            .annotations
            .insert("version".to_string(), "v2".to_string());
        let rollout = controller.reconcile(&spec, &observed).expect("rollout");

        assert_ne!(initial.revision, rollout.revision);
        assert!(rollout
            .nodes
            .iter()
            .all(|plan| matches!(plan.action, ReplicaSetPodAction::Retain)));
        assert!(rollout
            .nodes
            .iter()
            .all(|plan| plan.revision == observed.pods[0].revision));
        assert_eq!(rollout.max_unavailable, 0);
    }
}
