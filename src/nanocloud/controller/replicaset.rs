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

use crate::nanocloud::controller::reconcile::{
    DependencyHandle, ReconcileContext, ReconcileData, Reconciler, WorkloadFetcher,
};
use crate::nanocloud::controller::rollout::{plan_rollout, DefaultRolloutPolicy};
use crate::nanocloud::controller::runtime::ControllerTarget;
use crate::nanocloud::controller::scheduling::ReplicaSetScheduler;
use crate::nanocloud::k8s::pod::ObjectMeta;
use crate::nanocloud::k8s::statefulset::PodTemplateSpec;
use crate::nanocloud::k8s::store::normalize_namespace;
use crate::nanocloud::logger::log_debug;
use crate::nanocloud::util::{is_missing_value_error, Keyspace};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::cmp::min;
use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::fmt::{Display, Formatter};

const CONTROLLER_KEYSPACE: Keyspace = Keyspace::new("controllers");
const REPLICASET_PREFIX: &str = "/replicasets";
const TEMPLATE_HASH_LENGTH: usize = 10;

pub type ReplicaSetFetcher =
    dyn WorkloadFetcher<ReplicaSetSpec, Vec<ReplicaSetPodStatus>, ReplicaSetError>;

/// Label applied to pods to reference their owning ReplicaSet name.
pub const LABEL_REPLICASET_NAME: &str = "nanocloud.io/replicaset";
/// Label applied to pods to capture their stable ordinal index.
pub const LABEL_POD_ORDINAL: &str = "nanocloud.io/pod-ordinal";
/// Label applied to pods and ReplicaSets to distinguish template revisions.
pub const LABEL_POD_TEMPLATE_HASH: &str = "pod-template-hash";
/// Label tying pods back to their parent StatefulSet for discovery.
pub const LABEL_STATEFULSET_NAME: &str = "statefulset.kubernetes.io/name";
/// Annotation applied to pods to surface the computed name.
pub const ANNOTATION_POD_NAME: &str = "nanocloud.io/pod-name";
/// Annotation applied to pods to make the ordinal easily queryable.
pub const ANNOTATION_POD_ORDINAL: &str = "nanocloud.io/pod-ordinal";

/// Produces the canonical short hash used in ReplicaSet names and labels.
pub fn short_revision_hash(revision: &str) -> String {
    let end = min(TEMPLATE_HASH_LENGTH, revision.len());
    revision[..end].to_string()
}

/// Computed identity information for a pod managed by a ReplicaSet.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaSetPodIdentity {
    pub name: String,
    pub namespace: Option<String>,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
}

impl ReplicaSetPodIdentity {
    /// Converts the identity into an `ObjectMeta` suitable for constructing Pods.
    pub fn into_metadata(self) -> ObjectMeta {
        ObjectMeta {
            name: Some(self.name),
            namespace: self.namespace,
            labels: self.labels,
            annotations: self.annotations,
            resource_version: None,
        }
    }
}

/// ReplicaSet update behaviour supported by the controller.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ReplicaSetUpdateStrategy {
    RollingUpdate {
        #[serde(default)]
        max_unavailable: Option<u32>,
    },
    OnDelete,
}

impl Default for ReplicaSetUpdateStrategy {
    fn default() -> Self {
        Self::RollingUpdate {
            max_unavailable: Some(1),
        }
    }
}

/// ReplicaSet desired configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaSetSpec {
    pub replicas: u32,
    pub template: PodTemplateSpec,
    #[serde(default)]
    pub update_strategy: ReplicaSetUpdateStrategy,
}

/// Observed pod status passed into reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaSetPodStatus {
    pub name: String,
    pub revision: String,
    pub ready: bool,
}

/// Desired pod action emitted by reconciliation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReplicaSetPodAction {
    Create,
    Update,
    Retain,
}

/// Target state for a managed pod.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaSetPodDesiredState {
    pub ordinal: u32,
    pub identity: ReplicaSetPodIdentity,
    pub revision: String,
    pub action: ReplicaSetPodAction,
}

/// Strategy details persisted with the desired state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppliedReplicaSetStrategy {
    pub strategy: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_unavailable: Option<u32>,
}

/// Reference to the workload that owns the ReplicaSet.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaSetOwnerRef {
    pub kind: String,
    pub name: String,
}

/// Persisted desired ReplicaSet state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaSetDesiredState {
    pub revision: String,
    pub strategy: AppliedReplicaSetStrategy,
    pub pods: Vec<ReplicaSetPodDesiredState>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deletions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<ReplicaSetOwnerRef>,
}

impl ReplicaSetDesiredState {
    fn with_strategy(strategy: &ReplicaSetUpdateStrategy, revision: String) -> Self {
        Self {
            revision,
            strategy: AppliedReplicaSetStrategy::from(strategy),
            pods: Vec::new(),
            deletions: Vec::new(),
            owner: None,
        }
    }
}

impl AppliedReplicaSetStrategy {
    fn from(strategy: &ReplicaSetUpdateStrategy) -> Self {
        match strategy {
            ReplicaSetUpdateStrategy::RollingUpdate { max_unavailable } => Self {
                strategy: "RollingUpdate".to_string(),
                max_unavailable: Some(max_unavailable.unwrap_or(1)),
            },
            ReplicaSetUpdateStrategy::OnDelete => Self {
                strategy: "OnDelete".to_string(),
                max_unavailable: None,
            },
        }
    }
}

/// Minimal ReplicaSet controller that drives desired state persistence.
pub struct ReplicaSetController {
    name: String,
    namespace: Option<String>,
    template_metadata: ObjectMeta,
    pod_prefix: String,
}

impl ReplicaSetController {
    /// Creates a new ReplicaSet controller.
    pub fn new(
        name: impl Into<String>,
        namespace: Option<String>,
        template_metadata: ObjectMeta,
    ) -> Self {
        let name = name.into();
        Self::new_with_pod_prefix(name.clone(), namespace, template_metadata, name)
    }

    /// Creates a new ReplicaSet controller with an explicit pod name prefix.
    pub fn new_with_pod_prefix(
        name: impl Into<String>,
        namespace: Option<String>,
        template_metadata: ObjectMeta,
        pod_prefix: impl Into<String>,
    ) -> Self {
        let name = name.into();
        let pod_prefix = pod_prefix.into();
        assert!(
            !name.is_empty(),
            "ReplicaSetController requires a non-empty ReplicaSet name"
        );
        assert!(
            !pod_prefix.is_empty(),
            "ReplicaSetController requires a non-empty pod prefix"
        );

        Self {
            name,
            namespace,
            template_metadata,
            pod_prefix,
        }
    }

    /// Renders the canonical pod name for the supplied ordinal.
    pub fn pod_name(&self, ordinal: u32) -> String {
        format!("{}-{}", self.pod_prefix, ordinal)
    }

    /// Produces the identity (name, labels, annotations) for the provided ordinal.
    pub fn pod_identity(&self, ordinal: u32) -> ReplicaSetPodIdentity {
        self.pod_identity_from_metadata(&self.template_metadata, ordinal)
    }

    /// Produces the identity using the provided template metadata.
    pub fn pod_identity_from_metadata(
        &self,
        template_metadata: &ObjectMeta,
        ordinal: u32,
    ) -> ReplicaSetPodIdentity {
        self.build_identity(template_metadata, ordinal)
    }

    /// Loads the desired state previously persisted for this ReplicaSet.
    pub fn desired_state(&self) -> Result<Option<ReplicaSetDesiredState>, ReplicaSetError> {
        let key = self.state_key();
        match CONTROLLER_KEYSPACE.get(&key) {
            Ok(raw) => {
                let desired = serde_json::from_str(&raw).map_err(ReplicaSetError::Serialization)?;
                Ok(Some(desired))
            }
            Err(err) => {
                if is_missing_value_error(err.as_ref()) {
                    Ok(None)
                } else {
                    Err(ReplicaSetError::Persistence(err))
                }
            }
        }
    }

    /// Reconciles the ReplicaSet against observed pods and persists the desired state.
    pub fn reconcile(
        &self,
        spec: &ReplicaSetSpec,
        observed_pods: &[ReplicaSetPodStatus],
    ) -> Result<ReplicaSetDesiredState, ReplicaSetError> {
        let revision = compute_revision(&spec.template)?;
        let mut desired = ReplicaSetDesiredState::with_strategy(&spec.update_strategy, revision);

        let desired_replicas = spec.replicas;
        let identity_metadata = spec.template.metadata.clone();

        let mut observed_by_ordinal: HashMap<u32, ReplicaSetPodStatus> = HashMap::new();
        let mut deletions = BTreeSet::new();

        for pod in observed_pods {
            if let Some(ordinal) = parse_ordinal(&self.name, &pod.name) {
                if ordinal >= desired_replicas {
                    deletions.insert(pod.name.clone());
                } else {
                    observed_by_ordinal.insert(ordinal, pod.clone());
                }
            }
        }

        let rollout_plan = plan_rollout(
            desired_replicas,
            desired.revision.as_str(),
            &spec.update_strategy,
            &observed_by_ordinal,
            &DefaultRolloutPolicy,
        );

        for plan in rollout_plan.pods.iter() {
            let identity = self.pod_identity_from_metadata(&identity_metadata, plan.ordinal);

            desired.pods.push(ReplicaSetPodDesiredState {
                ordinal: plan.ordinal,
                identity,
                revision: plan.revision.clone(),
                action: plan.action.clone(),
            });
        }

        desired.deletions = deletions.into_iter().collect();

        self.persist_state(&desired)?;
        Ok(desired)
    }

    fn persist_state(&self, state: &ReplicaSetDesiredState) -> Result<(), ReplicaSetError> {
        let key = self.state_key();
        let payload =
            serde_json::to_string_pretty(state).map_err(ReplicaSetError::Serialization)?;
        CONTROLLER_KEYSPACE
            .put(&key, &payload)
            .map_err(ReplicaSetError::Persistence)?;

        let namespace_label = normalize_namespace(self.namespace.as_deref());
        let pod_target = state.pods.len().to_string();
        let deletions = state.deletions.len().to_string();
        log_debug(
            "replicaset-controller",
            "persisted desired state",
            &[
                ("namespace", namespace_label.as_str()),
                ("replicaset", self.name.as_str()),
                ("key", key.as_str()),
                ("revision", state.revision.as_str()),
                ("pods", pod_target.as_str()),
                ("deletions", deletions.as_str()),
            ],
        );

        Ok(())
    }

    pub fn clear_state(namespace: Option<&str>, name: &str) -> Result<(), ReplicaSetError> {
        let key = replicaset_state_key(namespace, name);
        match CONTROLLER_KEYSPACE.delete(&key) {
            Ok(()) => Ok(()),
            Err(err) => {
                if is_missing_value_error(err.as_ref()) {
                    Ok(())
                } else {
                    Err(ReplicaSetError::Persistence(err))
                }
            }
        }
    }

    fn state_key(&self) -> String {
        replicaset_state_key(self.namespace.as_deref(), &self.name)
    }

    fn build_identity(
        &self,
        template_metadata: &ObjectMeta,
        ordinal: u32,
    ) -> ReplicaSetPodIdentity {
        let name = self.pod_name(ordinal);
        let ordinal_string = ordinal.to_string();

        let mut labels = template_metadata.labels.clone();
        labels.insert(LABEL_REPLICASET_NAME.to_string(), self.name.clone());
        labels.insert(LABEL_POD_ORDINAL.to_string(), ordinal_string.clone());

        let mut annotations = template_metadata.annotations.clone();
        annotations.insert(ANNOTATION_POD_NAME.to_string(), name.clone());
        annotations.insert(ANNOTATION_POD_ORDINAL.to_string(), ordinal_string);

        ReplicaSetPodIdentity {
            name,
            namespace: self.namespace.clone(),
            labels,
            annotations,
        }
    }
}

impl Reconciler for ReplicaSetController {
    type Desired = ReplicaSetSpec;
    type Observed = Vec<ReplicaSetPodStatus>;
    type Plan = ReplicaSetDesiredState;
    type Error = ReplicaSetError;

    fn kind(&self) -> &'static str {
        "ReplicaSet"
    }

    fn fetch(
        &self,
        ctx: &ReconcileContext,
        target: &ControllerTarget,
    ) -> Result<Option<ReconcileData<Self::Desired, Self::Observed>>, Self::Error> {
        let fetcher = ctx
            .dependency::<DependencyHandle<ReplicaSetFetcher>>()
            .ok_or_else(|| {
                ReplicaSetError::Dependency(
                    "ReplicaSet fetcher dependency not registered with runtime".to_string(),
                )
            })?
            .get();

        let ControllerTarget::ReplicaSet { .. } = target else {
            return Err(ReplicaSetError::Dependency(
                "ReplicaSet reconciler received non-ReplicaSet target".to_string(),
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
        let ControllerTarget::ReplicaSet { .. } = target else {
            return Err(ReplicaSetError::Dependency(
                "ReplicaSet reconciler received non-ReplicaSet target".to_string(),
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
        let (namespace, name) = match target {
            ControllerTarget::ReplicaSet { namespace, name } => (namespace, name),
            _ => {
                return Err(ReplicaSetError::Dependency(
                    "ReplicaSet reconciler received non-ReplicaSet target".to_string(),
                ))
            }
        };
        self.persist_state(&plan)?;

        let scheduler = ctx
            .dependency::<DependencyHandle<dyn ReplicaSetScheduler>>()
            .ok_or_else(|| {
                ReplicaSetError::Dependency(
                    "ReplicaSet scheduler dependency not registered with runtime".to_string(),
                )
            })?
            .get();

        let namespace_label = normalize_namespace(namespace.as_deref());
        let create_count = plan
            .pods
            .iter()
            .filter(|pod| matches!(pod.action, ReplicaSetPodAction::Create))
            .count();
        let update_count = plan
            .pods
            .iter()
            .filter(|pod| matches!(pod.action, ReplicaSetPodAction::Update))
            .count();
        let retain_count = plan
            .pods
            .iter()
            .filter(|pod| matches!(pod.action, ReplicaSetPodAction::Retain))
            .count();
        let delete_count = plan.deletions.len();

        scheduler.schedule(target, &plan)?;

        let create_str = create_count.to_string();
        let update_str = update_count.to_string();
        let retain_str = retain_count.to_string();
        let delete_str = delete_count.to_string();
        log_debug(
            "replicaset-controller",
            "scheduled replica plan",
            &[
                ("namespace", namespace_label.as_str()),
                ("replicaset", name.as_str()),
                ("create", create_str.as_str()),
                ("update", update_str.as_str()),
                ("retain", retain_str.as_str()),
                ("delete", delete_str.as_str()),
            ],
        );
        Ok(())
    }
}

fn replicaset_state_key(namespace: Option<&str>, name: &str) -> String {
    let ns = normalize_namespace(namespace);
    format!("{}/{}/{}", REPLICASET_PREFIX, ns, name)
}

fn parse_ordinal(replica_name: &str, pod_name: &str) -> Option<u32> {
    let prefix = format!("{}-", replica_name);
    pod_name.strip_prefix(&prefix)?.parse().ok()
}

fn compute_revision(template: &PodTemplateSpec) -> Result<String, ReplicaSetError> {
    let payload = serde_json::to_vec(template).map_err(ReplicaSetError::Serialization)?;
    let digest = Sha1::digest(&payload);
    Ok(format!("{:x}", digest))
}

/// Errors raised by the ReplicaSet controller.
#[derive(Debug)]
pub enum ReplicaSetError {
    Persistence(Box<dyn Error + Send + Sync>),
    Serialization(serde_json::Error),
    Dependency(String),
}

impl Display for ReplicaSetError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplicaSetError::Persistence(err) => {
                write!(f, "ReplicaSet persistence error: {}", err)
            }
            ReplicaSetError::Serialization(err) => {
                write!(f, "ReplicaSet serialization error: {}", err)
            }
            ReplicaSetError::Dependency(message) => {
                write!(f, "ReplicaSet dependency error: {}", message)
            }
        }
    }
}

impl Error for ReplicaSetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ReplicaSetError::Persistence(err) => Some(err.as_ref()),
            ReplicaSetError::Serialization(err) => Some(err),
            ReplicaSetError::Dependency(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::controller::reconcile::DependencyHandle;
    use crate::nanocloud::controller::runtime::{ControllerRuntime, ControllerTarget};
    use crate::nanocloud::controller::scheduling::ReplicaSetScheduler;
    use crate::nanocloud::k8s::pod::{ContainerSpec, PodSpec};
    use crate::nanocloud::test_support;
    use std::env;
    use std::sync::{Arc, Mutex, MutexGuard};
    use tempfile::TempDir;

    fn base_metadata() -> ObjectMeta {
        let mut metadata = ObjectMeta::default();
        metadata
            .labels
            .insert("app".to_string(), "nanocloud".to_string());
        metadata.annotations.insert(
            "description".to_string(),
            "Nanocloud worker template".to_string(),
        );
        metadata
    }

    fn sample_template() -> PodTemplateSpec {
        PodTemplateSpec {
            metadata: base_metadata(),
            spec: PodSpec {
                containers: vec![ContainerSpec {
                    name: "worker".to_string(),
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
        _dir: TempDir,
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
                _dir: dir,
                _keyspace: keyspace_guard,
                _lock: lock_guard,
            }
        }
    }

    #[test]
    fn pod_name_uses_replica_set_name_and_ordinal() {
        let controller =
            ReplicaSetController::new("worker", Some("default".into()), base_metadata());
        assert_eq!(controller.pod_name(0), "worker-0");
        assert_eq!(controller.pod_name(42), "worker-42");
    }

    #[test]
    fn pod_identity_preserves_template_metadata_and_sets_labels() {
        let controller =
            ReplicaSetController::new("worker", Some("default".into()), base_metadata());
        let identity = controller.pod_identity(7);

        assert_eq!(identity.name, "worker-7");
        assert_eq!(identity.namespace.as_deref(), Some("default"));
        assert_eq!(identity.labels.get("app"), Some(&"nanocloud".to_string()));
        assert_eq!(
            identity
                .labels
                .get(LABEL_REPLICASET_NAME)
                .map(String::as_str),
            Some("worker")
        );
        assert_eq!(
            identity.labels.get(LABEL_POD_ORDINAL).map(String::as_str),
            Some("7")
        );
        assert_eq!(
            identity.annotations.get("description").map(String::as_str),
            Some("Nanocloud worker template")
        );
        assert_eq!(
            identity
                .annotations
                .get(ANNOTATION_POD_NAME)
                .map(String::as_str),
            Some("worker-7")
        );
        assert_eq!(
            identity
                .annotations
                .get(ANNOTATION_POD_ORDINAL)
                .map(String::as_str),
            Some("7")
        );
    }

    #[test]
    fn pod_identity_into_metadata_derives_object_meta() {
        let controller = ReplicaSetController::new("db", None, ObjectMeta::default());
        let metadata = controller.pod_identity(3).into_metadata();

        assert_eq!(metadata.name.as_deref(), Some("db-3"));
        assert!(metadata.namespace.is_none());
        assert_eq!(
            metadata.labels.get(LABEL_POD_ORDINAL).map(String::as_str),
            Some("3")
        );
        assert_eq!(
            metadata
                .annotations
                .get(ANNOTATION_POD_NAME)
                .map(String::as_str),
            Some("db-3")
        );
    }

    fn acquire_test_lock() -> MutexGuard<'static, ()> {
        test_support::keyspace_lock().lock()
    }

    fn controller_with_env() -> (ReplicaSetController, TestContext, MutexGuard<'static, ()>) {
        let guard = acquire_test_lock();
        let ctx = TestContext::new();
        let controller =
            ReplicaSetController::new("worker", Some("default".into()), base_metadata());
        (controller, ctx, guard)
    }

    struct StaticFetcher {
        desired: Option<ReplicaSetSpec>,
        observed: Vec<ReplicaSetPodStatus>,
    }

    impl WorkloadFetcher<ReplicaSetSpec, Vec<ReplicaSetPodStatus>, ReplicaSetError> for StaticFetcher {
        fn desired(&self, _: &ControllerTarget) -> Result<Option<ReplicaSetSpec>, ReplicaSetError> {
            Ok(self.desired.clone())
        }

        fn observed(
            &self,
            _: &ControllerTarget,
        ) -> Result<Vec<ReplicaSetPodStatus>, ReplicaSetError> {
            Ok(self.observed.clone())
        }
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
        ) -> Result<(), ReplicaSetError> {
            let mut guard = self.last.lock().expect("scheduler lock");
            guard.replace(plan.clone());
            Ok(())
        }
    }

    #[test]
    fn reconcile_scaling_up_persists_state() {
        let (controller, _ctx, _guard) = controller_with_env();
        let spec = ReplicaSetSpec {
            replicas: 3,
            template: sample_template(),
            update_strategy: ReplicaSetUpdateStrategy::default(),
        };

        let state = controller.reconcile(&spec, &[]).expect("reconcile");
        assert_eq!(state.pods.len(), 3);
        assert!(state.deletions.is_empty());
        assert!(state
            .pods
            .iter()
            .all(|pod| matches!(pod.action, ReplicaSetPodAction::Create)));

        let persisted = controller.desired_state().expect("load");
        assert_eq!(persisted, Some(state));
    }

    #[test]
    fn reconcile_scaling_down_marks_deletions() {
        let (controller, _ctx, _guard) = controller_with_env();
        let template = sample_template();
        let spec = ReplicaSetSpec {
            replicas: 3,
            template: template.clone(),
            update_strategy: ReplicaSetUpdateStrategy::default(),
        };

        let initial = controller.reconcile(&spec, &[]).expect("initial reconcile");
        let observed: Vec<ReplicaSetPodStatus> = initial
            .pods
            .iter()
            .map(|pod| ReplicaSetPodStatus {
                name: pod.identity.name.clone(),
                revision: initial.revision.clone(),
                ready: true,
            })
            .collect();

        let scale_down_spec = ReplicaSetSpec {
            replicas: 1,
            template,
            update_strategy: ReplicaSetUpdateStrategy::default(),
        };

        let state = controller
            .reconcile(&scale_down_spec, &observed)
            .expect("scale down");

        assert_eq!(state.pods.len(), 1);
        assert_eq!(state.deletions.len(), 2);
        assert!(state
            .pods
            .iter()
            .all(|pod| matches!(pod.action, ReplicaSetPodAction::Retain)));
    }

    #[test]
    fn rolling_update_honors_max_unavailable_budget() {
        let (controller, _ctx, _guard) = controller_with_env();
        let mut template = sample_template();
        let spec = ReplicaSetSpec {
            replicas: 3,
            template: template.clone(),
            update_strategy: ReplicaSetUpdateStrategy::RollingUpdate {
                max_unavailable: Some(1),
            },
        };

        let initial = controller.reconcile(&spec, &[]).expect("initial reconcile");
        let mut observed: Vec<ReplicaSetPodStatus> = initial
            .pods
            .iter()
            .map(|pod| ReplicaSetPodStatus {
                name: pod.identity.name.clone(),
                revision: initial.revision.clone(),
                ready: true,
            })
            .collect();

        template
            .metadata
            .annotations
            .insert("version".to_string(), "v2".to_string());
        let updated_spec = ReplicaSetSpec {
            replicas: 3,
            template: template.clone(),
            update_strategy: ReplicaSetUpdateStrategy::RollingUpdate {
                max_unavailable: Some(1),
            },
        };

        let first = controller
            .reconcile(&updated_spec, &observed)
            .expect("first rolling update");
        let updates: Vec<_> = first
            .pods
            .iter()
            .filter(|pod| matches!(pod.action, ReplicaSetPodAction::Update))
            .collect();
        assert_eq!(updates.len(), 1);

        // Simulate the updated pod becoming ready on the new revision.
        let updated_pod = updates[0].identity.name.clone();
        let new_revision = first.revision.clone();
        for pod in observed.iter_mut() {
            if pod.name == updated_pod {
                pod.revision = new_revision.clone();
            }
        }

        let second = controller
            .reconcile(&updated_spec, &observed)
            .expect("second rolling update");
        let updates_second: Vec<_> = second
            .pods
            .iter()
            .filter(|pod| matches!(pod.action, ReplicaSetPodAction::Update))
            .collect();
        assert_eq!(updates_second.len(), 1);
        assert!(
            updates_second[0].identity.name != updated_pod,
            "next pod should be targeted"
        );

        // Apply final revision to remaining pods.
        for pod in observed.iter_mut() {
            pod.revision = second.revision.clone();
        }

        let final_state = controller
            .reconcile(&updated_spec, &observed)
            .expect("final rolling update");
        assert!(final_state
            .pods
            .iter()
            .all(|pod| matches!(pod.action, ReplicaSetPodAction::Retain)));
    }

    #[test]
    fn on_delete_defers_updating_existing_pods() {
        let (controller, _ctx, _guard) = controller_with_env();
        let mut template = sample_template();
        let spec = ReplicaSetSpec {
            replicas: 2,
            template: template.clone(),
            update_strategy: ReplicaSetUpdateStrategy::OnDelete,
        };

        let initial = controller.reconcile(&spec, &[]).expect("initial reconcile");
        let mut observed: Vec<ReplicaSetPodStatus> = initial
            .pods
            .iter()
            .map(|pod| ReplicaSetPodStatus {
                name: pod.identity.name.clone(),
                revision: initial.revision.clone(),
                ready: true,
            })
            .collect();

        template
            .metadata
            .annotations
            .insert("version".to_string(), "v2".to_string());
        let updated_spec = ReplicaSetSpec {
            replicas: 3,
            template: template.clone(),
            update_strategy: ReplicaSetUpdateStrategy::OnDelete,
        };

        let state = controller
            .reconcile(&updated_spec, &observed)
            .expect("on delete reconcile");
        let mut retained_old = 0;
        let mut created_new = 0;
        for pod in state.pods.iter() {
            match pod.action {
                ReplicaSetPodAction::Retain => {
                    if pod.revision == initial.revision {
                        retained_old += 1;
                    }
                }
                ReplicaSetPodAction::Create => {
                    if pod.revision == state.revision {
                        created_new += 1;
                    }
                }
                ReplicaSetPodAction::Update => panic!("on delete should not update automatically"),
            }
        }
        assert_eq!(retained_old, 2, "existing pods should stay on old revision");
        assert_eq!(created_new, 1, "scaled-up pod uses new revision");

        // Incorporate the scaled replica that uses the new revision.
        observed.push(ReplicaSetPodStatus {
            name: state.pods[2].identity.name.clone(),
            revision: state.revision.clone(),
            ready: true,
        });

        // Simulate deleting an old pod; the replacement should adopt the new revision.
        let retired_name = state.pods[0].identity.name.clone();
        observed.retain(|pod| pod.name != retired_name);
        observed.push(ReplicaSetPodStatus {
            name: retired_name,
            revision: state.revision.clone(),
            ready: true,
        });

        let next = controller
            .reconcile(&updated_spec, &observed)
            .expect("post delete reconcile");
        assert_eq!(
            next.pods
                .iter()
                .filter(|pod| matches!(pod.action, ReplicaSetPodAction::Create))
                .count(),
            0
        );
    }

    #[test]
    fn reconciler_fetch_diff_apply_pipeline() {
        let (controller, _ctx, _guard) = controller_with_env();
        let runtime = ControllerRuntime::new();
        let spec = ReplicaSetSpec {
            replicas: 2,
            template: sample_template(),
            update_strategy: ReplicaSetUpdateStrategy::default(),
        };
        let observed: Vec<ReplicaSetPodStatus> = Vec::new();

        let fetcher: Arc<ReplicaSetFetcher> = Arc::new(StaticFetcher {
            desired: Some(spec.clone()),
            observed: observed.clone(),
        });
        let fetcher_handle = Arc::new(DependencyHandle::new(fetcher));
        let _ = runtime.register_dependency::<DependencyHandle<ReplicaSetFetcher>>(fetcher_handle);

        let scheduler_impl = Arc::new(RecordingScheduler::default());
        let scheduler_trait: Arc<dyn ReplicaSetScheduler> = scheduler_impl.clone();
        let scheduler_handle = Arc::new(DependencyHandle::new(scheduler_trait));
        let _ = runtime
            .register_dependency::<DependencyHandle<dyn ReplicaSetScheduler>>(scheduler_handle);

        let ctx = runtime.context();
        let target = ControllerTarget::ReplicaSet {
            namespace: Some("default".to_string()),
            name: "worker".to_string(),
        };

        let data = controller
            .fetch(&ctx, &target)
            .expect("fetch phase")
            .expect("replicaset should exist");
        assert_eq!(data.desired.replicas, spec.replicas);

        let plan = controller
            .diff(&target, &data.desired, &data.observed)
            .expect("diff phase");

        controller
            .apply(&ctx, &target, plan.clone())
            .expect("apply phase");

        let stored = controller
            .desired_state()
            .expect("load persisted")
            .expect("state present");
        assert_eq!(stored, plan);

        let recorded = scheduler_impl
            .last
            .lock()
            .expect("scheduler lock")
            .clone()
            .expect("scheduler captured plan");
        assert_eq!(recorded.revision, stored.revision);
    }
}
