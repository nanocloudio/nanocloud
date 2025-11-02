use nanocloud::nanocloud::controller::reconcile::{DependencyHandle, Reconciler, WorkloadFetcher};
use nanocloud::nanocloud::controller::replicaset::{
    ReplicaSetDesiredState, ReplicaSetError, ReplicaSetPodAction, ReplicaSetPodStatus,
};
use nanocloud::nanocloud::controller::runtime::{ControllerRuntime, ControllerTarget};
use nanocloud::nanocloud::controller::scheduling::ReplicaSetScheduler;
use nanocloud::nanocloud::controller::statefulset::{
    StatefulSetController, StatefulSetDesiredState, StatefulSetError, StatefulSetFetcher,
};
use nanocloud::nanocloud::k8s::pod::{ContainerSpec, ObjectMeta, PodSpec};
use nanocloud::nanocloud::k8s::statefulset::{LabelSelector, PodTemplateSpec, StatefulSetSpec};
use nanocloud::nanocloud::test_support::keyspace_lock;
use nanocloud::nanocloud::util::{Keyspace, KeyspaceEventType};
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::sync::{Arc, Mutex, MutexGuard};
use tempfile::TempDir;

const NAMESPACE: &str = "default";
const SET_NAME: &str = "worker";

#[test]
fn scenario_statefulset_steady_state_noop() {
    let spec = sample_spec(2);
    let harness = ScenarioHarness::new(SET_NAME, Some(NAMESPACE), spec);
    let revision = template_revision(&harness.current_spec());
    harness.set_observed(vec![
        pod_status(0, &revision, true),
        pod_status(1, &revision, true),
    ]);

    let plan = harness.run();
    let scheduled = harness.drain_scheduled();

    assert_eq!(plan.replicas, 2);
    assert_eq!(plan.ready_ordinals, vec![0, 1]);
    assert_eq!(plan.status.ready_replicas, Some(2));
    assert_eq!(plan.status.current_replicas, Some(2));
    assert!(plan.replica_plan.deletions.is_empty());
    assert!(plan
        .replica_plan
        .pods
        .iter()
        .all(|pod| matches!(pod.action, ReplicaSetPodAction::Retain)));
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].1, plan.replica_plan);
}

#[test]
fn scenario_statefulset_scale_up_creates_next_ordinal() {
    let spec = sample_spec(3);
    let harness = ScenarioHarness::new(SET_NAME, Some(NAMESPACE), spec);
    let revision = template_revision(&harness.current_spec());

    harness.set_observed(vec![pod_status(0, &revision, true)]);
    let first_plan = harness.run();
    harness.drain_scheduled();

    assert_eq!(first_plan.replicas, 3);
    assert_eq!(first_plan.ready_ordinals, vec![0]);
    assert!(matches!(
        first_plan.replica_plan.pods[0].action,
        ReplicaSetPodAction::Retain
    ));
    assert!(matches!(
        first_plan.replica_plan.pods[1].action,
        ReplicaSetPodAction::Create
    ));
    assert!(matches!(
        first_plan.replica_plan.pods[2].action,
        ReplicaSetPodAction::Retain
    ));
    assert_eq!(first_plan.status.ready_replicas, Some(1));
    assert_eq!(first_plan.status.current_replicas, Some(1));

    harness.set_observed(vec![
        pod_status(0, &revision, true),
        pod_status(1, &revision, true),
    ]);
    let second_plan = harness.run();
    let scheduled = harness.drain_scheduled();

    assert_eq!(second_plan.ready_ordinals, vec![0, 1]);
    assert!(matches!(
        second_plan.replica_plan.pods[2].action,
        ReplicaSetPodAction::Create
    ));
    assert_eq!(second_plan.status.ready_replicas, Some(2));
    assert_eq!(second_plan.status.current_replicas, Some(2));
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].1, second_plan.replica_plan);
}

#[test]
fn scenario_statefulset_scale_down_deletes_in_order() {
    let spec = sample_spec(3);
    let harness = ScenarioHarness::new(SET_NAME, Some(NAMESPACE), spec);
    let revision = template_revision(&harness.current_spec());
    harness.set_observed(vec![
        pod_status(0, &revision, true),
        pod_status(1, &revision, true),
        pod_status(2, &revision, true),
    ]);

    let baseline = harness.run();
    assert_eq!(baseline.ready_ordinals, vec![0, 1, 2]);
    harness.drain_scheduled();

    harness.update_spec(|spec| spec.replicas = 1);
    let shrink_plan = harness.run();
    let scheduled_first = harness.drain_scheduled();

    assert_eq!(shrink_plan.replicas, 1);
    assert_eq!(shrink_plan.ready_ordinals, vec![0]);
    assert_eq!(shrink_plan.status.ready_replicas, Some(1));
    assert_eq!(shrink_plan.status.current_replicas, Some(3));
    assert_eq!(
        shrink_plan.replica_plan.deletions,
        vec!["worker-2".to_string()]
    );
    assert_eq!(scheduled_first.len(), 1);
    assert_eq!(scheduled_first[0].1, shrink_plan.replica_plan);

    harness.set_observed(vec![
        pod_status(0, &revision, true),
        pod_status(1, &revision, true),
    ]);
    let follow_up = harness.run();
    let scheduled_second = harness.drain_scheduled();

    assert_eq!(
        follow_up.replica_plan.deletions,
        vec!["worker-1".to_string()]
    );
    assert_eq!(follow_up.status.current_replicas, Some(2));
    assert_eq!(follow_up.ready_ordinals, vec![0]);
    assert_eq!(scheduled_second.len(), 1);
    assert_eq!(scheduled_second[0].1, follow_up.replica_plan);
}

#[test]
fn scenario_statefulset_rolling_update_partitions() {
    let mut spec = sample_spec(2);
    let harness = ScenarioHarness::new(SET_NAME, Some(NAMESPACE), spec.clone());
    let revision_a = template_revision(&spec);
    harness.set_observed(vec![
        pod_status(0, &revision_a, true),
        pod_status(1, &revision_a, true),
    ]);

    let baseline = harness.run();
    assert_eq!(baseline.ready_ordinals, vec![0, 1]);
    harness.drain_scheduled();

    harness.update_spec(|spec| {
        spec.template
            .metadata
            .annotations
            .insert("nanocloud.io/revision".to_string(), "next".to_string());
    });
    spec = harness.current_spec();
    let revision_b = template_revision(&spec);
    harness.set_observed(vec![
        pod_status(0, &revision_a, true),
        pod_status(1, &revision_a, true),
    ]);

    let first_wave = harness.run();
    let scheduled_first = harness.drain_scheduled();
    assert!(first_wave.ready_ordinals.is_empty());
    assert!(matches!(
        first_wave.replica_plan.pods[0].action,
        ReplicaSetPodAction::Update
    ));
    assert!(matches!(
        first_wave.replica_plan.pods[1].action,
        ReplicaSetPodAction::Retain
    ));
    assert_eq!(first_wave.status.ready_replicas, Some(0));
    assert_eq!(first_wave.status.current_replicas, Some(2));
    assert_eq!(scheduled_first.len(), 1);
    assert_eq!(scheduled_first[0].1, first_wave.replica_plan);

    harness.set_observed(vec![
        pod_status(0, &revision_b, true),
        pod_status(1, &revision_a, true),
    ]);
    let second_wave = harness.run();
    let scheduled_second = harness.drain_scheduled();
    assert_eq!(second_wave.ready_ordinals, vec![0]);
    assert!(matches!(
        second_wave.replica_plan.pods[1].action,
        ReplicaSetPodAction::Update
    ));
    assert_eq!(scheduled_second.len(), 1);
    assert_eq!(scheduled_second[0].1, second_wave.replica_plan);

    harness.set_observed(vec![
        pod_status(0, &revision_b, true),
        pod_status(1, &revision_b, true),
    ]);
    let completion = harness.run();
    let scheduled_completion = harness.drain_scheduled();
    assert_eq!(completion.ready_ordinals, vec![0, 1]);
    assert!(completion
        .replica_plan
        .pods
        .iter()
        .all(|pod| matches!(pod.action, ReplicaSetPodAction::Retain)));
    assert_eq!(completion.status.ready_replicas, Some(2));
    assert_eq!(scheduled_completion.len(), 1);
    assert_eq!(scheduled_completion[0].1, completion.replica_plan);
}

#[test]
fn scenario_statefulset_rollback_promotes_previous_revision() {
    let mut spec = sample_spec(1);
    let harness = ScenarioHarness::new(SET_NAME, Some(NAMESPACE), spec.clone());
    let revision_a = template_revision(&spec);
    harness.set_observed(vec![pod_status(0, &revision_a, true)]);

    let baseline = harness.run();
    harness.drain_scheduled();
    assert_eq!(baseline.revision_history.len(), 0);

    harness.update_spec(|spec| {
        spec.template
            .metadata
            .annotations
            .insert("version".to_string(), "v2".to_string());
    });
    spec = harness.current_spec();
    let revision_b = template_revision(&spec);
    harness.set_observed(vec![pod_status(0, &revision_a, true)]);

    let rollout = harness.run();
    let scheduled_rollout = harness.drain_scheduled();
    assert_eq!(rollout.revision_history.len(), 1);
    assert_eq!(rollout.revision_history[0].revision, baseline.revision);
    assert_eq!(scheduled_rollout.len(), 1);
    assert!(matches!(
        rollout.replica_plan.pods[0].action,
        ReplicaSetPodAction::Update
    ));

    harness.set_observed(vec![pod_status(0, &revision_b, true)]);
    let stabilized = harness.run();
    harness.drain_scheduled();
    assert_eq!(stabilized.revision, revision_b);
    assert_eq!(stabilized.revision_history[0].revision, baseline.revision);

    harness.update_spec(|spec| {
        spec.template.metadata.annotations.remove("version");
    });
    harness.set_observed(vec![pod_status(0, &revision_b, true)]);

    let rollback = harness.run();
    let scheduled_rollback = harness.drain_scheduled();

    assert_eq!(rollback.revision, revision_a);
    assert_eq!(rollback.replicaset_name, baseline.replicaset_name);
    assert_eq!(rollback.revision_history[0].revision, stabilized.revision);
    assert!(matches!(
        rollback.replica_plan.pods[0].action,
        ReplicaSetPodAction::Update
    ));
    assert_eq!(scheduled_rollback.len(), 1);
    assert_eq!(scheduled_rollback[0].1.revision, rollback.replica_plan.revision);
    assert!(rollback.pruned_replicasets.is_empty());
}

#[test]
fn scenario_statefulset_recovery_recreates_evicted_pod() {
    let spec = sample_spec(1);
    let harness = ScenarioHarness::new(SET_NAME, Some(NAMESPACE), spec);
    let revision = template_revision(&harness.current_spec());
    harness.set_observed(vec![pod_status(0, &revision, true)]);

    let baseline = harness.run();
    assert_eq!(baseline.ready_ordinals, vec![0]);
    harness.drain_scheduled();

    harness.set_observed(Vec::new());
    let restarted = StatefulSetController::new(SET_NAME, Some(NAMESPACE.to_string()));
    let recovery = harness.run_with_controller(&restarted);
    let scheduled = harness.drain_scheduled();

    assert!(recovery.ready_ordinals.is_empty());
    assert_eq!(recovery.status.ready_replicas, Some(0));
    assert_eq!(recovery.status.current_replicas, Some(0));
    assert!(matches!(
        recovery.replica_plan.pods[0].action,
        ReplicaSetPodAction::Create
    ));
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].1, recovery.replica_plan);
}

#[tokio::test]
async fn chaos_statefulset_failure_campaign() {
    let spec = sample_spec(2);
    let mut harness = ScenarioHarness::new(SET_NAME, Some(NAMESPACE), spec);
    let revision = template_revision(&harness.current_spec());
    harness.set_observed(vec![
        pod_status(0, &revision, true),
        pod_status(1, &revision, true),
    ]);

    let controllers = Keyspace::new("controllers");
    let baseline_version = controllers.current_resource_version();
    let mut watch = controllers.watch("/statefulsets", Some(baseline_version));

    let baseline = harness.run();
    harness.drain_scheduled();
    assert_eq!(baseline.ready_ordinals, vec![0, 1]);

    let initial_event = watch.next().await.expect("initial keyspace event");
    assert_eq!(initial_event.key, harness.state_key());
    assert_eq!(initial_event.event_type, KeyspaceEventType::Added);
    let initial_version = initial_event.resource_version;

    harness.delete_state();
    let deleted_event = watch.next().await.expect("deleted keyspace event");
    assert_eq!(deleted_event.key, harness.state_key());
    assert_eq!(deleted_event.event_type, KeyspaceEventType::Deleted);

    harness.restart_runtime();
    harness.set_observed(Vec::new());
    let restart_plan = harness.run();
    harness.drain_scheduled();
    assert!(matches!(
        restart_plan.replica_plan.pods[0].action,
        ReplicaSetPodAction::Create
    ));
    assert_eq!(restart_plan.status.current_replicas, Some(0));

    let reapplied_event = watch.next().await.expect("reapplied keyspace event");
    assert_eq!(reapplied_event.key, harness.state_key());
    assert_eq!(reapplied_event.event_type, KeyspaceEventType::Added);
    assert!(reapplied_event.resource_version > deleted_event.resource_version);

    drop(watch);
    let mut resumed = controllers.watch("/statefulsets", Some(initial_version));
    let replay_deleted = resumed.next().await.expect("replay deleted event");
    assert_eq!(replay_deleted.key, harness.state_key());
    assert_eq!(replay_deleted.event_type, KeyspaceEventType::Deleted);
    let replay_added = resumed.next().await.expect("replay added event");
    assert_eq!(replay_added.key, harness.state_key());
    assert_eq!(replay_added.event_type, KeyspaceEventType::Added);
    assert_eq!(
        replay_added.resource_version,
        reapplied_event.resource_version
    );
}

struct ScenarioHarness {
    runtime: Arc<ControllerRuntime>,
    controller: StatefulSetController,
    spec: Arc<Mutex<StatefulSetSpec>>,
    observed: Arc<Mutex<Vec<ReplicaSetPodStatus>>>,
    scheduler: Arc<RecordingScheduler>,
    _fetcher: Arc<ScenarioFetcher>,
    target: ControllerTarget,
    _env: TestEnv,
    _lock: MutexGuard<'static, ()>,
}

impl ScenarioHarness {
    fn new(name: &str, namespace: Option<&str>, spec: StatefulSetSpec) -> Self {
        let lock = keyspace_lock().lock();
        let env = TestEnv::new();
        let runtime = ControllerRuntime::builder().build();

        let spec_arc = Arc::new(Mutex::new(spec));
        let observed_arc = Arc::new(Mutex::new(Vec::new()));

        let fetcher = Arc::new(ScenarioFetcher::new(spec_arc.clone(), observed_arc.clone()));
        let fetcher_trait: Arc<StatefulSetFetcher> = fetcher.clone();
        let fetcher_handle = Arc::new(DependencyHandle::new(fetcher_trait));
        runtime.register_dependency::<DependencyHandle<StatefulSetFetcher>>(fetcher_handle);

        let scheduler = Arc::new(RecordingScheduler::default());
        let scheduler_trait: Arc<dyn ReplicaSetScheduler> = scheduler.clone();
        let scheduler_handle = Arc::new(DependencyHandle::new(scheduler_trait));
        runtime.register_dependency::<DependencyHandle<dyn ReplicaSetScheduler>>(scheduler_handle);

        let namespace_owned = namespace.map(|ns| ns.to_string());
        let controller = StatefulSetController::new(name.to_string(), namespace_owned.clone());
        let target = ControllerTarget::StatefulSet {
            namespace: namespace_owned.clone(),
            name: name.to_string(),
        };

        Self {
            runtime,
            controller,
            spec: spec_arc,
            observed: observed_arc,
            scheduler,
            _fetcher: fetcher,
            target,
            _env: env,
            _lock: lock,
        }
    }

    fn current_spec(&self) -> StatefulSetSpec {
        self.spec.lock().expect("spec lock poisoned").clone()
    }

    fn set_observed(&self, pods: Vec<ReplicaSetPodStatus>) {
        *self.observed.lock().expect("observed lock poisoned") = pods;
    }

    fn update_spec<F: FnOnce(&mut StatefulSetSpec)>(&self, updater: F) {
        let mut guard = self.spec.lock().expect("spec lock poisoned");
        updater(&mut guard);
    }

    fn run(&self) -> StatefulSetDesiredState {
        self.run_with_controller(&self.controller)
    }

    fn run_with_controller(&self, controller: &StatefulSetController) -> StatefulSetDesiredState {
        let ctx = self.runtime.context();
        controller
            .reconcile_and_apply(&ctx, &self.target)
            .expect("statefulset reconcile and apply");
        controller
            .desired_state()
            .expect("load desired state")
            .expect("desired state present")
    }

    fn restart_runtime(&mut self) {
        let runtime = ControllerRuntime::builder().build();

        let fetcher = Arc::new(ScenarioFetcher::new(
            self.spec.clone(),
            self.observed.clone(),
        ));
        let fetcher_trait: Arc<StatefulSetFetcher> = fetcher.clone();
        let fetcher_handle = Arc::new(DependencyHandle::new(fetcher_trait));
        runtime.register_dependency::<DependencyHandle<StatefulSetFetcher>>(fetcher_handle);

        let scheduler = Arc::new(RecordingScheduler::default());
        let scheduler_trait: Arc<dyn ReplicaSetScheduler> = scheduler.clone();
        let scheduler_handle = Arc::new(DependencyHandle::new(scheduler_trait));
        runtime.register_dependency::<DependencyHandle<dyn ReplicaSetScheduler>>(scheduler_handle);

        let (namespace, name) = match &self.target {
            ControllerTarget::StatefulSet { namespace, name } => (namespace.clone(), name.clone()),
            _ => unreachable!("unsupported target for ScenarioHarness"),
        };

        self.runtime = runtime;
        self.scheduler = scheduler;
        self._fetcher = fetcher;
        self.controller = StatefulSetController::new(name, namespace);
    }

    fn drain_scheduled(&self) -> Vec<(ControllerTarget, ReplicaSetDesiredState)> {
        self.scheduler.drain()
    }

    fn state_key(&self) -> String {
        match &self.target {
            ControllerTarget::StatefulSet { namespace, name } => {
                format!(
                    "/statefulsets/{}/{}",
                    normalized_namespace(namespace.as_deref()),
                    name
                )
            }
            _ => unreachable!("unsupported target for ScenarioHarness"),
        }
    }

    fn delete_state(&self) {
        let keyspace = Keyspace::new("controllers");
        keyspace
            .delete(&self.state_key())
            .expect("delete controller state key");
    }
}

#[derive(Default)]
struct RecordingScheduler {
    records: Mutex<Vec<(ControllerTarget, ReplicaSetDesiredState)>>,
}

impl RecordingScheduler {
    fn drain(&self) -> Vec<(ControllerTarget, ReplicaSetDesiredState)> {
        let mut guard = self
            .records
            .lock()
            .expect("scheduler records lock poisoned");
        std::mem::take(&mut *guard)
    }
}

impl ReplicaSetScheduler for RecordingScheduler {
    fn schedule(
        &self,
        target: &ControllerTarget,
        plan: &ReplicaSetDesiredState,
    ) -> Result<(), ReplicaSetError> {
        self.records
            .lock()
            .expect("scheduler records lock poisoned")
            .push((target.clone(), plan.clone()));
        Ok(())
    }
}

#[derive(Clone)]
struct ScenarioFetcher {
    spec: Arc<Mutex<StatefulSetSpec>>,
    observed: Arc<Mutex<Vec<ReplicaSetPodStatus>>>,
}

impl ScenarioFetcher {
    fn new(
        spec: Arc<Mutex<StatefulSetSpec>>,
        observed: Arc<Mutex<Vec<ReplicaSetPodStatus>>>,
    ) -> Self {
        Self { spec, observed }
    }
}

impl WorkloadFetcher<StatefulSetSpec, Vec<ReplicaSetPodStatus>, StatefulSetError>
    for ScenarioFetcher
{
    fn desired(&self, _: &ControllerTarget) -> Result<Option<StatefulSetSpec>, StatefulSetError> {
        Ok(Some(self.spec.lock().expect("spec lock poisoned").clone()))
    }

    fn observed(&self, _: &ControllerTarget) -> Result<Vec<ReplicaSetPodStatus>, StatefulSetError> {
        Ok(self
            .observed
            .lock()
            .expect("observed lock poisoned")
            .clone())
    }
}

struct TestEnv {
    _dir: TempDir,
    keyspace_previous: Option<String>,
    lock_previous: Option<String>,
}

impl TestEnv {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");

        let keyspace_previous = env::var("NANOCLOUD_KEYSPACE").ok();
        env::set_var("NANOCLOUD_KEYSPACE", dir.path());

        let lock_previous = env::var("NANOCLOUD_LOCK_FILE").ok();
        let lock_path = dir.path().join("nanocloud.lock");
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).expect("lock dir");
        }
        fs::File::create(&lock_path).expect("lock file");
        env::set_var("NANOCLOUD_LOCK_FILE", lock_path.to_string_lossy().as_ref());

        Self {
            _dir: dir,
            keyspace_previous,
            lock_previous,
        }
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        if let Some(previous) = self.keyspace_previous.as_ref() {
            env::set_var("NANOCLOUD_KEYSPACE", previous);
        } else {
            env::remove_var("NANOCLOUD_KEYSPACE");
        }

        if let Some(previous) = self.lock_previous.as_ref() {
            env::set_var("NANOCLOUD_LOCK_FILE", previous);
        } else {
            env::remove_var("NANOCLOUD_LOCK_FILE");
        }
    }
}

fn template_revision(spec: &StatefulSetSpec) -> String {
    let payload = serde_json::to_vec(&spec.template).expect("serialize pod template");
    let digest = Sha1::digest(&payload);
    format!("{:x}", digest)
}

fn pod_status(ordinal: u32, revision: &str, ready: bool) -> ReplicaSetPodStatus {
    ReplicaSetPodStatus {
        name: format!("{SET_NAME}-{ordinal}"),
        revision: revision.to_string(),
        ready,
    }
}

fn sample_spec(replicas: i32) -> StatefulSetSpec {
    StatefulSetSpec {
        service_name: SET_NAME.to_string(),
        replicas,
        selector: sample_selector(),
        template: sample_template(),
        update_strategy: Default::default(),
        volume_claim_templates: Vec::new(),
    }
}

fn sample_selector() -> LabelSelector {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), SET_NAME.to_string());
    LabelSelector {
        match_labels: labels,
    }
}

fn sample_template() -> PodTemplateSpec {
    let mut metadata = ObjectMeta::default();
    metadata
        .labels
        .insert("app".to_string(), SET_NAME.to_string());
    PodTemplateSpec {
        metadata,
        spec: PodSpec {
            containers: vec![ContainerSpec {
                name: SET_NAME.to_string(),
                ..Default::default()
            }],
            ..Default::default()
        },
    }
}

fn normalized_namespace(namespace: Option<&str>) -> String {
    namespace
        .filter(|ns| !ns.is_empty())
        .unwrap_or("default")
        .to_string()
}
