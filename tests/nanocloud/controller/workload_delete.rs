use std::collections::HashMap;
use std::env;
use std::fs::{self, File};

use nanocloud::nanocloud::controller::daemonset::{
    DaemonSetController, DaemonSetDesiredState, DaemonSetNodePlan,
};
use nanocloud::nanocloud::controller::delete::DeletionPropagation;
use nanocloud::nanocloud::controller::deployment::{
    DeploymentController, DeploymentDesiredState, DeploymentReplicaSetPlan,
    DeploymentRevisionRecord,
};
use nanocloud::nanocloud::controller::replicaset::{
    AppliedReplicaSetStrategy, ReplicaSetDesiredState, ReplicaSetOwnerRef, ReplicaSetPodAction,
    ReplicaSetUpdateStrategy, LABEL_REPLICASET_NAME,
};
use nanocloud::nanocloud::controller::statefulset::{
    StatefulSetController, StatefulSetDesiredState, StatefulSetRevisionRecord,
};
use nanocloud::nanocloud::k8s::pod::{ContainerSpec, ObjectMeta, Pod, PodSpec};
use nanocloud::nanocloud::k8s::statefulset::StatefulSetStatus;
use nanocloud::nanocloud::k8s::store;
use nanocloud::nanocloud::test_support::{keyspace_lock, test_output_dir};
use nanocloud::nanocloud::util::{is_missing_value_error, Keyspace};
use serde::Serialize;
use serde_json;

#[test]
fn statefulset_foreground_cascade_removes_dependents() {
    let _lock = keyspace_lock().lock();
    let _env = TestEnv::new("statefulset-delete-foreground");

    let namespace = "default";
    let name = "web";
    let active_replicaset = format!("{name}-rev-a");
    let old_replicaset = format!("{name}-rev-b");

    write_statefulset_state(namespace, name, &active_replicaset, &old_replicaset);
    write_replicaset_state(namespace, &active_replicaset, "StatefulSet", name);
    write_replicaset_state(namespace, &old_replicaset, "StatefulSet", name);

    let pod_a = format!("{active_replicaset}-pod");
    let pod_b = format!("{old_replicaset}-pod");
    create_pod_manifest(namespace, &pod_a, &active_replicaset);
    create_pod_manifest(namespace, &pod_b, &old_replicaset);

    let controller = StatefulSetController::new(name.to_string(), Some(namespace.to_string()));
    controller
        .delete_with_propagation(DeletionPropagation::Foreground)
        .expect("cascade delete");

    assert!(
        controller.desired_state().expect("load state").is_none(),
        "controller state should be cleared"
    );

    let controllers = Keyspace::new("controllers");
    for replicaset in [&active_replicaset, &old_replicaset] {
        let key = format!("/replicasets/{}/{}", namespace, replicaset);
        let err = controllers.get(&key).expect_err("replicaset state removed");
        assert!(is_missing_value_error(err.as_ref()), "unexpected error: {err}");
    }

    for pod in [&pod_a, &pod_b] {
        let remaining = store::load_pod_manifest(Some(namespace), pod)
            .expect("load pod manifest should succeed");
        assert!(
            remaining.is_none(),
            "pod manifest {pod} should be removed"
        );
    }
}

#[test]
fn deployment_orphan_retains_dependents() {
    let _lock = keyspace_lock().lock();
    let _env = TestEnv::new("deployment-delete-orphan");

    let namespace = "default";
    let name = "api";
    let active_rs = format!("{name}-rs-active");
    let stale_rs = format!("{name}-rs-stale");

    write_deployment_state(namespace, name, &active_rs, &stale_rs);
    write_replicaset_state(namespace, &active_rs, "Deployment", name);
    write_replicaset_state(namespace, &stale_rs, "Deployment", name);

    let pod_active = format!("{active_rs}-pod");
    let pod_stale = format!("{stale_rs}-pod");
    create_pod_manifest(namespace, &pod_active, &active_rs);
    create_pod_manifest(namespace, &pod_stale, &stale_rs);

    let controller = DeploymentController::new(name.to_string(), Some(namespace.to_string()));
    controller
        .delete_with_propagation(DeletionPropagation::Orphan)
        .expect("orphan delete");

    assert!(
        controller.desired_state().expect("load state").is_none(),
        "deployment state should be cleared"
    );

    let controllers = Keyspace::new("controllers");
    for replicaset in [&active_rs, &stale_rs] {
        let key = format!("/replicasets/{}/{}", namespace, replicaset);
        let raw = controllers
            .get(&key)
            .unwrap_or_else(|err| panic!("replicaset preserved {replicaset}: {err}"));
        assert!(
            raw.contains(replicaset),
            "expected replicaset payload for {replicaset}"
        );
    }

    for pod in [&pod_active, &pod_stale] {
        let manifest = store::load_pod_manifest(Some(namespace), pod)
            .expect("load pod manifest succeed")
            .expect("pod manifest retained");
        let label = manifest
            .metadata
            .labels
            .get(LABEL_REPLICASET_NAME)
            .cloned()
            .unwrap();
        assert!(
            label == active_rs || label == stale_rs,
            "unexpected replicaset label {label}"
        );
    }
}

#[test]
fn daemonset_foreground_removes_node_pods() {
    let _lock = keyspace_lock().lock();
    let _env = TestEnv::new("daemonset-delete-foreground");

    let namespace = "default";
    let name = "logs";

    write_daemonset_state(namespace, name);

    let pod_a = format!("{name}-pod-a");
    let pod_b = format!("{name}-pod-b");
    create_pod_manifest(namespace, &pod_a, name);
    create_pod_manifest(namespace, &pod_b, name);

    let controller = DaemonSetController::new(name.to_string(), Some(namespace.to_string()));
    controller
        .delete_with_propagation(DeletionPropagation::Foreground)
        .expect("daemonset delete");

    assert!(
        controller.desired_state().expect("load state").is_none(),
        "daemonset state should be cleared"
    );
    for pod in [&pod_a, &pod_b] {
        let remaining = store::load_pod_manifest(Some(namespace), pod)
            .expect("load pod manifest should succeed");
        assert!(remaining.is_none(), "daemonset pod {pod} removed");
    }
}

fn write_statefulset_state(
    namespace: &str,
    name: &str,
    active_replicaset: &str,
    old_replicaset: &str,
) {
    let state = StatefulSetDesiredState {
        revision: "rev-a".to_string(),
        replicaset_name: active_replicaset.to_string(),
        replicas: 2,
        bounded_concurrency: 1,
        replica_plan: ReplicaSetDesiredState {
            revision: "rev-a".to_string(),
            strategy: AppliedReplicaSetStrategy::from(&ReplicaSetUpdateStrategy::default()),
            pods: Vec::new(),
            deletions: Vec::new(),
            owner: Some(ReplicaSetOwnerRef {
                kind: "StatefulSet".to_string(),
                name: name.to_string(),
            }),
        },
        status: StatefulSetStatus {
            ready_replicas: Some(0),
            current_replicas: Some(0),
            conditions: Vec::new(),
        },
        ready_ordinals: Vec::new(),
        revision_history: vec![StatefulSetRevisionRecord {
            revision: "rev-prev".to_string(),
            replicaset: old_replicaset.to_string(),
        }],
        pruned_replicasets: Vec::new(),
    };

    write_controller_state(
        &format!("/statefulsets/{}/{}", namespace, name),
        &state,
    );
}

fn write_deployment_state(
    namespace: &str,
    name: &str,
    active_replicaset: &str,
    stale_replicaset: &str,
) {
    let state = DeploymentDesiredState {
        revision: "rev-1".to_string(),
        replicas: 3,
        max_surge: 1,
        max_unavailable: 1,
        active_replicaset: active_replicaset.to_string(),
        replica_sets: vec![DeploymentReplicaSetPlan {
            name: active_replicaset.to_string(),
            revision: "rev-1".to_string(),
            target_replicas: 3,
            available_replicas: Some(2),
        }],
        revision_history: vec![DeploymentRevisionRecord {
            revision: "rev-0".to_string(),
            replicaset: stale_replicaset.to_string(),
        }],
        pruned_replicasets: Vec::new(),
    };

    write_controller_state(
        &format!("/deployments/{}/{}", namespace, name),
        &state,
    );
}

fn write_daemonset_state(namespace: &str, name: &str) {
    let state = DaemonSetDesiredState {
        revision: "rev-ds".to_string(),
        max_unavailable: 1,
        nodes: vec![
            DaemonSetNodePlan {
                node: "node-a".to_string(),
                pod_name: Some(format!("{name}-pod-a")),
                revision: "rev-ds".to_string(),
                action: ReplicaSetPodAction::Retain,
            },
            DaemonSetNodePlan {
                node: "node-b".to_string(),
                pod_name: Some(format!("{name}-pod-b")),
                revision: "rev-ds".to_string(),
                action: ReplicaSetPodAction::Retain,
            },
        ],
        unavailable_nodes: Vec::new(),
    };

    write_controller_state(&format!("/daemonsets/{}/{}", namespace, name), &state);
}

fn write_replicaset_state(
    namespace: &str,
    replicaset: &str,
    owner_kind: &str,
    owner_name: &str,
) {
    let state = ReplicaSetDesiredState {
        revision: "rev-a".to_string(),
        strategy: AppliedReplicaSetStrategy::from(&ReplicaSetUpdateStrategy::default()),
        pods: Vec::new(),
        deletions: Vec::new(),
        owner: Some(ReplicaSetOwnerRef {
            kind: owner_kind.to_string(),
            name: owner_name.to_string(),
        }),
    };

    write_controller_state(
        &format!("/replicasets/{}/{}", namespace, replicaset),
        &state,
    );
}

fn write_controller_state<T>(key: &str, value: &T)
where
    T: Serialize,
{
    let payload = serde_json::to_string_pretty(value).expect("serialize controller state");
    let controllers = Keyspace::new("controllers");
    controllers
        .put(key, &payload)
        .unwrap_or_else(|err| panic!("write controller state {key}: {err}"));
}

fn create_pod_manifest(namespace: &str, name: &str, replicaset: &str) {
    let mut labels = HashMap::new();
    labels.insert(LABEL_REPLICASET_NAME.to_string(), replicaset.to_string());

    let metadata = ObjectMeta {
        name: Some(name.to_string()),
        namespace: Some(namespace.to_string()),
        labels,
        annotations: HashMap::new(),
        resource_version: None,
    };

    let spec = PodSpec {
        init_containers: Vec::new(),
        containers: vec![ContainerSpec {
            name: "main".to_string(),
            ..Default::default()
        }],
        volumes: Vec::new(),
        restart_policy: None,
        service_account_name: None,
        node_name: None,
        host_network: false,
    };

    let pod = Pod::new(metadata, spec);
    store::save_pod_manifest(Some(namespace), name, &pod)
        .unwrap_or_else(|err| panic!("save pod manifest {name}: {err}"));
}

struct TestEnv {
    _keyspace: EnvGuard,
    _lock: EnvGuard,
}

impl TestEnv {
    fn new(component: &str) -> Self {
        let base = test_output_dir(component);
        fs::create_dir_all(&base).expect("create test directory");
        let keyspace_dir = base.join("keyspace");
        let lock_dir = base.join("lock");
        fs::create_dir_all(&keyspace_dir).expect("create keyspace dir");
        fs::create_dir_all(&lock_dir).expect("create lock dir");
        let lock_file = lock_dir.join("nanocloud.lock");
        File::create(&lock_file).expect("create lock file");

        let keyspace = EnvGuard::set_path("NANOCLOUD_KEYSPACE", &keyspace_dir);
        let lock = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", &lock_file);

        Self {
            _keyspace: keyspace,
            _lock: lock,
        }
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
