use std::cmp::max;
use std::collections::HashMap;

use crate::nanocloud::controller::replicaset::{
    ReplicaSetPodAction, ReplicaSetPodStatus, ReplicaSetUpdateStrategy,
};

/// Computed rollout plan identifying the desired action per ordinal.
#[derive(Debug, Default)]
pub struct RolloutPlan {
    pub pods: Vec<RolloutPodPlan>,
}

/// Desired state for an individual pod ordinal.
#[derive(Debug, Clone)]
pub struct RolloutPodPlan {
    pub ordinal: u32,
    pub revision: String,
    pub action: ReplicaSetPodAction,
    pub preferred_node: Option<String>,
}

/// Policy hook that allows callers to influence rollout behaviour.
pub trait RolloutPolicy {
    /// Maximum number of unavailable replicas permitted during rollout.
    fn max_unavailable(&self, desired_replicas: u32, strategy: &ReplicaSetUpdateStrategy) -> u32;

    /// Maximum number of surge replicas permitted during rollout.
    fn max_surge(&self, _desired_replicas: u32, _strategy: &ReplicaSetUpdateStrategy) -> u32 {
        0
    }

    /// Determines whether a pod is eligible for an update when the revision differs.
    fn can_update(&self, _ordinal: u32, pod: &ReplicaSetPodStatus) -> bool {
        pod.ready
    }

    /// Supplies an optional node preference for the ordinal.
    fn preferred_node(&self, _ordinal: u32) -> Option<String> {
        None
    }
}

/// Default rollout policy that mirrors the existing ReplicaSet semantics.
#[derive(Default)]
pub struct DefaultRolloutPolicy;

impl RolloutPolicy for DefaultRolloutPolicy {
    fn max_unavailable(&self, _desired_replicas: u32, strategy: &ReplicaSetUpdateStrategy) -> u32 {
        match strategy {
            ReplicaSetUpdateStrategy::RollingUpdate { max_unavailable } => {
                max(max_unavailable.unwrap_or(1), 1)
            }
            ReplicaSetUpdateStrategy::OnDelete => 0,
        }
    }
}

/// Computes the desired rollout plan for a ReplicaSet-style workload.
pub fn plan_rollout(
    desired_replicas: u32,
    desired_revision: &str,
    strategy: &ReplicaSetUpdateStrategy,
    observed: &HashMap<u32, ReplicaSetPodStatus>,
    policy: &dyn RolloutPolicy,
) -> RolloutPlan {
    let surge_budget = match strategy {
        ReplicaSetUpdateStrategy::RollingUpdate { .. } => {
            policy.max_surge(desired_replicas, strategy)
        }
        ReplicaSetUpdateStrategy::OnDelete => 0,
    } as usize;

    let mut pods = Vec::with_capacity(desired_replicas as usize + surge_budget);
    let mut update_budget = match strategy {
        ReplicaSetUpdateStrategy::RollingUpdate { .. } => {
            max(policy.max_unavailable(desired_replicas, strategy), 1) as usize
        }
        ReplicaSetUpdateStrategy::OnDelete => 0,
    };

    if update_budget > 0 {
        let unavailable = observed.values().filter(|pod| !pod.ready).count();
        update_budget = update_budget.saturating_sub(unavailable);
    }

    for ordinal in 0..desired_replicas {
        let mut action = ReplicaSetPodAction::Create;
        let mut revision = desired_revision.to_string();
        let preferred_node = policy.preferred_node(ordinal);

        if let Some(pod) = observed.get(&ordinal) {
            action = ReplicaSetPodAction::Retain;
            revision = pod.revision.clone();
            match strategy {
                ReplicaSetUpdateStrategy::OnDelete => {
                    // OnDelete rollouts retain existing revisions until manual intervention.
                }
                ReplicaSetUpdateStrategy::RollingUpdate { .. } => {
                    if pod.revision != desired_revision {
                        if policy.can_update(ordinal, pod) && update_budget > 0 {
                            action = ReplicaSetPodAction::Update;
                            revision = desired_revision.to_string();
                            update_budget = update_budget.saturating_sub(1);
                        }
                    } else {
                        revision = desired_revision.to_string();
                    }
                }
            }
        }

        pods.push(RolloutPodPlan {
            ordinal,
            revision,
            action,
            preferred_node,
        });
    }

    RolloutPlan { pods }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::controller::replicaset::ReplicaSetUpdateStrategy;

    fn pod_status(revision: &str, ready: bool) -> ReplicaSetPodStatus {
        ReplicaSetPodStatus {
            name: "replica".to_string(),
            revision: revision.to_string(),
            ready,
        }
    }

    #[test]
    fn rolling_update_honors_budget() {
        let mut observed = HashMap::new();
        observed.insert(0, pod_status("old", true));
        observed.insert(1, pod_status("old", true));

        let strategy = ReplicaSetUpdateStrategy::RollingUpdate {
            max_unavailable: Some(1),
        };
        let plan = plan_rollout(2, "new", &strategy, &observed, &DefaultRolloutPolicy);

        assert_eq!(plan.pods.len(), 2);
        assert!(matches!(plan.pods[0].action, ReplicaSetPodAction::Update));
        assert_eq!(plan.pods[0].revision, "new");
        assert!(matches!(plan.pods[1].action, ReplicaSetPodAction::Retain));
        assert_eq!(plan.pods[1].revision, "old");
    }

    #[test]
    fn on_delete_retains_existing_revisions() {
        let mut observed = HashMap::new();
        observed.insert(0, pod_status("old", true));

        let plan = plan_rollout(
            1,
            "new",
            &ReplicaSetUpdateStrategy::OnDelete,
            &observed,
            &DefaultRolloutPolicy,
        );

        assert_eq!(plan.pods.len(), 1);
        assert!(matches!(plan.pods[0].action, ReplicaSetPodAction::Retain));
        assert_eq!(plan.pods[0].revision, "old");
    }

    struct NoUpdatePolicy;

    impl RolloutPolicy for NoUpdatePolicy {
        fn max_unavailable(
            &self,
            desired_replicas: u32,
            strategy: &ReplicaSetUpdateStrategy,
        ) -> u32 {
            DefaultRolloutPolicy.max_unavailable(desired_replicas, strategy)
        }

        fn can_update(&self, _ordinal: u32, _pod: &ReplicaSetPodStatus) -> bool {
            false
        }
    }

    #[test]
    fn custom_policy_can_block_updates() {
        let mut observed = HashMap::new();
        observed.insert(0, pod_status("old", true));

        let strategy = ReplicaSetUpdateStrategy::RollingUpdate {
            max_unavailable: None,
        };
        let plan = plan_rollout(1, "new", &strategy, &observed, &NoUpdatePolicy);

        assert_eq!(plan.pods.len(), 1);
        assert!(matches!(plan.pods[0].action, ReplicaSetPodAction::Retain));
        assert_eq!(plan.pods[0].revision, "old");
    }
}
