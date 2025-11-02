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

use crate::nanocloud::controller::replicaset::{ReplicaSetDesiredState, ReplicaSetError};
use crate::nanocloud::controller::runtime::ControllerTarget;
use std::sync::Arc;

/// Executor abstraction that encapsulates kubelet interactions required to apply plans.
pub trait KubeletExecutor: Send + Sync {
    fn apply_replica_set(
        &self,
        target: &ControllerTarget,
        plan: &ReplicaSetDesiredState,
    ) -> Result<(), ReplicaSetError>;
}

/// Trait implemented by bridge components that translate controller plans into executor calls.
pub trait ReplicaSetScheduler: Send + Sync {
    fn schedule(
        &self,
        target: &ControllerTarget,
        plan: &ReplicaSetDesiredState,
    ) -> Result<(), ReplicaSetError>;
}

/// Bridge that forwards ReplicaSet plans to the kubelet executor.
pub struct KubeletSchedulingBridge {
    executor: Arc<dyn KubeletExecutor>,
}

impl KubeletSchedulingBridge {
    pub fn new(executor: Arc<dyn KubeletExecutor>) -> Self {
        Self { executor }
    }
}

impl ReplicaSetScheduler for KubeletSchedulingBridge {
    fn schedule(
        &self,
        target: &ControllerTarget,
        plan: &ReplicaSetDesiredState,
    ) -> Result<(), ReplicaSetError> {
        self.executor.apply_replica_set(target, plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::controller::replicaset::AppliedReplicaSetStrategy;
    use std::sync::{Arc, Mutex};

    struct RecordingExecutor {
        records: Mutex<Vec<(String, ReplicaSetDesiredState)>>,
    }

    impl RecordingExecutor {
        fn new() -> Self {
            Self {
                records: Mutex::new(Vec::new()),
            }
        }
    }

    impl KubeletExecutor for RecordingExecutor {
        fn apply_replica_set(
            &self,
            target: &ControllerTarget,
            plan: &ReplicaSetDesiredState,
        ) -> Result<(), ReplicaSetError> {
            let ControllerTarget::ReplicaSet { namespace, name } = target else {
                return Err(ReplicaSetError::Dependency(
                    "expected ReplicaSet target".to_string(),
                ));
            };
            let key = format!(
                "{}/{}",
                namespace.clone().unwrap_or_else(|| "default".into()),
                name
            );
            self.records
                .lock()
                .expect("record lock")
                .push((key, plan.clone()));
            Ok(())
        }
    }

    #[test]
    fn bridge_forwards_plan_to_executor() {
        let executor = Arc::new(RecordingExecutor::new());
        let bridge = KubeletSchedulingBridge::new(executor.clone());
        let target = ControllerTarget::ReplicaSet {
            namespace: Some("ns".to_string()),
            name: "demo".to_string(),
        };
        let plan = ReplicaSetDesiredState {
            revision: "rev".to_string(),
            strategy: AppliedReplicaSetStrategy {
                strategy: "RollingUpdate".to_string(),
                max_unavailable: Some(1),
            },
            pods: Vec::new(),
            deletions: Vec::new(),
            owner: None,
        };

        bridge
            .schedule(&target, &plan)
            .expect("bridge should forward plan");

        let records = executor.records.lock().expect("records lock");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, "ns/demo");
        assert_eq!(records[0].1.revision, "rev");
    }
}
