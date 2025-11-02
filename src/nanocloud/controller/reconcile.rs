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

use crate::nanocloud::controller::runtime::{ControllerRuntime, ControllerTarget};
use std::error::Error;
use std::sync::Arc;

/// Execution context shared across reconciliation phases.
///
/// The context exposes the underlying `ControllerRuntime` so controllers can
/// access shared caches, work queues, or dependency-injected helpers.
pub struct ReconcileContext<'a> {
    runtime: &'a ControllerRuntime,
}

impl<'a> ReconcileContext<'a> {
    /// Builds a context from the provided runtime reference.
    pub fn new(runtime: &'a ControllerRuntime) -> Self {
        Self { runtime }
    }

    /// Returns the underlying controller runtime.
    pub fn runtime(&self) -> &'a ControllerRuntime {
        self.runtime
    }

    /// Looks up a dependency that was previously registered with the runtime.
    pub fn dependency<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.runtime.dependency::<T>()
    }
}

/// Wrapper that stores an `Arc<T>` while remaining `Sized`, enabling trait-object dependencies.
pub struct DependencyHandle<T: ?Sized> {
    inner: Arc<T>,
}

impl<T: ?Sized> DependencyHandle<T> {
    pub fn new(inner: Arc<T>) -> Self {
        Self { inner }
    }

    pub fn get(&self) -> Arc<T> {
        Arc::clone(&self.inner)
    }
}

impl<T: ?Sized> Clone for DependencyHandle<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Desired input and observed state gathered during the fetch phase.
pub struct ReconcileData<Desired, Observed> {
    pub desired: Desired,
    pub observed: Observed,
}

type FetchResult<D, O, E> = Result<Option<ReconcileData<D, O>>, E>;

/// Abstraction used by controllers to retrieve desired specs and observed state.
pub trait WorkloadFetcher<D, O, E>: Send + Sync
where
    E: Error + Send + Sync + 'static,
{
    /// Loads the desired specification for the targeted resource.
    fn desired(&self, target: &ControllerTarget) -> Result<Option<D>, E>;

    /// Captures the observed state for the targeted resource (pods, status, etc).
    fn observed(&self, target: &ControllerTarget) -> Result<O, E>;
}

/// Shared reconciliation interface used by workload controllers.
pub trait Reconciler {
    type Desired;
    type Observed;
    type Plan;
    type Error: Error + Send + Sync + 'static;

    /// Returns the logical kind handled by this reconciler (used for diagnostics).
    fn kind(&self) -> &'static str;

    /// Fetches the desired spec and observed state. Returning `Ok(None)` indicates
    /// the resource no longer exists and no further work is required.
    fn fetch(
        &self,
        ctx: &ReconcileContext,
        target: &ControllerTarget,
    ) -> FetchResult<Self::Desired, Self::Observed, Self::Error>;

    /// Computes the plan needed to align observed state with the desired spec.
    fn diff(
        &self,
        target: &ControllerTarget,
        desired: &Self::Desired,
        observed: &Self::Observed,
    ) -> Result<Self::Plan, Self::Error>;

    /// Applies the previously computed plan to drive the system toward convergence.
    fn apply(
        &self,
        ctx: &ReconcileContext,
        target: &ControllerTarget,
        plan: Self::Plan,
    ) -> Result<(), Self::Error>;

    /// Convenience method that performs fetch and diff, returning a plan if work is needed.
    fn reconcile(
        &self,
        ctx: &ReconcileContext,
        target: &ControllerTarget,
    ) -> Result<Option<Self::Plan>, Self::Error> {
        if let Some(data) = self.fetch(ctx, target)? {
            let plan = self.diff(target, &data.desired, &data.observed)?;
            Ok(Some(plan))
        } else {
            Ok(None)
        }
    }

    /// Runs the full reconciliation pipeline, applying the plan when necessary.
    fn reconcile_and_apply(
        &self,
        ctx: &ReconcileContext,
        target: &ControllerTarget,
    ) -> Result<(), Self::Error> {
        if let Some(plan) = self.reconcile(ctx, target)? {
            self.apply(ctx, target, plan)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::controller::runtime::{ControllerRuntime, ControllerTarget};

    #[derive(Default)]
    struct DummyFetcher;

    impl WorkloadFetcher<u32, u32, DummyError> for DummyFetcher {
        fn desired(&self, _: &ControllerTarget) -> Result<Option<u32>, DummyError> {
            Ok(Some(1))
        }

        fn observed(&self, _: &ControllerTarget) -> Result<u32, DummyError> {
            Ok(0)
        }
    }

    #[derive(Debug)]
    struct DummyError;

    impl std::fmt::Display for DummyError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "dummy error")
        }
    }

    impl std::error::Error for DummyError {}

    struct DummyReconciler;

    impl Reconciler for DummyReconciler {
        type Desired = u32;
        type Observed = u32;
        type Plan = u32;
        type Error = DummyError;

        fn kind(&self) -> &'static str {
            "Dummy"
        }

        fn fetch(
            &self,
            ctx: &ReconcileContext,
            target: &ControllerTarget,
        ) -> Result<Option<ReconcileData<Self::Desired, Self::Observed>>, Self::Error> {
            let fetcher = ctx
                .dependency::<DependencyHandle<DynFetcher>>()
                .expect("missing fetcher dependency")
                .get();
            let desired = fetcher.desired(target)?.expect("desired missing");
            let observed = fetcher.observed(target)?;
            Ok(Some(ReconcileData { desired, observed }))
        }

        fn diff(
            &self,
            _: &ControllerTarget,
            desired: &Self::Desired,
            observed: &Self::Observed,
        ) -> Result<Self::Plan, Self::Error> {
            Ok(desired - observed)
        }

        fn apply(
            &self,
            _: &ReconcileContext,
            _: &ControllerTarget,
            plan: Self::Plan,
        ) -> Result<(), Self::Error> {
            assert_eq!(plan, 1);
            Ok(())
        }
    }

    type DynFetcher = dyn WorkloadFetcher<u32, u32, DummyError>;

    #[test]
    fn reconcile_pipeline_runs_with_dependency() {
        let runtime = ControllerRuntime::new();
        let fetcher: Arc<DynFetcher> = Arc::new(DummyFetcher);
        let handle = Arc::new(DependencyHandle::new(fetcher));
        let _ = runtime.register_dependency::<DependencyHandle<DynFetcher>>(handle);
        let ctx = runtime.context();
        let reconciler = DummyReconciler;
        let target = ControllerTarget::ReplicaSet {
            namespace: None,
            name: "dummy".to_string(),
        };

        reconciler
            .reconcile_and_apply(&ctx, &target)
            .expect("reconcile should succeed");
    }
}
