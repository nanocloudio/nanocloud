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

use super::runtime::{
    ContainerState, ContainerStatus, ContainerSummary, ExecRequest, ExecResult, OciConfig, Runtime,
};
use crate::nanocloud::k8s::pod::{ContainerSpec, PodSecurityContext, VolumeSpec};
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

type DynError = Box<dyn Error + Send + Sync>;
type DynResult<T> = Result<T, DynError>;
type RuntimeFuture = Pin<Box<dyn Future<Output = DynResult<()>> + Send>>;

/// Describes how exec requests are prepared inside a container namespace.
pub trait ExecPrepare: Send {
    fn prepare(self: Box<Self>) -> DynResult<ExecRequest>;
}

impl<F> ExecPrepare for F
where
    F: FnOnce() -> DynResult<ExecRequest> + Send + 'static,
{
    fn prepare(self: Box<Self>) -> DynResult<ExecRequest> {
        (*self)()
    }
}

/// Represents a unit of work that should execute inside a container namespace.
pub trait NamespaceAction: Send {
    fn run(self: Box<Self>) -> DynResult<()>;
}

impl<F> NamespaceAction for F
where
    F: FnOnce() -> DynResult<()> + Send + 'static,
{
    fn run(self: Box<Self>) -> DynResult<()> {
        (*self)()
    }
}

/// Interface for container runtime backends.
pub trait ContainerRuntime: Send + Sync {
    fn configure_from_spec(
        &self,
        container_id: &str,
        container_name: &str,
        container: &ContainerSpec,
        volumes: &[VolumeSpec],
        host_network: bool,
        security: &PodSecurityContext,
    ) -> OciConfig;

    fn create(
        &self,
        container_id: &str,
        env: &HashMap<String, String>,
        config: Vec<u8>,
    ) -> DynResult<()>;

    fn recreate(&self, container_id: &str) -> DynResult<()>;

    fn delete(&self, container_id: &str) -> DynResult<()>;

    fn state(&self, container_id: &str) -> DynResult<ContainerState>;

    fn list(&self) -> DynResult<Vec<ContainerSummary>>;

    fn send_start(&self, container_id: &str) -> DynResult<()>;

    fn set_status(&self, container_id: &str, status: ContainerStatus) -> DynResult<()>;

    fn with_namespace(&self, container_id: &str, action: Box<dyn NamespaceAction>)
        -> DynResult<()>;

    fn exec(&self, container_id: &str, prepare: Box<dyn ExecPrepare>) -> DynResult<ExecResult>;

    fn kill(&self, container_id: String) -> RuntimeFuture;

    fn take_exec_proc_mount_status(&self) -> Option<bool>;
}

struct LocalContainerRuntime;

impl ContainerRuntime for LocalContainerRuntime {
    fn configure_from_spec(
        &self,
        container_id: &str,
        container_name: &str,
        container: &ContainerSpec,
        volumes: &[VolumeSpec],
        host_network: bool,
        security: &PodSecurityContext,
    ) -> OciConfig {
        Runtime::configure_from_spec(
            container_id,
            container_name,
            container,
            volumes,
            host_network,
            security,
        )
    }

    fn create(
        &self,
        container_id: &str,
        env: &HashMap<String, String>,
        config: Vec<u8>,
    ) -> DynResult<()> {
        Runtime::create(container_id, env, std::io::Cursor::new(config))
    }

    fn recreate(&self, container_id: &str) -> DynResult<()> {
        Runtime::recreate(container_id)
    }

    fn delete(&self, container_id: &str) -> DynResult<()> {
        Runtime::delete(container_id)
    }

    fn state(&self, container_id: &str) -> DynResult<ContainerState> {
        Runtime::state(container_id)
    }

    fn list(&self) -> DynResult<Vec<ContainerSummary>> {
        Runtime::list()
    }

    fn send_start(&self, container_id: &str) -> DynResult<()> {
        Runtime::send_start(container_id)
    }

    fn set_status(&self, container_id: &str, status: ContainerStatus) -> DynResult<()> {
        Runtime::set_status(container_id, status)
    }

    fn with_namespace(
        &self,
        container_id: &str,
        action: Box<dyn NamespaceAction>,
    ) -> DynResult<()> {
        let mut action = Some(action);
        Runtime::with_namespace(container_id, move || {
            let runner = action.take().expect("namespace action already consumed");
            runner.run()
        })
    }

    fn exec(&self, container_id: &str, prepare: Box<dyn ExecPrepare>) -> DynResult<ExecResult> {
        let mut prepare = Some(prepare);
        Runtime::exec(container_id, move || {
            let runner = prepare.take().expect("exec preparation already consumed");
            runner.prepare()
        })
    }

    fn kill(&self, container_id: String) -> RuntimeFuture {
        Box::pin(async move { Runtime::kill(&container_id).await })
    }

    fn take_exec_proc_mount_status(&self) -> Option<bool> {
        Runtime::take_exec_proc_mount_status()
    }
}

static GLOBAL_CONTAINER_RUNTIME: OnceLock<Arc<dyn ContainerRuntime>> = OnceLock::new();

#[allow(dead_code)]
pub fn register_container_runtime(
    provider: Arc<dyn ContainerRuntime>,
) -> Result<(), Arc<dyn ContainerRuntime>> {
    GLOBAL_CONTAINER_RUNTIME.set(provider)
}

pub fn container_runtime() -> Arc<dyn ContainerRuntime> {
    GLOBAL_CONTAINER_RUNTIME
        .get_or_init(|| Arc::new(LocalContainerRuntime) as Arc<dyn ContainerRuntime>)
        .clone()
}
