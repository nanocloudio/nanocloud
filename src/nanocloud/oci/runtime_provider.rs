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
use crate::nanocloud::oci::hooks::emit_runtime_event;
use log::{info, warn};
use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

type DynError = Box<dyn Error + Send + Sync>;
type DynResult<T> = Result<T, DynError>;
type RuntimeFuture = Pin<Box<dyn Future<Output = DynResult<()>> + Send>>;

/// Capability flags exposed by a runtime provider.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct RuntimeCapabilities {
    /// Whether the runtime can exec into containers (used for probes and `exec` calls).
    pub exec_supported: bool,
    /// Whether namespace transitions are available for helpers that join container namespaces.
    pub namespaces_supported: bool,
    /// Whether encrypted volume mounts are supported end-to-end.
    pub encrypted_volumes_supported: bool,
}

impl Default for RuntimeCapabilities {
    fn default() -> Self {
        Self {
            exec_supported: true,
            namespaces_supported: true,
            encrypted_volumes_supported: true,
        }
    }
}

/// Errors returned by runtime provider operations.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum RuntimeError {
    /// A runtime is already registered and cannot be replaced.
    AlreadyRegistered(&'static str),
    /// No runtime has been registered.
    #[allow(dead_code)]
    ProviderMissing,
    /// A runtime operation failed with context.
    OperationFailed(String),
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RuntimeError::AlreadyRegistered(msg) => write!(f, "{msg}"),
            RuntimeError::ProviderMissing => write!(f, "No container runtime registered"),
            RuntimeError::OperationFailed(msg) => write!(f, "{msg}"),
        }
    }
}

impl Error for RuntimeError {}

type RuntimeResult<T> = Result<T, RuntimeError>;

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
///
/// This trait forms the stable integration surface between Nanocloud and pluggable
/// runtime implementations. Providers should keep these semantics backward-compatible
/// to avoid breaking existing consumers.
pub trait ContainerRuntime: Send + Sync {
    /// Report the capabilities supported by this provider.
    #[allow(dead_code)]
    fn capabilities(&self) -> RuntimeCapabilities;

    /// Optional shutdown hook to release resources.
    #[allow(dead_code)]
    fn shutdown(&self) -> DynResult<()> {
        Ok(())
    }

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
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities::default()
    }

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
        emit_runtime_event(
            "create.start",
            &[("container_id", Cow::Borrowed(container_id))],
        );
        let result = Runtime::create(container_id, env, std::io::Cursor::new(config));
        if let Err(err) = &result {
            emit_runtime_event(
                "create.error",
                &[
                    ("container_id", Cow::Borrowed(container_id)),
                    ("error", Cow::Owned(err.to_string())),
                ],
            );
        } else {
            emit_runtime_event(
                "create.ok",
                &[("container_id", Cow::Borrowed(container_id))],
            );
        }
        result
    }

    fn recreate(&self, container_id: &str) -> DynResult<()> {
        Runtime::recreate(container_id)
    }

    fn delete(&self, container_id: &str) -> DynResult<()> {
        emit_runtime_event(
            "delete.start",
            &[("container_id", Cow::Borrowed(container_id))],
        );
        let result = Runtime::delete(container_id);
        if let Err(err) = &result {
            emit_runtime_event(
                "delete.error",
                &[
                    ("container_id", Cow::Borrowed(container_id)),
                    ("error", Cow::Owned(err.to_string())),
                ],
            );
        } else {
            emit_runtime_event(
                "delete.ok",
                &[("container_id", Cow::Borrowed(container_id))],
            );
        }
        result
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
        emit_runtime_event(
            "exec.start",
            &[("container_id", Cow::Borrowed(container_id))],
        );
        let mut prepare = Some(prepare);
        let result = Runtime::exec(container_id, move || {
            let runner = prepare.take().expect("exec preparation already consumed");
            runner.prepare()
        });
        if let Err(err) = &result {
            emit_runtime_event(
                "exec.error",
                &[
                    ("container_id", Cow::Borrowed(container_id)),
                    ("error", Cow::Owned(err.to_string())),
                ],
            );
        } else {
            emit_runtime_event(
                "exec.ok",
                &[("container_id", Cow::Borrowed(container_id))],
            );
        }
        result
    }

    fn kill(&self, container_id: String) -> RuntimeFuture {
        Box::pin(async move { Runtime::kill(&container_id).await })
    }

    fn take_exec_proc_mount_status(&self) -> Option<bool> {
        Runtime::take_exec_proc_mount_status()
    }
}

#[derive(Default)]
struct RuntimeRegistry {
    provider: Option<Arc<dyn ContainerRuntime>>,
    is_default: bool,
}

impl RuntimeRegistry {
    fn install_default(&mut self) -> Arc<dyn ContainerRuntime> {
        if self.provider.is_none() {
            let provider: Arc<dyn ContainerRuntime> = Arc::new(LocalContainerRuntime);
            info!("Container runtime registered: default provider");
            self.provider = Some(provider.clone());
            self.is_default = true;
        }
        self.provider
            .as_ref()
            .cloned()
            .expect("default runtime must be installed")
    }

    fn set_provider(
        &mut self,
        provider: Arc<dyn ContainerRuntime>,
        from_default: bool,
    ) -> RuntimeResult<(Option<Arc<dyn ContainerRuntime>>, bool)> {
        let previous_is_default = self.is_default;
        if let Some(existing) = &self.provider {
            if Arc::ptr_eq(existing, &provider) {
                return Ok((None, previous_is_default));
            }
            if !self.is_default {
                return Err(RuntimeError::AlreadyRegistered(
                    "A container runtime is already registered",
                ));
            }
        }

        let previous = self.provider.take();
        self.provider = Some(provider);
        self.is_default = from_default;
        Ok((previous, previous_is_default))
    }

    fn provider(&self) -> Option<Arc<dyn ContainerRuntime>> {
        self.provider.clone()
    }
}

fn registry() -> &'static RwLock<RuntimeRegistry> {
    static GLOBAL_CONTAINER_RUNTIME: OnceLock<RwLock<RuntimeRegistry>> = OnceLock::new();
    GLOBAL_CONTAINER_RUNTIME.get_or_init(|| RwLock::new(RuntimeRegistry::default()))
}

#[allow(dead_code)]
fn registry_write() -> RuntimeResult<RwLockWriteGuard<'static, RuntimeRegistry>> {
    registry()
        .write()
        .map_err(|_| RuntimeError::OperationFailed("Failed to lock runtime registry".into()))
}

#[allow(dead_code)]
fn registry_read() -> RuntimeResult<RwLockReadGuard<'static, RuntimeRegistry>> {
    registry()
        .read()
        .map_err(|_| RuntimeError::OperationFailed("Failed to read runtime registry".into()))
}

/// Guard that restores the previous runtime provider on drop.
#[allow(dead_code)]
pub struct RuntimeProviderGuard {
    previous: Option<Arc<dyn ContainerRuntime>>,
    previous_is_default: bool,
    restored: bool,
}

impl Drop for RuntimeProviderGuard {
    fn drop(&mut self) {
        if self.restored {
            return;
        }
        match registry_write() {
            Ok(mut reg) => {
                reg.provider = self.previous.take();
                reg.is_default = self.previous_is_default;
                info!("Container runtime restored to previous provider");
            }
            Err(err) => warn!("Failed to restore container runtime provider: {err}"),
        }
        self.restored = true;
    }
}

#[allow(dead_code)]
/// Registers a custom runtime provider.
///
/// This call is idempotent when invoked with the same provider. A non-default
/// provider cannot be replaced without first restoring the default or using a scoped guard.
pub fn register_container_runtime(provider: Arc<dyn ContainerRuntime>) -> RuntimeResult<()> {
    let mut reg = registry_write()?;
    let (previous, _) = reg.set_provider(provider, false)?;
    if previous.is_some() {
        warn!("Replacing default container runtime with custom provider");
    }
    info!("Container runtime registered");
    Ok(())
}

/// Installs a scoped runtime provider, restoring the previous provider when the guard is dropped.
/// This is primarily intended for tests so overrides do not leak across cases.
#[allow(dead_code)]
///
/// ```
/// use std::sync::Arc;
/// use nanocloud::nanocloud::oci::{scoped_runtime_provider, ContainerRuntime, RuntimeCapabilities};
/// use nanocloud::nanocloud::k8s::pod::{ContainerSpec, PodSecurityContext, VolumeSpec};
/// use nanocloud::nanocloud::oci::runtime_provider::RuntimeFuture;
///
/// struct DummyRuntime;
/// impl ContainerRuntime for DummyRuntime {
///     fn capabilities(&self) -> RuntimeCapabilities { RuntimeCapabilities::default() }
///     fn configure_from_spec(&self, _id: &str, _name: &str, _c: &ContainerSpec, _v: &[VolumeSpec], _h: bool, _s: &PodSecurityContext) -> nanocloud::nanocloud::oci::runtime::OciConfig { unimplemented!() }
///     fn create(&self, _id: &str, _env: &std::collections::HashMap<String, String>, _cfg: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
///     fn recreate(&self, _id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
///     fn delete(&self, _id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
///     fn state(&self, _id: &str) -> Result<nanocloud::nanocloud::oci::runtime::ContainerState, Box<dyn std::error::Error + Send + Sync>> { unimplemented!() }
///     fn list(&self) -> Result<Vec<nanocloud::nanocloud::oci::runtime::ContainerSummary>, Box<dyn std::error::Error + Send + Sync>> { Ok(Vec::new()) }
///     fn send_start(&self, _id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
///     fn set_status(&self, _id: &str, _status: nanocloud::nanocloud::oci::runtime::ContainerStatus) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
///     fn with_namespace(&self, _id: &str, _action: Box<dyn nanocloud::nanocloud::oci::runtime_provider::NamespaceAction>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
///     fn exec(&self, _id: &str, _prepare: Box<dyn nanocloud::nanocloud::oci::runtime_provider::ExecPrepare>) -> Result<nanocloud::nanocloud::oci::runtime::ExecResult, Box<dyn std::error::Error + Send + Sync>> { Err("not implemented".into()) }
///     fn kill(&self, _id: String) -> nanocloud::nanocloud::oci::runtime_provider::RuntimeFuture { Box::pin(async { Ok(()) }) }
///     fn take_exec_proc_mount_status(&self) -> Option<bool> { None }
/// }
///
/// let guard = scoped_runtime_provider(Arc::new(DummyRuntime)).unwrap();
/// drop(guard);
/// ```
pub fn scoped_runtime_provider(
    provider: Arc<dyn ContainerRuntime>,
) -> RuntimeResult<RuntimeProviderGuard> {
    let mut reg = registry_write()?;
    let (previous, previous_is_default) = reg.set_provider(provider, false)?;
    info!("Container runtime registered for scoped use");
    Ok(RuntimeProviderGuard {
        previous,
        previous_is_default,
        restored: false,
    })
}

/// Returns the registered container runtime, installing the default if missing.
pub fn container_runtime() -> Arc<dyn ContainerRuntime> {
    let mut reg = registry()
        .write()
        .expect("container runtime registry poisoned");
    reg.provider().unwrap_or_else(|| reg.install_default())
}

/// Returns the runtime provider with an error when none is registered.
///
/// Prefer this when the caller wants to propagate a missing-provider error instead
/// of implicitly installing the default runtime.
#[allow(dead_code)]
pub fn try_container_runtime() -> RuntimeResult<Arc<dyn ContainerRuntime>> {
    registry_read()?
        .provider()
        .ok_or(RuntimeError::ProviderMissing)
}

/// Returns the capabilities reported by the active runtime provider.
#[allow(dead_code)]
pub fn runtime_capabilities() -> RuntimeResult<RuntimeCapabilities> {
    try_container_runtime().map(|provider| provider.capabilities())
}

/// Calls the shutdown hook on the active runtime provider and clears it.
#[allow(dead_code)]
pub fn shutdown_container_runtime() -> RuntimeResult<()> {
    let mut reg = registry_write()?;
    if let Some(provider) = reg.provider.take() {
        provider
            .shutdown()
            .map_err(|e| RuntimeError::OperationFailed(e.to_string()))?;
        reg.is_default = false;
        info!("Container runtime shutdown completed");
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn reset_runtime_provider_for_test() {
    if let Ok(mut reg) = registry_write() {
        reg.provider = None;
        reg.is_default = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::util::error::new_error;
    use serial_test::serial;
    use std::sync::Mutex;

    struct TestRuntime;

    impl ContainerRuntime for TestRuntime {
        fn capabilities(&self) -> RuntimeCapabilities {
            RuntimeCapabilities {
                exec_supported: false,
                namespaces_supported: false,
                encrypted_volumes_supported: false,
            }
        }

        fn configure_from_spec(
            &self,
            _container_id: &str,
            _container_name: &str,
            _container: &ContainerSpec,
            _volumes: &[VolumeSpec],
            _host_network: bool,
            _security: &PodSecurityContext,
        ) -> OciConfig {
            unimplemented!("not used in tests")
        }

        fn create(
            &self,
            _container_id: &str,
            _env: &HashMap<String, String>,
            _config: Vec<u8>,
        ) -> DynResult<()> {
            Ok(())
        }

        fn recreate(&self, _container_id: &str) -> DynResult<()> {
            Ok(())
        }

        fn delete(&self, _container_id: &str) -> DynResult<()> {
            Ok(())
        }

        fn state(&self, _container_id: &str) -> DynResult<ContainerState> {
            Err(new_error("not implemented"))
        }

        fn list(&self) -> DynResult<Vec<ContainerSummary>> {
            Err(new_error("not implemented"))
        }

        fn send_start(&self, _container_id: &str) -> DynResult<()> {
            Ok(())
        }

        fn set_status(&self, _container_id: &str, _status: ContainerStatus) -> DynResult<()> {
            Ok(())
        }

        fn with_namespace(
            &self,
            _container_id: &str,
            _action: Box<dyn NamespaceAction>,
        ) -> DynResult<()> {
            Ok(())
        }

        fn exec(
            &self,
            _container_id: &str,
            _prepare: Box<dyn ExecPrepare>,
        ) -> DynResult<ExecResult> {
            Err(new_error("not implemented"))
        }

        fn kill(&self, _container_id: String) -> RuntimeFuture {
            Box::pin(async { Ok(()) })
        }

        fn take_exec_proc_mount_status(&self) -> Option<bool> {
            None
        }
    }

    #[test]
    #[serial]
    fn scoped_provider_restores_previous_capabilities() {
        let _ = container_runtime();
        let guard =
            scoped_runtime_provider(Arc::new(TestRuntime)).expect("register scoped provider");
        let scoped_caps = runtime_capabilities().expect("scoped capabilities");
        assert!(!scoped_caps.exec_supported);
        drop(guard);
        let default_caps = runtime_capabilities().expect("default capabilities");
        assert!(default_caps.exec_supported);
    }

    #[test]
    #[serial]
    fn double_registration_is_rejected() {
        let _ = container_runtime();
        let guard =
            scoped_runtime_provider(Arc::new(TestRuntime)).expect("register scoped provider");
        let err = register_container_runtime(Arc::new(TestRuntime))
            .expect_err("double registration should fail");
        matches!(err, RuntimeError::AlreadyRegistered(_));
        drop(guard);
    }

    #[test]
    #[serial]
    fn missing_provider_errors() {
        reset_runtime_provider_for_test();
        let err = try_container_runtime().err().expect("should be missing");
        assert!(matches!(err, RuntimeError::ProviderMissing));
        let _ = container_runtime();
    }

    #[test]
    #[serial]
    fn shutdown_clears_provider() {
        reset_runtime_provider_for_test();
        let _ = container_runtime();
        shutdown_container_runtime().expect("shutdown");
        let err = try_container_runtime().err().expect("provider should be cleared");
        assert!(matches!(err, RuntimeError::ProviderMissing));
    }

    #[test]
    #[serial]
    fn integration_exec_with_mock_runtime() {
        struct MockExecRuntime {
            calls: Mutex<Vec<String>>,
        }

        impl MockExecRuntime {
            fn new() -> Self {
                Self {
                    calls: Mutex::new(Vec::new()),
                }
            }
        }

        impl ContainerRuntime for MockExecRuntime {
            fn capabilities(&self) -> RuntimeCapabilities {
                RuntimeCapabilities::default()
            }

            fn configure_from_spec(
                &self,
                _container_id: &str,
                _container_name: &str,
                _container: &ContainerSpec,
                _volumes: &[VolumeSpec],
                _host_network: bool,
                _security: &PodSecurityContext,
            ) -> OciConfig {
                unimplemented!("configure_from_spec not used")
            }

            fn create(
                &self,
                _container_id: &str,
                _env: &HashMap<String, String>,
                _config: Vec<u8>,
            ) -> DynResult<()> {
                Ok(())
            }

            fn recreate(&self, _container_id: &str) -> DynResult<()> {
                Ok(())
            }

            fn delete(&self, _container_id: &str) -> DynResult<()> {
                Ok(())
            }

            fn state(&self, _container_id: &str) -> DynResult<ContainerState> {
                Err(new_error("not implemented"))
            }

            fn list(&self) -> DynResult<Vec<ContainerSummary>> {
                Ok(Vec::new())
            }

            fn send_start(&self, _container_id: &str) -> DynResult<()> {
                Ok(())
            }

            fn set_status(&self, _container_id: &str, _status: ContainerStatus) -> DynResult<()> {
                Ok(())
            }

            fn with_namespace(
                &self,
                _container_id: &str,
                _action: Box<dyn NamespaceAction>,
            ) -> DynResult<()> {
                Ok(())
            }

            fn exec(
                &self,
                container_id: &str,
                _prepare: Box<dyn ExecPrepare>,
            ) -> DynResult<ExecResult> {
                self.calls
                    .lock()
                    .unwrap()
                    .push(format!("exec:{container_id}"));
                Ok(ExecResult {
                    wait_status: nix::sys::wait::WaitStatus::Exited(
                        nix::unistd::Pid::from_raw(1),
                        0,
                    ),
                })
            }

            fn kill(&self, _container_id: String) -> RuntimeFuture {
                Box::pin(async { Ok(()) })
            }

            fn take_exec_proc_mount_status(&self) -> Option<bool> {
                None
            }
        }

        reset_runtime_provider_for_test();
        let mock = Arc::new(MockExecRuntime::new());
        let guard = scoped_runtime_provider(mock.clone()).expect("install mock runtime");
        let result = container_runtime()
            .exec(
                "demo",
                Box::new(|| {
                    Ok(ExecRequest {
                        program: "/bin/true".to_string(),
                        args: Vec::new(),
                        env: None,
                    })
                }),
            )
            .expect("exec succeeds");
        matches!(result.wait_status, nix::sys::wait::WaitStatus::Exited(_, 0));
        assert_eq!(mock.calls.lock().unwrap().as_slice(), &["exec:demo"]);
        drop(guard);
        let _ = shutdown_container_runtime();
    }
}
