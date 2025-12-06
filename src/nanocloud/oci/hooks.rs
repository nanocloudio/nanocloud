use std::borrow::Cow;
use std::sync::{Arc, OnceLock, RwLock};

/// Hooks for observing OCI runtime and registry operations.
///
/// The default implementation is a no-op. Install custom hooks with
/// [`set_oci_hooks`] to integrate metrics or tracing without touching call sites.
pub trait OciHooks: Send + Sync {
    /// Called around runtime operations such as create/delete/exec.
    fn runtime_event(&self, _event: &str, _metadata: &[(&str, Cow<'_, str>)]) {}

    /// Called around registry operations such as pull and blob downloads.
    fn registry_event(&self, _event: &str, _metadata: &[(&str, Cow<'_, str>)]) {}
}

struct NoopHooks;
impl OciHooks for NoopHooks {}

fn hooks_cell() -> &'static RwLock<Arc<dyn OciHooks>> {
    static HOOKS: OnceLock<RwLock<Arc<dyn OciHooks>>> = OnceLock::new();
    HOOKS.get_or_init(|| RwLock::new(Arc::new(NoopHooks)))
}

#[allow(dead_code)]
/// Installs custom OCI hooks. Overwrites any previously registered hooks.
pub fn set_oci_hooks(hooks: Arc<dyn OciHooks>) {
    if let Ok(mut guard) = hooks_cell().write() {
        *guard = hooks;
    }
}

fn hooks() -> Arc<dyn OciHooks> {
    hooks_cell()
        .read()
        .map(|guard| guard.clone())
        .unwrap_or_else(|_| Arc::new(NoopHooks))
}

pub(crate) fn emit_runtime_event(event: &str, metadata: &[(&str, Cow<'_, str>)]) {
    hooks().runtime_event(event, metadata);
}

pub(crate) fn emit_registry_event(event: &str, metadata: &[(&str, Cow<'_, str>)]) {
    hooks().registry_event(event, metadata);
}
