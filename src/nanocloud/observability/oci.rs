use crate::nanocloud::oci::{set_oci_hooks, OciHooks};
use crate::nanocloud::observability::metrics;
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::OnceLock;

struct TelemetryOciHooks;

impl OciHooks for TelemetryOciHooks {
    fn runtime_event(&self, event: &str, metadata: &[(&str, Cow<'_, str>)]) {
        metrics::record_oci_runtime_event(event);
        if event.ends_with(".error") {
            tracing::warn!(
                target: "nanocloud::oci::runtime",
                event,
                metadata = ?metadata,
                "OCI runtime event"
            );
        } else {
            tracing::debug!(
                target: "nanocloud::oci::runtime",
                event,
                metadata = ?metadata,
                "OCI runtime event"
            );
        }
    }

    fn registry_event(&self, event: &str, metadata: &[(&str, Cow<'_, str>)]) {
        metrics::record_oci_registry_event(event);
        if event.ends_with(".error") {
            tracing::warn!(
                target: "nanocloud::oci::registry",
                event,
                metadata = ?metadata,
                "OCI registry event"
            );
        } else {
            tracing::debug!(
                target: "nanocloud::oci::registry",
                event,
                metadata = ?metadata,
                "OCI registry event"
            );
        }
    }
}

/// Installs OCI telemetry hooks exactly once so runtime/registry events
/// contribute to tracing and Prometheus metrics.
pub fn install_oci_telemetry_hooks() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| set_oci_hooks(Arc::new(TelemetryOciHooks)));
}
