#![allow(dead_code)]

use crate::nanocloud::observability::{health, metrics};

/// Lightweight helpers used by integration tests to assert observability output
/// without re-implementing parsing logic in each test.
pub struct MetricsHarness;

impl MetricsHarness {
    /// Returns the Prometheus text exposition for all registered metrics.
    pub fn snapshot() -> String {
        let body = metrics::gather().expect("metrics should encode");
        String::from_utf8(body).expect("metrics text must be utf8")
    }

    /// Filters the snapshot for the provided metric prefix (e.g. "nanocloud_controller_reconciles_total").
    pub fn lines_for<'a>(snapshot: &'a str, metric: &str) -> Vec<&'a str> {
        snapshot
            .lines()
            .filter(|line| line.starts_with(metric))
            .collect()
    }
}

pub struct HealthHarness;

impl HealthHarness {
    /// Returns the list of components that reported unhealthy in the provided report.
    pub fn failing_components(report: &health::HealthReport) -> Vec<&'static str> {
        report
            .components
            .iter()
            .filter(|component| !component.healthy)
            .map(|component| component.name)
            .collect()
    }
}
