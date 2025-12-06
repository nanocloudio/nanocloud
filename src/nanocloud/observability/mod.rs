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

//! Observability primitives shared across the Nanocloud control plane.
//!
//! The metrics exposed here follow the upstream Kubernetes Prometheus
//! conventions: snake_case names prefixed with the project (`nanocloud`),
//! counters ending with `_total`, and duration histograms ending with
//! `_seconds`. Label keys mirror Kubernetes resource identifiers such as
//! `namespace` and `workload` so metrics can be correlated with familiar
//! dashboards and alerting rules. Telemetry initialization is explicitly
//! guarded to avoid accidental double-installation of subscribers or
//! exporters.
//!
//! # Examples
//! Initialize tracing and metrics with a custom filter:
//! ```
//! use nanocloud::nanocloud::observability;
//! use nanocloud::nanocloud::observability::TelemetryConfig;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut config = TelemetryConfig::noop();
//! config.tracing.filter_directives = "nanocloud=info".to_string();
//! let handle = observability::init(&config)?;
//! handle.shutdown();
//! # Ok(())
//! # }
//! ```
//!
//! Expose standard health endpoints with Axum:
//! ```
//! use axum::Router;
//! use nanocloud::nanocloud::observability::health;
//!
//! let app = Router::new().merge(health::axum_routes());
//! # let _ = app;
//! ```

pub mod config;
pub mod health;
pub mod metrics;
pub mod oci;
pub mod testing;
pub mod tracing;

pub use config::{
    MetricsConfig, TelemetryConfig, TelemetryError, TracingConfig, TracingFormat, TracingOutput,
};

#[derive(Clone, Debug)]
pub struct TelemetryHandle {
    tracing: tracing::TracingHandle,
    metrics: metrics::MetricsHandle,
}

impl TelemetryHandle {
    pub fn shutdown(&self) {
        self.tracing.shutdown();
        self.metrics.shutdown();
    }
}

/// Initialize tracing and metrics exporters using the provided configuration.
pub fn init(config: &TelemetryConfig) -> Result<TelemetryHandle, TelemetryError> {
    let metrics = metrics::init(config.metrics.clone())?;
    match tracing::init_with_config(config.tracing.clone()) {
        Ok(tracing) => {
            oci::install_oci_telemetry_hooks();
            Ok(TelemetryHandle { tracing, metrics })
        }
        Err(err) => {
            metrics::record_telemetry_failure(
                metrics::TelemetryComponent::Tracing,
                metrics::TelemetryFailureKind::Init,
            );
            Err(err)
        }
    }
}
