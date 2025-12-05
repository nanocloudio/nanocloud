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

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use serde::Serialize;
use std::time::Duration;

// Health endpoints:
// - `/readyz` returns HTTP 200 with component details when healthy or 503 with
//   JSON body describing failing dependencies.
// - `/healthz` returns HTTP 200 when the process is alive; it avoids expensive
//   dependency checks to remain lightweight.

use crate::nanocloud::kubelet::Kubelet;
use crate::nanocloud::logger::{log_error, log_warn};
use crate::nanocloud::network::proxy;
use crate::nanocloud::observability::metrics::{
    record_telemetry_failure, TelemetryComponent, TelemetryFailureKind,
};
use crate::nanocloud::oci::container_runtime;
use crate::nanocloud::server::bridge::{self, BridgeReadinessSnapshot};

const ERROR_MAX_LEN: usize = 240;
const COMPONENT: &str = "observability.health";
const COMPONENT_BRIDGE: &str = "network_bridge";
const COMPONENT_PROXY: &str = "service_proxy";
const COMPONENT_RUNTIME: &str = "container_runtime";
const COMPONENT_KUBELET: &str = "kubelet_store";

type AsyncCheck<T> = Box<dyn Fn() -> BoxFuture<'static, Result<T, String>> + Send + Sync>;
type SyncCheck<T> = Box<dyn Fn() -> Result<T, String> + Send + Sync>;

pub struct HealthDependencies {
    bridge: AsyncCheck<Option<bridge::BridgeReadinessSnapshot>>,
    proxy: SyncCheck<()>,
    runtime: SyncCheck<()>,
    kubelet: AsyncCheck<()>,
}

impl Default for HealthDependencies {
    fn default() -> Self {
        HealthDependencies::new()
            .with_bridge_check(Box::new(|| bridge::readiness_snapshot().map(Ok).boxed()))
            .with_proxy_check(Box::new(|| {
                proxy::health_check().map_err(|err| err.to_string())
            }))
            .with_runtime_check(Box::new(|| {
                container_runtime()
                    .list()
                    .map(|_| ())
                    .map_err(|err| err.to_string())
            }))
            .with_kubelet_check(Box::new(|| {
                async {
                    Kubelet::shared()
                        .list_pods(None)
                        .await
                        .map(|_| ())
                        .map_err(|err| err.to_string())
                }
                .boxed()
            }))
    }
}

impl HealthDependencies {
    pub fn new() -> Self {
        HealthDependencies {
            bridge: Box::new(|| async { Err("bridge check not configured".to_string()) }.boxed()),
            proxy: Box::new(|| Err("proxy check not configured".to_string())),
            runtime: Box::new(|| Err("runtime check not configured".to_string())),
            kubelet: Box::new(|| async { Err("kubelet check not configured".to_string()) }.boxed()),
        }
    }

    pub fn with_bridge_check(mut self, check: AsyncCheck<Option<BridgeReadinessSnapshot>>) -> Self {
        self.bridge = check;
        self
    }

    pub fn with_proxy_check(mut self, check: SyncCheck<()>) -> Self {
        self.proxy = check;
        self
    }

    pub fn with_runtime_check(mut self, check: SyncCheck<()>) -> Self {
        self.runtime = check;
        self
    }

    pub fn with_kubelet_check(mut self, check: AsyncCheck<()>) -> Self {
        self.kubelet = check;
        self
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ComponentHealth {
    pub name: &'static str,
    pub healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ComponentHealth {
    fn healthy(name: &'static str) -> Self {
        ComponentHealth {
            name,
            healthy: true,
            error: None,
        }
    }

    fn unhealthy(name: &'static str, err: impl ToString) -> Self {
        let mut message = err.to_string();
        if message.len() > ERROR_MAX_LEN {
            message.truncate(ERROR_MAX_LEN);
        }
        ComponentHealth {
            name,
            healthy: false,
            error: Some(message),
        }
    }
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Ready,
    Degraded,
}

#[derive(Clone, Debug, Serialize)]
pub struct HealthReport {
    pub status: HealthStatus,
    pub components: Vec<ComponentHealth>,
}

impl HealthReport {
    pub fn is_ready(&self) -> bool {
        self.status == HealthStatus::Ready
    }
}

/// Returns a readiness assessment used by `/readyz` and `/healthz`.
pub async fn readiness_report() -> HealthReport {
    readiness_report_with(&HealthDependencies::default()).await
}

/// Returns a readiness assessment using the provided dependency hooks. This is
/// primarily exposed for tests to avoid calling real dependencies.
pub async fn readiness_report_with(dependencies: &HealthDependencies) -> HealthReport {
    let mut components = Vec::with_capacity(4);

    match (dependencies.bridge)().await {
        Ok(Some(snapshot)) => {
            components.push(bridge_health_from_snapshot(snapshot));
        }
        Ok(None) => components.push(ComponentHealth::healthy(COMPONENT_BRIDGE)),
        Err(err) => components.push(ComponentHealth::unhealthy(COMPONENT_BRIDGE, err)),
    }

    match (dependencies.proxy)() {
        Ok(_) => components.push(ComponentHealth::healthy(COMPONENT_PROXY)),
        Err(err) => components.push(ComponentHealth::unhealthy(COMPONENT_PROXY, err)),
    }

    match (dependencies.runtime)() {
        Ok(_) => components.push(ComponentHealth::healthy(COMPONENT_RUNTIME)),
        Err(err) => components.push(ComponentHealth::unhealthy(COMPONENT_RUNTIME, err)),
    }

    match (dependencies.kubelet)().await {
        Ok(_) => components.push(ComponentHealth::healthy(COMPONENT_KUBELET)),
        Err(err) => components.push(ComponentHealth::unhealthy(COMPONENT_KUBELET, err)),
    }

    for component in &components {
        if !component.healthy {
            if let Some(error) = component.error.as_deref() {
                log_dependency_error(component.name, error);
            }
        }
    }

    let status = if components.iter().all(|component| component.healthy) {
        HealthStatus::Ready
    } else {
        HealthStatus::Degraded
    };

    if status == HealthStatus::Degraded {
        record_telemetry_failure(TelemetryComponent::Health, TelemetryFailureKind::Check);
    }

    HealthReport { status, components }
}

/// Liveness probes check that the process is servicing requests. They do not
/// perform external dependency checks to remain lightweight.
pub fn liveness_report() -> HealthReport {
    HealthReport {
        status: HealthStatus::Ready,
        components: vec![ComponentHealth::healthy("process")],
    }
}

/// Ready-made handlers for Axum services exposing `/readyz` and `/healthz`.
#[allow(dead_code)]
pub fn axum_routes() -> Router {
    Router::new()
        .route("/readyz", get(readiness_response))
        .route("/healthz", get(liveness_response))
}

/// Returns an Axum response for the readiness endpoint.
pub async fn readiness_response() -> impl IntoResponse {
    let report = readiness_report().await;
    let status = readiness_status(&report);
    (status, Json(report))
}

/// Returns an Axum response for the liveness endpoint.
pub async fn liveness_response() -> impl IntoResponse {
    let report = liveness_report();
    (StatusCode::OK, Json(report))
}

pub fn readiness_status(report: &HealthReport) -> StatusCode {
    if report.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

fn bridge_health_from_snapshot(snapshot: BridgeReadinessSnapshot) -> ComponentHealth {
    if snapshot.ready {
        return ComponentHealth::healthy(COMPONENT_BRIDGE);
    }

    let attempts = snapshot.attempts;
    let elapsed = snapshot
        .started_at
        .map(|start| format_duration(start.elapsed()))
        .unwrap_or_else(|| "unknown".to_string());
    let mut message = format!(
        "waiting {} (attempt {}) for {} carrier=UP {}",
        elapsed, attempts, snapshot.bridge_name, snapshot.expected_cidr
    );

    if let Some(observation) = snapshot.last_observation {
        let carrier = observation
            .carrier
            .map(|state| if state { "UP" } else { "DOWN" })
            .unwrap_or("UNKNOWN");
        let operstate = observation.operstate.as_deref().unwrap_or("UNKNOWN");
        let addresses = if observation.addresses.is_empty() {
            "none".to_string()
        } else {
            observation.addresses.join(",")
        };
        message.push_str(&format!(
            "; last carrier={} operstate={} addr={}",
            carrier, operstate, addresses
        ));
        if !observation.has_expected_cidr {
            message.push_str(" missing_expected_cidr");
        }
    }

    if let Some(err) = snapshot.last_error {
        message.push_str(&format!("; error={}", err));
    }

    if let Some(last_check) = snapshot.last_attempt_completed {
        message.push_str(&format!(
            "; last_check={} ago",
            format_duration(last_check.elapsed())
        ));
    }

    log_warn(
        COMPONENT,
        "Network bridge is not ready",
        &[
            ("bridge", snapshot.bridge_name),
            ("expected_cidr", snapshot.expected_cidr),
            ("details", message.as_str()),
        ],
    );

    ComponentHealth::unhealthy(COMPONENT_BRIDGE, message)
}

fn log_dependency_error(component: &'static str, error: &str) {
    log_error(
        COMPONENT,
        "Health dependency check failed",
        &[("dependency", component), ("error", error)],
    );
}

fn format_duration(duration: Duration) -> String {
    if duration.as_secs() >= 1 {
        format!("{:.1}s", duration.as_secs_f32())
    } else {
        format!("{}ms", duration.as_millis())
    }
}
