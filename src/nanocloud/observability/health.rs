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

use serde::Serialize;
use std::time::Duration;

use crate::nanocloud::kubelet::Kubelet;
use crate::nanocloud::network::proxy;
use crate::nanocloud::oci::container_runtime;
use crate::nanocloud::server::bridge;

const ERROR_MAX_LEN: usize = 240;

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
    let mut components = Vec::with_capacity(4);

    match bridge::readiness_snapshot().await {
        Some(snapshot) => {
            if snapshot.ready {
                components.push(ComponentHealth::healthy("network_bridge"));
            } else {
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

                components.push(ComponentHealth::unhealthy("network_bridge", message));
            }
        }
        None => components.push(ComponentHealth::healthy("network_bridge")),
    }

    match proxy::health_check() {
        Ok(_) => components.push(ComponentHealth::healthy("service_proxy")),
        Err(err) => components.push(ComponentHealth::unhealthy("service_proxy", err)),
    }

    match container_runtime().list() {
        Ok(_) => components.push(ComponentHealth::healthy("container_runtime")),
        Err(err) => components.push(ComponentHealth::unhealthy("container_runtime", err)),
    }

    match Kubelet::shared().list_pods(None).await {
        Ok(_) => components.push(ComponentHealth::healthy("kubelet_store")),
        Err(err) => components.push(ComponentHealth::unhealthy("kubelet_store", err)),
    }

    let status = if components.iter().all(|component| component.healthy) {
        HealthStatus::Ready
    } else {
        HealthStatus::Degraded
    };

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

fn format_duration(duration: Duration) -> String {
    if duration.as_secs() >= 1 {
        format!("{:.1}s", duration.as_secs_f32())
    } else {
        format!("{}ms", duration.as_millis())
    }
}
