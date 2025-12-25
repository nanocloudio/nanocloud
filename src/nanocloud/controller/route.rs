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

//! Route controller for edge ingress routing.
//!
//! Watches Route CRDs, validates their configuration, resolves backend service
//! endpoints, and persists derived proxy configuration in the store. Emits
//! Kubernetes-style events on reconciliation successes/failures.

use crate::nanocloud::controller::events::{EventRecorder, InvolvedObjectRef};
use crate::nanocloud::controller::runtime::{
    ControllerRuntime, ControllerTarget, ControllerWorkItem,
};
use crate::nanocloud::controller::watch::{ControllerWatchEvent, ControllerWatchManager};
use crate::nanocloud::k8s::route::Route;
use crate::nanocloud::k8s::store::{get_route, list_routes, normalize_namespace, save_route};
use crate::nanocloud::logger::{log_debug, log_error, log_info, log_warn};
use crate::nanocloud::observability::metrics::{self, ControllerReconcileResult};
use crate::nanocloud::util::KeyspaceEventType;

use std::io;
use std::sync::Arc;
use tokio::task::JoinHandle;

const COMPONENT: &str = "route-controller";
const ROUTE_PREFIX: &str = "/routes";

/// Spawns the Route controller background task.
pub fn spawn() -> JoinHandle<()> {
    tokio::spawn(async move {
        let runtime = ControllerRuntime::shared();
        let recorder = EventRecorder::new(COMPONENT);

        start_route_executor(&runtime, recorder.clone());
        bootstrap_existing_routes(&runtime).await;
        watch_route_events(runtime, recorder).await;
    })
}

fn start_route_executor(runtime: &Arc<ControllerRuntime>, recorder: EventRecorder) {
    if let Err(err) = runtime.spawn_executor(move |item| {
        let recorder = recorder.clone();
        async move {
            if let ControllerTarget::Route { namespace, name } = &item.target {
                if let Err(err) =
                    reconcile_route(namespace.clone(), name.clone(), recorder.clone()).await
                {
                    log_error(
                        COMPONENT,
                        "Route reconciliation failed",
                        &[
                            ("namespace", namespace.as_deref().unwrap_or("default")),
                            ("route", name.as_str()),
                            ("error", err.as_str()),
                        ],
                    );
                    return Err(
                        Box::new(io::Error::other(err)) as Box<dyn std::error::Error + Send + Sync>
                    );
                }
            }
            Ok(())
        }
    }) {
        log_error(
            COMPONENT,
            "Failed to start route dispatcher",
            &[("error", err.to_string().as_str())],
        );
    }
}

async fn bootstrap_existing_routes(runtime: &Arc<ControllerRuntime>) {
    match list_routes() {
        Ok(existing) => {
            if !existing.is_empty() {
                log_info(
                    COMPONENT,
                    "Reconciling existing Routes on startup",
                    &[("count", existing.len().to_string().as_str())],
                );
            }
            for stored in existing {
                enqueue_route(runtime, stored.namespace, stored.name).await;
            }
        }
        Err(err) => {
            log_error(
                COMPONENT,
                "Failed to list existing Routes",
                &[("error", err.to_string().as_str())],
            );
        }
    }
}

async fn watch_route_events(runtime: Arc<ControllerRuntime>, recorder: EventRecorder) {
    let manager = ControllerWatchManager::shared();
    let mut subscription = manager.subscribe(ROUTE_PREFIX, None);

    while let Some(event) = subscription.recv().await {
        match event.event_type {
            KeyspaceEventType::Deleted => {
                if let Some((ns, name)) = parse_route_key(event.key.as_str()) {
                    let ns_label = ns.clone().unwrap_or_else(|| "default".to_string());
                    log_debug(
                        COMPONENT,
                        "Route deleted",
                        &[("namespace", ns_label.as_str()), ("route", name.as_str())],
                    );
                    enqueue_route(&runtime, ns, name).await;
                }
            }
            KeyspaceEventType::Added | KeyspaceEventType::Modified => {
                if let Some((namespace, name)) = route_identity(&event) {
                    enqueue_route(&runtime, namespace, name).await;
                } else {
                    log_warn(
                        COMPONENT,
                        "Route event missing identity",
                        &[("key", event.key.as_str())],
                    );
                }
            }
        }
    }

    drop(recorder);
}

fn route_identity(event: &ControllerWatchEvent) -> Option<(Option<String>, String)> {
    if let Some(value) = event.value.as_ref() {
        if let Ok(route) = serde_json::from_str::<Route>(value) {
            let name = route.metadata.name.clone().unwrap_or_default();
            if !name.is_empty() {
                return Some((route.metadata.namespace.clone(), name));
            }
        }
    }
    parse_route_key(event.key.as_str())
}

fn parse_route_key(key: &str) -> Option<(Option<String>, String)> {
    let parts: Vec<&str> = key
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    if parts.len() != 3 || parts[0] != ROUTE_PREFIX.trim_start_matches('/') {
        return None;
    }
    let namespace = if parts[1].eq_ignore_ascii_case("default") {
        Some("default".to_string())
    } else {
        Some(parts[1].to_string())
    };
    let name = parts[2].to_string();
    Some((namespace, name))
}

async fn enqueue_route(runtime: &Arc<ControllerRuntime>, namespace: Option<String>, name: String) {
    let item = ControllerWorkItem::route(namespace.as_deref(), name.as_str());
    match runtime.work_queue().enqueue(item).await {
        Ok(true) => {}
        Ok(false) => {
            log_debug(
                COMPONENT,
                "Coalesced route reconciliation request",
                &[
                    ("namespace", namespace.as_deref().unwrap_or("default")),
                    ("route", name.as_str()),
                ],
            );
        }
        Err(err) => {
            log_warn(
                COMPONENT,
                "Failed to enqueue route reconciliation",
                &[
                    ("namespace", namespace.as_deref().unwrap_or("default")),
                    ("route", name.as_str()),
                    ("error", err.to_string().as_str()),
                ],
            );
        }
    }
}

async fn reconcile_route(
    namespace: Option<String>,
    name: String,
    recorder: EventRecorder,
) -> Result<(), String> {
    let namespace_label = normalize_namespace(namespace.as_deref());

    let Some(mut route) = get_route(namespace.as_deref(), &name).map_err(|e| e.to_string())? else {
        log_debug(
            COMPONENT,
            "Route missing during reconciliation",
            &[
                ("namespace", namespace_label.as_str()),
                ("route", name.as_str()),
            ],
        );
        metrics::record_controller_reconcile("route", ControllerReconcileResult::Success);
        return Ok(());
    };

    log_info(
        COMPONENT,
        "Reconciling route",
        &[
            ("namespace", namespace_label.as_str()),
            ("name", name.as_str()),
            ("host", route.spec.host.as_str()),
        ],
    );

    // Validate the route specification
    let validation_result = route.validate();
    let (is_ready, reason, message) = match validation_result {
        Ok(()) => {
            // Route is valid - mark as ready
            (true, None, None)
        }
        Err(err) => {
            // Route validation failed
            (false, Some("ValidationFailed"), Some(err.to_string()))
        }
    };

    // Update the route status
    let mut status = route.status.take().unwrap_or_default();
    status.set_ready(is_ready, reason, message.as_deref());

    // For valid routes, attempt to resolve the backend endpoint
    if is_ready {
        let endpoint = format!("{}:{}", route.spec.service.name, route.spec.service.port);
        status.resolved_endpoint = Some(endpoint);
    } else {
        status.resolved_endpoint = None;
    }

    route.status = Some(status.clone());

    // Persist the updated route
    if let Err(err) = save_route(namespace.as_deref(), &name, route.clone()) {
        log_error(
            COMPONENT,
            "Failed to save route status",
            &[
                ("namespace", namespace_label.as_str()),
                ("route", name.as_str()),
                ("error", err.to_string().as_str()),
            ],
        );
        return Err(err.to_string());
    }

    // Emit Kubernetes event
    let involved = InvolvedObjectRef {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "Route".to_string(),
        name: name.clone(),
        uid: route.metadata.uid.clone(),
        namespace: Some(namespace_label.clone()),
    };

    let (event_reason, event_type, event_message) = if is_ready {
        (
            "RouteReconciled",
            "Normal",
            format!(
                "Route {} reconciled successfully, forwarding {} to {}",
                name,
                route.spec.host,
                status.resolved_endpoint.as_deref().unwrap_or("unknown")
            ),
        )
    } else {
        (
            "RouteValidationFailed",
            "Warning",
            format!(
                "Route {} validation failed: {}",
                name,
                message.as_deref().unwrap_or("unknown error")
            ),
        )
    };

    recorder
        .record(
            Some(namespace_label.as_str()),
            &involved,
            event_reason,
            event_type,
            event_message.as_str(),
        )
        .await;

    let result = if is_ready {
        ControllerReconcileResult::Success
    } else {
        ControllerReconcileResult::Error
    };
    metrics::record_controller_reconcile("route", result);

    if is_ready {
        log_info(
            COMPONENT,
            "Route reconciled",
            &[
                ("namespace", namespace_label.as_str()),
                ("name", name.as_str()),
                ("ready", "true"),
            ],
        );
    } else {
        log_warn(
            COMPONENT,
            "Route not ready",
            &[
                ("namespace", namespace_label.as_str()),
                ("name", name.as_str()),
                ("reason", message.as_deref().unwrap_or("unknown")),
            ],
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_route_key_extracts_namespace_and_name() {
        let result = parse_route_key("/routes/default/my-route");
        assert_eq!(
            result,
            Some((Some("default".to_string()), "my-route".to_string()))
        );

        let result = parse_route_key("/routes/production/api-gateway");
        assert_eq!(
            result,
            Some((Some("production".to_string()), "api-gateway".to_string()))
        );
    }

    #[test]
    fn parse_route_key_returns_none_for_invalid() {
        assert_eq!(parse_route_key("/other/default/name"), None);
        assert_eq!(parse_route_key("/routes/default"), None);
        assert_eq!(parse_route_key(""), None);
    }
}
