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

//! Nanocloud HTTP API Server
//!
//! This module implements the HTTP server for the Nanocloud API, providing
//! Kubernetes-compatible endpoints for managing containers, pods, and services.
//!
//! # Architecture
//!
//! The server is built on [Axum](https://docs.rs/axum) and provides:
//! - RESTful API endpoints following Kubernetes API conventions
//! - TLS with mutual authentication (mTLS) support
//! - Multiple authentication mechanisms (certificates, JWT, bootstrap tokens)
//! - Streaming endpoints for logs, exec, and watch operations
//!
//! # Routes Overview
//!
//! ## Core API Routes (`/api/v1`)
//!
//! | Method | Path | Description | Auth Required |
//! |--------|------|-------------|---------------|
//! | GET | `/api/v1/pods` | List all pods | Yes |
//! | GET | `/api/v1/namespaces/{ns}/pods` | List pods in namespace | Yes |
//! | GET | `/api/v1/namespaces/{ns}/pods/{name}` | Get pod by name | Yes |
//! | POST | `/api/v1/namespaces/{ns}/pods` | Create pod | Yes |
//! | DELETE | `/api/v1/namespaces/{ns}/pods/{name}` | Delete pod | Yes |
//! | GET | `/api/v1/namespaces/{ns}/pods/{name}/log` | Stream pod logs | Yes |
//! | POST | `/api/v1/namespaces/{ns}/pods/{name}/exec` | Execute command | Yes |
//!
//! ## Services
//!
//! | Method | Path | Description | Auth Required |
//! |--------|------|-------------|---------------|
//! | GET | `/api/v1/services` | List all services | Yes |
//! | GET | `/api/v1/namespaces/{ns}/services` | List services in namespace | Yes |
//! | GET | `/api/v1/namespaces/{ns}/services/{name}` | Get service | Yes |
//! | POST | `/api/v1/namespaces/{ns}/services` | Create service | Yes |
//! | DELETE | `/api/v1/namespaces/{ns}/services/{name}` | Delete service | Yes |
//!
//! ## ConfigMaps and Secrets
//!
//! | Method | Path | Description | Auth Required |
//! |--------|------|-------------|---------------|
//! | GET | `/api/v1/namespaces/{ns}/configmaps` | List configmaps | Yes |
//! | GET | `/api/v1/namespaces/{ns}/configmaps/{name}` | Get configmap | Yes |
//! | GET | `/api/v1/namespaces/{ns}/secrets` | List secrets | Yes |
//! | GET | `/api/v1/namespaces/{ns}/secrets/{name}` | Get secret | Yes |
//!
//! ## Watch Endpoints
//!
//! Watch endpoints support long-polling for resource changes:
//! - Add `?watch=true` to list endpoints to receive streaming updates
//! - Supports `resourceVersion` for resumable watches
//! - Emits bookmark events for checkpoint synchronization
//!
//! ## Health and Metrics
//!
//! | Method | Path | Description | Auth Required |
//! |--------|------|-------------|---------------|
//! | GET | `/healthz` | Liveness probe | No |
//! | GET | `/readyz` | Readiness probe | No |
//! | GET | `/metrics` | Prometheus metrics | Optional |
//!
//! # Authentication
//!
//! The server supports multiple authentication mechanisms:
//!
//! ## Client Certificates (mTLS)
//!
//! The primary authentication method uses client certificates issued by
//! the cluster CA. The certificate's Common Name (CN) is used as the
//! principal identity.
//!
//! - Certificates with `CN=device:*` prefix are treated as device identities
//! - Other certificates are treated as user/service identities
//!
//! ## JWT Bearer Tokens
//!
//! Service account tokens can be used for API authentication:
//!
//! ```text
//! Authorization: Bearer <jwt-token>
//! ```
//!
//! Tokens must be signed by the cluster's service account key and include:
//! - `sub`: Subject (service account name)
//! - `iss`: Issuer (cluster identifier)
//! - `scope`: List of granted scopes
//!
//! ## Bootstrap Tokens
//!
//! One-time tokens for initial node/device registration:
//!
//! ```text
//! Authorization: Bearer <token-id>.<token-secret>
//! ```
//!
//! Bootstrap tokens are consumed on first use and cannot be reused.
//!
//! # TLS Configuration
//!
//! The server requires TLS and loads certificates from the secure assets directory:
//!
//! ## Environment Variables
//!
//! - `NANOCLOUD_SECURE_ASSETS`: Path to directory containing TLS assets
//!
//! ## Required Files
//!
//! - `ca.pem`: Cluster CA certificate
//! - `server.pem`: Server certificate
//! - `server-key.pem`: Server private key
//!
//! ## Certificate Requirements
//!
//! - Server certificate must include SANs for all listen addresses
//! - Client certificates must be signed by the cluster CA
//! - ALPN negotiation supports `http/1.1`
//!
//! # Error Responses
//!
//! All errors follow Kubernetes API conventions:
//!
//! ```json
//! {
//!   "kind": "Status",
//!   "apiVersion": "v1",
//!   "status": "Failure",
//!   "message": "error description",
//!   "reason": "BadRequest",
//!   "code": 400
//! }
//! ```
//!
//! ## Common Status Codes
//!
//! | Code | Reason | Description |
//! |------|--------|-------------|
//! | 400 | BadRequest | Invalid request parameters |
//! | 401 | Unauthorized | Missing or invalid credentials |
//! | 403 | Forbidden | Authenticated but not authorized |
//! | 404 | NotFound | Resource does not exist |
//! | 409 | Conflict | Resource version conflict |
//! | 422 | UnprocessableEntity | Valid syntax but semantic error |
//! | 429 | TooManyRequests | Rate limit exceeded |
//! | 500 | InternalError | Server-side error |
//! | 503 | ServiceUnavailable | Server temporarily unavailable |
//!
//! # Rate Limiting
//!
//! The server implements per-endpoint rate limiting:
//!
//! - General endpoints: 1000 concurrent requests
//! - Streaming endpoints: 100 concurrent connections
//! - Exec endpoints: 50 concurrent sessions
//! - Watch endpoints: 200 concurrent watchers
//!
//! Requests exceeding limits receive HTTP 429 responses.
//!
//! # Streaming Endpoints
//!
//! Streaming endpoints (`/log`, `/exec`, watch) support:
//!
//! - Configurable timeouts (startup, inactivity, max duration)
//! - Backpressure via bounded channels
//! - Automatic cleanup on client disconnect
//! - Graceful shutdown coordination

use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;

use serde_json::Value;
use tokio_stream::StreamExt;

pub(crate) mod api_routes;
pub(crate) mod bridge;
pub mod handlers;
mod tls;

pub mod auth;

const EVENT_LOGGER_COMPONENT: &str = "server-event-listener";

use self::auth::bootstrap::spawn_bootstrap_token_maintenance;
use crate::nanocloud::cni::cni_plugin;
use crate::nanocloud::controller::runtime::ControllerRuntime;
use crate::nanocloud::diagnostics;
use crate::nanocloud::dns::{self, DnsConfig, DnsService};
use crate::nanocloud::events::in_memory::InMemoryEventBus;
use crate::nanocloud::events::{
    EventError, EventSubscriber, EventTopic, EventType, SubscriptionOptions,
};
use crate::nanocloud::http::{AppState, ServerBuilder};
use crate::nanocloud::http_middleware::MiddlewareStack;
use crate::nanocloud::k8s::event::{
    Event as KubeEvent, EventRegistry, EventSource, ObjectReference,
};
use crate::nanocloud::k8s::pod::ObjectMeta;
use crate::nanocloud::kubelet::Kubelet;
use crate::nanocloud::logger::{log_info, log_warn};
use crate::nanocloud::observability::metrics;
use crate::nanocloud::util::error::with_context;

#[derive(Clone)]
pub struct ServerConfig {
    pub http_listen: SocketAddr,
    pub dns: DnsConfig,
}

async fn ensure_runtime_prerequisites() -> Result<(), Box<dyn Error + Send + Sync>> {
    const BRIDGE_NAME: &str = "nanocloud0";
    const BRIDGE_CIDR: &str = "172.20.0.1/16";

    log_info(
        "server",
        "Ensuring network bridge",
        &[("bridge", BRIDGE_NAME), ("cidr", BRIDGE_CIDR)],
    );

    let plugin = cni_plugin();
    let bridge_result =
        tokio::task::spawn_blocking(move || plugin.bridge(BRIDGE_NAME, BRIDGE_CIDR))
            .await
            .map_err(|err| {
                with_context(
                    err,
                    format!("Failed to join network bridge repair task for {BRIDGE_NAME}"),
                )
            })?;
    bridge_result?;

    bridge::wait_for_bridge_ready(BRIDGE_NAME, BRIDGE_CIDR)
        .await
        .map_err(|e| {
            with_context(
                e,
                "Timed out waiting for network bridge to report carrier UP",
            )
        })?;

    log_info(
        "server",
        "Network bridge ready",
        &[("bridge", BRIDGE_NAME), ("cidr", BRIDGE_CIDR)],
    );

    Ok(())
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn require_client_certificate() -> bool {
    match env::var("NANOCLOUD_REQUIRE_CLIENT_CERTIFICATE") {
        Ok(value) => {
            let trimmed = value.trim();
            match parse_bool(trimmed) {
                Some(result) => result,
                None => {
                    log_warn(
                        "server",
                        "Invalid NANOCLOUD_REQUIRE_CLIENT_CERTIFICATE value; defaulting to optional client certificates",
                        &[("value", trimmed)],
                    );
                    false
                }
            }
        }
        Err(_) => false,
    }
}

fn bundle_envelope_to_event(
    envelope: &crate::nanocloud::events::EventEnvelope,
    status: &str,
    namespace: &str,
    bundle: &str,
    component: Option<&str>,
    payload: Option<&serde_json::Map<String, serde_json::Value>>,
) -> KubeEvent {
    let timestamp = envelope.timestamp.to_rfc3339();
    let is_error = status.eq_ignore_ascii_case("error");
    let message = if is_error {
        payload
            .and_then(|map| map.get("error").and_then(|value| value.as_str()))
            .map(|value| value.to_string())
            .unwrap_or_else(|| format!("Bundle {} reconciliation failed", bundle))
    } else if let Some(phase) =
        payload.and_then(|map| map.get("phase").and_then(|value| value.as_str()))
    {
        format!("Bundle {} reconciled to phase {}", bundle, phase)
    } else {
        format!("Bundle {} reconciled successfully", bundle)
    };

    let resource_version = payload
        .and_then(|map| map.get("resourceVersion").and_then(|value| value.as_str()))
        .map(|value| value.to_string());

    let reporting_component = component
        .map(|value| value.to_string())
        .or_else(|| Some(EVENT_LOGGER_COMPONENT.to_string()));

    let event_type = if is_error { "Warning" } else { "Normal" };
    let reason = payload
        .and_then(|map| map.get("reason").and_then(|value| value.as_str()))
        .map(|value| value.to_string())
        .unwrap_or_else(|| {
            if is_error {
                "BundleReconcileFailed".to_string()
            } else {
                "BundleReconciled".to_string()
            }
        });

    let involved_object = ObjectReference {
        api_version: Some("nanocloud.io/v1".to_string()),
        kind: Some("Bundle".to_string()),
        name: Some(bundle.to_string()),
        namespace: Some(namespace.to_string()),
        uid: Some(format!("bundle:{}/{}", namespace, bundle)),
        resource_version,
        field_path: None,
    };

    let source = component.map(|value| EventSource {
        component: Some(value.to_string()),
        host: None,
    });

    KubeEvent {
        api_version: "v1".to_string(),
        kind: "Event".to_string(),
        metadata: ObjectMeta {
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        involved_object,
        reason: Some(reason.to_string()),
        message: Some(message),
        event_type: Some(event_type.to_string()),
        first_timestamp: Some(timestamp.clone()),
        last_timestamp: Some(timestamp.clone()),
        event_time: Some(timestamp.clone()),
        count: Some(1),
        reporting_component,
        reporting_instance: Some(envelope.topic.full_name()),
        action: Some("Reconcile".to_string()),
        related: None,
        series: None,
        source: source.clone(),
        deprecated_source: source,
        deprecated_first_timestamp: Some(timestamp.clone()),
        deprecated_last_timestamp: Some(timestamp.clone()),
        deprecated_count: Some(1),
    }
}

fn spawn_event_logger() {
    let bus = InMemoryEventBus::global();
    let topic = EventTopic::new("controller", "bundles.reconcile");
    let topic_label = topic.full_name();
    let subscription = match bus.subscribe(&topic, SubscriptionOptions::default()) {
        Ok(subscription) => subscription,
        Err(err) => {
            log_warn(
                EVENT_LOGGER_COMPONENT,
                "Failed to subscribe to controller events",
                &[("error", err.to_string().as_str())],
            );
            return;
        }
    };

    let registry = EventRegistry::shared();

    tokio::spawn(async move {
        let topic_label = topic_label;
        let mut stream = subscription.stream;
        let registry = Arc::clone(&registry);
        while let Some(event) = stream.next().await {
            match event {
                Ok(envelope) => {
                    let topic_name = envelope.topic.full_name();
                    let key_partition = envelope.key.partition.clone();
                    let key_id = envelope.key.id.clone();
                    let content_type = envelope.content_type.to_string();
                    let timestamp = envelope.timestamp.to_rfc3339();
                    let event_type = match &envelope.event_type {
                        EventType::Updated => "updated".to_string(),
                        EventType::Custom(name) => {
                            format!("custom:{name}")
                        }
                    };
                    let status_attr = envelope
                        .attributes
                        .get("status")
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_string());
                    let namespace_attr = envelope
                        .attributes
                        .get("namespace")
                        .cloned()
                        .unwrap_or_else(|| "".to_string());
                    let bundle_attr = envelope
                        .attributes
                        .get("bundle")
                        .cloned()
                        .unwrap_or_else(|| "".to_string());

                    metrics::record_event_consume(&topic_label, &status_attr);

                    let mut metadata_pairs = vec![
                        ("topic".to_string(), topic_name),
                        ("status".to_string(), status_attr.clone()),
                        ("namespace".to_string(), namespace_attr.clone()),
                        ("bundle".to_string(), bundle_attr.clone()),
                        ("event_type".to_string(), event_type),
                        ("key_partition".to_string(), key_partition),
                        ("key_id".to_string(), key_id),
                        ("content_type".to_string(), content_type),
                        ("timestamp".to_string(), timestamp),
                    ];

                    if let Some(trace_id) = envelope.trace_id.as_ref() {
                        metadata_pairs.push(("trace_id".to_string(), trace_id.clone()));
                    }

                    let payload_value = serde_json::from_slice::<Value>(&envelope.payload).ok();
                    let payload_object = payload_value.as_ref().and_then(|value| value.as_object());

                    if let Some(err) = payload_object
                        .and_then(|map| map.get("error"))
                        .and_then(|value| value.as_str())
                    {
                        metadata_pairs.push(("error".to_string(), err.to_string()));
                    }

                    let metadata_refs: Vec<(&str, &str)> = metadata_pairs
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_str()))
                        .collect();

                    if status_attr == "error" {
                        log_warn(
                            EVENT_LOGGER_COMPONENT,
                            "Controller bundle reconcile reported error",
                            &metadata_refs,
                        );
                    } else {
                        log_info(
                            EVENT_LOGGER_COMPONENT,
                            "Controller bundle reconcile event",
                            &metadata_refs,
                        );
                    }

                    let namespace_for_event = if namespace_attr.is_empty() {
                        "default"
                    } else {
                        namespace_attr.as_str()
                    };
                    let bundle_for_event = if bundle_attr.is_empty() {
                        envelope.key.id.as_str()
                    } else {
                        bundle_attr.as_str()
                    };
                    let component_attr = envelope.attributes.get("component").map(|s| s.as_str());
                    let kube_event = bundle_envelope_to_event(
                        &envelope,
                        &status_attr,
                        namespace_for_event,
                        bundle_for_event,
                        component_attr,
                        payload_object,
                    );
                    registry.record(kube_event).await;
                }
                Err(EventError::Canceled) => break,
                Err(err) => {
                    metrics::record_event_stream_error(&topic_label, "consumer_error");
                    log_warn(
                        EVENT_LOGGER_COMPONENT,
                        "Event stream dropped",
                        &[("error", err.to_string().as_str())],
                    );
                }
            }
        }
    });
}

/// Main entry point for the API server.
///
/// This function initializes all server components (DNS, controllers, etc.)
/// and starts the HTTP server using the shared `http` primitives.
pub async fn serve(config: ServerConfig) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Initialize runtime prerequisites
    ensure_runtime_prerequisites().await?;
    diagnostics::reconcile_cni_artifacts_on_startup()
        .await
        .map_err(|e| with_context(e, "Failed to reconcile CNI artifacts during startup"))?;

    // Initialize DNS service
    let runtime = ControllerRuntime::shared();
    let dns_service = Arc::new(DnsService::new(config.dns.clone()));
    let _ = runtime.register_dependency(Arc::clone(&dns_service));
    let dns_listen = format!("{}:{}", config.dns.listen_address, config.dns.listen_port);
    let upstream_count = config.dns.upstream_servers.len().to_string();
    log_info(
        "server",
        "DNS configuration initialized",
        &[
            ("cluster_domain", config.dns.cluster_domain.as_str()),
            ("listen", dns_listen.as_str()),
            ("upstream_servers", upstream_count.as_str()),
        ],
    );
    let _dns_handle = dns::server::start(Arc::clone(&dns_service))
        .await
        .map_err(|e| with_context(e, "Failed to start DNS listeners"))?;

    // Spawn controllers
    drop(crate::nanocloud::controller::bundle::spawn());
    drop(crate::nanocloud::controller::snapshot::spawn());
    drop(crate::nanocloud::controller::endpoints::spawn());
    drop(crate::nanocloud::controller::networkpolicy::spawn());
    drop(crate::nanocloud::controller::statefulset::spawn());
    #[cfg(feature = "edge")]
    {
        drop(crate::nanocloud::controller::route::spawn());
        drop(crate::nanocloud::controller::webhook::spawn());
    }
    spawn_event_logger();
    spawn_bootstrap_token_maintenance();

    // Spawn edge server if enabled
    #[cfg(feature = "edge")]
    {
        let edge_config = crate::nanocloud::edge::EdgeConfig::from_env();
        let edge_state = std::sync::Arc::new(crate::nanocloud::edge::EdgeState::new());
        tokio::spawn(async move {
            if let Err(e) = crate::nanocloud::edge::serve(edge_config, edge_state).await {
                log_warn(
                    "server",
                    "Edge server failed",
                    &[("error", e.to_string().as_str())],
                );
            }
        });
    }

    // Build the API router with middleware
    let router = api_routes::build_api_router();
    let app = MiddlewareStack::new().apply(router);

    // Determine TLS configuration
    let require_client_cert = require_client_certificate();
    if require_client_cert {
        log_info("server", "Client certificates required", &[]);
    } else {
        log_warn(
            "server",
            "Client certificates optional; relying on secondary authentication",
            &[("env_var", "NANOCLOUD_REQUIRE_CLIENT_CERTIFICATE")],
        );
    }

    // Restore workload state before accepting connections
    let kubelet = Kubelet::shared();
    kubelet
        .restore_state()
        .await
        .map_err(|e| with_context(e, "Failed to restore workload state"))?;

    // Build and start the server using shared http primitives
    let server = ServerBuilder::new()
        .bind(config.http_listen)
        .tls_identity("nanocloud-server", &["localhost", "127.0.0.1", "::1"])
        .require_client_certificate(require_client_cert)
        .build(app, AppState::new())?;

    server.serve().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bool_accepts_common_truthy_values() {
        assert_eq!(parse_bool("1"), Some(true));
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("TRUE"), Some(true));
        assert_eq!(parse_bool("yes"), Some(true));
        assert_eq!(parse_bool("on"), Some(true));
    }

    #[test]
    fn parse_bool_accepts_common_falsy_values() {
        assert_eq!(parse_bool("0"), Some(false));
        assert_eq!(parse_bool("false"), Some(false));
        assert_eq!(parse_bool("FALSE"), Some(false));
        assert_eq!(parse_bool("no"), Some(false));
        assert_eq!(parse_bool("off"), Some(false));
    }

    #[test]
    fn parse_bool_returns_none_for_invalid_values() {
        assert_eq!(parse_bool("invalid"), None);
        assert_eq!(parse_bool("2"), None);
        assert_eq!(parse_bool(""), None);
    }

    #[test]
    fn parse_bool_trims_whitespace() {
        assert_eq!(parse_bool("  true  "), Some(true));
        assert_eq!(parse_bool("\tfalse\n"), Some(false));
    }
}
