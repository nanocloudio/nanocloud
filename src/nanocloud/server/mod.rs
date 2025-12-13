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

use std::convert::Infallible;
use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use axum::middleware;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use futures_util::future::{self, Either};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as HyperAcceptor;
use hyper_util::service::TowerToHyperService;
use openssl::x509::X509VerifyResult;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_stream::StreamExt;

pub(crate) mod bridge;
pub mod handlers;
mod tls;

pub mod auth;

const EVENT_LOGGER_COMPONENT: &str = "server-event-listener";

use self::auth::{bootstrap::spawn_bootstrap_token_maintenance, AuthLayer, ClientCertificate};
use crate::nanocloud::cni::cni_plugin;
use crate::nanocloud::controller::runtime::ControllerRuntime;
use crate::nanocloud::diagnostics;
use crate::nanocloud::dns::{self, DnsConfig, DnsService};
use crate::nanocloud::events::in_memory::InMemoryEventBus;
use crate::nanocloud::events::{
    EventError, EventSubscriber, EventTopic, EventType, SubscriptionOptions,
};
use crate::nanocloud::k8s::event::{
    Event as KubeEvent, EventRegistry, EventSource, ObjectReference,
};
use crate::nanocloud::k8s::pod::ObjectMeta;
use crate::nanocloud::kubelet::Kubelet;
use crate::nanocloud::logger::{log_error, log_info, log_warn};
use crate::nanocloud::observability::{metrics, tracing};
use crate::nanocloud::server::handlers::ApiError;
use crate::nanocloud::util::error::with_context;
use tls::{accept_with_tls, build_tls_acceptor};
use tower::Service;

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

fn unauthorized_response(message: &str) -> Response {
    ApiError::with_reason(StatusCode::UNAUTHORIZED, "Unauthorized", message).into_response()
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

#[derive(Clone)]
enum CertificateState {
    Valid(ClientCertificate),
    Missing,
    Invalid { reason: String },
}

#[derive(Clone)]
struct InjectClientCertificate<S> {
    inner: S,
    certificate_state: CertificateState,
    require_client_certificate: bool,
}

impl<S> InjectClientCertificate<S> {
    fn new(
        inner: S,
        certificate_state: CertificateState,
        require_client_certificate: bool,
    ) -> Self {
        Self {
            inner,
            certificate_state,
            require_client_certificate,
        }
    }
}

impl<S, ReqBody> Service<Request<ReqBody>> for InjectClientCertificate<S>
where
    S: Service<Request<ReqBody>, Response = Response, Error = Infallible>,
{
    type Response = Response;
    type Error = Infallible;
    type Future = Either<S::Future, future::Ready<Result<Self::Response, Self::Error>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<ReqBody>) -> Self::Future {
        match &self.certificate_state {
            CertificateState::Valid(certificate) => {
                request.extensions_mut().insert(certificate.clone());
                Either::Left(self.inner.call(request))
            }
            CertificateState::Missing => {
                if self.require_client_certificate {
                    Either::Right(future::ready(Ok(unauthorized_response(
                        "client certificate is required",
                    ))))
                } else {
                    Either::Left(self.inner.call(request))
                }
            }
            CertificateState::Invalid { reason } => {
                Either::Right(future::ready(Ok(unauthorized_response(reason.as_str()))))
            }
        }
    }
}

pub async fn serve(config: ServerConfig) -> Result<(), Box<dyn Error + Send + Sync>> {
    ensure_runtime_prerequisites().await?;
    diagnostics::reconcile_cni_artifacts_on_startup()
        .await
        .map_err(|e| with_context(e, "Failed to reconcile CNI artifacts during startup"))?;
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
    drop(crate::nanocloud::controller::bundle::spawn());
    drop(crate::nanocloud::controller::snapshot::spawn());
    drop(crate::nanocloud::controller::endpoints::spawn());
    drop(crate::nanocloud::controller::networkpolicy::spawn());
    drop(crate::nanocloud::controller::statefulset::spawn());
    spawn_event_logger();
    spawn_bootstrap_token_maintenance();
    let app = build_router();
    let require_client_certificate = require_client_certificate();
    if require_client_certificate {
        log_info("server", "Client certificates required", &[]);
    } else {
        log_warn(
            "server",
            "Client certificates optional; relying on secondary authentication",
            &[("env_var", "NANOCLOUD_REQUIRE_CLIENT_CERTIFICATE")],
        );
    }
    let http_addr = config.http_listen;
    let listener = TcpListener::bind(http_addr)
        .await
        .map_err(|e| with_context(e, format!("Failed to bind server listener at {http_addr}")))?;
    let tls_acceptor = Arc::new(
        build_tls_acceptor(&http_addr, require_client_certificate).map_err(|e| {
            with_context(e, format!("Failed to prepare TLS acceptor for {http_addr}"))
        })?,
    );

    let listen_addr_text = http_addr.to_string();
    log_info(
        "server",
        "HTTP server listening",
        &[("addr", listen_addr_text.as_str())],
    );

    let kubelet = Kubelet::shared();
    kubelet
        .restore_state()
        .await
        .map_err(|e| with_context(e, "Failed to restore workload state"))?;

    loop {
        let (stream, remote_addr) = listener
            .accept()
            .await
            .map_err(|e| with_context(e, "Failed to accept incoming TCP connection"))?;
        let service = app.clone();
        let tls_acceptor = Arc::clone(&tls_acceptor);
        let listen_addr = http_addr;
        tokio::spawn(async move {
            match accept_with_tls(tls_acceptor.as_ref(), stream).await {
                Ok(tls_stream) => {
                    let listen_addr_text = listen_addr.to_string();
                    let remote_addr_text = remote_addr.to_string();
                    let verify_result = tls_stream.ssl().verify_result();
                    let certificate_state = match tls_stream.ssl().peer_certificate() {
                        Some(cert) => match ClientCertificate::from_x509(&cert) {
                            Ok(client_cert) => {
                                if verify_result == X509VerifyResult::OK {
                                    CertificateState::Valid(client_cert)
                                } else {
                                    let reason = verify_result.error_string().to_string();
                                    log_warn(
                                        "server",
                                        "Client certificate validation failed",
                                        &[
                                            ("listen_addr", listen_addr_text.as_str()),
                                            ("remote_addr", remote_addr_text.as_str()),
                                            ("error", reason.as_str()),
                                        ],
                                    );
                                    CertificateState::Invalid {
                                        reason: format!(
                                            "client certificate validation failed: {}",
                                            verify_result.error_string()
                                        ),
                                    }
                                }
                            }
                            Err(err) => {
                                let error_text = err.to_string();
                                log_warn(
                                    "server",
                                    "Failed to process client certificate",
                                    &[
                                        ("listen_addr", listen_addr_text.as_str()),
                                        ("remote_addr", remote_addr_text.as_str()),
                                        ("error", error_text.as_str()),
                                    ],
                                );
                                CertificateState::Invalid {
                                    reason: "client certificate could not be processed".to_string(),
                                }
                            }
                        },
                        None => {
                            if require_client_certificate {
                                log_warn(
                                    "server",
                                    "Client did not present a certificate",
                                    &[
                                        ("listen_addr", listen_addr_text.as_str()),
                                        ("remote_addr", remote_addr_text.as_str()),
                                    ],
                                );
                            }
                            CertificateState::Missing
                        }
                    };

                    let io = TokioIo::new(tls_stream);
                    let service = TowerToHyperService::new(InjectClientCertificate::new(
                        service,
                        certificate_state,
                        require_client_certificate,
                    ));
                    if let Err(err) = HyperAcceptor::new(TokioExecutor::new())
                        .serve_connection_with_upgrades(io, service)
                        .await
                    {
                        let should_log = err
                            .downcast_ref::<hyper::Error>()
                            .map(|hyper_err| {
                                !(hyper_err.is_closed() || hyper_err.is_incomplete_message())
                            })
                            .unwrap_or(true);
                        if should_log {
                            let error_text = err.to_string();
                            let listen_addr_text = listen_addr.to_string();
                            let remote_addr_text = remote_addr.to_string();
                            log_error(
                                "server",
                                "HTTP serving error",
                                &[
                                    ("listen_addr", listen_addr_text.as_str()),
                                    ("remote_addr", remote_addr_text.as_str()),
                                    ("error", error_text.as_str()),
                                ],
                            );
                        }
                    }
                }
                Err(err) => {
                    let error_text = err.to_string();
                    let listen_addr_text = listen_addr.to_string();
                    let remote_addr_text = remote_addr.to_string();
                    log_warn(
                        "server",
                        "TLS handshake failed",
                        &[
                            ("listen_addr", listen_addr_text.as_str()),
                            ("remote_addr", remote_addr_text.as_str()),
                            ("error", error_text.as_str()),
                        ],
                    );
                }
            }
        });
    }
}

fn build_router() -> Router {
    let mut router = Router::new()
        .route("/version", get(handlers::discovery::version))
        .route("/api", get(handlers::discovery::core_api_versions))
        .route("/apis", get(handlers::discovery::api_groups))
        .route(
            "/apis/nanocloud.io",
            get(handlers::discovery::nanocloud_api_group),
        )
        .route("/apis/apps", get(handlers::discovery::apps_api_group))
        .route(
            "/apis/nanocloud.io/v1",
            get(handlers::discovery::nanocloud_api_resources),
        )
        .route(
            "/apis/apps/v1",
            get(handlers::discovery::apps_api_resources),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles",
            get(handlers::bundles::list).post(handlers::bundles::create),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}",
            get(handlers::bundles::get)
                .delete(handlers::bundles::delete)
                .patch(handlers::bundles::apply),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/exportProfile",
            post(handlers::bundles::export_profile),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/devices",
            get(handlers::devices::list).post(handlers::devices::create),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/devices/{name}",
            get(handlers::devices::get).delete(handlers::devices::delete),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/devices/certificates",
            post(handlers::devices::issue_certificate),
        )
        .route("/api/v1", get(handlers::discovery::core_api_resources))
        .route("/api/v1/events", get(handlers::events::list_all))
        .route(
            "/api/v1/namespaces/{namespace}/events",
            get(handlers::events::list_namespaced),
        )
        .route("/metrics", get(handlers::metrics))
        .route("/healthz", get(handlers::healthz))
        .route("/readyz", get(handlers::readyz))
        .route("/livez", get(handlers::livez))
        .route("/v1/dns/registry", get(handlers::dns::dump_registry))
        .route("/v1/setup", post(handlers::setup))
        .route("/v1/ca", post(handlers::issue_certificate))
        .route(
            "/v1/serviceaccounts/token",
            post(handlers::exchange_serviceaccount_token),
        )
        .route(
            "/apis/nanocloud.io/v1/certificates",
            post(handlers::certificates::issue_ephemeral_certificate),
        )
        .route(
            "/apis/nanocloud.io/v1/volumesnapshots",
            get(handlers::volumesnapshots::list_all),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/volumesnapshots",
            get(handlers::volumesnapshots::list_namespaced).post(handlers::volumesnapshots::create),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/volumesnapshots/{name}",
            get(handlers::volumesnapshots::get).delete(handlers::volumesnapshots::delete),
        )
        .route(
            "/apis/nanocloud.io/v1/roles",
            get(handlers::rbac::list_roles),
        )
        .route(
            "/apis/nanocloud.io/v1/roles/{name}",
            get(handlers::rbac::get_role_cluster),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/roles",
            get(handlers::rbac::list_roles_namespaced),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/roles/{name}",
            get(handlers::rbac::get_role),
        )
        .route(
            "/apis/nanocloud.io/v1/rolebindings",
            get(handlers::rbac::list_role_bindings),
        )
        .route(
            "/apis/nanocloud.io/v1/rolebindings/{name}",
            get(handlers::rbac::get_role_binding_cluster),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/rolebindings",
            get(handlers::rbac::list_role_bindings_namespaced),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/rolebindings/{name}",
            get(handlers::rbac::get_role_binding),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/actions/start",
            post(handlers::services::start_bundle),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/actions/stop",
            post(handlers::services::stop_bundle),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/actions/restart",
            post(handlers::services::restart_bundle),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/actions/uninstall",
            post(handlers::services::uninstall_bundle),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/backups/latest",
            get(handlers::services::stream_latest_backup),
        )
        .route(
            "/v1/networkpolicies/debug",
            get(handlers::networkpolicy_debug),
        )
        .route(
            "/api/v1/namespaces/{namespace}/pods",
            get(handlers::pods::list_pods),
        )
        .route(
            "/api/v1/namespaces/{namespace}/pods/{name}",
            get(handlers::pods::get_pod),
        )
        .route(
            "/api/v1/namespaces/{namespace}/pods/{name}/log",
            get(handlers::service_logs),
        )
        .route("/api/v1/pods/{name}/log", get(handlers::service_logs_no_ns))
        .route("/api/v1/pods", get(handlers::pods::list_pods_all))
        .route(
            "/api/v1/services",
            get(handlers::service_resources::list_all),
        )
        .route(
            "/api/v1/namespaces/{namespace}/services",
            get(handlers::service_resources::list_namespaced)
                .post(handlers::service_resources::create),
        )
        .route(
            "/api/v1/namespaces/{namespace}/services/{name}",
            get(handlers::service_resources::get).delete(handlers::service_resources::delete),
        )
        .route("/api/v1/endpoints", get(handlers::endpoints::list_all))
        .route(
            "/api/v1/namespaces/{namespace}/endpoints",
            get(handlers::endpoints::list_namespaced),
        )
        .route(
            "/api/v1/namespaces/{namespace}/endpoints/{name}",
            get(handlers::endpoints::get),
        )
        .route("/api/v1/configmaps", get(handlers::list_configmaps_all))
        .route(
            "/api/v1/namespaces/{namespace}/configmaps",
            get(handlers::list_configmaps).post(handlers::create_configmap),
        )
        .route(
            "/api/v1/namespaces/{namespace}/configmaps/{name}",
            get(handlers::get_configmap)
                .put(handlers::replace_configmap)
                .delete(handlers::delete_configmap),
        )
        .route(
            "/api/v1/persistentvolumeclaims",
            get(handlers::pvcs::list_all),
        )
        .route(
            "/api/v1/namespaces/{namespace}/persistentvolumeclaims",
            get(handlers::pvcs::list_namespaced),
        )
        .route(
            "/api/v1/namespaces/{namespace}/persistentvolumeclaims/{name}",
            get(handlers::pvcs::get),
        )
        .route("/api/v1/secrets", get(handlers::secrets::list_all))
        .route(
            "/api/v1/namespaces/{namespace}/secrets",
            get(handlers::secrets::list_namespace).post(handlers::secrets::create),
        )
        .route(
            "/api/v1/namespaces/{namespace}/secrets/{name}",
            get(handlers::secrets::get)
                .put(handlers::secrets::replace)
                .delete(handlers::secrets::delete),
        )
        .route(
            "/apis/apps/v1/statefulsets",
            get(handlers::statefulsets::list_all),
        )
        .route(
            "/apis/apps/v1/namespaces/{namespace}/statefulsets",
            get(handlers::statefulsets::list_namespaced).post(handlers::statefulsets::create),
        )
        .route(
            "/apis/apps/v1/namespaces/{namespace}/statefulsets/{name}",
            get(handlers::statefulsets::get).delete(handlers::statefulsets::delete),
        )
        .route(
            "/apis/apps/v1/deployments",
            get(handlers::deployments::list_all),
        )
        .route(
            "/apis/apps/v1/namespaces/{namespace}/deployments",
            get(handlers::deployments::list_namespaced),
        )
        .route(
            "/apis/apps/v1/namespaces/{namespace}/deployments/{name}",
            get(handlers::deployments::get).delete(handlers::deployments::delete),
        )
        .route(
            "/apis/apps/v1/daemonsets",
            get(handlers::daemonsets::list_all),
        )
        .route(
            "/apis/apps/v1/namespaces/{namespace}/daemonsets",
            get(handlers::daemonsets::list_namespaced),
        )
        .route(
            "/apis/apps/v1/namespaces/{namespace}/daemonsets/{name}",
            get(handlers::daemonsets::get).delete(handlers::daemonsets::delete),
        )
        .route("/apis/batch/v1/jobs", get(handlers::jobs::list_all))
        .route(
            "/apis/batch/v1/namespaces/{namespace}/jobs",
            get(handlers::jobs::list_namespaced),
        )
        .route(
            "/apis/batch/v1/namespaces/{namespace}/jobs/{name}",
            get(handlers::jobs::get).delete(handlers::jobs::delete),
        )
        .route(
            "/apis/apps/v1/replicasets",
            get(handlers::replicasets::list_all),
        )
        .route(
            "/apis/apps/v1/namespaces/{namespace}/replicasets",
            get(handlers::replicasets::list_namespaced),
        )
        .route(
            "/apis/apps/v1/namespaces/{namespace}/replicasets/{name}",
            get(handlers::replicasets::get),
        )
        .route("/v1/openapi.json", get(handlers::openapi_spec));

    router = router
        .route(
            "/api/v1/namespaces/{namespace}/pods/{name}/exec",
            get(handlers::exec::exec_ws_namespaced).post(handlers::exec::exec_http_post_namespaced),
        )
        .route(
            "/api/v1/pods/{name}/exec",
            get(handlers::exec::exec_ws_cluster).post(handlers::exec::exec_http_post_cluster),
        );

    router
        .layer(middleware::from_fn(auth::require_authenticated_subject))
        .layer(AuthLayer::new())
        .layer(middleware::from_fn(trace_http_request))
}

async fn trace_http_request(req: Request<Body>, next: Next) -> Result<Response, Infallible> {
    let method = req.method().clone();
    let method_str = method.as_str().to_string();
    let path = req.uri().path().to_string();
    let span_name = format!("{} {}", method, path);

    // Track in-flight requests
    metrics::inc_http_requests_in_flight(&method_str);
    let start = std::time::Instant::now();

    let response = tracing::with_span("api", span_name, next.run(req)).await;

    // Record metrics after request completes
    let duration = start.elapsed();
    let status = response.status().as_u16();
    metrics::dec_http_requests_in_flight(&method_str);
    metrics::record_http_request(&method_str, &path, status, duration);

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::convert::Infallible;
    use tower::service_fn;

    #[tokio::test]
    async fn missing_certificate_rejected_when_required() {
        let service = service_fn(|_: Request<()>| async {
            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::empty())
                    .unwrap(),
            )
        });
        let mut layer = InjectClientCertificate::new(service, CertificateState::Missing, true);

        let response = layer.call(Request::new(())).await.expect("response");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn missing_certificate_allowed_when_optional() {
        let service = service_fn(|_: Request<()>| async {
            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::empty())
                    .unwrap(),
            )
        });
        let mut layer = InjectClientCertificate::new(service, CertificateState::Missing, false);

        let response = layer.call(Request::new(())).await.expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn invalid_certificate_rejected_even_when_optional() {
        let service = service_fn(|_: Request<()>| async {
            Ok::<_, Infallible>(Response::new(Body::empty()))
        });
        let mut layer = InjectClientCertificate::new(
            service,
            CertificateState::Invalid {
                reason: "invalid chain".to_string(),
            },
            false,
        );

        let response = layer.call(Request::new(())).await.expect("response");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn valid_certificate_propagates_to_inner_service() {
        const EXPECTED_SUBJECT: &str = "CN=test";
        let service = service_fn(|req: Request<()>| async move {
            let subject = req
                .extensions()
                .get::<ClientCertificate>()
                .map(|cert| cert.subject())
                .unwrap_or("");
            let status = if subject == EXPECTED_SUBJECT {
                StatusCode::OK
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            Ok::<_, Infallible>(
                Response::builder()
                    .status(status)
                    .body(Body::empty())
                    .unwrap(),
            )
        });
        let mut layer = InjectClientCertificate::new(
            service,
            CertificateState::Valid(ClientCertificate::new(EXPECTED_SUBJECT.to_string())),
            false,
        );

        let response = layer.call(Request::new(())).await.expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }
}
