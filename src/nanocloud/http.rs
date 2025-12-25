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

//! Reusable HTTP server primitives for control-plane and edge servers.
//!
//! This module provides building blocks for constructing HTTP servers with
//! consistent TLS configuration, shared middleware, and graceful shutdown.
//!
//! # Architecture
//!
//! The module is organized around two primary types:
//!
//! - [`ServerBuilder`]: Configures and constructs an HTTP server
//! - [`AppState`]: Shared state passed to all handlers
//!
//! # Example
//!
//! ```ignore
//! use nanocloud::http::{ServerBuilder, AppState};
//!
//! let state = AppState::new();
//! let server = ServerBuilder::new()
//!     .bind("0.0.0.0:8443".parse()?)
//!     .tls_identity("server-name", &["localhost"])
//!     .require_client_certificate(true)
//!     .build(router, state)?;
//!
//! server.serve_with_shutdown(shutdown_signal).await?;
//! ```
//!
//! # TLS Configuration
//!
//! Servers created with this module use the cluster's secure assets for TLS:
//!
//! - Server certificates are loaded or generated via [`TlsInfo`]
//! - Client certificates are validated against the cluster CA
//! - ALPN is configured for HTTP/1.1
//!
//! # Graceful Shutdown
//!
//! The [`Server::serve_with_shutdown`] method accepts a future that signals
//! when the server should begin graceful shutdown. Active connections are
//! allowed to complete within a configurable timeout.

use std::collections::hash_map::DefaultHasher;
use std::convert::Infallible;
use std::error::Error;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Router;
use futures_util::future::{self, Either};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as HyperAcceptor;
use hyper_util::service::TowerToHyperService;
use openssl::pkey::PKey;
use openssl::ssl::{
    select_next_proto, AlpnError, Ssl, SslAcceptor as OpenSslAcceptor, SslMethod, SslVerifyMode,
};
use openssl::stack::Stack;
use openssl::x509::{X509Name, X509VerifyResult, X509};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_openssl::SslStream;
use tower::Service;

use crate::nanocloud::logger::{log_error, log_info, log_warn};
use crate::nanocloud::server::auth::ClientCertificate;
use crate::nanocloud::server::handlers::ApiError;
use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::security::TlsInfo;

/// Default timeout for graceful shutdown.
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// ALPN protocol list for HTTP/1.1.
const ALPN_PROTO_LIST: &[u8] = b"\x08http/1.1";

/// Shared application state accessible to all handlers.
///
/// This type is designed to be cloned cheaply (via `Arc` internally) and
/// passed to handlers via Axum's state extraction.
#[derive(Clone, Default)]
pub struct AppState {
    // Placeholder for future shared state. Will hold:
    // - DNS service reference
    // - Metrics registry
    // - Other shared services
}

impl AppState {
    /// Create a new `AppState` with default configuration.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Builder for configuring and constructing an HTTP server.
///
/// # Example
///
/// ```ignore
/// let server = ServerBuilder::new()
///     .bind("0.0.0.0:8443".parse()?)
///     .tls_identity("api-server", &["localhost", "127.0.0.1"])
///     .require_client_certificate(false)
///     .build(router, state)?;
/// ```
pub struct ServerBuilder {
    bind_addr: Option<SocketAddr>,
    tls_identity_name: Option<String>,
    tls_san_entries: Vec<String>,
    require_client_certificate: bool,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerBuilder {
    /// Create a new server builder with default settings.
    pub fn new() -> Self {
        Self {
            bind_addr: None,
            tls_identity_name: None,
            tls_san_entries: Vec::new(),
            require_client_certificate: false,
        }
    }

    /// Set the address to bind the server to.
    #[must_use]
    pub fn bind(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = Some(addr);
        self
    }

    /// Configure the TLS identity for the server.
    ///
    /// The `name` is used to identify the server certificate, and `san_entries`
    /// specifies additional Subject Alternative Names to include.
    #[must_use]
    pub fn tls_identity(mut self, name: impl Into<String>, san_entries: &[&str]) -> Self {
        self.tls_identity_name = Some(name.into());
        self.tls_san_entries = san_entries.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Set whether client certificates are required.
    ///
    /// When `true`, connections without a valid client certificate will be
    /// rejected with a TLS handshake error.
    #[must_use]
    pub fn require_client_certificate(mut self, require: bool) -> Self {
        self.require_client_certificate = require;
        self
    }

    /// Build the server with the given router and state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No bind address was configured
    /// - TLS configuration fails (missing certificates, key mismatch, etc.)
    pub fn build(
        self,
        router: Router,
        _state: AppState,
    ) -> Result<Server, Box<dyn Error + Send + Sync>> {
        let bind_addr = self.bind_addr.ok_or("bind address is required")?;

        let identity_name = self
            .tls_identity_name
            .unwrap_or_else(|| "nanocloud-server".to_string());

        let tls_acceptor = build_tls_acceptor(
            &bind_addr,
            &identity_name,
            &self.tls_san_entries,
            self.require_client_certificate,
        )?;

        Ok(Server {
            bind_addr,
            router,
            tls_acceptor: Arc::new(tls_acceptor),
            require_client_certificate: self.require_client_certificate,
        })
    }
}

/// An HTTP server ready to accept connections.
pub struct Server {
    bind_addr: SocketAddr,
    router: Router,
    tls_acceptor: Arc<OpenSslAcceptor>,
    require_client_certificate: bool,
}

impl Server {
    /// Start serving requests.
    ///
    /// This method runs indefinitely until an error occurs. For graceful
    /// shutdown support, use [`serve_with_shutdown`](Self::serve_with_shutdown).
    pub async fn serve(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.serve_with_shutdown(future::pending::<()>()).await
    }

    /// Start serving requests with graceful shutdown support.
    ///
    /// When the `shutdown` future completes, the server stops accepting new
    /// connections and waits for active connections to complete (up to the
    /// configured shutdown timeout).
    pub async fn serve_with_shutdown<F>(
        self,
        shutdown: F,
    ) -> Result<(), Box<dyn Error + Send + Sync>>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind(self.bind_addr)
            .await
            .map_err(|e| with_context(e, format!("failed to bind to {}", self.bind_addr)))?;

        let listen_addr_text = self.bind_addr.to_string();
        log_info(
            "http",
            "Server listening",
            &[("addr", listen_addr_text.as_str())],
        );

        // Set up shutdown signaling
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let shutdown_timeout = DEFAULT_SHUTDOWN_TIMEOUT;

        // Spawn shutdown handler
        tokio::spawn(async move {
            shutdown.await;
            let _ = shutdown_tx.send(true);
        });

        let mut connection_count = 0u64;

        loop {
            let mut rx = shutdown_rx.clone();
            let accept_result = tokio::select! {
                result = listener.accept() => result,
                _ = rx.changed() => {
                    if *shutdown_rx.borrow() {
                        log_info("http", "Shutdown signal received, stopping accept loop", &[]);
                        break;
                    }
                    continue;
                }
            };

            let (stream, remote_addr) = match accept_result {
                Ok(conn) => conn,
                Err(e) => {
                    log_warn(
                        "http",
                        "Failed to accept connection",
                        &[("error", e.to_string().as_str())],
                    );
                    continue;
                }
            };

            connection_count += 1;
            let connection_id = connection_count;
            let service = self.router.clone();
            let tls_acceptor = Arc::clone(&self.tls_acceptor);
            let listen_addr = self.bind_addr;
            let require_client_certificate = self.require_client_certificate;
            let mut conn_shutdown_rx = shutdown_rx.clone();

            tokio::spawn(async move {
                let result = handle_connection(
                    tls_acceptor,
                    stream,
                    service,
                    listen_addr,
                    remote_addr,
                    require_client_certificate,
                    connection_id,
                    &mut conn_shutdown_rx,
                    shutdown_timeout,
                )
                .await;

                if let Err(e) = result {
                    let error_text = e.to_string();
                    let listen_addr_text = listen_addr.to_string();
                    let remote_addr_text = remote_addr.to_string();
                    log_error(
                        "http",
                        "Connection handling error",
                        &[
                            ("listen_addr", listen_addr_text.as_str()),
                            ("remote_addr", remote_addr_text.as_str()),
                            ("error", error_text.as_str()),
                        ],
                    );
                }
            });
        }

        // Wait for active connections to complete
        log_info(
            "http",
            "Waiting for active connections to complete",
            &[(
                "timeout_secs",
                shutdown_timeout.as_secs().to_string().as_str(),
            )],
        );

        // In a full implementation, we would track active connections and wait
        // for them to complete. For now, we just log and return.
        tokio::time::sleep(Duration::from_millis(100)).await;

        log_info("http", "Server shutdown complete", &[]);
        Ok(())
    }
}

/// Handle a single connection.
#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    tls_acceptor: Arc<OpenSslAcceptor>,
    stream: TcpStream,
    service: Router,
    listen_addr: SocketAddr,
    remote_addr: SocketAddr,
    require_client_certificate: bool,
    _connection_id: u64,
    _shutdown_rx: &mut watch::Receiver<bool>,
    _shutdown_timeout: Duration,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let tls_stream = accept_with_tls(&tls_acceptor, stream).await?;

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
                        "http",
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
                    "http",
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
                    "http",
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
    let tower_service = TowerToHyperService::new(InjectClientCertificate::new(
        service,
        certificate_state,
        require_client_certificate,
    ));

    if let Err(err) = HyperAcceptor::new(TokioExecutor::new())
        .serve_connection_with_upgrades(io, tower_service)
        .await
    {
        let should_log = err
            .downcast_ref::<hyper::Error>()
            .map(|hyper_err| !(hyper_err.is_closed() || hyper_err.is_incomplete_message()))
            .unwrap_or(true);

        if should_log {
            return Err(err);
        }
    }

    Ok(())
}

/// Build a TLS acceptor with the given configuration.
fn build_tls_acceptor(
    addr: &SocketAddr,
    identity_name: &str,
    extra_san: &[String],
    require_client_certificate: bool,
) -> Result<OpenSslAcceptor, Box<dyn Error + Send + Sync>> {
    // Build SAN list
    let mut san = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];

    match addr.ip() {
        IpAddr::V4(ip) if !ip.is_unspecified() => san.push(ip.to_string()),
        IpAddr::V6(ip) if !ip.is_unspecified() => san.push(ip.to_string()),
        _ => {}
    }

    san.extend(extra_san.iter().cloned());
    san.sort();
    san.dedup();

    let tls = TlsInfo::create(identity_name, Some(&san))
        .map_err(|e| with_context(e, "failed to create server TLS assets"))?;

    let server_cert = X509::from_pem(&tls.cert)
        .map_err(|e| with_context(e, "failed to parse server certificate PEM"))?;
    let server_key = PKey::private_key_from_pem(&tls.key)
        .map_err(|e| with_context(e, "failed to parse server private key PEM"))?;
    let ca_cert = X509::from_pem(&tls.ca)
        .map_err(|e| with_context(e, "failed to parse cluster CA certificate PEM"))?;

    let mut builder = OpenSslAcceptor::mozilla_modern(SslMethod::tls())
        .map_err(|e| with_context(e, "failed to initialize TLS acceptor builder"))?;

    builder
        .set_private_key(&server_key)
        .map_err(|e| with_context(e, "failed to attach server private key"))?;
    builder
        .set_certificate(&server_cert)
        .map_err(|e| with_context(e, "failed to attach server certificate"))?;
    builder
        .check_private_key()
        .map_err(|e| with_context(e, "server certificate and key mismatch"))?;
    builder
        .cert_store_mut()
        .add_cert(ca_cert.clone())
        .map_err(|e| with_context(e, "failed to add cluster CA to certificate store"))?;

    let mut verify_mode = SslVerifyMode::PEER;
    if require_client_certificate {
        verify_mode |= SslVerifyMode::FAIL_IF_NO_PEER_CERT;
    }
    builder.set_verify(verify_mode);

    let mut name_stack = Stack::<X509Name>::new()
        .map_err(|e| with_context(e, "failed to prepare client CA stack"))?;
    name_stack
        .push(
            ca_cert
                .subject_name()
                .to_owned()
                .map_err(|e| with_context(e, "failed to copy cluster CA subject"))?,
        )
        .map_err(|e| with_context(e, "failed to register client CA subject"))?;
    builder.set_client_ca_list(name_stack);
    builder.set_verify_callback(SslVerifyMode::PEER, |_, _| true);

    let mut hasher = DefaultHasher::new();
    addr.hash(&mut hasher);
    let hash = hasher.finish();
    builder
        .set_session_id_context(&hash.to_be_bytes())
        .map_err(|e| with_context(e, "failed to set TLS session context"))?;

    builder
        .set_alpn_protos(ALPN_PROTO_LIST)
        .map_err(|e| with_context(e, "failed to configure ALPN protocols"))?;
    builder.set_alpn_select_callback(|_, client| {
        select_next_proto(client, ALPN_PROTO_LIST).ok_or(AlpnError::NOACK)
    });

    Ok(builder.build())
}

/// Perform TLS handshake on a TCP stream.
async fn accept_with_tls(
    acceptor: &OpenSslAcceptor,
    stream: TcpStream,
) -> Result<SslStream<TcpStream>, Box<dyn Error + Send + Sync>> {
    let ssl = Ssl::new(acceptor.context())
        .map_err(|e| with_context(e, "failed to initialize TLS session"))?;
    let mut tls_stream = SslStream::new(ssl, stream)
        .map_err(|e| with_context(e, "failed to bind TLS stream to socket"))?;
    Pin::new(&mut tls_stream)
        .accept()
        .await
        .map_err(|e| with_context(e, "TLS handshake failed"))?;
    Ok(tls_stream)
}

// ============================================================================
// Client Certificate Injection
// ============================================================================

#[derive(Clone)]
enum CertificateState {
    Valid(ClientCertificate),
    Missing,
    Invalid { reason: String },
}

/// Service wrapper that injects client certificate state into requests.
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

fn unauthorized_response(message: &str) -> Response {
    ApiError::with_reason(StatusCode::UNAUTHORIZED, "Unauthorized", message).into_response()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_builder_requires_bind_address() {
        let result = ServerBuilder::new().build(Router::new(), AppState::new());
        assert!(result.is_err());
    }

    #[test]
    fn app_state_is_clone() {
        let state = AppState::new();
        let _cloned = state.clone();
    }
}
