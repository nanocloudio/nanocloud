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

//! Edge server for Route-based ingress and Webhook handling.
//!
//! The edge server provides:
//!
//! - **Route Proxying**: Forward HTTP requests to backend Services based on
//!   host and path matching rules defined in Route CRDs.
//!
//! - **Webhook Handling**: Receive webhook payloads and trigger Job creation
//!   (to be implemented).
//!
//! # Architecture
//!
//! The edge server runs on a separate port from the main API server and uses
//! the shared `http` primitives for consistent TLS and middleware handling.
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────┐
//! │                      Edge Server                         │
//! │  ┌──────────────────────────────────────────────────┐   │
//! │  │              EdgeState (shared)                   │   │
//! │  │  ┌────────────┐  ┌────────────┐  ┌───────────┐  │   │
//! │  │  │   Routes   │  │  Endpoints │  │   HTTP    │  │   │
//! │  │  │  Registry  │  │   Cache    │  │  Client   │  │   │
//! │  │  └────────────┘  └────────────┘  └───────────┘  │   │
//! │  └──────────────────────────────────────────────────┘   │
//! │                          │                               │
//! │  ┌───────────────────────┼───────────────────────────┐  │
//! │  │                 Edge Router                        │  │
//! │  │  ┌─────────┐  ┌─────────────┐  ┌───────────────┐  │  │
//! │  │  │ /healthz│  │ Proxy       │  │   Webhook     │  │  │
//! │  │  │ /readyz │  │ Handler     │  │   Handler     │  │  │
//! │  │  └─────────┘  └─────────────┘  └───────────────┘  │  │
//! │  └───────────────────────────────────────────────────┘  │
//! └──────────────────────────────────────────────────────────┘
//! ```
//!
//! # Configuration
//!
//! The edge server is configured via environment variables:
//!
//! - `NANOCLOUD_EDGE_LISTEN`: Address to listen on (default: `0.0.0.0:8080`)
//! - `NANOCLOUD_REQUIRE_CLIENT_CERTIFICATE`: Whether to require client certs
//!
//! # Usage
//!
//! ```ignore
//! use nanocloud::edge::{EdgeConfig, serve};
//!
//! let config = EdgeConfig {
//!     listen: "0.0.0.0:8080".parse()?,
//! };
//!
//! serve(config).await?;
//! ```

pub mod proxy;
pub mod routes;
pub mod state;
pub mod webhook_trigger;

use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::nanocloud::http::{AppState, ServerBuilder};
use crate::nanocloud::http_middleware::MiddlewareStack;
use crate::nanocloud::logger::{log_info, log_warn};
use crate::nanocloud::util::error::with_context;

pub use routes::build_edge_router;
pub use state::EdgeState;

/// Configuration for the edge server.
#[derive(Clone, Debug)]
pub struct EdgeConfig {
    /// Address to listen on.
    pub listen: SocketAddr,

    /// Whether to require client certificates.
    pub require_client_certificate: bool,
}

impl Default for EdgeConfig {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:8080".parse().unwrap(),
            require_client_certificate: false,
        }
    }
}

impl EdgeConfig {
    /// Create a new EdgeConfig from environment variables.
    pub fn from_env() -> Self {
        let listen = env::var("NANOCLOUD_EDGE_LISTEN")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| "0.0.0.0:8080".parse().unwrap());

        let require_client_certificate = env::var("NANOCLOUD_REQUIRE_CLIENT_CERTIFICATE")
            .ok()
            .and_then(|s| parse_bool(&s))
            .unwrap_or(false);

        Self {
            listen,
            require_client_certificate,
        }
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Start the edge server.
///
/// This function starts the edge server with the given configuration and state.
/// It will run until an error occurs or the process is terminated.
///
/// # Arguments
///
/// * `config` - Edge server configuration
/// * `state` - Shared edge state (routes, endpoints, etc.)
///
/// # Errors
///
/// Returns an error if:
/// - TLS configuration fails
/// - The server fails to bind to the configured address
pub async fn serve(
    config: EdgeConfig,
    state: Arc<EdgeState>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let listen_addr = config.listen.to_string();
    log_info(
        "edge",
        "Starting edge server",
        &[("addr", listen_addr.as_str())],
    );

    // Build the edge router
    let router = build_edge_router(Arc::clone(&state));

    // Apply middleware stack (without auth for edge server by default)
    // Edge routes handle their own authentication if needed
    let app = MiddlewareStack::new().with_auth(false).apply(router);

    // Log TLS configuration
    if config.require_client_certificate {
        log_info("edge", "Client certificates required", &[]);
    } else {
        log_warn(
            "edge",
            "Client certificates optional",
            &[("env_var", "NANOCLOUD_REQUIRE_CLIENT_CERTIFICATE")],
        );
    }

    // Build and start the server
    let server = ServerBuilder::new()
        .bind(config.listen)
        .tls_identity("nanocloud-edge", &["localhost", "127.0.0.1", "::1"])
        .require_client_certificate(config.require_client_certificate)
        .build(app, AppState::new())
        .map_err(|e| with_context(e, "failed to build edge server"))?;

    // Mark the edge server as ready
    state.set_ready(true);

    server.serve().await
}

/// Start the edge server with default configuration.
///
/// Convenience function that creates configuration from environment variables.
pub async fn serve_default() -> Result<(), Box<dyn Error + Send + Sync>> {
    let config = EdgeConfig::from_env();
    let state = Arc::new(EdgeState::new());
    serve(config, state).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edge_config_default() {
        let config = EdgeConfig::default();
        assert_eq!(config.listen, "0.0.0.0:8080".parse().unwrap());
        assert!(!config.require_client_certificate);
    }

    #[test]
    fn parse_bool_truthy() {
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("TRUE"), Some(true));
        assert_eq!(parse_bool("1"), Some(true));
        assert_eq!(parse_bool("yes"), Some(true));
        assert_eq!(parse_bool("on"), Some(true));
    }

    #[test]
    fn parse_bool_falsy() {
        assert_eq!(parse_bool("false"), Some(false));
        assert_eq!(parse_bool("FALSE"), Some(false));
        assert_eq!(parse_bool("0"), Some(false));
        assert_eq!(parse_bool("no"), Some(false));
        assert_eq!(parse_bool("off"), Some(false));
    }

    #[test]
    fn parse_bool_invalid() {
        assert_eq!(parse_bool("invalid"), None);
        assert_eq!(parse_bool(""), None);
    }
}
