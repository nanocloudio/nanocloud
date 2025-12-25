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

//! Minimal reverse proxy implementation for the edge server.
//!
//! This module provides HTTP reverse proxying capabilities:
//!
//! - Service endpoint resolution via the endpoint registry
//! - Request forwarding with configurable timeouts
//! - Proper handling of hop-by-hop headers
//! - Strip-prefix path rewriting
//! - Error handling with appropriate status codes
//!
//! # Usage
//!
//! ```ignore
//! use nanocloud::edge::proxy::{ProxyClient, ProxyRequest, ProxyError};
//!
//! let client = ProxyClient::new()?;
//! let request = ProxyRequest {
//!     method: Method::GET,
//!     target_url: "http://10.0.0.1:8080/api/users".to_string(),
//!     headers: original_headers,
//!     body: None,
//!     timeout: Duration::from_secs(30),
//! };
//!
//! let response = client.forward(request).await?;
//! ```

use std::collections::HashSet;
use std::sync::LazyLock;
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, Method, Request, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::nanocloud::server::handlers::ApiError;

/// Default timeout for proxied requests.
pub const DEFAULT_PROXY_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum body size for proxied requests (10MB).
pub const MAX_PROXY_BODY_SIZE: usize = 10 * 1024 * 1024;

/// Hop-by-hop headers that should not be forwarded.
static HOP_BY_HOP_HEADERS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut set = HashSet::new();
    set.insert("connection");
    set.insert("keep-alive");
    set.insert("proxy-authenticate");
    set.insert("proxy-authorization");
    set.insert("te");
    set.insert("trailers");
    set.insert("transfer-encoding");
    set.insert("upgrade");
    set
});

/// Headers that should be set/overwritten by the proxy.
static PROXY_HEADERS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut set = HashSet::new();
    set.insert("host");
    set.insert("x-forwarded-for");
    set.insert("x-forwarded-host");
    set.insert("x-forwarded-proto");
    set.insert("x-real-ip");
    set
});

/// Check if a header is a hop-by-hop header that should not be forwarded.
pub fn is_hop_by_hop_header(name: &str) -> bool {
    HOP_BY_HOP_HEADERS.contains(name.to_lowercase().as_str())
}

/// Check if a header is set by the proxy and should be overwritten.
pub fn is_proxy_header(name: &str) -> bool {
    PROXY_HEADERS.contains(name.to_lowercase().as_str())
}

/// Errors that can occur during proxying.
#[derive(Debug)]
pub enum ProxyError {
    /// Failed to build the request.
    RequestBuild(String),
    /// Failed to connect to the backend.
    Connection(String),
    /// Request timed out.
    Timeout,
    /// Backend returned an error.
    Backend(StatusCode, String),
    /// Failed to read the request body.
    BodyRead(String),
    /// Failed to read the response body.
    ResponseRead(String),
    /// Invalid target URL.
    InvalidUrl(String),
}

impl std::fmt::Display for ProxyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RequestBuild(msg) => write!(f, "failed to build request: {}", msg),
            Self::Connection(msg) => write!(f, "connection failed: {}", msg),
            Self::Timeout => write!(f, "request timed out"),
            Self::Backend(status, msg) => write!(f, "backend error {}: {}", status, msg),
            Self::BodyRead(msg) => write!(f, "failed to read request body: {}", msg),
            Self::ResponseRead(msg) => write!(f, "failed to read response body: {}", msg),
            Self::InvalidUrl(msg) => write!(f, "invalid target URL: {}", msg),
        }
    }
}

impl std::error::Error for ProxyError {}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let (status, reason, message) = match &self {
            ProxyError::RequestBuild(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "ProxyRequestBuild",
                msg.as_str(),
            ),
            ProxyError::Connection(msg) => (
                StatusCode::BAD_GATEWAY,
                "ProxyConnectionFailed",
                msg.as_str(),
            ),
            ProxyError::Timeout => (
                StatusCode::GATEWAY_TIMEOUT,
                "ProxyTimeout",
                "upstream request timed out",
            ),
            ProxyError::Backend(status, msg) => (*status, "ProxyBackendError", msg.as_str()),
            ProxyError::BodyRead(msg) => (StatusCode::BAD_REQUEST, "ProxyBodyRead", msg.as_str()),
            ProxyError::ResponseRead(msg) => {
                (StatusCode::BAD_GATEWAY, "ProxyResponseRead", msg.as_str())
            }
            ProxyError::InvalidUrl(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "ProxyInvalidUrl",
                msg.as_str(),
            ),
        };

        ApiError::with_reason(status, reason, message).into_response()
    }
}

/// A request to be proxied to a backend.
#[derive(Debug)]
pub struct ProxyRequest {
    /// HTTP method.
    pub method: Method,
    /// Target URL (including scheme, host, port, path, query).
    pub target_url: String,
    /// Headers to forward (hop-by-hop headers will be filtered).
    pub headers: HeaderMap,
    /// Request body (if any).
    pub body: Option<Vec<u8>>,
    /// Timeout for the request.
    pub timeout: Duration,
    /// Original client IP (for X-Forwarded-For).
    pub client_ip: Option<String>,
    /// Original host header (for X-Forwarded-Host).
    pub original_host: Option<String>,
    /// Original scheme (for X-Forwarded-Proto).
    pub original_scheme: Option<String>,
}

impl ProxyRequest {
    /// Create a new ProxyRequest from an axum request.
    pub async fn from_request(
        request: Request<Body>,
        endpoint: &str,
        target_path: &str,
        timeout: Duration,
    ) -> Result<Self, ProxyError> {
        let method = request.method().clone();
        let headers = request.headers().clone();
        let uri = request.uri();

        // Build target URL
        let query = uri.query().map(|q| format!("?{}", q)).unwrap_or_default();
        let target_url = format!("http://{}{}{}", endpoint, target_path, query);

        // Read the body
        let body_bytes = axum::body::to_bytes(request.into_body(), MAX_PROXY_BODY_SIZE)
            .await
            .map_err(|e| ProxyError::BodyRead(e.to_string()))?;

        let body = if body_bytes.is_empty() {
            None
        } else {
            Some(body_bytes.to_vec())
        };

        Ok(Self {
            method,
            target_url,
            headers,
            body,
            timeout,
            client_ip: None,
            original_host: None,
            original_scheme: None,
        })
    }

    /// Set the client IP for X-Forwarded-For.
    #[must_use]
    pub fn with_client_ip(mut self, ip: impl Into<String>) -> Self {
        self.client_ip = Some(ip.into());
        self
    }

    /// Set the original host for X-Forwarded-Host.
    #[must_use]
    pub fn with_original_host(mut self, host: impl Into<String>) -> Self {
        self.original_host = Some(host.into());
        self
    }

    /// Set the original scheme for X-Forwarded-Proto.
    #[must_use]
    pub fn with_original_scheme(mut self, scheme: impl Into<String>) -> Self {
        self.original_scheme = Some(scheme.into());
        self
    }
}

/// HTTP client for proxying requests to backends.
#[derive(Clone)]
pub struct ProxyClient {
    client: reqwest::Client,
}

impl Default for ProxyClient {
    fn default() -> Self {
        Self::new().expect("failed to create proxy client")
    }
}

impl ProxyClient {
    /// Create a new ProxyClient with default settings.
    pub fn new() -> Result<Self, ProxyError> {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(60))
            .tcp_nodelay(true)
            .timeout(DEFAULT_PROXY_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| ProxyError::Connection(e.to_string()))?;

        Ok(Self { client })
    }

    /// Create a new ProxyClient with custom settings.
    pub fn with_client(client: reqwest::Client) -> Self {
        Self { client }
    }

    /// Forward a request to the backend and return the response.
    pub async fn forward(&self, request: ProxyRequest) -> Result<Response, ProxyError> {
        // Parse the target URL
        let url: reqwest::Url = request
            .target_url
            .parse()
            .map_err(|e| ProxyError::InvalidUrl(format!("{}", e)))?;

        // Build the reqwest request
        let mut builder = self
            .client
            .request(request.method.clone(), url)
            .timeout(request.timeout);

        // Copy headers, filtering hop-by-hop and proxy headers
        for (name, value) in request.headers.iter() {
            let name_str = name.as_str();
            if !is_hop_by_hop_header(name_str) && !is_proxy_header(name_str) {
                if let Ok(value_str) = value.to_str() {
                    builder = builder.header(name_str, value_str);
                }
            }
        }

        // Add proxy headers
        if let Some(ref ip) = request.client_ip {
            builder = builder.header("X-Forwarded-For", ip.as_str());
            builder = builder.header("X-Real-IP", ip.as_str());
        }
        if let Some(ref host) = request.original_host {
            builder = builder.header("X-Forwarded-Host", host.as_str());
        }
        if let Some(ref scheme) = request.original_scheme {
            builder = builder.header("X-Forwarded-Proto", scheme.as_str());
        }

        // Add body if present
        if let Some(body) = request.body {
            builder = builder.body(body);
        }

        // Send the request
        let response = builder.send().await.map_err(|e| {
            if e.is_timeout() {
                ProxyError::Timeout
            } else if e.is_connect() {
                ProxyError::Connection(e.to_string())
            } else {
                ProxyError::RequestBuild(e.to_string())
            }
        })?;

        // Convert to axum response
        self.convert_response(response).await
    }

    /// Convert a reqwest response to an axum response.
    async fn convert_response(&self, response: reqwest::Response) -> Result<Response, ProxyError> {
        let status = response.status();
        let headers = response.headers().clone();

        // Read the response body
        let body = response
            .bytes()
            .await
            .map_err(|e| ProxyError::ResponseRead(e.to_string()))?;

        // Build the axum response
        let mut builder = axum::http::Response::builder().status(status);

        // Copy headers, filtering hop-by-hop headers
        for (name, value) in headers.iter() {
            if !is_hop_by_hop_header(name.as_str()) {
                builder = builder.header(name, value);
            }
        }

        builder
            .body(Body::from(body))
            .map_err(|e| ProxyError::RequestBuild(e.to_string()))
    }

    /// Forward an axum request to the specified endpoint.
    ///
    /// This is a convenience method that combines `ProxyRequest::from_request`
    /// and `forward`.
    pub async fn forward_request(
        &self,
        request: Request<Body>,
        endpoint: &str,
        target_path: &str,
        timeout: Duration,
    ) -> Result<Response, ProxyError> {
        let proxy_request =
            ProxyRequest::from_request(request, endpoint, target_path, timeout).await?;
        self.forward(proxy_request).await
    }
}

/// Transform a path according to strip-prefix rules.
///
/// If `strip_prefix` is true and the path starts with `prefix`,
/// the prefix is removed. Otherwise, the path is returned unchanged.
pub fn transform_path<'a>(path: &'a str, prefix: Option<&str>, strip_prefix: bool) -> &'a str {
    if !strip_prefix {
        return path;
    }

    match prefix {
        Some(prefix) if path.starts_with(prefix) => {
            let stripped = &path[prefix.len()..];
            if stripped.is_empty() {
                "/"
            } else if !stripped.starts_with('/') {
                // This shouldn't happen if prefix matching is correct,
                // but handle it gracefully
                path
            } else {
                stripped
            }
        }
        _ => path,
    }
}

/// Build the target URL for a proxied request.
pub fn build_target_url(endpoint: &str, path: &str, query: Option<&str>) -> String {
    let query_string = query.map(|q| format!("?{}", q)).unwrap_or_default();
    format!("http://{}{}{}", endpoint, path, query_string)
}

/// Extract the client IP from request headers.
///
/// Checks headers in order: X-Forwarded-For, X-Real-IP, then falls back
/// to the provided default.
pub fn extract_client_ip(headers: &HeaderMap, default: Option<&str>) -> Option<String> {
    // Check X-Forwarded-For (may contain multiple IPs, use the first)
    if let Some(xff) = headers.get("x-forwarded-for") {
        if let Ok(value) = xff.to_str() {
            if let Some(first_ip) = value.split(',').next() {
                let ip = first_ip.trim();
                if !ip.is_empty() {
                    return Some(ip.to_string());
                }
            }
        }
    }

    // Check X-Real-IP
    if let Some(xri) = headers.get("x-real-ip") {
        if let Ok(value) = xri.to_str() {
            let ip = value.trim();
            if !ip.is_empty() {
                return Some(ip.to_string());
            }
        }
    }

    // Fall back to default
    default.map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn hop_by_hop_header_detection() {
        assert!(is_hop_by_hop_header("Connection"));
        assert!(is_hop_by_hop_header("connection"));
        assert!(is_hop_by_hop_header("Transfer-Encoding"));
        assert!(is_hop_by_hop_header("keep-alive"));
        assert!(is_hop_by_hop_header("upgrade"));

        assert!(!is_hop_by_hop_header("Content-Type"));
        assert!(!is_hop_by_hop_header("X-Custom-Header"));
        assert!(!is_hop_by_hop_header("Authorization"));
    }

    #[test]
    fn proxy_header_detection() {
        assert!(is_proxy_header("Host"));
        assert!(is_proxy_header("host"));
        assert!(is_proxy_header("X-Forwarded-For"));
        assert!(is_proxy_header("x-forwarded-host"));
        assert!(is_proxy_header("X-Real-IP"));

        assert!(!is_proxy_header("Content-Type"));
        assert!(!is_proxy_header("Authorization"));
    }

    #[test]
    fn path_transformation() {
        // With strip_prefix enabled
        assert_eq!(transform_path("/api/users", Some("/api"), true), "/users");
        assert_eq!(transform_path("/api", Some("/api"), true), "/");
        assert_eq!(transform_path("/other", Some("/api"), true), "/other");

        // Without strip_prefix
        assert_eq!(
            transform_path("/api/users", Some("/api"), false),
            "/api/users"
        );

        // No prefix
        assert_eq!(transform_path("/api/users", None, true), "/api/users");
    }

    #[test]
    fn target_url_building() {
        assert_eq!(
            build_target_url("10.0.0.1:8080", "/api/users", None),
            "http://10.0.0.1:8080/api/users"
        );

        assert_eq!(
            build_target_url("10.0.0.1:8080", "/api/users", Some("page=1")),
            "http://10.0.0.1:8080/api/users?page=1"
        );

        assert_eq!(
            build_target_url("backend:80", "/", Some("foo=bar&baz=qux")),
            "http://backend:80/?foo=bar&baz=qux"
        );
    }

    #[test]
    fn client_ip_extraction() {
        let mut headers = HeaderMap::new();

        // No headers
        assert_eq!(extract_client_ip(&headers, None), None);
        assert_eq!(
            extract_client_ip(&headers, Some("192.168.1.1")),
            Some("192.168.1.1".to_string())
        );

        // X-Forwarded-For with single IP
        headers.insert("x-forwarded-for", HeaderValue::from_static("10.0.0.1"));
        assert_eq!(
            extract_client_ip(&headers, None),
            Some("10.0.0.1".to_string())
        );

        // X-Forwarded-For with multiple IPs (use first)
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("10.0.0.1, 10.0.0.2, 10.0.0.3"),
        );
        assert_eq!(
            extract_client_ip(&headers, None),
            Some("10.0.0.1".to_string())
        );

        // X-Real-IP takes precedence when X-Forwarded-For is missing
        headers.remove("x-forwarded-for");
        headers.insert("x-real-ip", HeaderValue::from_static("172.16.0.1"));
        assert_eq!(
            extract_client_ip(&headers, None),
            Some("172.16.0.1".to_string())
        );
    }

    #[test]
    fn proxy_error_display() {
        let err = ProxyError::Timeout;
        assert_eq!(err.to_string(), "request timed out");

        let err = ProxyError::Connection("refused".to_string());
        assert_eq!(err.to_string(), "connection failed: refused");

        let err = ProxyError::InvalidUrl("bad url".to_string());
        assert_eq!(err.to_string(), "invalid target URL: bad url");
    }

    #[test]
    fn proxy_client_creation() {
        let client = ProxyClient::new();
        assert!(client.is_ok());
    }
}
