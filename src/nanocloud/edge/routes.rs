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

//! Edge server route definitions.
//!
//! This module defines HTTP routes for the edge server, including:
//! - Route-based reverse proxy endpoints
//! - Webhook trigger endpoints
//! - Health check endpoints

use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::header::HOST;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::Router;

use super::state::EdgeState;
use crate::nanocloud::server::handlers::ApiError;

/// Build the edge server router.
///
/// This router handles:
/// - Health checks at `/healthz` and `/readyz`
/// - Route-based proxying for all other paths
/// - Webhook endpoints (to be implemented)
pub fn build_edge_router(state: Arc<EdgeState>) -> Router {
    Router::new()
        // Health endpoints (no auth required)
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        // Catch-all for Route proxying - handle all methods
        .fallback(any(proxy_handler))
        .with_state(state)
}

/// Health check endpoint.
async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Readiness check endpoint.
async fn readyz(State(state): State<Arc<EdgeState>>) -> impl IntoResponse {
    if state.is_ready() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

/// Extract host from request headers.
fn extract_host(request: &Request<Body>) -> Option<String> {
    request
        .headers()
        .get(HOST)
        .and_then(|h| h.to_str().ok())
        .map(|h| {
            // Strip port if present
            h.split(':').next().unwrap_or(h).to_string()
        })
}

/// Main proxy handler that routes requests based on Host header and path.
async fn proxy_handler(State(state): State<Arc<EdgeState>>, request: Request<Body>) -> Response {
    let path = request.uri().path().to_string();

    // Extract host from headers
    let host = match extract_host(&request) {
        Some(h) => h,
        None => {
            return ApiError::with_reason(
                StatusCode::BAD_REQUEST,
                "MissingHost",
                "Host header is required",
            )
            .into_response();
        }
    };

    // Find matching route
    let route = match state.find_route(&host, &path) {
        Some(route) => route,
        None => {
            return ApiError::with_reason(
                StatusCode::NOT_FOUND,
                "RouteNotFound",
                format!("no route found for host '{}' and path '{}'", host, path),
            )
            .into_response();
        }
    };

    // Check if route is ready
    if !route.is_ready() {
        return ApiError::with_reason(
            StatusCode::SERVICE_UNAVAILABLE,
            "RouteNotReady",
            format!("route '{}' is not ready", route.name()),
        )
        .into_response();
    }

    // Transform the path if strip_prefix is enabled
    let target_path = route.transform_path(&path);

    // Resolve the backend endpoint
    let namespace = route.namespace();
    let service_namespace = route.spec.service.resolved_namespace(namespace);
    let service_name = &route.spec.service.name;
    let service_port = route.spec.service.port;

    let endpoint = match state.resolve_endpoint(service_namespace, service_name, service_port) {
        Some(ep) => ep,
        None => {
            return ApiError::with_reason(
                StatusCode::BAD_GATEWAY,
                "BackendUnavailable",
                format!(
                    "no endpoint found for service '{}/{}:{}'",
                    service_namespace, service_name, service_port
                ),
            )
            .into_response();
        }
    };

    // Forward the request to the backend
    let timeout = std::time::Duration::from_secs(route.spec.effective_timeout());

    match state
        .proxy_request(request, &endpoint, target_path, timeout)
        .await
    {
        Ok(response) => response,
        Err(err) => ApiError::with_reason(
            StatusCode::BAD_GATEWAY,
            "ProxyError",
            format!("failed to proxy request: {}", err),
        )
        .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn healthz_returns_ok() {
        let state = Arc::new(EdgeState::new());
        let app = build_edge_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_returns_unavailable_when_not_ready() {
        let state = Arc::new(EdgeState::new());
        let app = build_edge_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // New EdgeState is not ready by default
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn missing_host_returns_bad_request() {
        let state = Arc::new(EdgeState::new());
        let app = build_edge_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/some/path")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn unknown_route_returns_not_found() {
        let state = Arc::new(EdgeState::new());
        let app = build_edge_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/some/path")
                    .header("Host", "unknown.example.com")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn extract_host_strips_port() {
        let request = Request::builder()
            .uri("/test")
            .header("Host", "example.com:8080")
            .body(Body::empty())
            .unwrap();

        assert_eq!(extract_host(&request), Some("example.com".to_string()));
    }

    #[test]
    fn extract_host_without_port() {
        let request = Request::builder()
            .uri("/test")
            .header("Host", "example.com")
            .body(Body::empty())
            .unwrap();

        assert_eq!(extract_host(&request), Some("example.com".to_string()));
    }
}
