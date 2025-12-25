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

//! Shared state for the edge server.
//!
//! This module provides the `EdgeState` type which maintains:
//! - Route registry for host/path-based routing
//! - Service endpoint resolution
//! - HTTP client for proxying

use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use axum::response::Response;

use crate::nanocloud::k8s::route::Route;

/// Shared state for the edge server.
pub struct EdgeState {
    /// Whether the edge server is ready to serve traffic.
    ready: AtomicBool,

    /// Route registry indexed by host.
    /// Multiple routes can exist for a single host with different path prefixes.
    routes: RwLock<HashMap<String, Vec<Route>>>,

    /// Endpoint cache: (namespace, service_name, port) -> endpoint address
    endpoints: RwLock<HashMap<(String, String, u16), String>>,

    /// HTTP client for proxying requests.
    client: reqwest::Client,
}

impl Default for EdgeState {
    fn default() -> Self {
        Self::new()
    }
}

impl EdgeState {
    /// Create a new EdgeState.
    pub fn new() -> Self {
        Self {
            ready: AtomicBool::new(false),
            routes: RwLock::new(HashMap::new()),
            endpoints: RwLock::new(HashMap::new()),
            client: reqwest::Client::builder()
                .pool_max_idle_per_host(10)
                .timeout(Duration::from_secs(30))
                .build()
                .expect("failed to create HTTP client"),
        }
    }

    /// Check if the edge server is ready.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    /// Set the ready state.
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
    }

    /// Register a route.
    pub fn register_route(&self, route: Route) {
        let host = route.spec.host.clone();
        let mut routes = self.routes.write().expect("routes lock poisoned");

        let host_routes = routes.entry(host).or_default();

        // Remove existing route with same name/namespace
        host_routes.retain(|r| r.name() != route.name() || r.namespace() != route.namespace());

        // Insert new route, sorted by path prefix length (longest first)
        host_routes.push(route);
        host_routes.sort_by(|a, b| {
            let a_len = a.spec.path_prefix.as_ref().map(|p| p.len()).unwrap_or(0);
            let b_len = b.spec.path_prefix.as_ref().map(|p| p.len()).unwrap_or(0);
            b_len.cmp(&a_len) // Longest first
        });
    }

    /// Unregister a route by name and namespace.
    pub fn unregister_route(&self, namespace: &str, name: &str) {
        let mut routes = self.routes.write().expect("routes lock poisoned");

        for host_routes in routes.values_mut() {
            host_routes.retain(|r| r.name() != name || r.namespace() != namespace);
        }

        // Remove empty host entries
        routes.retain(|_, v| !v.is_empty());
    }

    /// Find a route matching the given host and path.
    pub fn find_route(&self, host: &str, path: &str) -> Option<Route> {
        let routes = self.routes.read().expect("routes lock poisoned");

        let host_routes = routes.get(host)?;

        // Routes are sorted by path prefix length (longest first)
        // so we find the most specific match
        for route in host_routes {
            if route.matches(host, path) {
                return Some(route.clone());
            }
        }

        None
    }

    /// Get all registered routes.
    pub fn list_routes(&self) -> Vec<Route> {
        let routes = self.routes.read().expect("routes lock poisoned");
        routes.values().flatten().cloned().collect()
    }

    /// Register a service endpoint.
    pub fn register_endpoint(
        &self,
        namespace: &str,
        service_name: &str,
        port: u16,
        endpoint: &str,
    ) {
        let mut endpoints = self.endpoints.write().expect("endpoints lock poisoned");
        endpoints.insert(
            (namespace.to_string(), service_name.to_string(), port),
            endpoint.to_string(),
        );
    }

    /// Unregister a service endpoint.
    pub fn unregister_endpoint(&self, namespace: &str, service_name: &str, port: u16) {
        let mut endpoints = self.endpoints.write().expect("endpoints lock poisoned");
        endpoints.remove(&(namespace.to_string(), service_name.to_string(), port));
    }

    /// Resolve a service endpoint.
    pub fn resolve_endpoint(
        &self,
        namespace: &str,
        service_name: &str,
        port: u16,
    ) -> Option<String> {
        let endpoints = self.endpoints.read().expect("endpoints lock poisoned");
        endpoints
            .get(&(namespace.to_string(), service_name.to_string(), port))
            .cloned()
    }

    /// Proxy a request to the given endpoint.
    pub async fn proxy_request(
        &self,
        request: Request<Body>,
        endpoint: &str,
        target_path: &str,
        timeout: Duration,
    ) -> Result<Response, Box<dyn Error + Send + Sync>> {
        let method = request.method().clone();
        let headers = request.headers().clone();

        // Build target URL
        let query = request
            .uri()
            .query()
            .map(|q| format!("?{}", q))
            .unwrap_or_default();
        let target_url = format!("http://{}{}{}", endpoint, target_path, query);

        // Build the proxied request
        let mut builder = self.client.request(method, &target_url).timeout(timeout);

        // Copy headers, skipping hop-by-hop headers
        for (name, value) in headers.iter() {
            if !is_hop_by_hop_header(name.as_str()) {
                if let Ok(value_str) = value.to_str() {
                    builder = builder.header(name.as_str(), value_str);
                }
            }
        }

        // Forward the body
        let body_bytes = axum::body::to_bytes(request.into_body(), usize::MAX).await?;
        if !body_bytes.is_empty() {
            builder = builder.body(body_bytes);
        }

        // Send the request
        let response = builder.send().await?;

        // Convert reqwest response to axum response
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.bytes().await?;

        let mut response_builder = axum::http::Response::builder().status(status);

        for (name, value) in headers.iter() {
            if !is_hop_by_hop_header(name.as_str()) {
                response_builder = response_builder.header(name, value);
            }
        }

        Ok(response_builder.body(Body::from(body))?)
    }
}

/// Check if a header is a hop-by-hop header that should not be forwarded.
fn is_hop_by_hop_header(name: &str) -> bool {
    matches!(
        name.to_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::route::{RouteSpec, RouteStatus, ServiceRef};

    fn make_route(name: &str, host: &str, path_prefix: Option<&str>) -> Route {
        let mut spec = RouteSpec::new(host, ServiceRef::new("backend", 8080));
        if let Some(prefix) = path_prefix {
            spec = spec.with_path_prefix(prefix);
        }
        let mut route = Route::new(name, spec);
        route.status = Some(RouteStatus::default());
        route.status.as_mut().unwrap().set_ready(true, None, None);
        route
    }

    #[test]
    fn register_and_find_route() {
        let state = EdgeState::new();

        let route = make_route("test", "example.com", Some("/api"));
        state.register_route(route);

        let found = state.find_route("example.com", "/api/users");
        assert!(found.is_some());
        assert_eq!(found.unwrap().name(), "test");

        let not_found = state.find_route("other.com", "/api/users");
        assert!(not_found.is_none());
    }

    #[test]
    fn longest_prefix_match() {
        let state = EdgeState::new();

        // Register routes with different path prefixes
        state.register_route(make_route("api", "example.com", Some("/api")));
        state.register_route(make_route("api-v2", "example.com", Some("/api/v2")));
        state.register_route(make_route("root", "example.com", None));

        // /api/v2/users should match "api-v2" (longest prefix)
        let found = state.find_route("example.com", "/api/v2/users");
        assert_eq!(found.unwrap().name(), "api-v2");

        // /api/v1/users should match "api"
        let found = state.find_route("example.com", "/api/v1/users");
        assert_eq!(found.unwrap().name(), "api");

        // /other should match "root"
        let found = state.find_route("example.com", "/other");
        assert_eq!(found.unwrap().name(), "root");
    }

    #[test]
    fn unregister_route() {
        let state = EdgeState::new();

        state.register_route(make_route("test", "example.com", Some("/api")));
        assert!(state.find_route("example.com", "/api").is_some());

        state.unregister_route("default", "test");
        assert!(state.find_route("example.com", "/api").is_none());
    }

    #[test]
    fn endpoint_resolution() {
        let state = EdgeState::new();

        state.register_endpoint("default", "backend", 8080, "10.0.0.1:8080");

        let endpoint = state.resolve_endpoint("default", "backend", 8080);
        assert_eq!(endpoint, Some("10.0.0.1:8080".to_string()));

        let not_found = state.resolve_endpoint("default", "other", 8080);
        assert!(not_found.is_none());
    }

    #[test]
    fn ready_state() {
        let state = EdgeState::new();

        assert!(!state.is_ready());

        state.set_ready(true);
        assert!(state.is_ready());

        state.set_ready(false);
        assert!(!state.is_ready());
    }

    #[test]
    fn hop_by_hop_headers() {
        assert!(is_hop_by_hop_header("Connection"));
        assert!(is_hop_by_hop_header("connection"));
        assert!(is_hop_by_hop_header("Transfer-Encoding"));
        assert!(!is_hop_by_hop_header("Content-Type"));
        assert!(!is_hop_by_hop_header("X-Custom-Header"));
    }
}
