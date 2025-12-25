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

//! End-to-end tests for Route resources and edge server routing.
//!
//! Tests verify:
//! - Route persistence and retrieval
//! - Host/path matching logic
//! - Strip-prefix transformation
//! - Timeout handling
//! - Controller reconciliation

use nanocloud::nanocloud::k8s::route::{Route, RouteSpec, ServiceRef};
use nanocloud::nanocloud::k8s::store::{
    delete_route, get_route, list_routes, list_routes_for, save_route,
};
use nanocloud::nanocloud::test_support::keyspace_lock;
use std::env;
use std::fs;
use std::sync::MutexGuard;
use tempfile::TempDir;

struct TestEnv {
    _dir: TempDir,
    _lock: MutexGuard<'static, ()>,
    keyspace_previous: Option<String>,
    lock_previous: Option<String>,
}

impl TestEnv {
    fn new() -> Self {
        let lock = keyspace_lock().lock();
        let dir = tempfile::tempdir().expect("tempdir");
        let keyspace_previous = env::var("NANOCLOUD_KEYSPACE").ok();
        env::set_var("NANOCLOUD_KEYSPACE", dir.path());

        let lock_previous = env::var("NANOCLOUD_LOCK_FILE").ok();
        let lock_path = dir.path().join("nanocloud.lock");
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).expect("lock dir");
        }
        fs::File::create(&lock_path).expect("lock file");
        env::set_var("NANOCLOUD_LOCK_FILE", lock_path.to_string_lossy().as_ref());

        Self {
            _dir: dir,
            _lock: lock,
            keyspace_previous,
            lock_previous,
        }
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        if let Some(previous) = self.keyspace_previous.as_ref() {
            env::set_var("NANOCLOUD_KEYSPACE", previous);
        } else {
            env::remove_var("NANOCLOUD_KEYSPACE");
        }

        if let Some(previous) = self.lock_previous.as_ref() {
            env::set_var("NANOCLOUD_LOCK_FILE", previous);
        } else {
            env::remove_var("NANOCLOUD_LOCK_FILE");
        }
    }
}

#[test]
fn route_crud_operations() {
    let _env = TestEnv::new();

    // Create a route
    let route = Route::new(
        "api-gateway",
        RouteSpec::new("api.example.com", ServiceRef::new("api-service", 8080))
            .with_path_prefix("/v1")
            .with_strip_prefix(true)
            .with_timeout(60),
    );

    // Save the route
    let saved = save_route(Some("default"), "api-gateway", route).expect("save route");
    assert!(saved.metadata.resource_version.is_some());
    assert_eq!(saved.name(), "api-gateway");

    // Read the route
    let loaded = get_route(Some("default"), "api-gateway")
        .expect("get route")
        .expect("route exists");
    assert_eq!(loaded.spec.host, "api.example.com");
    assert_eq!(loaded.spec.path_prefix, Some("/v1".to_string()));
    assert!(loaded.spec.strip_prefix);
    assert_eq!(loaded.spec.timeout_seconds, Some(60));

    // Update the route
    let mut updated = loaded.clone();
    updated.spec.timeout_seconds = Some(120);
    let saved_again = save_route(Some("default"), "api-gateway", updated).expect("update route");

    // Verify resource version was bumped
    let original_rv: i64 = saved
        .metadata
        .resource_version
        .as_ref()
        .unwrap()
        .parse()
        .unwrap();
    let updated_rv: i64 = saved_again
        .metadata
        .resource_version
        .as_ref()
        .unwrap()
        .parse()
        .unwrap();
    assert!(updated_rv > original_rv);

    // Delete the route
    let deleted = delete_route(Some("default"), "api-gateway").expect("delete route");
    assert!(deleted);

    // Verify deletion
    let gone = get_route(Some("default"), "api-gateway").expect("get deleted route");
    assert!(gone.is_none());
}

#[test]
fn route_list_operations() {
    let _env = TestEnv::new();

    // Create routes in different namespaces
    let routes = vec![
        ("default", "route-1", "host1.com"),
        ("default", "route-2", "host2.com"),
        ("production", "route-3", "host3.com"),
        ("staging", "route-4", "host4.com"),
    ];

    for (ns, name, host) in &routes {
        let route = Route::new(*name, RouteSpec::new(*host, ServiceRef::new("backend", 80)));
        save_route(Some(ns), name, route).expect("save route");
    }

    // List all routes
    let all = list_routes().expect("list all routes");
    assert_eq!(all.len(), 4);

    // List routes by namespace
    let default_routes = list_routes_for(Some("default")).expect("list default routes");
    assert_eq!(default_routes.len(), 2);

    let prod_routes = list_routes_for(Some("production")).expect("list production routes");
    assert_eq!(prod_routes.len(), 1);
    assert_eq!(prod_routes[0].name, "route-3");
}

#[test]
fn route_host_path_matching() {
    let route = Route::new(
        "api",
        RouteSpec::new("api.example.com", ServiceRef::new("api", 8080)).with_path_prefix("/api/v1"),
    );

    // Exact host match with matching path
    assert!(route.matches("api.example.com", "/api/v1"));
    assert!(route.matches("api.example.com", "/api/v1/users"));

    // Host match but path doesn't match
    assert!(!route.matches("api.example.com", "/web/v1"));

    // Wrong host
    assert!(!route.matches("web.example.com", "/api/v1"));

    // Route without path prefix matches any path
    let route_no_prefix = Route::new(
        "catch-all",
        RouteSpec::new("example.com", ServiceRef::new("default", 80)),
    );
    assert!(route_no_prefix.matches("example.com", "/anything"));
    assert!(route_no_prefix.matches("example.com", "/"));
}

#[test]
fn route_strip_prefix_transformation() {
    // With strip prefix enabled
    let route = Route::new(
        "api",
        RouteSpec::new("api.example.com", ServiceRef::new("api", 8080))
            .with_path_prefix("/api/v1")
            .with_strip_prefix(true),
    );

    assert_eq!(route.transform_path("/api/v1/users"), "/users");
    assert_eq!(route.transform_path("/api/v1"), "/");
    assert_eq!(route.transform_path("/api/v1/"), "/");
    assert_eq!(route.transform_path("/other"), "/other"); // No match, no transformation

    // Without strip prefix
    let route_no_strip = Route::new(
        "api",
        RouteSpec::new("api.example.com", ServiceRef::new("api", 8080))
            .with_path_prefix("/api/v1")
            .with_strip_prefix(false),
    );

    assert_eq!(
        route_no_strip.transform_path("/api/v1/users"),
        "/api/v1/users"
    );
}

#[test]
fn route_timeout_defaults() {
    // Route with explicit timeout
    let route = Route::new(
        "slow",
        RouteSpec::new("slow.example.com", ServiceRef::new("slow-service", 8080)).with_timeout(120),
    );
    assert_eq!(route.spec.effective_timeout(), 120);

    // Route without timeout uses default
    let route_default = Route::new(
        "fast",
        RouteSpec::new("fast.example.com", ServiceRef::new("fast-service", 8080)),
    );
    assert_eq!(route_default.spec.effective_timeout(), 30);
}

#[test]
fn route_validation_errors() {
    // Missing host
    let mut route = Route::new(
        "invalid",
        RouteSpec::new("", ServiceRef::new("backend", 80)),
    );
    assert!(route.validate().is_err());

    // Missing service name
    route = Route::new(
        "invalid",
        RouteSpec::new("example.com", ServiceRef::new("", 80)),
    );
    assert!(route.validate().is_err());

    // Invalid port
    route = Route::new(
        "invalid",
        RouteSpec::new("example.com", ServiceRef::new("backend", 0)),
    );
    assert!(route.validate().is_err());

    // Invalid path prefix (no leading slash)
    route = Route::new(
        "invalid",
        RouteSpec {
            host: "example.com".to_string(),
            path_prefix: Some("api".to_string()), // Missing leading slash
            service: ServiceRef::new("backend", 80),
            ..Default::default()
        },
    );
    assert!(route.validate().is_err());

    // Valid route
    route = Route::new(
        "valid",
        RouteSpec::new("example.com", ServiceRef::new("backend", 80)).with_path_prefix("/api"),
    );
    assert!(route.validate().is_ok());
}

#[test]
fn route_service_ref_namespace_resolution() {
    // ServiceRef without namespace uses route's namespace
    let sref = ServiceRef::new("backend", 8080);
    assert_eq!(sref.resolved_namespace("default"), "default");
    assert_eq!(sref.resolved_namespace("production"), "production");

    // ServiceRef with explicit namespace overrides
    let sref_with_ns = ServiceRef::new("backend", 8080).with_namespace("shared");
    assert_eq!(sref_with_ns.resolved_namespace("default"), "shared");
    assert_eq!(sref_with_ns.resolved_namespace("production"), "shared");
}

#[test]
fn route_status_ready_conditions() {
    let mut route = Route::new(
        "test",
        RouteSpec::new("example.com", ServiceRef::new("backend", 80)),
    );

    // Initially not ready (no status)
    assert!(!route.is_ready());

    // Set status to ready
    let mut status = nanocloud::nanocloud::k8s::route::RouteStatus::default();
    status.set_ready(true, None, None);
    route.status = Some(status);
    assert!(route.is_ready());

    // Set status to not ready
    let mut status = route.status.take().unwrap();
    status.set_ready(
        false,
        Some("ServiceNotFound"),
        Some("Backend service not found"),
    );
    route.status = Some(status);
    assert!(!route.is_ready());
}

#[test]
fn route_serialization_roundtrip() {
    let route = Route::new(
        "full-featured",
        RouteSpec::new(
            "api.example.com",
            ServiceRef::new("api-service", 8080).with_namespace("backend"),
        )
        .with_path_prefix("/api/v2")
        .with_strip_prefix(true)
        .with_timeout(90),
    );

    // Serialize to JSON
    let json = serde_json::to_string_pretty(&route).expect("serialize route");

    // Deserialize back
    let parsed: Route = serde_json::from_str(&json).expect("deserialize route");

    assert_eq!(parsed.name(), "full-featured");
    assert_eq!(parsed.spec.host, "api.example.com");
    assert_eq!(parsed.spec.path_prefix, Some("/api/v2".to_string()));
    assert!(parsed.spec.strip_prefix);
    assert_eq!(parsed.spec.timeout_seconds, Some(90));
    assert_eq!(parsed.spec.service.name, "api-service");
    assert_eq!(parsed.spec.service.namespace, Some("backend".to_string()));
    assert_eq!(parsed.spec.service.port, 8080);
}

#[test]
fn route_resource_version_management() {
    let _env = TestEnv::new();

    // Create new route should set resource version to 1
    let route = Route::new(
        "versioned",
        RouteSpec::new("example.com", ServiceRef::new("backend", 80)),
    );
    let saved = save_route(Some("default"), "versioned", route).expect("save");
    assert_eq!(saved.metadata.resource_version, Some("1".to_string()));

    // Update should increment resource version
    let mut updated = saved.clone();
    updated.spec.timeout_seconds = Some(60);
    let saved2 = save_route(Some("default"), "versioned", updated).expect("update");
    assert_eq!(saved2.metadata.resource_version, Some("2".to_string()));

    // Another update should increment again
    let mut updated2 = saved2.clone();
    updated2.spec.timeout_seconds = Some(120);
    let saved3 = save_route(Some("default"), "versioned", updated2).expect("update again");
    assert_eq!(saved3.metadata.resource_version, Some("3".to_string()));
}
