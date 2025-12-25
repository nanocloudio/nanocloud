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

//! Filesystem-backed storage for Route CRDs.

use crate::nanocloud::k8s::route::Route;
use crate::nanocloud::k8s::store::common::{
    bump_resource_version, deserialize_from_store, ensure_resource_version, namespaced_root,
    normalize_namespace, serialize_for_store, validate_resource_target, value_file_path,
    write_atomic_files,
};
use crate::nanocloud::util::error::with_context;

use std::error::Error;
use std::fs;
use std::io::ErrorKind;

pub const ROUTE_DIR: &str = "routes";

#[derive(Debug)]
pub struct StoredRoute {
    pub namespace: Option<String>,
    pub name: String,
    pub route: Route,
}

/// Lists all stored Routes across all namespaces.
pub fn list_routes() -> Result<Vec<StoredRoute>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = namespaced_root(ROUTE_DIR);
    let namespace_entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
        Err(err) => {
            return Err(with_context(
                err,
                format!("Failed to read Route root directory '{}'", root.display()),
            ))
        }
    };

    for namespace_entry in namespace_entries {
        let namespace_entry = namespace_entry.map_err(|err| {
            with_context(
                err,
                format!("Failed to iterate Route namespaces in '{}'", root.display()),
            )
        })?;
        let entry_type = namespace_entry.file_type().map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to inspect Route namespace entry '{}'",
                    namespace_entry.path().display()
                ),
            )
        })?;
        if !entry_type.is_dir() {
            continue;
        }
        let namespace_name = match namespace_entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };

        let namespace_path = namespace_entry.path();
        let route_entries = fs::read_dir(&namespace_path).map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to read Route namespace directory '{}'",
                    namespace_path.display()
                ),
            )
        })?;

        for route_entry in route_entries {
            let route_entry = route_entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate Route directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            let entry_type = route_entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect Route entry '{}'",
                        route_entry.path().display()
                    ),
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let route_name = match route_entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = value_file_path(ROUTE_DIR, &namespace_name, &route_name);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!("Failed to read Route value for '{}'", value_path.display()),
                    ))
                }
            };
            let route: Route =
                deserialize_from_store("Route", &raw, &value_path.display().to_string())?;

            let namespace = Some(namespace_name.clone()).filter(|ns| ns != "default");
            results.push(StoredRoute {
                namespace,
                name: route_name,
                route,
            });
        }
    }

    Ok(results)
}

/// Lists Routes in a specific namespace.
pub fn list_routes_for(
    namespace: Option<&str>,
) -> Result<Vec<StoredRoute>, Box<dyn Error + Send + Sync>> {
    let all = list_routes()?;
    let ns = normalize_namespace(namespace);
    Ok(all
        .into_iter()
        .filter(|stored| normalize_namespace(stored.namespace.as_deref()) == ns)
        .collect())
}

/// Gets a single Route by namespace and name.
pub fn get_route(
    namespace: Option<&str>,
    name: &str,
) -> Result<Option<Route>, Box<dyn Error + Send + Sync>> {
    let ns = normalize_namespace(namespace);
    let value_path = value_file_path(ROUTE_DIR, &ns, name);
    let raw = match fs::read_to_string(&value_path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(with_context(
                err,
                format!("Failed to read Route '{}/{}'", ns, name),
            ))
        }
    };
    let route: Route = deserialize_from_store("Route", &raw, &format!("{}/{}", ns, name))?;
    Ok(Some(route))
}

/// Saves a Route to the store.
pub fn save_route(
    namespace: Option<&str>,
    name: &str,
    mut route: Route,
) -> Result<Route, Box<dyn Error + Send + Sync>> {
    validate_resource_target(
        "Route",
        name,
        namespace,
        route.metadata.name.as_deref(),
        route.metadata.namespace.as_deref(),
    )?;

    let ns = normalize_namespace(namespace);

    // Ensure metadata is populated
    if route.metadata.name.is_none() {
        route.metadata.name = Some(name.to_string());
    }
    if route.metadata.namespace.is_none() && ns != "default" {
        route.metadata.namespace = Some(ns.clone());
    }

    // Handle resource version
    let existing = get_route(namespace, name)?;
    if existing.is_some() {
        bump_resource_version(&mut route.metadata);
    } else {
        ensure_resource_version(&mut route.metadata);
    }

    let value_path = value_file_path(ROUTE_DIR, &ns, name);
    let payload = serialize_for_store("Route", &route, &format!("Route {}/{}", ns, name))?;
    write_atomic_files(&[(&value_path, &payload)])?;

    Ok(route)
}

/// Deletes a Route from the store.
pub fn delete_route(
    namespace: Option<&str>,
    name: &str,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let ns = normalize_namespace(namespace);
    let route_dir = namespaced_root(ROUTE_DIR).join(&ns).join(name);

    match fs::remove_dir_all(&route_dir) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
        Err(err) => Err(with_context(
            err,
            format!("Failed to delete Route '{}/{}'", ns, name),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::route::{RouteSpec, ServiceRef};
    use tempfile::TempDir;

    fn with_temp_keyspace<F, T>(test: F) -> T
    where
        F: FnOnce() -> T,
    {
        use crate::nanocloud::test_support::keyspace_lock;
        let _lock = keyspace_lock().lock();
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let keyspace_prev = std::env::var("NANOCLOUD_KEYSPACE").ok();
        std::env::set_var("NANOCLOUD_KEYSPACE", temp_dir.path());
        let result = test();
        if let Some(prev) = keyspace_prev {
            std::env::set_var("NANOCLOUD_KEYSPACE", prev);
        } else {
            std::env::remove_var("NANOCLOUD_KEYSPACE");
        }
        result
    }

    #[test]
    fn save_and_get_route() {
        with_temp_keyspace(|| {
            let route = Route::new(
                "test-route",
                RouteSpec::new("example.com", ServiceRef::new("backend", 8080)),
            );

            let saved = save_route(Some("default"), "test-route", route).unwrap();
            assert!(saved.metadata.resource_version.is_some());

            let loaded = get_route(Some("default"), "test-route").unwrap();
            assert!(loaded.is_some());
            let loaded = loaded.unwrap();
            assert_eq!(loaded.name(), "test-route");
            assert_eq!(loaded.spec.host, "example.com");
        });
    }

    #[test]
    fn list_routes_returns_all() {
        with_temp_keyspace(|| {
            let route1 = Route::new(
                "route1",
                RouteSpec::new("host1.com", ServiceRef::new("svc1", 80)),
            );
            let route2 = Route::new(
                "route2",
                RouteSpec::new("host2.com", ServiceRef::new("svc2", 80)),
            );

            save_route(Some("default"), "route1", route1).unwrap();
            save_route(Some("other"), "route2", route2).unwrap();

            let all = list_routes().unwrap();
            assert_eq!(all.len(), 2);

            let default_only = list_routes_for(Some("default")).unwrap();
            assert_eq!(default_only.len(), 1);
            assert_eq!(default_only[0].name, "route1");
        });
    }

    #[test]
    fn delete_route_removes_resource() {
        with_temp_keyspace(|| {
            let route = Route::new(
                "to-delete",
                RouteSpec::new("delete.com", ServiceRef::new("svc", 80)),
            );

            save_route(Some("default"), "to-delete", route).unwrap();
            assert!(get_route(Some("default"), "to-delete").unwrap().is_some());

            let deleted = delete_route(Some("default"), "to-delete").unwrap();
            assert!(deleted);

            assert!(get_route(Some("default"), "to-delete").unwrap().is_none());

            // Deleting again should return false
            let deleted_again = delete_route(Some("default"), "to-delete").unwrap();
            assert!(!deleted_again);
        });
    }
}
