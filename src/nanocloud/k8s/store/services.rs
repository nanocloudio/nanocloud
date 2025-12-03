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

use crate::nanocloud::k8s::service::Service;
use crate::nanocloud::k8s::store::common::{
    bump_resource_version, deserialize_from_store, namespaced_key, namespaced_root,
    normalize_namespace, serialize_for_store, validate_resource_target, value_file_path,
    with_resource_lock, HotResourceCache, HotResourceCacheMetrics, K8S_KEYSPACE, SERVICE_PREFIX,
};
use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::is_missing_value_error;

use std::error::Error;
use std::fs;
use std::io::ErrorKind;
use std::sync::OnceLock;

/// Set to `1`/`true` to enable in-memory service list caching.
const SERVICE_CACHE_ENV: &str = "NANOCLOUD_K8S_CACHE_SERVICES";

fn service_cache() -> &'static HotResourceCache<Vec<Service>> {
    static CACHE: OnceLock<HotResourceCache<Vec<Service>>> = OnceLock::new();
    CACHE.get_or_init(|| HotResourceCache::new("services", SERVICE_CACHE_ENV))
}

fn service_cache_key(namespace: Option<&str>) -> String {
    namespace
        .map(|ns| format!("ns:{}", normalize_namespace(Some(ns))))
        .unwrap_or_else(|| "cluster".to_string())
}

fn invalidate_service_cache(namespace: Option<&str>) {
    let cache = service_cache();
    cache.invalidate(&service_cache_key(namespace));
    cache.invalidate(&service_cache_key(None));
}

#[allow(dead_code)]
pub fn service_cache_metrics() -> HotResourceCacheMetrics {
    service_cache().metrics()
}

pub fn list_services(
    namespace: Option<&str>,
) -> Result<Vec<Service>, Box<dyn Error + Send + Sync>> {
    let cache_key = service_cache_key(namespace);
    if let Some(cached) = service_cache().get(&cache_key) {
        return Ok(cached);
    }

    let mut results = Vec::new();
    let root = namespaced_root(SERVICE_PREFIX);

    let mut namespaces: Vec<String> = Vec::new();
    if let Some(ns) = namespace {
        namespaces.push(normalize_namespace(Some(ns)));
    } else {
        match fs::read_dir(&root) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry.map_err(|err| {
                        with_context(
                            err,
                            format!(
                                "Failed to iterate Service namespaces in '{}'",
                                root.display()
                            ),
                        )
                    })?;
                    if !entry
                        .file_type()
                        .map_err(|err| {
                            with_context(
                                err,
                                format!(
                                    "Failed to inspect Service namespace entry '{}'",
                                    entry.path().display()
                                ),
                            )
                        })?
                        .is_dir()
                    {
                        continue;
                    }
                    if let Ok(name) = entry.file_name().into_string() {
                        namespaces.push(name);
                    }
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
            Err(err) => {
                return Err(with_context(
                    err,
                    format!("Failed to read Service root directory '{}'", root.display()),
                ))
            }
        }
    }

    for ns in namespaces {
        let namespace_path = root.join(&ns);
        let entries = match fs::read_dir(&namespace_path) {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(with_context(
                    err,
                    format!(
                        "Failed to read Service namespace directory '{}'",
                        namespace_path.display()
                    ),
                ))
            }
        };

        for entry in entries {
            let entry = entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate Service directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            if !entry
                .file_type()
                .map_err(|err| {
                    with_context(
                        err,
                        format!(
                            "Failed to inspect Service entry '{}'",
                            entry.path().display()
                        ),
                    )
                })?
                .is_dir()
            {
                continue;
            }

            let name = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = value_file_path(SERVICE_PREFIX, &ns, &name);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!("Failed to load Service payload '{}'", value_path.display()),
                    ))
                }
            };

            let mut service: Service = deserialize_from_store(
                "Service",
                &raw,
                &format!(
                    "Failed to deserialize Service '{}' from '{}'",
                    name,
                    value_path.display()
                ),
            )?;

            if service.metadata.name.is_none() {
                service.metadata.name = Some(name.clone());
            }
            if service.metadata.namespace.is_none() {
                service.metadata.namespace = Some(ns.clone());
            }
            if service.metadata.resource_version.is_none() {
                service.metadata.resource_version = Some("1".to_string());
            }

            results.push(service);
        }
    }

    service_cache().insert(cache_key, results.clone());
    Ok(results)
}

pub fn save_service(
    namespace: Option<&str>,
    name: &str,
    service: &Service,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    validate_resource_target(
        "Service",
        name,
        namespace,
        service.metadata.name.as_deref(),
        service.metadata.namespace.as_deref(),
    )?;
    let key = namespaced_key(SERVICE_PREFIX, namespace, name);
    with_resource_lock(&key, || {
        let mut payload = service.clone();
        bump_resource_version(&mut payload.metadata);
        let payload = serialize_for_store(
            "Service",
            &payload,
            &format!("Failed to serialize Service for key '{}'", key),
        )?;
        let result = K8S_KEYSPACE
            .put(&key, &payload)
            .map_err(|err| with_context(err, format!("Failed to persist Service '{}'", key)));
        invalidate_service_cache(namespace);
        result
    })
}

pub fn delete_service(
    namespace: Option<&str>,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = namespaced_key(SERVICE_PREFIX, namespace, name);
    validate_resource_target("Service", name, namespace, None, None)?;
    with_resource_lock(&key, || {
        let result = match K8S_KEYSPACE.delete(&key) {
            Ok(_) => Ok(()),
            Err(err) => {
                if is_missing_value_error(err.as_ref()) {
                    Ok(())
                } else {
                    Err(with_context(
                        err,
                        format!("Failed to delete Service '{}' from keyspace", key),
                    ))
                }
            }
        };
        invalidate_service_cache(namespace);
        result
    })
}
