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

use crate::nanocloud::k8s::endpoints::Endpoints;
use crate::nanocloud::k8s::store::common::{
    bump_resource_version, deserialize_from_store, namespaced_key, namespaced_root,
    normalize_namespace, serialize_for_store, validate_resource_target, value_file_path,
    with_resource_lock, HotResourceCache, HotResourceCacheMetrics, ENDPOINTS_PREFIX, K8S_KEYSPACE,
};
use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::is_missing_value_error;

use std::error::Error;
use std::fs;
use std::io::ErrorKind;
use std::sync::OnceLock;

/// Set to `1`/`true` to enable in-memory endpoints list caching.
const ENDPOINTS_CACHE_ENV: &str = "NANOCLOUD_K8S_CACHE_ENDPOINTS";

fn endpoints_cache() -> &'static HotResourceCache<Vec<Endpoints>> {
    static CACHE: OnceLock<HotResourceCache<Vec<Endpoints>>> = OnceLock::new();
    CACHE.get_or_init(|| HotResourceCache::new("endpoints", ENDPOINTS_CACHE_ENV))
}

fn endpoints_cache_key(namespace: Option<&str>) -> String {
    namespace
        .map(|ns| format!("ns:{}", normalize_namespace(Some(ns))))
        .unwrap_or_else(|| "cluster".to_string())
}

fn invalidate_endpoints_cache(namespace: Option<&str>) {
    let cache = endpoints_cache();
    cache.invalidate(&endpoints_cache_key(namespace));
    cache.invalidate(&endpoints_cache_key(None));
}

#[allow(dead_code)]
pub fn endpoints_cache_metrics() -> HotResourceCacheMetrics {
    endpoints_cache().metrics()
}

pub fn list_endpoints(
    namespace: Option<&str>,
) -> Result<Vec<Endpoints>, Box<dyn Error + Send + Sync>> {
    let cache_key = endpoints_cache_key(namespace);
    if let Some(cached) = endpoints_cache().get(&cache_key) {
        return Ok(cached);
    }

    let mut results = Vec::new();
    let root = namespaced_root(ENDPOINTS_PREFIX);

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
                                "Failed to iterate Endpoints namespaces in '{}'",
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
                                    "Failed to inspect Endpoints namespace entry '{}'",
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
                    format!(
                        "Failed to read Endpoints root directory '{}'",
                        root.display()
                    ),
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
                        "Failed to read Endpoints namespace directory '{}'",
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
                        "Failed to iterate Endpoints directory '{}'",
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
                            "Failed to inspect Endpoints entry '{}'",
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

            let value_path = value_file_path(ENDPOINTS_PREFIX, &ns, &name);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!(
                            "Failed to load Endpoints payload '{}'",
                            value_path.display()
                        ),
                    ))
                }
            };

            let mut endpoints: Endpoints = deserialize_from_store(
                "Endpoints",
                &raw,
                &format!(
                    "Failed to deserialize Endpoints '{}' from '{}'",
                    name,
                    value_path.display()
                ),
            )?;

            if endpoints.api_version.is_empty() {
                endpoints.api_version = "nanocloud.io/v1".to_string();
            }
            if endpoints.kind.is_empty() {
                endpoints.kind = "Endpoints".to_string();
            }
            if endpoints.metadata.name.is_none() {
                endpoints.metadata.name = Some(name.clone());
            }
            if endpoints.metadata.namespace.is_none() {
                endpoints.metadata.namespace = Some(ns.clone());
            }
            if endpoints.metadata.resource_version.is_none() {
                endpoints.metadata.resource_version = Some("1".to_string());
            }

            results.push(endpoints);
        }
    }

    endpoints_cache().insert(cache_key, results.clone());
    Ok(results)
}

pub fn save_endpoints(
    namespace: Option<&str>,
    name: &str,
    endpoints: &Endpoints,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    validate_resource_target(
        "Endpoints",
        name,
        namespace,
        endpoints.metadata.name.as_deref(),
        endpoints.metadata.namespace.as_deref(),
    )?;
    let key = namespaced_key(ENDPOINTS_PREFIX, namespace, name);
    with_resource_lock(&key, || {
        let mut payload = endpoints.clone();
        bump_resource_version(&mut payload.metadata);
        let payload = serialize_for_store(
            "Endpoints",
            &payload,
            &format!("Failed to serialize Endpoints for key '{}'", key),
        )?;
        let result = K8S_KEYSPACE
            .put(&key, &payload)
            .map_err(|err| with_context(err, format!("Failed to persist Endpoints '{}'", key)));
        invalidate_endpoints_cache(namespace);
        result
    })
}

pub fn delete_endpoints(
    namespace: Option<&str>,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = namespaced_key(ENDPOINTS_PREFIX, namespace, name);
    validate_resource_target("Endpoints", name, namespace, None, None)?;
    with_resource_lock(&key, || {
        let result = match K8S_KEYSPACE.delete(&key) {
            Ok(_) => Ok(()),
            Err(err) => {
                if is_missing_value_error(err.as_ref()) {
                    Ok(())
                } else {
                    Err(with_context(
                        err,
                        format!("Failed to delete Endpoints '{}' from keyspace", key),
                    ))
                }
            }
        };
        invalidate_endpoints_cache(namespace);
        result
    })
}
