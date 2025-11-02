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

use super::error::ApiError;
use super::selectors::{ensure_named_resource, matches_config_map_filter, parse_object_selector};
use super::watch::{
    ensure_resource_version_match, parse_resource_version, resource_version_is_newer,
    ResourceVersionMatchPolicy, WatchPredicate, WatchStreamBuilder,
};
use crate::nanocloud::k8s::configmap::{ConfigMap, ConfigMapList};
use crate::nanocloud::k8s::configmap_manager::{
    ConfigMapError, ConfigMapRegistry, ConfigMapWatchEvent,
};
use crate::nanocloud::k8s::store::{
    decode_continue_token, encode_continue_token, paginate_entries, PaginationError,
};

use axum::extract::{Path, Query};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

#[derive(Default, Deserialize)]
pub struct WatchParams {
    #[serde(default)]
    watch: Option<bool>,
    #[serde(rename = "resourceVersion")]
    resource_version: Option<String>,
    #[serde(rename = "fieldSelector")]
    field_selector: Option<String>,
    #[serde(rename = "labelSelector")]
    label_selector: Option<String>,
    #[serde(rename = "timeoutSeconds")]
    timeout_seconds: Option<u64>,
    #[serde(rename = "allowWatchBookmarks")]
    allow_watch_bookmarks: Option<bool>,
    #[serde(rename = "limit")]
    limit: Option<u32>,
    #[serde(rename = "continue")]
    continue_token: Option<String>,
    #[serde(rename = "resourceVersionMatch")]
    resource_version_match: Option<ResourceVersionMatchPolicy>,
}

fn map_error(err: ConfigMapError) -> ApiError {
    match err {
        ConfigMapError::AlreadyExists(msg) | ConfigMapError::Conflict(msg) => {
            ApiError::new(StatusCode::CONFLICT, msg)
        }
        ConfigMapError::NotFound(msg) => ApiError::new(StatusCode::NOT_FOUND, msg),
        ConfigMapError::Invalid(msg) => ApiError::bad_request(msg),
        ConfigMapError::Persistence(err) => ApiError::internal_error(err),
    }
}

pub async fn list_all(Query(params): Query<WatchParams>) -> Result<Response, ApiError> {
    handle_list(None, params).await
}

pub async fn list_namespace(
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response, ApiError> {
    handle_list(Some(namespace.as_str()), params).await
}

pub async fn get(
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<WatchParams>,
) -> Result<Response, ApiError> {
    let registry = ConfigMapRegistry::shared();
    let WatchParams {
        watch,
        resource_version,
        field_selector,
        label_selector,
        timeout_seconds,
        allow_watch_bookmarks,
        limit,
        continue_token,
        resource_version_match,
    } = params;
    let resource_version_threshold = parse_resource_version(resource_version.as_deref())?;
    let allow_bookmarks = allow_watch_bookmarks.unwrap_or(false);
    let timeout = timeout_seconds.and_then(|seconds| {
        if seconds == 0 {
            None
        } else {
            Some(Duration::from_secs(seconds))
        }
    });

    if let Some(limit) = limit {
        if limit == 0 {
            return Err(ApiError::bad_request("limit must be greater than 0"));
        }
    }

    if limit.is_some() {
        return Err(ApiError::bad_request(
            "limit is not supported for single resource requests",
        ));
    }
    if continue_token.is_some() {
        return Err(ApiError::bad_request(
            "continue is not supported for single resource requests",
        ));
    }
    if resource_version_match.is_some() {
        return Err(ApiError::bad_request(
            "resourceVersionMatch is not supported for single resource requests",
        ));
    }
    let filter =
        parse_object_selector(field_selector.as_deref(), label_selector.as_deref())?.map(Arc::new);

    let existing = registry.get(&namespace, &name).await;

    if watch.unwrap_or(false) {
        let config_map = match existing.clone() {
            Some(config_map) => config_map,
            None => {
                return Err(ApiError::new(
                    StatusCode::NOT_FOUND,
                    format!("ConfigMap '{}' not found", name),
                ))
            }
        };

        let matches_selector = matches_config_map_filter(filter.as_deref(), &config_map);
        let include =
            matches_selector && resource_version_is_newer(&config_map, resource_version_threshold);
        let events = if include {
            vec![ConfigMapWatchEvent {
                event_type: "ADDED".to_string(),
                object: config_map,
            }]
        } else {
            Vec::new()
        };
        let receiver = registry.watch_config_map(&namespace, &name).await;
        let filter_for_watch: Option<Arc<WatchPredicate<ConfigMap>>> = filter.as_ref().map(|sel| {
            let selector = Arc::clone(sel);
            Arc::new(move |config_map: &ConfigMap| selector.matches_config_map(config_map))
                as Arc<WatchPredicate<ConfigMap>>
        });
        let body = WatchStreamBuilder::new(
            "server_configmaps",
            "ConfigMap watch serialization error",
            events,
            receiver,
        )
        .with_filter(filter_for_watch)
        .with_bookmarks(allow_bookmarks)
        .with_timeout(timeout)
        .into_body();
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .unwrap();
        Ok(response)
    } else {
        let config_map = ensure_named_resource(
            existing,
            filter.as_deref(),
            |config_map, selector| selector.matches_config_map(config_map),
            format!("ConfigMap '{}' not found", name),
        )?;
        Ok(Json(config_map).into_response())
    }
}

pub async fn create(
    Path(namespace): Path<String>,
    Json(payload): Json<ConfigMap>,
) -> Result<(StatusCode, Json<ConfigMap>), ApiError> {
    let registry = ConfigMapRegistry::shared();
    registry
        .create(&namespace, payload)
        .await
        .map(|config_map| (StatusCode::CREATED, Json(config_map)))
        .map_err(map_error)
}

pub async fn replace(
    Path((namespace, name)): Path<(String, String)>,
    Json(payload): Json<ConfigMap>,
) -> Result<Json<ConfigMap>, ApiError> {
    let registry = ConfigMapRegistry::shared();
    registry
        .replace(&namespace, &name, payload)
        .await
        .map(Json)
        .map_err(map_error)
}

pub async fn delete(
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<ConfigMap>, ApiError> {
    let registry = ConfigMapRegistry::shared();
    registry
        .delete(&namespace, &name)
        .await
        .map(Json)
        .map_err(map_error)
}

async fn handle_list(namespace: Option<&str>, params: WatchParams) -> Result<Response, ApiError> {
    let registry = ConfigMapRegistry::shared();
    let WatchParams {
        watch,
        resource_version,
        field_selector,
        label_selector,
        timeout_seconds,
        allow_watch_bookmarks,
        limit,
        continue_token,
        resource_version_match,
    } = params;
    let resource_version_threshold = parse_resource_version(resource_version.as_deref())?;
    let allow_bookmarks = allow_watch_bookmarks.unwrap_or(false);
    let timeout = timeout_seconds.and_then(|seconds| {
        if seconds == 0 {
            None
        } else {
            Some(Duration::from_secs(seconds))
        }
    });
    if let Some(limit) = limit {
        if limit == 0 {
            return Err(ApiError::bad_request("limit must be greater than 0"));
        }
    }
    let current_resource_version = registry.current_resource_version();
    let current_resource_version_u64 = current_resource_version.parse::<u64>().ok();
    ensure_resource_version_match(
        resource_version_match,
        resource_version_threshold,
        current_resource_version_u64,
    )?;
    let filter =
        parse_object_selector(field_selector.as_deref(), label_selector.as_deref())?.map(Arc::new);

    if watch.unwrap_or(false) {
        if limit.is_some() {
            return Err(ApiError::bad_request(
                "limit cannot be combined with watch=true",
            ));
        }
        if continue_token.is_some() {
            return Err(ApiError::bad_request(
                "continue cannot be combined with watch=true",
            ));
        }
        let configmaps = registry
            .list_since(namespace, resource_version_threshold)
            .await;
        let filter_for_events = filter.as_deref();
        let events: Vec<_> = configmaps
            .into_iter()
            .filter(|config_map| matches_config_map_filter(filter_for_events, config_map))
            .map(|config_map| ConfigMapWatchEvent {
                event_type: "ADDED".to_string(),
                object: config_map,
            })
            .collect();

        let receiver = match namespace {
            Some(ns) => registry.watch_namespace(ns).await,
            None => registry.watch_cluster().await,
        };
        let filter_for_watch: Option<Arc<WatchPredicate<ConfigMap>>> = filter.as_ref().map(|sel| {
            let selector = Arc::clone(sel);
            Arc::new(move |config_map: &ConfigMap| selector.matches_config_map(config_map))
                as Arc<WatchPredicate<ConfigMap>>
        });
        let body = WatchStreamBuilder::new(
            "server_configmaps",
            "ConfigMap watch serialization error",
            events,
            receiver,
        )
        .with_filter(filter_for_watch)
        .with_bookmarks(allow_bookmarks)
        .with_timeout(timeout)
        .into_body();
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .unwrap();
        Ok(response)
    } else {
        if resource_version_match.is_some() && continue_token.is_some() {
            return Err(ApiError::bad_request(
                "continue cannot be combined with resourceVersionMatch",
            ));
        }
        let continue_cursor = continue_token
            .as_deref()
            .map(|token| decode_continue_token(token, "configmaps"))
            .transpose()
            .map_err(|err| ApiError::new(StatusCode::GONE, err.to_string()))?;

        let effective_threshold = match resource_version_match {
            Some(ResourceVersionMatchPolicy::Exact) => {
                resource_version_threshold.map(|rv| rv.saturating_sub(1))
            }
            _ => resource_version_threshold,
        };

        let entries = registry
            .collect_entries(namespace, effective_threshold)
            .await;
        let selector_ref = filter.as_deref();
        let filtered: Vec<_> = entries
            .into_iter()
            .filter(|(_, config_map, _)| matches_config_map_filter(selector_ref, config_map))
            .collect();
        let page =
            paginate_entries(filtered, continue_cursor.as_ref(), limit).map_err(
                |err| match err {
                    PaginationError::InvalidLimit(msg) => ApiError::bad_request(msg),
                    PaginationError::InvalidContinue(msg) => ApiError::new(StatusCode::GONE, msg),
                },
            )?;
        let next_continue = page
            .next_cursor
            .as_ref()
            .map(|cursor| encode_continue_token("configmaps", cursor));
        let remaining_item_count = if page.remaining > 0 {
            Some(page.remaining.min(u32::MAX as usize) as u32)
        } else {
            None
        };
        let mut list = ConfigMapList::new(page.items, current_resource_version.clone());
        list.metadata.continue_token = next_continue;
        list.metadata.remaining_item_count = remaining_item_count;
        Ok(Json(list).into_response())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::configmap::ConfigMap;
    use crate::nanocloud::k8s::pod::ObjectMeta;
    use crate::nanocloud::test_support::keyspace_lock;
    use axum::body::to_bytes;
    use axum::response::IntoResponse;
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::sync::{MutexGuard, OnceLock};
    use tempfile::tempdir;

    const TEST_NAMESPACE: &str = "selector-tests";

    struct EnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvGuard {
        fn set_path(key: &'static str, value: &std::path::Path) -> Self {
            let previous = env::var(key).ok();
            env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(prev) = self.previous.as_ref() {
                env::set_var(self.key, prev);
            } else {
                env::remove_var(self.key);
            }
        }
    }

    struct TestEnv {
        _dir: tempfile::TempDir,
        _lock: MutexGuard<'static, ()>,
        _keyspace: EnvGuard,
        _lock_file: EnvGuard,
    }

    impl TestEnv {
        fn new() -> Self {
            let guard = keyspace_lock().lock();
            let dir = tempdir().expect("tempdir");
            let keyspace = dir.path().join("keyspace");
            let lock_dir = dir.path().join("lock");
            fs::create_dir_all(&keyspace).expect("keyspace dir");
            fs::create_dir_all(&lock_dir).expect("lock dir");
            let lock_file = lock_dir.join("nanocloud.lock");
            fs::File::create(&lock_file).expect("lock file");

            let keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", &keyspace);
            let lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", &lock_file);

            Self {
                _dir: dir,
                _lock: guard,
                _keyspace: keyspace_guard,
                _lock_file: lock_guard,
            }
        }
    }

    fn build_config_map(name: &str, labels: &[(&str, &str)]) -> ConfigMap {
        let mut label_map = HashMap::new();
        for (key, value) in labels {
            label_map.insert((*key).to_string(), (*value).to_string());
        }
        ConfigMap {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(TEST_NAMESPACE.to_string()),
                labels: label_map,
                annotations: HashMap::new(),
                resource_version: None,
            },
            data: HashMap::new(),
            binary_data: HashMap::new(),
            immutable: None,
        }
    }

    async fn upsert_config_map(config_map: ConfigMap) {
        let registry = ConfigMapRegistry::shared();
        let name = config_map
            .metadata
            .name
            .clone()
            .expect("config map must have name");
        match registry.create(TEST_NAMESPACE, config_map.clone()).await {
            Ok(_) => {}
            Err(ConfigMapError::AlreadyExists(_)) => {
                registry
                    .replace(TEST_NAMESPACE, &name, config_map)
                    .await
                    .expect("replace should succeed");
            }
            Err(err) => panic!("failed to create config map: {err:?}"),
        }
    }

    async fn cleanup_config_map(name: &str) {
        let registry = ConfigMapRegistry::shared();
        let _ = registry.delete(TEST_NAMESPACE, name).await;
    }

    async fn clear_namespace() {
        let registry = ConfigMapRegistry::shared();
        let existing = registry.collect_entries(Some(TEST_NAMESPACE), None).await;
        for (_, config_map, _) in existing {
            if let Some(name) = config_map.metadata.name.as_deref() {
                let _ = registry.delete(TEST_NAMESPACE, name).await;
            }
        }
    }

    fn test_guard() -> &'static tokio::sync::Mutex<()> {
        static GUARD: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
        GUARD.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    #[tokio::test]
    async fn get_returns_not_found_for_selector_mismatch() {
        let _env = TestEnv::new();
        let _lock = test_guard().lock().await;
        clear_namespace().await;
        let name = "mismatch";
        let config_map = build_config_map(name, &[("app", "demo")]);
        upsert_config_map(config_map).await;

        let params = WatchParams {
            field_selector: Some("metadata.name=other".to_string()),
            ..Default::default()
        };
        let result = get(
            Path((TEST_NAMESPACE.to_string(), name.to_string())),
            Query(params),
        )
        .await;

        cleanup_config_map(name).await;

        let error = result.expect_err("expected selector mismatch to return error");
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn watch_allows_selector_mismatch() {
        let _env = TestEnv::new();
        let _lock = test_guard().lock().await;
        clear_namespace().await;
        let name = "watch-mismatch";
        let config_map = build_config_map(name, &[("tier", "backend")]);
        upsert_config_map(config_map).await;

        let params = WatchParams {
            watch: Some(true),
            field_selector: Some("metadata.name=other".to_string()),
            ..Default::default()
        };
        let response = get(
            Path((TEST_NAMESPACE.to_string(), name.to_string())),
            Query(params),
        )
        .await
        .expect("watch should accept mismatched selectors");

        cleanup_config_map(name).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .expect("content type"),
            "application/json"
        );
    }

    #[tokio::test]
    async fn list_respects_limit_and_continue() {
        let _env = TestEnv::new();
        let _lock = test_guard().lock().await;
        clear_namespace().await;
        upsert_config_map(build_config_map("cm-one", &[("app", "demo")])).await;
        upsert_config_map(build_config_map("cm-two", &[("app", "demo")])).await;

        let response = handle_list(
            Some(TEST_NAMESPACE),
            WatchParams {
                limit: Some(1),
                ..Default::default()
            },
        )
        .await
        .expect("first page should succeed");
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("bytes");
        let list: ConfigMapList = serde_json::from_slice(&body).expect("json");
        assert_eq!(list.items.len(), 1);
        let token = list
            .metadata
            .continue_token
            .clone()
            .expect("continue token available");

        let response = handle_list(
            Some(TEST_NAMESPACE),
            WatchParams {
                continue_token: Some(token),
                ..Default::default()
            },
        )
        .await
        .expect("second page should succeed");
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("bytes");
        let list: ConfigMapList = serde_json::from_slice(&body).expect("json");
        assert_eq!(list.items.len(), 1);
        assert!(list.metadata.continue_token.is_none());

        cleanup_config_map("cm-one").await;
        cleanup_config_map("cm-two").await;
        clear_namespace().await;
    }

    #[tokio::test]
    async fn list_enforces_resource_version_match_exact() {
        let _env = TestEnv::new();
        let _lock = test_guard().lock().await;
        clear_namespace().await;
        let name = "match-exact";
        upsert_config_map(build_config_map(name, &[("app", "demo")])).await;

        let registry = ConfigMapRegistry::shared();
        let current_rv = registry.current_resource_version();

        let response = handle_list(
            Some(TEST_NAMESPACE),
            WatchParams {
                resource_version: Some(current_rv.clone()),
                resource_version_match: Some(ResourceVersionMatchPolicy::Exact),
                ..Default::default()
            },
        )
        .await
        .expect("exact match should succeed");
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("bytes");
        let list: ConfigMapList = serde_json::from_slice(&body).expect("json");
        assert!(!list.items.is_empty());

        // Bump the registry resourceVersion so the stale request is rejected.
        upsert_config_map(build_config_map("match-exact-b", &[("app", "demo")])).await;

        let err = handle_list(
            Some(TEST_NAMESPACE),
            WatchParams {
                resource_version: Some("1".to_string()),
                resource_version_match: Some(ResourceVersionMatchPolicy::Exact),
                ..Default::default()
            },
        )
        .await
        .expect_err("stale match should fail");
        assert_eq!(err.into_response().status(), StatusCode::GONE);

        cleanup_config_map(name).await;
        cleanup_config_map("match-exact-b").await;
        clear_namespace().await;
    }
}
