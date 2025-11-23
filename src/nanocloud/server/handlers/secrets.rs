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
use super::selectors::{ensure_named_resource, matches_secret_filter, parse_object_selector};
use super::watch::{
    ensure_resource_version_match, parse_resource_version, resource_version_is_newer,
    ResourceVersionMatchPolicy, WatchPredicate, WatchStreamBuilder,
};
use crate::nanocloud::k8s::secret::{Secret, SecretList};
use crate::nanocloud::k8s::secret_manager::{SecretError, SecretRegistry, SecretWatchEvent};
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

fn map_error(err: SecretError) -> ApiError {
    match err {
        SecretError::AlreadyExists(msg) => {
            ApiError::with_reason(StatusCode::CONFLICT, "AlreadyExists", msg)
        }
        SecretError::Conflict(msg) => ApiError::with_reason(StatusCode::CONFLICT, "Conflict", msg),
        SecretError::NotFound(msg) => ApiError::new(StatusCode::NOT_FOUND, msg),
        SecretError::Invalid(msg) => ApiError::bad_request(msg),
        SecretError::Persistence(err) => ApiError::internal_error(err),
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
    let registry = SecretRegistry::shared();
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
        let secret = match existing.clone() {
            Some(secret) => secret,
            None => {
                return Err(ApiError::new(
                    StatusCode::NOT_FOUND,
                    format!("Secret '{}' not found", name),
                ))
            }
        };

        let matches_selector = matches_secret_filter(filter.as_deref(), &secret);
        let include =
            matches_selector && resource_version_is_newer(&secret, resource_version_threshold);
        let events = if include {
            vec![SecretWatchEvent {
                event_type: "ADDED".to_string(),
                object: secret,
            }]
        } else {
            Vec::new()
        };
        let receiver = registry.watch_secret(&namespace, &name).await;
        let filter_for_watch: Option<Arc<WatchPredicate<Secret>>> = filter.as_ref().map(|sel| {
            let selector = Arc::clone(sel);
            Arc::new(move |secret: &Secret| selector.matches_secret(secret))
                as Arc<WatchPredicate<Secret>>
        });
        let body = WatchStreamBuilder::new(
            "server_secrets",
            "Secret watch serialization error",
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
        let secret = ensure_named_resource(
            existing,
            filter.as_deref(),
            |secret, selector| selector.matches_secret(secret),
            format!("Secret '{}' not found", name),
        )?;
        Ok(Json(secret).into_response())
    }
}

pub async fn create(
    Path(namespace): Path<String>,
    Json(payload): Json<Secret>,
) -> Result<(StatusCode, Json<Secret>), ApiError> {
    let registry = SecretRegistry::shared();
    registry
        .create(&namespace, payload)
        .await
        .map(|secret| (StatusCode::CREATED, Json(secret)))
        .map_err(map_error)
}

pub async fn replace(
    Path((namespace, name)): Path<(String, String)>,
    Json(payload): Json<Secret>,
) -> Result<Json<Secret>, ApiError> {
    let registry = SecretRegistry::shared();
    registry
        .replace(&namespace, &name, payload)
        .await
        .map(Json)
        .map_err(map_error)
}

pub async fn delete(
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Secret>, ApiError> {
    let registry = SecretRegistry::shared();
    registry
        .delete(&namespace, &name)
        .await
        .map(Json)
        .map_err(map_error)
}

async fn handle_list(namespace: Option<&str>, params: WatchParams) -> Result<Response, ApiError> {
    let registry = SecretRegistry::shared();
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

    let filter =
        parse_object_selector(field_selector.as_deref(), label_selector.as_deref())?.map(Arc::new);

    let current_resource_version = registry.current_resource_version();
    let current_resource_version_u64 = current_resource_version.parse::<u64>().ok();
    ensure_resource_version_match(
        resource_version_match,
        resource_version_threshold,
        current_resource_version_u64,
    )?;

    if watch.unwrap_or(false) {
        if continue_token.is_some() {
            return Err(ApiError::bad_request(
                "continue cannot be combined with watch=true",
            ));
        }
        if limit.is_some() {
            return Err(ApiError::bad_request(
                "limit cannot be combined with watch=true",
            ));
        }
        let secrets = registry
            .list_since(namespace, resource_version_threshold)
            .await;
        let selector_ref = filter.as_deref();
        let events: Vec<_> = secrets
            .into_iter()
            .filter(|secret| matches_secret_filter(selector_ref, secret))
            .map(|secret| SecretWatchEvent {
                event_type: "ADDED".to_string(),
                object: secret,
            })
            .collect();
        let receiver = match namespace {
            Some(ns) => registry.watch_namespace(ns).await,
            None => registry.watch_cluster().await,
        };
        let filter_for_watch: Option<Arc<WatchPredicate<Secret>>> = filter.as_ref().map(|sel| {
            let selector = Arc::clone(sel);
            Arc::new(move |secret: &Secret| selector.matches_secret(secret))
                as Arc<WatchPredicate<Secret>>
        });
        let body = WatchStreamBuilder::new(
            "server_secrets",
            "Secret watch serialization error",
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
        let continue_cursor = continue_token
            .as_deref()
            .map(|token| decode_continue_token(token, "secrets"))
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
            .filter(|(_, secret, _)| matches_secret_filter(selector_ref, secret))
            .collect();

        let pagination = paginate_entries(filtered, continue_cursor.as_ref(), limit).map_err(
            |err| match err {
                PaginationError::InvalidLimit(msg) => ApiError::bad_request(msg),
                PaginationError::InvalidContinue(msg) => ApiError::new(StatusCode::GONE, msg),
            },
        )?;

        let next_continue = pagination
            .next_cursor
            .as_ref()
            .map(|cursor| encode_continue_token("secrets", cursor));
        let remaining_item_count = if pagination.remaining > 0 {
            Some(pagination.remaining.min(u32::MAX as usize) as u32)
        } else {
            None
        };

        let mut list = SecretList::new(pagination.items, current_resource_version);
        list.metadata.continue_token = next_continue;
        list.metadata.remaining_item_count = remaining_item_count;
        Ok(Json(list).into_response())
    }
}
