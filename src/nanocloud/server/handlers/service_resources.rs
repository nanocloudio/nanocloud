use super::error::ApiError;
use super::selectors::{ensure_named_resource, matches_service_filter, parse_object_selector};
use super::watch::{
    ensure_resource_version_match, parse_resource_version, resource_version_is_newer,
    ResourceVersionMatchPolicy, WatchPredicate, WatchStreamBuilder,
};
use crate::nanocloud::k8s::pod::ListMeta;
use crate::nanocloud::k8s::service::{Service, ServiceList};
use crate::nanocloud::k8s::service_registry::{ServiceError, ServiceRegistry, ServiceWatchEvent};
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

fn map_error(err: ServiceError) -> ApiError {
    match err {
        ServiceError::AlreadyExists(msg) => {
            ApiError::with_reason(StatusCode::CONFLICT, "AlreadyExists", msg)
        }
        ServiceError::NotFound(msg) => ApiError::new(StatusCode::NOT_FOUND, msg),
        ServiceError::Invalid(msg) => ApiError::bad_request(msg),
        ServiceError::Persistence(err) => ApiError::internal_error(err),
    }
}

pub async fn list_all(Query(params): Query<WatchParams>) -> Result<Response, ApiError> {
    handle_list(None, params).await
}

pub async fn list_namespaced(
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response, ApiError> {
    handle_list(Some(namespace.as_str()), params).await
}

pub async fn get(
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<WatchParams>,
) -> Result<Response, ApiError> {
    let registry = ServiceRegistry::shared();
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
    let existing = registry.get(&namespace, &name);

    if watch.unwrap_or(false) {
        let service = match existing.clone() {
            Some(svc) => svc,
            None => {
                return Err(ApiError::new(
                    StatusCode::NOT_FOUND,
                    format!("Service '{name}' not found"),
                ))
            }
        };

        let include = matches_service_filter(filter.as_deref(), &service)
            && resource_version_is_newer(&service, resource_version_threshold);
        let events = if include {
            vec![ServiceWatchEvent {
                event_type: "ADDED".to_string(),
                object: service,
            }]
        } else {
            Vec::new()
        };
        let receiver = registry.watch_service(&namespace, &name);
        let filter_for_watch: Option<Arc<WatchPredicate<Service>>> = filter.as_ref().map(|sel| {
            let selector = Arc::clone(sel);
            Arc::new(move |service: &Service| selector.matches_service(service))
                as Arc<WatchPredicate<Service>>
        });
        let body = WatchStreamBuilder::new(
            "server_services",
            "Service watch serialization error",
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
        let service = ensure_named_resource(
            existing,
            filter.as_deref(),
            |svc, selector| selector.matches_service(svc),
            format!("Service '{name}' not found"),
        )?;
        Ok(Json(service).into_response())
    }
}

pub async fn create(
    Path(namespace): Path<String>,
    Json(payload): Json<Service>,
) -> Result<(StatusCode, Json<Service>), ApiError> {
    let registry = ServiceRegistry::shared();
    registry
        .create(&namespace, payload)
        .map(|svc| (StatusCode::CREATED, Json(svc)))
        .map_err(map_error)
}

pub async fn delete(
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Service>, ApiError> {
    let registry = ServiceRegistry::shared();
    registry
        .delete(&namespace, &name)
        .map(Json)
        .map_err(map_error)
}

async fn handle_list(namespace: Option<&str>, params: WatchParams) -> Result<Response, ApiError> {
    let registry = ServiceRegistry::shared();
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

        let services = registry.list_since(namespace, resource_version_threshold);
        let filter_for_events = filter.as_deref();
        let events: Vec<_> = services
            .into_iter()
            .filter(|service| matches_service_filter(filter_for_events, service))
            .map(|service| ServiceWatchEvent {
                event_type: "ADDED".to_string(),
                object: service,
            })
            .collect();

        let receiver = match namespace {
            Some(ns) => registry.watch_namespace(ns),
            None => registry.watch_cluster(),
        };
        let filter_for_watch: Option<Arc<WatchPredicate<Service>>> = filter.as_ref().map(|sel| {
            let selector = Arc::clone(sel);
            Arc::new(move |service: &Service| selector.matches_service(service))
                as Arc<WatchPredicate<Service>>
        });
        let body = WatchStreamBuilder::new(
            "server_services",
            "Service watch serialization error",
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
            .map(|token| decode_continue_token(token, "services"))
            .transpose()
            .map_err(|err| ApiError::new(StatusCode::GONE, err.to_string()))?;

        let effective_threshold = match resource_version_match {
            Some(ResourceVersionMatchPolicy::Exact) => {
                resource_version_threshold.map(|rv| rv.saturating_sub(1))
            }
            _ => resource_version_threshold,
        };

        let entries = registry.collect_entries(namespace, effective_threshold);
        let selector_ref = filter.as_deref();
        let filtered: Vec<_> = entries
            .into_iter()
            .filter(|(_, service, _)| matches_service_filter(selector_ref, service))
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
            .map(|cursor| encode_continue_token("services", cursor));
        let remaining_item_count = if page.remaining > 0 {
            Some(page.remaining.min(u32::MAX as usize) as u32)
        } else {
            None
        };
        let list = ServiceList {
            metadata: ListMeta {
                resource_version: Some(current_resource_version),
                continue_token: next_continue,
                remaining_item_count,
            },
            items: page.items,
            ..Default::default()
        };
        Ok(Json(list).into_response())
    }
}
