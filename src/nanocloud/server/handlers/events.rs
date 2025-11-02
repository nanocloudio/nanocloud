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
use super::watch::{
    ensure_resource_version_match, parse_resource_version, ResourceVersionMatchPolicy,
    WatchPredicate, WatchStreamBuilder,
};
use crate::nanocloud::k8s::event::{Event, EventList, EventRegistry, EventWatchEvent};
use crate::nanocloud::k8s::pod::ListMeta;
use crate::nanocloud::k8s::store::{
    decode_continue_token, encode_continue_token, paginate_entries, PaginationError,
};
use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use humantime::parse_duration;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

#[derive(Clone, Debug, Default)]
struct EventFilter {
    metadata_name: Option<String>,
    metadata_namespace: Option<String>,
    event_type: Option<String>,
    reason: Option<String>,
    involved_name: Option<String>,
    involved_namespace: Option<String>,
    involved_kind: Option<String>,
}

impl EventFilter {
    fn matches(&self, event: &Event) -> bool {
        if let Some(expected) = &self.metadata_name {
            if event.metadata.name.as_deref() != Some(expected.as_str()) {
                return false;
            }
        }

        if let Some(expected) = &self.metadata_namespace {
            let namespace = event.metadata.namespace.as_deref().unwrap_or("default");
            if namespace != expected.as_str() {
                return false;
            }
        }

        if let Some(expected) = &self.event_type {
            if event.event_type.as_deref() != Some(expected.as_str()) {
                return false;
            }
        }

        if let Some(expected) = &self.reason {
            if event.reason.as_deref() != Some(expected.as_str()) {
                return false;
            }
        }

        if let Some(expected) = &self.involved_name {
            if event.involved_object.name.as_deref() != Some(expected.as_str()) {
                return false;
            }
        }

        if let Some(expected) = &self.involved_namespace {
            let namespace = event
                .involved_object
                .namespace
                .as_deref()
                .unwrap_or("default");
            if namespace != expected.as_str() {
                return false;
            }
        }

        if let Some(expected) = &self.involved_kind {
            if event.involved_object.kind.as_deref() != Some(expected.as_str()) {
                return false;
            }
        }

        true
    }

    fn is_empty(&self) -> bool {
        self.metadata_name.is_none()
            && self.metadata_namespace.is_none()
            && self.event_type.is_none()
            && self.reason.is_none()
            && self.involved_name.is_none()
            && self.involved_namespace.is_none()
            && self.involved_kind.is_none()
    }
}

#[derive(Default, Deserialize)]
pub(crate) struct EventWatchParams {
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
    #[serde(default)]
    _format: Option<String>,
    #[serde(default)]
    since: Option<String>,
    #[serde(default)]
    level: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

pub(crate) async fn list_all(Query(params): Query<EventWatchParams>) -> Result<Response, ApiError> {
    handle_request(None, params).await
}

pub(crate) async fn list_namespaced(
    Path(namespace): Path<String>,
    Query(params): Query<EventWatchParams>,
) -> Result<Response, ApiError> {
    handle_request(Some(namespace), params).await
}

async fn handle_request(
    namespace: Option<String>,
    params: EventWatchParams,
) -> Result<Response, ApiError> {
    let registry = EventRegistry::shared();
    let EventWatchParams {
        watch,
        resource_version,
        field_selector,
        label_selector,
        timeout_seconds,
        allow_watch_bookmarks,
        limit,
        continue_token,
        resource_version_match,
        _format: _,
        since,
        level,
        reason,
    } = params;

    if label_selector.is_some() {
        return Err(ApiError::bad_request(
            "labelSelector is not supported for events",
        ));
    }

    let namespace_ref = namespace.as_deref();
    let filter = parse_event_field_selector(field_selector.as_deref())?;
    let resource_version_threshold = parse_resource_version(resource_version.as_deref())?;
    let allow_bookmarks = allow_watch_bookmarks.unwrap_or(false);
    let timeout = timeout_seconds.and_then(|seconds| {
        if seconds == 0 {
            None
        } else {
            Some(Duration::from_secs(seconds))
        }
    });

    let current_resource_version = registry.current_resource_version();
    let current_rv_u64 = current_resource_version.parse::<u64>().ok();
    ensure_resource_version_match(
        resource_version_match,
        resource_version_threshold,
        current_rv_u64,
    )?;

    let since_cutoff = since.as_deref().map(parse_since_param).transpose()?;
    let level_filter = level.as_deref().map(parse_level_filter).transpose()?;
    let reason_filters = reason.as_deref().map(parse_reason_filters).transpose()?;

    let watch_requested = watch.unwrap_or(false);
    if watch_requested {
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

        let events = registry
            .list_since(namespace_ref, resource_version_threshold)
            .await;
        let filtered: Vec<EventWatchEvent> = events
            .into_iter()
            .filter(|event| {
                event_matches(
                    event,
                    filter.as_ref(),
                    since_cutoff.as_ref(),
                    level_filter.as_deref(),
                    reason_filters.as_deref(),
                )
            })
            .map(|event| EventWatchEvent {
                event_type: "ADDED".to_string(),
                object: event,
            })
            .collect();

        let receiver = match namespace_ref {
            Some(ns) => registry.watch_namespace(ns).await,
            None => registry.watch_cluster().await,
        };

        let filter_for_watch = build_watch_predicate(
            filter.clone(),
            since_cutoff,
            level_filter.clone(),
            reason_filters.clone(),
        );

        let body = WatchStreamBuilder::new(
            "server_events",
            "Event watch serialization error",
            filtered,
            receiver,
        )
        .with_filter(filter_for_watch)
        .with_bookmarks(allow_bookmarks)
        .with_timeout(timeout)
        .into_body();

        let response = Response::builder()
            .status(StatusCode::OK)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
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
            .map(|token| decode_continue_token(token, "events"))
            .transpose()
            .map_err(|err| match err {
                PaginationError::InvalidContinue(msg) => {
                    ApiError::new(StatusCode::GONE, msg.to_string())
                }
                PaginationError::InvalidLimit(msg) => ApiError::bad_request(msg),
            })?;

        let effective_threshold = match resource_version_match {
            Some(ResourceVersionMatchPolicy::Exact) => {
                resource_version_threshold.map(|rv| rv.saturating_sub(1))
            }
            _ => resource_version_threshold,
        };

        let mut entries = registry
            .collect_entries(namespace_ref, effective_threshold)
            .await;
        if filter.is_some()
            || since_cutoff.is_some()
            || level_filter.is_some()
            || reason_filters.is_some()
        {
            entries.retain(|(_, event, _)| {
                event_matches(
                    event,
                    filter.as_ref(),
                    since_cutoff.as_ref(),
                    level_filter.as_deref(),
                    reason_filters.as_deref(),
                )
            });
        }

        let page =
            paginate_entries(entries, continue_cursor.as_ref(), limit).map_err(
                |err| match err {
                    PaginationError::InvalidLimit(msg) => ApiError::bad_request(msg),
                    PaginationError::InvalidContinue(msg) => {
                        ApiError::new(StatusCode::GONE, msg.to_string())
                    }
                },
            )?;

        let next_continue = page
            .next_cursor
            .as_ref()
            .map(|cursor| encode_continue_token("events", cursor));
        let remaining_item_count = if page.remaining > 0 {
            Some(page.remaining.min(u32::MAX as usize) as u32)
        } else {
            None
        };

        let metadata = ListMeta {
            resource_version: Some(current_resource_version),
            continue_token: next_continue,
            remaining_item_count,
        };

        let list = EventList::new(page.items, metadata);
        Ok(Json(list).into_response())
    }
}

fn parse_event_field_selector(
    field_selector: Option<&str>,
) -> Result<Option<EventFilter>, ApiError> {
    let Some(raw) = field_selector else {
        return Ok(None);
    };

    let mut filter = EventFilter::default();
    for expr in split_selector_terms(raw) {
        if expr.is_empty() {
            continue;
        }
        let (left, right) = expr.split_once('=').ok_or_else(|| {
            ApiError::bad_request("Unsupported fieldSelector expression; expected key=value")
        })?;
        let key = left.trim();
        if key.is_empty() {
            return Err(ApiError::bad_request(
                "Unsupported fieldSelector expression; missing key",
            ));
        }
        let value = normalize_value(right);
        match key {
            "metadata.name" => filter.metadata_name = Some(value),
            "metadata.namespace" => filter.metadata_namespace = Some(value),
            "type" => filter.event_type = Some(value),
            "reason" => filter.reason = Some(value),
            "involvedObject.name" => filter.involved_name = Some(value),
            "involvedObject.namespace" => filter.involved_namespace = Some(value),
            "involvedObject.kind" => filter.involved_kind = Some(value),
            unsupported => {
                return Err(ApiError::bad_request(format!(
                    "Unsupported fieldSelector key '{}'",
                    unsupported
                )))
            }
        }
    }

    if filter.is_empty() {
        Ok(None)
    } else {
        Ok(Some(filter))
    }
}

fn split_selector_terms(raw: &str) -> impl Iterator<Item = &str> {
    raw.split(',')
        .map(|term| term.trim())
        .filter(|term| !term.is_empty())
}

fn normalize_value(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    }
}

fn event_matches(
    event: &Event,
    selector: Option<&EventFilter>,
    since: Option<&SystemTime>,
    level: Option<&str>,
    reasons: Option<&[String]>,
) -> bool {
    selector.map(|f| f.matches(event)).unwrap_or(true)
        && matches_since(event, since)
        && matches_level(event, level)
        && matches_reason(event, reasons)
}

fn parse_level_filter(raw: &str) -> Result<String, ApiError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "normal" => Ok("Normal".to_string()),
        "warning" => Ok("Warning".to_string()),
        other => Err(ApiError::bad_request(format!(
            "Unsupported level '{}'; expected Normal or Warning",
            other
        ))),
    }
}

fn parse_reason_filters(raw: &str) -> Result<Vec<String>, ApiError> {
    let values: Vec<String> = raw
        .split(',')
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .collect();
    if values.is_empty() {
        Err(ApiError::bad_request(
            "reason query parameter must include at least one value",
        ))
    } else {
        Ok(values)
    }
}

fn parse_since_param(raw: &str) -> Result<SystemTime, ApiError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(ApiError::bad_request("since must not be empty"));
    }

    if let Ok(timestamp) = DateTime::parse_from_rfc3339(trimmed) {
        return Ok(timestamp.with_timezone(&Utc).into());
    }

    match parse_duration(trimmed) {
        Ok(duration) => SystemTime::now()
            .checked_sub(duration)
            .ok_or_else(|| ApiError::bad_request("since duration exceeds UNIX_EPOCH")),
        Err(_) => Err(ApiError::bad_request(
            "since must be an RFC3339 timestamp or a duration such as 30m, 6h, 2d",
        )),
    }
}

fn matches_since(event: &Event, cutoff: Option<&SystemTime>) -> bool {
    let Some(target) = cutoff else {
        return true;
    };

    match event_timestamp(event) {
        Some(timestamp) => timestamp >= *target,
        None => true,
    }
}

fn matches_level(event: &Event, allowed: Option<&str>) -> bool {
    let Some(expected) = allowed else {
        return true;
    };
    let observed = event.event_type.as_deref().unwrap_or("Normal");
    observed.eq_ignore_ascii_case(expected)
}

fn matches_reason(event: &Event, allowed: Option<&[String]>) -> bool {
    let Some(list) = allowed else {
        return true;
    };
    let Some(reason) = event.reason.as_deref() else {
        return false;
    };
    list.iter()
        .any(|value| reason.eq_ignore_ascii_case(value.as_str()))
}

fn event_timestamp(event: &Event) -> Option<SystemTime> {
    for value in [
        event.event_time.as_deref(),
        event.last_timestamp.as_deref(),
        event.first_timestamp.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
            return Some(parsed.with_timezone(&Utc).into());
        }
    }
    None
}

fn build_watch_predicate(
    filter: Option<EventFilter>,
    since_cutoff: Option<SystemTime>,
    level_filter: Option<String>,
    reason_filters: Option<Vec<String>>,
) -> Option<Arc<WatchPredicate<Event>>> {
    if filter.is_none()
        && since_cutoff.is_none()
        && level_filter.is_none()
        && reason_filters.is_none()
    {
        return None;
    }

    let selector = filter.clone();
    let since_clone = since_cutoff;
    let level_clone = level_filter.clone();
    let reasons_clone = reason_filters.clone();
    Some(Arc::new(move |event: &Event| {
        event_matches(
            event,
            selector.as_ref(),
            since_clone.as_ref(),
            level_clone.as_deref(),
            reasons_clone.as_deref(),
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::event::{EventSource, ObjectReference};
    use crate::nanocloud::k8s::pod::ObjectMeta;
    use chrono::{Duration as ChronoDuration, Utc};
    use futures_util::StreamExt;
    use std::sync::OnceLock;

    fn registry_guard() -> &'static tokio::sync::Mutex<()> {
        static GUARD: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
        GUARD.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    fn sample_event(namespace: &str, name: &str, reason: &str, event_type: &str) -> Event {
        let timestamp = Utc::now().to_rfc3339();
        Event {
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                ..Default::default()
            },
            involved_object: ObjectReference {
                api_version: Some("nanocloud.io/v1".to_string()),
                kind: Some("Bundle".to_string()),
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                uid: Some(format!("bundle:{}/{}", namespace, name)),
                resource_version: None,
                field_path: None,
            },
            reason: Some(reason.to_string()),
            message: Some(format!("Event for {}", name)),
            event_type: Some(event_type.to_string()),
            first_timestamp: Some(timestamp.clone()),
            last_timestamp: Some(timestamp.clone()),
            event_time: Some(timestamp.clone()),
            count: Some(1),
            reporting_component: Some("tests".to_string()),
            reporting_instance: Some("tests".to_string()),
            action: Some("Reconcile".to_string()),
            related: None,
            series: None,
            source: Some(EventSource {
                component: Some("tests".to_string()),
                host: None,
            }),
            deprecated_source: None,
            deprecated_first_timestamp: None,
            deprecated_last_timestamp: None,
            deprecated_count: None,
        }
    }

    fn set_event_timestamp(event: &mut Event, timestamp: &str) {
        event.event_time = Some(timestamp.to_string());
        event.first_timestamp = Some(timestamp.to_string());
        event.last_timestamp = Some(timestamp.to_string());
    }

    #[tokio::test]
    async fn list_returns_events() {
        let _guard = registry_guard().lock().await;
        let registry = EventRegistry::shared();
        registry.clear().await;
        registry
            .record(sample_event(
                "default",
                "bundle-a",
                "BundleReconciled",
                "Normal",
            ))
            .await;

        let response = handle_request(None, EventWatchParams::default())
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let list: EventList = serde_json::from_slice(&body).expect("list json");
        assert_eq!(list.items.len(), 1);
        let event = &list.items[0];
        assert_eq!(event.metadata.name.as_deref(), Some("event-1"));
        assert_eq!(event.reason.as_deref(), Some("BundleReconciled"));
    }

    #[tokio::test]
    async fn watch_stream_includes_initial_events() {
        let _guard = registry_guard().lock().await;
        let registry = EventRegistry::shared();
        registry.clear().await;
        registry
            .record(sample_event(
                "default",
                "bundle-b",
                "BundleReconciled",
                "Normal",
            ))
            .await;

        let params: EventWatchParams =
            serde_urlencoded::from_str("watch=true").expect("watch params");
        let response = handle_request(None, params).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let mut stream = response.into_body().into_data_stream();
        let first_chunk = stream
            .next()
            .await
            .expect("first chunk")
            .expect("chunk result");
        let text = String::from_utf8(first_chunk.to_vec()).expect("utf8");
        assert!(text.contains("\"type\":\"ADDED\""));
        assert!(text.contains("\"BundleReconciled\""));
    }

    #[tokio::test]
    async fn field_selector_filters_events() {
        let _guard = registry_guard().lock().await;
        let registry = EventRegistry::shared();
        registry.clear().await;
        registry
            .record(sample_event(
                "default",
                "bundle-a",
                "BundleReconciled",
                "Normal",
            ))
            .await;
        registry
            .record(sample_event(
                "default",
                "bundle-b",
                "BundleReconcileFailed",
                "Warning",
            ))
            .await;

        let params: EventWatchParams =
            serde_urlencoded::from_str("fieldSelector=reason%3DBundleReconciled")
                .expect("field selector params");
        let response = handle_request(None, params).await.expect("response");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let list: EventList = serde_json::from_slice(&body).expect("list json");
        assert_eq!(list.items.len(), 1);
        let event = &list.items[0];
        assert_eq!(event.involved_object.name.as_deref(), Some("bundle-a"));
    }

    #[tokio::test]
    async fn since_query_filters_old_events() {
        let _guard = registry_guard().lock().await;
        let registry = EventRegistry::shared();
        registry.clear().await;

        let now = Utc::now();
        let fresh_ts = now.to_rfc3339();
        let old_ts = (now - ChronoDuration::minutes(30)).to_rfc3339();

        let mut old_event = sample_event("default", "bundle-old", "BundleReconciled", "Normal");
        set_event_timestamp(&mut old_event, &old_ts);
        registry.record(old_event).await;

        let mut fresh_event = sample_event("default", "bundle-new", "BundleReconciled", "Normal");
        set_event_timestamp(&mut fresh_event, &fresh_ts);
        registry.record(fresh_event).await;

        let params: EventWatchParams = serde_urlencoded::from_str("since=5m").unwrap();
        let response = handle_request(None, params).await.expect("response");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let list: EventList = serde_json::from_slice(&body).expect("list json");
        assert_eq!(list.items.len(), 1);
        assert_eq!(
            list.items[0].involved_object.name.as_deref(),
            Some("bundle-new")
        );
    }

    #[tokio::test]
    async fn level_query_filters_events() {
        let _guard = registry_guard().lock().await;
        let registry = EventRegistry::shared();
        registry.clear().await;
        registry
            .record(sample_event(
                "default",
                "bundle-a",
                "BundleReconciled",
                "Normal",
            ))
            .await;
        registry
            .record(sample_event(
                "default",
                "bundle-b",
                "BundleReconcileFailed",
                "Warning",
            ))
            .await;

        let params: EventWatchParams =
            serde_urlencoded::from_str("level=Warning").expect("level params");
        let response = handle_request(None, params).await.expect("response");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let list: EventList = serde_json::from_slice(&body).expect("list json");
        assert_eq!(list.items.len(), 1);
        assert_eq!(
            list.items[0].involved_object.name.as_deref(),
            Some("bundle-b")
        );
    }

    #[tokio::test]
    async fn reason_query_supports_multiple_values() {
        let _guard = registry_guard().lock().await;
        let registry = EventRegistry::shared();
        registry.clear().await;
        registry
            .record(sample_event(
                "default",
                "bundle-a",
                "BundleReconciled",
                "Normal",
            ))
            .await;
        registry
            .record(sample_event(
                "default",
                "bundle-b",
                "SecurityPolicyViolation",
                "Warning",
            ))
            .await;

        let params: EventWatchParams =
            serde_urlencoded::from_str("reason=BundleReconciled,PrivilegeEscalationDenied")
                .expect("reason params");
        let response = handle_request(None, params).await.expect("response");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let list: EventList = serde_json::from_slice(&body).expect("list json");
        assert_eq!(list.items.len(), 1);
        assert_eq!(
            list.items[0].involved_object.name.as_deref(),
            Some("bundle-a")
        );
    }
}
