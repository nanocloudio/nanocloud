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
use crate::nanocloud::k8s::table::{Table, TableColumnDefinition, TableRow};
use crate::nanocloud::server::handlers::pods::TABLE_CONTENT_TYPE;
use axum::extract::{Path, Query};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use humantime::parse_duration;
use serde::Deserialize;
use serde_json;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EventListOutput {
    JsonList,
    Table,
}

impl EventListOutput {
    fn from_request(format: Option<&str>, headers: &HeaderMap) -> Self {
        if let Some(value) = format {
            if value.eq_ignore_ascii_case("table") {
                return EventListOutput::Table;
            }
            if value.eq_ignore_ascii_case("json") {
                return EventListOutput::JsonList;
            }
        }

        if Self::accepts_table(headers) {
            EventListOutput::Table
        } else {
            EventListOutput::JsonList
        }
    }

    fn accepts_table(headers: &HeaderMap) -> bool {
        headers
            .get(header::ACCEPT)
            .and_then(|value| value.to_str().ok())
            .map(|raw| {
                raw.split(',')
                    .map(|candidate| candidate.trim().to_ascii_lowercase())
                    .any(|candidate| {
                        candidate.starts_with("application/json")
                            && candidate.contains("as=table")
                            && candidate.contains("g=meta.k8s.io")
                            && candidate.contains("v=v1")
                    })
            })
            .unwrap_or(false)
    }
}

#[derive(Default, Deserialize)]
pub struct EventWatchParams {
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
    #[serde(rename = "format", default)]
    format: Option<String>,
    #[serde(default)]
    since: Option<String>,
    #[serde(default)]
    level: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

pub(crate) async fn list_all(
    Query(params): Query<EventWatchParams>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    handle_request(None, params, headers).await
}

pub(crate) async fn list_namespaced(
    Path(namespace): Path<String>,
    Query(params): Query<EventWatchParams>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    handle_request(Some(namespace), params, headers).await
}

pub async fn handle_request(
    namespace: Option<String>,
    params: EventWatchParams,
    headers: HeaderMap,
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
        format,
        since,
        level,
        reason,
    } = params;

    let requested_output = EventListOutput::from_request(format.as_deref(), &headers);
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
        match requested_output {
            EventListOutput::JsonList => Ok(Json(list).into_response()),
            EventListOutput::Table => {
                let table = event_list_to_table(&list);
                let mut response = Json(table).into_response();
                response.headers_mut().insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static(TABLE_CONTENT_TYPE),
                );
                Ok(response)
            }
        }
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

#[derive(Debug)]
struct EventTableRow {
    last_seen: String,
    event_type: String,
    reason: String,
    object: String,
    message: String,
    raw: Option<serde_json::Value>,
}

impl EventTableRow {
    fn from_event(event: &Event, now: DateTime<Utc>) -> Self {
        let last_seen = format_last_seen(event, now);
        let event_type = event.event_type.as_deref().unwrap_or("Normal").to_string();
        let reason = event.reason.as_deref().unwrap_or("-").to_string();
        let namespace = event
            .involved_object
            .namespace
            .as_deref()
            .unwrap_or_else(|| event.metadata.namespace.as_deref().unwrap_or("default"));
        let name = event
            .involved_object
            .name
            .as_deref()
            .unwrap_or_else(|| event.metadata.name.as_deref().unwrap_or("<unknown>"));
        let object = format!("{}/{}", namespace, name);
        let message = event.message.as_deref().unwrap_or("-").to_string();

        Self {
            last_seen,
            event_type,
            reason,
            object,
            message,
            raw: serde_json::to_value(event).ok(),
        }
    }
}

impl From<EventTableRow> for TableRow {
    fn from(row: EventTableRow) -> Self {
        TableRow {
            cells: vec![
                row.last_seen.into(),
                row.event_type.into(),
                row.reason.into(),
                row.object.into(),
                row.message.into(),
            ],
            object: row.raw,
        }
    }
}

fn event_list_to_table(list: &EventList) -> Table {
    let now = Utc::now();
    let rows: Vec<TableRow> = list
        .items
        .iter()
        .map(|event| EventTableRow::from_event(event, now).into())
        .collect();

    Table {
        api_version: "meta.k8s.io/v1".to_string(),
        kind: "Table".to_string(),
        metadata: list.metadata.clone(),
        column_definitions: event_table_columns(),
        rows,
    }
}

fn event_table_columns() -> Vec<TableColumnDefinition> {
    vec![
        TableColumnDefinition {
            name: "LAST SEEN".to_string(),
            type_name: "string".to_string(),
            format: None,
            description: Some("Time since the last observation".to_string()),
            priority: None,
        },
        TableColumnDefinition {
            name: "TYPE".to_string(),
            type_name: "string".to_string(),
            format: None,
            description: Some("Event type".to_string()),
            priority: None,
        },
        TableColumnDefinition {
            name: "REASON".to_string(),
            type_name: "string".to_string(),
            format: None,
            description: Some("Short machine-readable reason".to_string()),
            priority: None,
        },
        TableColumnDefinition {
            name: "OBJECT".to_string(),
            type_name: "string".to_string(),
            format: None,
            description: Some("Involved object".to_string()),
            priority: None,
        },
        TableColumnDefinition {
            name: "MESSAGE".to_string(),
            type_name: "string".to_string(),
            format: None,
            description: Some("Human-readable message".to_string()),
            priority: None,
        },
    ]
}

fn format_last_seen(event: &Event, now: DateTime<Utc>) -> String {
    let timestamp = event
        .last_timestamp
        .as_deref()
        .or(event.event_time.as_deref())
        .or(event.first_timestamp.as_deref())
        .or(event.metadata.creation_timestamp.as_deref());

    let Some(raw) = timestamp else {
        return "-".to_string();
    };
    let parsed = DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| now);
    let duration = now.signed_duration_since(parsed);
    humanize_duration(duration)
}

fn humanize_duration(duration: chrono::Duration) -> String {
    let seconds = duration.num_seconds();
    if seconds <= 0 {
        return "0s".to_string();
    }
    const MINUTE: i64 = 60;
    const HOUR: i64 = 60 * MINUTE;
    const DAY: i64 = 24 * HOUR;
    const WEEK: i64 = 7 * DAY;
    const YEAR: i64 = 365 * DAY;

    if seconds >= YEAR {
        return format!("{}y", seconds / YEAR);
    }
    if seconds >= WEEK {
        return format!("{}w", seconds / WEEK);
    }
    if seconds >= DAY {
        return format!("{}d", seconds / DAY);
    }
    if seconds >= HOUR {
        return format!("{}h", seconds / HOUR);
    }
    if seconds >= MINUTE {
        return format!("{}m", seconds / MINUTE);
    }
    format!("{}s", seconds)
}
