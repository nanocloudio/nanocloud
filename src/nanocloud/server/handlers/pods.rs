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

use super::selectors::{ensure_named_resource, matches_pod_filter, parse_object_selector};
use super::watch::{
    ensure_resource_version_match, parse_resource_version, resource_version_is_newer,
    ResourceVersionMatchPolicy, WatchPredicate, WatchStreamBuilder,
};
use crate::nanocloud::k8s::pod::{ContainerStatus, ListMeta, Pod, PodStatus};
use crate::nanocloud::k8s::store::{
    decode_continue_token, encode_continue_token, paginate_entries, PaginationError,
};
use crate::nanocloud::k8s::table::{Table, TableColumnDefinition, TableRow};
use crate::nanocloud::kubelet::Kubelet;
use crate::nanocloud::kubelet::WatchEvent;
use crate::nanocloud::server::handlers::error::ApiError;

use axum::extract::{Path, Query};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
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
    #[serde(default)]
    format: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
    use chrono::Duration as ChronoDuration;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::OnceLock;

    use crate::nanocloud::k8s::pod::{ContainerSpec, ObjectMeta, PodSpec};

    pub(super) fn kubelet_guard() -> &'static tokio::sync::Mutex<()> {
        static GUARD: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
        GUARD.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    pub(super) fn build_pod(
        name: &str,
        namespace: Option<&str>,
        start_offset: ChronoDuration,
        ready: bool,
        restart_count: u32,
        waiting_reason: Option<&str>,
    ) -> Pod {
        let now = Utc::now();
        let start_time = (now - start_offset).to_rfc3339();
        let metadata = ObjectMeta {
            name: Some(name.to_string()),
            namespace: namespace.map(|value| value.to_string()),
            resource_version: Some("10".to_string()),
            ..Default::default()
        };
        let container = ContainerSpec {
            name: "main".to_string(),
            ..Default::default()
        };
        let spec = PodSpec {
            containers: vec![container],
            ..Default::default()
        };
        let mut status = PodStatus {
            phase: Some("Running".to_string()),
            start_time: Some(start_time),
            ..Default::default()
        };
        let mut state_map: Option<HashMap<String, Value>> = None;
        if let Some(reason) = waiting_reason {
            let mut state = HashMap::new();
            state.insert("waiting".to_string(), json!({ "reason": reason }));
            state_map = Some(state);
        }
        status.container_statuses = vec![ContainerStatus {
            name: "main".to_string(),
            restart_count,
            ready,
            state: state_map,
            ..Default::default()
        }];

        Pod {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            metadata,
            spec,
            status: Some(status),
        }
    }

    #[test]
    fn pod_table_row_formats_waiting_reason() {
        let offset = ChronoDuration::seconds(310);
        let pod = build_pod(
            "demo",
            Some("demo-ns"),
            offset,
            false,
            3,
            Some("CrashLoopBackOff"),
        );
        let now = Utc::now();
        let row: TableRow = PodTableRow::from_pod(&pod, now).into();
        let values: Vec<String> = row
            .cells
            .iter()
            .map(|value| value.as_str().unwrap_or("").to_string())
            .collect();
        assert_eq!(values[0], "demo-ns");
        assert_eq!(values[1], "demo");
        assert_eq!(values[2], "0/1");
        assert_eq!(values[3], "CrashLoopBackOff");
        assert_eq!(values[4], "3");
        assert_eq!(values.len(), 6);
        assert!(row.object.is_some());
    }

    #[test]
    fn format_status_prefers_terminated_reason() {
        let mut status = PodStatus::default();
        let mut state = HashMap::new();
        state.insert(
            "terminated".to_string(),
            json!({ "reason": "Error", "exitCode": 137 }),
        );
        status.container_statuses = vec![ContainerStatus {
            name: "main".to_string(),
            state: Some(state),
            ..Default::default()
        }];
        assert_eq!(format_status(Some(&status)), "Error");
    }

    #[test]
    fn humanize_duration_handles_large_ranges() {
        assert_eq!(humanize_duration(chrono::Duration::seconds(45)), "45s");
        assert_eq!(humanize_duration(chrono::Duration::seconds(3600)), "1h");
        assert_eq!(
            humanize_duration(chrono::Duration::seconds(86400 * 3)),
            "3d"
        );
        assert_eq!(
            humanize_duration(chrono::Duration::seconds(86400 * 10)),
            "1w"
        );
    }

    #[test]
    fn table_preserves_list_metadata() {
        let pod = build_pod(
            "meta",
            Some("default"),
            ChronoDuration::seconds(60),
            true,
            0,
            None,
        );
        let metadata = ListMeta {
            resource_version: Some("42".to_string()),
            continue_token: Some("token".to_string()),
            remaining_item_count: Some(7),
        };
        let list = PodList {
            api_version: "v1".to_string(),
            kind: "PodList".to_string(),
            metadata: metadata.clone(),
            items: vec![pod],
        };

        let table = pod_list_to_table(&list);
        assert_eq!(table.metadata.resource_version, metadata.resource_version);
        assert_eq!(table.metadata.continue_token, metadata.continue_token);
        assert_eq!(
            table.metadata.remaining_item_count,
            metadata.remaining_item_count
        );
        assert_eq!(table.rows.len(), 1);
    }

    #[test]
    fn accept_header_requests_table() {
        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT, HeaderValue::from_static(TABLE_CONTENT_TYPE));
        assert_eq!(
            PodListOutput::from_request(None, &headers),
            PodListOutput::Table
        );
    }

    #[test]
    fn query_format_overrides_accept() {
        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT, HeaderValue::from_static(TABLE_CONTENT_TYPE));
        assert_eq!(
            PodListOutput::from_request(Some("json"), &headers),
            PodListOutput::JsonList
        );
    }

    #[tokio::test]
    async fn watch_request_keeps_json_content_type() {
        let _lock = kubelet_guard().lock().await;
        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT, HeaderValue::from_static(TABLE_CONTENT_TYPE));
        let params = WatchParams {
            watch: Some(true),
            format: Some("table".to_string()),
            ..Default::default()
        };
        let kubelet = Kubelet::shared();
        let response = list_pods_impl(None, params, headers, kubelet)
            .await
            .expect("watch request should succeed");
        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap();
        assert_eq!(content_type, "application/json");
    }

    #[tokio::test]
    async fn table_response_sets_projection_content_type() {
        let _lock = kubelet_guard().lock().await;
        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT, HeaderValue::from_static(TABLE_CONTENT_TYPE));
        let kubelet = Kubelet::shared();
        let response = list_pods_impl(None, WatchParams::default(), headers, kubelet)
            .await
            .expect("list request should succeed");
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap()
            .to_string();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let table: Table = serde_json::from_slice(&body).expect("table json");
        assert_eq!(content_type, TABLE_CONTENT_TYPE);
        assert_eq!(table.kind, "Table");
        assert_eq!(table.column_definitions.len(), 6);
    }

    #[tokio::test]
    async fn format_json_query_returns_list_payload() {
        let _lock = kubelet_guard().lock().await;
        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT, HeaderValue::from_static(TABLE_CONTENT_TYPE));
        let params = WatchParams {
            format: Some("json".to_string()),
            ..Default::default()
        };
        let kubelet = Kubelet::shared();
        let response = list_pods_impl(None, params, headers, kubelet)
            .await
            .expect("list request should succeed");
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap()
            .to_string();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let list: PodList = serde_json::from_slice(&body).expect("pod list json");
        assert_eq!(content_type, "application/json");
        assert_eq!(list.kind, "PodList");
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct PodList {
    #[serde(rename = "apiVersion")]
    api_version: String,
    kind: String,
    metadata: ListMeta,
    items: Vec<Pod>,
}

pub const TABLE_CONTENT_TYPE: &str = "application/json;as=Table;g=meta.k8s.io;v=v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PodListOutput {
    JsonList,
    Table,
}

impl PodListOutput {
    fn from_request(format: Option<&str>, headers: &HeaderMap) -> Self {
        if let Some(value) = format {
            if value.eq_ignore_ascii_case("table") {
                return PodListOutput::Table;
            }
            if value.eq_ignore_ascii_case("json") {
                return PodListOutput::JsonList;
            }
        }
        if Self::accepts_table(headers) {
            PodListOutput::Table
        } else {
            PodListOutput::JsonList
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

pub async fn list_pods(
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let kubelet = Kubelet::shared();
    list_pods_impl(Some(namespace.as_str()), params, headers, kubelet).await
}

pub async fn list_pods_all(
    Query(params): Query<WatchParams>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let kubelet = Kubelet::shared();
    list_pods_impl(None, params, headers, kubelet).await
}

pub async fn get_pod(
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<WatchParams>,
) -> Result<Response, ApiError> {
    let kubelet = Kubelet::shared();
    get_pod_impl(Some(namespace.as_str()), &name, params, kubelet).await
}

async fn list_pods_impl(
    namespace: Option<&str>,
    params: WatchParams,
    headers: HeaderMap,
    kubelet: Arc<Kubelet>,
) -> Result<Response, ApiError> {
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
        format,
    } = params;

    let requested_output = PodListOutput::from_request(format.as_deref(), &headers);
    let watch_requested = watch.unwrap_or(false);

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

    if let Some(limit) = limit {
        if limit == 0 {
            return Err(ApiError::bad_request("limit must be greater than 0"));
        }
    }

    let current_resource_version = kubelet.current_resource_version();
    let current_resource_version_u64 = current_resource_version.parse::<u64>().ok();
    ensure_resource_version_match(
        resource_version_match,
        resource_version_threshold,
        current_resource_version_u64,
    )?;

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
        let pods = kubelet
            .list_pods_since(namespace, resource_version_threshold)
            .await
            .map_err(ApiError::internal_error)?;
        let selector_ref = filter.as_deref();
        let events: Vec<_> = pods
            .into_iter()
            .filter(|pod| matches_pod_filter(selector_ref, pod))
            .map(|pod| WatchEvent {
                event_type: "ADDED".to_string(),
                object: pod,
            })
            .collect();
        let receiver = match namespace {
            Some(ns) => kubelet.watch_namespace(Some(ns)).await,
            None => kubelet.watch_cluster().await,
        };
        let filter_for_watch: Option<Arc<WatchPredicate<Pod>>> = filter.as_ref().map(|selector| {
            let selector = Arc::clone(selector);
            Arc::new(move |pod: &Pod| selector.matches_pod(pod)) as Arc<WatchPredicate<Pod>>
        });
        let body = WatchStreamBuilder::new(
            "server_pods",
            "Pod watch serialization error",
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
            .map(|token| decode_continue_token(token, "pods"))
            .transpose()
            .map_err(|err| ApiError::new(StatusCode::GONE, err.to_string()))?;

        let effective_threshold = match resource_version_match {
            Some(ResourceVersionMatchPolicy::Exact) => {
                resource_version_threshold.map(|rv| rv.saturating_sub(1))
            }
            _ => resource_version_threshold,
        };

        let entries = kubelet
            .collect_pod_entries(namespace, effective_threshold)
            .await
            .map_err(ApiError::internal_error)?;

        let selector_ref = filter.as_deref();
        let filtered: Vec<_> = entries
            .into_iter()
            .filter(|(_, pod, _)| matches_pod_filter(selector_ref, pod))
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
            .map(|cursor| encode_continue_token("pods", cursor));
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
        let list = PodList {
            api_version: "v1".to_string(),
            kind: "PodList".to_string(),
            metadata,
            items: page.items,
        };
        match requested_output {
            PodListOutput::JsonList => Ok(Json(list).into_response()),
            PodListOutput::Table => {
                let table = pod_list_to_table(&list);
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

fn pod_list_to_table(list: &PodList) -> Table {
    let now = Utc::now();
    let rows: Vec<TableRow> = list
        .items
        .iter()
        .map(|pod| PodTableRow::from_pod(pod, now).into())
        .collect();
    Table {
        api_version: "meta.k8s.io/v1".to_string(),
        kind: "Table".to_string(),
        metadata: list.metadata.clone(),
        column_definitions: pod_table_columns(),
        rows,
    }
}

fn pod_table_columns() -> Vec<TableColumnDefinition> {
    vec![
        TableColumnDefinition {
            name: "NAMESPACE".to_string(),
            type_name: "string".to_string(),
            format: None,
            description: Some("Pod namespace".to_string()),
            priority: None,
        },
        TableColumnDefinition {
            name: "NAME".to_string(),
            type_name: "string".to_string(),
            format: None,
            description: Some("Pod name".to_string()),
            priority: None,
        },
        TableColumnDefinition {
            name: "READY".to_string(),
            type_name: "string".to_string(),
            format: None,
            description: Some("Ready containers vs total containers".to_string()),
            priority: None,
        },
        TableColumnDefinition {
            name: "STATUS".to_string(),
            type_name: "string".to_string(),
            format: None,
            description: Some("Current pod phase or container state reason".to_string()),
            priority: None,
        },
        TableColumnDefinition {
            name: "RESTARTS".to_string(),
            type_name: "string".to_string(),
            format: None,
            description: Some("Aggregate container restart count".to_string()),
            priority: None,
        },
        TableColumnDefinition {
            name: "AGE".to_string(),
            type_name: "string".to_string(),
            format: None,
            description: Some("Elapsed time since pod creation".to_string()),
            priority: None,
        },
    ]
}

pub struct PodTableRow {
    namespace: String,
    name: String,
    ready: String,
    status: String,
    restarts: String,
    age: String,
    object: Option<Value>,
}

impl PodTableRow {
    pub fn from_pod(pod: &Pod, now: DateTime<Utc>) -> Self {
        let status = pod.status.as_ref();
        PodTableRow {
            namespace: pod
                .metadata
                .namespace
                .clone()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "default".to_string()),
            name: pod.metadata.name.clone().unwrap_or_else(|| "-".to_string()),
            ready: format_ready(pod, status),
            status: format_status(status),
            restarts: format_restarts(status),
            age: format_age(status, now),
            object: serde_json::to_value(pod).ok(),
        }
    }
}

impl From<PodTableRow> for TableRow {
    fn from(row: PodTableRow) -> Self {
        TableRow {
            cells: vec![
                Value::String(row.namespace),
                Value::String(row.name),
                Value::String(row.ready),
                Value::String(row.status),
                Value::String(row.restarts),
                Value::String(row.age),
            ],
            object: row.object,
        }
    }
}

fn format_ready(pod: &Pod, status: Option<&PodStatus>) -> String {
    let total = pod.spec.containers.len();
    let ready = status
        .map(|value| {
            value
                .container_statuses
                .iter()
                .filter(|container| container.ready)
                .count()
        })
        .unwrap_or(0);
    if total == 0 {
        "0/0".to_string()
    } else {
        format!("{ready}/{total}")
    }
}

fn format_restarts(status: Option<&PodStatus>) -> String {
    let restarts: u32 = status
        .map(|value| {
            value
                .container_statuses
                .iter()
                .map(|container| container.restart_count)
                .sum()
        })
        .unwrap_or(0);
    restarts.to_string()
}

fn format_status(status: Option<&PodStatus>) -> String {
    let Some(status) = status else {
        return "Unknown".to_string();
    };
    if let Some(reason) = container_state_reason(&status.container_statuses, "waiting", "Waiting") {
        return reason;
    }
    if let Some(reason) =
        container_state_reason(&status.container_statuses, "terminated", "Terminated")
    {
        return reason;
    }
    status
        .phase
        .as_deref()
        .map(|phase| phase.to_string())
        .unwrap_or_else(|| "Unknown".to_string())
}

fn container_state_reason(
    statuses: &[ContainerStatus],
    key: &str,
    fallback: &str,
) -> Option<String> {
    for container in statuses {
        let Some(state) = container.state.as_ref() else {
            continue;
        };
        let Some(entry) = state.get(key) else {
            continue;
        };
        let Some(object) = entry.as_object() else {
            continue;
        };
        if let Some(reason) = object
            .get("reason")
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
        {
            return Some(reason.to_string());
        }
        if key == "terminated" {
            if let Some(signal) = object.get("signal").and_then(|value| value.as_i64()) {
                return Some(format!("Terminated(Signal:{signal})"));
            }
            if let Some(exit_code) = object.get("exitCode").and_then(|value| value.as_i64()) {
                return Some(format!("Terminated({exit_code})"));
            }
        }
        return Some(fallback.to_string());
    }
    None
}

fn format_age(status: Option<&PodStatus>, now: DateTime<Utc>) -> String {
    status
        .and_then(|value| value.start_time.as_deref())
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|timestamp| {
            let duration = now.signed_duration_since(timestamp.with_timezone(&Utc));
            humanize_duration(duration)
        })
        .unwrap_or_else(|| "-".to_string())
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

async fn get_pod_impl(
    namespace: Option<&str>,
    name: &str,
    params: WatchParams,
    kubelet: Arc<Kubelet>,
) -> Result<Response, ApiError> {
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
        ..
    } = params;
    let watch_requested = watch.unwrap_or(false);
    let resource_version_threshold = parse_resource_version(resource_version.as_deref())?;

    let allow_bookmarks = allow_watch_bookmarks.unwrap_or(false);
    let timeout = timeout_seconds.and_then(|seconds| {
        if seconds == 0 {
            None
        } else {
            Some(Duration::from_secs(seconds))
        }
    });

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

    let pod = kubelet
        .get_pod(namespace, name)
        .await
        .map_err(ApiError::internal_error)?;

    if watch_requested {
        let filter_for_events = filter.as_deref();
        let initial_event = pod
            .into_iter()
            .filter(|pod| resource_version_is_newer(pod, resource_version_threshold))
            .filter(|pod| matches_pod_filter(filter_for_events, pod))
            .map(|pod| WatchEvent {
                event_type: "ADDED".to_string(),
                object: pod,
            })
            .collect();
        let receiver = kubelet.watch_pod(namespace, name).await;
        let filter_for_watch: Option<Arc<WatchPredicate<Pod>>> = filter.as_ref().map(|selector| {
            let selector = Arc::clone(selector);
            Arc::new(move |pod: &Pod| selector.matches_pod(pod)) as Arc<WatchPredicate<Pod>>
        });
        let body = WatchStreamBuilder::new(
            "server_pods",
            "Pod watch serialization error",
            initial_event,
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
        let pod = ensure_named_resource(
            pod,
            filter.as_deref(),
            |pod, selector| selector.matches_pod(pod),
            "pod not found",
        )?;
        Ok(Json(pod).into_response())
    }
}
