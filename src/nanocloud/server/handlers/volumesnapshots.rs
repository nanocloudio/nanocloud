use super::error::ApiError;
use super::selectors::{matches_metadata_filter, parse_object_selector};
use super::watch::{parse_resource_version, ResourceVersionMatchPolicy};
use crate::nanocloud::api::types::{VolumeSnapshot, VolumeSnapshotPhase};
use crate::nanocloud::k8s::pod::ListMeta;
use crate::nanocloud::k8s::store::{
    decode_continue_token, delete_volume_snapshot, encode_continue_token, list_volume_snapshots,
    paginate_entries, save_volume_snapshot, PaginationError,
};
use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use chrono::Utc;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Default, Deserialize)]
pub struct ListParams {
    #[serde(default)]
    watch: Option<bool>,
    #[serde(rename = "fieldSelector")]
    field_selector: Option<String>,
    #[serde(rename = "labelSelector")]
    label_selector: Option<String>,
    #[serde(rename = "resourceVersion")]
    resource_version: Option<String>,
    #[serde(rename = "resourceVersionMatch")]
    resource_version_match: Option<ResourceVersionMatchPolicy>,
    #[serde(rename = "limit")]
    limit: Option<u32>,
    #[serde(rename = "continue")]
    continue_token: Option<String>,
}

pub async fn list_all(Query(params): Query<ListParams>) -> Result<impl IntoResponse, ApiError> {
    handle_list(None, params).await
}

pub async fn list_namespaced(
    Path(namespace): Path<String>,
    Query(params): Query<ListParams>,
) -> Result<impl IntoResponse, ApiError> {
    handle_list(Some(namespace.as_str()), params).await
}

pub async fn get(
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<ListParams>,
) -> Result<impl IntoResponse, ApiError> {
    let ListParams { watch, .. } = params;
    if watch.unwrap_or(false) {
        return Err(ApiError::bad_request(
            "watch is not supported for volumesnapshots",
        ));
    }

    let snapshots =
        list_volume_snapshots(Some(namespace.as_str())).map_err(ApiError::internal_error)?;
    let snapshot = snapshots
        .into_iter()
        .find(|snap| snap.metadata.name.as_deref() == Some(name.as_str()))
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "VolumeSnapshot not found"))?;
    Ok(Json(snapshot))
}

pub async fn create(
    Path(namespace): Path<String>,
    Json(mut snapshot): Json<VolumeSnapshot>,
) -> Result<(StatusCode, Json<VolumeSnapshot>), ApiError> {
    normalize_snapshot(&mut snapshot, Some(namespace.as_str()))?;
    snapshot.metadata.resource_version = Some(next_resource_version());
    if snapshot.status.is_none() {
        snapshot.status = Some(Default::default());
    }
    if let Some(status) = snapshot.status.as_mut() {
        status.phase = Some(VolumeSnapshotPhase::Pending);
    }

    save_volume_snapshot(
        snapshot.metadata.namespace.as_deref(),
        snapshot.metadata.name.as_deref().unwrap_or("snapshot"),
        &snapshot,
    )
    .map_err(ApiError::internal_error)?;
    Ok((StatusCode::CREATED, Json(snapshot)))
}

pub async fn delete(
    Path((namespace, name)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    delete_volume_snapshot(Some(namespace.as_str()), &name).map_err(ApiError::internal_error)?;
    Ok(StatusCode::NO_CONTENT)
}

fn normalize_snapshot(
    snapshot: &mut VolumeSnapshot,
    namespace: Option<&str>,
) -> Result<(), ApiError> {
    if snapshot.api_version.trim().is_empty() {
        snapshot.api_version = "nanocloud.io/v1".to_string();
    }
    if snapshot.kind.trim().is_empty() {
        snapshot.kind = "VolumeSnapshot".to_string();
    }
    let ns = snapshot
        .metadata
        .namespace
        .clone()
        .or_else(|| namespace.map(|ns| ns.to_string()))
        .unwrap_or_else(|| "default".to_string());
    snapshot.metadata.namespace = Some(ns.clone());
    if snapshot
        .metadata
        .name
        .as_deref()
        .map(str::is_empty)
        .unwrap_or(true)
    {
        return Err(ApiError::bad_request("metadata.name is required"));
    }
    let name_value = snapshot
        .metadata
        .name
        .clone()
        .unwrap_or_else(|| "snapshot".to_string());
    snapshot
        .metadata
        .ensure_common_fields(Some(&ns), Some(&name_value));
    Ok(())
}

async fn handle_list(
    namespace: Option<&str>,
    params: ListParams,
) -> Result<impl IntoResponse, ApiError> {
    let ListParams {
        watch,
        field_selector,
        label_selector,
        resource_version,
        resource_version_match,
        limit,
        continue_token,
    } = params;

    if watch.unwrap_or(false) {
        return Err(ApiError::bad_request(
            "watch is not supported for volumesnapshots",
        ));
    }
    if let Some(limit) = limit {
        if limit == 0 {
            return Err(ApiError::bad_request("limit must be greater than 0"));
        }
    }
    if resource_version_match.is_some() && continue_token.is_some() {
        return Err(ApiError::bad_request(
            "continue cannot be combined with resourceVersionMatch",
        ));
    }

    let resource_version_threshold = parse_resource_version(resource_version.as_deref())?;
    let continue_cursor = continue_token
        .as_deref()
        .map(|token| decode_continue_token(token, "volumesnapshots"))
        .transpose()
        .map_err(|err| ApiError::new(StatusCode::GONE, err.to_string()))?;
    let filter =
        parse_object_selector(field_selector.as_deref(), label_selector.as_deref())?.map(Arc::new);

    let snapshots = list_volume_snapshots(namespace).map_err(ApiError::internal_error)?;
    let selector_ref = filter.as_deref();
    let effective_threshold = match resource_version_match {
        Some(ResourceVersionMatchPolicy::Exact) => {
            resource_version_threshold.map(|rv| rv.saturating_sub(1))
        }
        _ => resource_version_threshold,
    };
    let entries: Vec<(String, VolumeSnapshot, Option<String>)> = snapshots
        .into_iter()
        .filter(|snap| {
            matches_metadata_filter(selector_ref, &snap.metadata)
                && snap
                    .metadata
                    .resource_version
                    .as_deref()
                    .and_then(|rv| rv.parse::<u64>().ok())
                    .map(|rv| effective_threshold.map(|th| rv > th).unwrap_or(true))
                    .unwrap_or(true)
        })
        .map(|snap| {
            let key = format!(
                "{}/{}",
                snap.metadata.namespace.as_deref().unwrap_or("default"),
                snap.metadata.name.as_deref().unwrap_or("snapshot"),
            );
            let rv = snap.metadata.resource_version.clone();
            (key, snap, rv)
        })
        .collect();

    let page =
        paginate_entries(entries, continue_cursor.as_ref(), limit).map_err(|err| match err {
            PaginationError::InvalidLimit(msg) => ApiError::bad_request(msg),
            PaginationError::InvalidContinue(msg) => ApiError::new(StatusCode::GONE, msg),
        })?;
    let next_continue = page
        .next_cursor
        .as_ref()
        .map(|cursor| encode_continue_token("volumesnapshots", cursor));
    let remaining_item_count = if page.remaining > 0 {
        Some(page.remaining.min(u32::MAX as usize) as u32)
    } else {
        None
    };
    let current_resource_version = page
        .items
        .iter()
        .filter_map(|snap| {
            snap.metadata
                .resource_version
                .as_deref()
                .and_then(|rv| rv.parse::<u64>().ok())
        })
        .max()
        .unwrap_or(1)
        .to_string();

    let list = crate::nanocloud::api::types::VolumeSnapshotList {
        metadata: ListMeta {
            resource_version: Some(current_resource_version),
            continue_token: next_continue,
            remaining_item_count,
        },
        items: page.items,
        ..Default::default()
    };

    Ok(Json(list))
}

fn next_resource_version() -> String {
    let millis = Utc::now().timestamp_millis();
    if millis > 0 {
        millis.to_string()
    } else {
        "1".to_string()
    }
}
