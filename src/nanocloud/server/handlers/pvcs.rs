use super::error::ApiError;
use super::selectors::{matches_pvc_filter, parse_object_selector};
use super::watch::{parse_resource_version, ResourceVersionMatchPolicy};
use crate::nanocloud::csi::CsiDriver;
use crate::nanocloud::k8s::persistentvolumeclaim::{
    PersistentVolumeClaim, PersistentVolumeClaimList, PersistentVolumeClaimSpec,
    PersistentVolumeClaimStatus,
};
use crate::nanocloud::k8s::pod::{ListMeta, ObjectMeta};
use crate::nanocloud::k8s::service_registry::ServiceRegistry;
use crate::nanocloud::k8s::store::{
    decode_continue_token, encode_continue_token, paginate_entries, PaginationError,
};
use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use std::collections::HashMap;
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
            "watch is not supported for persistentvolumeclaims",
        ));
    }

    let claims = collect_claims(Some(namespace.as_str()))?;
    let pvc = claims
        .into_iter()
        .find(|pvc| pvc.metadata.name.as_deref() == Some(name.as_str()))
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "PersistentVolumeClaim not found"))?;
    Ok(Json(pvc))
}

fn collect_claims(namespace: Option<&str>) -> Result<Vec<PersistentVolumeClaim>, ApiError> {
    let driver = CsiDriver::shared();
    let registry = ServiceRegistry::shared();
    let services = registry.list(namespace);
    let mut claims = Vec::new();

    for service in services {
        let ns = service
            .metadata
            .namespace
            .as_deref()
            .unwrap_or("default")
            .to_string();
        let svc_name = service
            .metadata
            .name
            .as_deref()
            .unwrap_or("service")
            .to_string();
        match driver.list_service_volumes(&ns, &svc_name) {
            Ok(volumes) => {
                for volume in volumes {
                    let claim_name = volume
                        .parameters
                        .get("claim")
                        .cloned()
                        .unwrap_or_else(|| volume.volume.volume_id.clone());
                    let mut pvc = PersistentVolumeClaim {
                        api_version: Some("v1".to_string()),
                        kind: Some("PersistentVolumeClaim".to_string()),
                        metadata: ObjectMeta {
                            name: Some(claim_name.clone()),
                            namespace: Some(ns.clone()),
                            annotations: {
                                let mut map = HashMap::new();
                                map.insert("nanocloud.io/service".to_string(), svc_name.clone());
                                map.insert(
                                    "nanocloud.io/volumeId".to_string(),
                                    volume.volume.volume_id.clone(),
                                );
                                map
                            },
                            ..Default::default()
                        },
                        spec: PersistentVolumeClaimSpec::default(),
                        status: None,
                    };
                    pvc.metadata
                        .ensure_common_fields(Some(&ns), Some(&claim_name));
                    pvc.metadata.resource_version = Some("1".to_string());
                    let mut capacity = HashMap::new();
                    capacity.insert(
                        "storage".to_string(),
                        format!("{}B", volume.volume.capacity_bytes),
                    );
                    let status = PersistentVolumeClaimStatus {
                        access_modes: pvc.spec.access_modes.clone(),
                        capacity,
                        phase: Some("Bound".to_string()),
                    };
                    pvc.status = Some(status);
                    claims.push(pvc);
                }
            }
            Err(err) => return Err(ApiError::internal_error(err)),
        }
    }

    Ok(claims)
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
            "watch is not supported for persistentvolumeclaims",
        ));
    }
    let resource_version_threshold = parse_resource_version(resource_version.as_deref())?;
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

    let continue_cursor = continue_token
        .as_deref()
        .map(|token| decode_continue_token(token, "persistentvolumeclaims"))
        .transpose()
        .map_err(|err| ApiError::new(StatusCode::GONE, err.to_string()))?;

    let filter =
        parse_object_selector(field_selector.as_deref(), label_selector.as_deref())?.map(Arc::new);

    let claims = collect_claims(namespace)?;
    let selector_ref = filter.as_deref();
    let effective_threshold = match resource_version_match {
        Some(ResourceVersionMatchPolicy::Exact) => {
            resource_version_threshold.map(|rv| rv.saturating_sub(1))
        }
        _ => resource_version_threshold,
    };
    let entries: Vec<(String, PersistentVolumeClaim, Option<String>)> = claims
        .into_iter()
        .filter(|pvc| {
            matches_pvc_filter(selector_ref, pvc)
                && pvc
                    .metadata
                    .resource_version
                    .as_deref()
                    .and_then(|rv| rv.parse::<u64>().ok())
                    .map(|rv| effective_threshold.map(|th| rv > th).unwrap_or(true))
                    .unwrap_or(true)
        })
        .map(|pvc| {
            let key = format!(
                "{}/{}",
                pvc.metadata.namespace.as_deref().unwrap_or("default"),
                pvc.metadata.name.as_deref().unwrap_or("claim"),
            );
            let rv = pvc.metadata.resource_version.clone();
            (key, pvc, rv)
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
        .map(|cursor| encode_continue_token("persistentvolumeclaims", cursor));
    let remaining_item_count = if page.remaining > 0 {
        Some(page.remaining.min(u32::MAX as usize) as u32)
    } else {
        None
    };
    let current_resource_version = page
        .items
        .iter()
        .filter_map(|pvc| {
            pvc.metadata
                .resource_version
                .as_deref()
                .and_then(|rv| rv.parse::<u64>().ok())
        })
        .max()
        .unwrap_or(1)
        .to_string();
    let list = PersistentVolumeClaimList {
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
