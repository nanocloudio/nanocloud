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

mod backup;

use axum::extract::Path;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::Json;

#[cfg(feature = "openapi")]
use crate::nanocloud::api::types::ErrorBody;
use crate::nanocloud::api::types::ServiceActionResponse;
use crate::nanocloud::engine::container::{start, stop, uninstall, BackupPlan};
use crate::nanocloud::k8s::store;
use crate::nanocloud::server::handlers::error::ApiError;

const OWNER_HEADER: &str = "x-nanocloud-owner";
const BACKUP_CONFIG_NAMESPACE: &str = "kube-system";
const BACKUP_CONFIG_NAME: &str = "nanocloud.io";
const BACKUP_RETENTION_KEY: &str = "backup.retentionCount";
const DEFAULT_BACKUP_RETENTION: usize = 3;

fn extract_owner(headers: &HeaderMap) -> Result<String, ApiError> {
    let value = headers.get(OWNER_HEADER).ok_or_else(|| {
        ApiError::new(StatusCode::UNAUTHORIZED, "missing X-Nanocloud-Owner header")
    })?;
    let owner = value.to_str().map_err(|_| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "X-Nanocloud-Owner header contains invalid characters",
        )
    })?;
    let trimmed = owner.trim();
    if trimmed.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "X-Nanocloud-Owner header must not be empty",
        ));
    }
    Ok(trimmed.to_string())
}

fn load_backup_retention() -> usize {
    match store::load_config_map(Some(BACKUP_CONFIG_NAMESPACE), BACKUP_CONFIG_NAME) {
        Ok(Some(config)) => config
            .data
            .get(BACKUP_RETENTION_KEY)
            .and_then(|value| value.trim().parse::<u32>().ok())
            .map(|value| value.max(1) as usize)
            .unwrap_or(DEFAULT_BACKUP_RETENTION),
        Ok(None) | Err(_) => DEFAULT_BACKUP_RETENTION,
    }
}

fn namespace_option(namespace: &str) -> Option<String> {
    let trimmed = namespace.trim();
    if trimmed.is_empty() || trimmed == "default" {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn namespace_ref(namespace: &str) -> Option<&str> {
    let trimmed = namespace.trim();
    if trimmed.is_empty() || trimmed == "default" {
        None
    } else {
        Some(trimmed)
    }
}

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/actions/start",
    params(
        ("namespace" = String, Path, description = "Bundle namespace"),
        ("name" = String, Path, description = "Bundle name")
    ),
    responses(
        (status = 202, description = "Start accepted", body = ServiceActionResponse),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["services.manage"])
    ),
    tag = "nanocloud"
))]
pub(crate) async fn start_bundle(
    Path((namespace, service)): Path<(String, String)>,
) -> Result<(StatusCode, Json<ServiceActionResponse>), ApiError> {
    start(namespace_ref(&namespace), &service)
        .await
        .map_err(ApiError::map_container_error)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(ServiceActionResponse {
            service,
            namespace: namespace_option(&namespace),
            action: "starting".to_string(),
            started: None,
        }),
    ))
}

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/actions/stop",
    params(
        ("namespace" = String, Path, description = "Bundle namespace"),
        ("name" = String, Path, description = "Bundle name")
    ),
    responses(
        (status = 202, description = "Stop accepted", body = ServiceActionResponse),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["services.manage"])
    ),
    tag = "nanocloud"
))]
pub(crate) async fn stop_bundle(
    Path((namespace, service)): Path<(String, String)>,
) -> Result<(StatusCode, Json<ServiceActionResponse>), ApiError> {
    stop(namespace_ref(&namespace), &service)
        .await
        .map_err(ApiError::map_container_error)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(ServiceActionResponse {
            service,
            namespace: namespace_option(&namespace),
            action: "stopping".to_string(),
            started: None,
        }),
    ))
}

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/actions/restart",
    params(
        ("namespace" = String, Path, description = "Bundle namespace"),
        ("name" = String, Path, description = "Bundle name")
    ),
    responses(
        (status = 202, description = "Restart accepted", body = ServiceActionResponse),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["services.manage"])
    ),
    tag = "nanocloud"
))]
pub(crate) async fn restart_bundle(
    Path((namespace, service)): Path<(String, String)>,
) -> Result<(StatusCode, Json<ServiceActionResponse>), ApiError> {
    stop(namespace_ref(&namespace), &service)
        .await
        .map_err(ApiError::map_container_error)?;
    start(namespace_ref(&namespace), &service)
        .await
        .map_err(ApiError::map_container_error)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(ServiceActionResponse {
            service,
            namespace: namespace_option(&namespace),
            action: "restarting".to_string(),
            started: None,
        }),
    ))
}

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/actions/uninstall",
    params(
        ("namespace" = String, Path, description = "Bundle namespace"),
        ("name" = String, Path, description = "Bundle name"),
        ("X-Nanocloud-Owner" = String, Header, description = "Backup owner identifier")
    ),
    responses(
        (status = 202, description = "Uninstall accepted", body = ServiceActionResponse),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["services.manage"])
    ),
    tag = "nanocloud"
))]
pub(crate) async fn uninstall_bundle(
    headers: HeaderMap,
    Path((namespace, service)): Path<(String, String)>,
) -> Result<(StatusCode, Json<ServiceActionResponse>), ApiError> {
    let owner = extract_owner(&headers)?;
    let retention = load_backup_retention();

    stop(namespace_ref(&namespace), &service)
        .await
        .map_err(ApiError::map_container_error)?;

    let plan = BackupPlan { owner, retention };

    uninstall(namespace_ref(&namespace), &service, plan)
        .await
        .map_err(ApiError::map_container_error)?;

    Ok((
        StatusCode::ACCEPTED,
        Json(ServiceActionResponse {
            service,
            namespace: namespace_option(&namespace),
            action: "uninstalling".to_string(),
            started: None,
        }),
    ))
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/backups/latest",
    params(
        ("namespace" = String, Path, description = "Bundle namespace"),
        ("name" = String, Path, description = "Bundle name"),
        ("X-Nanocloud-Owner" = String, Header, description = "Backup owner identifier")
    ),
    responses(
        (status = 200, description = "Latest backup archive"),
        (status = 404, description = "No backup available", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["services.read"])
    ),
    tag = "nanocloud"
))]
pub(crate) async fn stream_latest_backup(
    headers: HeaderMap,
    Path((namespace, service)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let owner = extract_owner(&headers)?;
    let namespace_opt = namespace_option(&namespace);
    backup::stream_latest_backup(owner, namespace_opt, service).await
}
