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

use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::Json;

#[cfg(feature = "openapi")]
use crate::nanocloud::api::types::ErrorBody;
use crate::nanocloud::api::types::{Bundle, BundleList};
use crate::nanocloud::k8s::bundle_manager::{BundleError, BundleRegistry};
use crate::nanocloud::k8s::pod::ListMeta;

use super::error::ApiError;

#[derive(Default, serde::Deserialize)]
pub struct ListParams {
    #[serde(rename = "limit")]
    _limit: Option<u32>,
    #[serde(rename = "continue")]
    _continue_token: Option<String>,
}

fn map_error(err: BundleError) -> ApiError {
    match err {
        BundleError::AlreadyExists(msg) | BundleError::Conflict(msg) => {
            ApiError::new(StatusCode::CONFLICT, msg)
        }
        BundleError::Invalid(msg) => ApiError::bad_request(msg),
        BundleError::NotFound(msg) => ApiError::new(StatusCode::NOT_FOUND, msg),
        BundleError::Persistence(err) => ApiError::internal_error(err),
    }
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles",
    params(
        ("namespace" = String, Path, description = "Namespace to query"),
        ("limit" = Option<u32>, Query, description = "Maximum number of entries to return"),
        ("continue" = Option<String>, Query, description = "Continue token for pagination")
    ),
    responses(
        (status = 200, description = "Bundle list", body = BundleList),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["bundles.read"])
    ),
    tag = "nanocloud"
))]
pub async fn list(
    Path(namespace): Path<String>,
    Query(_params): Query<ListParams>,
) -> Result<Json<BundleList>, ApiError> {
    let registry = BundleRegistry::shared();
    let items = registry.list(Some(namespace.as_str())).await;
    let list = BundleList {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "BundleList".to_string(),
        metadata: ListMeta::default(),
        items,
    };
    Ok(Json(list))
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}",
    params(
        ("namespace" = String, Path, description = "Namespace of the bundle"),
        ("name" = String, Path, description = "Bundle name")
    ),
    responses(
        (status = 200, description = "Bundle", body = Bundle),
        (status = 404, description = "Bundle not found", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["bundles.read"])
    ),
    tag = "nanocloud"
))]
pub async fn get(
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Bundle>, ApiError> {
    let registry = BundleRegistry::shared();
    match registry.get(&namespace, &name).await {
        Some(bundle) => Ok(Json(bundle)),
        None => Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("Bundle '{name}' not found"),
        )),
    }
}

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles",
    request_body = Bundle,
    params(
        ("namespace" = String, Path, description = "Namespace for the new bundle")
    ),
    responses(
        (status = 201, description = "Bundle created", body = Bundle),
        (status = 400, description = "Invalid bundle", body = ErrorBody),
        (status = 409, description = "Bundle already exists", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["bundles.manage"])
    ),
    tag = "nanocloud"
))]
pub async fn create(
    Path(namespace): Path<String>,
    Json(payload): Json<Bundle>,
) -> Result<(StatusCode, Json<Bundle>), ApiError> {
    let registry = BundleRegistry::shared();
    registry
        .create(&namespace, payload)
        .await
        .map(|bundle| (StatusCode::CREATED, Json(bundle)))
        .map_err(map_error)
}

#[cfg_attr(feature = "openapi", utoipa::path(
    delete,
    path = "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}",
    params(
        ("namespace" = String, Path, description = "Namespace of the bundle"),
        ("name" = String, Path, description = "Bundle name")
    ),
    responses(
        (status = 202, description = "Bundle deletion accepted", body = Bundle),
        (status = 404, description = "Bundle not found", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["bundles.manage"])
    ),
    tag = "nanocloud"
))]
pub async fn delete(
    Path((namespace, name)): Path<(String, String)>,
) -> Result<(StatusCode, Json<Bundle>), ApiError> {
    let registry = BundleRegistry::shared();
    let bundle = match registry.get(&namespace, &name).await {
        Some(bundle) => bundle,
        None => {
            return Err(ApiError::new(
                StatusCode::NOT_FOUND,
                format!("Bundle '{name}' not found"),
            ))
        }
    };
    registry
        .delete(&namespace, &name)
        .await
        .map_err(map_error)?;
    Ok((StatusCode::ACCEPTED, Json(bundle)))
}
