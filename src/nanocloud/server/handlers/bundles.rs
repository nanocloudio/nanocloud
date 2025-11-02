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

use axum::body::Bytes;
use axum::extract::{Path, Query};
use axum::http::{
    header::{CONTENT_DISPOSITION, CONTENT_TYPE},
    HeaderMap, HeaderValue, StatusCode,
};
use axum::Json;

use crate::nanocloud::api::schema::{
    format_bundle_error_summary, validate_bundle, BundleSchemaError,
};
#[cfg(feature = "openapi")]
use crate::nanocloud::api::types::ErrorBody;
use crate::nanocloud::api::types::{
    ApplyConflict, Bundle, BundleList, BundleProfileArtifact, BundleProfileArtifactData,
    BundleProfileArtifactIntegrity, BundleProfileExportManifest, BundleProfileExportMetadata,
};
use crate::nanocloud::engine::profile::Profile;
use crate::nanocloud::k8s::bundle_manager::{
    BundleApplyError, BundleApplyOptions, BundleError, BundleRegistry,
};
use crate::nanocloud::k8s::pod::ListMeta;
use chrono::{SecondsFormat, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tar::{Builder, Header};

use super::error::ApiError;

#[derive(Default, serde::Deserialize)]
pub struct ListParams {
    #[serde(rename = "limit")]
    _limit: Option<u32>,
    #[serde(rename = "continue")]
    _continue_token: Option<String>,
}

#[derive(Default, serde::Deserialize)]
pub struct ApplyParams {
    #[serde(rename = "fieldManager")]
    field_manager: Option<String>,
    #[serde(default)]
    force: Option<bool>,
    #[serde(rename = "dryRun", default)]
    dry_run: Option<bool>,
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
    validate_bundle(&payload).map_err(map_schema_error)?;
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

#[cfg_attr(feature = "openapi", utoipa::path(
    patch,
    path = "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}",
    params(
        ("namespace" = String, Path, description = "Bundle namespace"),
        ("name" = String, Path, description = "Bundle name"),
        ("fieldManager" = String, Query, description = "Field manager identifier for SSA"),
        ("force" = Option<bool>, Query, description = "Whether to override existing managers on conflict"),
        ("dryRun" = Option<bool>, Query, description = "Validate only without persisting changes")
    ),
    request_body(
        content = Bundle,
        description = "Bundle manifest fragment containing spec changes",
        content_type = "application/apply-patch+json"
    ),
    responses(
        (status = 200, description = "Bundle updated", body = Bundle),
        (status = 400, description = "Invalid apply payload", body = ErrorBody),
        (status = 404, description = "Bundle not found", body = ErrorBody),
        (status = 409, description = "Conflict with another field manager", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["bundles.manage"])
    ),
    tag = "nanocloud"
))]
pub async fn apply(
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<ApplyParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Bundle>, ApiError> {
    let field_manager = params
        .field_manager
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ApiError::bad_request("fieldManager query parameter is required for server-side apply")
        })?;
    let format = parse_apply_content_type(&headers)?;
    let patch = decode_apply_body(format, &body)?;
    let force = params.force.unwrap_or(false);
    let dry_run = params.dry_run.unwrap_or(false);

    let registry = BundleRegistry::shared();
    match registry
        .apply_bundle(
            &namespace,
            &name,
            patch,
            BundleApplyOptions {
                manager: field_manager,
                force,
                dry_run,
            },
        )
        .await
    {
        Ok(bundle) => Ok(Json(bundle)),
        Err(BundleApplyError::Conflict { message, conflicts }) => {
            let details = conflicts
                .into_iter()
                .map(|conflict| ApplyConflict {
                    path: conflict.path,
                    existing_manager: conflict.owner,
                })
                .collect();
            Err(ApiError::conflict_with_details(message, details))
        }
        Err(BundleApplyError::Failure(err)) => Err(map_error(err)),
    }
}

fn map_schema_error(err: BundleSchemaError) -> ApiError {
    match err {
        BundleSchemaError::UnsupportedVersion(version) => ApiError::bad_request(format!(
            "Unsupported bundle apiVersion '{}'; supported: nanocloud.io/v1",
            version
        )),
        BundleSchemaError::Validation(issues) => {
            let message = format_bundle_error_summary(&issues);
            ApiError::bad_request(message)
        }
    }
}

enum ApplyBodyFormat {
    Json,
    Yaml,
}

fn parse_apply_content_type(headers: &HeaderMap) -> Result<ApplyBodyFormat, ApiError> {
    let value = headers.get(CONTENT_TYPE).ok_or_else(|| {
        ApiError::bad_request(
            "Content-Type must be application/apply-patch+json or application/apply-patch+yaml",
        )
    })?;
    let raw = value
        .to_str()
        .map_err(|_| ApiError::bad_request("Content-Type header must be valid UTF-8"))?;
    let normalized = raw
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    match normalized.as_str() {
        "application/apply-patch+json" => Ok(ApplyBodyFormat::Json),
        "application/apply-patch+yaml" => Ok(ApplyBodyFormat::Yaml),
        _ => Err(ApiError::bad_request(format!(
            "Unsupported Content-Type '{raw}'. Use application/apply-patch+json or application/apply-patch+yaml",
        ))),
    }
}

fn decode_apply_body(format: ApplyBodyFormat, body: &Bytes) -> Result<Value, ApiError> {
    if body.is_empty() {
        return Err(ApiError::bad_request("Apply payload must not be empty"));
    }
    match format {
        ApplyBodyFormat::Json => serde_json::from_slice::<Value>(body)
            .map_err(|err| ApiError::bad_request(format!("Invalid JSON apply payload: {err}"))),
        ApplyBodyFormat::Yaml => serde_yaml::from_slice::<Value>(body)
            .map_err(|err| ApiError::bad_request(format!("Invalid YAML apply payload: {err}"))),
    }
}

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/exportProfile",
    params(
        ("namespace" = String, Path, description = "Bundle namespace"),
        ("name" = String, Path, description = "Bundle name")
    ),
    responses(
        (status = 200, description = "Profile export tarball"),
        (status = 404, description = "Bundle not found", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["bundles.read"])
    ),
    tag = "nanocloud"
))]
pub async fn export_profile(
    Path((namespace, name)): Path<(String, String)>,
) -> Result<(HeaderMap, Bytes), ApiError> {
    let registry = BundleRegistry::shared();
    let bundle = registry.get(&namespace, &name).await.ok_or_else(|| {
        ApiError::new(StatusCode::NOT_FOUND, format!("Bundle '{name}' not found"))
    })?;
    let profile =
        Profile::from_spec_fields(bundle.spec.profile_key.as_deref(), &bundle.spec.options)
            .map_err(ApiError::internal_error)?;
    let (profile_key, options, secrets) = profile
        .export_components()
        .map_err(ApiError::internal_error)?;
    let bindings = profile.binding_history_entries();
    let created_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

    let metadata = BundleProfileExportMetadata {
        name: bundle.metadata.name.clone().unwrap_or_else(|| name.clone()),
        namespace: namespace.clone(),
        service: bundle.spec.service.clone(),
        resource_version: bundle.metadata.resource_version.clone(),
        created_at: created_at.clone(),
    };
    let data = BundleProfileArtifactData {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "BundleProfileArtifact".to_string(),
        metadata: metadata.clone(),
        created_at: created_at.clone(),
        profile_key,
        options,
        secrets,
        bindings,
    };
    let data_bytes =
        serde_json::to_vec_pretty(&data).map_err(|err| ApiError::internal_error(Box::new(err)))?;

    let mut digest = Sha256::new();
    digest.update(&data_bytes);
    let digest_hex = format!("{:x}", digest.finalize());

    let artifact = BundleProfileArtifact {
        data,
        integrity: BundleProfileArtifactIntegrity {
            sha256: digest_hex.clone(),
        },
    };
    let profile_json = serde_json::to_string_pretty(&artifact)
        .map_err(|err| ApiError::internal_error(Box::new(err)))?;

    let manifest = BundleProfileExportManifest {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "BundleProfileExport".to_string(),
        metadata,
        digest: digest_hex,
    };
    let manifest_json = serde_json::to_string_pretty(&manifest)
        .map_err(|err| ApiError::internal_error(Box::new(err)))?;

    let mut archive = Vec::new();
    {
        let mut builder = Builder::new(&mut archive);
        append_tar_entry(&mut builder, "manifest.json", manifest_json.as_bytes())?;
        append_tar_entry(&mut builder, "profile.json", profile_json.as_bytes())?;
        builder
            .finish()
            .map_err(|err| ApiError::internal_error(Box::new(err)))?;
    }

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/x-tar"));
    let filename = format!("{}-profile.tar", name);
    headers.insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{filename}\""))
            .map_err(|err| ApiError::internal_error(Box::new(err)))?,
    );
    Ok((headers, Bytes::from(archive)))
}

fn append_tar_entry(
    builder: &mut Builder<&mut Vec<u8>>,
    path: &str,
    contents: &[u8],
) -> Result<(), ApiError> {
    let mut header = Header::new_gnu();
    header
        .set_path(path)
        .map_err(|err| ApiError::internal_error(Box::new(err)))?;
    header.set_size(contents.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    builder
        .append(&header, contents)
        .map_err(|err| ApiError::internal_error(Box::new(err)))?;
    Ok(())
}
