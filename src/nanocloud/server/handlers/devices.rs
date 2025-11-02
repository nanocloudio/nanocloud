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

use chrono::{SecondsFormat, Utc};
use openssl::nid::Nid;
use openssl::x509::X509Req;
use std::time::{Duration, SystemTime};

#[cfg(feature = "openapi")]
use crate::nanocloud::api::types::ErrorBody;
use crate::nanocloud::api::types::{
    CertificateRequest, CertificateResponse, CertificateStatus, Device, DeviceList, DeviceStatus,
};
use crate::nanocloud::k8s::device_manager::{DeviceError, DeviceRegistry};
use crate::nanocloud::k8s::pod::ListMeta;
use crate::nanocloud::logger::{log_info, log_warn};
use crate::nanocloud::server::auth::{AuthContext, AuthScope, AuthSubject};
use crate::nanocloud::util::security::{load_ca, sign_csr};

use super::error::ApiError;

#[derive(Default, serde::Deserialize)]
pub struct ListParams {
    #[serde(rename = "limit")]
    _limit: Option<u32>,
    #[serde(rename = "continue")]
    _continue_token: Option<String>,
}

const COMPONENT: &str = "devices";
const API_VERSION: &str = "nanocloud.io/v1";
const CERTIFICATE_KIND: &str = "Certificate";
const DEVICE_CERTIFICATE_TTL_DAYS: u32 = 7;
const DEVICE_CERTIFICATE_TTL: Duration =
    Duration::from_secs((DEVICE_CERTIFICATE_TTL_DAYS as u64) * 24 * 60 * 60);

fn map_error(err: DeviceError) -> ApiError {
    match err {
        DeviceError::AlreadyExists(msg) => ApiError::new(StatusCode::CONFLICT, msg),
        DeviceError::Invalid(msg) => ApiError::bad_request(msg),
        DeviceError::NotFound(msg) => ApiError::new(StatusCode::NOT_FOUND, msg),
        DeviceError::Persistence(err) => ApiError::internal_error(err),
    }
}

fn ensure_admin_scope(context: &AuthContext) -> Result<(), ApiError> {
    if matches!(context.scope(), AuthScope::Device) {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "device credentials are not authorized for this operation",
        ));
    }
    Ok(())
}

fn authorize_device_read(
    context: &AuthContext,
    _namespace: &str,
    name: &str,
) -> Result<(), ApiError> {
    match (context.scope(), context.subject()) {
        (AuthScope::Device, AuthSubject::Device { device_id, .. }) => {
            let expected_name = format!("device-{device_id}");
            if expected_name != name {
                return Err(ApiError::new(
                    StatusCode::FORBIDDEN,
                    "device certificate does not match requested resource",
                ));
            }
            Ok(())
        }
        _ => ensure_admin_scope(context),
    }
}

fn parse_csr(csr_pem: &str) -> Result<X509Req, ApiError> {
    X509Req::from_pem(csr_pem.as_bytes())
        .map_err(|err| ApiError::bad_request(format!("invalid CSR: {err}")))
}

fn verify_csr_signature(csr: &X509Req) -> Result<(), ApiError> {
    let public_key = csr
        .public_key()
        .map_err(|err| ApiError::bad_request(format!("invalid CSR key: {err}")))?;
    let verified = csr
        .verify(&public_key)
        .map_err(|err| ApiError::bad_request(format!("failed to verify CSR signature: {err}")))?;
    if !verified {
        return Err(ApiError::bad_request(
            "CSR signature verification returned false",
        ));
    }
    Ok(())
}

fn extract_common_name(csr: &X509Req) -> Option<String> {
    let subject = csr.subject_name();
    for entry in subject.entries() {
        if entry.object().nid() == Nid::COMMONNAME {
            if let Ok(value) = entry.data().as_utf8() {
                return Some(value.to_string());
            }
        }
    }
    None
}

fn format_timestamp(timestamp: SystemTime) -> String {
    let datetime: chrono::DateTime<Utc> = timestamp.into();
    datetime.to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/nanocloud.io/v1/namespaces/{namespace}/devices",
    params(
        ("namespace" = String, Path, description = "Namespace to query"),
        ("limit" = Option<u32>, Query, description = "Maximum number of entries to return"),
        ("continue" = Option<String>, Query, description = "Continue token for pagination")
    ),
    responses(
        (status = 200, description = "Device list", body = DeviceList),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["devices.read"])
    ),
    tag = "nanocloud"
))]
pub async fn list(
    auth: AuthContext,
    Path(namespace): Path<String>,
    Query(_params): Query<ListParams>,
) -> Result<Json<DeviceList>, ApiError> {
    ensure_admin_scope(&auth)?;
    let registry = DeviceRegistry::shared();
    let items = registry.list(Some(namespace.as_str())).await;
    let list = DeviceList {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "DeviceList".to_string(),
        metadata: ListMeta::default(),
        items,
    };
    Ok(Json(list))
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/nanocloud.io/v1/namespaces/{namespace}/devices/{name}",
    params(
        ("namespace" = String, Path, description = "Namespace of the device"),
        ("name" = String, Path, description = "Device name")
    ),
    responses(
        (status = 200, description = "Device", body = Device),
        (status = 404, description = "Device not found", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["devices.read"]),
        ("NanocloudDevice" = [])
    ),
    tag = "nanocloud"
))]
pub async fn get(
    auth: AuthContext,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Device>, ApiError> {
    authorize_device_read(&auth, namespace.as_str(), name.as_str())?;
    let registry = DeviceRegistry::shared();
    match registry.get(&namespace, &name).await {
        Some(device) => Ok(Json(device)),
        None => Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("Device '{name}' not found"),
        )),
    }
}

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/apis/nanocloud.io/v1/namespaces/{namespace}/devices",
    request_body = Device,
    params(
        ("namespace" = String, Path, description = "Namespace for the new device")
    ),
    responses(
        (status = 201, description = "Device created", body = Device),
        (status = 400, description = "Invalid device", body = ErrorBody),
        (status = 409, description = "Device already exists", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["devices.manage"])
    ),
    tag = "nanocloud"
))]
pub async fn create(
    auth: AuthContext,
    Path(namespace): Path<String>,
    Json(payload): Json<Device>,
) -> Result<(StatusCode, Json<Device>), ApiError> {
    ensure_admin_scope(&auth)?;
    let registry = DeviceRegistry::shared();
    registry
        .create(&namespace, payload)
        .await
        .map(|device| (StatusCode::CREATED, Json(device)))
        .map_err(map_error)
}

#[cfg_attr(feature = "openapi", utoipa::path(
    delete,
    path = "/apis/nanocloud.io/v1/namespaces/{namespace}/devices/{name}",
    params(
        ("namespace" = String, Path, description = "Namespace of the device"),
        ("name" = String, Path, description = "Device name")
    ),
    responses(
        (status = 202, description = "Device deletion accepted", body = Device),
        (status = 404, description = "Device not found", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["devices.manage"])
    ),
    tag = "nanocloud"
))]
pub async fn delete(
    auth: AuthContext,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<(StatusCode, Json<Device>), ApiError> {
    ensure_admin_scope(&auth)?;
    let registry = DeviceRegistry::shared();
    registry
        .delete(&namespace, &name)
        .await
        .map(|device| (StatusCode::ACCEPTED, Json(device)))
        .map_err(map_error)
}

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/apis/nanocloud.io/v1/namespaces/{namespace}/devices/certificates",
    request_body = CertificateRequest,
    params(
        ("namespace" = String, Path, description = "Namespace owning the device")
    ),
    responses(
        (status = 200, description = "Certificate issued", body = CertificateResponse),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["devices.manage"])
    ),
    tag = "nanocloud"
))]
pub async fn issue_certificate(
    auth: AuthContext,
    Path(namespace): Path<String>,
    Json(request): Json<CertificateRequest>,
) -> Result<Json<CertificateResponse>, ApiError> {
    ensure_admin_scope(&auth)?;

    if request.api_version != API_VERSION {
        return Err(ApiError::bad_request(
            "apiVersion must be 'nanocloud.io/v1'",
        ));
    }
    if request.kind.to_lowercase() != CERTIFICATE_KIND.to_lowercase() {
        return Err(ApiError::bad_request("kind must be 'Certificate'"));
    }

    let csr_pem = request.spec.csr_pem.trim();
    if csr_pem.is_empty() {
        return Err(ApiError::bad_request("spec.csr must not be empty"));
    }

    let csr = parse_csr(csr_pem)?;
    verify_csr_signature(&csr)?;

    let common_name = extract_common_name(&csr).ok_or_else(|| {
        ApiError::bad_request("CSR must include a common name (CN) in the subject")
    })?;
    let device_id = common_name
        .strip_prefix("device:")
        .ok_or_else(|| {
            ApiError::bad_request("CSR common name must use the 'device:<hash>' format")
        })?
        .trim();
    if device_id.is_empty() {
        return Err(ApiError::bad_request(
            "CSR common name must include a non-empty device hash",
        ));
    }
    let device_id = device_id.to_string();

    let (ca_cert, ca_key) = load_ca().map_err(ApiError::internal_error)?;
    let signed_cert = sign_csr(&csr, &ca_cert, &ca_key, DEVICE_CERTIFICATE_TTL_DAYS)
        .map_err(|err| ApiError::internal_error(err.into()))?;

    let certificate_pem = signed_cert
        .to_pem()
        .map_err(|err| ApiError::internal_error(err.into()))?;
    let ca_bundle_pem = ca_cert
        .to_pem()
        .map_err(|err| ApiError::internal_error(err.into()))?;

    let issued_at = SystemTime::now();
    let expires_at = issued_at
        .checked_add(DEVICE_CERTIFICATE_TTL)
        .ok_or_else(|| ApiError::internal_message("certificate TTL overflow"))?;

    log_info(
        COMPONENT,
        "Issued device certificate",
        &[
            ("namespace", namespace.as_str()),
            ("device", device_id.as_str()),
        ],
    );

    let status = DeviceStatus {
        certificate_issued_at: Some(format_timestamp(issued_at)),
        certificate_expires_at: Some(format_timestamp(expires_at)),
        ..DeviceStatus::default()
    };

    let device_name = format!("device-{device_id}");
    let registry = DeviceRegistry::shared();
    if let Err(err) = registry
        .update_status(namespace.as_str(), device_name.as_str(), status)
        .await
    {
        if !matches!(err, DeviceError::NotFound(_)) {
            let error_text = err.to_string();
            log_warn(
                COMPONENT,
                "Failed to update device status after certificate issuance",
                &[
                    ("namespace", namespace.as_str()),
                    ("device", device_name.as_str()),
                    ("error", error_text.as_str()),
                ],
            );
        }
    }

    let response = CertificateResponse {
        api_version: request.api_version,
        kind: request.kind,
        status: CertificateStatus {
            certificate_pem: String::from_utf8_lossy(&certificate_pem).into_owned(),
            ca_bundle_pem: String::from_utf8_lossy(&ca_bundle_pem).into_owned(),
            expiration_timestamp: Some(format_timestamp(expires_at)),
        },
    };

    Ok(Json(response))
}
