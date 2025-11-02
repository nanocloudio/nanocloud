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

use std::time::{Duration, SystemTime};

use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::Json;
use chrono::{DateTime, SecondsFormat, Utc};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use openssl::nid::Nid;
use openssl::x509::X509Req;

use super::error::ApiError;
use super::serviceaccounts::{ServiceAccountClaims, CERTIFICATE_SCOPE, DEFAULT_AUDIENCE};
use crate::nanocloud::api::types::{CertificateRequest, CertificateResponse, CertificateStatus};
use crate::nanocloud::logger::log_info;
use crate::nanocloud::server::auth::bootstrap::{BootstrapTokenError, BootstrapTokenService};
use crate::nanocloud::util::security::{load_ca, load_service_secret_key, sign_csr};

const COMPONENT: &str = "certificates";
const API_VERSION: &str = "nanocloud.io/v1";
const KIND: &str = "Certificate";
const CERTIFICATE_TTL_DAYS: u32 = 7;
const CERTIFICATE_TTL: Duration = Duration::from_secs((CERTIFICATE_TTL_DAYS as u64) * 24 * 60 * 60);

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/apis/nanocloud.io/v1/certificates",
    request_body = CertificateRequest,
    responses(
        (status = 200, description = "Certificate issued", body = CertificateResponse),
        (
            status = 400,
            description = "Invalid request",
            body = crate::nanocloud::api::types::ErrorBody
        ),
        (
            status = 401,
            description = "Token expired or invalid",
            body = crate::nanocloud::api::types::ErrorBody
        ),
        (
            status = 403,
            description = "Token lacks certificate scope",
            body = crate::nanocloud::api::types::ErrorBody
        ),
        (
            status = 500,
            description = "Internal error",
            body = crate::nanocloud::api::types::ErrorBody
        )
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBootstrap" = ["bootstrap"]),
        (
            "NanocloudBearer" = ["certificate.issue"]
        )
    ),
    tag = "nanocloud"
))]
pub async fn issue_ephemeral_certificate(
    headers: HeaderMap,
    Json(request): Json<CertificateRequest>,
) -> Result<Json<CertificateResponse>, ApiError> {
    if request.api_version != API_VERSION {
        return Err(ApiError::bad_request(
            "apiVersion must be 'nanocloud.io/v1'",
        ));
    }
    if request.kind.to_lowercase() != KIND.to_lowercase() {
        return Err(ApiError::bad_request("kind must be 'Certificate'"));
    }
    let csr_pem = request.spec.csr_pem.trim();
    if csr_pem.is_empty() {
        return Err(ApiError::bad_request("spec.csr must not be empty"));
    }

    let csr = parse_csr(csr_pem)?;
    verify_csr_signature(&csr)?;
    let csr_common_name = extract_common_name(&csr).ok_or_else(|| {
        ApiError::bad_request("CSR must include a common name (CN) in the subject")
    })?;
    let token = extract_bearer_token(&headers)?;
    let subject = if token_is_jwt(&token) {
        let claims = validate_serviceaccount_token(&token)?;
        if !claims.scope.iter().any(|scope| scope == CERTIFICATE_SCOPE) {
            return Err(ApiError::new(
                StatusCode::FORBIDDEN,
                "token is not authorized to issue certificates",
            ));
        }
        if claims.sub != csr_common_name {
            return Err(ApiError::bad_request(
                "token subject does not match CSR common name",
            ));
        }
        claims.sub
    } else if token_is_bootstrap(&token) {
        let service = BootstrapTokenService::new();
        match service.lookup(&token) {
            Ok(Some(bootstrap)) => {
                if !bootstrap
                    .scopes
                    .iter()
                    .any(|scope| scope == CERTIFICATE_SCOPE)
                {
                    return Err(ApiError::new(
                        StatusCode::FORBIDDEN,
                        "token is not authorized to issue certificates",
                    ));
                }
                if bootstrap.subject != csr_common_name {
                    return Err(ApiError::bad_request(
                        "token subject does not match CSR common name",
                    ));
                }
                let result = service.consume_token(&token);
                if let Err(err) = result {
                    return Err(ApiError::internal_error(err.into()));
                }
                bootstrap.subject
            }
            Ok(None) => {
                return Err(ApiError::new(StatusCode::UNAUTHORIZED, "invalid token"));
            }
            Err(BootstrapTokenError::Malformed(_)) => {
                return Err(ApiError::new(StatusCode::UNAUTHORIZED, "invalid token"));
            }
            Err(BootstrapTokenError::Storage(err)) => {
                return Err(ApiError::internal_error(err.into()));
            }
        }
    } else {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "Authorization header contained unsupported token format",
        ));
    };

    let (ca_cert, ca_key) = load_ca().map_err(ApiError::internal_error)?;
    let signed_cert = sign_csr(&csr, &ca_cert, &ca_key, CERTIFICATE_TTL_DAYS)
        .map_err(|err| ApiError::internal_error(err.into()))?;

    let certificate_pem = signed_cert
        .to_pem()
        .map_err(|err| ApiError::internal_error(err.into()))?;
    let ca_bundle_pem = ca_cert
        .to_pem()
        .map_err(|err| ApiError::internal_error(err.into()))?;

    let issued_at = SystemTime::now();
    let expires_at = issued_at
        .checked_add(CERTIFICATE_TTL)
        .ok_or_else(|| ApiError::internal_message("certificate TTL overflow"))?;

    log_info(
        COMPONENT,
        "Issued ephemeral certificate",
        &[("subject", subject.as_str())],
    );

    let response = CertificateResponse {
        api_version: API_VERSION.to_string(),
        kind: KIND.to_string(),
        status: CertificateStatus {
            certificate_pem: String::from_utf8_lossy(&certificate_pem).into_owned(),
            ca_bundle_pem: String::from_utf8_lossy(&ca_bundle_pem).into_owned(),
            expiration_timestamp: Some(format_timestamp(expires_at)),
        },
    };

    Ok(Json(response))
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
        return Err(ApiError::bad_request("CSR signature verification failed"));
    }
    Ok(())
}

fn extract_common_name(csr: &X509Req) -> Option<String> {
    csr.subject_name()
        .entries_by_nid(Nid::COMMONNAME)
        .next()
        .and_then(|entry| entry.data().as_utf8().ok().map(|data| data.to_string()))
}

fn token_is_jwt(token: &str) -> bool {
    let mut segments = token.split('.');
    matches!(
        (segments.next(), segments.next(), segments.next(), segments.next()),
        (Some(a), Some(b), Some(c), None) if !a.is_empty() && !b.is_empty() && !c.is_empty()
    )
}

fn token_is_bootstrap(token: &str) -> bool {
    let mut segments = token.split('.');
    matches!(
        (segments.next(), segments.next(), segments.next()),
        (Some(id), Some(secret), None) if !id.is_empty() && !secret.is_empty()
    )
}

fn extract_bearer_token(headers: &HeaderMap) -> Result<String, ApiError> {
    let value = headers
        .get(AUTHORIZATION)
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "missing Authorization header"))?;
    let value_str = value
        .to_str()
        .map_err(|_| ApiError::new(StatusCode::UNAUTHORIZED, "invalid Authorization header"))?;
    let token = value_str
        .strip_prefix("Bearer ")
        .or_else(|| value_str.strip_prefix("bearer "))
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::UNAUTHORIZED,
                "Authorization header must be a Bearer token",
            )
        })?
        .trim();
    if token.is_empty() {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "Authorization header must include a token",
        ));
    }
    Ok(token.to_string())
}

fn validate_serviceaccount_token(token: &str) -> Result<ServiceAccountClaims, ApiError> {
    let decoding_key = decoding_key()?;
    let mut validation = Validation::new(Algorithm::RS256);
    let expected_audience = vec![DEFAULT_AUDIENCE.to_string()];
    validation.set_audience(&expected_audience);
    validation.required_spec_claims.insert("sub".to_string());
    validation.required_spec_claims.insert("scope".to_string());

    decode::<ServiceAccountClaims>(token, &decoding_key, &validation)
        .map(|data| data.claims)
        .map_err(|err| match err.into_kind() {
            jsonwebtoken::errors::ErrorKind::ExpiredSignature => {
                ApiError::new(StatusCode::UNAUTHORIZED, "token expired")
            }
            _ => ApiError::new(StatusCode::UNAUTHORIZED, "invalid token"),
        })
}

fn decoding_key() -> Result<DecodingKey, ApiError> {
    let key = load_service_secret_key().map_err(ApiError::internal_error)?;
    let pem = key
        .public_key_to_pem()
        .map_err(|err| ApiError::internal_error(err.into()))?;
    DecodingKey::from_rsa_pem(&pem).map_err(|err| ApiError::internal_error(err.into()))
}

fn format_timestamp(time: SystemTime) -> String {
    let datetime: DateTime<Utc> = time.into();
    datetime.to_rfc3339_opts(SecondsFormat::Secs, true)
}
