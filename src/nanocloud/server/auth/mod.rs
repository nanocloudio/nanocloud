pub(crate) mod bootstrap;

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

use axum::body::Body;
use axum::extract::{FromRequestParts, MatchedPath};
use axum::http::header::AUTHORIZATION;
use axum::http::{request::Parts, HeaderMap, Method, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use openssl::error::ErrorStack;
use openssl::x509::{X509NameRef, X509};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt::{self, Write};
use std::future;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::layer::Layer;
use tower::Service;

use self::bootstrap::{BootstrapTokenError, BootstrapTokenService};
use crate::nanocloud::api::types::ErrorBody;
use crate::nanocloud::logger::{log_error, log_info, log_warn};
use crate::nanocloud::observability::metrics::{self, BootstrapAuthOutcome};
use crate::nanocloud::server::handlers;
use crate::nanocloud::server::handlers::serviceaccounts::ServiceAccountClaims;
use crate::nanocloud::util::security::load_service_secret_key;
const AUTH_LOG_COMPONENT: &str = "auth";
#[cfg_attr(not(test), allow(dead_code))]
pub const SECURITY_SCHEME_BOOTSTRAP: &str = "NanocloudBootstrap";
pub const SECURITY_SCHEME_BEARER: &str = "NanocloudBearer";
pub const SECURITY_SCHEME_DEVICE: &str = "NanocloudDevice";

type SecurityRequirement = BTreeMap<String, Vec<String>>;

/// Scope assigned to the authenticated subject for the current request.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub enum AuthScope {
    /// No authentication information has been established.
    #[default]
    Unauthenticated,
    /// Subject authenticated via trusted client certificate.
    Certificate,
    /// Subject authenticated via device certificate.
    Device,
    /// Subject authenticated via bootstrap token exchange.
    Bootstrap,
    /// Subject authenticated via JWT bearer token. Carries the scopes granted by the token.
    Jwt(Vec<String>),
}

/// Identity of the user or workload that initiated the current request.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub enum AuthSubject {
    /// No authenticated identity has been established.
    #[default]
    Anonymous,
    /// Identity derived from a distinguished name present on a client certificate.
    DistinguishedName(String),
    /// Identity derived from a device certificate.
    Device {
        device_id: String,
        distinguished_name: String,
    },
    /// Identity derived from a bootstrap token.
    BootstrapToken(String),
    /// Identity derived from a JWT bearer token.
    Jwt {
        subject: String,
        issuer: Option<String>,
    },
}

#[derive(Clone)]
struct CertificateClassification {
    subject: AuthSubject,
    scope: AuthScope,
}

fn parse_common_name_from_subject(subject: &str) -> Option<String> {
    subject.split(',').find_map(|component| {
        let trimmed = component.trim();
        trimmed.strip_prefix("CN=").map(|value| value.to_string())
    })
}

fn classify_certificate_identity(distinguished_name: &str) -> CertificateClassification {
    if let Some(common_name) = parse_common_name_from_subject(distinguished_name) {
        if let Some(device_id) = common_name.strip_prefix("device:") {
            if !device_id.is_empty() {
                return CertificateClassification {
                    subject: AuthSubject::Device {
                        device_id: device_id.to_string(),
                        distinguished_name: distinguished_name.to_string(),
                    },
                    scope: AuthScope::Device,
                };
            }
        }
    }

    CertificateClassification {
        subject: AuthSubject::DistinguishedName(distinguished_name.to_string()),
        scope: AuthScope::Certificate,
    }
}

/// Client certificate details extracted during the TLS handshake.
#[derive(Clone, Debug)]
pub struct ClientCertificate {
    subject: String,
}

impl ClientCertificate {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn new(subject: String) -> Self {
        Self { subject }
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }

    pub fn from_x509(certificate: &X509) -> Result<Self, ErrorStack> {
        let subject = format_distinguished_name(certificate.subject_name())?;
        Ok(Self { subject })
    }
}

fn format_distinguished_name(name: &X509NameRef) -> Result<String, ErrorStack> {
    let mut parts = Vec::new();

    for entry in name.entries() {
        let object = entry.object();
        let nid = object.nid();
        let field = nid.short_name().or_else(|_| nid.long_name())?.to_string();
        let value = entry
            .data()
            .as_utf8()
            .map(|cow| cow.to_string())
            .unwrap_or_else(|_| {
                let mut buffer = String::new();
                for byte in entry.data().as_slice() {
                    let _ = write!(&mut buffer, "{byte:02X}");
                }
                buffer
            });
        parts.push(format!("{field}={value}"));
    }

    Ok(parts.join(","))
}

fn redact_token(token: &str) -> String {
    if token.len() <= 4 {
        "***".to_string()
    } else if token.len() <= 8 {
        let prefix_len = usize::min(4, token.len());
        let prefix = &token[..prefix_len];
        format!("{}…", prefix)
    } else {
        let prefix = &token[..4];
        let suffix = &token[token.len() - 4..];
        format!("{}…{}", prefix, suffix)
    }
}

#[derive(Clone)]
pub(crate) struct BearerTokenClaims {
    claims: ServiceAccountClaims,
}

impl BearerTokenClaims {
    pub(crate) fn new(claims: ServiceAccountClaims) -> Self {
        Self { claims }
    }
}

impl fmt::Debug for BearerTokenClaims {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BearerTokenClaims")
            .field("subject", &self.claims.sub)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub enum AuthError {
    InvalidAuthorization(String),
    InvalidToken(String),
    TokenExpired,
    KeyLoad(String),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthError::InvalidAuthorization(msg) => {
                write!(f, "invalid authorization header: {msg}")
            }
            AuthError::InvalidToken(msg) => write!(f, "invalid token: {msg}"),
            AuthError::TokenExpired => write!(f, "token expired"),
            AuthError::KeyLoad(msg) => {
                write!(f, "failed to load token verification key: {msg}")
            }
        }
    }
}

impl std::error::Error for AuthError {}

fn load_decoding_key() -> Result<DecodingKey, AuthError> {
    let key = load_service_secret_key().map_err(|err| AuthError::KeyLoad(err.to_string()))?;
    let pem = key
        .public_key_to_pem()
        .map_err(|err| AuthError::KeyLoad(err.to_string()))?;
    DecodingKey::from_rsa_pem(&pem).map_err(|err| AuthError::KeyLoad(err.to_string()))
}

fn extract_bearer_token(headers: &HeaderMap) -> Result<Option<String>, AuthError> {
    let header_value = match headers.get(AUTHORIZATION) {
        Some(value) => value,
        None => return Ok(None),
    };

    let value = header_value
        .to_str()
        .map_err(|_| AuthError::InvalidAuthorization("invalid header encoding".to_string()))?;

    let token = value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .ok_or_else(|| {
            AuthError::InvalidAuthorization(
                "Authorization header must be a Bearer token".to_string(),
            )
        })?
        .trim();

    if token.is_empty() {
        return Err(AuthError::InvalidAuthorization(
            "Authorization header must include a token".to_string(),
        ));
    }

    Ok(Some(token.to_string()))
}

fn is_jwt_format(token: &str) -> bool {
    let mut segments = token.split('.');
    match (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) {
        (Some(header), Some(payload), Some(signature), None) => {
            !header.is_empty() && !payload.is_empty() && !signature.is_empty()
        }
        _ => false,
    }
}

fn is_bootstrap_token_format(token: &str) -> bool {
    let mut segments = token.split('.');
    match (segments.next(), segments.next(), segments.next()) {
        (Some(id), Some(secret), None) => !id.is_empty() && !secret.is_empty(),
        _ => false,
    }
}

fn decode_bearer_token(token: &str) -> Result<ServiceAccountClaims, AuthError> {
    let key = load_decoding_key()?;
    let mut validation = Validation::new(Algorithm::RS256);
    validation.required_spec_claims.insert("sub".to_string());
    validation.required_spec_claims.insert("iss".to_string());

    decode::<ServiceAccountClaims>(token, &key, &validation)
        .map(|data| data.claims)
        .map_err(|err| match err.into_kind() {
            ErrorKind::ExpiredSignature => AuthError::TokenExpired,
            other => AuthError::InvalidToken(format!("{other:?}")),
        })
}

/// Per-request authentication context that flows alongside request handling.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AuthContext {
    subject: AuthSubject,
    scope: AuthScope,
}

impl AuthContext {
    /// Returns the authenticated subject associated with the request.
    pub fn subject(&self) -> &AuthSubject {
        &self.subject
    }

    /// Returns the scope granted to the authenticated subject.
    pub fn scope(&self) -> &AuthScope {
        &self.scope
    }

    /// Updates the subject in-place.
    pub fn set_subject(&mut self, subject: AuthSubject) {
        self.subject = subject;
    }

    /// Updates the scope in-place.
    pub fn set_scope(&mut self, scope: AuthScope) {
        self.scope = scope;
    }
}

/// Tower layer that guarantees an [`AuthContext`] is attached to every request.
#[derive(Clone)]
pub struct AuthLayer {
    bootstrap_tokens: Arc<BootstrapTokenService>,
}

impl AuthLayer {
    /// Construct a new [`AuthLayer`].
    pub fn new() -> Self {
        Self {
            bootstrap_tokens: Arc::new(BootstrapTokenService::new()),
        }
    }
}

impl Default for AuthLayer {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct AuthService<S> {
    inner: S,
    bootstrap_tokens: Arc<BootstrapTokenService>,
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthService {
            inner,
            bootstrap_tokens: Arc::clone(&self.bootstrap_tokens),
        }
    }
}

impl<S, ReqBody> Service<Request<ReqBody>> for AuthService<S>
where
    S: Service<Request<ReqBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<ReqBody>) -> S::Future {
        let certificate = request.extensions().get::<ClientCertificate>().cloned();
        let authorization_token = match extract_bearer_token(request.headers()) {
            Ok(token) => token,
            Err(err) => {
                let error_text = err.to_string();
                log_warn(
                    AUTH_LOG_COMPONENT,
                    "Rejected Authorization header",
                    &[("error", error_text.as_str())],
                );
                None
            }
        };

        if request.extensions().get::<AuthContext>().is_none() {
            request.extensions_mut().insert(AuthContext::default());
        }

        if let Some(certificate) = certificate {
            let classification = classify_certificate_identity(certificate.subject());
            if let Some(context) = request.extensions_mut().get_mut::<AuthContext>() {
                if matches!(context.subject(), AuthSubject::Anonymous) {
                    context.set_subject(classification.subject.clone());
                }
                if matches!(context.scope(), AuthScope::Unauthenticated) {
                    context.set_scope(classification.scope.clone());
                }
            }
        }

        if let Some(token) = authorization_token {
            if is_jwt_format(&token) {
                match decode_bearer_token(&token) {
                    Ok(claims) => {
                        request
                            .extensions_mut()
                            .insert(BearerTokenClaims::new(claims.clone()));
                        if let Some(context) = request.extensions_mut().get_mut::<AuthContext>() {
                            if matches!(context.subject(), AuthSubject::Anonymous) {
                                context.set_subject(AuthSubject::Jwt {
                                    subject: claims.sub.clone(),
                                    issuer: Some(claims.iss.clone()),
                                });
                            }
                            if matches!(context.scope(), AuthScope::Unauthenticated) {
                                context.set_scope(AuthScope::Jwt(claims.scope.clone()));
                            }
                        }
                    }
                    Err(err) => {
                        let error_text = err.to_string();
                        log_warn(
                            AUTH_LOG_COMPONENT,
                            "Rejected bearer token",
                            &[("error", error_text.as_str())],
                        );
                    }
                }
            } else if is_bootstrap_token_format(&token) {
                match self.bootstrap_tokens.lookup(&token) {
                    Ok(Some(bootstrap)) => {
                        let mut owned = vec![
                            ("token", redact_token(&bootstrap.token)),
                            ("subject", bootstrap.subject.clone()),
                        ];
                        if let Some(cluster) = bootstrap.cluster.as_deref() {
                            owned.push(("cluster", cluster.to_string()));
                        }
                        metrics::record_bootstrap_token_attempt(BootstrapAuthOutcome::Success);
                        let metadata: Vec<(&str, &str)> = owned
                            .iter()
                            .map(|(key, value)| (*key, value.as_str()))
                            .collect();
                        log_info(
                            AUTH_LOG_COMPONENT,
                            "Bootstrap token authenticated",
                            &metadata,
                        );
                        request.extensions_mut().insert(bootstrap.clone());
                        if let Some(context) = request.extensions_mut().get_mut::<AuthContext>() {
                            context.set_subject(AuthSubject::BootstrapToken(
                                bootstrap.subject.clone(),
                            ));
                            if matches!(context.scope(), AuthScope::Unauthenticated) {
                                context.set_scope(AuthScope::Bootstrap);
                            }
                        }
                    }
                    Ok(None) => {
                        metrics::record_bootstrap_token_attempt(BootstrapAuthOutcome::NotFound);
                        let owned = [("token", redact_token(&token))];
                        let metadata: Vec<(&str, &str)> = owned
                            .iter()
                            .map(|(key, value)| (*key, value.as_str()))
                            .collect();
                        log_warn(AUTH_LOG_COMPONENT, "Bootstrap token not found", &metadata);
                    }
                    Err(BootstrapTokenError::Malformed(message)) => {
                        metrics::record_bootstrap_token_attempt(BootstrapAuthOutcome::Invalid);
                        let owned = [("token", redact_token(&token)), ("error", message)];
                        let metadata: Vec<(&str, &str)> = owned
                            .iter()
                            .map(|(key, value)| (*key, value.as_str()))
                            .collect();
                        log_warn(AUTH_LOG_COMPONENT, "Bootstrap token rejected", &metadata);
                    }
                    Err(BootstrapTokenError::Storage(message)) => {
                        metrics::record_bootstrap_token_attempt(BootstrapAuthOutcome::Error);
                        let owned = [("token", redact_token(&token)), ("error", message)];
                        let metadata: Vec<(&str, &str)> = owned
                            .iter()
                            .map(|(key, value)| (*key, value.as_str()))
                            .collect();
                        log_error(
                            AUTH_LOG_COMPONENT,
                            "Bootstrap token lookup failed",
                            &metadata,
                        );
                    }
                }
            } else {
                metrics::record_bootstrap_token_attempt(BootstrapAuthOutcome::Invalid);
                let owned = [("token", redact_token(&token))];
                let metadata: Vec<(&str, &str)> = owned
                    .iter()
                    .map(|(key, value)| (*key, value.as_str()))
                    .collect();
                log_warn(
                    AUTH_LOG_COMPONENT,
                    "Authorization token is not a valid JWT or bootstrap token",
                    &metadata,
                );
            }
        }

        self.inner.call(request)
    }
}

/// Error returned when a request is missing the expected [`AuthContext`] extension.
#[derive(Debug)]
pub struct MissingAuthContext;

impl IntoResponse for MissingAuthContext {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "missing auth context extension",
        )
            .into_response()
    }
}

impl<S> FromRequestParts<S> for AuthContext
where
    S: Send + Sync,
{
    type Rejection = MissingAuthContext;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> impl future::Future<Output = Result<Self, Self::Rejection>> + Send {
        future::ready(
            parts
                .extensions
                .get::<AuthContext>()
                .cloned()
                .ok_or(MissingAuthContext),
        )
    }
}

/// Convenience methods for retrieving the authentication context from a request.
pub trait RequestAuthExt {
    /// Returns a reference to the authentication context when present.
    fn auth_context(&self) -> Option<&AuthContext>;
}

impl<B> RequestAuthExt for Request<B> {
    fn auth_context(&self) -> Option<&AuthContext> {
        self.extensions().get::<AuthContext>()
    }
}

/// Middleware that rejects requests without an authenticated subject or required scope.
pub async fn require_authenticated_subject(request: Request<Body>, next: Next) -> Response {
    let matched_path = request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let method = request.method().clone();
    let method_text = method.as_str().to_string();
    let context = request.auth_context().cloned();

    let (missing_context, requires_auth) = match context.as_ref() {
        Some(context) => (false, matches!(context.subject(), AuthSubject::Anonymous)),
        None => (true, true),
    };

    if requires_auth {
        if missing_context {
            log_error(
                AUTH_LOG_COMPONENT,
                "Rejected request missing authentication context",
                &[
                    ("method", method_text.as_str()),
                    ("path", matched_path.as_str()),
                ],
            );
            let body = Json(ErrorBody {
                error: "authentication context missing".to_string(),
            });
            return (StatusCode::UNAUTHORIZED, body).into_response();
        } else {
            log_warn(
                AUTH_LOG_COMPONENT,
                "Rejected request without authenticated subject",
                &[
                    ("method", method_text.as_str()),
                    ("path", matched_path.as_str()),
                ],
            );
            let body = Json(ErrorBody {
                error: "authentication required".to_string(),
            });
            return (StatusCode::UNAUTHORIZED, body).into_response();
        }
    }

    if let Some(requirements) = resolve_security_requirements(&method, matched_path.as_str()) {
        if !requirements.is_empty() {
            let scope = context
                .as_ref()
                .map(|ctx| ctx.scope().clone())
                .unwrap_or(AuthScope::Unauthenticated);
            if !scope_authorized(&scope, &requirements) {
                log_warn(
                    AUTH_LOG_COMPONENT,
                    "Rejected request without required authorization scope",
                    &[
                        ("method", method_text.as_str()),
                        ("path", matched_path.as_str()),
                    ],
                );
                let body = Json(ErrorBody {
                    error: "insufficient scope".to_string(),
                });
                return (StatusCode::FORBIDDEN, body).into_response();
            }
        }
    }

    next.run(request).await
}

fn resolve_security_requirements(
    method: &Method,
    matched_path: &str,
) -> Option<Vec<SecurityRequirement>> {
    let doc = handlers::openapi_document()?;
    let spec = doc.as_ref();
    let paths = spec.get("paths")?.as_object()?;
    let path_item = paths.get(matched_path)?.as_object()?;
    let method_key = map_http_method(method)?;
    if let Some(operation) = path_item.get(method_key)?.as_object() {
        if let Some(requirements) = operation.get("security") {
            if let Some(parsed) = parse_security_requirements(requirements) {
                return Some(parsed);
            } else {
                return None;
            }
        }
    }

    spec.get("security").and_then(parse_security_requirements)
}

fn map_http_method(method: &Method) -> Option<&'static str> {
    match *method {
        Method::GET => Some("get"),
        Method::POST => Some("post"),
        Method::PUT => Some("put"),
        Method::DELETE => Some("delete"),
        Method::OPTIONS => Some("options"),
        Method::HEAD => Some("head"),
        Method::PATCH => Some("patch"),
        Method::TRACE => Some("trace"),
        _ => None,
    }
}

fn parse_security_requirements(value: &Value) -> Option<Vec<SecurityRequirement>> {
    let array = value.as_array()?;
    if array.is_empty() {
        return None;
    }

    let mut requirements = Vec::with_capacity(array.len());
    for entry in array {
        let object = entry.as_object()?;
        let mut requirement = SecurityRequirement::new();
        for (scheme, scopes_value) in object {
            let scopes_array = scopes_value.as_array()?;
            let mut scopes = Vec::with_capacity(scopes_array.len());
            for scope in scopes_array {
                scopes.push(scope.as_str()?.to_string());
            }
            requirement.insert(scheme.clone(), scopes);
        }
        requirements.push(requirement);
    }

    if requirements.is_empty() {
        None
    } else {
        Some(requirements)
    }
}

fn scope_authorized(scope: &AuthScope, requirements: &[SecurityRequirement]) -> bool {
    if matches!(scope, AuthScope::Certificate) {
        return true;
    }

    if requirements.is_empty() {
        return true;
    }

    requirements
        .iter()
        .any(|requirement| requirement_satisfied(scope, requirement))
}

fn requirement_satisfied(scope: &AuthScope, requirement: &SecurityRequirement) -> bool {
    match scope {
        AuthScope::Certificate => return true,
        AuthScope::Unauthenticated => return false,
        _ => {}
    }

    let pairs = match parse_security_requirement(requirement) {
        Some(pairs) if !pairs.is_empty() => pairs,
        _ => return true,
    };

    for (scheme, scopes) in pairs {
        match scope {
            AuthScope::Bootstrap => {
                if scheme != SECURITY_SCHEME_BOOTSTRAP {
                    return false;
                }
                if !bootstrap_scopes_satisfied(&scopes) {
                    return false;
                }
            }
            AuthScope::Jwt(granted) => {
                if scheme != SECURITY_SCHEME_BEARER {
                    return false;
                }
                if !jwt_scopes_cover(granted, &scopes) {
                    return false;
                }
            }
            AuthScope::Device => {
                if scheme != SECURITY_SCHEME_DEVICE {
                    return false;
                }
                if !scopes.is_empty() {
                    return false;
                }
            }
            AuthScope::Unauthenticated => return false,
            AuthScope::Certificate => unreachable!(),
        }
    }

    true
}

fn parse_security_requirement(
    requirement: &SecurityRequirement,
) -> Option<Vec<(String, Vec<String>)>> {
    let mut pairs = Vec::with_capacity(requirement.len());
    for (scheme, scopes) in requirement {
        pairs.push((scheme.clone(), scopes.clone()));
    }
    Some(pairs)
}

fn bootstrap_scopes_satisfied(scopes: &[String]) -> bool {
    scopes.is_empty()
        || scopes
            .iter()
            .all(|scope| scope == "bootstrap" || scope == "*")
}

fn jwt_scopes_cover(granted: &[String], required: &[String]) -> bool {
    if required.is_empty() {
        return true;
    }

    required.iter().all(|required_scope| {
        required_scope == "*"
            || granted
                .iter()
                .any(|granted_scope| granted_scope == required_scope || granted_scope == "*")
    })
}

#[cfg(all(test, feature = "openapi"))]
mod tests {
    use super::*;
    use crate::nanocloud::test_support::keyspace_lock;
    use axum::body::Body;
    use axum::http::header::AUTHORIZATION;
    use axum::http::HeaderValue;
    use axum::middleware;
    use axum::routing::get;
    use axum::Router;
    use chrono::{Duration as ChronoDuration, Utc};
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde_json::json;
    use serial_test::serial;
    use std::convert::Infallible;
    use std::env;
    use std::fs;
    use std::path::Path;
    use std::sync::{Mutex, Once, OnceLock};
    use std::time::Duration;
    use tower::{service_fn, ServiceExt};
    use utoipa::openapi::path::{HttpMethod, OperationBuilder, PathItem, PathsBuilder};
    use utoipa::openapi::response::ResponseBuilder;
    use utoipa::openapi::security::SecurityRequirement;
    use utoipa::openapi::{InfoBuilder, OpenApiBuilder, ResponsesBuilder};

    use crate::nanocloud::util::security::{
        clear_asset_caches, load_service_secret_key, EncryptionKey, SecureAssets,
    };
    use crate::nanocloud::util::Keyspace;
    use crate::nanocloud::Config;
    use tempfile::tempdir;

    const SECURITY_SCHEME_CERTIFICATE: &str = "NanocloudCertificate";

    fn test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn install_openapi_doc() {
        static OPENAPI_INIT: Once = Once::new();
        OPENAPI_INIT.call_once(|| {
            let ok_response = ResponseBuilder::new().description("Success").build();
            let responses = ResponsesBuilder::new().response("200", ok_response).build();

            let bootstrap_requirement =
                SecurityRequirement::new(SECURITY_SCHEME_BOOTSTRAP, Vec::<String>::new());
            let bootstrap_operation = OperationBuilder::new()
                .responses(responses.clone())
                .security(bootstrap_requirement)
                .build();

            let bearer_requirement =
                SecurityRequirement::new(SECURITY_SCHEME_BEARER, ["services.read"]);
            let bearer_operation = OperationBuilder::new()
                .responses(responses.clone())
                .security(bearer_requirement)
                .build();

            let certificate_requirement =
                SecurityRequirement::new(SECURITY_SCHEME_CERTIFICATE, Vec::<String>::new());
            let certificate_operation = OperationBuilder::new()
                .responses(responses)
                .security(certificate_requirement)
                .build();

            let paths = PathsBuilder::new()
                .path(
                    "/secure",
                    PathItem::new(HttpMethod::Get, bootstrap_operation),
                )
                .path("/token", PathItem::new(HttpMethod::Get, bearer_operation))
                .path(
                    "/cert",
                    PathItem::new(HttpMethod::Get, certificate_operation),
                )
                .build();

            let doc = OpenApiBuilder::new()
                .info(InfoBuilder::new().title("Test API").version("1.0").build())
                .paths(paths)
                .build();

            let doc_json =
                serde_json::to_value(&doc).expect("failed to serialize OpenAPI test fixture");
            handlers::set_openapi_doc_for_testing(doc_json);
        });
    }

    struct EnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvGuard {
        fn set<P: AsRef<Path>>(key: &'static str, value: P) -> Self {
            let previous = env::var(key).ok();
            let value_string = value.as_ref().to_string_lossy().into_owned();
            env::set_var(key, value_string);
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(prev) = self.previous.as_ref() {
                env::set_var(self.key, prev);
            } else {
                env::remove_var(self.key);
            }
        }
    }

    #[tokio::test]
    async fn inserts_default_auth_context() {
        let layer = AuthLayer::new();
        let svc = layer.layer(service_fn(|req: Request<()>| async move {
            assert!(req.extensions().get::<AuthContext>().is_some());
            Ok::<_, Infallible>(())
        }));

        svc.oneshot(Request::new(())).await.unwrap();
    }

    #[tokio::test]
    async fn preserves_existing_auth_context() {
        let layer = AuthLayer::new();
        let custom_context = AuthContext {
            subject: AuthSubject::BootstrapToken("bootstrap-token".to_string()),
            scope: AuthScope::Bootstrap,
        };

        let svc = layer.layer(service_fn(|req: Request<()>| async move {
            let context = req.extensions().get::<AuthContext>().cloned().unwrap();
            Ok::<_, Infallible>(context)
        }));

        let mut request = Request::new(());
        request.extensions_mut().insert(custom_context.clone());

        let observed_context = svc.oneshot(request).await.unwrap();
        assert_eq!(observed_context, custom_context);
    }

    #[tokio::test]
    async fn populates_context_from_client_certificate() {
        let layer = AuthLayer::new();
        let svc = layer.layer(service_fn(|req: Request<()>| async move {
            Ok::<_, Infallible>(
                req.extensions()
                    .get::<AuthContext>()
                    .cloned()
                    .expect("auth context missing"),
            )
        }));

        let mut request = Request::new(());
        request
            .extensions_mut()
            .insert(ClientCertificate::new("CN=example".to_string()));

        let context = svc.oneshot(request).await.unwrap();
        assert_eq!(
            context,
            AuthContext {
                subject: AuthSubject::DistinguishedName("CN=example".to_string()),
                scope: AuthScope::Certificate,
            }
        );
    }

    #[tokio::test]
    async fn classifies_device_certificate_scope() {
        let layer = AuthLayer::new();
        let svc = layer.layer(service_fn(|req: Request<()>| async move {
            Ok::<_, Infallible>(
                req.extensions()
                    .get::<AuthContext>()
                    .cloned()
                    .expect("auth context missing"),
            )
        }));

        let mut request = Request::new(());
        request
            .extensions_mut()
            .insert(ClientCertificate::new("CN=device:abc123".to_string()));

        let context = svc.oneshot(request).await.unwrap();
        assert_eq!(
            context,
            AuthContext {
                subject: AuthSubject::Device {
                    device_id: "abc123".to_string(),
                    distinguished_name: "CN=device:abc123".to_string(),
                },
                scope: AuthScope::Device,
            }
        );
    }

    #[tokio::test]
    async fn populates_context_from_bearer_token() {
        let layer = AuthLayer::new();
        let svc = layer.layer(service_fn(|req: Request<()>| async move {
            let context = req
                .extensions()
                .get::<AuthContext>()
                .cloned()
                .expect("auth context missing");
            let claims = req.extensions().get::<BearerTokenClaims>().cloned();
            Ok::<_, Infallible>((context, claims))
        }));

        let key = match load_service_secret_key() {
            Ok(key) => key,
            Err(_) => {
                // Environment does not expose the service key; skip validation path.
                return;
            }
        };
        let pem = match key.private_key_to_pem_pkcs8() {
            Ok(pem) => pem,
            Err(_) => return,
        };
        let encoding_key = match EncodingKey::from_rsa_pem(&pem) {
            Ok(key) => key,
            Err(_) => return,
        };

        let now = Utc::now();
        let claims = ServiceAccountClaims {
            iss: "nanocloud/test".to_string(),
            sub: "user@example.com".to_string(),
            aud: vec!["nanocloud".to_string()],
            scope: vec!["read".to_string(), "write".to_string()],
            iat: now.timestamp(),
            exp: (now + ChronoDuration::minutes(5)).timestamp(),
            jti: "test-jti".to_string(),
            cluster: None,
        };

        let token =
            encode(&Header::new(Algorithm::RS256), &claims, &encoding_key).expect("encode token");

        let mut request = Request::new(());
        request.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).expect("header value"),
        );

        let (context, stored_claims) = svc.oneshot(request).await.unwrap();
        match context.subject() {
            AuthSubject::Jwt { subject, issuer } => {
                assert_eq!(subject, "user@example.com");
                assert_eq!(issuer.as_deref(), Some("nanocloud/test"));
            }
            other => panic!("unexpected subject: {other:?}"),
        }

        match context.scope() {
            AuthScope::Jwt(scopes) => {
                assert_eq!(scopes, &vec!["read".to_string(), "write".to_string()])
            }
            other => panic!("unexpected scope: {other:?}"),
        }

        let stored_claims = stored_claims.expect("claims missing");
        assert_eq!(stored_claims.claims.sub, "user@example.com");
    }

    #[tokio::test]
    async fn bearer_token_does_not_override_certificate_identity() {
        let layer = AuthLayer::new();
        let svc = layer.layer(service_fn(|req: Request<()>| async move {
            let context = req
                .extensions()
                .get::<AuthContext>()
                .cloned()
                .expect("auth context missing");
            Ok::<_, Infallible>(context)
        }));

        let key = match load_service_secret_key() {
            Ok(key) => key,
            Err(_) => return,
        };
        let pem = match key.private_key_to_pem_pkcs8() {
            Ok(pem) => pem,
            Err(_) => return,
        };
        let encoding_key = match EncodingKey::from_rsa_pem(&pem) {
            Ok(key) => key,
            Err(_) => return,
        };

        let now = Utc::now();
        let claims = ServiceAccountClaims {
            iss: "nanocloud/test".to_string(),
            sub: "user@example.com".to_string(),
            aud: vec!["nanocloud".to_string()],
            scope: vec!["read".to_string()],
            iat: now.timestamp(),
            exp: (now + ChronoDuration::minutes(5)).timestamp(),
            jti: "test-jti".to_string(),
            cluster: None,
        };

        let token = match encode(&Header::new(Algorithm::RS256), &claims, &encoding_key) {
            Ok(token) => token,
            Err(_) => return,
        };

        let mut request = Request::new(());
        request
            .extensions_mut()
            .insert(ClientCertificate::new("CN=example".to_string()));
        request.headers_mut().insert(
            AUTHORIZATION,
            match HeaderValue::from_str(&format!("Bearer {token}")) {
                Ok(value) => value,
                Err(_) => return,
            },
        );

        let context = svc.oneshot(request).await.unwrap();
        assert!(matches!(
            context.subject(),
            AuthSubject::DistinguishedName(subject) if subject == "CN=example"
        ));
        assert!(matches!(context.scope(), AuthScope::Certificate));
    }

    #[tokio::test]
    #[serial]
    async fn populates_context_from_bootstrap_token() {
        let guard = test_lock().lock().unwrap();
        let temp_dir = tempdir().expect("tempdir");
        let base = temp_dir.path().to_path_buf();
        let keyspace_dir = base.join("keyspace");
        let lock_dir = base.join("lock");
        let secure_dir = base.join("secure");
        fs::create_dir_all(&keyspace_dir).expect("keyspace dir");
        fs::create_dir_all(&lock_dir).expect("lock dir");
        fs::create_dir_all(&secure_dir).expect("secure dir");
        let lock_file = lock_dir.join("nanocloud.lock");
        fs::File::create(&lock_file).expect("lock file");
        let _keyspace_env = EnvGuard::set("NANOCLOUD_KEYSPACE", keyspace_dir.clone());
        let _lock_guard = EnvGuard::set("NANOCLOUD_LOCK_FILE", lock_file);
        let _secure_guard = EnvGuard::set("NANOCLOUD_SECURE_ASSETS", secure_dir.clone());
        let keyspace_guard = keyspace_lock().lock();

        let token_id = "bootstrap";
        let token_secret = "authsecret";
        clear_asset_caches();
        SecureAssets::generate(&secure_dir, false).expect("generate secure assets");
        assert!(
            secure_dir.join("secret.key").exists(),
            "secret key missing after generation"
        );
        let keyspace = Keyspace::new("tokens");
        let encryption_key = EncryptionKey::new(None);
        let wrapped_key = encryption_key.wrap().expect("wrap key");
        let encrypted_secret = encryption_key
            .encrypt(token_secret.as_bytes())
            .expect("encrypt secret");
        let grant = json!({
            "user": "bootstrap-user",
            "cluster": "demo",
            "secret": {
                "key": wrapped_key,
                "ciphertext": encrypted_secret,
            }
        });
        let grant_text = grant.to_string();
        keyspace
            .put_with_ttl(
                &format!("/v1/token/{token_id}"),
                &grant_text,
                Duration::from_secs(60),
            )
            .expect("store bootstrap token");

        let secure_root = std::env::var("NANOCLOUD_SECURE_ASSETS").expect("secure assets path set");
        assert_eq!(
            Config::SecureAssets.get_path(),
            secure_dir,
            "secure assets config path should point at test directory"
        );
        assert!(
            Path::new(&secure_root).join("secret.key").exists(),
            "secure assets secret key must be created under {}",
            secure_root
        );

        drop(keyspace_guard);
        drop(guard);

        let layer = AuthLayer::new();
        let svc = layer.layer(service_fn(|req: Request<()>| async move {
            let context = req
                .extensions()
                .get::<AuthContext>()
                .cloned()
                .expect("auth context missing");
            let bootstrap = req.extensions().get::<bootstrap::BootstrapToken>().cloned();
            Ok::<_, Infallible>((context, bootstrap))
        }));

        let mut request = Request::new(());
        request.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token_id}.{token_secret}"))
                .expect("header value"),
        );

        let (context, bootstrap) = svc.oneshot(request).await.unwrap();

        assert!(matches!(context.scope(), AuthScope::Bootstrap));
        match context.subject() {
            AuthSubject::BootstrapToken(subject) => assert_eq!(subject, "bootstrap-user"),
            other => panic!("unexpected subject: {other:?}"),
        }

        let bootstrap = bootstrap.expect("bootstrap token missing");
        assert_eq!(bootstrap.subject, "bootstrap-user");
        assert_eq!(bootstrap.cluster.as_deref(), Some("demo"));

        clear_asset_caches();
    }

    #[tokio::test]
    async fn guard_rejects_anonymous_requests() {
        install_openapi_doc();
        let router = Router::new()
            .route("/secure", get(|| async { StatusCode::OK }))
            .route("/token", get(|| async { StatusCode::OK }))
            .route("/cert", get(|| async { StatusCode::OK }))
            .layer(middleware::from_fn(require_authenticated_subject))
            .layer(AuthLayer::new());

        let request = Request::builder()
            .uri("/secure")
            .body(Body::empty())
            .expect("request");

        let response = router.oneshot(request).await.expect("response");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn guard_allows_authenticated_requests() {
        install_openapi_doc();
        let router = Router::new()
            .route("/secure", get(|| async { StatusCode::OK }))
            .route("/token", get(|| async { StatusCode::OK }))
            .layer(middleware::from_fn(require_authenticated_subject))
            .layer(AuthLayer::new());

        let mut request = Request::builder()
            .uri("/secure")
            .body(Body::empty())
            .expect("request");
        request.extensions_mut().insert(AuthContext {
            subject: AuthSubject::BootstrapToken("bootstrap-user".to_string()),
            scope: AuthScope::Bootstrap,
        });

        let response = router.oneshot(request).await.expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn guard_rejects_when_scope_missing() {
        install_openapi_doc();
        let router = Router::new()
            .route("/secure", get(|| async { StatusCode::OK }))
            .route("/token", get(|| async { StatusCode::OK }))
            .layer(middleware::from_fn(require_authenticated_subject))
            .layer(AuthLayer::new());

        let mut request = Request::builder()
            .uri("/cert")
            .body(Body::empty())
            .expect("request");
        request.extensions_mut().insert(AuthContext {
            subject: AuthSubject::BootstrapToken("bootstrap-user".to_string()),
            scope: AuthScope::Bootstrap,
        });

        let response = router.oneshot(request).await.expect("response");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn guard_allows_jwt_with_required_scope() {
        install_openapi_doc();
        let router = Router::new()
            .route("/secure", get(|| async { StatusCode::OK }))
            .route("/token", get(|| async { StatusCode::OK }))
            .layer(middleware::from_fn(require_authenticated_subject))
            .layer(AuthLayer::new());

        let mut request = Request::builder()
            .uri("/token")
            .body(Body::empty())
            .expect("request");
        request.extensions_mut().insert(AuthContext {
            subject: AuthSubject::Jwt {
                subject: "user@example.com".to_string(),
                issuer: None,
            },
            scope: AuthScope::Jwt(vec!["services.read".to_string()]),
        });

        let response = router.oneshot(request).await.expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn guard_allows_certificate_for_any_requirement() {
        install_openapi_doc();
        let router = Router::new()
            .route("/secure", get(|| async { StatusCode::OK }))
            .route("/token", get(|| async { StatusCode::OK }))
            .layer(middleware::from_fn(require_authenticated_subject))
            .layer(AuthLayer::new());

        let mut request = Request::builder()
            .uri("/token")
            .body(Body::empty())
            .expect("request");
        request.extensions_mut().insert(AuthContext {
            subject: AuthSubject::DistinguishedName("CN=test".to_string()),
            scope: AuthScope::Certificate,
        });

        let response = router.oneshot(request).await.expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }
}
