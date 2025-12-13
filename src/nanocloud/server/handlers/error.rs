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

//! Centralized HTTP error mapping and response formatting.
//!
//! This module provides a unified [`ApiError`] type that all handlers should use
//! for error responses. It ensures consistent Kubernetes-compatible Status responses
//! across the API surface.
//!
//! # Usage
//!
//! ```ignore
//! use crate::nanocloud::server::handlers::ApiError;
//!
//! // Simple error with automatic reason
//! let err = ApiError::bad_request("invalid selector");
//!
//! // Error with custom reason
//! let err = ApiError::with_reason(StatusCode::NOT_FOUND, "NotFound", "pod not found");
//!
//! ```

use std::error::Error;
use std::fmt;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

use crate::nanocloud::api::types::{ApplyConflict, ErrorBody};

/// Unified API error type for consistent HTTP error responses.
///
/// All handlers should return `Result<T, ApiError>` for error cases.
/// The error automatically formats as a Kubernetes-compatible Status response.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
    reason: Option<String>,
    conflicts: Option<Vec<ApplyConflict>>,
}

impl ApiError {
    /// Create a new error with automatic reason based on status code.
    pub(crate) fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self::with_reason(status, default_reason(status), message)
    }

    /// Create a new error with explicit reason.
    pub(crate) fn with_reason(
        status: StatusCode,
        reason: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status,
            message: message.into(),
            reason: Some(reason.into()),
            conflicts: None,
        }
    }

    /// Create a 400 Bad Request error.
    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    /// Create a 403 Forbidden error.
    pub(crate) fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, message)
    }

    /// Create a 409 Conflict error.
    pub(crate) fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, message)
    }

    /// Map a container runtime error to an appropriate HTTP status.
    pub(crate) fn map_container_error(err: Box<dyn Error + Send + Sync>) -> Self {
        let message = err.to_string();
        let lower = message.to_lowercase();
        if lower.contains("not found") || lower.contains("no such") {
            Self::new(StatusCode::NOT_FOUND, message)
        } else if lower.contains("permission denied") || lower.contains("access denied") {
            Self::forbidden(message)
        } else if lower.contains("already exists") {
            Self::conflict(message)
        } else {
            Self::new(StatusCode::INTERNAL_SERVER_ERROR, message)
        }
    }

    /// Create a 500 Internal Server Error from any error type.
    pub(crate) fn internal_error(err: Box<dyn Error + Send + Sync>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
    }

    /// Create a 500 Internal Server Error with a custom message.
    pub(crate) fn internal_message(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, message)
    }

    /// Create a 409 Conflict error with apply conflict details.
    pub(crate) fn conflict_with_details(
        message: impl Into<String>,
        conflicts: Vec<ApplyConflict>,
    ) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
            reason: Some("Conflict".to_string()),
            conflicts: Some(conflicts),
        }
    }

    /// Returns the HTTP status code for this error.
    #[allow(dead_code)]
    pub(crate) fn status(&self) -> StatusCode {
        self.status
    }

    /// Returns the error message.
    #[allow(dead_code)]
    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    /// Returns the reason code if set.
    #[allow(dead_code)]
    pub(crate) fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.status, self.message)
    }
}

impl Error for ApiError {}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(ErrorBody {
            api_version: "v1".to_string(),
            kind: "Status".to_string(),
            status: "Failure".to_string(),
            message: Some(self.message),
            reason: Some(
                self.reason
                    .unwrap_or_else(|| default_reason(self.status).to_string()),
            ),
            code: Some(self.status.as_u16()),
            conflicts: self.conflicts,
        });
        (self.status, body).into_response()
    }
}

fn default_reason(status: StatusCode) -> &'static str {
    match status {
        StatusCode::BAD_REQUEST => "BadRequest",
        StatusCode::UNAUTHORIZED => "Unauthorized",
        StatusCode::FORBIDDEN => "Forbidden",
        StatusCode::NOT_FOUND => "NotFound",
        StatusCode::METHOD_NOT_ALLOWED => "MethodNotAllowed",
        StatusCode::CONFLICT => "Conflict",
        StatusCode::GONE => "Gone",
        StatusCode::UNPROCESSABLE_ENTITY => "Invalid",
        StatusCode::TOO_MANY_REQUESTS => "TooManyRequests",
        StatusCode::SERVICE_UNAVAILABLE => "ServiceUnavailable",
        StatusCode::GATEWAY_TIMEOUT => "Timeout",
        _ if status.is_client_error() => "Invalid",
        _ if status.is_server_error() => "InternalError",
        _ => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bad_request_returns_400() {
        let err = ApiError::bad_request("invalid input");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(err.message(), "invalid input");
        assert_eq!(err.reason(), Some("BadRequest"));
    }

    #[test]
    fn map_container_error_detects_not_found() {
        let err: Box<dyn Error + Send + Sync> = "container not found".into();
        let api_err = ApiError::map_container_error(err);
        assert_eq!(api_err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn map_container_error_detects_permission_denied() {
        let err: Box<dyn Error + Send + Sync> = "permission denied".into();
        let api_err = ApiError::map_container_error(err);
        assert_eq!(api_err.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn map_container_error_detects_already_exists() {
        let err: Box<dyn Error + Send + Sync> = "container already exists".into();
        let api_err = ApiError::map_container_error(err);
        assert_eq!(api_err.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn default_reason_covers_common_statuses() {
        assert_eq!(default_reason(StatusCode::BAD_REQUEST), "BadRequest");
        assert_eq!(default_reason(StatusCode::UNAUTHORIZED), "Unauthorized");
        assert_eq!(default_reason(StatusCode::FORBIDDEN), "Forbidden");
        assert_eq!(default_reason(StatusCode::NOT_FOUND), "NotFound");
        assert_eq!(default_reason(StatusCode::CONFLICT), "Conflict");
        assert_eq!(default_reason(StatusCode::GONE), "Gone");
        assert_eq!(default_reason(StatusCode::TOO_MANY_REQUESTS), "TooManyRequests");
        assert_eq!(default_reason(StatusCode::SERVICE_UNAVAILABLE), "ServiceUnavailable");
        assert_eq!(default_reason(StatusCode::INTERNAL_SERVER_ERROR), "InternalError");
    }

    #[test]
    fn into_response_formats_kubernetes_status() {
        let err = ApiError::with_reason(
            StatusCode::NOT_FOUND,
            "NotFound",
            "Secret \"my-secret\" not found in namespace \"prod\"",
        );
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn display_implementation() {
        let err = ApiError::bad_request("test error");
        let display = format!("{}", err);
        assert!(display.contains("400"));
        assert!(display.contains("test error"));
    }

    // Tests for malformed request handling

    #[test]
    fn malformed_selector_produces_bad_request() {
        // When a malformed selector is detected, it should produce a 400 Bad Request
        let err = ApiError::bad_request("invalid labelSelector: missing operator");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().contains("labelSelector"));
    }

    #[test]
    fn malformed_json_payload_produces_bad_request() {
        // When JSON parsing fails, it should produce a 400 Bad Request
        let err = ApiError::bad_request("failed to parse request body: invalid JSON");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().contains("JSON"));
    }

    #[test]
    fn invalid_resource_name_produces_bad_request() {
        // Invalid resource names should produce 400 Bad Request
        let err = ApiError::bad_request("invalid resource name: 'MY-POD' contains uppercase");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().contains("resource name"));
    }

    #[test]
    fn invalid_namespace_produces_bad_request() {
        let err = ApiError::bad_request("invalid namespace: '-invalid' starts with hyphen");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().contains("namespace"));
    }

    #[test]
    fn missing_required_field_produces_bad_request() {
        let err = ApiError::bad_request("missing required field: metadata.name");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().contains("metadata.name"));
    }

    #[test]
    fn invalid_container_name_produces_bad_request() {
        let err = ApiError::bad_request("invalid container name: contains illegal characters");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().contains("container"));
    }

    #[test]
    fn error_preserves_full_message() {
        let long_message = "this is a very long error message that describes in detail what went wrong with the request including specifics about the field path, expected format, and actual value received";
        let err = ApiError::bad_request(long_message);
        assert_eq!(err.message(), long_message);
    }

    #[test]
    fn unprocessable_entity_for_validation_errors() {
        // When validation fails (but parsing succeeded), use 422
        let err = ApiError::with_reason(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid",
            "spec.replicas must be >= 0",
        );
        assert_eq!(err.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(err.reason(), Some("Invalid"));
    }

    #[test]
    fn error_can_be_cloned_for_logging() {
        let err = ApiError::bad_request("test");
        // Can format for logging
        let debug = format!("{:?}", err);
        let display = format!("{}", err);
        assert!(!debug.is_empty());
        assert!(!display.is_empty());
    }

    #[test]
    fn conflict_error_with_conflicts_field() {
        let conflicts = vec![
            ApplyConflict {
                path: "spec.replicas".to_string(),
                existing_manager: "kubectl".to_string(),
            },
        ];
        let err = ApiError::conflict_with_details("apply conflict", conflicts);
        assert_eq!(err.status(), StatusCode::CONFLICT);
        // conflicts are preserved in the error
        assert!(err.conflicts.is_some());
        assert_eq!(err.conflicts.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn error_reason_fallback() {
        // When no explicit reason, use default for status code
        let err = ApiError::new(StatusCode::BAD_REQUEST, "test");
        assert_eq!(err.reason(), Some("BadRequest"));

        let err = ApiError::new(StatusCode::NOT_FOUND, "not found");
        assert_eq!(err.reason(), Some("NotFound"));

        let err = ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "oops");
        assert_eq!(err.reason(), Some("InternalError"));
    }
}
