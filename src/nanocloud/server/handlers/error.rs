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

use std::error::Error;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

use crate::nanocloud::api::types::{ApplyConflict, ErrorBody};

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
    reason: Option<String>,
    conflicts: Option<Vec<ApplyConflict>>,
}

impl ApiError {
    pub(crate) fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self::with_reason(status, default_reason(status), message)
    }

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

    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    pub(crate) fn map_container_error(err: Box<dyn Error + Send + Sync>) -> Self {
        let message = err.to_string();
        if message.contains("not found") {
            Self::new(StatusCode::NOT_FOUND, message)
        } else {
            Self::new(StatusCode::INTERNAL_SERVER_ERROR, message)
        }
    }

    pub(crate) fn internal_error(err: Box<dyn Error + Send + Sync>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
    }

    pub(crate) fn internal_message(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, message)
    }

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
}

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
        StatusCode::CONFLICT => "Conflict",
        StatusCode::GONE => "Gone",
        StatusCode::UNPROCESSABLE_ENTITY => "Invalid",
        _ if status.is_client_error() => "Invalid",
        _ if status.is_server_error() => "InternalError",
        _ => "Unknown",
    }
}
