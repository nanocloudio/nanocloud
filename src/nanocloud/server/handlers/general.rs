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

use crate::nanocloud::util::security::{JsonTlsInfo, TlsInfo};
use axum::http::StatusCode;
use axum::Json;

use super::error::ApiError;
use crate::nanocloud::api::types::{AsyncStatus, CaRequest};

pub(super) async fn setup() -> Result<(StatusCode, Json<AsyncStatus>), ApiError> {
    Err(ApiError::new(
        StatusCode::NOT_IMPLEMENTED,
        "Setup is only available via the CLI",
    ))
}

pub(super) async fn issue_certificate(
    Json(payload): Json<CaRequest>,
) -> Result<Json<JsonTlsInfo>, ApiError> {
    if payload.common_name.is_empty() {
        return Err(ApiError::bad_request("common_name is required"));
    }
    let tls = TlsInfo::create(&payload.common_name, payload.additional.as_ref())
        .map_err(ApiError::internal_error)?
        .wrap();
    Ok(Json(tls))
}
