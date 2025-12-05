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
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::nanocloud::observability::{health, metrics};

const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4";

pub(super) async fn metrics() -> Response {
    match metrics::gather() {
        Ok(buffer) => {
            let mut response = Response::new(Body::from(buffer));
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static(PROMETHEUS_CONTENT_TYPE),
            );
            response
        }
        Err(err) => {
            metrics::record_telemetry_failure(
                metrics::TelemetryComponent::Metrics,
                metrics::TelemetryFailureKind::Exporter,
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to encode metrics: {err}"),
            )
                .into_response()
        }
    }
}

pub(super) async fn readiness() -> Response {
    health::readiness_response().await.into_response()
}

pub(super) async fn liveness() -> Response {
    health::liveness_response().await.into_response()
}

pub(super) async fn combined_health() -> Response {
    readiness().await
}
