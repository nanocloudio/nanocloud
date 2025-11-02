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

use std::io;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::nanocloud::api::types::LogQuery;
use crate::nanocloud::engine::container::resolve_container_id;
use crate::nanocloud::engine::log::{
    container_log_path, container_previous_log_path, spawn_log_stream, TailOptions,
};
use crate::nanocloud::logger::{log_error, log_info, log_warn};

use super::error::ApiError;

pub(super) async fn stream_service_logs(
    namespace: Option<String>,
    service: String,
    query: LogQuery,
) -> Result<Response, ApiError> {
    let LogQuery {
        follow,
        container,
        previous,
        tail_lines,
        since_seconds,
    } = query;

    let follow = follow.unwrap_or(false);
    let previous = previous.unwrap_or(false);
    let tail_lines_label = tail_lines
        .map(|value| value.to_string())
        .unwrap_or_else(|| "all".to_string());
    let since_label = since_seconds
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string());
    let since = since_seconds.map(|seconds| {
        let duration = Duration::from_secs(seconds);
        SystemTime::now()
            .checked_sub(duration)
            .unwrap_or(UNIX_EPOCH)
    });
    let tail_lines = tail_lines.map(|value| {
        let max = usize::MAX as u64;
        let clamped = value.min(max);
        clamped as usize
    });

    let namespace_label = namespace
        .clone()
        .filter(|ns| !ns.is_empty())
        .unwrap_or_else(|| "default".to_string());
    let requested_container = container.as_deref();
    let container_lookup = requested_container.unwrap_or(service.as_str());
    let container_id = resolve_container_id(namespace.as_deref(), container_lookup)
        .map_err(ApiError::map_container_error)?;
    let log_path = if previous {
        container_previous_log_path(&container_id)
    } else {
        container_log_path(&container_id)
    };

    if previous && !log_path.exists() {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("Previous log file not found for service '{}'.", service),
        ));
    }

    if !follow && !log_path.exists() {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("Log file not found for service '{}'.", service),
        ));
    }

    let options = TailOptions {
        follow,
        tail_lines,
        since,
        ..Default::default()
    };

    let (handle, mut stream) = spawn_log_stream(log_path, options);
    let (tx, rx) = mpsc::channel::<Result<Bytes, io::Error>>(64);

    let follow_label = if follow { "true" } else { "false" };
    let previous_label = if previous { "true" } else { "false" };
    log_info(
        "server_logs",
        "Streaming service logs",
        &[
            ("service", service.as_str()),
            ("namespace", namespace_label.as_str()),
            ("container_id", container_id.as_str()),
            ("follow", follow_label),
            ("container", container_lookup),
            ("previous", previous_label),
            ("tail_lines", tail_lines_label.as_str()),
            ("since_seconds", since_label.as_str()),
        ],
    );

    let service_label = service.clone();
    let namespace_clone = namespace_label.clone();
    let container_id_clone = container_id.clone();

    tokio::spawn(async move {
        let tail_handle = handle;
        while let Some(item) = stream.next().await {
            match item {
                Ok(entry) => {
                    let mut line = entry.message;
                    if !line.ends_with('\n') {
                        line.push('\n');
                    }
                    if tx.send(Ok(Bytes::from(line))).await.is_err() {
                        break;
                    }
                }
                Err(err) => {
                    let error_text = err.to_string();
                    log_error(
                        "server_logs",
                        "Log stream error",
                        &[
                            ("service", service_label.as_str()),
                            ("namespace", namespace_clone.as_str()),
                            ("container_id", container_id_clone.as_str()),
                            ("error", error_text.as_str()),
                        ],
                    );
                    break;
                }
            }
        }

        if let Err(err) = tail_handle.shutdown().await {
            let error_text = err.to_string();
            log_warn(
                "server_logs",
                "Log tail shutdown error",
                &[
                    ("service", service_label.as_str()),
                    ("namespace", namespace_clone.as_str()),
                    ("container_id", container_id_clone.as_str()),
                    ("error", error_text.as_str()),
                ],
            );
        }
    });

    let body = Body::from_stream(ReceiverStream::new(rx));
    let response = Response::builder()
        .header("content-type", "text/plain; charset=utf-8")
        .body(body)
        .map_err(|err| ApiError::internal_error(Box::new(err)))?;
    Ok(response)
}
