use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use axum::body::Body;
use axum::http::header::{CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use bytes::Bytes;
use futures_core::stream::Stream;
use tokio::fs::File;
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::ReaderStream;

use crate::nanocloud::engine::container::latest_backup_path;
use crate::nanocloud::engine::{
    get_streaming_backup, streaming_backup_enabled, SnapshotChunkResult, StreamingSnapshot,
};
use crate::nanocloud::logger::{log_info, log_warn};
use crate::nanocloud::observability::metrics;
use crate::nanocloud::server::handlers::error::ApiError;
use crate::nanocloud::util::error::with_context;

pub(super) async fn stream_latest_backup(
    owner: String,
    namespace: Option<String>,
    service: String,
) -> Result<Response, ApiError> {
    let backup_path = latest_backup_path(&owner, namespace.as_deref(), &service)
        .map_err(ApiError::internal_error)?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                format!("No backups available for service '{}'", service),
            )
        })?;

    let file_name = backup_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| ApiError::internal_message("backup filename contains invalid UTF-8"))?;

    let metadata = tokio::fs::metadata(&backup_path).await.map_err(|err| {
        ApiError::internal_error(with_context(
            err,
            format!("Failed to stat backup '{}'", backup_path.display()),
        ))
    })?;

    if streaming_backup_enabled() {
        if let Some(snapshot) = get_streaming_backup(&backup_path) {
            return stream_backup_via_snapshot(
                owner,
                namespace,
                service,
                file_name.to_string(),
                metadata.len(),
                snapshot,
            );
        }
    }

    let file = File::open(&backup_path).await.map_err(|err| {
        ApiError::internal_error(with_context(
            err,
            format!("Failed to open backup '{}'", backup_path.display()),
        ))
    })?;
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);
    let mut response = Response::new(body);
    apply_backup_headers(response.headers_mut(), file_name, metadata.len())?;
    Ok(response)
}

fn apply_backup_headers(
    headers: &mut HeaderMap,
    file_name: &str,
    content_length: u64,
) -> Result<(), ApiError> {
    headers.insert(
        CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/x-tar"),
    );
    let disposition = format!("attachment; filename=\"{file_name}\"");
    headers.insert(
        CONTENT_DISPOSITION,
        axum::http::HeaderValue::from_str(&disposition).map_err(|_| {
            ApiError::internal_message("failed to format Content-Disposition header")
        })?,
    );
    headers.insert(
        CONTENT_LENGTH,
        axum::http::HeaderValue::from_str(&content_length.to_string())
            .map_err(|_| ApiError::internal_message("failed to encode Content-Length header"))?,
    );
    Ok(())
}

struct InstrumentedBackupStream {
    inner: ReceiverStream<SnapshotChunkResult>,
    owner: String,
    namespace: Option<String>,
    namespace_label: String,
    service: String,
    start: Instant,
    bytes_sent: u64,
    finished: bool,
}

impl InstrumentedBackupStream {
    fn new(
        owner: String,
        namespace: Option<String>,
        service: String,
        receiver: Receiver<SnapshotChunkResult>,
    ) -> Self {
        let namespace_label = namespace.as_deref().unwrap_or("default").to_string();
        InstrumentedBackupStream {
            inner: ReceiverStream::new(receiver),
            owner,
            namespace,
            namespace_label,
            service,
            start: Instant::now(),
            bytes_sent: 0,
            finished: false,
        }
    }

    fn finish(&mut self, error: Option<String>) {
        if self.finished {
            return;
        }
        self.finished = true;

        let duration = self.start.elapsed();
        metrics::record_backup_stream(
            self.owner.as_str(),
            self.namespace.as_deref(),
            self.service.as_str(),
            self.bytes_sent,
            duration,
        );

        let duration_secs = duration.as_secs_f64();
        let bytes_text = self.bytes_sent.to_string();
        let duration_text = format!("{duration_secs:.3}");

        if let Some(message) = error {
            let fields = [
                ("owner", self.owner.as_str()),
                ("namespace", self.namespace_label.as_str()),
                ("service", self.service.as_str()),
                ("bytes", bytes_text.as_str()),
                ("duration_seconds", duration_text.as_str()),
                ("error", message.as_str()),
            ];
            log_warn(
                "services",
                "Backup stream terminated before completion",
                &fields,
            );
        } else {
            let throughput = if duration_secs > 0.0 && self.bytes_sent > 0 {
                (self.bytes_sent as f64 / (1 << 20) as f64) / duration_secs
            } else {
                0.0
            };
            let throughput_text = format!("{throughput:.2}");
            let fields = [
                ("owner", self.owner.as_str()),
                ("namespace", self.namespace_label.as_str()),
                ("service", self.service.as_str()),
                ("bytes", bytes_text.as_str()),
                ("duration_seconds", duration_text.as_str()),
                ("throughput_mib_s", throughput_text.as_str()),
            ];
            log_info("services", "Streamed backup artifact", &fields);
        }
    }
}

impl Stream for InstrumentedBackupStream {
    type Item = Result<Bytes, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    if chunk.is_last {
                        self.finish(None);
                        continue;
                    }
                    self.bytes_sent += chunk.bytes.len() as u64;
                    return Poll::Ready(Some(Ok(chunk.bytes)));
                }
                Poll::Ready(Some(Err(err))) => {
                    self.finish(Some(err.to_string()));
                    return Poll::Ready(None);
                }
                Poll::Ready(None) => {
                    self.finish(None);
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn stream_backup_via_snapshot(
    owner: String,
    namespace: Option<String>,
    service: String,
    file_name: String,
    content_length: u64,
    snapshot: StreamingSnapshot,
) -> Result<Response, ApiError> {
    let runner = snapshot.clone();
    let receiver = snapshot.subscribe();
    let stream = InstrumentedBackupStream::new(owner, namespace, service, receiver);
    tokio::spawn(async move {
        if let Err(err) = runner.run().await {
            let error_text = err.to_string();
            log_warn(
                "services",
                "Backup stream runner failed",
                &[("error", error_text.as_str())],
            );
        }
    });
    let body = Body::from_stream(stream);
    let mut response = Response::new(body);
    apply_backup_headers(response.headers_mut(), &file_name, content_length)?;
    Ok(response)
}
