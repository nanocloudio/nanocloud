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

use crate::nanocloud::logger::{log_debug, log_info, log_warn};
use bytes::Bytes;
use futures_util::{pin_mut, StreamExt};
use std::collections::HashMap;
use std::env;
use std::fmt::{self, Display, Formatter};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio_util::io::ReaderStream;
use tokio_util::sync::CancellationToken;

const DEFAULT_CHUNK_BYTES: usize = 128 * 1024;
const DEFAULT_BUFFER_BYTES: usize = 2 * 1024 * 1024;
const STREAMING_COMPONENT: &str = "engine.streaming";
const STREAMING_BACKUP_ENV: &str = "NANOCLOUD_STREAMING_BACKUP";
const STREAMING_CHUNK_BYTES_ENV: &str = "NANOCLOUD_STREAMING_CHUNK_BYTES";
const STREAMING_BUFFER_BYTES_ENV: &str = "NANOCLOUD_STREAMING_BUFFER_BYTES";
const STREAMING_SUBSCRIBER_CAPACITY_ENV: &str = "NANOCLOUD_STREAMING_SUBSCRIBER_CAPACITY";
const STREAMING_BACKPRESSURE_ENV: &str = "NANOCLOUD_STREAMING_BACKPRESSURE";
const STREAMING_IDLE_SECS_ENV: &str = "NANOCLOUD_STREAMING_IDLE_SECS";
const STREAMING_ALLOW_REPLACE_ENV: &str = "NANOCLOUD_STREAMING_ALLOW_REPLACE";
const STREAMING_THROTTLE_MS_ENV: &str = "NANOCLOUD_STREAMING_THROTTLE_MS";

fn capacity_from_bytes(buffer_bytes: usize, chunk_bytes: usize) -> usize {
    let divisor = chunk_bytes.max(1);
    buffer_bytes.div_ceil(divisor).max(1)
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BackpressureStrategy {
    Block,
    DropNewest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamingBackupConfig {
    pub enabled: bool,
    pub chunk_bytes: usize,
    pub buffer_bytes: usize,
    pub subscriber_capacity: usize,
    pub idle_ttl: Option<Duration>,
    pub backpressure: BackpressureStrategy,
    pub allow_replace: bool,
    pub throttle_interval: Option<Duration>,
}

impl Default for StreamingBackupConfig {
    fn default() -> Self {
        let chunk_bytes = DEFAULT_CHUNK_BYTES;
        let buffer_bytes = DEFAULT_BUFFER_BYTES;
        let capacity = capacity_from_bytes(buffer_bytes, chunk_bytes);
        StreamingBackupConfig {
            enabled: false,
            chunk_bytes,
            buffer_bytes,
            subscriber_capacity: capacity,
            idle_ttl: None,
            backpressure: BackpressureStrategy::Block,
            allow_replace: false,
            throttle_interval: None,
        }
    }
}

impl StreamingBackupConfig {
    pub fn from_env() -> Self {
        let mut config = StreamingBackupConfig::default();
        if let Ok(value) = env::var(STREAMING_BACKUP_ENV) {
            let upper = value.trim().to_ascii_uppercase();
            config.enabled = matches!(upper.as_str(), "1" | "TRUE" | "ON");
        }
        if let Ok(value) = env::var(STREAMING_CHUNK_BYTES_ENV) {
            if let Ok(parsed) = value.trim().parse::<usize>() {
                if parsed > 0 {
                    config.chunk_bytes = parsed;
                }
            }
        }
        if let Ok(value) = env::var(STREAMING_BUFFER_BYTES_ENV) {
            if let Ok(parsed) = value.trim().parse::<usize>() {
                if parsed > 0 {
                    config.buffer_bytes = parsed;
                }
            }
        }
        if let Ok(value) = env::var(STREAMING_SUBSCRIBER_CAPACITY_ENV) {
            if let Ok(parsed) = value.trim().parse::<usize>() {
                config.subscriber_capacity = parsed.max(1);
            }
        } else {
            config.subscriber_capacity =
                capacity_from_bytes(config.buffer_bytes, config.chunk_bytes);
        }
        if let Ok(value) = env::var(STREAMING_BACKPRESSURE_ENV) {
            match value.trim().to_ascii_lowercase().as_str() {
                "drop" | "drop_newest" => {
                    config.backpressure = BackpressureStrategy::DropNewest;
                }
                _ => {
                    config.backpressure = BackpressureStrategy::Block;
                }
            }
        }
        if let Ok(value) = env::var(STREAMING_IDLE_SECS_ENV) {
            if let Ok(parsed) = value.trim().parse::<u64>() {
                if parsed > 0 {
                    config.idle_ttl = Some(Duration::from_secs(parsed));
                }
            }
        }
        if let Ok(value) = env::var(STREAMING_ALLOW_REPLACE_ENV) {
            let upper = value.trim().to_ascii_uppercase();
            config.allow_replace = matches!(upper.as_str(), "1" | "TRUE" | "ON");
        }
        if let Ok(value) = env::var(STREAMING_THROTTLE_MS_ENV) {
            if let Ok(parsed) = value.trim().parse::<u64>() {
                if parsed > 0 {
                    config.throttle_interval = Some(Duration::from_millis(parsed));
                }
            }
        }
        config
    }
}

/// Returns true when the streaming backup pipeline is enabled via feature flag.
pub fn streaming_backup_enabled() -> bool {
    StreamingBackupConfig::from_env().enabled
}

/// Hook interface invoked during streaming lifecycle events.
pub trait StreamingHooks: Send + Sync {
    fn on_register(&self, _path: &Path, _id: u64) {}
    fn on_unregister(&self, _path: &Path, _id: u64) {}
    fn on_stream_start(&self, _path: &Path, _id: u64) {}
    fn on_stream_complete(
        &self,
        _path: &Path,
        _id: u64,
        _stats: &StreamingSnapshotStats,
        _duration: Duration,
    ) {
    }
    fn on_stream_error(
        &self,
        _path: &Path,
        _id: u64,
        _error: &StreamingSnapshotError,
        _duration: Duration,
        _stats: &StreamingSnapshotStats,
    ) {
    }
    fn on_backpressure_drop(&self, _path: &Path, _id: u64, _dropped: usize, _failed: usize) {}
}

#[derive(Default)]
struct NoopStreamingHooks;

impl StreamingHooks for NoopStreamingHooks {}

fn streaming_hooks() -> &'static RwLock<Arc<dyn StreamingHooks>> {
    static HOOKS: OnceLock<RwLock<Arc<dyn StreamingHooks>>> = OnceLock::new();
    HOOKS.get_or_init(|| RwLock::new(Arc::new(NoopStreamingHooks)))
}

/// Overrides the streaming hooks used for observability. Primarily used in tests.
#[allow(dead_code)]
pub fn set_streaming_hooks(hooks: Arc<dyn StreamingHooks>) {
    if let Ok(mut guard) = streaming_hooks().write() {
        *guard = hooks;
    }
}

fn with_hooks<F: FnOnce(&dyn StreamingHooks)>(callback: F) {
    if let Ok(guard) = streaming_hooks().read() {
        callback(guard.as_ref());
    }
}

#[cfg(test)]
pub fn reset_streaming_hooks() {
    if let Ok(mut guard) = streaming_hooks().write() {
        *guard = Arc::new(NoopStreamingHooks);
    }
}

static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(1);

/// Result type emitted to streaming subscribers.
pub type SnapshotChunkResult = Result<Arc<SnapshotChunk>, StreamingSnapshotError>;

#[derive(Clone, Debug)]
struct Subscriber {
    id: u64,
    sender: Sender<SnapshotChunkResult>,
}

#[derive(Default)]
struct SubscriberState {
    subscribers: Vec<Subscriber>,
    inflight: usize,
    closing: bool,
}

#[derive(Default)]
struct BroadcastReport {
    dropped: usize,
    failed: usize,
}

impl BroadcastReport {
    fn has_errors(&self) -> bool {
        self.dropped > 0 || self.failed > 0
    }
}

struct StreamingSnapshotInner {
    id: u64,
    volume: Arc<str>,
    path: PathBuf,
    chunk_bytes: usize,
    subscriber_capacity: usize,
    backpressure: BackpressureStrategy,
    throttle_interval: Option<Duration>,
    state: Mutex<SubscriberState>,
    inflight_notify: Notify,
    next_subscriber_id: AtomicU64,
    #[cfg(test)]
    fail_after_chunk: Option<u64>,
}

impl StreamingSnapshotInner {
    fn new(
        volume: Arc<str>,
        path: PathBuf,
        chunk_bytes: usize,
        subscriber_capacity: usize,
        backpressure: BackpressureStrategy,
        throttle_interval: Option<Duration>,
        #[cfg(test)] fail_after_chunk: Option<u64>,
    ) -> Self {
        StreamingSnapshotInner {
            id: NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed),
            volume,
            path,
            chunk_bytes,
            subscriber_capacity,
            backpressure,
            throttle_interval,
            state: Mutex::new(SubscriberState::default()),
            inflight_notify: Notify::new(),
            next_subscriber_id: AtomicU64::new(0),
            #[cfg(test)]
            fail_after_chunk,
        }
    }

    fn register(&self, sender: Sender<SnapshotChunkResult>) -> u64 {
        let id = self.next_subscriber_id.fetch_add(1, Ordering::AcqRel);
        let mut state = self
            .state
            .lock()
            .expect("streaming snapshot subscriber lock poisoned");
        if !state.closing {
            state.subscribers.push(Subscriber { id, sender });
        }
        id
    }

    async fn has_subscribers(&self) -> bool {
        let state = self
            .state
            .lock()
            .expect("streaming snapshot subscriber lock poisoned");
        !state.subscribers.is_empty()
    }

    async fn broadcast(&self, event: SnapshotChunkResult) -> BroadcastReport {
        let subscribers = {
            let mut state = self
                .state
                .lock()
                .expect("streaming snapshot subscriber lock poisoned");
            if state.closing {
                return BroadcastReport::default();
            }
            state.inflight += 1;
            state.subscribers.clone()
        };

        let mut dropped = Vec::new();
        let mut failed = 0usize;
        for subscriber in subscribers {
            let send_result: Result<(), TrySendError<SnapshotChunkResult>> = match self.backpressure
            {
                BackpressureStrategy::Block => subscriber
                    .sender
                    .send(event.clone())
                    .await
                    .map_err(|err| TrySendError::Closed(err.0)),
                BackpressureStrategy::DropNewest => subscriber.sender.try_send(event.clone()),
            };

            match send_result {
                Ok(()) => {}
                Err(TrySendError::Closed(_)) => {
                    failed += 1;
                    dropped.push(subscriber.id);
                }
                Err(TrySendError::Full(_)) => {
                    failed += 1;
                    dropped.push(subscriber.id);
                    log_warn(
                        STREAMING_COMPONENT,
                        "Dropping slow subscriber due to backpressure",
                        &[("stream_id", &self.id.to_string())],
                    );
                }
            }
        }

        let mut state = self
            .state
            .lock()
            .expect("streaming snapshot subscriber lock poisoned");
        if !dropped.is_empty() {
            state
                .subscribers
                .retain(|subscriber| !dropped.contains(&subscriber.id));
        }
        state.inflight = state.inflight.saturating_sub(1);
        if state.inflight == 0 {
            self.inflight_notify.notify_waiters();
        }

        let dropped_count = dropped.len();
        if dropped_count > 0 || failed > 0 {
            with_hooks(|hooks| {
                hooks.on_backpressure_drop(&self.path, self.id, dropped_count, failed)
            });
        }

        BroadcastReport {
            dropped: dropped_count,
            failed,
        }
    }

    async fn broadcast_best_effort(&self, event: SnapshotChunkResult) -> BroadcastReport {
        let subscribers = {
            let state = self
                .state
                .lock()
                .expect("streaming snapshot subscriber lock poisoned");
            if state.closing {
                return BroadcastReport::default();
            }
            state.subscribers.clone()
        };

        let mut dropped = Vec::new();
        let mut failed = 0usize;
        for subscriber in subscribers {
            match subscriber.sender.try_send(event.clone()) {
                Ok(()) => {}
                Err(TrySendError::Closed(_)) => {
                    failed += 1;
                    dropped.push(subscriber.id);
                }
                Err(TrySendError::Full(_)) => {
                    failed += 1;
                    dropped.push(subscriber.id);
                    log_warn(
                        STREAMING_COMPONENT,
                        "Dropping slow subscriber due to backpressure",
                        &[("stream_id", &self.id.to_string())],
                    );
                }
            }
        }

        let mut state = self
            .state
            .lock()
            .expect("streaming snapshot subscriber lock poisoned");
        if !dropped.is_empty() {
            state
                .subscribers
                .retain(|subscriber| !dropped.contains(&subscriber.id));
        }

        if !dropped.is_empty() {
            with_hooks(|hooks| {
                hooks.on_backpressure_drop(&self.path, self.id, dropped.len(), failed)
            });
        }

        BroadcastReport {
            dropped: dropped.len(),
            failed,
        }
    }

    async fn close(&self) {
        {
            let mut state = self
                .state
                .lock()
                .expect("streaming snapshot subscriber lock poisoned");
            state.closing = true;
        }

        loop {
            let inflight = {
                let state = self
                    .state
                    .lock()
                    .expect("streaming snapshot subscriber lock poisoned");
                state.inflight
            };
            if inflight == 0 {
                let mut state = self
                    .state
                    .lock()
                    .expect("streaming snapshot subscriber lock poisoned");
                state.subscribers.clear();
                break;
            }
            self.inflight_notify.notified().await;
        }
    }
}

/// Metadata and payload for an individual snapshot chunk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotChunk {
    pub volume: Arc<str>,
    pub chunk_index: u64,
    pub offset: u64,
    pub bytes: Bytes,
    pub is_last: bool,
}

/// Aggregate statistics collected while streaming a snapshot.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StreamingSnapshotStats {
    pub bytes: u64,
    pub chunks: u64,
    pub dropped_subscribers: u64,
    pub delivery_errors: u64,
}

/// Error propagated to streaming subscribers.
#[derive(Clone, Debug)]
pub struct StreamingSnapshotError {
    kind: Arc<StreamingSnapshotErrorKind>,
}

#[derive(Debug)]
enum StreamingSnapshotErrorKind {
    Open {
        path: PathBuf,
        error: Arc<io::Error>,
    },
    Read {
        path: PathBuf,
        error: Arc<io::Error>,
    },
    Delivery {
        path: PathBuf,
        dropped: usize,
    },
    Cancelled {
        path: PathBuf,
    },
}

impl StreamingSnapshotError {
    fn open(path: PathBuf, error: io::Error) -> Self {
        StreamingSnapshotError {
            kind: Arc::new(StreamingSnapshotErrorKind::Open {
                path,
                error: Arc::new(error),
            }),
        }
    }

    fn read(path: PathBuf, error: io::Error) -> Self {
        StreamingSnapshotError {
            kind: Arc::new(StreamingSnapshotErrorKind::Read {
                path,
                error: Arc::new(error),
            }),
        }
    }

    fn delivery(path: PathBuf, dropped: usize) -> Self {
        StreamingSnapshotError {
            kind: Arc::new(StreamingSnapshotErrorKind::Delivery { path, dropped }),
        }
    }

    fn cancelled(path: PathBuf) -> Self {
        StreamingSnapshotError {
            kind: Arc::new(StreamingSnapshotErrorKind::Cancelled { path }),
        }
    }
}

impl Display for StreamingSnapshotError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.kind.as_ref() {
            StreamingSnapshotErrorKind::Open { path, error } => write!(
                f,
                "failed to open snapshot '{}' for streaming: {}",
                path.display(),
                error
            ),
            StreamingSnapshotErrorKind::Read { path, error } => write!(
                f,
                "failed to read snapshot '{}' during streaming: {}",
                path.display(),
                error
            ),
            StreamingSnapshotErrorKind::Delivery { path, dropped } => write!(
                f,
                "failed to deliver snapshot '{}' to {} subscriber(s)",
                path.display(),
                dropped
            ),
            StreamingSnapshotErrorKind::Cancelled { path } => write!(
                f,
                "streaming snapshot '{}' cancelled before completion",
                path.display()
            ),
        }
    }
}

impl std::error::Error for StreamingSnapshotError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.kind.as_ref() {
            StreamingSnapshotErrorKind::Open { error, .. }
            | StreamingSnapshotErrorKind::Read { error, .. } => Some(error.as_ref()),
            _ => None,
        }
    }
}

/// Streaming run options such as cancellation.
#[derive(Clone, Debug, Default)]
pub struct StreamingRunOptions {
    pub cancellation: Option<CancellationToken>,
}

/// Builder for configuring a [`StreamingSnapshot`] instance.
pub struct StreamingSnapshotBuilder {
    volume: String,
    path: PathBuf,
    chunk_bytes: usize,
    buffer_bytes: usize,
    subscriber_capacity: usize,
    backpressure: BackpressureStrategy,
    custom_capacity: bool,
    throttle_interval: Option<Duration>,
    #[cfg(test)]
    fail_after_chunk: Option<u64>,
}

impl StreamingSnapshotBuilder {
    pub fn from_config(
        volume: impl Into<String>,
        path: impl Into<PathBuf>,
        config: &StreamingBackupConfig,
    ) -> StreamingSnapshotBuilder {
        StreamingSnapshotBuilder {
            volume: volume.into(),
            path: path.into(),
            chunk_bytes: config.chunk_bytes,
            buffer_bytes: config.buffer_bytes,
            subscriber_capacity: config.subscriber_capacity,
            backpressure: config.backpressure,
            custom_capacity: true,
            throttle_interval: config.throttle_interval,
            #[cfg(test)]
            fail_after_chunk: None,
        }
    }

    pub fn build(self) -> StreamingSnapshot {
        let capacity = if self.custom_capacity {
            self.subscriber_capacity
        } else {
            capacity_from_bytes(self.buffer_bytes, self.chunk_bytes)
        };
        StreamingSnapshot {
            inner: Arc::new(StreamingSnapshotInner::new(
                Arc::<str>::from(self.volume),
                self.path,
                self.chunk_bytes,
                capacity,
                self.backpressure,
                self.throttle_interval,
                #[cfg(test)]
                self.fail_after_chunk,
            )),
            cleanup: None,
        }
    }

    #[allow(dead_code)]
    pub fn throttle_interval(mut self, interval: Option<Duration>) -> Self {
        self.throttle_interval = interval;
        self
    }

    #[allow(dead_code)]
    pub fn chunk_bytes(mut self, chunk_bytes: usize) -> Self {
        self.chunk_bytes = chunk_bytes.max(1);
        if !self.custom_capacity {
            self.subscriber_capacity = capacity_from_bytes(self.buffer_bytes, self.chunk_bytes);
        }
        self
    }

    #[allow(dead_code)]
    pub fn buffer_bytes(mut self, buffer_bytes: usize) -> Self {
        self.buffer_bytes = buffer_bytes.max(1);
        if !self.custom_capacity {
            self.subscriber_capacity = capacity_from_bytes(self.buffer_bytes, self.chunk_bytes);
        }
        self
    }

    #[allow(dead_code)]
    pub fn subscriber_capacity(mut self, capacity: usize) -> Self {
        self.subscriber_capacity = capacity.max(1);
        self.custom_capacity = true;
        self
    }

    #[allow(dead_code)]
    pub fn backpressure(mut self, strategy: BackpressureStrategy) -> Self {
        self.backpressure = strategy;
        self
    }

    #[cfg(test)]
    pub fn fail_after_chunk(mut self, chunk: u64) -> Self {
        self.fail_after_chunk = Some(chunk);
        self
    }
}

/// Streaming reader for snapshot archives backed by [`ReaderStream`].
///
/// # Examples
///
/// ```
/// use nanocloud::nanocloud::engine::StreamingSnapshot;
/// use tokio::runtime::Runtime;
///
/// let dir = tempfile::tempdir().unwrap();
/// let path = dir.path().join("snapshot.doc.tar");
/// std::fs::write(&path, b"example-bytes").unwrap();
///
/// let snapshot = StreamingSnapshot::builder("docs", &path)
///     .chunk_bytes(4)
///     .build();
/// let mut receiver = snapshot.subscribe();
///
/// let stats = Runtime::new().unwrap().block_on(async move {
///     let runner = snapshot.clone();
///     let handle = tokio::spawn(async move { runner.run().await });
///     while let Some(event) = receiver.recv().await {
///         let chunk = event.unwrap();
///         if chunk.is_last {
///             break;
///         }
///     }
///     handle.await.unwrap().unwrap()
/// });
///
/// assert_eq!(stats.bytes, 13);
/// ```
pub struct StreamingSnapshot {
    inner: Arc<StreamingSnapshotInner>,
    cleanup: Option<Arc<StreamingCleanup>>,
}

impl Clone for StreamingSnapshot {
    fn clone(&self) -> Self {
        StreamingSnapshot {
            inner: Arc::clone(&self.inner),
            cleanup: self.cleanup.clone(),
        }
    }
}

impl StreamingSnapshot {
    #[allow(dead_code)]
    pub fn builder(
        volume: impl Into<String>,
        path: impl Into<PathBuf>,
    ) -> StreamingSnapshotBuilder {
        StreamingSnapshotBuilder {
            volume: volume.into(),
            path: path.into(),
            chunk_bytes: DEFAULT_CHUNK_BYTES,
            buffer_bytes: DEFAULT_BUFFER_BYTES,
            subscriber_capacity: capacity_from_bytes(DEFAULT_BUFFER_BYTES, DEFAULT_CHUNK_BYTES),
            backpressure: BackpressureStrategy::Block,
            custom_capacity: false,
            throttle_interval: None,
            #[cfg(test)]
            fail_after_chunk: None,
        }
    }

    pub fn subscribe(&self) -> Receiver<SnapshotChunkResult> {
        let (sender, receiver) = mpsc::channel(self.inner.subscriber_capacity);
        let _id = self.inner.register(sender);
        receiver
    }

    pub async fn run(&self) -> Result<StreamingSnapshotStats, StreamingSnapshotError> {
        self.run_with_options(StreamingRunOptions::default()).await
    }

    pub async fn run_with_options(
        &self,
        options: StreamingRunOptions,
    ) -> Result<StreamingSnapshotStats, StreamingSnapshotError> {
        let start = Instant::now();
        let path = self.inner.path.clone();
        let file = match File::open(&path).await {
            Ok(file) => file,
            Err(error) => {
                let err = StreamingSnapshotError::open(path.clone(), error);
                let _ = self.inner.broadcast(Err(err.clone())).await;
                return self
                    .finish_run(
                        path.clone(),
                        start,
                        StreamingSnapshotStats::default(),
                        Some(err),
                    )
                    .await;
            }
        };

        with_hooks(|hooks| hooks.on_stream_start(&path, self.inner.id));

        let reader = ReaderStream::with_capacity(file, self.inner.chunk_bytes);
        let mut stats = StreamingSnapshotStats::default();
        let mut chunk_index = 0u64;
        let mut offset = 0u64;
        let cancellation = options.cancellation.clone();

        pin_mut!(reader);
        loop {
            tokio::select! {
                _ = async {
                    if let Some(token) = cancellation.as_ref() {
                        token.cancelled().await;
                    }
                }, if cancellation.is_some() => {
                    let err = StreamingSnapshotError::cancelled(path.clone());
                    let _ = self.inner.broadcast_best_effort(Err(err.clone())).await;
                    return self.finish_run(path.clone(), start, stats, Some(err)).await;
                }
                result = reader.next() => {
                    match result {
                        Some(Ok(bytes)) => {
                            let len = bytes.len() as u64;
                            stats.bytes += len;
                            stats.chunks += 1;

                            let chunk = Arc::new(SnapshotChunk {
                                volume: Arc::clone(&self.inner.volume),
                                chunk_index,
                                offset,
                                bytes,
                                is_last: false,
                            });

                            #[cfg(test)]
                            if let Some(limit) = self.inner.fail_after_chunk {
                                if chunk_index >= limit {
                                    let err = StreamingSnapshotError::read(
                                        path.clone(),
                                        io::Error::other("injected read failure"),
                                    );
                                    let _ = self.inner.broadcast(Err(err.clone())).await;
                                    return self
                                        .finish_run(path.clone(), start, stats, Some(err))
                                        .await;
                                }
                            }

                            chunk_index += 1;
                            offset += len;

                            let report = self.inner.broadcast(Ok(chunk)).await;
                            if report.has_errors() {
                                stats.dropped_subscribers += report.dropped as u64;
                                stats.delivery_errors += report.failed as u64;
                                let failures = std::cmp::max(report.dropped, report.failed);
                                let err = StreamingSnapshotError::delivery(path.clone(), failures);
                                let _ = self.inner.broadcast(Err(err.clone())).await;
                                return self.finish_run(path.clone(), start, stats, Some(err)).await;
                            }

                            if let Some(delay) = self.inner.throttle_interval {
                                sleep(delay).await;
                            }
                        }
                        Some(Err(error)) => {
                            let err = StreamingSnapshotError::read(path.clone(), error);
                            let _ = self.inner.broadcast(Err(err.clone())).await;
                            return self.finish_run(path.clone(), start, stats, Some(err)).await;
                        }
                        None => break,
                    }
                }
            }
        }

        if self.inner.has_subscribers().await {
            let final_chunk = Arc::new(SnapshotChunk {
                volume: Arc::clone(&self.inner.volume),
                chunk_index,
                offset,
                bytes: Bytes::new(),
                is_last: true,
            });
            let report = self.inner.broadcast(Ok(final_chunk)).await;
            if report.has_errors() {
                stats.dropped_subscribers += report.dropped as u64;
                stats.delivery_errors += report.failed as u64;
                let failures = std::cmp::max(report.dropped, report.failed);
                let err = StreamingSnapshotError::delivery(path.clone(), failures);
                let _ = self.inner.broadcast(Err(err.clone())).await;
                return self.finish_run(path.clone(), start, stats, Some(err)).await;
            }
        }

        self.finish_run(path.clone(), start, stats, None).await
    }

    fn cleanup(&self) {
        if let Some(cleanup) = self.cleanup.as_ref() {
            cleanup.run();
        }
    }

    async fn finish_run(
        &self,
        path: PathBuf,
        start: Instant,
        stats: StreamingSnapshotStats,
        error: Option<StreamingSnapshotError>,
    ) -> Result<StreamingSnapshotStats, StreamingSnapshotError> {
        self.inner.close().await;
        let duration = start.elapsed();
        if let Some(err) = error {
            with_hooks(|hooks| hooks.on_stream_error(&path, self.inner.id, &err, duration, &stats));
            self.cleanup();
            Err(err)
        } else {
            with_hooks(|hooks| hooks.on_stream_complete(&path, self.inner.id, &stats, duration));
            self.cleanup();
            Ok(stats)
        }
    }
}

struct StreamingCleanup {
    path: PathBuf,
    registry: Arc<StreamingSnapshotRegistry>,
    id: u64,
    executed: AtomicBool,
}

impl StreamingCleanup {
    fn new(path: PathBuf, registry: Arc<StreamingSnapshotRegistry>, id: u64) -> Self {
        StreamingCleanup {
            path,
            registry,
            id,
            executed: AtomicBool::new(false),
        }
    }

    fn run(&self) -> bool {
        if self.executed.swap(true, Ordering::AcqRel) {
            return false;
        }
        {
            let _ = registration_handles()
                .lock()
                .expect("registration handle lock poisoned")
                .remove(&self.path);
        }
        let removed = self.registry.unregister(&self.path, Some(self.id));
        if removed {
            let path_text = self.path.display().to_string();
            let stream_id = self.id.to_string();
            log_info(
                STREAMING_COMPONENT,
                "Unregistered streaming snapshot",
                &[
                    ("path", path_text.as_str()),
                    ("stream_id", stream_id.as_str()),
                ],
            );
            with_hooks(|hooks| hooks.on_unregister(&self.path, self.id));
        }
        removed
    }
}

struct RegistryEntry {
    id: u64,
    snapshot: StreamingSnapshot,
    last_used: Instant,
}

impl RegistryEntry {
    fn new(id: u64, snapshot: StreamingSnapshot) -> Self {
        RegistryEntry {
            id,
            snapshot,
            last_used: Instant::now(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum StreamingRegistryError {
    Duplicate(PathBuf),
}

impl Display for StreamingRegistryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            StreamingRegistryError::Duplicate(path) => {
                write!(
                    f,
                    "streaming snapshot for '{}' is already registered",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for StreamingRegistryError {}

struct StreamingSnapshotRegistry {
    entries: Mutex<HashMap<PathBuf, RegistryEntry>>,
    next_id: AtomicU64,
}

impl StreamingSnapshotRegistry {
    fn new() -> Self {
        StreamingSnapshotRegistry {
            entries: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    fn evict_expired(&self, ttl: Option<Duration>) -> Vec<PathBuf> {
        if ttl.is_none() {
            return Vec::new();
        }
        let ttl = ttl.unwrap();
        let mut guard = self
            .entries
            .lock()
            .expect("streaming snapshot registry lock poisoned");
        let now = Instant::now();
        let mut removed = Vec::new();
        guard.retain(|path, entry| {
            let stale = now.duration_since(entry.last_used) >= ttl;
            if stale {
                removed.push(path.clone());
            }
            !stale
        });
        removed
    }

    fn register(
        &self,
        path: PathBuf,
        snapshot: StreamingSnapshot,
        config: &StreamingBackupConfig,
    ) -> Result<StreamingRegistration, StreamingRegistryError> {
        let mut guard = self
            .entries
            .lock()
            .expect("streaming snapshot registry lock poisoned");
        if guard.contains_key(&path) {
            if !config.allow_replace {
                return Err(StreamingRegistryError::Duplicate(path));
            }
            guard.remove(&path);
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let registry_handle = Arc::clone(registry());
        let cleanup = Arc::new(StreamingCleanup::new(
            path.clone(),
            Arc::clone(&registry_handle),
            id,
        ));
        let mut snapshot = snapshot;
        snapshot.cleanup = Some(Arc::clone(&cleanup));
        let entry = RegistryEntry::new(id, snapshot.clone());
        guard.insert(path.clone(), entry);
        drop(guard);

        let path_text = path.display().to_string();
        let stream_id = id.to_string();
        log_info(
            STREAMING_COMPONENT,
            "Registered streaming snapshot",
            &[
                ("path", path_text.as_str()),
                ("stream_id", stream_id.as_str()),
            ],
        );
        with_hooks(|hooks| hooks.on_register(&path, id));

        Ok(StreamingRegistration::new(
            path,
            id,
            snapshot,
            registry_handle,
            Some(cleanup),
        ))
    }

    fn get(&self, path: &Path, ttl: Option<Duration>) -> Option<StreamingSnapshot> {
        let mut guard = self
            .entries
            .lock()
            .expect("streaming snapshot registry lock poisoned");
        let now = Instant::now();
        if let Some(mut entry) = guard.remove(path) {
            if let Some(ttl) = ttl {
                if now.duration_since(entry.last_used) >= ttl {
                    drop(guard);
                    let _ = registration_handles()
                        .lock()
                        .expect("registration handle lock poisoned")
                        .remove(path);
                    let path_text = path.display().to_string();
                    log_debug(
                        STREAMING_COMPONENT,
                        "Evicted stale streaming snapshot",
                        &[("path", path_text.as_str())],
                    );
                    return None;
                }
            }
            entry.last_used = now;
            let snapshot = entry.snapshot.clone();
            guard.insert(path.to_path_buf(), entry);
            return Some(snapshot);
        }
        None
    }

    fn unregister(&self, path: &Path, id: Option<u64>) -> bool {
        let mut guard = self
            .entries
            .lock()
            .expect("streaming snapshot registry lock poisoned");
        match guard.remove(path) {
            Some(entry) if id.map(|expected| expected == entry.id).unwrap_or(true) => true,
            Some(entry) => {
                // Put it back if ids do not match (handle may be stale).
                guard.insert(path.to_path_buf(), entry);
                false
            }
            None => false,
        }
    }

    #[cfg(test)]
    fn clear(&self) {
        let mut guard = self
            .entries
            .lock()
            .expect("streaming snapshot registry lock poisoned");
        guard.clear();
    }
}

static STREAMING_REGISTRY: OnceLock<Arc<StreamingSnapshotRegistry>> = OnceLock::new();

fn registry() -> &'static Arc<StreamingSnapshotRegistry> {
    STREAMING_REGISTRY.get_or_init(|| Arc::new(StreamingSnapshotRegistry::new()))
}

fn registration_handles() -> &'static Mutex<HashMap<PathBuf, StreamingRegistration>> {
    static HANDLES: OnceLock<Mutex<HashMap<PathBuf, StreamingRegistration>>> = OnceLock::new();
    HANDLES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub struct StreamingRegistration {
    path: PathBuf,
    id: u64,
    snapshot: StreamingSnapshot,
    registry: Arc<StreamingSnapshotRegistry>,
    refs: Arc<AtomicUsize>,
    cleanup: Option<Arc<StreamingCleanup>>,
}

impl StreamingRegistration {
    fn new(
        path: PathBuf,
        id: u64,
        snapshot: StreamingSnapshot,
        registry: Arc<StreamingSnapshotRegistry>,
        cleanup: Option<Arc<StreamingCleanup>>,
    ) -> Self {
        StreamingRegistration {
            path,
            id,
            snapshot,
            registry,
            refs: Arc::new(AtomicUsize::new(1)),
            cleanup,
        }
    }

    #[allow(dead_code)]
    pub fn snapshot(&self) -> StreamingSnapshot {
        self.snapshot.clone()
    }

    pub fn unregister(&self) -> bool {
        let prev = self.refs.fetch_sub(1, Ordering::AcqRel);
        if prev == 0 {
            return false;
        }
        if prev > 1 {
            return false;
        }
        if let Some(cleanup) = self.cleanup.as_ref() {
            return cleanup.run();
        }
        let removed = self.registry.unregister(&self.path, Some(self.id));
        if removed {
            let path_text = self.path.display().to_string();
            let stream_id = self.id.to_string();
            log_info(
                STREAMING_COMPONENT,
                "Unregistered streaming snapshot",
                &[
                    ("path", path_text.as_str()),
                    ("stream_id", stream_id.as_str()),
                ],
            );
            with_hooks(|hooks| hooks.on_unregister(&self.path, self.id));
        }
        removed
    }
}

impl Clone for StreamingRegistration {
    fn clone(&self) -> Self {
        self.refs.fetch_add(1, Ordering::AcqRel);
        StreamingRegistration {
            path: self.path.clone(),
            id: self.id,
            snapshot: self.snapshot.clone(),
            registry: Arc::clone(&self.registry),
            refs: Arc::clone(&self.refs),
            cleanup: self.cleanup.clone(),
        }
    }
}

impl Drop for StreamingRegistration {
    fn drop(&mut self) {
        let _ = self.unregister();
    }
}

/// Registers a streaming snapshot for the given artifact path when the feature flag is enabled.
pub fn register_streaming_backup(
    volume_label: impl Into<String>,
    path: impl AsRef<Path>,
) -> Result<Option<StreamingRegistration>, StreamingRegistryError> {
    let config = StreamingBackupConfig::from_env();
    register_streaming_backup_with_config(volume_label, path, &config)
}

/// Registers a streaming snapshot with an explicit configuration.
pub fn register_streaming_backup_with_config(
    volume_label: impl Into<String>,
    path: impl AsRef<Path>,
    config: &StreamingBackupConfig,
) -> Result<Option<StreamingRegistration>, StreamingRegistryError> {
    if !config.enabled {
        return Ok(None);
    }
    let path_ref = path.as_ref();
    let path_buf = path_ref.to_path_buf();
    let snapshot = StreamingSnapshotBuilder::from_config(volume_label, path_ref, config).build();
    let removed = registry().evict_expired(config.idle_ttl);
    if !removed.is_empty() {
        for evicted in removed {
            let path_text = evicted.display().to_string();
            let _ = registration_handles()
                .lock()
                .expect("registration handle lock poisoned")
                .remove(&evicted);
            log_debug(
                STREAMING_COMPONENT,
                "Evicted stale streaming snapshot",
                &[("path", path_text.as_str())],
            );
        }
    }
    if config.allow_replace {
        let _ = registration_handles()
            .lock()
            .expect("registration handle lock poisoned")
            .remove(&path_buf);
    }
    let handle = registry().register(path_buf.clone(), snapshot, config)?;
    registration_handles()
        .lock()
        .expect("registration handle lock poisoned")
        .insert(path_buf, handle.clone());
    Ok(Some(handle))
}

/// Fetches a previously registered streaming snapshot, if present.
pub fn get_streaming_backup(path: impl AsRef<Path>) -> Option<StreamingSnapshot> {
    let config = StreamingBackupConfig::from_env();
    let removed = registry().evict_expired(config.idle_ttl);
    if !removed.is_empty() {
        for evicted in removed {
            let path_text = evicted.display().to_string();
            let _ = registration_handles()
                .lock()
                .expect("registration handle lock poisoned")
                .remove(&evicted);
            log_debug(
                STREAMING_COMPONENT,
                "Evicted stale streaming snapshot",
                &[("path", path_text.as_str())],
            );
        }
    }
    registry().get(path.as_ref(), config.idle_ttl)
}

/// Removes a registered streaming snapshot for the given path, if present.
pub fn remove_streaming_backup(path: impl AsRef<Path>) {
    let path_buf = path.as_ref().to_path_buf();
    let handle = registration_handles()
        .lock()
        .expect("registration handle lock poisoned")
        .remove(&path_buf);
    if let Some(handle) = handle {
        let _ = handle.unregister();
    }
    let removed = registry().unregister(&path_buf, None);
    if removed {
        let path_text = path_buf.display().to_string();
        log_info(
            STREAMING_COMPONENT,
            "Unregistered streaming snapshot",
            &[("path", path_text.as_str())],
        );
        with_hooks(|hooks| hooks.on_unregister(&path_buf, 0));
    }
}

#[cfg(test)]
pub fn clear_streaming_backups() {
    let handles = {
        let mut guard = registration_handles()
            .lock()
            .expect("registration handle lock poisoned");
        std::mem::take(&mut *guard)
    };
    drop(handles);
    if let Some(registry) = STREAMING_REGISTRY.get() {
        registry.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::api::types::BundleSpec;
    use crate::nanocloud::engine::profile::Profile;
    use crate::nanocloud::engine::snapshot::Snapshot;
    use crate::nanocloud::util::security::EncryptionKey;
    use serde_json::json;
    use std::collections::HashMap;
    use std::ptr;
    use std::sync::{Arc, Mutex, OnceLock};
    use tar::{Builder, Header};
    use tempfile::tempdir;
    use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};
    use tokio::time::{sleep, Duration};

    #[derive(Clone)]
    struct EnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: impl AsRef<str>) -> Self {
            let previous = std::env::var(key).ok();
            std::env::set_var(key, value.as_ref());
            EnvGuard { key, previous }
        }

        #[allow(dead_code)]
        fn unset(key: &'static str) -> Self {
            let previous = std::env::var(key).ok();
            std::env::remove_var(key);
            EnvGuard { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(prev) = self.previous.as_ref() {
                std::env::set_var(self.key, prev);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    struct HookGuard;

    impl Drop for HookGuard {
        fn drop(&mut self) {
            reset_streaming_hooks();
        }
    }

    fn install_hooks(hooks: Arc<dyn StreamingHooks>) -> HookGuard {
        set_streaming_hooks(hooks);
        HookGuard
    }

    fn streaming_test_lock() -> &'static AsyncMutex<()> {
        static LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| AsyncMutex::new(()))
    }

    fn stream_guard() -> AsyncMutexGuard<'static, ()> {
        streaming_test_lock().blocking_lock()
    }

    async fn stream_guard_async() -> AsyncMutexGuard<'static, ()> {
        streaming_test_lock().lock().await
    }

    #[test]
    fn streaming_feature_flag_defaults_off_and_reads_env() {
        let _guard = stream_guard();
        std::env::remove_var(STREAMING_BACKUP_ENV);
        clear_streaming_backups();
        assert!(!streaming_backup_enabled());

        std::env::set_var(STREAMING_BACKUP_ENV, "true");
        assert!(streaming_backup_enabled());

        std::env::set_var(STREAMING_BACKUP_ENV, "0");
        assert!(!streaming_backup_enabled());

        std::env::remove_var(STREAMING_BACKUP_ENV);
        clear_streaming_backups();
    }

    #[test]
    fn parses_streaming_config_from_env() {
        let _guard = stream_guard();
        let _flag = EnvGuard::set(STREAMING_BACKUP_ENV, "true");
        let _chunk = EnvGuard::set(STREAMING_CHUNK_BYTES_ENV, "4096");
        let _buffer = EnvGuard::set(STREAMING_BUFFER_BYTES_ENV, "8192");
        let _capacity = EnvGuard::set(STREAMING_SUBSCRIBER_CAPACITY_ENV, "3");
        let _backpressure = EnvGuard::set(STREAMING_BACKPRESSURE_ENV, "drop");
        let _idle = EnvGuard::set(STREAMING_IDLE_SECS_ENV, "2");
        let _replace = EnvGuard::set(STREAMING_ALLOW_REPLACE_ENV, "true");
        let _throttle = EnvGuard::set(STREAMING_THROTTLE_MS_ENV, "25");

        let config = StreamingBackupConfig::from_env();
        assert!(config.enabled);
        assert_eq!(config.chunk_bytes, 4096);
        assert_eq!(config.buffer_bytes, 8192);
        assert_eq!(config.subscriber_capacity, 3);
        assert_eq!(config.backpressure, BackpressureStrategy::DropNewest);
        assert_eq!(config.idle_ttl, Some(Duration::from_secs(2)));
        assert!(config.allow_replace);
        assert_eq!(config.throttle_interval, Some(Duration::from_millis(25)));
    }

    #[test]
    fn registers_and_clears_streaming_backups() {
        let _guard = stream_guard();
        std::env::set_var(STREAMING_BACKUP_ENV, "on");
        clear_streaming_backups();

        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("artifact.tar");
        std::fs::write(&path, b"payload").expect("write artifact");

        let registered = register_streaming_backup("svc", &path)
            .expect("register")
            .expect("handle missing");

        let fetched = get_streaming_backup(&path);
        assert!(fetched.is_some(), "snapshot should be retrievable");

        drop(registered);
        assert!(
            get_streaming_backup(&path).is_some(),
            "registry should retain default handle"
        );
        remove_streaming_backup(&path);
        assert!(get_streaming_backup(&path).is_none(), "explicit removal");

        clear_streaming_backups();
        std::env::remove_var(STREAMING_BACKUP_ENV);
    }

    #[test]
    fn rejects_duplicate_registration_by_default() {
        std::env::set_var(STREAMING_BACKUP_ENV, "on");
        clear_streaming_backups();

        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("artifact.tar");
        std::fs::write(&path, b"payload").expect("write artifact");

        let first = register_streaming_backup("svc", &path)
            .expect("first register")
            .expect("handle missing");
        let second = register_streaming_backup("svc", &path);
        assert!(
            matches!(second, Err(StreamingRegistryError::Duplicate(_))),
            "expected duplicate error"
        );
        drop(first);
        std::env::remove_var(STREAMING_BACKUP_ENV);
        clear_streaming_backups();
    }

    #[test]
    fn evicts_idle_streaming_backups() {
        let _guard = stream_guard();
        let _flag = EnvGuard::set(STREAMING_BACKUP_ENV, "on");
        let _ttl = EnvGuard::set(STREAMING_IDLE_SECS_ENV, "1");
        clear_streaming_backups();

        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("stale.tar");
        std::fs::write(&path, b"payload").expect("write artifact");

        let _handle = register_streaming_backup("svc", &path)
            .expect("register")
            .expect("handle missing");

        std::thread::sleep(std::time::Duration::from_millis(1100));
        assert!(
            get_streaming_backup(&path).is_none(),
            "stale entry should be evicted"
        );

        clear_streaming_backups();
    }

    #[tokio::test]
    async fn streams_chunks_with_metadata() {
        let _guard = stream_guard_async().await;
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("snapshot.tar");
        let data = b"abcdefghijklmno";
        std::fs::write(&path, data).expect("write snapshot payload");

        let snapshot = StreamingSnapshot::builder("vol-a", &path)
            .chunk_bytes(4)
            .buffer_bytes(16)
            .subscriber_capacity(8)
            .build();
        let mut receiver = snapshot.subscribe();

        let runner = snapshot.clone();
        let handle = tokio::spawn(async move { runner.run().await });

        let mut collected = Vec::new();
        while let Some(event) = receiver.recv().await {
            let chunk = event.expect("chunk error");
            collected.push((
                chunk.chunk_index,
                chunk.offset,
                chunk.bytes.clone(),
                chunk.is_last,
            ));
            if chunk.is_last {
                break;
            }
        }

        let stats = handle
            .await
            .expect("runner task panicked")
            .expect("streaming failed");
        assert_eq!(
            stats,
            StreamingSnapshotStats {
                bytes: data.len() as u64,
                chunks: 4,
                dropped_subscribers: 0,
                delivery_errors: 0
            }
        );

        assert_eq!(collected.len(), 5);
        assert_eq!(collected[0].0, 0);
        assert_eq!(collected[0].1, 0);
        assert_eq!(collected[0].2, Bytes::from_static(b"abcd"));
        assert!(!collected[0].3);

        assert_eq!(collected[1].0, 1);
        assert_eq!(collected[1].1, 4);
        assert_eq!(collected[1].2, Bytes::from_static(b"efgh"));

        assert_eq!(collected[2].0, 2);
        assert_eq!(collected[2].1, 8);
        assert_eq!(collected[2].2, Bytes::from_static(b"ijkl"));

        assert_eq!(collected[3].0, 3);
        assert_eq!(collected[3].1, 12);
        assert_eq!(collected[3].2, Bytes::from_static(b"mno"));

        let final_chunk = &collected[4];
        assert!(final_chunk.3);
        assert_eq!(final_chunk.0, 4);
        assert_eq!(final_chunk.1, data.len() as u64);
        assert_eq!(final_chunk.2.len(), 0);
    }

    #[tokio::test]
    async fn enforces_backpressure_across_subscribers() {
        let _guard = stream_guard_async().await;
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("snapshot.tar");
        let data = b"abcdefgh";
        std::fs::write(&path, data).expect("write snapshot payload");

        let snapshot = StreamingSnapshot::builder("vol-b", &path)
            .chunk_bytes(4)
            .buffer_bytes(4)
            .subscriber_capacity(1)
            .build();
        let mut recv_a = snapshot.subscribe();
        let mut recv_b = snapshot.subscribe();

        let runner = snapshot.clone();
        let handle = tokio::spawn(async move { runner.run().await });

        let first_a = recv_a.recv().await.expect("first chunk missing");
        let first_a = first_a.expect("unexpected error");
        assert_eq!(first_a.chunk_index, 0);
        assert_eq!(first_a.bytes, Bytes::from_static(b"abcd"));

        // Allow the runner to attempt delivering the next chunk; it must block
        // until receiver B drains its buffer.
        sleep(Duration::from_millis(10)).await;
        assert!(
            !handle.is_finished(),
            "runner should be awaiting slow subscriber"
        );

        let first_b = recv_b.recv().await.expect("receiver b missing chunk");
        let first_b = first_b.expect("unexpected error");
        assert_eq!(first_b.chunk_index, 0);
        assert_eq!(first_b.bytes, Bytes::from_static(b"abcd"));

        let second_a = recv_a
            .recv()
            .await
            .expect("receiver a missing second chunk");
        let second_a = second_a.expect("unexpected error");
        assert_eq!(second_a.chunk_index, 1);
        assert_eq!(second_a.bytes, Bytes::from_static(b"efgh"));

        let second_b = recv_b
            .recv()
            .await
            .expect("receiver b missing second chunk");
        let second_b = second_b.expect("unexpected error");
        assert_eq!(second_b.chunk_index, 1);
        assert_eq!(second_b.bytes, Bytes::from_static(b"efgh"));

        let final_a = recv_a.recv().await.expect("receiver a missing final chunk");
        let final_a = final_a.expect("unexpected error");
        assert!(final_a.is_last);

        let final_b = recv_b.recv().await.expect("receiver b missing final chunk");
        let final_b = final_b.expect("unexpected error");
        assert!(final_b.is_last);

        assert!(recv_a.recv().await.is_none());
        assert!(recv_b.recv().await.is_none());

        let stats = handle
            .await
            .expect("runner task panicked")
            .expect("stream failed");
        assert_eq!(
            stats,
            StreamingSnapshotStats {
                bytes: data.len() as u64,
                chunks: 2,
                dropped_subscribers: 0,
                delivery_errors: 0
            }
        );
    }

    #[tokio::test]
    async fn cleans_up_registry_after_stream_completion() {
        let _guard = stream_guard_async().await;
        let _flag = EnvGuard::set(STREAMING_BACKUP_ENV, "on");
        clear_streaming_backups();

        #[derive(Default)]
        struct RegistrationHooks {
            events: Mutex<Vec<&'static str>>,
        }

        impl StreamingHooks for RegistrationHooks {
            fn on_register(&self, _: &Path, _: u64) {
                self.events
                    .lock()
                    .expect("register hook lock")
                    .push("register");
            }

            fn on_unregister(&self, _: &Path, _: u64) {
                self.events
                    .lock()
                    .expect("unregister hook lock")
                    .push("unregister");
            }
        }

        let hooks = Arc::new(RegistrationHooks::default());
        let _hook_guard = install_hooks(hooks.clone());

        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("cleanup.tar");
        std::fs::write(&path, b"abcdefg").expect("write artifact");

        let registration = register_streaming_backup("svc", &path)
            .expect("register")
            .expect("handle missing");
        let snapshot = registration.snapshot();

        let mut receiver = snapshot.subscribe();
        let runner = snapshot.clone();
        let handle = tokio::spawn(async move { runner.run().await });

        while let Some(event) = receiver.recv().await {
            if event.as_ref().map(|chunk| chunk.is_last).unwrap_or(false) {
                break;
            }
        }

        let _ = handle
            .await
            .expect("runner panicked")
            .expect("stream failed");
        assert!(
            get_streaming_backup(&path).is_none(),
            "stream completion should remove registry entry"
        );

        drop(registration);
        let events = hooks.events.lock().expect("hook lock");
        assert_eq!(
            events.iter().filter(|value| **value == "register").count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|value| **value == "unregister")
                .count(),
            1
        );

        clear_streaming_backups();
    }

    #[tokio::test]
    async fn cancels_streaming_with_token() {
        let _guard = stream_guard_async().await;
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("snapshot_cancel.tar");
        let data = b"abcdef";
        std::fs::write(&path, data).expect("write snapshot payload");

        let snapshot = StreamingSnapshot::builder("vol-c", &path)
            .chunk_bytes(2)
            .buffer_bytes(4)
            .subscriber_capacity(2)
            .build();
        let mut receiver = snapshot.subscribe();
        let token = CancellationToken::new();

        let runner = snapshot.clone();
        let cancel_clone = token.clone();
        let handle = tokio::spawn(async move {
            runner
                .run_with_options(StreamingRunOptions {
                    cancellation: Some(cancel_clone),
                })
                .await
        });

        let _ = receiver.recv().await;
        token.cancel();

        let result = handle.await.expect("runner panicked");
        assert!(result.is_err(), "expected cancellation error");
    }

    #[tokio::test]
    async fn drops_slow_subscriber_with_drop_newest_strategy() {
        let _guard = stream_guard_async().await;
        #[derive(Default)]
        struct DropHooks {
            drops: Mutex<Vec<(usize, usize)>>,
        }

        impl StreamingHooks for DropHooks {
            fn on_backpressure_drop(&self, _: &Path, _: u64, dropped: usize, failed: usize) {
                self.drops
                    .lock()
                    .expect("drops hook lock")
                    .push((dropped, failed));
            }
        }

        let hooks = Arc::new(DropHooks::default());
        let _hook_guard = install_hooks(hooks.clone());

        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("snapshot_drop.tar");
        std::fs::write(&path, b"abcdefgh").expect("write snapshot payload");

        let snapshot = StreamingSnapshot::builder("vol-drop", &path)
            .chunk_bytes(2)
            .buffer_bytes(2)
            .subscriber_capacity(1)
            .backpressure(BackpressureStrategy::DropNewest)
            .build();
        let mut fast = snapshot.subscribe();
        let _slow = snapshot.subscribe();

        let runner = snapshot.clone();
        let handle = tokio::spawn(async move { runner.run().await });

        let first = fast
            .recv()
            .await
            .expect("fast recv missing")
            .expect("chunk");
        assert_eq!(first.chunk_index, 0);

        let result = handle.await.expect("runner panicked");
        assert!(
            result.is_err(),
            "drop newest should surface delivery failure"
        );

        let _ = fast.recv().await;

        let drops = hooks.drops.lock().expect("drops hook guard");
        assert!(!drops.is_empty(), "expected at least one drop event");
        let (dropped, failed) = drops.last().copied().unwrap_or((0, 0));
        assert!(dropped >= 1);
        assert!(failed >= 1);
    }

    #[tokio::test]
    async fn surfaces_mid_stream_io_errors() {
        let _guard = stream_guard_async().await;
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("snapshot_injected_error.tar");
        std::fs::write(&path, b"abcdefgh").expect("write snapshot payload");

        let snapshot = StreamingSnapshot::builder("vol-error", &path)
            .chunk_bytes(2)
            .buffer_bytes(4)
            .subscriber_capacity(2)
            .fail_after_chunk(1)
            .build();

        let mut receiver = snapshot.subscribe();
        let runner = snapshot.clone();
        let handle = tokio::spawn(async move { runner.run().await });

        let first = receiver.recv().await.expect("first chunk missing");
        let first = first.expect("unexpected error");
        assert_eq!(first.chunk_index, 0);

        let error_event = receiver.recv().await.expect("error event missing");
        assert!(error_event.is_err(), "expected mid-stream error broadcast");

        let result = handle.await.expect("runner panicked");
        assert!(result.is_err(), "stream should fail after injected error");
    }

    #[tokio::test]
    async fn reuses_chunk_buffers_between_subscribers() {
        let _guard = stream_guard_async().await;
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("snapshot_shared.tar");
        let data = b"abcdefgh";
        std::fs::write(&path, data).expect("write snapshot payload");

        let snapshot = StreamingSnapshot::builder("vol-shared", &path)
            .chunk_bytes(4)
            .buffer_bytes(8)
            .subscriber_capacity(4)
            .build();
        let mut recv_a = snapshot.subscribe();
        let mut recv_b = snapshot.subscribe();

        let runner = snapshot.clone();
        let handle = tokio::spawn(async move { runner.run().await });

        let first_a = recv_a.recv().await.expect("receiver a missing first chunk");
        let first_a = first_a.expect("unexpected error");
        let first_b = recv_b.recv().await.expect("receiver b missing first chunk");
        let first_b = first_b.expect("unexpected error");

        assert!(
            ptr::eq(first_a.bytes.as_ptr(), first_b.bytes.as_ptr()),
            "bytes should share underlying buffer"
        );

        while let Some(event) = recv_a.recv().await {
            if event.expect("receiver a error").is_last {
                break;
            }
        }
        while let Some(event) = recv_b.recv().await {
            if event.expect("receiver b error").is_last {
                break;
            }
        }

        let stats = handle
            .await
            .expect("runner panicked")
            .expect("stream failed");
        assert_eq!(stats.bytes, data.len() as u64);
    }

    #[tokio::test]
    async fn integrates_profile_snapshot_and_streaming_registration() {
        let _guard = stream_guard_async().await;
        let _flag = EnvGuard::set(STREAMING_BACKUP_ENV, "1");
        clear_streaming_backups();

        let dir = tempdir().expect("tempdir");
        let snapshot_path = dir.path().join("integration_snapshot.tar");

        let mut config_bytes = HashMap::new();
        config_bytes.insert("config".to_string(), b"value".to_vec());
        let profile = Profile {
            key: EncryptionKey::new(Some(vec![0; 32])),
            config: config_bytes,
            bindings: HashMap::new(),
        };
        let mut overrides = HashMap::new();
        overrides.insert("extra".to_string(), "setting".to_string());
        let serialized_options = profile.extend(&overrides);
        let profile_key = "integration-key".to_string();

        let bundle = BundleSpec {
            service: "svc".into(),
            namespace: Some("ns".into()),
            options: serialized_options.clone(),
            profile_key: Some(profile_key.clone()),
            snapshot: None,
            start: true,
            update: false,
            security: None,
            runtime: None,
        };

        let manifest = json!({
            "namespace": "ns",
            "service": "svc",
            "createdAt": "2024-01-01T00:00:00Z",
            "snapshots": [{
                "claim": "data",
                "volume_id": "vol-1",
                "snapshot_id": "snap-1",
                "archivePath": "snapshots/vol-1.tar",
                "sizeBytes": 12
            }]
        });

        {
            let file = std::fs::File::create(&snapshot_path).expect("create snapshot file");
            let mut builder = Builder::new(file);

            let spec_bytes = serde_json::to_vec(&bundle).expect("serialize bundle");
            let mut spec_header = Header::new_gnu();
            spec_header.set_size(spec_bytes.len() as u64);
            spec_header.set_mode(0o644);
            spec_header.set_cksum();
            builder
                .append_data(&mut spec_header, "spec.json", spec_bytes.as_slice())
                .expect("append spec");

            let manifest_bytes = serde_json::to_vec_pretty(&manifest).expect("serialize manifest");
            let mut manifest_header = Header::new_gnu();
            manifest_header.set_size(manifest_bytes.len() as u64);
            manifest_header.set_mode(0o644);
            manifest_header.set_cksum();
            builder
                .append_data(
                    &mut manifest_header,
                    "manifest.json",
                    manifest_bytes.as_slice(),
                )
                .expect("append manifest");

            let volume_bytes = b"hello-volume";
            let mut volume_header = Header::new_gnu();
            volume_header.set_size(volume_bytes.len() as u64);
            volume_header.set_mode(0o644);
            volume_header.set_cksum();
            builder
                .append_data(&mut volume_header, "snapshots/vol-1.tar", &volume_bytes[..])
                .expect("append volume");

            builder.finish().expect("finish tar");
        }

        let snapshot = Snapshot::new(snapshot_path.to_str().expect("path text")).expect("snapshot");
        let parsed_spec = snapshot.read_spec().expect("read spec");
        assert_eq!(parsed_spec.service, "svc");
        assert_eq!(
            parsed_spec.profile_key.as_deref(),
            Some(profile_key.as_str())
        );
        assert_eq!(parsed_spec.options, serialized_options);

        let summary = snapshot
            .summary()
            .expect("summary read")
            .expect("missing manifest");
        assert_eq!(summary.entries.len(), 1);
        assert_eq!(summary.entries[0].snapshot_id, "snap-1");

        let registration = register_streaming_backup_with_config(
            "svc",
            &snapshot_path,
            &StreamingBackupConfig {
                enabled: true,
                allow_replace: true,
                ..StreamingBackupConfig::default()
            },
        )
        .expect("register")
        .expect("handle missing");
        let stream = registration.snapshot();
        let mut receiver = stream.subscribe();
        let runner = stream.clone();
        let handle = tokio::spawn(async move { runner.run().await });

        let mut observed_bytes = 0usize;
        while let Some(event) = receiver.recv().await {
            let chunk = event.expect("chunk");
            observed_bytes += chunk.bytes.len();
            if chunk.is_last {
                break;
            }
        }

        let stats = handle
            .await
            .expect("runner panicked")
            .expect("stream failed");
        let file_len = std::fs::metadata(&snapshot_path)
            .expect("stat snapshot")
            .len() as usize;
        assert_eq!(stats.bytes as usize, file_len);
        assert_eq!(observed_bytes, file_len);
        assert!(
            get_streaming_backup(&snapshot_path).is_none(),
            "streaming completion should cleanup registration"
        );

        clear_streaming_backups();
    }

    #[tokio::test]
    async fn throttles_streaming_when_configured() {
        let _guard = stream_guard_async().await;
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("snapshot_throttled.tar");
        let data = b"throttle";
        std::fs::write(&path, data).expect("write snapshot payload");

        let snapshot = StreamingSnapshot::builder("vol-throttle", &path)
            .chunk_bytes(1)
            .buffer_bytes(4)
            .subscriber_capacity(2)
            .throttle_interval(Some(Duration::from_millis(15)))
            .build();
        let mut receiver = snapshot.subscribe();
        let runner = snapshot.clone();

        let start = std::time::Instant::now();
        let handle = tokio::spawn(async move { runner.run().await });
        while let Some(event) = receiver.recv().await {
            if event.as_ref().map(|chunk| chunk.is_last).unwrap_or(false) {
                break;
            }
        }

        let _ = handle
            .await
            .expect("runner panicked")
            .expect("stream failed unexpectedly");
        assert!(
            start.elapsed() >= std::time::Duration::from_millis(60),
            "throttling should delay chunk delivery"
        );
    }
}
