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

use bytes::Bytes;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::env;
use std::fmt::{self, Display, Formatter};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::fs::File;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio_util::io::ReaderStream;

const DEFAULT_CHUNK_BYTES: usize = 128 * 1024;
const DEFAULT_BUFFER_BYTES: usize = 2 * 1024 * 1024;
const STREAMING_BACKUP_ENV: &str = "NANOCLOUD_STREAMING_BACKUP";

/// Returns true when the streaming backup pipeline is enabled via feature flag.
pub fn streaming_backup_enabled() -> bool {
    match env::var(STREAMING_BACKUP_ENV) {
        Ok(value) => {
            let upper = value.trim().to_ascii_uppercase();
            matches!(upper.as_str(), "1" | "TRUE" | "ON")
        }
        Err(_) => false,
    }
}

/// Result type emitted to streaming subscribers.
pub type SnapshotChunkResult = Result<SnapshotChunk, StreamingSnapshotError>;

#[derive(Clone, Debug)]
struct Subscriber {
    id: usize,
    sender: Sender<SnapshotChunkResult>,
}

struct StreamingSnapshotInner {
    volume: Arc<str>,
    path: PathBuf,
    chunk_bytes: usize,
    buffer_bytes: usize,
    subscribers: Mutex<Vec<Subscriber>>,
    next_id: AtomicUsize,
}

impl StreamingSnapshotInner {
    fn new(volume: Arc<str>, path: PathBuf, chunk_bytes: usize, buffer_bytes: usize) -> Self {
        StreamingSnapshotInner {
            volume,
            path,
            chunk_bytes,
            buffer_bytes,
            subscribers: Mutex::new(Vec::new()),
            next_id: AtomicUsize::new(0),
        }
    }

    fn register(&self, sender: Sender<SnapshotChunkResult>) {
        let id = self.next_id.fetch_add(1, Ordering::AcqRel);
        let mut guard = self
            .subscribers
            .lock()
            .expect("streaming snapshot subscriber lock poisoned");
        guard.push(Subscriber { id, sender });
    }

    fn snapshot_subscribers(&self) -> Vec<Subscriber> {
        let guard = self
            .subscribers
            .lock()
            .expect("streaming snapshot subscriber lock poisoned");
        guard.clone()
    }

    fn remove_subscribers(&self, ids: &[usize]) {
        if ids.is_empty() {
            return;
        }
        let mut guard = self
            .subscribers
            .lock()
            .expect("streaming snapshot subscriber lock poisoned");
        guard.retain(|subscriber| !ids.contains(&subscriber.id));
    }

    fn has_subscribers(&self) -> bool {
        let guard = self
            .subscribers
            .lock()
            .expect("streaming snapshot subscriber lock poisoned");
        !guard.is_empty()
    }

    async fn broadcast(&self, event: SnapshotChunkResult) {
        let subscribers = self.snapshot_subscribers();
        if subscribers.is_empty() {
            return;
        }

        let mut dropped = Vec::new();
        for subscriber in subscribers {
            if subscriber.sender.send(event.clone()).await.is_err() {
                dropped.push(subscriber.id);
            }
        }

        if !dropped.is_empty() {
            self.remove_subscribers(&dropped);
        }
    }

    fn close(&self) {
        let mut guard = self
            .subscribers
            .lock()
            .expect("streaming snapshot subscriber lock poisoned");
        guard.clear();
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
        }
    }
}

impl std::error::Error for StreamingSnapshotError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.kind.as_ref() {
            StreamingSnapshotErrorKind::Open { error, .. }
            | StreamingSnapshotErrorKind::Read { error, .. } => Some(error.as_ref()),
        }
    }
}

/// Builder for configuring a [`StreamingSnapshot`] instance.
pub struct StreamingSnapshotBuilder {
    volume: String,
    path: PathBuf,
    chunk_bytes: usize,
    buffer_bytes: usize,
}

impl StreamingSnapshotBuilder {
    pub fn build(self) -> StreamingSnapshot {
        StreamingSnapshot {
            inner: Arc::new(StreamingSnapshotInner::new(
                Arc::<str>::from(self.volume),
                self.path,
                self.chunk_bytes,
                self.buffer_bytes,
            )),
        }
    }
}

/// Streaming reader for snapshot archives backed by [`ReaderStream`].
#[derive(Clone)]
pub struct StreamingSnapshot {
    inner: Arc<StreamingSnapshotInner>,
}

impl StreamingSnapshot {
    pub fn builder(
        volume: impl Into<String>,
        path: impl Into<PathBuf>,
    ) -> StreamingSnapshotBuilder {
        StreamingSnapshotBuilder {
            volume: volume.into(),
            path: path.into(),
            chunk_bytes: DEFAULT_CHUNK_BYTES,
            buffer_bytes: DEFAULT_BUFFER_BYTES,
        }
    }

    pub fn subscribe(&self) -> Receiver<SnapshotChunkResult> {
        let capacity = std::cmp::max(1, self.inner.buffer_bytes.div_ceil(self.inner.chunk_bytes));
        let (sender, receiver) = mpsc::channel(capacity);
        self.inner.register(sender);
        receiver
    }

    pub async fn run(&self) -> Result<StreamingSnapshotStats, StreamingSnapshotError> {
        let path = self.inner.path.clone();
        let file = match File::open(&path).await {
            Ok(file) => file,
            Err(error) => {
                let err = StreamingSnapshotError::open(path.clone(), error);
                self.inner.broadcast(Err(err.clone())).await;
                return Err(err);
            }
        };

        let mut reader = ReaderStream::with_capacity(file, self.inner.chunk_bytes);
        let mut stats = StreamingSnapshotStats::default();
        let mut chunk_index = 0u64;
        let mut offset = 0u64;

        while let Some(result) = reader.next().await {
            let bytes = match result {
                Ok(bytes) => bytes,
                Err(error) => {
                    let err = StreamingSnapshotError::read(path.clone(), error);
                    self.inner.broadcast(Err(err.clone())).await;
                    return Err(err);
                }
            };

            let len = bytes.len() as u64;
            stats.bytes += len;
            stats.chunks += 1;

            let chunk = SnapshotChunk {
                volume: Arc::clone(&self.inner.volume),
                chunk_index,
                offset,
                bytes,
                is_last: false,
            };

            chunk_index += 1;
            offset += len;
            self.inner.broadcast(Ok(chunk)).await;
        }

        if self.inner.has_subscribers() {
            let final_chunk = SnapshotChunk {
                volume: Arc::clone(&self.inner.volume),
                chunk_index,
                offset,
                bytes: Bytes::new(),
                is_last: true,
            };
            self.inner.broadcast(Ok(final_chunk)).await;
        }

        self.inner.close();
        Ok(stats)
    }
}

#[cfg(test)]
impl StreamingSnapshotBuilder {
    pub fn chunk_bytes(mut self, chunk_bytes: usize) -> Self {
        self.chunk_bytes = chunk_bytes.max(1);
        self
    }

    pub fn buffer_bytes(mut self, buffer_bytes: usize) -> Self {
        self.buffer_bytes = buffer_bytes.max(1);
        self
    }
}

struct StreamingSnapshotRegistry {
    entries: Mutex<HashMap<PathBuf, StreamingSnapshot>>,
}

impl StreamingSnapshotRegistry {
    fn new() -> Self {
        StreamingSnapshotRegistry {
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn insert(&self, path: PathBuf, snapshot: StreamingSnapshot) {
        let mut guard = self
            .entries
            .lock()
            .expect("streaming snapshot registry lock poisoned");
        guard.insert(path, snapshot);
    }

    fn get(&self, path: &Path) -> Option<StreamingSnapshot> {
        let guard = self
            .entries
            .lock()
            .expect("streaming snapshot registry lock poisoned");
        guard.get(path).cloned()
    }

    fn remove(&self, path: &Path) -> Option<StreamingSnapshot> {
        let mut guard = self
            .entries
            .lock()
            .expect("streaming snapshot registry lock poisoned");
        guard.remove(path)
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

static STREAMING_REGISTRY: OnceLock<StreamingSnapshotRegistry> = OnceLock::new();

fn registry() -> &'static StreamingSnapshotRegistry {
    STREAMING_REGISTRY.get_or_init(StreamingSnapshotRegistry::new)
}

/// Registers a streaming snapshot for the given artifact path when the feature flag is enabled.
pub fn register_streaming_backup(
    volume_label: impl Into<String>,
    path: impl AsRef<Path>,
) -> Option<StreamingSnapshot> {
    if !streaming_backup_enabled() {
        return None;
    }
    let path_ref = path.as_ref();
    let snapshot = StreamingSnapshot::builder(volume_label, path_ref).build();
    registry().insert(path_ref.to_path_buf(), snapshot.clone());
    Some(snapshot)
}

/// Fetches a previously registered streaming snapshot, if present.
pub fn get_streaming_backup(path: impl AsRef<Path>) -> Option<StreamingSnapshot> {
    registry().get(path.as_ref())
}

/// Removes a registered streaming snapshot for the given path, if present.
pub fn remove_streaming_backup(path: impl AsRef<Path>) {
    let _ = registry().remove(path.as_ref());
}

#[cfg(test)]
pub fn clear_streaming_backups() {
    if let Some(registry) = STREAMING_REGISTRY.get() {
        registry.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::time::{sleep, Duration};

    #[test]
    fn streaming_feature_flag_defaults_off_and_reads_env() {
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
    fn registers_and_clears_streaming_backups() {
        std::env::set_var(STREAMING_BACKUP_ENV, "on");
        clear_streaming_backups();

        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("artifact.tar");
        std::fs::write(&path, b"payload").expect("write artifact");

        let registered = register_streaming_backup("svc", &path);
        assert!(registered.is_some(), "registry should accept snapshot");

        let fetched = get_streaming_backup(&path);
        assert!(fetched.is_some(), "snapshot should be retrievable");

        remove_streaming_backup(&path);
        assert!(get_streaming_backup(&path).is_none());

        clear_streaming_backups();
        std::env::remove_var(STREAMING_BACKUP_ENV);
    }

    #[tokio::test]
    async fn streams_chunks_with_metadata() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("snapshot.tar");
        let data = b"abcdefghijklmno";
        std::fs::write(&path, data).expect("write snapshot payload");

        let snapshot = StreamingSnapshot::builder("vol-a", &path)
            .chunk_bytes(4)
            .buffer_bytes(16)
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
                chunks: 4
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
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("snapshot.tar");
        let data = b"abcdefgh";
        std::fs::write(&path, data).expect("write snapshot payload");

        let snapshot = StreamingSnapshot::builder("vol-b", &path)
            .chunk_bytes(4)
            .buffer_bytes(4)
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
                chunks: 2
            }
        );
    }
}
