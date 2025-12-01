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

//! Execution and storage engine used by Nanocloud services.
//!
//! The public surface is intended for workspace consumers and covers container lifecycle helpers,
//! snapshot/streaming backup APIs, bindings, and profile utilities. Streaming backups are gated by
//! the `NANOCLOUD_STREAMING_BACKUP` flag and can be tuned via `NANOCLOUD_STREAMING_CHUNK_BYTES`,
//! `NANOCLOUD_STREAMING_BUFFER_BYTES`, `NANOCLOUD_STREAMING_SUBSCRIBER_CAPACITY`,
//! `NANOCLOUD_STREAMING_BACKPRESSURE`, `NANOCLOUD_STREAMING_ALLOW_REPLACE`,
//! `NANOCLOUD_STREAMING_IDLE_SECS`, and `NANOCLOUD_STREAMING_THROTTLE_MS`. Defaults preserve the
//! current behavior when flags are unset.

mod bindings;
pub mod container;
mod image;
pub mod log;
pub mod profile;
mod snapshot;
mod streaming;

#[allow(unused_imports)]
/// Binding invocation helpers considered stable for callers that embed bundle execution.
pub use bindings::{BindingEnvelopePolicy, BindingInvocation, BindingResult};
/// OCI image helper used by the container runtime.
pub use image::Image;
/// Profile management used by container/profile orchestration.
pub use profile::Profile;
#[allow(unused_imports)]
/// Snapshot helpers for saving and querying on-disk artifacts.
pub use snapshot::{Snapshot, SnapshotSummary, SnapshotVolumeEntry};
#[allow(unused_imports)] // Re-exported streaming helpers form part of the public backup API.
/// Streaming backup helpers (behind the `NANOCLOUD_STREAMING_BACKUP` flag). These remain
/// experimental; consumers should treat them as best-effort and honor documented feature flags.
pub use streaming::{
    get_streaming_backup, register_streaming_backup, remove_streaming_backup, set_streaming_hooks,
    streaming_backup_enabled, SnapshotChunk, SnapshotChunkResult, StreamingHooks,
    StreamingSnapshot, StreamingSnapshotBuilder, StreamingSnapshotError, StreamingSnapshotStats,
};
