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

pub mod container;
mod image;
pub mod log;
mod profile;
mod snapshot;
mod streaming;

pub use image::Image;
pub use profile::Profile;
#[allow(unused_imports)]
// Re-exported for downstream crates/tests that rely on the engine snapshot API.
pub use snapshot::{Snapshot, SnapshotSummary, SnapshotVolumeEntry};
#[allow(unused_imports)] // Re-exported streaming helpers form part of the public backup API.
pub use streaming::{
    get_streaming_backup, register_streaming_backup, remove_streaming_backup,
    streaming_backup_enabled, SnapshotChunk, SnapshotChunkResult, StreamingSnapshot,
    StreamingSnapshotBuilder, StreamingSnapshotError, StreamingSnapshotStats,
};
