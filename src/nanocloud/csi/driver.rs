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

//! CSI driver entrypoint. Mutating operations are serialized per volume via async locks and
//! snapshot creation enforces size/depth and symlink policies configurable through
//! `NANOCLOUD_CSI_SNAPSHOT_*` environment variables. Publish targets must live under the
//! `publish_root` (`NANOCLOUD_CSI_ROOT/publish` by default), and service indices are stored
//! under the keyspace keys `/services/{namespace}/{service}` listing volume IDs for that service.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

#[allow(unused_imports)]
pub use encryption::CreateVolumeEncryptionConfig;
#[allow(unused_imports)]
pub use metadata::{StoredEncryptedVolume, StoredSnapshot, StoredVolume};
#[allow(unused_imports)]
pub use snapshot::{SnapshotOptions, SnapshotSymlinkPolicy};

use locks::VolumeLockRegistry;
use paths::publish_root_path;
use std::time::Instant;

mod encryption;
mod lifecycle;
mod locks;
mod metadata;
mod observer;
mod paths;
mod publish;
mod rollback;
mod snapshot;
mod validation;

pub struct CsiDriver {
    publish_root: PathBuf,
    locks: Arc<VolumeLockRegistry>,
    snapshot_options: SnapshotOptions,
    observer: Arc<dyn observer::CsiObserver>,
}

static INSTANCE: OnceLock<Arc<CsiDriver>> = OnceLock::new();
static SNAPSHOT_OPTIONS: OnceLock<SnapshotOptions> = OnceLock::new();
static OBSERVER: OnceLock<Arc<dyn observer::CsiObserver>> = OnceLock::new();

impl CsiDriver {
    pub fn shared() -> Arc<CsiDriver> {
        INSTANCE
            .get_or_init(|| {
                paths::ensure_storage_roots().expect("failed to prepare CSI storage roots");
                Arc::new(CsiDriver {
                    publish_root: publish_root_path(),
                    locks: VolumeLockRegistry::global(),
                    snapshot_options: SNAPSHOT_OPTIONS
                        .get_or_init(SnapshotOptions::default)
                        .clone(),
                    observer: OBSERVER
                        .get_or_init(observer::NoopObserver::arc)
                        .clone(),
                })
            })
            .clone()
    }

    pub fn publish_root(&self) -> &Path {
        &self.publish_root
    }

    #[cfg(test)]
    pub fn for_test(snapshot_options: SnapshotOptions) -> Arc<CsiDriver> {
        Self::for_test_with(snapshot_options, observer::NoopObserver::arc())
    }

    #[cfg(test)]
    pub fn for_test_with(
        snapshot_options: SnapshotOptions,
        observer: Arc<dyn observer::CsiObserver>,
    ) -> Arc<CsiDriver> {
        paths::ensure_storage_roots().expect("failed to prepare CSI storage roots");
        Arc::new(CsiDriver {
            publish_root: publish_root_path(),
            locks: VolumeLockRegistry::global(),
            snapshot_options,
            observer,
        })
    }

    fn op_start(&self, op: &str) -> Instant {
        self.observer.on_operation_start(op);
        Instant::now()
    }

    fn op_finish(&self, op: &str, started: Instant, result: Result<(), String>) {
        let _ = started;
        self.observer.on_operation_end(op, result);
    }

    fn track_lock_wait(&self, op: &str, volume: &str, started: Instant) {
        let wait = started.elapsed().as_millis();
        self.observer.on_lock_wait(op, volume, wait);
    }
}

impl CsiDriver {
    /// Configure global snapshot options (limits, symlink policy, throttling) for the shared driver.
    /// Call before `CsiDriver::shared()`; subsequent calls are ignored.
    #[allow(dead_code)]
    pub fn configure_snapshot_options(
        options: SnapshotOptions,
    ) -> Result<(), SnapshotOptions> {
        SNAPSHOT_OPTIONS.set(options)
    }

    /// Register an observer for lifecycle and snapshot events. Call before `CsiDriver::shared()`.
    #[allow(dead_code)]
    pub fn register_observer(
        observer: Arc<dyn observer::CsiObserver>,
    ) -> Result<(), Arc<dyn observer::CsiObserver>> {
        OBSERVER.set(observer)
    }
}

#[cfg(test)]
mod tests {
    use super::paths::*;

    #[test]
    fn key_and_path_helpers_produce_expected_locations() {
        assert_eq!(volume_key("vol-1"), "/volumes/vol-1");
        assert_eq!(service_key("ns", "svc"), "/services/ns/svc");
        assert_eq!(snapshot_key("snap"), "/snapshots/snap");
        assert_eq!(
            volume_path("vol-1"),
            storage_root().join(VOLUMES_DIR).join("vol-1")
        );
        assert_eq!(
            publication_path("/mnt/data"),
            std::path::PathBuf::from("/mnt/data")
        );
        assert_eq!(
            snapshot_archive_path("snap"),
            storage_root().join(SNAPSHOTS_DIR).join("snap.tar")
        );
    }
}

#[cfg(test)]
mod integration_tests;
