//! Snapshot creation utilities with guardrails for symlink traversal and size/depth limits.
//! Environment overrides:
//! - `NANOCLOUD_CSI_SNAPSHOT_MAX_BYTES`: caps total archived bytes (default: 20GiB).
//! - `NANOCLOUD_CSI_SNAPSHOT_MAX_DEPTH`: caps recursion depth (default: 32).
//! - `NANOCLOUD_CSI_SNAPSHOT_SYMLINKS`: one of `archive` (default), `skip`, or `error`.
//! - `NANOCLOUD_CSI_SNAPSHOT_THROTTLE_BYTES`: optional byte threshold to sleep after each chunk.
//! - `NANOCLOUD_CSI_SNAPSHOT_THROTTLE_SLEEP_MS`: sleep duration when throttling (default: 10ms).
//!   Canonicalization ensures entries stay under `NANOCLOUD_CSI_ROOT`, and symlinks pointing
//!   outside the storage root are rejected unless explicitly skipped.

use std::error::Error;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use tar::{Archive, Builder, EntryType, Header};

use crate::nanocloud::csi::types::Snapshot;
use crate::nanocloud::util::error::{new_error, with_context};

use super::metadata::{
    delete_snapshot_record, ensure_volume_exists, load_snapshot, now_rfc3339, persist_snapshot,
    StoredSnapshot, StoredVolume,
};
use super::paths::{snapshot_archive_path, storage_root};
use super::rollback::Rollback;
use super::validation::sanitize_name;
use super::CsiDriver;
use crate::nanocloud::csi::types::{
    CreateSnapshotRequest, CreateSnapshotResponse, DeleteSnapshotRequest,
};

const DEFAULT_MAX_DEPTH: usize = 32;
const DEFAULT_MAX_BYTES: u64 = 20 * 1024 * 1024 * 1024; // 20 GiB

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotSymlinkPolicy {
    Archive,
    Skip,
    Error,
}

#[derive(Debug, Clone)]
pub struct SnapshotOptions {
    pub max_total_bytes: Option<u64>,
    pub max_depth: usize,
    pub symlink_policy: SnapshotSymlinkPolicy,
    pub throttle_bytes_per_chunk: Option<u64>,
    pub throttle_sleep: Duration,
}

impl Default for SnapshotOptions {
    fn default() -> Self {
        SnapshotOptions {
            max_total_bytes: snapshot_max_bytes_from_env(),
            max_depth: snapshot_max_depth_from_env(),
            symlink_policy: snapshot_symlink_policy_from_env(),
            throttle_bytes_per_chunk: snapshot_throttle_bytes_from_env(),
            throttle_sleep: snapshot_throttle_sleep_from_env(),
        }
    }
}

impl CsiDriver {
    pub async fn create_snapshot(
        &self,
        request: CreateSnapshotRequest,
    ) -> Result<CreateSnapshotResponse, Box<dyn Error + Send + Sync>> {
        let started = self.op_start("create_snapshot");
        let result: Result<CreateSnapshotResponse, Box<dyn Error + Send + Sync>> = async {
            let lock_wait = Instant::now();
            let _lock = self.locks.lock(&request.source_volume_id).await;
            self.track_lock_wait("create_snapshot", &request.source_volume_id, lock_wait);
            let stored = ensure_volume_exists(&request.source_volume_id)?;

            let snapshot_id = if request.name.trim().is_empty() {
                format!("{}-snapshot", stored.volume.volume_id)
            } else {
                sanitize_name(&request.name, "snapshot")
            };

            let mut rollback = Rollback::new();
            let archive_path = super::paths::snapshot_archive_path(&snapshot_id);
            let archive_cleanup = archive_path.clone();
            rollback.push(move || {
                if archive_cleanup.exists() {
                    fs::remove_file(&archive_cleanup).map_err(|e| {
                        with_context(e, format!("Failed to remove {}", archive_cleanup.display()))
                    })?;
                }
                Ok(())
            });

            let began = Instant::now();
            let (snapshot, stored_snapshot) =
                create_snapshot(&stored, &snapshot_id, &self.snapshot_options)?;
            let elapsed = began.elapsed().as_millis();

            persist_snapshot(&stored_snapshot)?;
            rollback.commit();
            self.observer
                .on_snapshot_complete(&snapshot.snapshot_id, snapshot.size_bytes, elapsed);

            Ok(CreateSnapshotResponse {
                snapshot,
                archive_path: stored_snapshot.archive_path,
            })
        }
            .await;

        match result {
            Ok(resp) => {
                self.op_finish("create_snapshot", started, Ok(()));
                Ok(resp)
            }
            Err(err) => {
                self.op_finish("create_snapshot", started, Err(format!("{err}")));
                Err(err)
            }
        }
    }

    pub async fn delete_snapshot(
        &self,
        request: DeleteSnapshotRequest,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let started = self.op_start("delete_snapshot");
        let result: Result<(), Box<dyn Error + Send + Sync>> = async {
            let lock_wait = Instant::now();
            let _lock = self.locks.lock(&request.snapshot_id).await;
            self.track_lock_wait("delete_snapshot", &request.snapshot_id, lock_wait);
            let stored = match load_snapshot(&request.snapshot_id)? {
                Some(stored) => stored,
                None => return Ok(()),
            };
            if Path::new(&stored.archive_path).exists() {
                fs::remove_file(&stored.archive_path).map_err(|e| {
                    with_context(
                        e,
                        format!("Failed to remove snapshot archive {}", stored.archive_path),
                    )
                })?;
            }
            delete_snapshot_record(&request.snapshot_id)?;
            Ok(())
        }
        .await;

        match result {
            Ok(()) => {
                self.op_finish("delete_snapshot", started, Ok(()));
                Ok(())
            }
            Err(err) => {
                self.op_finish("delete_snapshot", started, Err(format!("{err}")));
                Err(err)
            }
        }
    }

    pub fn restore_from_snapshot(
        &self,
        snapshot_id: &str,
        path: &Path,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let stored = self
            .load_snapshot(snapshot_id)?
            .ok_or_else(|| new_error(format!("Snapshot '{}' not found", snapshot_id)))?;
        restore_from_snapshot(&stored, path)
    }
}

fn snapshot_max_bytes_from_env() -> Option<u64> {
    match std::env::var("NANOCLOUD_CSI_SNAPSHOT_MAX_BYTES") {
        Ok(value) => value.parse::<u64>().ok().filter(|limit| *limit > 0),
        Err(_) => Some(DEFAULT_MAX_BYTES),
    }
}

fn snapshot_max_depth_from_env() -> usize {
    match std::env::var("NANOCLOUD_CSI_SNAPSHOT_MAX_DEPTH") {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|d| *d > 0)
            .unwrap_or(DEFAULT_MAX_DEPTH),
        Err(_) => DEFAULT_MAX_DEPTH,
    }
}

fn snapshot_symlink_policy_from_env() -> SnapshotSymlinkPolicy {
    match std::env::var("NANOCLOUD_CSI_SNAPSHOT_SYMLINKS") {
        Ok(value) => match value.to_ascii_lowercase().as_str() {
            "skip" => SnapshotSymlinkPolicy::Skip,
            "error" => SnapshotSymlinkPolicy::Error,
            _ => SnapshotSymlinkPolicy::Archive,
        },
        Err(_) => SnapshotSymlinkPolicy::Archive,
    }
}

fn snapshot_throttle_bytes_from_env() -> Option<u64> {
    match std::env::var("NANOCLOUD_CSI_SNAPSHOT_THROTTLE_BYTES") {
        Ok(value) => value.parse::<u64>().ok().filter(|v| *v > 0),
        Err(_) => None,
    }
}

fn snapshot_throttle_sleep_from_env() -> Duration {
    match std::env::var("NANOCLOUD_CSI_SNAPSHOT_THROTTLE_SLEEP_MS") {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_millis(10)),
        Err(_) => Duration::from_millis(10),
    }
}

pub fn create_snapshot(
    stored_volume: &StoredVolume,
    snapshot_id: &str,
    options: &SnapshotOptions,
) -> Result<(Snapshot, StoredSnapshot), Box<dyn Error + Send + Sync>> {
    let archive_path = snapshot_archive_path(snapshot_id);
    if let Some(parent) = archive_path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            with_context(
                e,
                format!("Failed to create snapshot directory {}", parent.display()),
            )
        })?;
    }

    let file = fs::File::create(&archive_path).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to create snapshot archive {}",
                archive_path.display()
            ),
        )
    })?;

    let mut walker = SnapshotWalker::new(file, Path::new(&stored_volume.path), options)?;
    walker.walk()?;
    let mut file = walker.finish()?;
    file.flush()
        .map_err(|e| with_context(e, "Failed to flush snapshot archive"))?;
    drop(file);

    let size_bytes = fs::metadata(&archive_path)
        .map_err(|e| {
            with_context(
                e,
                format!("Failed to stat snapshot {}", archive_path.display()),
            )
        })?
        .len();

    let creation_time = now_rfc3339();
    let snapshot = Snapshot {
        snapshot_id: snapshot_id.to_string(),
        source_volume_id: stored_volume.volume.volume_id.clone(),
        size_bytes,
        ready_to_use: true,
        creation_time,
    };

    let stored_snapshot = StoredSnapshot {
        snapshot: snapshot.clone(),
        archive_path: archive_path.display().to_string(),
    };

    Ok((snapshot, stored_snapshot))
}

pub fn restore_from_snapshot(
    stored_snapshot: &StoredSnapshot,
    path: &Path,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if !path.exists() {
        fs::create_dir_all(path).map_err(|e| {
            with_context(
                e,
                format!("Failed to create restore directory {}", path.display()),
            )
        })?;
    }
    let file = fs::File::open(&stored_snapshot.archive_path).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to open snapshot archive {}",
                stored_snapshot.archive_path
            ),
        )
    })?;
    let mut archive = Archive::new(file);
    archive.set_preserve_permissions(true);
    archive.set_preserve_ownerships(true);
    archive.unpack(path).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to unpack snapshot '{}' into {}",
                stored_snapshot.snapshot.snapshot_id,
                path.display()
            ),
        )
    })?;
    Ok(())
}

struct SnapshotWalker<W: Write> {
    builder: Builder<W>,
    root: PathBuf,
    root_canonical: PathBuf,
    options: SnapshotOptions,
    total_bytes: u64,
    throttled_bytes: u64,
}

impl<W: Write> SnapshotWalker<W> {
    fn new(
        writer: W,
        root: &Path,
        options: &SnapshotOptions,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let root_canonical = root.canonicalize().map_err(|e| {
            with_context(
                e,
                format!("Failed to canonicalize snapshot root {}", root.display()),
            )
        })?;
        Ok(SnapshotWalker {
            builder: Builder::new(writer),
            root: root.to_path_buf(),
            root_canonical,
            options: options.clone(),
            total_bytes: 0,
            throttled_bytes: 0,
        })
    }

    fn walk(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let root = self.root.clone();
        self.append_entry(&root, 0)
    }

    fn finish(self) -> Result<W, Box<dyn Error + Send + Sync>> {
        self.builder
            .into_inner()
            .map_err(|e| with_context(e, "Failed to finalize snapshot archive builder"))
    }

    fn relative_path(&self, path: &Path) -> Result<Option<PathBuf>, Box<dyn Error + Send + Sync>> {
        let relative = path.strip_prefix(&self.root).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to compute relative snapshot path for {}",
                    path.display()
                ),
            )
        })?;
        if relative.as_os_str().is_empty() {
            Ok(None)
        } else {
            Ok(Some(relative.to_path_buf()))
        }
    }

    fn append_entry(
        &mut self,
        path: &Path,
        depth: usize,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if depth > self.options.max_depth {
            return Err(new_error(format!(
                "Snapshot recursion depth exceeded at {} (limit {})",
                path.display(),
                self.options.max_depth
            )));
        }

        let metadata = fs::symlink_metadata(path).map_err(|e| {
            with_context(
                e,
                format!("Failed to inspect snapshot source entry {}", path.display()),
            )
        })?;
        let relative = self.relative_path(path)?;

        if metadata.file_type().is_symlink() {
            if let Some(relative_path) = relative {
                self.append_symlink(path, &relative_path)?;
            }
            return Ok(());
        }

        if metadata.is_dir() {
            self.append_directory(path, relative.as_ref())?;
            for entry in fs::read_dir(path).map_err(|e| {
                with_context(
                    e,
                    format!(
                        "Failed to traverse snapshot source directory {}",
                        path.display()
                    ),
                )
            })? {
                let entry = entry.map_err(|e| {
                    with_context(
                        e,
                        format!(
                            "Failed to iterate snapshot source directory {}",
                            path.display()
                        ),
                    )
                })?;
                self.append_entry(&entry.path(), depth + 1)?;
            }
            return Ok(());
        }

        if metadata.is_file() {
            if let Some(relative_path) = relative {
                self.append_file(path, &relative_path, &metadata)?;
            }
            return Ok(());
        }

        Err(new_error(format!(
            "Unsupported snapshot entry type at {}",
            path.display()
        )))
    }

    fn append_directory(
        &mut self,
        path: &Path,
        relative: Option<&PathBuf>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let canonical = path.canonicalize().map_err(|e| {
            with_context(
                e,
                format!("Failed to canonicalize directory {}", path.display()),
            )
        })?;
        if !canonical.starts_with(&self.root_canonical) {
            return Err(new_error(format!(
                "Directory {} escapes storage root {}",
                path.display(),
                storage_root().display()
            )));
        }
        if let Some(relative_path) = relative {
            self.builder.append_dir(relative_path, path).map_err(|e| {
                with_context(
                    e,
                    format!(
                        "Failed to append snapshot directory '{}'",
                        relative_path.display()
                    ),
                )
            })?;
        }
        Ok(())
    }

    fn append_symlink(
        &mut self,
        path: &Path,
        relative: &Path,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let target = fs::read_link(path)
            .map_err(|e| with_context(e, format!("Failed to read symlink {}", path.display())))?;
        let resolved_target = if target.is_absolute() {
            target.clone()
        } else {
            path.parent().unwrap_or(&self.root).join(&target)
        };

        if let Ok(canonical_target) = resolved_target.canonicalize() {
            if !canonical_target.starts_with(&self.root_canonical) {
                match self.options.symlink_policy {
                    SnapshotSymlinkPolicy::Error => {
                        return Err(new_error(format!(
                            "Symlink '{}' points outside storage root: {}",
                            relative.display(),
                            target.display()
                        )))
                    }
                    SnapshotSymlinkPolicy::Skip => return Ok(()),
                    SnapshotSymlinkPolicy::Archive => {
                        return Err(new_error(format!(
                            "Symlink '{}' resolves outside storage root: {}",
                            relative.display(),
                            canonical_target.display()
                        )))
                    }
                }
            }
        } else if matches!(self.options.symlink_policy, SnapshotSymlinkPolicy::Error) {
            return Err(new_error(format!(
                "Symlink '{}' target '{}' could not be resolved",
                relative.display(),
                target.display()
            )));
        }

        match self.options.symlink_policy {
            SnapshotSymlinkPolicy::Skip => Ok(()),
            SnapshotSymlinkPolicy::Error => Err(new_error(format!(
                "Symlink '{}' not permitted by policy",
                relative.display()
            ))),
            SnapshotSymlinkPolicy::Archive => {
                let mut header = Header::new_gnu();
                header.set_entry_type(EntryType::Symlink);
                header.set_size(0);
                header.set_mode(0o777);
                self.builder
                    .append_link(&mut header, relative, target)
                    .map_err(|e| {
                        with_context(
                            e,
                            format!("Failed to append snapshot symlink '{}'", relative.display()),
                        )
                    })
            }
        }
    }

    fn append_file(
        &mut self,
        path: &Path,
        relative: &Path,
        metadata: &fs::Metadata,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let file_size = metadata.len();
        if let Some(limit) = self.options.max_total_bytes {
            if self.total_bytes + file_size > limit {
                return Err(new_error(format!(
                    "Snapshot size limit exceeded ({} > {} bytes)",
                    self.total_bytes + file_size,
                    limit
                )));
            }
        }
        self.total_bytes += file_size;
        self.throttled_bytes += file_size;

        self.builder
            .append_path_with_name(path, relative)
            .map_err(|e| {
                with_context(
                    e,
                    format!("Failed to append snapshot entry '{}'", relative.display()),
                )
            })?;

        if let Some(chunk) = self.options.throttle_bytes_per_chunk {
            if self.throttled_bytes >= chunk {
                thread::sleep(self.options.throttle_sleep);
                self.throttled_bytes = 0;
            }
        }
        Ok(())
    }
}
