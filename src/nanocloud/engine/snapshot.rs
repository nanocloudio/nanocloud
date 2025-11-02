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

use crate::nanocloud::api::types::BundleSpec;
use crate::nanocloud::csi::{csi_plugin, CreateSnapshotRequest, DeleteSnapshotRequest};
use crate::nanocloud::k8s::bundle_manager::BundleRegistry;
use crate::nanocloud::logger::{log_info, log_warn};
use crate::nanocloud::util::error::{new_error, with_context};

use bytes::Bytes;
use chrono::{SecondsFormat, Utc};
use futures_core::stream::Stream;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Keys;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::pin::Pin;
use std::task::{Context, Poll};
use tar::{Archive, Builder, Header};

/// A `RangeReader` that restricts reading to a specific range of bytes.
pub struct RangeReader<R: Read + Seek> {
    reader: BufReader<R>,
    max_bytes: usize,
    bytes_read: usize,
}

impl<R: Read + Seek> RangeReader<R> {
    /// Create a new `RangeReader` starting at `start_index` and limited to `max_bytes`.
    pub fn new(mut reader: R, start_index: u64, max_bytes: usize) -> io::Result<Self> {
        reader.seek(SeekFrom::Start(start_index))?;
        Ok(RangeReader {
            reader: BufReader::new(reader),
            max_bytes,
            bytes_read: 0,
        })
    }

    /// Reads all the remaining bytes within the range into the buffer.
    pub fn read_to_end(
        &mut self,
        buf: &mut Vec<u8>,
    ) -> Result<usize, Box<dyn Error + Send + Sync>> {
        let mut total_read = 0;

        while self.bytes_read < self.max_bytes {
            let remaining = self.max_bytes - self.bytes_read;

            // Temporary buffer for reading
            let mut temp_buf = vec![0u8; std::cmp::min(remaining, 8192)];
            let bytes_read = self
                .reader
                .read(&mut temp_buf)
                .map_err(|e| with_context(e, "Failed to read snapshot range"))?;

            if bytes_read == 0 {
                break; // End of file or range
            }

            temp_buf.truncate(bytes_read); // Adjust temp buffer size to actual bytes read
            buf.extend_from_slice(&temp_buf); // Append to the output buffer

            self.bytes_read += bytes_read;
            total_read += bytes_read;
        }

        Ok(total_read)
    }
}

impl<R: Read + Seek + Unpin> Stream for RangeReader<R> {
    type Item = Bytes;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let remaining = self.max_bytes.saturating_sub(self.bytes_read);

        if remaining == 0 {
            return Poll::Ready(None); // No more bytes to read
        }

        let chunk_size = std::cmp::min(remaining, 8192); // Set chunk size (8192 bytes max)
        let mut buf = vec![0u8; chunk_size];

        match self.reader.read(&mut buf) {
            Ok(0) => Poll::Ready(None), // End of stream
            Ok(bytes_read) => {
                self.bytes_read += bytes_read;
                buf.truncate(bytes_read);
                Poll::Ready(Some(Bytes::from(buf))) // Convert Vec<u8> to Bytes
            }
            Err(_) => Poll::Ready(None), // Handle errors by stopping the stream
        }
    }
}

impl<R: Read + Seek> Read for RangeReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = self.max_bytes.saturating_sub(self.bytes_read);
        if remaining == 0 {
            return Ok(0);
        }

        let to_read = remaining.min(buf.len());
        let n = self.reader.read(&mut buf[..to_read])?;
        self.bytes_read += n;
        Ok(n)
    }
}

#[derive(Clone)]
pub struct Snapshot {
    file_path: String,
    index: HashMap<String, (u64, u64)>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct VolumeSnapshotEntry {
    claim: String,
    volume_id: String,
    snapshot_id: String,
    #[serde(rename = "archivePath")]
    archive_path: String,
    #[serde(rename = "sizeBytes")]
    size_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ServiceSnapshotManifest {
    namespace: String,
    service: String,
    #[serde(rename = "createdAt")]
    created_at: String,
    snapshots: Vec<VolumeSnapshotEntry>,
}

const SNAPSHOT_MANIFEST_PATH: &str = "manifest.json";

#[derive(Debug, Clone)]
pub struct SnapshotVolumeEntry {
    pub claim: String,
    pub volume_id: String,
    pub snapshot_id: String,
    pub archive_path: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct SnapshotSummary {
    pub namespace: String,
    pub service: String,
    pub created_at: String,
    pub entries: Vec<SnapshotVolumeEntry>,
}

impl From<ServiceSnapshotManifest> for SnapshotSummary {
    fn from(manifest: ServiceSnapshotManifest) -> Self {
        let entries = manifest
            .snapshots
            .into_iter()
            .map(|entry| SnapshotVolumeEntry {
                claim: entry.claim,
                volume_id: entry.volume_id,
                snapshot_id: entry.snapshot_id,
                archive_path: entry.archive_path,
                size_bytes: entry.size_bytes,
            })
            .collect();
        SnapshotSummary {
            namespace: manifest.namespace,
            service: manifest.service,
            created_at: manifest.created_at,
            entries,
        }
    }
}

impl Snapshot {
    pub fn new(file_path: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let index = index_snapshot(file_path).map_err(|e| {
            with_context(e, format!("Failed to index snapshot archive {}", file_path))
        })?;
        Ok(Snapshot {
            file_path: file_path.to_string(),
            index,
        })
    }

    pub fn read_snapshot_entry(
        &self,
        entry_path: &str,
    ) -> Result<RangeReader<File>, Box<dyn Error + Send + Sync>> {
        let file = File::open(&self.file_path).map_err(|e| {
            with_context(
                e,
                format!("Failed to open snapshot archive {}", self.file_path),
            )
        })?;
        let (position, size) = self
            .index
            .get(entry_path)
            .ok_or_else(|| new_error(format!("Entry '{}' not found in index", entry_path)))?;
        RangeReader::new(file, *position, *size as usize).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to create snapshot reader for entry '{}' in {}",
                    entry_path, self.file_path
                ),
            )
        })
    }

    fn read_manifest(
        &self,
    ) -> Result<Option<ServiceSnapshotManifest>, Box<dyn Error + Send + Sync>> {
        if !self.index.contains_key(SNAPSHOT_MANIFEST_PATH) {
            return Ok(None);
        }
        let mut stream = self
            .read_snapshot_entry(SNAPSHOT_MANIFEST_PATH)
            .map_err(|e| with_context(e, "Failed to read snapshot manifest"))?;
        let mut buffer = Vec::new();
        stream
            .read_to_end(&mut buffer)
            .map_err(|e| with_context(e, "Failed to read snapshot manifest content"))?;
        let manifest = serde_json::from_slice::<ServiceSnapshotManifest>(&buffer)
            .map_err(|e| with_context(e, "Failed to parse snapshot manifest"))?;
        Ok(Some(manifest))
    }

    pub async fn save(
        namespace: Option<&str>,
        app: &str,
        target_claim: Option<&str>,
        file_path: &str,
    ) -> Result<SnapshotSummary, Box<dyn std::error::Error + Send + Sync>> {
        let namespace_value = namespace.unwrap_or("default");
        let save_fields = [
            ("namespace", namespace_value),
            ("app", app),
            ("file_path", file_path),
        ];
        log_info("snapshot", "Saving service snapshot", &save_fields);

        let claim_filter = target_claim
            .map(|value| value.trim())
            .filter(|value| !value.is_empty());

        if let Some(parent) = std::path::Path::new(file_path).parent() {
            fs::create_dir_all(parent).map_err(|e| {
                with_context(
                    e,
                    format!("Failed to create snapshot directory {}", parent.display()),
                )
            })?;
        }

        let csi = csi_plugin();

        let volumes = csi
            .list_service_volumes(namespace_value, app)
            .map_err(|e| with_context(e, format!("Failed to enumerate volumes for '{}'", app)))?;

        let file = File::create(file_path).map_err(|e| {
            with_context(e, format!("Failed to create snapshot file {}", file_path))
        })?;
        let mut writer = BufWriter::new(file);
        let mut captured_snapshot_ids = Vec::new();
        let mut manifest = ServiceSnapshotManifest {
            namespace: namespace_value.to_string(),
            service: app.to_string(),
            created_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            snapshots: Vec::new(),
        };

        {
            let mut tar_builder = Builder::new(&mut writer);

            let mut processed_volumes = 0usize;

            for volume in volumes.iter() {
                let claim = volume
                    .volume
                    .volume_context
                    .get("claim")
                    .cloned()
                    .unwrap_or_else(|| volume.volume.volume_id.clone());

                if let Some(filter) = claim_filter {
                    if filter != claim {
                        continue;
                    }
                }

                let snapshot_name = format!(
                    "{}-{}",
                    volume.volume.volume_id,
                    Utc::now().format("%Y%m%d%H%M%S")
                );

                let response = csi
                    .create_snapshot(CreateSnapshotRequest {
                        name: snapshot_name,
                        source_volume_id: volume.volume.volume_id.clone(),
                    })
                    .await
                    .map_err(|e| {
                        with_context(
                            e,
                            format!("Failed to snapshot volume '{}' for '{}'", claim, app),
                        )
                    })?;

                let archive_name = format!("snapshots/{}.tar", response.snapshot.snapshot_id);
                let mut archive_file = File::open(&response.archive_path).map_err(|e| {
                    with_context(
                        e,
                        format!("Failed to open snapshot archive {}", response.archive_path),
                    )
                })?;
                let metadata = archive_file.metadata().map_err(|e| {
                    with_context(
                        e,
                        format!("Failed to read metadata for {}", response.archive_path),
                    )
                })?;
                let mut header = Header::new_gnu();
                header.set_size(metadata.len());
                header.set_mode(0o644);
                header.set_cksum();
                tar_builder
                    .append_data(&mut header, archive_name.as_str(), &mut archive_file)
                    .map_err(|e| {
                        with_context(
                            e,
                            format!(
                                "Failed to append archive '{}' to snapshot stream",
                                archive_name
                            ),
                        )
                    })?;

                manifest.snapshots.push(VolumeSnapshotEntry {
                    claim,
                    volume_id: response.snapshot.source_volume_id.clone(),
                    snapshot_id: response.snapshot.snapshot_id.clone(),
                    archive_path: archive_name,
                    size_bytes: response.snapshot.size_bytes,
                });

                captured_snapshot_ids.push(response.snapshot.snapshot_id);
                processed_volumes += 1;
            }

            if let Some(target) = claim_filter {
                if processed_volumes == 0 {
                    return Err(new_error(format!(
                        "Volume claim '{}' not found for service '{}'",
                        target, app
                    )));
                }
            }

            let bundle_registry = BundleRegistry::shared();
            match bundle_registry.get(namespace_value, app).await {
                Some(bundle) => {
                    let spec_bytes = serde_json::to_vec_pretty(&bundle.spec).map_err(|e| {
                        with_context(e, "Failed to serialize bundle spec for snapshot")
                    })?;
                    add_to_tar(&mut tar_builder, "spec.json", &spec_bytes)?;
                }
                None => {
                    let warn_fields = [("namespace", namespace_value), ("app", app)];
                    log_warn(
                        "snapshot",
                        "Bundle spec not found while building snapshot",
                        &warn_fields,
                    );
                }
            }

            let manifest_bytes = serde_json::to_vec_pretty(&manifest)
                .map_err(|e| with_context(e, "Failed to serialize snapshot manifest"))?;
            add_to_tar(&mut tar_builder, SNAPSHOT_MANIFEST_PATH, &manifest_bytes)?;

            tar_builder
                .finish()
                .map_err(|e| with_context(e, "Failed to finalize snapshot archive"))?;
        }

        writer.flush().map_err(|e| {
            with_context(e, format!("Failed to flush snapshot writer {}", file_path))
        })?;

        for snapshot_id in captured_snapshot_ids {
            let _ = csi
                .delete_snapshot(DeleteSnapshotRequest { snapshot_id })
                .await;
        }

        Ok(SnapshotSummary::from(manifest))
    }

    // pub async fn save(docker: &Docker, namespace: Option<&str>, app: &str, container_id: &str, file_path: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    //     // Get mount points for volumes and create backups
    //     let volume_info = get_volume_info(&docker, &container_id).await?;

    //     // Prepare the snapshot file
    //     let file = File::create(file_path)?;
    //     let mut writer = BufWriter::new(file);
    //     let mut tar_builder = Builder::new(Vec::new());

    //     // Add each volume to the tar archive
    //     for (volume_name, path) in volume_info.clone() {
    //         let tar = download_volume(&docker, &container_id, &path).await?;
    //         add_to_tar(&mut tar_builder, &format!("volumes/{}.tar", volume_name), tar.as_slice())?;
    //     }

    //     // Fetch profile and add to the tar archive
    //     match Profile::load(namespace, app).await {
    //         Ok(profile) => {
    //             let buffer = profile.to_json()?;
    //             add_to_tar(&mut tar_builder, "profile.json", buffer.as_bytes())?;
    //         },
    //         Err(_) => {
    //             eprintln!("\x1b[31mWarning: No profile found for app '{}' in namespace '{}'\x1b[0m", app, namespace.unwrap_or("default"));
    //         },
    //     }

    //     // Finalize the tar file
    //     let data = tar_builder.into_inner()?;
    //     writer.write_all(&data)?;
    //     writer.flush()?;

    //     Ok(())
    // }

    pub async fn restore(
        &self,
        _app: &str,
        host_paths: &HashMap<String, String>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(manifest) = self.read_manifest()? {
            for entry in manifest.snapshots {
                if let Some(host_path) = host_paths.get(&entry.claim) {
                    let stream = self.read_snapshot_entry(&entry.archive_path).map_err(|e| {
                        with_context(
                            e,
                            format!(
                                "Failed to open snapshot '{}' for volume '{}'",
                                entry.snapshot_id, entry.claim
                            ),
                        )
                    })?;
                    let restore_fields = [
                        ("claim", entry.claim.as_str()),
                        ("host_path", host_path.as_str()),
                        ("snapshot_id", entry.snapshot_id.as_str()),
                        ("archive_path", entry.archive_path.as_str()),
                    ];
                    log_info(
                        "snapshot",
                        "Restoring volume from snapshot",
                        &restore_fields,
                    );
                    unpack_volume(host_path, stream).await?;
                } else {
                    let warn_fields = [
                        ("claim", entry.claim.as_str()),
                        ("snapshot_id", entry.snapshot_id.as_str()),
                    ];
                    log_warn(
                        "snapshot",
                        "Missing host path for captured volume claim",
                        &warn_fields,
                    );
                }
            }
            return Ok(());
        }

        // Legacy fallback: attempt to restore volumes/* entries using provided host paths
        for (volume_name, host_path) in host_paths {
            let entry_path = format!("volumes/{}.tar", volume_name);
            if !self.index.contains_key(&entry_path) {
                continue;
            }
            let stream = self.read_snapshot_entry(&entry_path).map_err(|e| {
                with_context(
                    e,
                    format!("Failed to open legacy snapshot '{}'", entry_path),
                )
            })?;
            let legacy_fields = [
                ("claim", volume_name.as_str()),
                ("host_path", host_path.as_str()),
                ("archive_path", entry_path.as_str()),
            ];
            log_info(
                "snapshot",
                "Restoring legacy snapshot volume",
                &legacy_fields,
            );
            unpack_volume(host_path, stream).await?;
        }

        Ok(())
    }

    pub fn read_spec(&self) -> Result<BundleSpec, Box<dyn Error + Send + Sync>> {
        let mut stream = self.read_snapshot_entry("spec.json")?;
        let mut buffer = Vec::new();
        stream
            .read_to_end(&mut buffer)
            .map_err(|e| with_context(e, "Failed to read bundle spec from snapshot"))?;
        serde_json::from_slice(&buffer)
            .map_err(|e| with_context(e, "Failed to parse bundle spec from snapshot"))
    }

    pub fn summary(&self) -> Result<Option<SnapshotSummary>, Box<dyn Error + Send + Sync>> {
        match self.read_manifest()? {
            Some(manifest) => Ok(Some(SnapshotSummary::from(manifest))),
            None => Ok(None),
        }
    }
}

// Implement IntoIterator for &Snapshot to return keys
impl<'a> IntoIterator for &'a Snapshot {
    type Item = &'a String;
    type IntoIter = Keys<'a, String, (u64, u64)>;

    fn into_iter(self) -> Self::IntoIter {
        self.index.keys()
    }
}

// Implement IntoIterator for Snapshot (consuming it)
impl IntoIterator for Snapshot {
    type Item = String; // Owned keys
    type IntoIter = std::collections::hash_map::IntoKeys<String, (u64, u64)>;

    fn into_iter(self) -> Self::IntoIter {
        self.index.into_keys()
    }
}

/// Function to index tar entries with their start positions
fn index_snapshot(
    file_path: &str,
) -> Result<HashMap<String, (u64, u64)>, Box<dyn Error + Send + Sync>> {
    let file = File::open(file_path)
        .map_err(|e| with_context(e, format!("Failed to open snapshot archive {}", file_path)))?;
    let mut archive = Archive::new(file);

    let mut entry_positions = HashMap::new();

    // Iterate through the entries in the archive
    let entries = archive.entries().map_err(|e| {
        with_context(
            e,
            format!("Failed to list entries in snapshot {}", file_path),
        )
    })?;
    for entry_result in entries {
        let entry = entry_result.map_err(|e| {
            with_context(
                e,
                format!("Failed to read entry from snapshot {}", file_path),
            )
        })?;

        // Record the current file position (the starting position of this entry)
        let entry_path = entry
            .path()
            .map_err(|e| with_context(e, "Failed to resolve snapshot entry path"))?
            .to_string_lossy()
            .to_string();
        entry_positions.insert(entry_path, (entry.raw_file_position(), entry.size()));
    }

    Ok(entry_positions)
}

fn add_to_tar<W: std::io::Write>(
    builder: &mut Builder<W>,
    path: &str,
    data: &[u8],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut header = tar::Header::new_gnu();
    header.set_size(data.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();

    builder
        .append_data(&mut header, path, data)
        .map_err(|e| with_context(e, format!("Failed to add '{}' to snapshot archive", path)))?;

    Ok(())
}

async fn unpack_volume(
    host_path: &str,
    stream: impl Stream<Item = Bytes> + Send + Read + 'static,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let target = std::path::Path::new(host_path);
    if !target.exists() {
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                with_context(
                    e,
                    format!("Failed to create parent directory {}", parent.display()),
                )
            })?;
        }
        fs::create_dir_all(target).map_err(|e| {
            with_context(
                e,
                format!("Failed to create volume directory {}", target.display()),
            )
        })?;
    }

    let mut archive = tar::Archive::new(stream);
    archive.set_preserve_ownerships(true);
    archive.set_preserve_permissions(true);
    archive.unpack(target).map_err(|e| {
        with_context(
            e,
            format!("Failed to unpack archive into {}", target.display()),
        )
    })?;

    Ok(())
}
