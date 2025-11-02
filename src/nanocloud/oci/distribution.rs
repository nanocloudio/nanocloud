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

use crate::nanocloud::logger::log_info;
use crate::nanocloud::oci::{fake_registry_root, image_store_root};
use crate::nanocloud::util::error::{new_error, with_context};
use flate2::read::GzDecoder;
use futures_util::stream::StreamExt;
use openssl::hash::{Hasher, MessageDigest};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs::{create_dir_all, File};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use tar::Archive;

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum ImageDescriptor {
    Index(ImageIndex),
    Manifest(OciManifest),
}

#[derive(Debug, Serialize, Deserialize)]
struct ImageIndex {
    #[serde(rename = "schemaVersion")]
    schema_version: u32,
    #[serde(rename = "mediaType")]
    media_type: String,
    manifests: Vec<ManifestDescriptor>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestDescriptor {
    #[serde(rename = "mediaType")]
    media_type: String,
    digest: String,
    size: u64,
    platform: Option<Platform>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Platform {
    architecture: String,
    os: String,
    variant: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OciManifest {
    #[serde(rename = "schemaVersion")]
    pub schema_version: u32,
    #[serde(rename = "mediaType")]
    pub media_type: String,
    pub config: ConfigDescriptor,
    pub layers: Vec<LayerDescriptor>,
    pub annotations: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConfigDescriptor {
    #[serde(rename = "mediaType")]
    pub media_type: String,
    pub digest: String,
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LayerDescriptor {
    #[serde(rename = "mediaType")]
    pub media_type: String,
    pub digest: String,
    pub size: u64,
}

trait Descriptor {
    fn digest(&self) -> &str;
    fn media_type(&self) -> &str;
    fn size(&self) -> u64;
}
impl Descriptor for ConfigDescriptor {
    fn digest(&self) -> &str {
        &self.digest
    }

    fn media_type(&self) -> &str {
        &self.media_type
    }

    fn size(&self) -> u64 {
        self.size
    }
}

impl Descriptor for LayerDescriptor {
    fn digest(&self) -> &str {
        &self.digest
    }

    fn media_type(&self) -> &str {
        &self.media_type
    }

    fn size(&self) -> u64 {
        self.size
    }
}

#[derive(Debug, Clone)]
pub struct ImageReference {
    pub registry: String,
    pub repository: String,
    pub tag: Option<String>,
    pub digest: Option<String>,
}

impl ImageReference {
    pub fn tag_or_default(&self) -> &str {
        self.tag.as_deref().unwrap_or("latest")
    }
}

pub fn parse_image_reference(image: &str) -> Result<ImageReference, Box<dyn Error + Send + Sync>> {
    if image.trim().is_empty() {
        return Err(new_error("Image reference is empty"));
    }

    let (without_digest, digest) = match image.split_once('@') {
        Some((reference, digest)) => {
            validate_digest(digest, image)?;
            (reference, Some(digest.to_string()))
        }
        None => (image, None),
    };

    let (reference_without_tag, tag) = match without_digest.rsplit_once(':') {
        Some((reference, tag_candidate)) if !tag_candidate.contains('/') => {
            validate_tag(tag_candidate)?;
            (reference, Some(tag_candidate.to_string()))
        }
        _ => (without_digest, None),
    };

    let (registry, repository) = match reference_without_tag.split_once('/') {
        Some((registry_candidate, remainder)) => {
            if remainder.is_empty() {
                return Err(new_error(format!(
                    "Image reference missing repository: {image}"
                )));
            }
            validate_registry(registry_candidate)?;
            validate_repository(remainder)?;
            (registry_candidate.to_string(), remainder.to_string())
        }
        None => {
            validate_repository(reference_without_tag)?;
            (
                "registry.nanocloud.io".to_string(),
                reference_without_tag.to_string(),
            )
        }
    };

    let tag = match (&tag, &digest) {
        (None, None) => Some("latest".to_string()),
        _ => tag,
    };

    Ok(ImageReference {
        registry,
        repository,
        tag,
        digest,
    })
}

fn validate_registry(registry: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    if registry.is_empty()
        || !registry
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':'))
    {
        return Err(new_error(format!(
            "Invalid registry component in image reference: {registry}"
        )));
    }
    Ok(())
}

fn validate_repository(repository: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    if repository.is_empty() {
        return Err(new_error("Image reference missing repository"));
    }

    for segment in repository.split('/') {
        if segment.is_empty()
            || !segment
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
        {
            return Err(new_error(format!(
                "Invalid repository component in image reference: {repository}"
            )));
        }
    }
    Ok(())
}

fn validate_tag(tag: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    if tag.is_empty()
        || !tag
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
    {
        return Err(new_error(format!("Invalid image tag: {tag}")));
    }
    Ok(())
}

fn validate_digest(digest: &str, original: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    const PREFIX: &str = "sha256:";
    if !digest.starts_with(PREFIX) {
        return Err(new_error(format!("Invalid image reference: {original}")));
    }
    let hex = &digest[PREFIX.len()..];
    if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(new_error(format!("Invalid image reference: {original}")));
    }
    Ok(())
}

pub fn load_manifest_from_store(
    reference: &ImageReference,
) -> Result<OciManifest, Box<dyn Error + Send + Sync>> {
    let image_root = image_store_root();
    let manifest_path = if let Some(digest) = &reference.digest {
        if !digest.starts_with("sha256:") || digest.len() != 71 {
            return Err(new_error(format!(
                "Unsupported manifest digest format: {digest}"
            )));
        }
        image_root.join("blobs").join("sha256").join(&digest[7..])
    } else {
        let tag = reference
            .tag
            .as_ref()
            .ok_or_else(|| new_error("Image reference missing tag and digest"))?;
        image_root
            .join("refs")
            .join(&reference.registry)
            .join(&reference.repository)
            .join(tag)
    };

    let file = File::open(&manifest_path).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to open cached manifest at {}",
                manifest_path.display()
            ),
        )
    })?;
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).map_err(|e| {
        with_context(
            e,
            format!("Failed to parse manifest at {}", manifest_path.display()),
        )
    })
}

pub struct Registry {}

impl Registry {
    pub async fn pull(
        image: &str,
        force_update: bool,
    ) -> Result<OciManifest, Box<dyn Error + Send + Sync>> {
        let reference = parse_image_reference(image)?;
        if let Some(fake_root) = fake_registry_root() {
            return pull_from_fake_registry(&reference, &fake_root, force_update);
        }
        let registry = reference.registry.clone();
        let repository = reference.repository.clone();
        let tag = reference.tag_or_default().to_string();
        let mut manifest_ref = reference.digest.clone().unwrap_or_else(|| tag.clone());

        let image_dir = image_store_root();
        let blobs_dir = image_dir.join("blobs/sha256");
        let refs_dir = image_dir.join("refs").join(&registry).join(&repository);
        let overlay_dir = image_dir.join("overlay");
        for dir in [&blobs_dir, &refs_dir, &overlay_dir] {
            create_dir_all(dir).map_err(|e| {
                with_context(e, format!("Failed to create directory {}", dir.display()))
            })?;
        }

        let client = Client::new();
        let manifest = match get_manifest(&client, &registry, &repository, &manifest_ref).await? {
            ImageDescriptor::Index(index) => {
                manifest_ref = get_digest(&index)?;
                match get_manifest(&client, &registry, &repository, &manifest_ref).await? {
                    ImageDescriptor::Index(_) => None,
                    ImageDescriptor::Manifest(manifest) => Some(manifest),
                }
            }
            ImageDescriptor::Manifest(manifest) => Some(manifest),
        }
        .ok_or_else(|| new_error("Unable to get manifest"))?;

        let layout_path = image_dir.join("oci-layout");
        std::fs::write(&layout_path, r#"{ "imageLayoutVersion": "1.0.0" }"#).map_err(|e| {
            with_context(
                e,
                format!("Failed to write OCI layout at {}", layout_path.display()),
            )
        })?;

        let manifest_json = serde_json::to_string_pretty(&manifest)
            .map_err(|e| with_context(e, format!("Failed to serialize manifest {manifest_ref}")))?;
        let manifest_path = blobs_dir.join(&manifest_ref[7..]);
        std::fs::write(&manifest_path, manifest_json).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to write manifest {manifest_ref} to {}",
                    manifest_path.display()
                ),
            )
        })?;

        validate_config_media_type(&manifest.config.media_type)?;
        let tag_symlink = refs_dir.join(&tag);
        if let Err(error) = std::fs::remove_file(&tag_symlink) {
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(with_context(
                    error,
                    format!("Failed to remove tag symlink {}", tag_symlink.display()),
                ));
            }
        }
        std::os::unix::fs::symlink(
            Path::new("../../../blobs/sha256").join(&manifest_ref[7..]),
            &tag_symlink,
        )
        .map_err(|e| {
            with_context(
                e,
                format!("Failed to create tag symlink {}", tag_symlink.display()),
            )
        })?;

        download_blob(
            &client,
            registry.as_str(),
            repository.as_str(),
            &manifest.config,
            &blobs_dir,
            force_update,
        )
        .await?;
        for layer in &manifest.layers {
            let layer_excerpt = &layer.digest[7..19];
            log_info(
                "oci",
                "Fetching OCI layer",
                &[
                    ("digest", layer.digest.as_str()),
                    ("excerpt", layer_excerpt),
                ],
            );
            download_blob(
                &client,
                registry.as_str(),
                repository.as_str(),
                layer,
                &blobs_dir,
                force_update,
            )
            .await?;
            unpack_layer(layer, &blobs_dir, &overlay_dir, force_update)?;
        }

        Ok(manifest)
    }
}

fn pull_from_fake_registry(
    reference: &ImageReference,
    fake_root: &Path,
    force_update: bool,
) -> Result<OciManifest, Box<dyn Error + Send + Sync>> {
    let manifest_path = fake_root
        .join("manifests")
        .join(&reference.registry)
        .join(&reference.repository)
        .join(format!("{}.json", reference.tag_or_default()));
    let manifest_bytes = std::fs::read(&manifest_path).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to read fake manifest at {}",
                manifest_path.display()
            ),
        )
    })?;
    let manifest: OciManifest = serde_json::from_slice(&manifest_bytes).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to parse fake manifest at {}",
                manifest_path.display()
            ),
        )
    })?;
    persist_fake_artifacts(
        reference,
        &manifest,
        &manifest_bytes,
        fake_root,
        force_update,
    )?;
    Ok(manifest)
}

fn persist_fake_artifacts(
    reference: &ImageReference,
    manifest: &OciManifest,
    manifest_bytes: &[u8],
    fake_root: &Path,
    force_update: bool,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let manifest_digest = compute_manifest_digest(manifest_bytes)?;
    let image_dir = image_store_root();
    let blobs_dir = image_dir.join("blobs/sha256");
    let refs_dir = image_dir
        .join("refs")
        .join(&reference.registry)
        .join(&reference.repository);
    let overlay_dir = image_dir.join("overlay");
    for dir in [&blobs_dir, &refs_dir, &overlay_dir] {
        create_dir_all(dir).map_err(|e| {
            with_context(e, format!("Failed to create directory {}", dir.display()))
        })?;
    }

    let layout_path = image_dir.join("oci-layout");
    std::fs::write(&layout_path, r#"{ "imageLayoutVersion": "1.0.0" }"#).map_err(|e| {
        with_context(
            e,
            format!("Failed to write OCI layout at {}", layout_path.display()),
        )
    })?;

    let manifest_hex = digest_hex(&manifest_digest)?;
    let manifest_path = blobs_dir.join(manifest_hex);
    std::fs::write(&manifest_path, manifest_bytes).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to persist fake manifest to {}",
                manifest_path.display()
            ),
        )
    })?;

    let tag_symlink = refs_dir.join(reference.tag_or_default());
    if let Err(error) = std::fs::remove_file(&tag_symlink) {
        if error.kind() != std::io::ErrorKind::NotFound {
            return Err(with_context(
                error,
                format!("Failed to remove tag symlink {}", tag_symlink.display()),
            ));
        }
    }
    std::os::unix::fs::symlink(
        Path::new("../../../blobs/sha256").join(manifest_hex),
        &tag_symlink,
    )
    .map_err(|e| {
        with_context(
            e,
            format!("Failed to create tag symlink {}", tag_symlink.display()),
        )
    })?;

    copy_fake_blob(fake_root, &manifest.config, &blobs_dir, force_update)?;
    for layer in &manifest.layers {
        copy_fake_blob(fake_root, layer, &blobs_dir, force_update)?;
        unpack_layer(layer, &blobs_dir, &overlay_dir, force_update)?;
    }

    Ok(())
}

fn compute_manifest_digest(bytes: &[u8]) -> Result<String, Box<dyn Error + Send + Sync>> {
    let mut hasher = Hasher::new(MessageDigest::sha256())
        .map_err(|e| with_context(e, "Failed to initialize manifest digest"))?;
    hasher
        .update(bytes)
        .map_err(|e| with_context(e, "Failed to hash manifest bytes"))?;
    let digest = hasher
        .finish()
        .map_err(|e| with_context(e, "Failed to finalize manifest digest"))?;
    Ok(format!("sha256:{}", bytes_to_hex(&digest)))
}

fn copy_fake_blob<D: Descriptor>(
    fake_root: &Path,
    descriptor: &D,
    blobs_dir: &Path,
    force_update: bool,
) -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    let digest_suffix = digest_hex(descriptor.digest())?.to_owned();
    let dest = blobs_dir.join(&digest_suffix);
    if dest.exists() && !force_update {
        verify_blob(&dest, descriptor)?;
        return Ok(dest);
    }

    if let Err(err) = create_dir_all(dest.parent().unwrap()) {
        return Err(with_context(
            err,
            format!("Failed to prepare blob destination {}", dest.display()),
        ));
    }

    let source = fake_root.join("blobs").join("sha256").join(&digest_suffix);
    let copied = std::fs::copy(&source, &dest).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to copy fake blob from {} to {}",
                source.display(),
                dest.display()
            ),
        )
    })?;
    if copied == 0 {
        return Err(new_error(format!(
            "Fake blob {} produced zero bytes",
            source.display()
        )));
    }
    verify_blob(&dest, descriptor)?;
    Ok(dest)
}

async fn get_manifest(
    client: &Client,
    registry: &str,
    repository: &str,
    reference: &str,
) -> Result<ImageDescriptor, Box<dyn Error + Send + Sync>> {
    let url = format!(
        "https://{}/v2/{}/manifests/{}",
        registry, repository, reference
    );
    let response = client
        .get(&url)
        .header(
            "Accept",
            "application/vnd.oci.image.manifest.v1+json, application/vnd.oci.image.index.v1+json",
        )
        .send()
        .await
        .map_err(|e| {
            with_context(
                e,
                format!("Failed to request manifest {reference} from {registry}"),
            )
        })?
        .error_for_status()
        .map_err(|e| {
            with_context(
                e,
                format!("Registry returned error status fetching {reference} from {registry}"),
            )
        })?
        .text()
        .await
        .map_err(|e| {
            with_context(
                e,
                format!("Failed to read manifest body for {reference} from {registry}"),
            )
        })?;
    serde_json::from_str(&response).map_err(|e| {
        with_context(
            e,
            format!("Failed to parse manifest for {reference} from {registry}"),
        )
    })
}

async fn download_blob<D: Descriptor>(
    client: &Client,
    registry: &str,
    repository: &str,
    descriptor: &D,
    blobs_dir: &Path,
    force_update: bool,
) -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    let digest = descriptor.digest();
    let digest_hex = digest_hex(digest)?;
    let file_path = blobs_dir.join(digest_hex);

    if file_path.exists() {
        if force_update {
            if let Err(err) = std::fs::remove_file(&file_path) {
                if err.kind() != std::io::ErrorKind::NotFound {
                    return Err(with_context(
                        err,
                        format!("Failed to remove cached blob {}", file_path.display()),
                    ));
                }
            }
        } else {
            match verify_blob(&file_path, descriptor) {
                Ok(()) => return Ok(file_path),
                Err(error) => {
                    log_info(
                        "oci",
                        "Existing blob failed verification; re-downloading",
                        &[
                            ("digest", descriptor.digest()),
                            ("error", error.to_string().as_str()),
                        ],
                    );
                    std::fs::remove_file(&file_path).map_err(|e| {
                        with_context(
                            e,
                            format!("Failed to remove invalid blob {}", file_path.display()),
                        )
                    })?;
                }
            }
        }
    }

    let tmp_path = blobs_dir.join(format!("{}.partial", digest_hex));
    let mut file = File::create(&tmp_path).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to create temporary blob {} for {}",
                tmp_path.display(),
                digest
            ),
        )
    })?;
    let url = format!("https://{}/v2/{}/blobs/{}", registry, repository, digest);
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| {
            with_context(
                e,
                format!("Failed to download blob {digest} from {registry}/{repository}"),
            )
        })?
        .error_for_status()
        .map_err(|e| {
            with_context(
                e,
                format!("Registry returned error status downloading blob {digest}"),
            )
        })?;

    let mut hasher = Hasher::new(MessageDigest::sha256())
        .map_err(|e| with_context(e, format!("Failed to create hasher for blob {digest}")))?;
    let mut written: u64 = 0;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let data = chunk
            .map_err(|e| with_context(e, format!("Failed to read blob chunk for {digest}")))?;
        hasher
            .update(&data)
            .map_err(|e| with_context(e, format!("Failed to hash blob {digest}")))?;
        file.write_all(&data).map_err(|e| {
            with_context(
                e,
                format!("Failed to write blob {digest} to {}", tmp_path.display()),
            )
        })?;
        written += data.len() as u64;
    }
    file.flush().map_err(|e| {
        with_context(
            e,
            format!("Failed to flush blob {digest} to {}", tmp_path.display()),
        )
    })?;
    drop(file);

    if descriptor.size() != 0 && written != descriptor.size() {
        std::fs::remove_file(&tmp_path).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to remove partial blob {} after size mismatch",
                    tmp_path.display()
                ),
            )
        })?;
        return Err(new_error(format!(
            "Size mismatch for blob {digest}: expected {}, got {written} bytes",
            descriptor.size()
        )));
    }

    let digest_bytes = hasher
        .finish()
        .map_err(|e| with_context(e, format!("Failed to finalize digest for blob {digest}")))?;
    let actual_hex = bytes_to_hex(&digest_bytes);
    let actual_digest = format!("sha256:{}", actual_hex);
    if actual_digest != digest {
        std::fs::remove_file(&tmp_path).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to remove partial blob {} after digest mismatch",
                    tmp_path.display()
                ),
            )
        })?;
        return Err(new_error(format!(
            "Digest mismatch for blob {digest} (media type {}): expected {digest}, got {actual_digest}",
            descriptor.media_type()
        )));
    }

    std::fs::rename(&tmp_path, &file_path).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to finalize blob {digest} at {}",
                file_path.display()
            ),
        )
    })?;
    Ok(file_path)
}

fn unpack_layer(
    layer: &LayerDescriptor,
    blobs_dir: &Path,
    overlay_dir: &Path,
    force_update: bool,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let digest_hex = digest_hex(&layer.digest)?;
    let file_path = blobs_dir.join(digest_hex);
    let out_path = overlay_dir.join(digest_hex);

    if out_path.exists() {
        if force_update {
            std::fs::remove_dir_all(&out_path).map_err(|e| {
                with_context(
                    e,
                    format!(
                        "Failed to remove cached layer directory {}",
                        out_path.display()
                    ),
                )
            })?;
        } else {
            return Ok(());
        }
    }

    create_dir_all(&out_path).map_err(|e| {
        with_context(
            e,
            format!("Failed to create layer directory {}", out_path.display()),
        )
    })?;

    match classify_layer_media_type(&layer.media_type)? {
        LayerMediaKind::TarGzip => {
            let file = File::open(&file_path).map_err(|e| {
                with_context(
                    e,
                    format!("Failed to open compressed layer {}", file_path.display()),
                )
            })?;
            let decoder = GzDecoder::new(file);
            let mut archive = Archive::new(decoder);
            archive.set_preserve_permissions(true);
            archive.set_preserve_ownerships(true);
            archive.unpack(&out_path).map_err(|e| {
                with_context(
                    e,
                    format!("Failed to unpack gzip layer to {}", out_path.display()),
                )
            })?;
        }
        LayerMediaKind::Tar => {
            let file = File::open(&file_path).map_err(|e| {
                with_context(
                    e,
                    format!("Failed to open layer tar {}", file_path.display()),
                )
            })?;
            let mut archive = Archive::new(file);
            archive.set_preserve_permissions(true);
            archive.set_preserve_ownerships(true);
            archive.unpack(&out_path).map_err(|e| {
                with_context(
                    e,
                    format!("Failed to unpack layer to {}", out_path.display()),
                )
            })?;
        }
    }

    Ok(())
}

fn verify_blob<D: Descriptor>(
    path: &Path,
    descriptor: &D,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (actual_digest, actual_size) = compute_file_digest(path).map_err(|e| {
        with_context(
            e,
            format!("Failed to compute digest for {}", path.display()),
        )
    })?;

    if descriptor.size() != 0 && actual_size != descriptor.size() {
        return Err(new_error(format!(
            "Existing blob {} has size {}, expected {}",
            descriptor.digest(),
            actual_size,
            descriptor.size()
        )));
    }

    if actual_digest != descriptor.digest() {
        return Err(new_error(format!(
            "Existing blob {} digest mismatch: expected {}, got {}",
            path.display(),
            descriptor.digest(),
            actual_digest
        )));
    }

    Ok(())
}

fn compute_file_digest(path: &Path) -> Result<(String, u64), Box<dyn Error + Send + Sync>> {
    let mut file = File::open(path)
        .map_err(|e| with_context(e, format!("Failed to open blob {}", path.display())))?;
    let mut hasher = Hasher::new(MessageDigest::sha256())
        .map_err(|e| with_context(e, "Failed to create SHA256 hasher"))?;
    let mut buffer = [0u8; 8192];
    let mut total = 0u64;

    loop {
        let read = file.read(&mut buffer).map_err(|e| {
            with_context(
                e,
                format!("Failed to read {} while hashing", path.display()),
            )
        })?;
        if read == 0 {
            break;
        }
        hasher
            .update(&buffer[..read])
            .map_err(|e| with_context(e, "Failed to update digest while hashing"))?;
        total += read as u64;
    }

    let digest_bytes = hasher
        .finish()
        .map_err(|e| with_context(e, "Failed to finalize blob digest"))?;
    let digest = bytes_to_hex(&digest_bytes);
    Ok((format!("sha256:{}", digest), total))
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut hex, "{:02x}", byte).expect("write! to String cannot fail");
    }
    hex
}

fn digest_hex(digest: &str) -> Result<&str, Box<dyn Error + Send + Sync>> {
    let (algorithm, value) = digest
        .split_once(':')
        .ok_or_else(|| new_error(format!("Invalid digest format: {digest}")))?;
    if algorithm != "sha256" {
        return Err(new_error(format!(
            "Unsupported digest algorithm: {algorithm}"
        )));
    }
    Ok(value)
}

enum LayerMediaKind {
    Tar,
    TarGzip,
}

fn classify_layer_media_type(
    media_type: &str,
) -> Result<LayerMediaKind, Box<dyn Error + Send + Sync>> {
    match media_type {
        "application/vnd.oci.image.layer.v1.tar"
        | "application/vnd.oci.image.layer.nondistributable.v1.tar"
        | "application/vnd.docker.image.rootfs.diff.tar"
        | "application/vnd.docker.image.rootfs.foreign.diff.tar" => Ok(LayerMediaKind::Tar),
        "application/vnd.oci.image.layer.v1.tar+gzip"
        | "application/vnd.oci.image.layer.nondistributable.v1.tar+gzip"
        | "application/vnd.docker.image.rootfs.diff.tar.gzip"
        | "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip" => {
            Ok(LayerMediaKind::TarGzip)
        }
        other if other.ends_with("+gzip") => Ok(LayerMediaKind::TarGzip),
        other if other.ends_with(".tar") => Ok(LayerMediaKind::Tar),
        other => Err(new_error(format!("Unsupported layer media type: {other}"))),
    }
}

fn validate_config_media_type(media_type: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    if media_type.ends_with("+json") {
        Ok(())
    } else {
        Err(new_error(format!(
            "Unsupported config media type: {media_type}"
        )))
    }
}

fn get_digest(index: &ImageIndex) -> Result<String, Box<dyn Error + Send + Sync>> {
    let os = std::env::consts::OS;
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "arm" => "arm",
        "mips" => "mips",
        other => other,
    };

    index
        .manifests
        .iter()
        .find(|manifest| {
            manifest
                .platform
                .as_ref()
                .is_some_and(|platform| platform.architecture == arch && platform.os == os)
        })
        .map(|manifest| manifest.digest.clone())
        .ok_or_else(|| new_error("No valid manifest found"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn expected_arch() -> &'static str {
        match std::env::consts::ARCH {
            "x86_64" => "amd64",
            "aarch64" => "arm64",
            "arm" => "arm",
            "mips" => "mips",
            other => other,
        }
    }

    #[test]
    fn parse_image_reference_defaults_registry_and_tag() {
        let reference = parse_image_reference("platform").expect("parse");
        assert_eq!(reference.registry, "registry.nanocloud.io");
        assert_eq!(reference.repository, "platform");
        assert_eq!(reference.tag.as_deref(), Some("latest"));
        assert!(reference.digest.is_none());
    }

    #[test]
    fn parse_image_reference_handles_explicit_registry_digest() {
        let digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let reference = parse_image_reference(&format!("registry.example.com/demo/app@{digest}"))
            .expect("parse");
        assert_eq!(reference.registry, "registry.example.com");
        assert_eq!(reference.repository, "demo/app");
        assert!(reference.tag.is_none());
        assert_eq!(reference.digest.as_deref(), Some(digest));
    }

    #[test]
    fn parse_image_reference_rejects_invalid_reference() {
        let error = parse_image_reference("demo@@bad").expect_err("should fail");
        assert!(error.to_string().contains("Invalid image reference"));
    }

    #[test]
    fn image_reference_tag_or_default_resolves_latest() {
        let reference = ImageReference {
            registry: "registry.nanocloud.io".to_string(),
            repository: "platform".to_string(),
            tag: None,
            digest: None,
        };
        assert_eq!(reference.tag_or_default(), "latest");
    }

    #[test]
    fn digest_hex_extracts_value() {
        let digest = "sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
        assert_eq!(digest_hex(digest).expect("digest"), &digest[7..]);
    }

    #[test]
    fn digest_hex_rejects_unsupported_algorithms() {
        let error = digest_hex("sha1:deadbeef").expect_err("should fail");
        assert!(error.to_string().contains("Unsupported digest algorithm"));
    }

    #[test]
    fn classify_layer_media_type_detects_tar_and_gzip() {
        assert!(matches!(
            classify_layer_media_type("application/vnd.oci.image.layer.v1.tar").expect("tar"),
            LayerMediaKind::Tar
        ));
        assert!(matches!(
            classify_layer_media_type("application/vnd.oci.image.layer.v1.tar+gzip").expect("gzip"),
            LayerMediaKind::TarGzip
        ));
        assert!(matches!(
            classify_layer_media_type("application/custom+gzip").expect("suffix gzip"),
            LayerMediaKind::TarGzip
        ));
    }

    #[test]
    fn classify_layer_media_type_rejects_unknown_media_type() {
        assert!(
            classify_layer_media_type("application/octet-stream").is_err(),
            "unknown types should be rejected"
        );
    }

    #[test]
    fn validate_config_media_type_requires_json_suffix() {
        validate_config_media_type("application/vnd.oci.image.config.v1+json")
            .expect("json config accepted");

        let error = validate_config_media_type("application/vnd.oci.image.config.v1")
            .expect_err("should fail");
        assert!(error.to_string().contains("Unsupported config media type"));
    }

    #[test]
    fn bytes_to_hex_formats_lowercase() {
        let bytes = [0xAB, 0xCD, 0x01];
        assert_eq!(bytes_to_hex(&bytes), "abcd01");
    }

    #[test]
    fn compute_file_digest_returns_expected_sha256() {
        let mut temp = NamedTempFile::new().expect("temp file");
        temp.write_all(b"hello").expect("write");
        temp.flush().expect("flush");

        let (digest, size) = compute_file_digest(temp.path()).expect("digest");
        assert_eq!(size, 5);
        assert_eq!(
            digest,
            "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn verify_blob_validates_existing_blob() {
        let mut temp = NamedTempFile::new().expect("temp file");
        temp.write_all(b"nanocloud").expect("write");
        temp.flush().expect("flush");

        let (digest, size) = compute_file_digest(temp.path()).expect("digest");
        let descriptor = LayerDescriptor {
            media_type: "application/vnd.oci.image.layer.v1.tar".to_string(),
            digest: digest.clone(),
            size,
        };

        verify_blob(temp.path(), &descriptor).expect("verification succeeds");
    }

    #[test]
    fn verify_blob_detects_size_mismatch() {
        let mut temp = NamedTempFile::new().expect("temp file");
        temp.write_all(b"nanocloud").expect("write");
        temp.flush().expect("flush");

        let (digest, size) = compute_file_digest(temp.path()).expect("digest");
        let descriptor = LayerDescriptor {
            media_type: "application/vnd.oci.image.layer.v1.tar".to_string(),
            digest,
            size: size + 1,
        };

        let error = verify_blob(temp.path(), &descriptor).expect_err("should fail");
        assert!(error.to_string().contains("Existing blob"));
    }

    #[test]
    fn get_digest_selects_matching_platform() {
        let digest = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
        let index = ImageIndex {
            schema_version: 2,
            media_type: "application/vnd.oci.image.index.v1+json".to_string(),
            manifests: vec![
                ManifestDescriptor {
                    media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
                    digest: digest.to_string(),
                    size: 1024,
                    platform: Some(Platform {
                        architecture: expected_arch().to_string(),
                        os: std::env::consts::OS.to_string(),
                        variant: None,
                    }),
                },
                ManifestDescriptor {
                    media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
                    digest:
                        "sha256:2222222222222222222222222222222222222222222222222222222222222222"
                            .to_string(),
                    size: 2048,
                    platform: Some(Platform {
                        architecture: "other".to_string(),
                        os: "other-os".to_string(),
                        variant: None,
                    }),
                },
            ],
        };

        assert_eq!(get_digest(&index).expect("digest"), digest);
    }

    #[test]
    fn get_digest_errors_when_no_matching_manifest() {
        let index = ImageIndex {
            schema_version: 2,
            media_type: "application/vnd.oci.image.index.v1+json".to_string(),
            manifests: vec![ManifestDescriptor {
                media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
                digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
                size: 512,
                platform: Some(Platform {
                    architecture: "unmatched".to_string(),
                    os: "other-os".to_string(),
                    variant: None,
                }),
            }],
        };

        let error = get_digest(&index).expect_err("should fail");
        assert_eq!(error.to_string(), "No valid manifest found");
    }

    #[test]
    fn load_manifest_from_store_rejects_invalid_digest_format() {
        let reference = ImageReference {
            registry: "registry.nanocloud.io".to_string(),
            repository: "app".to_string(),
            tag: None,
            digest: Some("md5:1234".to_string()),
        };

        let error = load_manifest_from_store(&reference).expect_err("should fail");
        assert!(error
            .to_string()
            .contains("Unsupported manifest digest format"));
    }
}
