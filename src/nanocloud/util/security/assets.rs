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

use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use openssl::pkey::{PKey, Private};
use openssl::rand::rand_bytes;
use openssl::sha::sha256;
use openssl::x509::X509;

use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::Config;

use super::cert::{gen_ecc_key, gen_rsa_key, gen_x509_cert};

#[derive(Clone, Debug)]
pub struct VolumeKeyMetadata {
    pub volume: String,
    pub path: PathBuf,
    pub fingerprint: String,
    pub created: bool,
}

pub struct SecureAssets;

struct CachedSecretKey {
    path: PathBuf,
    modified: SystemTime,
    key: PKey<Private>,
}

struct CachedCa {
    cert_path: PathBuf,
    cert_modified: SystemTime,
    key_path: PathBuf,
    key_modified: SystemTime,
    cert: X509,
    key: PKey<Private>,
}

static SECRET_KEY_CACHE: OnceLock<Mutex<Option<CachedSecretKey>>> = OnceLock::new();
static CA_CACHE: OnceLock<Mutex<Option<CachedCa>>> = OnceLock::new();

fn secret_key_cache() -> &'static Mutex<Option<CachedSecretKey>> {
    SECRET_KEY_CACHE.get_or_init(|| Mutex::new(None))
}

fn ca_cache() -> &'static Mutex<Option<CachedCa>> {
    CA_CACHE.get_or_init(|| Mutex::new(None))
}

fn modified_time(path: &Path) -> SystemTime {
    fs::metadata(path)
        .and_then(|meta| meta.modified())
        .unwrap_or(UNIX_EPOCH)
}

#[cfg(test)]
pub(crate) fn clear_asset_caches() {
    if let Some(cache) = SECRET_KEY_CACHE.get() {
        *cache.lock().unwrap() = None;
    }
    if let Some(cache) = CA_CACHE.get() {
        *cache.lock().unwrap() = None;
    }
}

impl SecureAssets {
    pub fn generate(dir: &Path, repair: bool) -> Result<(), Box<dyn Error + Send + Sync>> {
        let ca_key_path = dir.join("ca.key");
        let ca_cert_path = dir.join("ca.crt");
        let secret_key_path = dir.join("secret.key");

        if repair && ca_cert_path.exists() && !ca_key_path.exists() {
            return Err(new_error(
                "Cannot repair secure assets: ca.crt exists but ca.key is missing. Move or remove the certificate before repairing.",
            ));
        }

        let allow_overwrite = !repair;

        let ca_key = if ca_key_path.exists() {
            load_from_file(&ca_key_path, |buffer| {
                PKey::private_key_from_pem(buffer).map_err(|e| {
                    with_context(
                        e,
                        format!(
                            "Failed to parse CA private key PEM at '{}'",
                            ca_key_path.display()
                        ),
                    )
                })
            })
            .map_err(|e| with_context(e, "Failed to load CA private key"))?
        } else {
            let key =
                gen_ecc_key().map_err(|e| with_context(e, "Failed to generate CA private key"))?;
            let pem = key
                .private_key_to_pem_pkcs8()
                .map_err(|e| with_context(e, "Failed to encode CA private key"))?;
            save_to_file(dir, "ca.key", &pem, true, allow_overwrite)
                .map_err(|e| with_context(e, "Failed to persist CA private key"))?;
            key
        };

        if !ca_cert_path.exists() {
            let ca_cert = gen_x509_cert("nanocloud-ca", &ca_key, None, None)
                .map_err(|e| with_context(e, "Failed to generate CA certificate"))?;
            let pem = ca_cert
                .to_pem()
                .map_err(|e| with_context(e, "Failed to encode CA certificate"))?;
            save_to_file(dir, "ca.crt", &pem, false, allow_overwrite)
                .map_err(|e| with_context(e, "Failed to persist CA certificate"))?;
        }

        if !secret_key_path.exists() {
            let secret_key = gen_rsa_key()
                .map_err(|e| with_context(e, "Failed to generate service secret key"))?;
            let pem = secret_key
                .private_key_to_pem_pkcs8()
                .map_err(|e| with_context(e, "Failed to encode service secret key"))?;
            save_to_file(dir, "secret.key", &pem, true, allow_overwrite)
                .map_err(|e| with_context(e, "Failed to persist service secret key"))?;
        }

        Ok(())
    }

    pub fn ensure_volume_keys(
        dir: &Path,
        volumes: &[String],
        overwrite: bool,
    ) -> Result<Vec<VolumeKeyMetadata>, Box<dyn Error + Send + Sync>> {
        let mut dedup = std::collections::BTreeSet::new();
        let mut metadata = Vec::new();
        for volume in volumes {
            if !dedup.insert(volume.clone()) {
                continue;
            }
            metadata.push(Self::ensure_volume_key(dir, volume, overwrite)?);
        }
        Ok(metadata)
    }

    fn ensure_volume_key(
        dir: &Path,
        volume: &str,
        overwrite: bool,
    ) -> Result<VolumeKeyMetadata, Box<dyn Error + Send + Sync>> {
        validate_volume_name(volume)?;

        let volumes_dir = dir.join("volumes");
        fs::create_dir_all(&volumes_dir).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to prepare secure assets volume directory '{}'",
                    volumes_dir.display()
                ),
            )
        })?;

        let file_name = format!("{volume}.key");
        let key_path = volumes_dir.join(&file_name);

        if key_path.exists() && !overwrite {
            let encoded = fs::read_to_string(&key_path).map_err(|e| {
                with_context(
                    e,
                    format!(
                        "Failed to read existing volume key '{}'",
                        key_path.display()
                    ),
                )
            })?;
            let key_bytes = BASE64
                .decode(encoded.trim().as_bytes())
                .map_err(|e| with_context(e, "Failed to decode existing volume key"))?;
            let fingerprint = hex_sha256(&key_bytes);
            return Ok(VolumeKeyMetadata {
                volume: volume.to_string(),
                path: key_path,
                fingerprint,
                created: false,
            });
        }

        let mut key_bytes = [0u8; 32];
        rand_bytes(&mut key_bytes).map_err(|e| with_context(e, "Failed to generate volume key"))?;
        let encoded = BASE64.encode(key_bytes);

        save_to_file(
            &volumes_dir,
            &file_name,
            encoded.as_bytes(),
            true,
            overwrite,
        )
        .map_err(|e| {
            with_context(
                e,
                format!("Failed to persist volume key '{}'", key_path.display()),
            )
        })?;

        let fingerprint = hex_sha256(&key_bytes);
        Ok(VolumeKeyMetadata {
            volume: volume.to_string(),
            path: key_path,
            fingerprint,
            created: true,
        })
    }
}

pub(crate) fn load_ca() -> Result<(X509, PKey<Private>), Box<dyn Error + Send + Sync>> {
    let dir = Config::SecureAssets.get_path();
    let ca_cert_path = dir.join("ca.crt");
    let ca_key_path = dir.join("ca.key");
    let cert_modified = modified_time(&ca_cert_path);
    let key_modified = modified_time(&ca_key_path);

    {
        let cache = ca_cache().lock().unwrap();
        if let Some(cached) = cache.as_ref() {
            if cached.cert_path == ca_cert_path
                && cached.key_path == ca_key_path
                && cached.cert_modified == cert_modified
                && cached.key_modified == key_modified
            {
                return Ok((cached.cert.clone(), cached.key.clone()));
            }
        }
    }

    let ca_cert = load_from_file(&ca_cert_path, |buffer| {
        X509::from_pem(buffer).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to parse CA certificate PEM at '{}'",
                    ca_cert_path.display()
                ),
            )
        })
    })
    .map_err(|e| with_context(e, "Failed to load CA certificate"))?;
    let ca_key = load_from_file(&ca_key_path, |buffer| {
        PKey::private_key_from_pem(buffer).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to parse CA private key PEM at '{}'",
                    ca_key_path.display()
                ),
            )
        })
    })
    .map_err(|e| with_context(e, "Failed to load CA private key"))?;

    let mut cache = ca_cache().lock().unwrap();
    *cache = Some(CachedCa {
        cert_path: ca_cert_path.clone(),
        cert_modified,
        key_path: ca_key_path.clone(),
        key_modified,
        cert: ca_cert.clone(),
        key: ca_key.clone(),
    });

    Ok((ca_cert, ca_key))
}

pub(crate) fn load_secret_key() -> Result<PKey<Private>, Box<dyn Error + Send + Sync>> {
    let dir = Config::SecureAssets.get_path();
    let secret_key_path = dir.join("secret.key");
    let modified = modified_time(&secret_key_path);

    {
        let cache = secret_key_cache().lock().unwrap();
        if let Some(cached) = cache.as_ref() {
            if cached.path == secret_key_path && cached.modified == modified {
                return Ok(cached.key.clone());
            }
        }
    }

    let key = load_from_file(&secret_key_path, |buffer| {
        PKey::private_key_from_pem(buffer).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to parse service secret key PEM at '{}'",
                    secret_key_path.display()
                ),
            )
        })
    })
    .map_err(|e| with_context(e, "Failed to load service secret key"))?;

    let mut cache = secret_key_cache().lock().unwrap();
    *cache = Some(CachedSecretKey {
        path: secret_key_path.clone(),
        modified,
        key: key.clone(),
    });

    Ok(key)
}

fn load_from_file<F, T>(path: &Path, loader: F) -> Result<T, Box<dyn Error + Send + Sync>>
where
    F: Fn(&[u8]) -> Result<T, Box<dyn Error + Send + Sync>>,
{
    let mut file = File::open(path).map_err(|e| {
        with_context(
            e,
            format!("Failed to open secure asset '{}'", path.display()),
        )
    })?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).map_err(|e| {
        with_context(
            e,
            format!("Failed to read secure asset '{}'", path.display()),
        )
    })?;

    loader(&buffer).map_err(|e| {
        with_context(
            e,
            format!("Failed to parse secure asset '{}'", path.display()),
        )
    })
}

fn save_to_file(
    dir: &Path,
    file_name: &str,
    data: &[u8],
    is_private: bool,
    overwrite: bool,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let path = dir.join(file_name);

    if !overwrite && path.exists() {
        return Ok(());
    }

    let mut file = if overwrite {
        File::create(&path).map_err(|e| {
            with_context(
                e,
                format!("Failed to create secure asset '{}'", path.display()),
            )
        })?
    } else {
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| {
                with_context(
                    e,
                    format!("Failed to create secure asset '{}'", path.display()),
                )
            })?
    };
    file.write_all(data).map_err(|e| {
        with_context(
            e,
            format!("Failed to write secure asset '{}'", path.display()),
        )
    })?;
    file.set_permissions(fs::Permissions::from_mode(if is_private {
        0o400
    } else {
        0o444
    }))
    .map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to set permissions on secure asset '{}'",
                path.display()
            ),
        )
    })?;

    Ok(())
}

fn validate_volume_name(name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    if name.is_empty() {
        return Err(new_error("Volume name cannot be empty"));
    }
    if name.len() > 63 {
        return Err(new_error(format!(
            "Volume name '{}' exceeds 63 characters",
            name
        )));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err(new_error(
            "Volume name must use lowercase ASCII letters, digits, or '-'",
        ));
    }
    if name.starts_with('-') || name.ends_with('-') {
        return Err(new_error("Volume name cannot start or end with '-'"));
    }
    Ok(())
}

fn hex_sha256(data: &[u8]) -> String {
    let digest = sha256(data);
    digest.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use openssl::ec::{EcGroup, EcKey};
    use openssl::nid::Nid;
    use openssl::pkey::PKey;
    use openssl::rsa::Rsa;
    use serial_test::serial;
    use std::io::Write;
    use std::thread;
    use std::time::Duration;
    use tempfile::tempdir;

    struct EnvOverride {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvOverride {
        fn set(key: &'static str, value: impl AsRef<str>) -> Self {
            let previous = std::env::var(key).ok();
            std::env::set_var(key, value.as_ref());
            EnvOverride { key, previous }
        }
    }

    impl Drop for EnvOverride {
        fn drop(&mut self) {
            if let Some(prev) = &self.previous {
                std::env::set_var(self.key, prev);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    fn write_private_key(path: &Path, pem: &[u8]) {
        let mut file = File::create(path).expect("create private key");
        file.write_all(pem).expect("write private key");
        file.set_permissions(fs::Permissions::from_mode(0o400))
            .expect("set permissions");
    }

    #[test]
    #[serial]
    fn reloads_secret_key_after_change() {
        clear_asset_caches();
        let dir = tempdir().expect("tempdir");
        SecureAssets::generate(dir.path(), false).expect("generate assets");
        let _env = EnvOverride::set("NANOCLOUD_SECURE_ASSETS", dir.path().to_string_lossy());

        let first = load_secret_key().expect("first load");
        let first_der = first.private_key_to_der().expect("serialize first key");

        thread::sleep(Duration::from_millis(20));

        let new_key = Rsa::generate(2048).expect("generate rsa");
        let pkey = PKey::from_rsa(new_key).expect("pkey from rsa");
        let pem = pkey.private_key_to_pem_pkcs8().expect("encode private key");
        let secret_path = dir.path().join("secret.key");
        fs::remove_file(&secret_path).expect("remove existing secret key");
        write_private_key(&secret_path, &pem);

        let second = load_secret_key().expect("reload secret key");
        let second_der = second.private_key_to_der().expect("serialize second key");

        assert_ne!(first_der, second_der, "secret key cache did not refresh");
    }

    #[test]
    #[serial]
    fn reloads_ca_after_change() {
        clear_asset_caches();
        let dir = tempdir().expect("tempdir");
        SecureAssets::generate(dir.path(), false).expect("generate assets");
        let _env = EnvOverride::set("NANOCLOUD_SECURE_ASSETS", dir.path().to_string_lossy());

        let (_, first_key) = load_ca().expect("load ca");
        let first_der = first_key
            .private_key_to_der()
            .expect("serialize first ca key");

        thread::sleep(Duration::from_millis(20));

        let group = EcGroup::from_curve_name(Nid::SECP384R1).expect("ecc group");
        let ecc_key = EcKey::generate(&group).expect("generate ecc key");
        let pkey = PKey::from_ec_key(ecc_key).expect("pkey from ecc");
        let pem = pkey.private_key_to_pem_pkcs8().expect("encode ecc key");
        let ca_key_path = dir.path().join("ca.key");
        fs::remove_file(&ca_key_path).expect("remove existing ca key");
        write_private_key(&ca_key_path, &pem);

        let (_, second_key) = load_ca().expect("reload ca");
        let second_der = second_key
            .private_key_to_der()
            .expect("serialize second ca key");

        assert_ne!(
            first_der, second_der,
            "CA cache did not refresh after key update"
        );
    }
}
