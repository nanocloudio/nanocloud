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

use std::env;
use std::error::Error;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, PathBuf};

/// Enum for supported configuration parameters
#[derive(Debug)]
pub enum Config {
    Backup,
    LockFile,
    Keyspace,
    SecureAssets,
    EncryptedVolumes,
}

impl Config {
    /// Returns the associated environment variable for the config parameter.
    pub fn env_var(&self) -> &'static str {
        match self {
            Config::Backup => "NANOCLOUD_BACKUP",
            Config::LockFile => "NANOCLOUD_LOCK_FILE",
            Config::Keyspace => "NANOCLOUD_KEYSPACE",
            Config::SecureAssets => "NANOCLOUD_SECURE_ASSETS",
            Config::EncryptedVolumes => "NANOCLOUD_ENCRYPTED_VOLUMES",
        }
    }

    /// Returns the associated environment variable and default value for the config parameter.
    pub fn default_path(&self) -> &'static str {
        match self {
            Config::Backup => {
                #[cfg(test)]
                {
                    "/tmp/nanocloud-test/backups"
                }
                #[cfg(not(test))]
                {
                    "/var/lib/nanocloud.io/backups"
                }
            }
            Config::LockFile => {
                #[cfg(test)]
                {
                    "/tmp/nanocloud-test/keyspace/.lock"
                }
                #[cfg(not(test))]
                {
                    "/var/lib/nanocloud.io/keyspace/.lock"
                }
            }
            Config::Keyspace => {
                #[cfg(test)]
                {
                    "/tmp/nanocloud-test/keyspace"
                }
                #[cfg(not(test))]
                {
                    "/var/lib/nanocloud.io/keyspace"
                }
            }
            Config::SecureAssets => {
                #[cfg(test)]
                {
                    "/tmp/nanocloud-test/secure_assets"
                }
                #[cfg(not(test))]
                {
                    "/var/lib/nanocloud.io/secure_assets"
                }
            }
            Config::EncryptedVolumes => {
                #[cfg(test)]
                {
                    "/tmp/nanocloud-test/runtime/encrypted"
                }
                #[cfg(not(test))]
                {
                    "/var/lib/nanocloud.io/runtime/encrypted"
                }
            }
        }
    }

    /// Returns the effective value, either from environment or default.
    pub fn get_path(&self) -> PathBuf {
        // Use environment variable if set, otherwise fall back to default
        env::var(self.env_var()).map_or_else(
            |_| Self::normalize_path(self.default_path()),
            |value| Self::normalize_path(&value),
        )
    }

    /// Create or verify that a directory is empty.
    pub fn verify(
        &self,
        subpath: Option<&str>,
        require_empty: bool,
    ) -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
        // Append subpath if provided.
        let mut path = self.get_path();
        if let Some(subpath) = subpath {
            path = path.join(subpath);
        }

        // Create the directory if it is absent else reject paths that already exist but are not directories.
        if !&path.exists() {
            fs::create_dir_all(&path)
                .map_err(|e| format!("Failed to create directory '{}': {}", path.display(), e))?;
        } else if !path.is_dir() {
            return Err(format!("Path '{}' exists but is not a directory", path.display()).into());
        }

        // Optionally ensure the directory is empty.
        if require_empty && path.read_dir()?.next().is_some() {
            return Err(format!("Directory '{}' must be empty", path.display()).into());
        }

        if subpath.is_none() {
            if let Some(mode) = self.desired_mode() {
                let permissions = fs::Permissions::from_mode(mode);
                if let Err(error) = fs::set_permissions(&path, permissions) {
                    return Err(std::io::Error::other(format!(
                        "Failed to set permissions on '{}': {}",
                        path.display(),
                        error
                    ))
                    .into());
                }
            }
        }

        Ok(path)
    }

    /// Normalize a directory path by expanding ~, resolving ., .., and returning an absolute, cleaned path.
    fn normalize_path(input: &str) -> PathBuf {
        // Expand leading ~ and make path absolute.
        let path: PathBuf = match input {
            _ if input.starts_with("~/") => env::var("HOME")
                .ok()
                .map(|home| PathBuf::from(home).join(&input[2..])),
            _ if !input.starts_with("/") => env::current_dir().ok().map(|cwd| cwd.join(input)),
            _ => None,
        }
        .unwrap_or_else(|| PathBuf::from(input));

        // Collapse `.` and `..` components.
        path.components()
            .fold(PathBuf::new(), |mut normalized, component| {
                match component {
                    Component::CurDir => {}
                    Component::ParentDir => {
                        normalized.pop();
                    }
                    _ => normalized.push(component),
                }
                normalized
            })
    }

    fn desired_mode(&self) -> Option<u32> {
        match self {
            Config::Backup => Some(0o750),
            Config::LockFile => None,
            Config::Keyspace => Some(0o750),
            Config::SecureAssets => Some(0o700),
            Config::EncryptedVolumes => Some(0o700),
        }
    }
}
