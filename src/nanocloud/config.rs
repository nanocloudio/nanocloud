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
use std::fmt;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, PathBuf};
use std::str::FromStr;

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

// =============================================================================
// Subsystem Backend Configuration
// =============================================================================

/// Identifies which subsystem a backend configuration applies to.
///
/// This enum provides a consistent pattern for configuring whether subsystems
/// use embedded (built-in) or external implementations. Each subsystem has
/// an associated environment variable following the convention:
/// `NANOCLOUD_<SUBSYSTEM>_BACKEND`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Subsystem {
    /// State storage (keyspace) - supports `embedded` or `etcd`
    Keyspace,
    // Future subsystems:
    // Cni,      // Container networking - `embedded` or `external`
    // Oci,      // Container runtime - `embedded` or `external`
    // Csi,      // Storage interface - `embedded` or `external`
}

impl Subsystem {
    /// Returns the environment variable name for this subsystem's backend.
    pub const fn env_var(&self) -> &'static str {
        match self {
            Subsystem::Keyspace => "NANOCLOUD_KEYSPACE_BACKEND",
        }
    }

    /// Returns the default backend for this subsystem.
    pub const fn default_backend(&self) -> &'static str {
        match self {
            Subsystem::Keyspace => "embedded",
        }
    }

    /// Returns the list of valid backend names for this subsystem.
    pub const fn valid_backends(&self) -> &'static [&'static str] {
        match self {
            Subsystem::Keyspace => &["embedded", "etcd"],
        }
    }

    /// Gets the configured backend from the environment or returns the default.
    pub fn get_backend(&self) -> String {
        env::var(self.env_var())
            .ok()
            .map(|v| v.trim().to_ascii_lowercase())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| self.default_backend().to_string())
    }

    /// Validates and returns the configured backend.
    ///
    /// Returns an error if the configured backend is not in the valid list.
    pub fn get_validated_backend(&self) -> Result<String, Box<dyn Error + Send + Sync>> {
        let backend = self.get_backend();
        if self.valid_backends().contains(&backend.as_str()) {
            Ok(backend)
        } else {
            Err(format!(
                "Invalid backend '{}' for {}. Valid options: {}",
                backend,
                self.env_var(),
                self.valid_backends().join(", ")
            )
            .into())
        }
    }
}

/// Backend type for the keyspace subsystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeyspaceBackend {
    /// Filesystem-backed storage (default)
    #[default]
    Embedded,
    /// External etcd cluster
    Etcd,
}

impl KeyspaceBackend {
    /// Returns the backend type from the environment configuration.
    pub fn from_env() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let backend = Subsystem::Keyspace.get_validated_backend()?;
        backend.parse()
    }
}

impl FromStr for KeyspaceBackend {
    type Err = Box<dyn Error + Send + Sync>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "embedded" | "filesystem" | "fs" => Ok(KeyspaceBackend::Embedded),
            "etcd" => Ok(KeyspaceBackend::Etcd),
            other => Err(format!(
                "Unknown keyspace backend '{}'. Valid options: embedded, etcd",
                other
            )
            .into()),
        }
    }
}

impl fmt::Display for KeyspaceBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyspaceBackend::Embedded => write!(f, "embedded"),
            KeyspaceBackend::Etcd => write!(f, "etcd"),
        }
    }
}

// =============================================================================
// Etcd Configuration
// =============================================================================

/// Environment variable for etcd endpoints (comma-separated URLs).
pub const ETCD_ENDPOINTS_ENV: &str = "NANOCLOUD_ETCD_ENDPOINTS";

/// Environment variable for etcd key prefix.
pub const ETCD_PREFIX_ENV: &str = "NANOCLOUD_ETCD_PREFIX";

/// Environment variable for etcd username (optional authentication).
pub const ETCD_USERNAME_ENV: &str = "NANOCLOUD_ETCD_USERNAME";

/// Environment variable for etcd password (optional authentication).
pub const ETCD_PASSWORD_ENV: &str = "NANOCLOUD_ETCD_PASSWORD";

/// Default etcd endpoint when not configured.
pub const ETCD_DEFAULT_ENDPOINT: &str = "http://127.0.0.1:2379";

/// Default etcd key prefix.
pub const ETCD_DEFAULT_PREFIX: &str = "/nanocloud";

/// Configuration for connecting to an etcd cluster.
#[derive(Debug, Clone)]
pub struct EtcdConfig {
    /// Etcd server endpoints (comma-separated in env var)
    pub endpoints: Vec<String>,
    /// Key prefix for all keyspace operations
    pub prefix: String,
    /// Optional username for authentication
    pub username: Option<String>,
    /// Optional password for authentication
    pub password: Option<String>,
}

impl EtcdConfig {
    /// Loads etcd configuration from environment variables.
    pub fn from_env() -> Self {
        let endpoints = env::var(ETCD_ENDPOINTS_ENV)
            .map(|v| {
                v.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|_| vec![ETCD_DEFAULT_ENDPOINT.to_string()]);

        let prefix = env::var(ETCD_PREFIX_ENV)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| ETCD_DEFAULT_PREFIX.to_string());

        let username = env::var(ETCD_USERNAME_ENV)
            .ok()
            .filter(|v| !v.trim().is_empty());

        let password = env::var(ETCD_PASSWORD_ENV)
            .ok()
            .filter(|v| !v.trim().is_empty());

        Self {
            endpoints,
            prefix,
            username,
            password,
        }
    }

    /// Returns true if authentication credentials are configured.
    pub fn has_auth(&self) -> bool {
        self.username.is_some() && self.password.is_some()
    }

    /// Returns the full key path with prefix for a given partition and key.
    pub fn full_key(&self, partition: &str, key: &str) -> String {
        format!("{}/{}{}", self.prefix, partition, key)
    }
}
