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

//! Configuration validation for secret store key material.
//!
//! This module provides validation helpers to ensure the secret store
//! has proper key material configured before use. It checks:
//!
//! - Secure assets directory exists and has correct permissions
//! - Master encryption key (secret.key) exists and is readable
//! - Key material is in the correct format (RSA private key in PEM)
//!
//! # Usage
//!
//! Call [`validate_key_material`] during startup to verify the secret store
//! is properly configured. This provides clear error messages for common
//! configuration issues.
//!
//! ```ignore
//! use nanocloud::secrets::config::validate_key_material;
//!
//! if let Err(e) = validate_key_material() {
//!     eprintln!("Secret store configuration error: {}", e);
//!     std::process::exit(1);
//! }
//! ```

use std::error::Error;
use std::fmt;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use crate::nanocloud::Config;

/// Configuration validation error types.
#[derive(Debug)]
pub enum ConfigError {
    /// Secure assets directory does not exist.
    MissingDirectory {
        path: PathBuf,
        hint: String,
    },

    /// Secure assets directory has incorrect permissions.
    InsecurePermissions {
        path: PathBuf,
        actual: u32,
        expected: u32,
        hint: String,
    },

    /// Master key file does not exist.
    MissingKeyFile {
        path: PathBuf,
        hint: String,
    },

    /// Master key file is not readable.
    UnreadableKeyFile {
        path: PathBuf,
        error: String,
        hint: String,
    },

    /// Master key file has incorrect permissions.
    InsecureKeyPermissions {
        path: PathBuf,
        actual: u32,
        expected: u32,
        hint: String,
    },

    /// Master key file is empty.
    EmptyKeyFile {
        path: PathBuf,
        hint: String,
    },

    /// Master key file is not valid PEM format.
    InvalidKeyFormat {
        path: PathBuf,
        error: String,
        hint: String,
    },

    /// Master key is not an RSA private key.
    WrongKeyType {
        path: PathBuf,
        hint: String,
    },

    /// Master key has insufficient key size.
    InsufficientKeySize {
        path: PathBuf,
        actual_bits: u32,
        minimum_bits: u32,
        hint: String,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::MissingDirectory { path, hint } => {
                write!(
                    f,
                    "Secure assets directory does not exist: '{}'\n  Hint: {}",
                    path.display(),
                    hint
                )
            }
            ConfigError::InsecurePermissions { path, actual, expected, hint } => {
                write!(
                    f,
                    "Secure assets directory has insecure permissions: '{}' (mode {:04o}, expected {:04o})\n  Hint: {}",
                    path.display(),
                    actual,
                    expected,
                    hint
                )
            }
            ConfigError::MissingKeyFile { path, hint } => {
                write!(
                    f,
                    "Master encryption key file does not exist: '{}'\n  Hint: {}",
                    path.display(),
                    hint
                )
            }
            ConfigError::UnreadableKeyFile { path, error, hint } => {
                write!(
                    f,
                    "Master encryption key file is not readable: '{}': {}\n  Hint: {}",
                    path.display(),
                    error,
                    hint
                )
            }
            ConfigError::InsecureKeyPermissions { path, actual, expected, hint } => {
                write!(
                    f,
                    "Master encryption key has insecure permissions: '{}' (mode {:04o}, expected {:04o} or stricter)\n  Hint: {}",
                    path.display(),
                    actual,
                    expected,
                    hint
                )
            }
            ConfigError::EmptyKeyFile { path, hint } => {
                write!(
                    f,
                    "Master encryption key file is empty: '{}'\n  Hint: {}",
                    path.display(),
                    hint
                )
            }
            ConfigError::InvalidKeyFormat { path, error, hint } => {
                write!(
                    f,
                    "Master encryption key is not valid PEM format: '{}': {}\n  Hint: {}",
                    path.display(),
                    error,
                    hint
                )
            }
            ConfigError::WrongKeyType { path, hint } => {
                write!(
                    f,
                    "Master encryption key is not an RSA private key: '{}'\n  Hint: {}",
                    path.display(),
                    hint
                )
            }
            ConfigError::InsufficientKeySize { path, actual_bits, minimum_bits, hint } => {
                write!(
                    f,
                    "Master encryption key has insufficient size: '{}' ({} bits, minimum {} bits)\n  Hint: {}",
                    path.display(),
                    actual_bits,
                    minimum_bits,
                    hint
                )
            }
        }
    }
}

impl Error for ConfigError {}

/// Result of configuration validation.
#[derive(Debug)]
pub struct ValidationResult {
    /// Path to the secure assets directory.
    pub secure_assets_path: PathBuf,
    /// Path to the master key file.
    pub master_key_path: PathBuf,
    /// Key size in bits.
    pub key_size_bits: u32,
    /// Whether the keyspace root is ready for secrets.
    pub keyspace_ready: bool,
}

/// Validates the secret store key material configuration.
///
/// This function checks that:
/// 1. The secure assets directory exists with correct permissions (0700)
/// 2. The master key file (secret.key) exists
/// 3. The master key file has correct permissions (0400 or 0600)
/// 4. The master key is a valid RSA private key in PEM format
/// 5. The master key has at least 2048-bit security
///
/// # Returns
///
/// `Ok(ValidationResult)` with details about the configuration.
/// `Err(ConfigError)` with a descriptive error and remediation hint.
///
/// # Example
///
/// ```ignore
/// match validate_key_material() {
///     Ok(result) => {
///         println!("Key material validated:");
///         println!("  Master key: {}", result.master_key_path.display());
///         println!("  Key size: {} bits", result.key_size_bits);
///     }
///     Err(e) => {
///         eprintln!("Configuration error: {}", e);
///     }
/// }
/// ```
pub fn validate_key_material() -> Result<ValidationResult, ConfigError> {
    let secure_assets_path = Config::SecureAssets.get_path();
    let master_key_path = secure_assets_path.join("secret.key");

    // Check secure assets directory exists
    if !secure_assets_path.exists() {
        return Err(ConfigError::MissingDirectory {
            path: secure_assets_path,
            hint: "Run 'nanoctl install' or 'nanoctl setup' to generate secure assets, \
                   or set NANOCLOUD_SECURE_ASSETS to an existing directory.".to_string(),
        });
    }

    // Check directory permissions
    validate_directory_permissions(&secure_assets_path)?;

    // Check master key file exists
    if !master_key_path.exists() {
        return Err(ConfigError::MissingKeyFile {
            path: master_key_path,
            hint: "Run 'nanoctl install' or 'nanoctl setup' to generate the master encryption key. \
                   The file should be named 'secret.key' in the secure assets directory.".to_string(),
        });
    }

    // Check key file permissions
    validate_key_file_permissions(&master_key_path)?;

    // Read and validate key content
    let key_size_bits = validate_key_content(&master_key_path)?;

    // Check keyspace root
    let keyspace_ready = validate_keyspace_root();

    Ok(ValidationResult {
        secure_assets_path,
        master_key_path,
        key_size_bits,
        keyspace_ready,
    })
}

/// Validates secure assets directory permissions.
fn validate_directory_permissions(path: &Path) -> Result<(), ConfigError> {
    let metadata = match fs::metadata(path) {
        Ok(m) => m,
        Err(e) => {
            return Err(ConfigError::MissingDirectory {
                path: path.to_path_buf(),
                hint: format!("Error reading directory: {}", e),
            });
        }
    };

    let mode = metadata.permissions().mode() & 0o777;

    // Allow 0700 (owner rwx only)
    if mode & 0o077 != 0 {
        return Err(ConfigError::InsecurePermissions {
            path: path.to_path_buf(),
            actual: mode,
            expected: 0o700,
            hint: format!(
                "Run: chmod 700 '{}' to restrict access to owner only.",
                path.display()
            ),
        });
    }

    Ok(())
}

/// Validates master key file permissions.
fn validate_key_file_permissions(path: &Path) -> Result<(), ConfigError> {
    let metadata = match fs::metadata(path) {
        Ok(m) => m,
        Err(e) => {
            return Err(ConfigError::UnreadableKeyFile {
                path: path.to_path_buf(),
                error: e.to_string(),
                hint: "Check file permissions and ownership.".to_string(),
            });
        }
    };

    let mode = metadata.permissions().mode() & 0o777;

    // Allow 0400 (owner read only) or 0600 (owner read-write)
    // Reject group/other access
    if mode & 0o077 != 0 {
        return Err(ConfigError::InsecureKeyPermissions {
            path: path.to_path_buf(),
            actual: mode,
            expected: 0o400,
            hint: format!(
                "Run: chmod 400 '{}' to restrict access to owner read-only.",
                path.display()
            ),
        });
    }

    Ok(())
}

/// Validates master key content.
fn validate_key_content(path: &Path) -> Result<u32, ConfigError> {
    use openssl::pkey::PKey;

    // Read the file
    let content = match fs::read(path) {
        Ok(c) => c,
        Err(e) => {
            return Err(ConfigError::UnreadableKeyFile {
                path: path.to_path_buf(),
                error: e.to_string(),
                hint: "Check file permissions and ownership.".to_string(),
            });
        }
    };

    // Check not empty
    if content.is_empty() {
        return Err(ConfigError::EmptyKeyFile {
            path: path.to_path_buf(),
            hint: "Regenerate the key using 'nanoctl install --force' or \
                   delete the file and run 'nanoctl setup'.".to_string(),
        });
    }

    // Parse as PEM private key
    let pkey = match PKey::private_key_from_pem(&content) {
        Ok(k) => k,
        Err(e) => {
            return Err(ConfigError::InvalidKeyFormat {
                path: path.to_path_buf(),
                error: e.to_string(),
                hint: "The key file must be a PEM-encoded private key. \
                       Regenerate using 'nanoctl install --force'.".to_string(),
            });
        }
    };

    // Check it's an RSA key
    let rsa = match pkey.rsa() {
        Ok(r) => r,
        Err(_) => {
            return Err(ConfigError::WrongKeyType {
                path: path.to_path_buf(),
                hint: "The master encryption key must be an RSA private key. \
                       Regenerate using 'nanoctl install --force'.".to_string(),
            });
        }
    };

    // Check key size (minimum 2048 bits)
    let key_size = rsa.size() * 8; // size() returns bytes
    let minimum_bits = 2048;

    if key_size < minimum_bits {
        return Err(ConfigError::InsufficientKeySize {
            path: path.to_path_buf(),
            actual_bits: key_size,
            minimum_bits,
            hint: format!(
                "RSA key must be at least {} bits for security. \
                 Regenerate using 'nanoctl install --force'.",
                minimum_bits
            ),
        });
    }

    Ok(key_size)
}

/// Checks if the keyspace root is configured and writable.
fn validate_keyspace_root() -> bool {
    let keyspace_root = Config::Keyspace.get_path();
    let secrets_root = keyspace_root.join("secrets");

    // Check if directory exists or can be created
    if secrets_root.exists() {
        // Check if writable by attempting to create a test file
        let test_path = secrets_root.join(".config_test");
        match fs::File::create(&test_path) {
            Ok(_) => {
                let _ = fs::remove_file(test_path);
                true
            }
            Err(_) => false,
        }
    } else {
        // Check if parent is writable
        match fs::create_dir_all(&secrets_root) {
            Ok(_) => {
                let _ = fs::remove_dir(&secrets_root);
                true
            }
            Err(_) => false,
        }
    }
}

/// Quick check if key material exists (without full validation).
///
/// This is a fast check suitable for startup paths where full validation
/// would be too slow.
pub fn key_material_exists() -> bool {
    let secure_assets_path = Config::SecureAssets.get_path();
    let master_key_path = secure_assets_path.join("secret.key");
    master_key_path.exists()
}

/// Returns a human-readable status summary of the configuration.
pub fn configuration_status() -> String {
    match validate_key_material() {
        Ok(result) => {
            format!(
                "Secret store configuration: OK\n\
                 - Secure assets: {}\n\
                 - Master key: {} ({} bits)\n\
                 - Keyspace: {}",
                result.secure_assets_path.display(),
                result.master_key_path.display(),
                result.key_size_bits,
                if result.keyspace_ready { "ready" } else { "not ready" }
            )
        }
        Err(e) => {
            format!("Secret store configuration: ERROR\n{}", e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::util::security::SecureAssets;
    use serial_test::serial;
    use std::env;
    use tempfile::tempdir;

    #[test]
    #[serial]
    fn validates_complete_setup() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let assets_dir = temp_dir.path().join("assets");
        fs::create_dir_all(&assets_dir).expect("failed to create assets dir");
        SecureAssets::generate(&assets_dir, false).expect("failed to generate assets");
        env::set_var(
            "NANOCLOUD_SECURE_ASSETS",
            assets_dir.to_string_lossy().to_string(),
        );

        let result = validate_key_material();
        assert!(result.is_ok(), "validation should succeed: {:?}", result);

        let info = result.unwrap();
        assert!(info.key_size_bits >= 2048);

        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn detects_missing_directory() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let assets_dir = temp_dir.path().join("nonexistent");
        env::set_var(
            "NANOCLOUD_SECURE_ASSETS",
            assets_dir.to_string_lossy().to_string(),
        );

        let result = validate_key_material();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ConfigError::MissingDirectory { .. }));

        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn detects_missing_key_file() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let assets_dir = temp_dir.path().join("assets");
        fs::create_dir_all(&assets_dir).expect("failed to create assets dir");
        fs::set_permissions(&assets_dir, fs::Permissions::from_mode(0o700))
            .expect("failed to set permissions");
        env::set_var(
            "NANOCLOUD_SECURE_ASSETS",
            assets_dir.to_string_lossy().to_string(),
        );

        let result = validate_key_material();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ConfigError::MissingKeyFile { .. }));

        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn detects_empty_key_file() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let assets_dir = temp_dir.path().join("assets");
        fs::create_dir_all(&assets_dir).expect("failed to create assets dir");
        fs::set_permissions(&assets_dir, fs::Permissions::from_mode(0o700))
            .expect("failed to set permissions");

        let key_path = assets_dir.join("secret.key");
        fs::write(&key_path, "").expect("failed to write empty key");
        fs::set_permissions(&key_path, fs::Permissions::from_mode(0o400))
            .expect("failed to set key permissions");

        env::set_var(
            "NANOCLOUD_SECURE_ASSETS",
            assets_dir.to_string_lossy().to_string(),
        );

        let result = validate_key_material();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ConfigError::EmptyKeyFile { .. }));

        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn detects_invalid_key_format() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let assets_dir = temp_dir.path().join("assets");
        fs::create_dir_all(&assets_dir).expect("failed to create assets dir");
        fs::set_permissions(&assets_dir, fs::Permissions::from_mode(0o700))
            .expect("failed to set permissions");

        let key_path = assets_dir.join("secret.key");
        fs::write(&key_path, "not a valid PEM key").expect("failed to write invalid key");
        fs::set_permissions(&key_path, fs::Permissions::from_mode(0o400))
            .expect("failed to set key permissions");

        env::set_var(
            "NANOCLOUD_SECURE_ASSETS",
            assets_dir.to_string_lossy().to_string(),
        );

        let result = validate_key_material();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ConfigError::InvalidKeyFormat { .. }));

        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn key_material_exists_returns_true() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let assets_dir = temp_dir.path().join("assets");
        fs::create_dir_all(&assets_dir).expect("failed to create assets dir");
        SecureAssets::generate(&assets_dir, false).expect("failed to generate assets");
        env::set_var(
            "NANOCLOUD_SECURE_ASSETS",
            assets_dir.to_string_lossy().to_string(),
        );

        assert!(key_material_exists());

        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    #[serial]
    fn key_material_exists_returns_false() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let assets_dir = temp_dir.path().join("nonexistent");
        env::set_var(
            "NANOCLOUD_SECURE_ASSETS",
            assets_dir.to_string_lossy().to_string(),
        );

        assert!(!key_material_exists());

        env::remove_var("NANOCLOUD_SECURE_ASSETS");
    }

    #[test]
    fn config_error_display() {
        let err = ConfigError::MissingDirectory {
            path: PathBuf::from("/path/to/dir"),
            hint: "Run setup".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("/path/to/dir"));
        assert!(msg.contains("Hint"));
        assert!(msg.contains("Run setup"));
    }
}
