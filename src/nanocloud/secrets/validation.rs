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

//! Validation for secret metadata and timestamps.
//!
//! This module provides validation functions for:
//! - Namespace names
//! - Secret names
//! - Type names
//! - Timestamps (RFC3339 format)

use chrono::{DateTime, Utc};

use crate::nanocloud::secrets::error::SecretError;

/// Maximum length for namespace names.
const MAX_NAMESPACE_LENGTH: usize = 253;

/// Maximum length for secret names.
const MAX_NAME_LENGTH: usize = 253;

/// Maximum length for type names.
const MAX_TYPE_LENGTH: usize = 253;

/// Validates a namespace name.
///
/// Requirements:
/// - Must not be empty
/// - Must not exceed 253 characters
/// - Must contain only alphanumeric characters, hyphens, and dots
/// - Must start and end with an alphanumeric character
pub fn validate_namespace(namespace: &str) -> Result<(), SecretError> {
    if namespace.is_empty() {
        return Err(SecretError::Validation(
            "Namespace must not be empty".to_string(),
        ));
    }

    if namespace.len() > MAX_NAMESPACE_LENGTH {
        return Err(SecretError::Validation(format!(
            "Namespace '{}' exceeds maximum length of {} characters",
            namespace, MAX_NAMESPACE_LENGTH
        )));
    }

    if !is_valid_dns_label(namespace) {
        return Err(SecretError::Validation(format!(
            "Namespace '{}' contains invalid characters; must contain only alphanumeric characters, hyphens, and dots, and start/end with alphanumeric",
            namespace
        )));
    }

    Ok(())
}

/// Validates a secret name.
///
/// Requirements:
/// - Must not be empty
/// - Must not exceed 253 characters
/// - Must contain only alphanumeric characters, hyphens, underscores, and dots
/// - Must start and end with an alphanumeric character
pub fn validate_name(name: &str) -> Result<(), SecretError> {
    if name.is_empty() {
        return Err(SecretError::Validation(
            "Secret name must not be empty".to_string(),
        ));
    }

    if name.len() > MAX_NAME_LENGTH {
        return Err(SecretError::Validation(format!(
            "Secret name '{}' exceeds maximum length of {} characters",
            name, MAX_NAME_LENGTH
        )));
    }

    if !is_valid_resource_name(name) {
        return Err(SecretError::Validation(format!(
            "Secret name '{}' contains invalid characters; must contain only alphanumeric characters, hyphens, underscores, and dots, and start/end with alphanumeric",
            name
        )));
    }

    Ok(())
}

/// Validates a secret type name.
///
/// Requirements:
/// - Must not be empty
/// - Must not exceed 253 characters
pub fn validate_type_name(type_name: &str) -> Result<(), SecretError> {
    if type_name.is_empty() {
        return Err(SecretError::Validation(
            "Secret type must not be empty".to_string(),
        ));
    }

    if type_name.len() > MAX_TYPE_LENGTH {
        return Err(SecretError::Validation(format!(
            "Secret type '{}' exceeds maximum length of {} characters",
            type_name, MAX_TYPE_LENGTH
        )));
    }

    Ok(())
}

/// Parses and validates an RFC3339 timestamp.
pub fn parse_timestamp(timestamp: &str) -> Result<DateTime<Utc>, SecretError> {
    DateTime::parse_from_rfc3339(timestamp)
        .map_err(|e| {
            SecretError::Validation(format!(
                "Invalid timestamp '{}': {}",
                timestamp, e
            ))
        })
        .map(|dt| dt.with_timezone(&Utc))
}

/// Checks if a string is a valid DNS label (for namespaces).
fn is_valid_dns_label(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    let chars: Vec<char> = s.chars().collect();

    // Must start with alphanumeric
    if !chars.first().is_some_and(|c| c.is_ascii_alphanumeric()) {
        return false;
    }

    // Must end with alphanumeric
    if !chars.last().is_some_and(|c| c.is_ascii_alphanumeric()) {
        return false;
    }

    // All characters must be alphanumeric, hyphen, or dot
    chars
        .iter()
        .all(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '.')
}

/// Checks if a string is a valid resource name.
fn is_valid_resource_name(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    let chars: Vec<char> = s.chars().collect();

    // Must start with alphanumeric
    if !chars.first().is_some_and(|c| c.is_ascii_alphanumeric()) {
        return false;
    }

    // Must end with alphanumeric
    if !chars.last().is_some_and(|c| c.is_ascii_alphanumeric()) {
        return false;
    }

    // All characters must be alphanumeric, hyphen, underscore, or dot
    chars
        .iter()
        .all(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_' || *c == '.')
}

#[cfg(test)]
mod tests {
    use super::*;

    // Namespace validation tests

    #[test]
    fn valid_namespace() {
        assert!(validate_namespace("default").is_ok());
        assert!(validate_namespace("kube-system").is_ok());
        assert!(validate_namespace("my.namespace").is_ok());
        assert!(validate_namespace("a").is_ok());
        assert!(validate_namespace("a1b2c3").is_ok());
    }

    #[test]
    fn invalid_namespace_empty() {
        let result = validate_namespace("");
        assert!(matches!(result, Err(SecretError::Validation(_))));
    }

    #[test]
    fn invalid_namespace_too_long() {
        let long_name = "a".repeat(MAX_NAMESPACE_LENGTH + 1);
        let result = validate_namespace(&long_name);
        assert!(matches!(result, Err(SecretError::Validation(_))));
    }

    #[test]
    fn invalid_namespace_starts_with_hyphen() {
        let result = validate_namespace("-invalid");
        assert!(matches!(result, Err(SecretError::Validation(_))));
    }

    #[test]
    fn invalid_namespace_ends_with_hyphen() {
        let result = validate_namespace("invalid-");
        assert!(matches!(result, Err(SecretError::Validation(_))));
    }

    #[test]
    fn invalid_namespace_special_chars() {
        assert!(matches!(
            validate_namespace("invalid/namespace"),
            Err(SecretError::Validation(_))
        ));
        assert!(matches!(
            validate_namespace("invalid namespace"),
            Err(SecretError::Validation(_))
        ));
        assert!(matches!(
            validate_namespace("invalid@namespace"),
            Err(SecretError::Validation(_))
        ));
    }

    // Name validation tests

    #[test]
    fn valid_name() {
        assert!(validate_name("my-secret").is_ok());
        assert!(validate_name("my_secret").is_ok());
        assert!(validate_name("my.secret").is_ok());
        assert!(validate_name("MySecret123").is_ok());
        assert!(validate_name("a").is_ok());
    }

    #[test]
    fn invalid_name_empty() {
        let result = validate_name("");
        assert!(matches!(result, Err(SecretError::Validation(_))));
    }

    #[test]
    fn invalid_name_too_long() {
        let long_name = "a".repeat(MAX_NAME_LENGTH + 1);
        let result = validate_name(&long_name);
        assert!(matches!(result, Err(SecretError::Validation(_))));
    }

    #[test]
    fn invalid_name_starts_with_hyphen() {
        let result = validate_name("-invalid");
        assert!(matches!(result, Err(SecretError::Validation(_))));
    }

    #[test]
    fn invalid_name_special_chars() {
        assert!(matches!(
            validate_name("invalid/name"),
            Err(SecretError::Validation(_))
        ));
        assert!(matches!(
            validate_name("invalid name"),
            Err(SecretError::Validation(_))
        ));
    }

    // Type name validation tests

    #[test]
    fn valid_type_name() {
        assert!(validate_type_name("Opaque").is_ok());
        assert!(validate_type_name("kubernetes.io/tls").is_ok());
        assert!(validate_type_name("bootstrap.kubernetes.io/token").is_ok());
    }

    #[test]
    fn invalid_type_name_empty() {
        let result = validate_type_name("");
        assert!(matches!(result, Err(SecretError::Validation(_))));
    }

    #[test]
    fn invalid_type_name_too_long() {
        let long_type = "a".repeat(MAX_TYPE_LENGTH + 1);
        let result = validate_type_name(&long_type);
        assert!(matches!(result, Err(SecretError::Validation(_))));
    }

    // Timestamp validation tests

    #[test]
    fn valid_timestamp() {
        let result = parse_timestamp("2024-01-15T10:30:00Z");
        assert!(result.is_ok());

        let result = parse_timestamp("2024-01-15T10:30:00+00:00");
        assert!(result.is_ok());

        let result = parse_timestamp("2024-06-15T14:30:00.123456789Z");
        assert!(result.is_ok());
    }

    #[test]
    fn invalid_timestamp() {
        assert!(matches!(
            parse_timestamp("not-a-timestamp"),
            Err(SecretError::Validation(_))
        ));
        assert!(matches!(
            parse_timestamp("2024-01-15"),
            Err(SecretError::Validation(_))
        ));
        assert!(matches!(
            parse_timestamp(""),
            Err(SecretError::Validation(_))
        ));
    }
}
