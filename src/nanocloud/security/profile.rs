// Allow dead_code: This module provides a public API for security validation
// that is tested but not yet used by the main binary. The functions will be
// consumed by container runtime code when it integrates security helpers.
#![allow(dead_code)]

//! Linux capability normalization and validation utilities.
//!
//! This module provides helpers for normalizing and validating Linux capability
//! names used in container security profiles.
//!
//! # Capability Normalization
//!
//! Capability names are normalized to the canonical `CAP_FOO_BAR` format:
//!
//! - **Case Handling**: All input is converted to uppercase. Input like `cap_net_raw`,
//!   `Cap_Net_Raw`, or `NET_RAW` all normalize to `CAP_NET_RAW`.
//!
//! - **Prefix Handling**: The `CAP_` prefix is added if missing. `NET_ADMIN` becomes
//!   `CAP_NET_ADMIN`.
//!
//! - **Separator Aliases**: Dashes (`-`) and spaces are converted to underscores.
//!   `cap-net-raw` and `cap net raw` both normalize to `CAP_NET_RAW`.
//!
//! - **Whitespace**: Leading and trailing whitespace is trimmed.
//!
//! # Kernel Alignment
//!
//! The capability names correspond to Linux kernel capabilities defined in
//! `<linux/capability.h>`. The supported set is aligned with the capabilities
//! available in Linux 5.x+ kernels.
//!
//! ## Update Policy
//!
//! When new capabilities are added to the Linux kernel, this module should be
//! updated to include them. The `NON_PRIVILEGED_CAPABILITIES` list specifically
//! contains capabilities that can be granted without enabling privileged mode.
//!
//! ## Known Capability Sets
//!
//! - **Non-privileged**: Capabilities in `NON_PRIVILEGED_CAPABILITIES` can be
//!   requested by non-privileged containers. These are typically networking
//!   capabilities needed for common operations.
//!
//! # Error Semantics
//!
//! The normalization functions in this module do not validate that a capability
//! exists in the kernel. They only transform the input to canonical form. Use
//! [`crate::nanocloud::security::SecurityError::UnknownCapability`] when
//! validation against a known set fails.
//!
//! Empty or whitespace-only input normalizes to an empty string, which can be
//! used by callers to skip invalid entries.
//!
//! # Examples
//!
//! ```
//! use nanocloud::nanocloud::security::profile::{normalize_capability_name, dedupe_capabilities};
//!
//! // Case normalization
//! assert_eq!(normalize_capability_name("cap_net_raw"), "CAP_NET_RAW");
//!
//! // Prefix addition
//! assert_eq!(normalize_capability_name("NET_ADMIN"), "CAP_NET_ADMIN");
//!
//! // Separator aliases
//! assert_eq!(normalize_capability_name("cap-net-bind-service"), "CAP_NET_BIND_SERVICE");
//!
//! // Deduplication with normalization
//! let caps = vec!["CAP_NET_RAW", "cap_net_raw", "NET_ADMIN"];
//! let deduped = dedupe_capabilities(caps);
//! assert_eq!(deduped, vec!["CAP_NET_RAW", "CAP_NET_ADMIN"]);
//! ```

use std::collections::HashSet;
#[cfg(feature = "security-test-caps")]
use std::sync::OnceLock;

use super::error::SecurityError;
use crate::nanocloud::logger::log_debug;

const COMPONENT: &str = "security";

// =============================================================================
// Runtime Capability List Extension (feature: security-test-caps)
// =============================================================================
//
// When the `security-test-caps` feature is enabled, additional capabilities
// can be loaded from the `NANOCLOUD_EXTRA_CAPABILITIES` environment variable.
// This is intended for testing scenarios where new kernel capabilities need
// to be validated before being added to the built-in list.
//
// Format: Comma-separated list of capability names (any format accepted)
// Example: NANOCLOUD_EXTRA_CAPABILITIES="CAP_FUTURE_CAP,CAP_ANOTHER"
//
// Security note: This feature should NOT be enabled in production builds.
// =============================================================================

#[cfg(feature = "security-test-caps")]
static EXTRA_CAPABILITIES: OnceLock<Vec<String>> = OnceLock::new();

/// Environment variable name for extra capabilities (only with `security-test-caps` feature).
#[cfg(feature = "security-test-caps")]
pub const EXTRA_CAPABILITIES_ENV: &str = "NANOCLOUD_EXTRA_CAPABILITIES";

/// Returns extra capabilities loaded from environment (only with `security-test-caps` feature).
///
/// This function reads from `NANOCLOUD_EXTRA_CAPABILITIES` environment variable
/// on first call and caches the result. Returns an empty slice if the variable
/// is not set or the feature is disabled.
///
/// # Security Warning
///
/// This feature is intended for testing only. Do not enable `security-test-caps`
/// in production builds as it allows bypassing capability validation.
#[cfg(feature = "security-test-caps")]
pub fn get_extra_capabilities() -> &'static [String] {
    EXTRA_CAPABILITIES.get_or_init(|| {
        std::env::var(EXTRA_CAPABILITIES_ENV)
            .ok()
            .map(|val| {
                val.split(',')
                    .map(|s| normalize_capability_name(s.trim()))
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    })
}

/// Checks if a capability is in the extra capabilities list (only with `security-test-caps` feature).
#[cfg(feature = "security-test-caps")]
fn is_extra_capability(normalized: &str) -> bool {
    get_extra_capabilities().iter().any(|c| c == normalized)
}

// =============================================================================
// Kernel Capability List Maintenance
// =============================================================================
//
// The capability lists in this module are derived from the Linux kernel's
// `include/uapi/linux/capability.h` header file. When updating for new kernel
// versions, follow this process:
//
// 1. Check the kernel source for new capabilities:
//    https://github.com/torvalds/linux/blob/master/include/uapi/linux/capability.h
//
// 2. Look for `#define CAP_*` entries and their corresponding `CAP_LAST_CAP`
//
// 3. Update the `ALL_CAPABILITIES` constant below with any new entries
//
// 4. Run the `capability_list_contains_core_set` test to verify completeness
//
// 5. Document the kernel version in the constant's docstring
//
// Current capability list is aligned with Linux kernel 6.x (up to CAP_CHECKPOINT_RESTORE)
// =============================================================================

/// Complete list of Linux capabilities as of kernel 6.x.
///
/// This list includes all capabilities from `CAP_CHOWN` (0) through
/// `CAP_CHECKPOINT_RESTORE` (40). When the kernel adds new capabilities,
/// update this list and run the verification tests.
///
/// # Kernel Version Alignment
///
/// - Linux 2.6.24+: CAP_CHOWN through CAP_SETFCAP (0-34)
/// - Linux 2.6.25+: Added CAP_MAC_OVERRIDE, CAP_MAC_ADMIN (35-36)
/// - Linux 2.6.39+: Added CAP_SYSLOG (37)
/// - Linux 3.5+: Added CAP_WAKE_ALARM (38)
/// - Linux 3.8+: Added CAP_BLOCK_SUSPEND (39)
/// - Linux 5.9+: Added CAP_CHECKPOINT_RESTORE (40)
///
/// # Updating This List
///
/// See the maintenance comment at the top of this file for instructions on
/// updating this list for new kernel versions.
pub const ALL_CAPABILITIES: &[&str] = &[
    "CAP_AUDIT_CONTROL",
    "CAP_AUDIT_READ",
    "CAP_AUDIT_WRITE",
    "CAP_BLOCK_SUSPEND",
    "CAP_BPF",
    "CAP_CHECKPOINT_RESTORE",
    "CAP_CHOWN",
    "CAP_DAC_OVERRIDE",
    "CAP_DAC_READ_SEARCH",
    "CAP_FOWNER",
    "CAP_FSETID",
    "CAP_IPC_LOCK",
    "CAP_IPC_OWNER",
    "CAP_KILL",
    "CAP_LEASE",
    "CAP_LINUX_IMMUTABLE",
    "CAP_MAC_ADMIN",
    "CAP_MAC_OVERRIDE",
    "CAP_MKNOD",
    "CAP_NET_ADMIN",
    "CAP_NET_BIND_SERVICE",
    "CAP_NET_BROADCAST",
    "CAP_NET_RAW",
    "CAP_PERFMON",
    "CAP_SETFCAP",
    "CAP_SETGID",
    "CAP_SETPCAP",
    "CAP_SETUID",
    "CAP_SYSLOG",
    "CAP_SYS_ADMIN",
    "CAP_SYS_BOOT",
    "CAP_SYS_CHROOT",
    "CAP_SYS_MODULE",
    "CAP_SYS_NICE",
    "CAP_SYS_PACCT",
    "CAP_SYS_PTRACE",
    "CAP_SYS_RAWIO",
    "CAP_SYS_RESOURCE",
    "CAP_SYS_TIME",
    "CAP_SYS_TTY_CONFIG",
    "CAP_WAKE_ALARM",
];

/// Core capabilities that must always be present in `ALL_CAPABILITIES`.
///
/// This constant defines the minimum set of capabilities that any correct
/// implementation must support. Tests verify that `ALL_CAPABILITIES` includes
/// all of these entries.
const CORE_CAPABILITIES: &[&str] = &[
    "CAP_CHOWN",
    "CAP_DAC_OVERRIDE",
    "CAP_FOWNER",
    "CAP_KILL",
    "CAP_NET_ADMIN",
    "CAP_NET_BIND_SERVICE",
    "CAP_NET_RAW",
    "CAP_SETGID",
    "CAP_SETUID",
    "CAP_SYS_ADMIN",
    "CAP_SYS_CHROOT",
];

/// Checks if a capability name is a valid Linux capability.
///
/// This function normalizes the input and checks if it exists in the
/// `ALL_CAPABILITIES` list.
///
/// # Arguments
///
/// * `name` - The capability name to validate (any case/format)
///
/// # Returns
///
/// `true` if the capability is valid, `false` otherwise.
///
/// # Examples
///
/// ```
/// use nanocloud::nanocloud::security::profile::is_valid_capability;
///
/// assert!(is_valid_capability("CAP_NET_RAW"));
/// assert!(is_valid_capability("net_raw"));
/// assert!(is_valid_capability("cap-net-raw"));
/// assert!(!is_valid_capability("CAP_INVALID"));
/// ```
pub fn is_valid_capability(name: &str) -> bool {
    let normalized = normalize_capability_name(name);
    if normalized.is_empty() {
        return false;
    }
    if ALL_CAPABILITIES.contains(&normalized.as_str()) {
        return true;
    }
    // When security-test-caps feature is enabled, also check extra capabilities
    #[cfg(feature = "security-test-caps")]
    if is_extra_capability(&normalized) {
        return true;
    }
    false
}

/// Validates a capability name and returns the normalized form or an error.
///
/// This is the recommended entry point for external callers who need to
/// validate and normalize capability names in one operation.
///
/// # Arguments
///
/// * `name` - The capability name to validate (any case/format)
///
/// # Returns
///
/// - `Ok(String)` with the normalized capability name if valid
/// - `Err(SecurityError::UnknownCapability)` if the capability is not recognized
///
/// # Examples
///
/// ```
/// use nanocloud::nanocloud::security::profile::validate_capability;
///
/// assert_eq!(validate_capability("net_raw").unwrap(), "CAP_NET_RAW");
/// assert!(validate_capability("invalid_cap").is_err());
/// ```
pub fn validate_capability(name: &str) -> Result<String, SecurityError> {
    let normalized = normalize_capability_name(name);
    if normalized.is_empty() {
        log_debug(
            COMPONENT,
            "capability validation failed: empty input",
            &[("input", name)],
        );
        return Err(SecurityError::unknown_capability(name));
    }
    if ALL_CAPABILITIES.contains(&normalized.as_str()) {
        return Ok(normalized);
    }
    // When security-test-caps feature is enabled, also check extra capabilities
    #[cfg(feature = "security-test-caps")]
    if is_extra_capability(&normalized) {
        return Ok(normalized);
    }
    log_debug(
        COMPONENT,
        "capability validation failed: unknown capability",
        &[("input", name), ("normalized", &normalized)],
    );
    Err(SecurityError::unknown_capability(name))
}

/// Validates a list of capabilities and returns normalized forms or errors.
///
/// All capabilities must be valid for the function to succeed. If any
/// capability is invalid, returns an error for the first invalid one.
///
/// # Arguments
///
/// * `caps` - An iterator of capability names to validate
///
/// # Returns
///
/// - `Ok(Vec<String>)` with deduplicated, normalized capability names
/// - `Err(SecurityError::UnknownCapability)` for the first invalid capability
pub fn validate_capabilities<'a>(
    caps: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<String>, SecurityError> {
    let mut seen = HashSet::new();
    let mut result = Vec::new();

    for cap in caps {
        let trimmed = cap.trim();
        if trimmed.is_empty() {
            continue;
        }
        let normalized = validate_capability(trimmed)?;
        if seen.insert(normalized.clone()) {
            result.push(normalized);
        }
    }

    Ok(result)
}

/// Capabilities that can be requested without toggling `allowPrivileged`.
///
/// These networking capabilities are commonly needed by containers and are
/// considered safe to grant without full privileged mode:
///
/// - `CAP_NET_BIND_SERVICE`: Bind to ports below 1024
/// - `CAP_NET_ADMIN`: Various network administration operations
/// - `CAP_NET_RAW`: Use raw and packet sockets
///
/// # Kernel Alignment
///
/// These capabilities are available in all Linux kernels 2.6.24+. The list
/// may be expanded as new safe capabilities are identified.
pub const NON_PRIVILEGED_CAPABILITIES: &[&str] =
    &["CAP_NET_BIND_SERVICE", "CAP_NET_ADMIN", "CAP_NET_RAW"];

/// Normalizes capability names to the canonical `CAP_FOO_BAR` form.
///
/// # Normalization Rules
///
/// 1. Leading and trailing whitespace is trimmed
/// 2. Dashes (`-`) and spaces are replaced with underscores (`_`)
/// 3. All characters are converted to uppercase
/// 4. The `CAP_` prefix is added if not already present
///
/// # Returns
///
/// - The normalized capability name in `CAP_FOO_BAR` format
/// - An empty string if the input is empty or contains only whitespace
///
/// # Examples
///
/// ```
/// use nanocloud::nanocloud::security::profile::normalize_capability_name;
///
/// assert_eq!(normalize_capability_name("net_raw"), "CAP_NET_RAW");
/// assert_eq!(normalize_capability_name("CAP-NET-RAW"), "CAP_NET_RAW");
/// assert_eq!(normalize_capability_name("  cap net admin  "), "CAP_NET_ADMIN");
/// assert_eq!(normalize_capability_name(""), "");
/// ```
pub fn normalize_capability_name(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let mut candidate = trimmed
        .chars()
        .map(|ch| match ch {
            '-' | ' ' => '_',
            other => other,
        })
        .collect::<String>()
        .to_ascii_uppercase();
    if !candidate.starts_with("CAP_") {
        candidate = format!("CAP_{candidate}");
    }
    candidate
}

/// Deduplicates capability entries (after normalization) preserving insertion order.
///
/// This function normalizes each capability name and removes duplicates while
/// preserving the order of first occurrence. Empty or whitespace-only entries
/// are skipped entirely.
///
/// # Arguments
///
/// * `caps` - An iterator of capability name strings (any case/format)
///
/// # Returns
///
/// A vector of unique, normalized capability names in insertion order.
///
/// # Examples
///
/// ```
/// use nanocloud::nanocloud::security::profile::dedupe_capabilities;
///
/// // Duplicates after normalization are removed
/// let caps = vec!["CAP_NET_RAW", "cap_net_raw", "Cap-Net-Raw"];
/// assert_eq!(dedupe_capabilities(caps), vec!["CAP_NET_RAW"]);
///
/// // Order is preserved (first occurrence wins)
/// let caps = vec!["NET_ADMIN", "NET_RAW", "net_admin"];
/// assert_eq!(dedupe_capabilities(caps), vec!["CAP_NET_ADMIN", "CAP_NET_RAW"]);
///
/// // Empty entries are skipped
/// let caps = vec!["NET_RAW", "", "  ", "NET_ADMIN"];
/// assert_eq!(dedupe_capabilities(caps), vec!["CAP_NET_RAW", "CAP_NET_ADMIN"]);
/// ```
pub fn dedupe_capabilities<'a>(caps: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut ordered = Vec::new();
    for cap in caps {
        let normalized = normalize_capability_name(cap);
        if normalized.is_empty() {
            continue;
        }
        if !seen.insert(normalized.clone()) {
            continue;
        }
        ordered.push(normalized);
    }
    ordered
}

// =============================================================================
// Capability Set Comparison Helpers
// =============================================================================

/// Checks if a capability set is a subset of the allowed capabilities.
///
/// All capabilities in `caps` must be present in `ALL_CAPABILITIES` for
/// this function to return `true`. Capabilities are normalized before
/// comparison.
///
/// # Arguments
///
/// * `caps` - An iterator of capability names to check
///
/// # Returns
///
/// `true` if all capabilities in `caps` are valid Linux capabilities.
///
/// # Examples
///
/// ```
/// use nanocloud::nanocloud::security::profile::capabilities_are_subset_of_all;
///
/// assert!(capabilities_are_subset_of_all(["CAP_NET_RAW", "CAP_CHOWN"]));
/// assert!(capabilities_are_subset_of_all(["net_raw", "chown"]));
/// assert!(!capabilities_are_subset_of_all(["CAP_NET_RAW", "CAP_INVALID"]));
/// assert!(capabilities_are_subset_of_all(Vec::<&str>::new())); // empty set is a subset
/// ```
pub fn capabilities_are_subset_of_all<S: AsRef<str>>(caps: impl IntoIterator<Item = S>) -> bool {
    for cap in caps {
        if !is_valid_capability(cap.as_ref()) {
            return false;
        }
    }
    true
}

/// Checks if a capability set is a subset of the non-privileged capabilities.
///
/// All capabilities in `caps` must be present in `NON_PRIVILEGED_CAPABILITIES`
/// for this function to return `true`. Capabilities are normalized before
/// comparison.
///
/// # Arguments
///
/// * `caps` - An iterator of capability names to check
///
/// # Returns
///
/// `true` if all capabilities in `caps` are non-privileged.
///
/// # Examples
///
/// ```
/// use nanocloud::nanocloud::security::profile::capabilities_are_non_privileged;
///
/// assert!(capabilities_are_non_privileged(["CAP_NET_RAW", "CAP_NET_ADMIN"]));
/// assert!(capabilities_are_non_privileged(["net_raw"]));
/// assert!(!capabilities_are_non_privileged(["CAP_SYS_ADMIN"]));
/// assert!(capabilities_are_non_privileged(Vec::<&str>::new())); // empty set is non-privileged
/// ```
pub fn capabilities_are_non_privileged<S: AsRef<str>>(caps: impl IntoIterator<Item = S>) -> bool {
    for cap in caps {
        let normalized = normalize_capability_name(cap.as_ref());
        if normalized.is_empty() {
            continue;
        }
        if !NON_PRIVILEGED_CAPABILITIES.contains(&normalized.as_str()) {
            return false;
        }
    }
    true
}

/// Checks if two capability sets are equal after normalization.
///
/// Two sets are equal if they contain the same capabilities, regardless
/// of order, case, or duplicate entries.
///
/// # Arguments
///
/// * `a` - First capability set
/// * `b` - Second capability set
///
/// # Returns
///
/// `true` if both sets contain exactly the same normalized capabilities.
///
/// # Examples
///
/// ```
/// use nanocloud::nanocloud::security::profile::capability_sets_equal;
///
/// // Same capabilities, different case
/// assert!(capability_sets_equal(
///     ["CAP_NET_RAW", "CAP_CHOWN"],
///     ["cap_net_raw", "cap_chown"]
/// ));
///
/// // Same capabilities, different order
/// assert!(capability_sets_equal(
///     ["CAP_CHOWN", "CAP_NET_RAW"],
///     ["CAP_NET_RAW", "CAP_CHOWN"]
/// ));
///
/// // With duplicates
/// assert!(capability_sets_equal(
///     ["CAP_NET_RAW", "cap_net_raw"],
///     ["CAP_NET_RAW"]
/// ));
///
/// // Different sets
/// assert!(!capability_sets_equal(
///     ["CAP_NET_RAW"],
///     ["CAP_CHOWN"]
/// ));
/// ```
pub fn capability_sets_equal<S1: AsRef<str>, S2: AsRef<str>>(
    a: impl IntoIterator<Item = S1>,
    b: impl IntoIterator<Item = S2>,
) -> bool {
    let set_a: HashSet<String> = a
        .into_iter()
        .map(|s| normalize_capability_name(s.as_ref()))
        .filter(|s| !s.is_empty())
        .collect();
    let set_b: HashSet<String> = b
        .into_iter()
        .map(|s| normalize_capability_name(s.as_ref()))
        .filter(|s| !s.is_empty())
        .collect();
    set_a == set_b
}

/// Checks if capability set `a` is a subset of capability set `b`.
///
/// Returns `true` if every capability in `a` is also in `b`.
/// Capabilities are normalized before comparison.
///
/// # Arguments
///
/// * `a` - The potential subset
/// * `b` - The potential superset
///
/// # Returns
///
/// `true` if `a` is a subset of `b` (a ⊆ b).
///
/// # Examples
///
/// ```
/// use nanocloud::nanocloud::security::profile::capability_set_is_subset;
///
/// // Subset relationship
/// assert!(capability_set_is_subset(
///     ["CAP_NET_RAW"],
///     ["CAP_NET_RAW", "CAP_CHOWN"]
/// ));
///
/// // Equal sets are subsets of each other
/// assert!(capability_set_is_subset(
///     ["CAP_NET_RAW"],
///     ["cap_net_raw"]
/// ));
///
/// // Empty set is subset of anything
/// assert!(capability_set_is_subset(Vec::<&str>::new(), ["CAP_NET_RAW"]));
///
/// // Not a subset
/// assert!(!capability_set_is_subset(
///     ["CAP_NET_RAW", "CAP_CHOWN"],
///     ["CAP_NET_RAW"]
/// ));
/// ```
pub fn capability_set_is_subset<S1: AsRef<str>, S2: AsRef<str>>(
    a: impl IntoIterator<Item = S1>,
    b: impl IntoIterator<Item = S2>,
) -> bool {
    let set_a: HashSet<String> = a
        .into_iter()
        .map(|s| normalize_capability_name(s.as_ref()))
        .filter(|s| !s.is_empty())
        .collect();
    let set_b: HashSet<String> = b
        .into_iter()
        .map(|s| normalize_capability_name(s.as_ref()))
        .filter(|s| !s.is_empty())
        .collect();
    set_a.is_subset(&set_b)
}

/// Returns the capabilities in `a` that are not in `b`.
///
/// This is useful for finding which additional capabilities a set requests
/// beyond a baseline.
///
/// # Arguments
///
/// * `a` - The capability set to check
/// * `b` - The baseline capability set
///
/// # Returns
///
/// A vector of normalized capability names that are in `a` but not in `b`.
///
/// # Examples
///
/// ```
/// use nanocloud::nanocloud::security::profile::capability_set_difference;
///
/// let diff = capability_set_difference(
///     ["CAP_NET_RAW", "CAP_CHOWN", "CAP_SYS_ADMIN"],
///     ["CAP_NET_RAW", "CAP_CHOWN"]
/// );
/// assert_eq!(diff, vec!["CAP_SYS_ADMIN"]);
///
/// // No difference
/// let diff = capability_set_difference(
///     ["CAP_NET_RAW"],
///     ["CAP_NET_RAW", "CAP_CHOWN"]
/// );
/// assert!(diff.is_empty());
/// ```
pub fn capability_set_difference<S1: AsRef<str>, S2: AsRef<str>>(
    a: impl IntoIterator<Item = S1>,
    b: impl IntoIterator<Item = S2>,
) -> Vec<String> {
    let set_a: HashSet<String> = a
        .into_iter()
        .map(|s| normalize_capability_name(s.as_ref()))
        .filter(|s| !s.is_empty())
        .collect();
    let set_b: HashSet<String> = b
        .into_iter()
        .map(|s| normalize_capability_name(s.as_ref()))
        .filter(|s| !s.is_empty())
        .collect();
    let mut diff: Vec<String> = set_a.difference(&set_b).cloned().collect();
    diff.sort();
    diff
}

/// Returns the capabilities that require privileged mode.
///
/// Given a capability set, returns those capabilities that are NOT in
/// `NON_PRIVILEGED_CAPABILITIES` and therefore require privileged mode.
///
/// # Arguments
///
/// * `caps` - The capability set to check
///
/// # Returns
///
/// A sorted vector of normalized capability names requiring privileged mode.
///
/// # Examples
///
/// ```
/// use nanocloud::nanocloud::security::profile::capabilities_requiring_privileged;
///
/// let privileged = capabilities_requiring_privileged([
///     "CAP_NET_RAW",      // non-privileged
///     "CAP_SYS_ADMIN",    // privileged
///     "CAP_CHOWN"         // privileged
/// ]);
/// assert_eq!(privileged, vec!["CAP_CHOWN", "CAP_SYS_ADMIN"]);
///
/// // All non-privileged
/// let privileged = capabilities_requiring_privileged(["CAP_NET_RAW", "CAP_NET_ADMIN"]);
/// assert!(privileged.is_empty());
/// ```
pub fn capabilities_requiring_privileged<S: AsRef<str>>(caps: impl IntoIterator<Item = S>) -> Vec<String> {
    let mut result = Vec::new();
    for cap in caps {
        let normalized = normalize_capability_name(cap.as_ref());
        if normalized.is_empty() {
            continue;
        }
        if !NON_PRIVILEGED_CAPABILITIES.contains(&normalized.as_str()) {
            result.push(normalized);
        }
    }
    result.sort();
    result.dedup();
    result
}
