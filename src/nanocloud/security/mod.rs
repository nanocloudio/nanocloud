//! Security module for container capability and seccomp profile management.
//!
//! This module provides centralized security helpers for container operations.
//! Other modules should use these helpers instead of reimplementing security
//! logic to ensure consistent behavior and validation.
//!
//! # Module Overview
//!
//! - [`profile`] - Linux capability normalization, validation, and comparison
//! - [`seccomp`] - Seccomp BPF profile parsing, validation, and application
//! - [`error`] - Unified error types for security operations
//!
//! # When to Use This Module
//!
//! ## Capability Operations
//!
//! **Use [`profile`] when you need to:**
//!
//! - Normalize capability names from user input (Docker, K8s, OCI formats)
//! - Validate that requested capabilities are known Linux capabilities
//! - Check if capabilities require privileged mode
//! - Compare capability sets for equality or containment
//! - Deduplicate capability lists
//!
//! ```ignore
//! use nanocloud::nanocloud::security::profile::{
//!     validate_capabilities,
//!     capabilities_are_non_privileged,
//!     capabilities_requiring_privileged,
//! };
//!
//! // Validate and normalize user-provided capabilities
//! let caps = validate_capabilities(["net_raw", "CAP_CHOWN", "sys-admin"])?;
//!
//! // Check if privileged mode is required
//! if !capabilities_are_non_privileged(caps.iter().map(|s| s.as_str())) {
//!     let privileged = capabilities_requiring_privileged(caps.iter().map(|s| s.as_str()));
//!     warn!("Privileged mode required for: {:?}", privileged);
//! }
//! ```
//!
//! ## Seccomp Operations
//!
//! **Use [`seccomp`] when you need to:**
//!
//! - Parse seccomp profiles from JSON files or strings
//! - Validate syscall names and actions
//! - Apply seccomp filters to processes
//!
//! ```ignore
//! use nanocloud::nanocloud::security::seccomp::SeccompFilter;
//! use std::str::FromStr;
//!
//! // Parse and validate a seccomp profile
//! let filter = SeccompFilter::from_str(r#"{"deny": ["ptrace", "mount"]}"#)?;
//!
//! // Apply to current process
//! filter.apply()?;
//! ```
//!
//! # Anti-Patterns to Avoid
//!
//! **Do NOT:**
//!
//! - Manually parse capability names with string manipulation
//! - Hardcode capability lists without validation
//! - Skip normalization when comparing capability sets
//! - Implement custom seccomp profile parsing
//! - Bypass validation for "trusted" input
//!
//! **Instead:**
//!
//! - Always use [`profile::normalize_capability_name`] for normalization
//! - Always use [`profile::validate_capability`] before accepting capabilities
//! - Use [`profile::capability_sets_equal`] for comparison (handles case/format)
//! - Use [`seccomp::SeccompFilter`] for all seccomp operations
//!
//! # Capability Name Formats
//!
//! The [`profile`] module accepts capabilities in any of these formats:
//!
//! | Format | Example | Normalized Form |
//! |--------|---------|-----------------|
//! | Canonical | `CAP_NET_RAW` | `CAP_NET_RAW` |
//! | Lowercase | `cap_net_raw` | `CAP_NET_RAW` |
//! | Without prefix | `NET_RAW` | `CAP_NET_RAW` |
//! | Docker-style | `net_raw` | `CAP_NET_RAW` |
//! | Dash-separated | `cap-net-raw` | `CAP_NET_RAW` |
//!
//! # Privileged vs Non-Privileged Capabilities
//!
//! Only these capabilities can be granted without privileged mode:
//!
//! - `CAP_NET_BIND_SERVICE` - Bind to ports below 1024
//! - `CAP_NET_ADMIN` - Network administration
//! - `CAP_NET_RAW` - Raw socket access
//!
//! All other capabilities require privileged mode or explicit approval.
//! Use [`profile::capabilities_are_non_privileged`] to check.
//!
//! # Error Handling
//!
//! All validation functions return [`SecurityError`] on failure:
//!
//! ```ignore
//! use nanocloud::nanocloud::security::{SecurityError, profile::validate_capability};
//!
//! match validate_capability("INVALID_CAP") {
//!     Ok(normalized) => println!("Valid: {}", normalized),
//!     Err(SecurityError::UnknownCapability { name }) => {
//!         eprintln!("Unknown capability: {}", name);
//!     }
//!     Err(e) => eprintln!("Validation error: {}", e),
//! }
//! ```
//!
//! # Testing
//!
//! Run security module tests with:
//! ```bash
//! make test-security
//! ```
//!
//! This runs both unit tests and integration tests from
//! `tests/nanocloud/security/`.
//!
//! # Feature Flags
//!
//! - `security-test-caps` - Enables runtime capability list override via
//!   `NANOCLOUD_EXTRA_CAPABILITIES` environment variable. **For testing only.**

pub mod error;
pub mod profile;
pub mod seccomp;

// Re-export for consumers - allow unused until integration
#[allow(unused_imports)]
pub use error::SecurityError;

pub const SECURITY_POLICY_VIOLATION: &str = "[SecurityPolicyViolation]";
pub const PRIVILEGE_ESCALATION_DENIED: &str = "[PrivilegeEscalationDenied]";
