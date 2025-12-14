// Allow dead_code: This module provides error types for the security module
// that are tested but not yet used by the main binary.
#![allow(dead_code)]

use std::error::Error;
use std::fmt;
use std::io;
use std::path::PathBuf;

/// Consolidated error type for security validation operations.
///
/// This enum provides specific error variants for capability and seccomp
/// validation failures, allowing callers to distinguish between different
/// error conditions and provide appropriate feedback.
#[derive(Debug)]
pub enum SecurityError {
    /// An unknown or invalid capability was specified.
    UnknownCapability {
        /// The capability name that was not recognized.
        capability: String,
    },

    /// An unknown syscall was referenced in a seccomp profile.
    UnknownSyscall {
        /// The syscall name that was not recognized.
        syscall: String,
    },

    /// The seccomp profile JSON structure is invalid.
    InvalidSeccompProfile {
        /// Description of the parsing/structure error.
        reason: String,
    },

    /// Failed to read a seccomp profile from the filesystem.
    SeccompFileRead {
        /// The path that could not be read.
        path: PathBuf,
        /// The underlying I/O error.
        source: io::Error,
    },

    /// Failed to apply a seccomp filter to the process.
    SeccompApplyFailed {
        /// Description of the failure.
        reason: String,
    },

    /// A required capability set is empty when it should not be.
    EmptyCapabilitySet {
        /// Context about where the empty set was encountered.
        context: String,
    },

    /// An invalid seccomp action was specified.
    InvalidSeccompAction {
        /// The action that was not recognized.
        action: String,
    },
}

impl fmt::Display for SecurityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SecurityError::UnknownCapability { capability } => {
                write!(f, "unknown capability: '{}'", capability)
            }
            SecurityError::UnknownSyscall { syscall } => {
                write!(
                    f,
                    "unknown syscall '{}' referenced by seccomp profile",
                    syscall
                )
            }
            SecurityError::InvalidSeccompProfile { reason } => {
                write!(f, "invalid seccomp profile: {}", reason)
            }
            SecurityError::SeccompFileRead { path, source } => {
                write!(f, "failed to read {}: {}", path.display(), source)
            }
            SecurityError::SeccompApplyFailed { reason } => {
                write!(f, "failed to apply seccomp filter: {}", reason)
            }
            SecurityError::EmptyCapabilitySet { context } => {
                write!(f, "empty capability set: {}", context)
            }
            SecurityError::InvalidSeccompAction { action } => {
                write!(f, "invalid seccomp action: '{}'", action)
            }
        }
    }
}

impl Error for SecurityError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            SecurityError::SeccompFileRead { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl SecurityError {
    /// Creates an error for an unknown capability.
    pub fn unknown_capability(capability: impl Into<String>) -> Self {
        SecurityError::UnknownCapability {
            capability: capability.into(),
        }
    }

    /// Creates an error for an unknown syscall.
    pub fn unknown_syscall(syscall: impl Into<String>) -> Self {
        SecurityError::UnknownSyscall {
            syscall: syscall.into(),
        }
    }

    /// Creates an error for an invalid seccomp profile structure.
    pub fn invalid_profile(reason: impl Into<String>) -> Self {
        SecurityError::InvalidSeccompProfile {
            reason: reason.into(),
        }
    }

    /// Creates an error for a failed file read.
    pub fn file_read(path: impl Into<PathBuf>, source: io::Error) -> Self {
        SecurityError::SeccompFileRead {
            path: path.into(),
            source,
        }
    }

    /// Creates an error for a failed seccomp filter application.
    pub fn apply_failed(reason: impl Into<String>) -> Self {
        SecurityError::SeccompApplyFailed {
            reason: reason.into(),
        }
    }

    /// Creates an error for an empty capability set.
    pub fn empty_capability_set(context: impl Into<String>) -> Self {
        SecurityError::EmptyCapabilitySet {
            context: context.into(),
        }
    }

    /// Creates an error for an invalid seccomp action.
    pub fn invalid_action(action: impl Into<String>) -> Self {
        SecurityError::InvalidSeccompAction {
            action: action.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_capability_displays_correctly() {
        let err = SecurityError::unknown_capability("CAP_INVALID");
        assert_eq!(err.to_string(), "unknown capability: 'CAP_INVALID'");
    }

    #[test]
    fn unknown_syscall_displays_correctly() {
        let err = SecurityError::unknown_syscall("invalid_syscall");
        assert_eq!(
            err.to_string(),
            "unknown syscall 'invalid_syscall' referenced by seccomp profile"
        );
    }

    #[test]
    fn invalid_profile_displays_correctly() {
        let err = SecurityError::invalid_profile("missing deny field");
        assert_eq!(
            err.to_string(),
            "invalid seccomp profile: missing deny field"
        );
    }

    #[test]
    fn file_read_error_displays_correctly() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = SecurityError::file_read("/path/to/profile.json", io_err);
        assert!(err.to_string().contains("/path/to/profile.json"));
        assert!(err.to_string().contains("file not found"));
    }

    #[test]
    fn apply_failed_displays_correctly() {
        let err = SecurityError::apply_failed("no_new_privs failed");
        assert_eq!(
            err.to_string(),
            "failed to apply seccomp filter: no_new_privs failed"
        );
    }

    #[test]
    fn empty_capability_set_displays_correctly() {
        let err = SecurityError::empty_capability_set("container spec");
        assert_eq!(err.to_string(), "empty capability set: container spec");
    }

    #[test]
    fn invalid_action_displays_correctly() {
        let err = SecurityError::invalid_action("INVALID_ACTION");
        assert_eq!(err.to_string(), "invalid seccomp action: 'INVALID_ACTION'");
    }

    #[test]
    fn error_source_for_file_read() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = SecurityError::file_read("/path", io_err);
        assert!(err.source().is_some());
    }

    #[test]
    fn error_source_for_other_variants() {
        let err = SecurityError::unknown_capability("CAP_TEST");
        assert!(err.source().is_none());

        let err = SecurityError::unknown_syscall("test_syscall");
        assert!(err.source().is_none());

        let err = SecurityError::invalid_profile("test");
        assert!(err.source().is_none());
    }

    #[test]
    fn debug_format_works() {
        let err = SecurityError::unknown_capability("CAP_TEST");
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("UnknownCapability"));
        assert!(debug_str.contains("CAP_TEST"));
    }
}
