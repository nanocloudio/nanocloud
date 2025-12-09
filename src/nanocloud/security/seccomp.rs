// Allow dead_code: This module provides a public API for seccomp profile
// handling that is tested but not yet used by the main binary. The functions
// will be consumed by container runtime code when it integrates security helpers.
#![allow(dead_code)]

//! Seccomp (Secure Computing Mode) profile parsing and application.
//!
//! This module provides types and functions for parsing seccomp profiles from
//! JSON format and applying them to the current process using Linux's seccomp
//! BPF (Berkeley Packet Filter) mechanism.
//!
//! # Profile Format
//!
//! Seccomp profiles are JSON objects with the following structure:
//!
//! ```json
//! {
//!     "deny": ["syscall1", "syscall2", ...],
//!     "defaultAction": "SCMP_ACT_ALLOW"  // optional
//! }
//! ```
//!
//! - **`deny`** (required): Array of syscall names that should be blocked.
//!   Blocked syscalls will return `EPERM` to the caller.
//! - **`defaultAction`** (optional): The action for syscalls not in the deny
//!   list. If specified, must be a valid seccomp action (see
//!   [`SECCOMP_ACTIONS`]). Defaults to `SCMP_ACT_ALLOW`.
//!
//! # Default Behavior
//!
//! - **Default Action**: `SCMP_ACT_ALLOW` - all syscalls not in the deny list
//!   are allowed. Can be overridden via `defaultAction` in the profile.
//! - **Deny Action**: `SCMP_ACT_ERRNO` with `EPERM` - denied syscalls fail
//!   with "Operation not permitted".
//! - **Empty Profile**: A profile with an empty deny list is valid and performs
//!   no filtering.
//!
//! # Supported Syscalls
//!
//! The following syscalls can be denied (case-insensitive, whitespace-trimmed):
//!
//! | Syscall | Description |
//! |---------|-------------|
//! | `add_key` | Add a key to the kernel keyring |
//! | `bpf` | BPF system operations |
//! | `delete_module` | Unload a kernel module |
//! | `finit_module` | Load a kernel module from file descriptor |
//! | `init_module` | Load a kernel module |
//! | `kexec_load` | Load a new kernel for later execution |
//! | `keyctl` | Kernel keyring manipulation |
//! | `mount` | Mount a filesystem |
//! | `move_pages` | Move process pages to another node |
//! | `open_by_handle_at` | Open file via handle |
//! | `perf_event_open` | Performance monitoring |
//! | `pivot_root` | Change the root filesystem |
//! | `process_vm_readv` | Read from another process's memory |
//! | `process_vm_writev` | Write to another process's memory |
//! | `ptrace` | Process tracing |
//! | `reboot` | Reboot the system |
//! | `request_key` | Request a key from the kernel |
//! | `setns` | Reassociate thread with a namespace |
//! | `swapoff` | Disable swap |
//! | `swapon` | Enable swap |
//! | `syslog` | Read/clear kernel message buffer |
//! | `umount2` | Unmount a filesystem |
//! | `unshare` | Disassociate parts of process execution context |
//!
//! # Serialization Format
//!
//! Profiles are serialized as JSON. Example minimal profile:
//!
//! ```json
//! {"deny": []}
//! ```
//!
//! Example profile blocking dangerous syscalls:
//!
//! ```json
//! {
//!     "deny": [
//!         "ptrace",
//!         "mount",
//!         "reboot",
//!         "kexec_load"
//!     ]
//! }
//! ```
//!
//! # Error Handling
//!
//! - **Unknown Syscall**: Returns an error including the unknown syscall name.
//! - **Invalid JSON**: Returns a parse error with details.
//! - **File Read Errors**: Returns the path and underlying I/O error.
//! - **Application Errors**: Returns details about the failed `prctl` call.
//!
//! # Security Considerations
//!
//! - The `PR_SET_NO_NEW_PRIVS` flag is set before applying the filter, preventing
//!   privilege escalation through execve.
//! - Once applied, seccomp filters cannot be removed (only made more restrictive).
//! - Filters are inherited by child processes.
//!
//! # Examples
//!
//! ```ignore
//! use nanocloud::nanocloud::security::seccomp::SeccompFilter;
//! use std::str::FromStr;
//!
//! // Parse from string
//! let filter = SeccompFilter::from_str(r#"{"deny": ["ptrace", "mount"]}"#)?;
//!
//! // Parse from file
//! let filter = SeccompFilter::from_path(Path::new("/etc/nanocloud/seccomp.json"))?;
//!
//! // Apply to current process
//! filter.apply()?;
//! ```

use crate::nanocloud::logger::log_debug;
use crate::nanocloud::util::error::{new_error, with_context};
use super::error::SecurityError;
use libc::{sock_filter, sock_fprog};
use serde::Deserialize;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::str::FromStr;

type DynError = Box<dyn Error + Send + Sync>;

const COMPONENT: &str = "security";

// =============================================================================
// Seccomp Action Constants
// =============================================================================

/// Supported seccomp actions.
///
/// These actions determine what happens when a syscall matches a filter rule.
/// Actions are case-insensitive during normalization.
pub const SECCOMP_ACTIONS: &[&str] = &[
    "SCMP_ACT_ALLOW",
    "SCMP_ACT_ERRNO",
    "SCMP_ACT_KILL",
    "SCMP_ACT_KILL_PROCESS",
    "SCMP_ACT_KILL_THREAD",
    "SCMP_ACT_LOG",
    "SCMP_ACT_NOTIFY",
    "SCMP_ACT_TRACE",
    "SCMP_ACT_TRAP",
];

/// Supported seccomp architectures.
///
/// These architectures define which CPU architecture the seccomp filter
/// applies to. Architectures are case-insensitive during normalization.
pub const SECCOMP_ARCHITECTURES: &[&str] = &[
    "SCMP_ARCH_AARCH64",
    "SCMP_ARCH_ARM",
    "SCMP_ARCH_MIPS",
    "SCMP_ARCH_MIPS64",
    "SCMP_ARCH_MIPS64N32",
    "SCMP_ARCH_MIPSEL",
    "SCMP_ARCH_MIPSEL64",
    "SCMP_ARCH_MIPSEL64N32",
    "SCMP_ARCH_NATIVE",
    "SCMP_ARCH_PPC",
    "SCMP_ARCH_PPC64",
    "SCMP_ARCH_PPC64LE",
    "SCMP_ARCH_RISCV64",
    "SCMP_ARCH_S390",
    "SCMP_ARCH_S390X",
    "SCMP_ARCH_X32",
    "SCMP_ARCH_X86",
    "SCMP_ARCH_X86_64",
];

// =============================================================================
// Action Normalization
// =============================================================================

/// Normalizes a seccomp action string to canonical form.
///
/// # Normalization Rules
///
/// 1. Leading and trailing whitespace is trimmed
/// 2. All characters are converted to uppercase
/// 3. The `SCMP_ACT_` prefix is added if not already present
/// 4. Common aliases are resolved (e.g., `kill` → `SCMP_ACT_KILL`)
///
/// # Returns
///
/// The normalized action string in `SCMP_ACT_*` format, or an empty string
/// if the input is empty/whitespace-only.
///
/// # Examples
///
/// ```
/// use nanocloud::nanocloud::security::seccomp::normalize_action;
///
/// assert_eq!(normalize_action("allow"), "SCMP_ACT_ALLOW");
/// assert_eq!(normalize_action("SCMP_ACT_ERRNO"), "SCMP_ACT_ERRNO");
/// assert_eq!(normalize_action("kill"), "SCMP_ACT_KILL");
/// ```
pub fn normalize_action(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let upper = trimmed.to_ascii_uppercase();

    // Handle common aliases
    let resolved = match upper.as_str() {
        "ALLOW" => "SCMP_ACT_ALLOW",
        "ERRNO" => "SCMP_ACT_ERRNO",
        "KILL" => "SCMP_ACT_KILL",
        "KILL_PROCESS" => "SCMP_ACT_KILL_PROCESS",
        "KILL_THREAD" => "SCMP_ACT_KILL_THREAD",
        "LOG" => "SCMP_ACT_LOG",
        "NOTIFY" => "SCMP_ACT_NOTIFY",
        "TRACE" => "SCMP_ACT_TRACE",
        "TRAP" => "SCMP_ACT_TRAP",
        other if other.starts_with("SCMP_ACT_") => other,
        other => return format!("SCMP_ACT_{}", other),
    };

    resolved.to_string()
}

/// Validates a seccomp action and returns the normalized form.
///
/// # Arguments
///
/// * `action` - The action string to validate (any case/format)
///
/// # Returns
///
/// - `Ok(String)` with the normalized action if valid
/// - `Err(SecurityError::InvalidSeccompAction)` if the action is not recognized
///
/// # Examples
///
/// ```
/// use nanocloud::nanocloud::security::seccomp::validate_action;
///
/// assert_eq!(validate_action("allow").unwrap(), "SCMP_ACT_ALLOW");
/// assert!(validate_action("invalid").is_err());
/// ```
pub fn validate_action(action: &str) -> Result<String, SecurityError> {
    let normalized = normalize_action(action);
    if normalized.is_empty() {
        log_debug(
            COMPONENT,
            "seccomp action validation failed: empty input",
            &[("input", action)],
        );
        return Err(SecurityError::invalid_action(action));
    }
    if SECCOMP_ACTIONS.contains(&normalized.as_str()) {
        Ok(normalized)
    } else {
        log_debug(
            COMPONENT,
            "seccomp action validation failed: unknown action",
            &[("input", action), ("normalized", &normalized)],
        );
        Err(SecurityError::invalid_action(action))
    }
}

/// Validates a seccomp default action and returns a result.
///
/// This function is used internally by profile parsing to validate
/// the `defaultAction` field when present.
///
/// # Arguments
///
/// * `action` - The default action string to validate (any case/format)
///
/// # Returns
///
/// - `Ok(())` if the action is valid
/// - `Err` with a descriptive error if the action is not recognized
///
/// # Examples
///
/// ```
/// use nanocloud::nanocloud::security::seccomp::validate_default_action;
///
/// assert!(validate_default_action("SCMP_ACT_ALLOW").is_ok());
/// assert!(validate_default_action("allow").is_ok());
/// assert!(validate_default_action("invalid").is_err());
/// ```
pub fn validate_default_action(action: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let normalized = normalize_action(action);
    if normalized.is_empty() {
        log_debug(
            COMPONENT,
            "seccomp default action validation failed: empty input",
            &[("input", action)],
        );
        return Err(new_error(format!(
            "Invalid seccomp defaultAction: empty or whitespace-only value"
        )));
    }
    if SECCOMP_ACTIONS.contains(&normalized.as_str()) {
        Ok(())
    } else {
        log_debug(
            COMPONENT,
            "seccomp default action validation failed: unknown action",
            &[("input", action), ("normalized", &normalized)],
        );
        Err(new_error(format!(
            "Invalid seccomp defaultAction '{}': must be one of {}",
            action,
            SECCOMP_ACTIONS.join(", ")
        )))
    }
}

// =============================================================================
// Architecture Normalization
// =============================================================================

/// Normalizes a seccomp architecture string to canonical form.
///
/// # Normalization Rules
///
/// 1. Leading and trailing whitespace is trimmed
/// 2. All characters are converted to uppercase
/// 3. The `SCMP_ARCH_` prefix is added if not already present
/// 4. Common aliases are resolved (e.g., `x64` → `SCMP_ARCH_X86_64`)
///
/// # Returns
///
/// The normalized architecture string in `SCMP_ARCH_*` format, or an empty
/// string if the input is empty/whitespace-only.
///
/// # Examples
///
/// ```
/// use nanocloud::nanocloud::security::seccomp::normalize_architecture;
///
/// assert_eq!(normalize_architecture("x86_64"), "SCMP_ARCH_X86_64");
/// assert_eq!(normalize_architecture("aarch64"), "SCMP_ARCH_AARCH64");
/// assert_eq!(normalize_architecture("SCMP_ARCH_ARM"), "SCMP_ARCH_ARM");
/// ```
pub fn normalize_architecture(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let upper = trimmed.to_ascii_uppercase();

    // Handle common aliases
    let resolved = match upper.as_str() {
        // x86 family aliases
        "X64" | "AMD64" => "SCMP_ARCH_X86_64",
        "X86" | "I386" | "I686" => "SCMP_ARCH_X86",
        "X86_64" => "SCMP_ARCH_X86_64",
        "X32" => "SCMP_ARCH_X32",
        // ARM family aliases
        "ARM" | "ARM32" | "ARMV7" | "ARMV7L" => "SCMP_ARCH_ARM",
        "AARCH64" | "ARM64" | "ARMV8" => "SCMP_ARCH_AARCH64",
        // PowerPC aliases
        "PPC" | "POWERPC" => "SCMP_ARCH_PPC",
        "PPC64" | "POWERPC64" => "SCMP_ARCH_PPC64",
        "PPC64LE" | "POWERPC64LE" => "SCMP_ARCH_PPC64LE",
        // s390 aliases
        "S390" => "SCMP_ARCH_S390",
        "S390X" => "SCMP_ARCH_S390X",
        // RISC-V aliases
        "RISCV64" | "RISCV" => "SCMP_ARCH_RISCV64",
        // MIPS aliases
        "MIPS" => "SCMP_ARCH_MIPS",
        "MIPS64" => "SCMP_ARCH_MIPS64",
        "MIPSEL" | "MIPSLE" => "SCMP_ARCH_MIPSEL",
        "MIPSEL64" | "MIPS64LE" => "SCMP_ARCH_MIPSEL64",
        // Native
        "NATIVE" => "SCMP_ARCH_NATIVE",
        // Already prefixed
        other if other.starts_with("SCMP_ARCH_") => other,
        other => return format!("SCMP_ARCH_{}", other),
    };

    resolved.to_string()
}

/// Validates a seccomp architecture and returns the normalized form.
///
/// # Arguments
///
/// * `arch` - The architecture string to validate (any case/format)
///
/// # Returns
///
/// - `Ok(String)` with the normalized architecture if valid
/// - `Err(SecurityError::InvalidSeccompAction)` if the architecture is not recognized
///
/// # Examples
///
/// ```
/// use nanocloud::nanocloud::security::seccomp::validate_architecture;
///
/// assert_eq!(validate_architecture("x86_64").unwrap(), "SCMP_ARCH_X86_64");
/// assert!(validate_architecture("invalid_arch").is_err());
/// ```
pub fn validate_architecture(arch: &str) -> Result<String, SecurityError> {
    let normalized = normalize_architecture(arch);
    if normalized.is_empty() {
        log_debug(
            COMPONENT,
            "seccomp architecture validation failed: empty input",
            &[("input", arch)],
        );
        return Err(SecurityError::invalid_action(format!(
            "invalid architecture: {}",
            arch
        )));
    }
    if SECCOMP_ARCHITECTURES.contains(&normalized.as_str()) {
        Ok(normalized)
    } else {
        log_debug(
            COMPONENT,
            "seccomp architecture validation failed: unknown architecture",
            &[("input", arch), ("normalized", &normalized)],
        );
        Err(SecurityError::invalid_action(format!(
            "unknown architecture: {}",
            arch
        )))
    }
}

/// Checks if a syscall name is valid and recognized.
///
/// # Arguments
///
/// * `name` - The syscall name to check (case-insensitive)
///
/// # Returns
///
/// `true` if the syscall is recognized, `false` otherwise.
pub fn is_valid_syscall(name: &str) -> bool {
    let normalized = name.trim().to_lowercase();
    syscall_number(&normalized).is_some()
}

/// Validates a syscall name and returns the normalized (lowercase) form.
///
/// # Arguments
///
/// * `name` - The syscall name to validate
///
/// # Returns
///
/// - `Ok(String)` with the normalized syscall name if valid
/// - `Err(SecurityError::UnknownSyscall)` if the syscall is not recognized
pub fn validate_syscall(name: &str) -> Result<String, SecurityError> {
    let normalized = name.trim().to_lowercase();
    if normalized.is_empty() {
        log_debug(
            COMPONENT,
            "seccomp syscall validation failed: empty input",
            &[("input", name)],
        );
        return Err(SecurityError::unknown_syscall(name));
    }
    if syscall_number(&normalized).is_some() {
        Ok(normalized)
    } else {
        log_debug(
            COMPONENT,
            "seccomp syscall validation failed: unknown syscall",
            &[("input", name), ("normalized", &normalized)],
        );
        Err(SecurityError::unknown_syscall(name))
    }
}

/// A parsed seccomp filter ready for application.
///
/// This struct holds the compiled list of syscall numbers to deny. It can be
/// created from a JSON profile string or file, and applied to the current
/// process.
///
/// # Thread Safety
///
/// `SeccompFilter` is `Clone` and `Send + Sync`, allowing it to be shared
/// across threads. However, applying a filter affects the entire process.
#[derive(Debug, Clone)]
pub struct SeccompFilter {
    denied_syscalls: Vec<i64>,
}

/// Default action for seccomp profiles when not explicitly specified.
pub const DEFAULT_SECCOMP_ACTION: &str = "SCMP_ACT_ALLOW";

/// Internal deserialization structure for seccomp profiles.
#[derive(Deserialize)]
struct SeccompProfileData {
    deny: Vec<String>,
    /// Optional default action. When present, must be a valid seccomp action.
    /// When absent, defaults to `SCMP_ACT_ALLOW`.
    #[serde(rename = "defaultAction")]
    default_action: Option<String>,
}

impl SeccompFilter {
    /// Loads a seccomp profile from a file path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to a JSON file containing the seccomp profile
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file cannot be read
    /// - The file contains invalid JSON
    /// - The profile references unknown syscalls
    pub fn from_path(path: &Path) -> Result<Self, DynError> {
        let contents = fs::read_to_string(path)
            .map_err(|err| with_context(err, format!("Failed to read {}", path.display())))?;
        Self::from_str(&contents)
    }

    /// Parses a seccomp profile from a JSON string.
    ///
    /// Syscall names are normalized (trimmed, lowercased) before lookup.
    /// If `defaultAction` is specified, it must be a valid seccomp action.
    fn parse(raw: &str) -> Result<Self, DynError> {
        let data: SeccompProfileData = serde_json::from_str(raw)
            .map_err(|err| new_error(format!("Invalid seccomp profile: {}", err)))?;

        // Validate default action if provided
        if let Some(ref action) = data.default_action {
            validate_default_action(action)?;
        }

        let mut denied = Vec::with_capacity(data.deny.len());
        for entry in data.deny {
            let normalized = entry.trim().to_lowercase();
            let number = syscall_number(&normalized).ok_or_else(|| {
                new_error(format!(
                    "Unknown syscall '{}' referenced by seccomp profile",
                    normalized
                ))
            })?;
            denied.push(number);
        }
        Ok(Self {
            denied_syscalls: denied,
        })
    }

    /// Applies the seccomp filter to the current process.
    ///
    /// This method:
    /// 1. Sets `PR_SET_NO_NEW_PRIVS` to prevent privilege escalation
    /// 2. Installs a BPF filter that denies the specified syscalls
    ///
    /// # Behavior
    ///
    /// - Denied syscalls will return `EPERM` to the caller
    /// - All other syscalls are allowed
    /// - An empty deny list results in no filtering (returns immediately)
    ///
    /// # Safety
    ///
    /// This method uses `unsafe` to call `libc::prctl`. The filter cannot be
    /// removed once applied and affects all threads in the process.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `PR_SET_NO_NEW_PRIVS` fails
    /// - `PR_SET_SECCOMP` fails
    pub fn apply(&self) -> Result<(), DynError> {
        if self.denied_syscalls.is_empty() {
            return Ok(());
        }

        const BPF_LD: u16 = 0x00;
        const BPF_W: u16 = 0x00;
        const BPF_ABS: u16 = 0x20;
        const BPF_JMP: u16 = 0x05;
        const BPF_JEQ: u16 = 0x10;
        const BPF_K: u16 = 0x00;
        const BPF_RET: u16 = 0x06;

        const LD_SYSCALL_NR: u16 = BPF_LD | BPF_W | BPF_ABS;
        const JMP_EQ: u16 = BPF_JMP | BPF_JEQ | BPF_K;
        const RET: u16 = BPF_RET | BPF_K;
        const ERR_ACTION: u32 = libc::SECCOMP_RET_ERRNO | ((libc::EPERM as u32) & 0xFFFF);
        const ALLOW_ACTION: u32 = libc::SECCOMP_RET_ALLOW;

        let mut filters = Vec::with_capacity(self.denied_syscalls.len() * 2 + 2);
        filters.push(bpf_stmt(LD_SYSCALL_NR, 0));
        for sysno in &self.denied_syscalls {
            filters.push(bpf_jump(JMP_EQ, *sysno as u32, 0, 1));
            filters.push(bpf_stmt(RET, ERR_ACTION));
        }
        filters.push(bpf_stmt(RET, ALLOW_ACTION));

        let mut prog = sock_fprog {
            len: filters.len() as u16,
            filter: filters.as_mut_ptr(),
        };

        unsafe {
            if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
                return Err(new_error("Failed to enable no_new_privs before seccomp"));
            }
            if libc::prctl(
                libc::PR_SET_SECCOMP,
                libc::SECCOMP_MODE_FILTER,
                &mut prog as *mut _,
            ) != 0
            {
                return Err(new_error("Failed to install seccomp filter"));
            }
        }

        Ok(())
    }
}

impl FromStr for SeccompFilter {
    type Err = DynError;

    fn from_str(raw: &str) -> Result<Self, DynError> {
        Self::parse(raw)
    }
}

/// Creates a BPF statement (instruction with no jumps).
fn bpf_stmt(code: u16, k: u32) -> sock_filter {
    sock_filter {
        code,
        jt: 0,
        jf: 0,
        k,
    }
}

/// Creates a BPF jump instruction with true/false jump targets.
fn bpf_jump(code: u16, k: u32, jt: u8, jf: u8) -> sock_filter {
    sock_filter { code, jt, jf, k }
}

/// Maps a syscall name to its numeric identifier.
///
/// Returns `None` for unknown syscalls. Names must be lowercase.
fn syscall_number(name: &str) -> Option<i64> {
    match name {
        "add_key" => Some(libc::SYS_add_key),
        "bpf" => Some(libc::SYS_bpf),
        "delete_module" => Some(libc::SYS_delete_module),
        "finit_module" => Some(libc::SYS_finit_module),
        "init_module" => Some(libc::SYS_init_module),
        "keyctl" => Some(libc::SYS_keyctl),
        "kexec_load" => Some(libc::SYS_kexec_load),
        "move_pages" => Some(libc::SYS_move_pages),
        "open_by_handle_at" => Some(libc::SYS_open_by_handle_at),
        "perf_event_open" => Some(libc::SYS_perf_event_open),
        "pivot_root" => Some(libc::SYS_pivot_root),
        "process_vm_readv" => Some(libc::SYS_process_vm_readv),
        "process_vm_writev" => Some(libc::SYS_process_vm_writev),
        "ptrace" => Some(libc::SYS_ptrace),
        "reboot" => Some(libc::SYS_reboot),
        "request_key" => Some(libc::SYS_request_key),
        "setns" => Some(libc::SYS_setns),
        "swapon" => Some(libc::SYS_swapon),
        "swapoff" => Some(libc::SYS_swapoff),
        "syslog" => Some(libc::SYS_syslog),
        "umount2" => Some(libc::SYS_umount2),
        "unshare" => Some(libc::SYS_unshare),
        "mount" => Some(libc::SYS_mount),
        _ => None,
    }
}
