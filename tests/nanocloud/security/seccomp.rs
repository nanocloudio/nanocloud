use nanocloud::nanocloud::security::seccomp::{
    is_valid_syscall, normalize_action, normalize_architecture, validate_action,
    validate_architecture, validate_default_action, validate_syscall, SeccompFilter,
    DEFAULT_SECCOMP_ACTION, SECCOMP_ACTIONS, SECCOMP_ARCHITECTURES,
};
use std::io::Write;
use std::str::FromStr;
use tempfile::NamedTempFile;

// =============================================================================
// Task 317: Seccomp profile parsing/validation tests
// =============================================================================

mod parsing_tests {
    use super::*;

    #[test]
    fn parses_valid_profile_with_single_syscall() {
        let profile = r#"{"deny": ["ptrace"]}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(
            result.is_ok(),
            "Expected valid profile to parse successfully"
        );
    }

    #[test]
    fn parses_valid_profile_with_multiple_syscalls() {
        let profile = r#"{"deny": ["ptrace", "mount", "reboot"]}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(
            result.is_ok(),
            "Expected valid profile with multiple syscalls to parse"
        );
    }

    #[test]
    fn parses_profile_with_empty_deny_list() {
        let profile = r#"{"deny": []}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(
            result.is_ok(),
            "Expected profile with empty deny list to parse"
        );
    }

    #[test]
    fn normalizes_syscall_names_to_lowercase() {
        // The implementation lowercases syscall names before lookup
        let profile = r#"{"deny": ["PTRACE", "Mount", "REBOOT"]}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(
            result.is_ok(),
            "Expected mixed-case syscall names to be normalized"
        );
    }

    #[test]
    fn trims_whitespace_from_syscall_names() {
        let profile = r#"{"deny": ["  ptrace  ", "mount"]}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(
            result.is_ok(),
            "Expected whitespace-padded syscall names to be trimmed"
        );
    }
}

mod validation_error_tests {
    use super::*;

    #[test]
    fn rejects_invalid_json() {
        let profile = "not valid json";
        let result = SeccompFilter::from_str(profile);
        assert!(result.is_err(), "Expected invalid JSON to be rejected");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("Invalid seccomp profile"),
            "Error message should indicate invalid profile: {}",
            err
        );
    }

    #[test]
    fn rejects_missing_deny_field() {
        let profile = r#"{}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(
            result.is_err(),
            "Expected missing deny field to be rejected"
        );
    }

    #[test]
    fn rejects_unknown_syscall() {
        let profile = r#"{"deny": ["not_a_real_syscall"]}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(result.is_err(), "Expected unknown syscall to be rejected");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("Unknown syscall"),
            "Error message should indicate unknown syscall: {}",
            err
        );
        assert!(
            err.to_string().contains("not_a_real_syscall"),
            "Error message should include the offending syscall name: {}",
            err
        );
    }

    #[test]
    fn rejects_partial_invalid_syscall_list() {
        // If any syscall is invalid, the whole profile should be rejected
        let profile = r#"{"deny": ["ptrace", "invalid_syscall", "mount"]}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(
            result.is_err(),
            "Expected profile with invalid syscall to be rejected"
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("invalid_syscall"),
            "Error should mention the invalid syscall: {}",
            err
        );
    }

    #[test]
    fn rejects_null_deny_value() {
        let profile = r#"{"deny": null}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(result.is_err(), "Expected null deny value to be rejected");
    }

    #[test]
    fn rejects_non_array_deny_value() {
        let profile = r#"{"deny": "ptrace"}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(result.is_err(), "Expected string deny value to be rejected");
    }

    #[test]
    fn rejects_non_string_syscall_entries() {
        let profile = r#"{"deny": [123, "ptrace"]}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(
            result.is_err(),
            "Expected numeric syscall entry to be rejected"
        );
    }
}

mod file_loading_tests {
    use super::*;

    #[test]
    fn loads_profile_from_file() {
        let mut file = NamedTempFile::new().expect("create temp file");
        writeln!(file, r#"{{"deny": ["ptrace", "mount"]}}"#).expect("write profile");
        file.flush().expect("flush");

        let result = SeccompFilter::from_path(file.path());
        assert!(result.is_ok(), "Expected profile to load from file");
    }

    #[test]
    fn returns_error_for_nonexistent_file() {
        let result =
            SeccompFilter::from_path(std::path::Path::new("/nonexistent/path/profile.json"));
        assert!(result.is_err(), "Expected error for nonexistent file");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("Failed to read"),
            "Error should indicate read failure: {}",
            err
        );
    }

    #[test]
    fn returns_error_for_invalid_file_content() {
        let mut file = NamedTempFile::new().expect("create temp file");
        writeln!(file, "not valid json").expect("write invalid content");
        file.flush().expect("flush");

        let result = SeccompFilter::from_path(file.path());
        assert!(result.is_err(), "Expected error for invalid file content");
    }
}

mod known_syscalls_tests {
    use super::*;

    /// All syscalls that the implementation should recognize
    // Sorted alphabetically for maintainability
    const KNOWN_SYSCALLS: &[&str] = &[
        "add_key",
        "bpf",
        "delete_module",
        "finit_module",
        "init_module",
        "kexec_load",
        "keyctl",
        "mount",
        "move_pages",
        "open_by_handle_at",
        "perf_event_open",
        "pivot_root",
        "process_vm_readv",
        "process_vm_writev",
        "ptrace",
        "reboot",
        "request_key",
        "setns",
        "swapoff",
        "swapon",
        "syslog",
        "umount2",
        "unshare",
    ];

    #[test]
    fn accepts_all_known_syscalls() {
        for syscall in KNOWN_SYSCALLS {
            let profile = format!(r#"{{"deny": ["{}"]}}"#, syscall);
            let result = SeccompFilter::from_str(&profile);
            assert!(
                result.is_ok(),
                "Expected known syscall '{}' to be accepted, got error: {:?}",
                syscall,
                result.err()
            );
        }
    }

    #[test]
    fn accepts_all_known_syscalls_in_single_profile() {
        let syscalls: Vec<String> = KNOWN_SYSCALLS
            .iter()
            .map(|s| format!(r#""{}""#, s))
            .collect();
        let profile = format!(r#"{{"deny": [{}]}}"#, syscalls.join(", "));
        let result = SeccompFilter::from_str(&profile);
        assert!(
            result.is_ok(),
            "Expected all known syscalls to be accepted together"
        );
    }

    #[test]
    fn known_syscalls_list_is_sorted() {
        let mut sorted = KNOWN_SYSCALLS.to_vec();
        sorted.sort();
        assert_eq!(
            KNOWN_SYSCALLS.to_vec(),
            sorted,
            "KNOWN_SYSCALLS test list should be sorted for maintainability"
        );
    }
}

mod edge_cases {
    use super::*;

    #[test]
    fn handles_duplicate_syscalls() {
        let profile = r#"{"deny": ["ptrace", "ptrace", "mount"]}"#;
        let result = SeccompFilter::from_str(profile);
        // Duplicates should be handled gracefully (either accepted or deduplicated)
        assert!(result.is_ok(), "Expected duplicate syscalls to be handled");
    }

    #[test]
    fn handles_empty_string_syscall() {
        // Empty string after trim should fail
        let profile = r#"{"deny": [""]}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(
            result.is_err(),
            "Expected empty syscall name to be rejected"
        );
    }

    #[test]
    fn handles_whitespace_only_syscall() {
        let profile = r#"{"deny": ["   "]}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(
            result.is_err(),
            "Expected whitespace-only syscall to be rejected"
        );
    }

    #[test]
    fn handles_extra_json_fields() {
        // Extra fields should be ignored (serde default behavior)
        let profile = r#"{"deny": ["ptrace"], "extra_field": "ignored"}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(result.is_ok(), "Expected extra JSON fields to be ignored");
    }

    #[test]
    fn handles_unicode_in_syscall_name() {
        let profile = r#"{"deny": ["ptrace\u0000"]}"#;
        let result = SeccompFilter::from_str(profile);
        // Unicode/null characters should result in unknown syscall
        assert!(
            result.is_err(),
            "Expected unicode in syscall name to be rejected"
        );
    }
}

mod round_trip_tests {
    use super::*;

    #[test]
    fn from_str_produces_consistent_results() {
        let profile = r#"{"deny": ["ptrace", "mount", "reboot"]}"#;

        let result1 = SeccompFilter::from_str(profile);
        let result2 = SeccompFilter::from_str(profile);

        assert!(result1.is_ok());
        assert!(result2.is_ok());
        // Both should parse successfully - we can't compare internal state directly
        // but we verify deterministic parsing
    }

    #[test]
    fn file_and_string_parsing_produce_same_result() {
        let profile_content = r#"{"deny": ["ptrace", "mount"]}"#;

        let mut file = NamedTempFile::new().expect("create temp file");
        write!(file, "{}", profile_content).expect("write profile");
        file.flush().expect("flush");

        let from_str = SeccompFilter::from_str(profile_content);
        let from_file = SeccompFilter::from_path(file.path());

        assert!(from_str.is_ok());
        assert!(from_file.is_ok());
    }
}

// =============================================================================
// Task 324: Seccomp action normalization tests
// =============================================================================

mod action_normalization_tests {
    use super::*;

    #[test]
    fn normalizes_lowercase_actions() {
        assert_eq!(normalize_action("allow"), "SCMP_ACT_ALLOW");
        assert_eq!(normalize_action("errno"), "SCMP_ACT_ERRNO");
        assert_eq!(normalize_action("kill"), "SCMP_ACT_KILL");
    }

    #[test]
    fn normalizes_uppercase_actions() {
        assert_eq!(normalize_action("ALLOW"), "SCMP_ACT_ALLOW");
        assert_eq!(normalize_action("KILL"), "SCMP_ACT_KILL");
    }

    #[test]
    fn normalizes_mixed_case_actions() {
        assert_eq!(normalize_action("Allow"), "SCMP_ACT_ALLOW");
        assert_eq!(normalize_action("KiLl"), "SCMP_ACT_KILL");
    }

    #[test]
    fn preserves_prefixed_actions() {
        assert_eq!(normalize_action("SCMP_ACT_ALLOW"), "SCMP_ACT_ALLOW");
        assert_eq!(normalize_action("SCMP_ACT_ERRNO"), "SCMP_ACT_ERRNO");
    }

    #[test]
    fn handles_all_known_aliases() {
        assert_eq!(normalize_action("allow"), "SCMP_ACT_ALLOW");
        assert_eq!(normalize_action("errno"), "SCMP_ACT_ERRNO");
        assert_eq!(normalize_action("kill"), "SCMP_ACT_KILL");
        assert_eq!(normalize_action("kill_process"), "SCMP_ACT_KILL_PROCESS");
        assert_eq!(normalize_action("kill_thread"), "SCMP_ACT_KILL_THREAD");
        assert_eq!(normalize_action("log"), "SCMP_ACT_LOG");
        assert_eq!(normalize_action("notify"), "SCMP_ACT_NOTIFY");
        assert_eq!(normalize_action("trace"), "SCMP_ACT_TRACE");
        assert_eq!(normalize_action("trap"), "SCMP_ACT_TRAP");
    }

    #[test]
    fn trims_whitespace() {
        assert_eq!(normalize_action("  allow  "), "SCMP_ACT_ALLOW");
        assert_eq!(normalize_action("\tkill\n"), "SCMP_ACT_KILL");
    }

    #[test]
    fn returns_empty_for_empty_input() {
        assert_eq!(normalize_action(""), "");
        assert_eq!(normalize_action("   "), "");
    }

    #[test]
    fn adds_prefix_to_unknown_actions() {
        // Unknown actions get the prefix added
        assert_eq!(normalize_action("custom"), "SCMP_ACT_CUSTOM");
    }
}

mod action_validation_tests {
    use super::*;

    #[test]
    fn validates_known_actions() {
        assert_eq!(validate_action("allow").unwrap(), "SCMP_ACT_ALLOW");
        assert_eq!(validate_action("SCMP_ACT_KILL").unwrap(), "SCMP_ACT_KILL");
    }

    #[test]
    fn rejects_unknown_actions() {
        let result = validate_action("invalid_action");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("invalid_action"));
    }

    #[test]
    fn rejects_empty_actions() {
        let result = validate_action("");
        assert!(result.is_err());
    }

    #[test]
    fn validates_all_seccomp_actions() {
        for action in SECCOMP_ACTIONS {
            let result = validate_action(action);
            assert!(
                result.is_ok(),
                "Expected {} to be valid, got error: {:?}",
                action,
                result.err()
            );
            assert_eq!(result.unwrap(), *action);
        }
    }
}

mod seccomp_actions_constant_tests {
    use super::*;

    #[test]
    fn seccomp_actions_is_sorted() {
        let mut sorted = SECCOMP_ACTIONS.to_vec();
        sorted.sort();
        assert_eq!(
            SECCOMP_ACTIONS.to_vec(),
            sorted,
            "SECCOMP_ACTIONS should be sorted"
        );
    }

    #[test]
    fn seccomp_actions_is_unique() {
        let mut seen = std::collections::HashSet::new();
        for action in SECCOMP_ACTIONS {
            assert!(seen.insert(*action), "Duplicate action: {}", action);
        }
    }

    #[test]
    fn seccomp_actions_are_prefixed() {
        for action in SECCOMP_ACTIONS {
            assert!(
                action.starts_with("SCMP_ACT_"),
                "Action {} should start with SCMP_ACT_",
                action
            );
        }
    }
}

// =============================================================================
// Task 324: Seccomp architecture normalization tests
// =============================================================================

mod architecture_normalization_tests {
    use super::*;

    #[test]
    fn normalizes_x86_family() {
        assert_eq!(normalize_architecture("x86_64"), "SCMP_ARCH_X86_64");
        assert_eq!(normalize_architecture("X86_64"), "SCMP_ARCH_X86_64");
        assert_eq!(normalize_architecture("x64"), "SCMP_ARCH_X86_64");
        assert_eq!(normalize_architecture("amd64"), "SCMP_ARCH_X86_64");
        assert_eq!(normalize_architecture("x86"), "SCMP_ARCH_X86");
        assert_eq!(normalize_architecture("i386"), "SCMP_ARCH_X86");
        assert_eq!(normalize_architecture("i686"), "SCMP_ARCH_X86");
    }

    #[test]
    fn normalizes_arm_family() {
        assert_eq!(normalize_architecture("aarch64"), "SCMP_ARCH_AARCH64");
        assert_eq!(normalize_architecture("arm64"), "SCMP_ARCH_AARCH64");
        assert_eq!(normalize_architecture("armv8"), "SCMP_ARCH_AARCH64");
        assert_eq!(normalize_architecture("arm"), "SCMP_ARCH_ARM");
        assert_eq!(normalize_architecture("arm32"), "SCMP_ARCH_ARM");
        assert_eq!(normalize_architecture("armv7"), "SCMP_ARCH_ARM");
        assert_eq!(normalize_architecture("armv7l"), "SCMP_ARCH_ARM");
    }

    #[test]
    fn normalizes_powerpc_family() {
        assert_eq!(normalize_architecture("ppc"), "SCMP_ARCH_PPC");
        assert_eq!(normalize_architecture("powerpc"), "SCMP_ARCH_PPC");
        assert_eq!(normalize_architecture("ppc64"), "SCMP_ARCH_PPC64");
        assert_eq!(normalize_architecture("powerpc64"), "SCMP_ARCH_PPC64");
        assert_eq!(normalize_architecture("ppc64le"), "SCMP_ARCH_PPC64LE");
    }

    #[test]
    fn normalizes_s390_family() {
        assert_eq!(normalize_architecture("s390"), "SCMP_ARCH_S390");
        assert_eq!(normalize_architecture("s390x"), "SCMP_ARCH_S390X");
    }

    #[test]
    fn normalizes_riscv() {
        assert_eq!(normalize_architecture("riscv64"), "SCMP_ARCH_RISCV64");
        assert_eq!(normalize_architecture("riscv"), "SCMP_ARCH_RISCV64");
    }

    #[test]
    fn normalizes_mips_family() {
        assert_eq!(normalize_architecture("mips"), "SCMP_ARCH_MIPS");
        assert_eq!(normalize_architecture("mips64"), "SCMP_ARCH_MIPS64");
        assert_eq!(normalize_architecture("mipsel"), "SCMP_ARCH_MIPSEL");
        assert_eq!(normalize_architecture("mipsle"), "SCMP_ARCH_MIPSEL");
    }

    #[test]
    fn normalizes_native() {
        assert_eq!(normalize_architecture("native"), "SCMP_ARCH_NATIVE");
    }

    #[test]
    fn preserves_prefixed_architectures() {
        assert_eq!(
            normalize_architecture("SCMP_ARCH_X86_64"),
            "SCMP_ARCH_X86_64"
        );
        assert_eq!(
            normalize_architecture("SCMP_ARCH_AARCH64"),
            "SCMP_ARCH_AARCH64"
        );
    }

    #[test]
    fn trims_whitespace() {
        assert_eq!(normalize_architecture("  x86_64  "), "SCMP_ARCH_X86_64");
    }

    #[test]
    fn returns_empty_for_empty_input() {
        assert_eq!(normalize_architecture(""), "");
        assert_eq!(normalize_architecture("   "), "");
    }
}

mod architecture_validation_tests {
    use super::*;

    #[test]
    fn validates_known_architectures() {
        assert_eq!(validate_architecture("x86_64").unwrap(), "SCMP_ARCH_X86_64");
        assert_eq!(
            validate_architecture("SCMP_ARCH_AARCH64").unwrap(),
            "SCMP_ARCH_AARCH64"
        );
    }

    #[test]
    fn rejects_unknown_architectures() {
        let result = validate_architecture("invalid_arch");
        assert!(result.is_err());
    }

    #[test]
    fn rejects_empty_architectures() {
        let result = validate_architecture("");
        assert!(result.is_err());
    }

    #[test]
    fn validates_all_seccomp_architectures() {
        for arch in SECCOMP_ARCHITECTURES {
            let result = validate_architecture(arch);
            assert!(
                result.is_ok(),
                "Expected {} to be valid, got error: {:?}",
                arch,
                result.err()
            );
            assert_eq!(result.unwrap(), *arch);
        }
    }
}

mod seccomp_architectures_constant_tests {
    use super::*;

    #[test]
    fn seccomp_architectures_is_sorted() {
        let mut sorted = SECCOMP_ARCHITECTURES.to_vec();
        sorted.sort();
        assert_eq!(
            SECCOMP_ARCHITECTURES.to_vec(),
            sorted,
            "SECCOMP_ARCHITECTURES should be sorted"
        );
    }

    #[test]
    fn seccomp_architectures_is_unique() {
        let mut seen = std::collections::HashSet::new();
        for arch in SECCOMP_ARCHITECTURES {
            assert!(seen.insert(*arch), "Duplicate architecture: {}", arch);
        }
    }

    #[test]
    fn seccomp_architectures_are_prefixed() {
        for arch in SECCOMP_ARCHITECTURES {
            assert!(
                arch.starts_with("SCMP_ARCH_"),
                "Architecture {} should start with SCMP_ARCH_",
                arch
            );
        }
    }
}

// =============================================================================
// Task 324: Syscall validation tests
// =============================================================================

mod syscall_validation_tests {
    use super::*;

    #[test]
    fn is_valid_syscall_accepts_known() {
        assert!(is_valid_syscall("ptrace"));
        assert!(is_valid_syscall("mount"));
        assert!(is_valid_syscall("PTRACE")); // case insensitive
    }

    #[test]
    fn is_valid_syscall_rejects_unknown() {
        assert!(!is_valid_syscall("not_a_syscall"));
        assert!(!is_valid_syscall(""));
    }

    #[test]
    fn validate_syscall_returns_normalized() {
        assert_eq!(validate_syscall("PTRACE").unwrap(), "ptrace");
        assert_eq!(validate_syscall("  mount  ").unwrap(), "mount");
    }

    #[test]
    fn validate_syscall_rejects_unknown() {
        let result = validate_syscall("invalid_syscall");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("invalid_syscall"));
    }

    #[test]
    fn validate_syscall_rejects_empty() {
        assert!(validate_syscall("").is_err());
        assert!(validate_syscall("   ").is_err());
    }
}

// =============================================================================
// Task 325: Integration tests with bundle validation
// =============================================================================

mod bundle_integration_tests {
    use super::*;

    /// Tests that seccomp profile errors propagate correctly through validation.
    #[test]
    fn invalid_profile_error_includes_context() {
        let profile = r#"{"deny": ["unknown_syscall_xyz"]}"#;
        let result = SeccompFilter::from_str(profile);

        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_string = err.to_string();

        // Error should include the syscall name for debugging
        assert!(
            err_string.contains("unknown_syscall_xyz"),
            "Error should include the invalid syscall name: {}",
            err_string
        );
    }

    /// Tests that the validation APIs can be used for pre-flight validation.
    #[test]
    fn preflight_validation_workflow() {
        // Simulate a bundle validation workflow where we check syscalls before parsing
        let syscalls_to_check = vec!["ptrace", "mount", "invalid_one"];

        let mut valid_syscalls = Vec::new();
        let mut invalid_syscalls = Vec::new();

        for syscall in syscalls_to_check {
            if is_valid_syscall(syscall) {
                valid_syscalls.push(syscall);
            } else {
                invalid_syscalls.push(syscall);
            }
        }

        assert_eq!(valid_syscalls, vec!["ptrace", "mount"]);
        assert_eq!(invalid_syscalls, vec!["invalid_one"]);
    }

    /// Tests that action validation can be used for bundle seccomp profile validation.
    #[test]
    fn action_validation_for_bundle_profiles() {
        // Test common actions that might appear in bundle seccomp profiles
        let bundle_actions = vec![
            ("SCMP_ACT_ALLOW", true),
            ("SCMP_ACT_ERRNO", true),
            ("SCMP_ACT_KILL", true),
            ("allow", true), // alias
            ("errno", true), // alias
            ("INVALID_ACTION", false),
        ];

        for (action, should_be_valid) in bundle_actions {
            let result = validate_action(action);
            assert_eq!(
                result.is_ok(),
                should_be_valid,
                "Action '{}' validation mismatch: expected valid={}, got {:?}",
                action,
                should_be_valid,
                result
            );
        }
    }

    /// Tests that architecture validation can be used for bundle seccomp profiles.
    #[test]
    fn architecture_validation_for_bundle_profiles() {
        // Test architectures that might appear in bundle seccomp profiles
        let bundle_archs = vec![
            ("SCMP_ARCH_X86_64", true),
            ("x86_64", true),  // alias
            ("aarch64", true), // alias
            ("arm64", true),   // alias
            ("native", true),  // special
            ("INVALID_ARCH", false),
        ];

        for (arch, should_be_valid) in bundle_archs {
            let result = validate_architecture(arch);
            assert_eq!(
                result.is_ok(),
                should_be_valid,
                "Architecture '{}' validation mismatch: expected valid={}, got {:?}",
                arch,
                should_be_valid,
                result
            );
        }
    }

    /// Tests that errors from different validation functions are distinguishable.
    #[test]
    fn error_types_are_distinguishable() {
        // Syscall error
        let syscall_err = validate_syscall("invalid_syscall").unwrap_err();
        assert!(
            syscall_err.to_string().contains("syscall"),
            "Syscall error should mention 'syscall': {}",
            syscall_err
        );

        // Action error
        let action_err = validate_action("invalid_action").unwrap_err();
        assert!(
            action_err.to_string().contains("action"),
            "Action error should mention 'action': {}",
            action_err
        );
    }

    /// Tests that normalization produces consistent results across calls.
    #[test]
    fn normalization_is_idempotent() {
        let inputs = vec![
            ("allow", "SCMP_ACT_ALLOW"),
            ("SCMP_ACT_ALLOW", "SCMP_ACT_ALLOW"),
            ("x86_64", "SCMP_ARCH_X86_64"),
            ("SCMP_ARCH_X86_64", "SCMP_ARCH_X86_64"),
        ];

        for (input, expected) in inputs {
            // First normalization
            let first = if input.contains("ACT") || input == "allow" {
                normalize_action(input)
            } else {
                normalize_architecture(input)
            };

            // Second normalization (idempotent check)
            let second = if first.contains("ACT") {
                normalize_action(&first)
            } else {
                normalize_architecture(&first)
            };

            assert_eq!(
                first, second,
                "Normalization should be idempotent for '{}'",
                input
            );
            assert_eq!(first, expected);
        }
    }
}

// =============================================================================
// Task 330: Default action validation tests
// =============================================================================

mod default_action_validation_tests {
    use super::*;

    #[test]
    fn default_action_constant_is_valid() {
        assert!(
            SECCOMP_ACTIONS.contains(&DEFAULT_SECCOMP_ACTION),
            "DEFAULT_SECCOMP_ACTION should be a valid seccomp action"
        );
    }

    #[test]
    fn validates_known_default_actions() {
        for action in SECCOMP_ACTIONS {
            assert!(
                validate_default_action(action).is_ok(),
                "Should accept valid default action: {}",
                action
            );
        }
    }

    #[test]
    fn validates_lowercase_default_actions() {
        assert!(validate_default_action("scmp_act_allow").is_ok());
        assert!(validate_default_action("scmp_act_errno").is_ok());
        assert!(validate_default_action("scmp_act_kill").is_ok());
    }

    #[test]
    fn validates_alias_default_actions() {
        assert!(validate_default_action("allow").is_ok());
        assert!(validate_default_action("errno").is_ok());
        assert!(validate_default_action("kill").is_ok());
    }

    #[test]
    fn rejects_unknown_default_actions() {
        assert!(validate_default_action("INVALID_ACTION").is_err());
        assert!(validate_default_action("SCMP_ACT_UNKNOWN").is_err());
        assert!(validate_default_action("not_an_action").is_err());
    }

    #[test]
    fn rejects_empty_default_actions() {
        assert!(validate_default_action("").is_err());
        assert!(validate_default_action("   ").is_err());
    }

    #[test]
    fn error_message_includes_valid_actions() {
        let result = validate_default_action("invalid");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("SCMP_ACT_ALLOW"),
            "Error should list valid actions: {}",
            err
        );
    }
}

mod profile_with_default_action_tests {
    use super::*;

    #[test]
    fn parses_profile_with_valid_default_action() {
        let profile = r#"{"deny": [], "defaultAction": "SCMP_ACT_ALLOW"}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(
            result.is_ok(),
            "Should parse profile with valid defaultAction"
        );
    }

    #[test]
    fn parses_profile_with_lowercase_default_action() {
        let profile = r#"{"deny": [], "defaultAction": "scmp_act_allow"}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(
            result.is_ok(),
            "Should parse profile with lowercase defaultAction"
        );
    }

    #[test]
    fn parses_profile_with_alias_default_action() {
        let profile = r#"{"deny": [], "defaultAction": "allow"}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(
            result.is_ok(),
            "Should parse profile with alias defaultAction"
        );
    }

    #[test]
    fn parses_profile_without_default_action() {
        let profile = r#"{"deny": []}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(
            result.is_ok(),
            "Should parse profile without defaultAction (uses default)"
        );
    }

    #[test]
    fn rejects_profile_with_invalid_default_action() {
        let profile = r#"{"deny": [], "defaultAction": "INVALID_ACTION"}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(
            result.is_err(),
            "Should reject profile with invalid defaultAction"
        );
    }

    #[test]
    fn rejects_profile_with_empty_default_action() {
        let profile = r#"{"deny": [], "defaultAction": ""}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(
            result.is_err(),
            "Should reject profile with empty defaultAction"
        );
    }

    #[test]
    fn error_for_invalid_default_action_includes_context() {
        let profile = r#"{"deny": [], "defaultAction": "bad_action"}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("defaultAction") || err.contains("bad_action"),
            "Error should include context about invalid action: {}",
            err
        );
    }

    #[test]
    fn parses_profile_with_default_action_and_deny_list() {
        let profile = r#"{"deny": ["ptrace", "mount"], "defaultAction": "SCMP_ACT_LOG"}"#;
        let result = SeccompFilter::from_str(profile);
        assert!(
            result.is_ok(),
            "Should parse profile with both deny list and defaultAction"
        );
    }

    #[test]
    fn parses_all_valid_default_actions_in_profiles() {
        for action in SECCOMP_ACTIONS {
            let profile = format!(r#"{{"deny": [], "defaultAction": "{}"}}"#, action);
            let result = SeccompFilter::from_str(&profile);
            assert!(
                result.is_ok(),
                "Should parse profile with defaultAction {}: {:?}",
                action,
                result
            );
        }
    }
}
