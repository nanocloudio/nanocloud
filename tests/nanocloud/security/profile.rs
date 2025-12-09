use nanocloud::nanocloud::security::profile::{
    capabilities_are_non_privileged, capabilities_are_subset_of_all,
    capabilities_requiring_privileged, capability_set_difference, capability_set_is_subset,
    capability_sets_equal, dedupe_capabilities, is_valid_capability, normalize_capability_name,
    validate_capabilities, validate_capability, ALL_CAPABILITIES, NON_PRIVILEGED_CAPABILITIES,
};

// =============================================================================
// Task 316: Capability normalization tests (cases, aliases, invalids)
// =============================================================================

mod normalize_capability_name_tests {
    use super::*;

    // -------------------------------------------------------------------------
    // Case-insensitivity tests
    // -------------------------------------------------------------------------

    #[test]
    fn normalizes_lowercase_to_uppercase() {
        assert_eq!(normalize_capability_name("cap_net_raw"), "CAP_NET_RAW");
    }

    #[test]
    fn normalizes_mixed_case_to_uppercase() {
        assert_eq!(normalize_capability_name("Cap_Net_Raw"), "CAP_NET_RAW");
        assert_eq!(normalize_capability_name("cAp_NeT_rAw"), "CAP_NET_RAW");
    }

    #[test]
    fn preserves_uppercase_input() {
        assert_eq!(normalize_capability_name("CAP_NET_RAW"), "CAP_NET_RAW");
    }

    // -------------------------------------------------------------------------
    // Prefix handling tests
    // -------------------------------------------------------------------------

    #[test]
    fn adds_cap_prefix_when_missing() {
        assert_eq!(normalize_capability_name("net_raw"), "CAP_NET_RAW");
        assert_eq!(normalize_capability_name("NET_RAW"), "CAP_NET_RAW");
    }

    #[test]
    fn does_not_double_prefix() {
        assert_eq!(
            normalize_capability_name("CAP_CAP_NET_RAW"),
            "CAP_CAP_NET_RAW"
        );
    }

    // -------------------------------------------------------------------------
    // Character replacement (aliases/separators)
    // -------------------------------------------------------------------------

    #[test]
    fn replaces_dashes_with_underscores() {
        assert_eq!(normalize_capability_name("cap-net-raw"), "CAP_NET_RAW");
        assert_eq!(normalize_capability_name("net-bind-service"), "CAP_NET_BIND_SERVICE");
    }

    #[test]
    fn replaces_spaces_with_underscores() {
        assert_eq!(normalize_capability_name("cap net raw"), "CAP_NET_RAW");
        assert_eq!(normalize_capability_name("net bind service"), "CAP_NET_BIND_SERVICE");
    }

    #[test]
    fn handles_mixed_separators() {
        assert_eq!(normalize_capability_name("cap-net raw"), "CAP_NET_RAW");
        assert_eq!(normalize_capability_name("net-bind service"), "CAP_NET_BIND_SERVICE");
    }

    // -------------------------------------------------------------------------
    // Whitespace handling
    // -------------------------------------------------------------------------

    #[test]
    fn trims_leading_whitespace() {
        assert_eq!(normalize_capability_name("  CAP_NET_RAW"), "CAP_NET_RAW");
        assert_eq!(normalize_capability_name("\t\nCAP_NET_RAW"), "CAP_NET_RAW");
    }

    #[test]
    fn trims_trailing_whitespace() {
        assert_eq!(normalize_capability_name("CAP_NET_RAW  "), "CAP_NET_RAW");
        assert_eq!(normalize_capability_name("CAP_NET_RAW\t\n"), "CAP_NET_RAW");
    }

    #[test]
    fn trims_both_whitespace() {
        assert_eq!(normalize_capability_name("  CAP_NET_RAW  "), "CAP_NET_RAW");
    }

    // -------------------------------------------------------------------------
    // Empty/invalid input handling
    // -------------------------------------------------------------------------

    #[test]
    fn returns_empty_for_empty_input() {
        assert_eq!(normalize_capability_name(""), "");
    }

    #[test]
    fn returns_empty_for_whitespace_only() {
        assert_eq!(normalize_capability_name("   "), "");
        assert_eq!(normalize_capability_name("\t\n"), "");
    }

    // -------------------------------------------------------------------------
    // Known capabilities from NON_PRIVILEGED_CAPABILITIES
    // -------------------------------------------------------------------------

    #[test]
    fn normalizes_non_privileged_capabilities_correctly() {
        for cap in NON_PRIVILEGED_CAPABILITIES {
            // Already normalized form should be unchanged
            assert_eq!(normalize_capability_name(cap), *cap);

            // Lowercase version should normalize correctly
            let lowercase = cap.to_lowercase();
            assert_eq!(normalize_capability_name(&lowercase), *cap);

            // Without prefix should normalize correctly
            let without_prefix = cap.strip_prefix("CAP_").unwrap_or(cap);
            assert_eq!(normalize_capability_name(without_prefix), *cap);
        }
    }
}

mod dedupe_capabilities_tests {
    use super::*;

    #[test]
    fn removes_duplicate_entries() {
        let caps = vec!["CAP_NET_RAW", "CAP_NET_RAW", "CAP_NET_ADMIN"];
        let result = dedupe_capabilities(caps);
        assert_eq!(result, vec!["CAP_NET_RAW", "CAP_NET_ADMIN"]);
    }

    #[test]
    fn removes_duplicates_after_normalization() {
        let caps = vec!["cap_net_raw", "CAP_NET_RAW", "Cap_Net_Raw"];
        let result = dedupe_capabilities(caps);
        assert_eq!(result, vec!["CAP_NET_RAW"]);
    }

    #[test]
    fn preserves_insertion_order() {
        let caps = vec!["CAP_NET_ADMIN", "CAP_NET_RAW", "CAP_NET_BIND_SERVICE"];
        let result = dedupe_capabilities(caps);
        assert_eq!(result, vec!["CAP_NET_ADMIN", "CAP_NET_RAW", "CAP_NET_BIND_SERVICE"]);
    }

    #[test]
    fn normalizes_all_entries() {
        let caps = vec!["net_raw", "NET_ADMIN", "cap-net-bind-service"];
        let result = dedupe_capabilities(caps);
        assert_eq!(
            result,
            vec!["CAP_NET_RAW", "CAP_NET_ADMIN", "CAP_NET_BIND_SERVICE"]
        );
    }

    #[test]
    fn skips_empty_entries() {
        let caps = vec!["CAP_NET_RAW", "", "  ", "CAP_NET_ADMIN"];
        let result = dedupe_capabilities(caps);
        assert_eq!(result, vec!["CAP_NET_RAW", "CAP_NET_ADMIN"]);
    }

    #[test]
    fn handles_empty_input() {
        let caps: Vec<&str> = vec![];
        let result = dedupe_capabilities(caps);
        assert!(result.is_empty());
    }

    #[test]
    fn handles_all_empty_entries() {
        let caps = vec!["", "  ", "\t"];
        let result = dedupe_capabilities(caps);
        assert!(result.is_empty());
    }

    #[test]
    fn complex_deduplication_scenario() {
        // Mix of normalized, unnormalized, duplicates, and empty entries
        let caps = vec![
            "CAP_NET_RAW",
            "cap_net_raw",       // duplicate (after normalization)
            "NET_ADMIN",         // needs prefix
            "cap-net-admin",     // duplicate (after normalization)
            "",                  // empty
            "  CAP_SYS_ADMIN  ", // whitespace
            "sys_admin",         // duplicate (after normalization)
            "CAP_CHOWN",
        ];
        let result = dedupe_capabilities(caps);
        assert_eq!(
            result,
            vec!["CAP_NET_RAW", "CAP_NET_ADMIN", "CAP_SYS_ADMIN", "CAP_CHOWN"]
        );
    }
}

mod non_privileged_capabilities_tests {
    use super::*;

    // Note: Task 329 tracks enforcing sorted capability lists.
    // This test documents the current state rather than enforcing sorting.
    #[test]
    fn non_privileged_capabilities_list_is_not_empty() {
        assert!(
            !NON_PRIVILEGED_CAPABILITIES.is_empty(),
            "NON_PRIVILEGED_CAPABILITIES should not be empty"
        );
    }

    #[test]
    fn non_privileged_capabilities_list_is_unique() {
        let mut seen = std::collections::HashSet::new();
        for cap in NON_PRIVILEGED_CAPABILITIES {
            assert!(
                seen.insert(*cap),
                "Duplicate capability found: {}",
                cap
            );
        }
    }

    #[test]
    fn non_privileged_capabilities_are_normalized() {
        for cap in NON_PRIVILEGED_CAPABILITIES {
            assert!(
                cap.starts_with("CAP_"),
                "Capability {} should start with CAP_",
                cap
            );
            assert_eq!(
                *cap,
                cap.to_ascii_uppercase(),
                "Capability {} should be uppercase",
                cap
            );
            assert!(
                !cap.contains('-'),
                "Capability {} should not contain dashes",
                cap
            );
            assert!(
                !cap.contains(' '),
                "Capability {} should not contain spaces",
                cap
            );
        }
    }

    #[test]
    fn non_privileged_capabilities_contains_expected_entries() {
        // These are networking-related capabilities that should not require privileged mode
        assert!(NON_PRIVILEGED_CAPABILITIES.contains(&"CAP_NET_BIND_SERVICE"));
        assert!(NON_PRIVILEGED_CAPABILITIES.contains(&"CAP_NET_ADMIN"));
        assert!(NON_PRIVILEGED_CAPABILITIES.contains(&"CAP_NET_RAW"));
    }
}

// =============================================================================
// Task 321: Kernel capability list helpers
// =============================================================================

mod all_capabilities_tests {
    use super::*;

    /// Core capabilities that must always be in ALL_CAPABILITIES.
    /// These are fundamental capabilities present since early Linux versions.
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

    #[test]
    fn all_capabilities_contains_core_set() {
        for core_cap in CORE_CAPABILITIES {
            assert!(
                ALL_CAPABILITIES.contains(core_cap),
                "ALL_CAPABILITIES missing core capability: {}",
                core_cap
            );
        }
    }

    #[test]
    fn all_capabilities_list_is_not_empty() {
        assert!(
            !ALL_CAPABILITIES.is_empty(),
            "ALL_CAPABILITIES should not be empty"
        );
    }

    #[test]
    fn all_capabilities_list_is_sorted() {
        let mut sorted = ALL_CAPABILITIES.to_vec();
        sorted.sort();
        assert_eq!(
            ALL_CAPABILITIES.to_vec(),
            sorted,
            "ALL_CAPABILITIES should be sorted alphabetically for maintainability"
        );
    }

    #[test]
    fn all_capabilities_list_is_unique() {
        let mut seen = std::collections::HashSet::new();
        for cap in ALL_CAPABILITIES {
            assert!(
                seen.insert(*cap),
                "Duplicate capability in ALL_CAPABILITIES: {}",
                cap
            );
        }
    }

    #[test]
    fn all_capabilities_are_normalized() {
        for cap in ALL_CAPABILITIES {
            assert!(
                cap.starts_with("CAP_"),
                "Capability {} should start with CAP_",
                cap
            );
            assert_eq!(
                *cap,
                cap.to_ascii_uppercase(),
                "Capability {} should be uppercase",
                cap
            );
        }
    }

    #[test]
    fn all_capabilities_includes_non_privileged() {
        // NON_PRIVILEGED_CAPABILITIES should be a subset of ALL_CAPABILITIES
        for cap in NON_PRIVILEGED_CAPABILITIES {
            assert!(
                ALL_CAPABILITIES.contains(cap),
                "NON_PRIVILEGED_CAPABILITIES entry {} not found in ALL_CAPABILITIES",
                cap
            );
        }
    }

    #[test]
    fn all_capabilities_has_expected_count() {
        // Linux 6.x has 41 capabilities (0-40)
        // This test will catch if capabilities are accidentally removed
        assert!(
            ALL_CAPABILITIES.len() >= 41,
            "ALL_CAPABILITIES should have at least 41 entries (Linux 6.x), found {}",
            ALL_CAPABILITIES.len()
        );
    }
}

// =============================================================================
// Task 322: Normalization helper for external callers
// =============================================================================

mod is_valid_capability_tests {
    use super::*;

    #[test]
    fn accepts_valid_capabilities() {
        assert!(is_valid_capability("CAP_NET_RAW"));
        assert!(is_valid_capability("CAP_CHOWN"));
        assert!(is_valid_capability("CAP_SYS_ADMIN"));
    }

    #[test]
    fn accepts_lowercase_capabilities() {
        assert!(is_valid_capability("cap_net_raw"));
        assert!(is_valid_capability("cap_chown"));
    }

    #[test]
    fn accepts_without_prefix() {
        assert!(is_valid_capability("NET_RAW"));
        assert!(is_valid_capability("CHOWN"));
    }

    #[test]
    fn accepts_with_dashes() {
        assert!(is_valid_capability("cap-net-raw"));
        assert!(is_valid_capability("net-admin"));
    }

    #[test]
    fn rejects_invalid_capabilities() {
        assert!(!is_valid_capability("CAP_INVALID"));
        assert!(!is_valid_capability("NOT_A_CAP"));
        assert!(!is_valid_capability("random_string"));
    }

    #[test]
    fn rejects_empty_input() {
        assert!(!is_valid_capability(""));
        assert!(!is_valid_capability("   "));
    }
}

mod validate_capability_tests {
    use super::*;

    #[test]
    fn returns_normalized_form_for_valid() {
        assert_eq!(validate_capability("net_raw").unwrap(), "CAP_NET_RAW");
        assert_eq!(validate_capability("CAP_CHOWN").unwrap(), "CAP_CHOWN");
        assert_eq!(validate_capability("cap-sys-admin").unwrap(), "CAP_SYS_ADMIN");
    }

    #[test]
    fn returns_error_for_invalid() {
        let result = validate_capability("INVALID_CAP");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("INVALID_CAP"));
    }

    #[test]
    fn returns_error_for_empty() {
        let result = validate_capability("");
        assert!(result.is_err());
    }

    #[test]
    fn error_includes_original_input() {
        let result = validate_capability("my_invalid_cap");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("my_invalid_cap"),
            "Error should include the original input: {}",
            err
        );
    }
}

mod validate_capabilities_tests {
    use super::*;

    #[test]
    fn validates_all_entries() {
        let caps = vec!["NET_RAW", "CHOWN", "cap-sys-admin"];
        let result = validate_capabilities(caps).unwrap();
        assert_eq!(result, vec!["CAP_NET_RAW", "CAP_CHOWN", "CAP_SYS_ADMIN"]);
    }

    #[test]
    fn deduplicates_entries() {
        let caps = vec!["NET_RAW", "cap_net_raw", "CAP_NET_RAW"];
        let result = validate_capabilities(caps).unwrap();
        assert_eq!(result, vec!["CAP_NET_RAW"]);
    }

    #[test]
    fn skips_empty_entries() {
        let caps = vec!["NET_RAW", "", "  ", "CHOWN"];
        let result = validate_capabilities(caps).unwrap();
        assert_eq!(result, vec!["CAP_NET_RAW", "CAP_CHOWN"]);
    }

    #[test]
    fn fails_on_first_invalid() {
        let caps = vec!["NET_RAW", "INVALID", "CHOWN"];
        let result = validate_capabilities(caps);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("INVALID"));
    }

    #[test]
    fn handles_empty_list() {
        let caps: Vec<&str> = vec![];
        let result = validate_capabilities(caps).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn handles_all_empty_entries() {
        let caps = vec!["", "  ", "\t"];
        let result = validate_capabilities(caps).unwrap();
        assert!(result.is_empty());
    }
}

// =============================================================================
// Task 323: Capability set emptiness/defaults validation
// =============================================================================

mod capability_set_validation_tests {
    use super::*;

    #[test]
    fn empty_list_is_valid() {
        // Policy: empty list is valid (means no additional capabilities)
        let caps: Vec<&str> = vec![];
        let result = validate_capabilities(caps);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn whitespace_only_treated_as_empty() {
        let caps = vec!["", "   ", "\t\n"];
        let result = validate_capabilities(caps);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn all_caps_produces_full_list() {
        // Users can request all capabilities by listing them explicitly
        let result = validate_capabilities(ALL_CAPABILITIES.iter().copied());
        assert!(result.is_ok());
        let validated = result.unwrap();
        assert_eq!(validated.len(), ALL_CAPABILITIES.len());
    }

    #[test]
    fn non_privileged_caps_is_subset_of_all() {
        let result = validate_capabilities(NON_PRIVILEGED_CAPABILITIES.iter().copied());
        assert!(result.is_ok());
        let validated = result.unwrap();
        assert_eq!(validated.len(), NON_PRIVILEGED_CAPABILITIES.len());
    }
}

// =============================================================================
// Task 325: Integration tests with bundle validation
// =============================================================================

mod bundle_integration_tests {
    use super::*;

    /// Tests that capability validation produces errors usable by bundle validation.
    #[test]
    fn capability_validation_error_includes_offending_value() {
        let result = validate_capability("INVALID_CAP_XYZ");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_string = err.to_string();

        // Error should include the capability name for debugging
        assert!(
            err_string.contains("INVALID_CAP_XYZ"),
            "Error should include the invalid capability name: {}",
            err_string
        );
    }

    /// Tests that bundle-style capability lists are validated correctly.
    #[test]
    fn bundle_style_capability_list_validation() {
        // Simulate a bundle spec extraCapabilities field
        let bundle_caps = vec![
            "CAP_NET_RAW",
            "cap_net_admin", // lowercase - should normalize
            "NET_BIND_SERVICE", // without prefix - should normalize
        ];

        let result = validate_capabilities(bundle_caps);
        assert!(result.is_ok());

        let validated = result.unwrap();
        assert_eq!(validated.len(), 3);
        assert!(validated.contains(&"CAP_NET_RAW".to_string()));
        assert!(validated.contains(&"CAP_NET_ADMIN".to_string()));
        assert!(validated.contains(&"CAP_NET_BIND_SERVICE".to_string()));
    }

    /// Tests that validation catches invalid capabilities in a mixed list.
    #[test]
    fn mixed_valid_invalid_capabilities_fails_fast() {
        let caps = vec!["CAP_NET_RAW", "INVALID_CAP", "CAP_CHOWN"];
        let result = validate_capabilities(caps);

        assert!(result.is_err());
        let err = result.unwrap_err();
        // Should fail on the first invalid capability
        assert!(err.to_string().contains("INVALID_CAP"));
    }

    /// Tests that non-privileged capabilities check works for bundle validation.
    #[test]
    fn non_privileged_capabilities_are_identified() {
        for cap in NON_PRIVILEGED_CAPABILITIES {
            assert!(
                is_valid_capability(cap),
                "NON_PRIVILEGED_CAPABILITY {} should be valid",
                cap
            );
            // Also test lowercase and without prefix
            let lowercase = cap.to_lowercase();
            assert!(
                is_valid_capability(&lowercase),
                "Lowercase {} should be valid",
                lowercase
            );
        }
    }

    /// Tests that privileged capabilities are correctly identified.
    #[test]
    fn privileged_capabilities_are_valid_but_not_in_non_privileged() {
        let privileged_caps = vec![
            "CAP_SYS_ADMIN",
            "CAP_SYS_MODULE",
            "CAP_SYS_RAWIO",
            "CAP_SYS_PTRACE",
        ];

        for cap in privileged_caps {
            // Should be valid capabilities
            assert!(
                is_valid_capability(cap),
                "{} should be a valid capability",
                cap
            );
            // But not in non-privileged list
            assert!(
                !NON_PRIVILEGED_CAPABILITIES.contains(&cap),
                "{} should not be in NON_PRIVILEGED_CAPABILITIES",
                cap
            );
        }
    }

    /// Tests that dedupe_capabilities works correctly for bundle validation.
    #[test]
    fn deduplication_preserves_bundle_semantics() {
        // Bundle specs might have duplicates due to user error or templating
        let bundle_caps = vec![
            "CAP_NET_RAW",
            "cap_net_raw",   // duplicate after normalization
            "CAP_NET_ADMIN",
            "net_admin",     // duplicate after normalization
            "",              // empty entry
            "CAP_CHOWN",
        ];

        let result = dedupe_capabilities(bundle_caps);

        // Should have exactly 3 unique capabilities
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], "CAP_NET_RAW");
        assert_eq!(result[1], "CAP_NET_ADMIN");
        assert_eq!(result[2], "CAP_CHOWN");
    }

    /// Tests that normalization is consistent between is_valid and validate.
    #[test]
    fn validation_functions_are_consistent() {
        let test_caps = vec![
            "CAP_NET_RAW",
            "cap_net_raw",
            "NET_RAW",
            "net-raw",
            "  cap_net_raw  ",
        ];

        for cap in test_caps {
            let is_valid_result = is_valid_capability(cap);
            let validate_result = validate_capability(cap);

            assert_eq!(
                is_valid_result,
                validate_result.is_ok(),
                "is_valid_capability and validate_capability should agree for '{}'",
                cap
            );

            if validate_result.is_ok() {
                // All variations should normalize to the same value
                assert_eq!(
                    validate_result.unwrap(),
                    "CAP_NET_RAW",
                    "All valid variations of CAP_NET_RAW should normalize identically"
                );
            }
        }
    }

    /// Tests that error messages are suitable for user-facing bundle validation errors.
    #[test]
    fn error_messages_are_user_friendly() {
        let result = validate_capability("my_custom_cap");
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_string = err.to_string();

        // Error should be descriptive
        assert!(
            err_string.contains("capability") || err_string.contains("unknown"),
            "Error message should indicate it's a capability issue: {}",
            err_string
        );
        // Error should include the input
        assert!(
            err_string.contains("my_custom_cap"),
            "Error message should include the invalid value: {}",
            err_string
        );
    }
}

// =============================================================================
// Task 331: Capability set comparison helpers
// =============================================================================

mod capabilities_are_subset_of_all_tests {
    use super::*;

    #[test]
    fn accepts_valid_capabilities() {
        assert!(capabilities_are_subset_of_all(["CAP_NET_RAW", "CAP_CHOWN"]));
    }

    #[test]
    fn accepts_lowercase_capabilities() {
        assert!(capabilities_are_subset_of_all(["cap_net_raw", "cap_chown"]));
    }

    #[test]
    fn accepts_without_prefix() {
        assert!(capabilities_are_subset_of_all(["NET_RAW", "CHOWN"]));
    }

    #[test]
    fn rejects_invalid_capabilities() {
        assert!(!capabilities_are_subset_of_all(["CAP_NET_RAW", "CAP_INVALID"]));
    }

    #[test]
    fn accepts_empty_set() {
        assert!(capabilities_are_subset_of_all(Vec::<&str>::new()));
    }

    #[test]
    fn accepts_all_capabilities() {
        assert!(capabilities_are_subset_of_all(ALL_CAPABILITIES.iter().copied()));
    }
}

mod capabilities_are_non_privileged_tests {
    use super::*;

    #[test]
    fn accepts_non_privileged_capabilities() {
        assert!(capabilities_are_non_privileged(["CAP_NET_RAW", "CAP_NET_ADMIN"]));
    }

    #[test]
    fn accepts_all_non_privileged() {
        assert!(capabilities_are_non_privileged(
            NON_PRIVILEGED_CAPABILITIES.iter().copied()
        ));
    }

    #[test]
    fn rejects_privileged_capabilities() {
        assert!(!capabilities_are_non_privileged(["CAP_SYS_ADMIN"]));
        assert!(!capabilities_are_non_privileged(["CAP_CHOWN"]));
    }

    #[test]
    fn rejects_mixed_set() {
        assert!(!capabilities_are_non_privileged([
            "CAP_NET_RAW",
            "CAP_SYS_ADMIN"
        ]));
    }

    #[test]
    fn accepts_empty_set() {
        assert!(capabilities_are_non_privileged(Vec::<&str>::new()));
    }

    #[test]
    fn accepts_lowercase() {
        assert!(capabilities_are_non_privileged(["cap_net_raw"]));
    }
}

mod capability_sets_equal_tests {
    use super::*;

    #[test]
    fn equal_sets_are_equal() {
        assert!(capability_sets_equal(
            ["CAP_NET_RAW", "CAP_CHOWN"],
            ["CAP_NET_RAW", "CAP_CHOWN"]
        ));
    }

    #[test]
    fn equal_regardless_of_order() {
        assert!(capability_sets_equal(
            ["CAP_CHOWN", "CAP_NET_RAW"],
            ["CAP_NET_RAW", "CAP_CHOWN"]
        ));
    }

    #[test]
    fn equal_regardless_of_case() {
        assert!(capability_sets_equal(
            ["CAP_NET_RAW", "CAP_CHOWN"],
            ["cap_net_raw", "cap_chown"]
        ));
    }

    #[test]
    fn equal_with_duplicates() {
        assert!(capability_sets_equal(
            ["CAP_NET_RAW", "cap_net_raw", "CAP_NET_RAW"],
            ["CAP_NET_RAW"]
        ));
    }

    #[test]
    fn different_sets_not_equal() {
        assert!(!capability_sets_equal(["CAP_NET_RAW"], ["CAP_CHOWN"]));
    }

    #[test]
    fn subset_not_equal() {
        assert!(!capability_sets_equal(
            ["CAP_NET_RAW"],
            ["CAP_NET_RAW", "CAP_CHOWN"]
        ));
    }

    #[test]
    fn empty_sets_are_equal() {
        let empty: Vec<&str> = vec![];
        assert!(capability_sets_equal(empty.clone(), empty));
    }

    #[test]
    fn empty_vs_non_empty_not_equal() {
        assert!(!capability_sets_equal(Vec::<&str>::new(), ["CAP_NET_RAW"]));
    }
}

mod capability_set_is_subset_tests {
    use super::*;

    #[test]
    fn subset_is_subset() {
        assert!(capability_set_is_subset(
            ["CAP_NET_RAW"],
            ["CAP_NET_RAW", "CAP_CHOWN"]
        ));
    }

    #[test]
    fn equal_sets_are_subsets() {
        assert!(capability_set_is_subset(
            ["CAP_NET_RAW"],
            ["CAP_NET_RAW"]
        ));
    }

    #[test]
    fn empty_is_subset_of_anything() {
        assert!(capability_set_is_subset(Vec::<&str>::new(), ["CAP_NET_RAW"]));
        let empty: Vec<&str> = vec![];
        assert!(capability_set_is_subset(empty.clone(), empty));
    }

    #[test]
    fn superset_is_not_subset() {
        assert!(!capability_set_is_subset(
            ["CAP_NET_RAW", "CAP_CHOWN"],
            ["CAP_NET_RAW"]
        ));
    }

    #[test]
    fn disjoint_not_subset() {
        assert!(!capability_set_is_subset(["CAP_NET_RAW"], ["CAP_CHOWN"]));
    }

    #[test]
    fn normalizes_before_comparison() {
        assert!(capability_set_is_subset(
            ["cap_net_raw"],
            ["CAP_NET_RAW", "CAP_CHOWN"]
        ));
    }
}

mod capability_set_difference_tests {
    use super::*;

    #[test]
    fn finds_difference() {
        let diff = capability_set_difference(
            ["CAP_NET_RAW", "CAP_CHOWN", "CAP_SYS_ADMIN"],
            ["CAP_NET_RAW", "CAP_CHOWN"],
        );
        assert_eq!(diff, vec!["CAP_SYS_ADMIN"]);
    }

    #[test]
    fn empty_when_subset() {
        let diff = capability_set_difference(
            ["CAP_NET_RAW"],
            ["CAP_NET_RAW", "CAP_CHOWN"],
        );
        assert!(diff.is_empty());
    }

    #[test]
    fn empty_when_equal() {
        let diff = capability_set_difference(
            ["CAP_NET_RAW", "CAP_CHOWN"],
            ["CAP_NET_RAW", "CAP_CHOWN"],
        );
        assert!(diff.is_empty());
    }

    #[test]
    fn returns_all_when_disjoint() {
        let diff = capability_set_difference(["CAP_NET_RAW"], ["CAP_CHOWN"]);
        assert_eq!(diff, vec!["CAP_NET_RAW"]);
    }

    #[test]
    fn empty_from_empty() {
        let diff = capability_set_difference(Vec::<&str>::new(), ["CAP_NET_RAW"]);
        assert!(diff.is_empty());
    }

    #[test]
    fn result_is_sorted() {
        let diff = capability_set_difference(
            ["CAP_SYS_ADMIN", "CAP_CHOWN", "CAP_NET_RAW"],
            Vec::<&str>::new(),
        );
        assert_eq!(diff, vec!["CAP_CHOWN", "CAP_NET_RAW", "CAP_SYS_ADMIN"]);
    }

    #[test]
    fn normalizes_before_comparison() {
        let diff = capability_set_difference(
            ["cap_net_raw", "CAP_CHOWN"],
            ["CAP_NET_RAW"],
        );
        assert_eq!(diff, vec!["CAP_CHOWN"]);
    }
}

mod capabilities_requiring_privileged_tests {
    use super::*;

    #[test]
    fn finds_privileged_capabilities() {
        let privileged = capabilities_requiring_privileged([
            "CAP_NET_RAW",   // non-privileged
            "CAP_SYS_ADMIN", // privileged
            "CAP_CHOWN",     // privileged
        ]);
        assert_eq!(privileged, vec!["CAP_CHOWN", "CAP_SYS_ADMIN"]);
    }

    #[test]
    fn empty_when_all_non_privileged() {
        let privileged = capabilities_requiring_privileged([
            "CAP_NET_RAW",
            "CAP_NET_ADMIN",
            "CAP_NET_BIND_SERVICE",
        ]);
        assert!(privileged.is_empty());
    }

    #[test]
    fn empty_for_empty_input() {
        let privileged = capabilities_requiring_privileged(Vec::<&str>::new());
        assert!(privileged.is_empty());
    }

    #[test]
    fn deduplicates_results() {
        let privileged = capabilities_requiring_privileged([
            "CAP_CHOWN",
            "cap_chown",
            "chown",
        ]);
        assert_eq!(privileged, vec!["CAP_CHOWN"]);
    }

    #[test]
    fn result_is_sorted() {
        let privileged = capabilities_requiring_privileged([
            "CAP_SYS_ADMIN",
            "CAP_CHOWN",
            "CAP_KILL",
        ]);
        assert_eq!(privileged, vec!["CAP_CHOWN", "CAP_KILL", "CAP_SYS_ADMIN"]);
    }

    #[test]
    fn normalizes_input() {
        let privileged = capabilities_requiring_privileged(["cap_sys_admin"]);
        assert_eq!(privileged, vec!["CAP_SYS_ADMIN"]);
    }
}

// =============================================================================
// Task 332: Backward-compatibility tests for legacy capability names
// =============================================================================
//
// This module tests that various legacy and alternative naming conventions
// for Linux capabilities are correctly normalized and accepted. These tests
// ensure backward compatibility with older configurations and different
// naming styles used in container ecosystems (Docker, Kubernetes, OCI).

mod legacy_capability_name_tests {
    use super::*;

    // -------------------------------------------------------------------------
    // Docker-style naming (lowercase without CAP_ prefix)
    // -------------------------------------------------------------------------
    // Docker historically used lowercase capability names without the CAP_ prefix
    // in its --cap-add and --cap-drop flags.

    #[test]
    fn accepts_docker_style_lowercase_names() {
        // Docker-style: lowercase without prefix
        let docker_caps = [
            "chown",
            "dac_override",
            "fowner",
            "kill",
            "net_admin",
            "net_bind_service",
            "net_raw",
            "setgid",
            "setuid",
            "sys_admin",
            "sys_chroot",
        ];

        for cap in docker_caps {
            assert!(
                is_valid_capability(cap),
                "Docker-style capability '{}' should be valid",
                cap
            );
            let normalized = normalize_capability_name(cap);
            assert!(
                normalized.starts_with("CAP_"),
                "Normalized '{}' should have CAP_ prefix: {}",
                cap,
                normalized
            );
        }
    }

    // -------------------------------------------------------------------------
    // Kubernetes/OCI-style naming (uppercase without CAP_ prefix)
    // -------------------------------------------------------------------------
    // Kubernetes and OCI specs often use uppercase names without the CAP_ prefix.

    #[test]
    fn accepts_kubernetes_style_uppercase_names() {
        let k8s_caps = [
            "CHOWN",
            "DAC_OVERRIDE",
            "FOWNER",
            "KILL",
            "NET_ADMIN",
            "NET_BIND_SERVICE",
            "NET_RAW",
            "SETGID",
            "SETUID",
            "SYS_ADMIN",
            "SYS_CHROOT",
        ];

        for cap in k8s_caps {
            assert!(
                is_valid_capability(cap),
                "Kubernetes-style capability '{}' should be valid",
                cap
            );
            assert_eq!(
                normalize_capability_name(cap),
                format!("CAP_{}", cap),
                "Should add CAP_ prefix to '{}'",
                cap
            );
        }
    }

    // -------------------------------------------------------------------------
    // Dash-separated names (alternative separator)
    // -------------------------------------------------------------------------
    // Some tools and configs use dashes instead of underscores.

    #[test]
    fn accepts_dash_separated_names() {
        let dash_caps = [
            ("cap-net-raw", "CAP_NET_RAW"),
            ("cap-net-admin", "CAP_NET_ADMIN"),
            ("cap-sys-admin", "CAP_SYS_ADMIN"),
            ("net-bind-service", "CAP_NET_BIND_SERVICE"),
            ("dac-override", "CAP_DAC_OVERRIDE"),
            ("CAP-NET-RAW", "CAP_NET_RAW"),
        ];

        for (input, expected) in dash_caps {
            assert!(
                is_valid_capability(input),
                "Dash-separated capability '{}' should be valid",
                input
            );
            assert_eq!(
                normalize_capability_name(input),
                expected,
                "Dash-separated '{}' should normalize to '{}'",
                input,
                expected
            );
        }
    }

    // -------------------------------------------------------------------------
    // Space-separated names (rarely used but supported)
    // -------------------------------------------------------------------------

    #[test]
    fn accepts_space_separated_names() {
        let space_caps = [
            ("cap net raw", "CAP_NET_RAW"),
            ("net bind service", "CAP_NET_BIND_SERVICE"),
            ("sys admin", "CAP_SYS_ADMIN"),
        ];

        for (input, expected) in space_caps {
            assert!(
                is_valid_capability(input),
                "Space-separated capability '{}' should be valid",
                input
            );
            assert_eq!(
                normalize_capability_name(input),
                expected,
                "Space-separated '{}' should normalize to '{}'",
                input,
                expected
            );
        }
    }

    // -------------------------------------------------------------------------
    // Mixed case names
    // -------------------------------------------------------------------------

    #[test]
    fn accepts_mixed_case_names() {
        let mixed_caps = [
            ("Cap_Net_Raw", "CAP_NET_RAW"),
            ("cap_Net_Admin", "CAP_NET_ADMIN"),
            ("CAP_net_raw", "CAP_NET_RAW"),
            ("Cap-Net-Bind-Service", "CAP_NET_BIND_SERVICE"),
        ];

        for (input, expected) in mixed_caps {
            assert!(
                is_valid_capability(input),
                "Mixed-case capability '{}' should be valid",
                input
            );
            assert_eq!(
                normalize_capability_name(input),
                expected,
                "Mixed-case '{}' should normalize to '{}'",
                input,
                expected
            );
        }
    }

    // -------------------------------------------------------------------------
    // Full canonical names (CAP_* uppercase)
    // -------------------------------------------------------------------------

    #[test]
    fn accepts_canonical_names() {
        // All canonical names should pass through unchanged
        for cap in ALL_CAPABILITIES {
            assert!(
                is_valid_capability(cap),
                "Canonical capability '{}' should be valid",
                cap
            );
            assert_eq!(
                normalize_capability_name(cap),
                *cap,
                "Canonical '{}' should remain unchanged",
                cap
            );
        }
    }

    // -------------------------------------------------------------------------
    // Whitespace handling
    // -------------------------------------------------------------------------

    #[test]
    fn handles_whitespace_variations() {
        let whitespace_caps = [
            ("  CAP_NET_RAW  ", "CAP_NET_RAW"),
            ("\tnet_admin\t", "CAP_NET_ADMIN"),
            ("  chown", "CAP_CHOWN"),
            ("sys_admin  ", "CAP_SYS_ADMIN"),
        ];

        for (input, expected) in whitespace_caps {
            assert!(
                is_valid_capability(input),
                "Whitespace-padded capability '{}' should be valid",
                input
            );
            assert_eq!(
                normalize_capability_name(input),
                expected,
                "Whitespace-padded '{}' should normalize to '{}'",
                input,
                expected
            );
        }
    }

    // -------------------------------------------------------------------------
    // Invalid/unknown capabilities should fail gracefully
    // -------------------------------------------------------------------------

    #[test]
    fn rejects_unknown_capabilities_with_clear_error() {
        let invalid_caps = [
            "CAP_UNKNOWN",
            "UNKNOWN_CAP",
            "not_a_capability",
            "CAP_FOO_BAR",
            "random_string",
        ];

        for cap in invalid_caps {
            assert!(
                !is_valid_capability(cap),
                "Unknown capability '{}' should be invalid",
                cap
            );

            let result = validate_capability(cap);
            assert!(
                result.is_err(),
                "validate_capability('{}') should return error",
                cap
            );

            let err = result.unwrap_err();
            let err_msg = err.to_string();
            // Error should mention "capability" or "unknown"
            assert!(
                err_msg.to_lowercase().contains("capability")
                    || err_msg.to_lowercase().contains("unknown"),
                "Error for '{}' should be descriptive: {}",
                cap,
                err_msg
            );
        }
    }

    // -------------------------------------------------------------------------
    // Equivalence across naming styles
    // -------------------------------------------------------------------------

    #[test]
    fn different_styles_normalize_to_same_value() {
        // All these should normalize to CAP_NET_RAW
        let net_raw_variants = [
            "CAP_NET_RAW",
            "cap_net_raw",
            "Cap_Net_Raw",
            "NET_RAW",
            "net_raw",
            "Net_Raw",
            "cap-net-raw",
            "CAP-NET-RAW",
            "net-raw",
            "  cap_net_raw  ",
        ];

        let normalized: std::collections::HashSet<_> = net_raw_variants
            .iter()
            .map(|s| normalize_capability_name(s))
            .collect();

        assert_eq!(
            normalized.len(),
            1,
            "All variants should normalize to the same value"
        );
        assert!(
            normalized.contains("CAP_NET_RAW"),
            "All variants should normalize to CAP_NET_RAW"
        );
    }

    // -------------------------------------------------------------------------
    // Validate capabilities function handles mixed styles
    // -------------------------------------------------------------------------

    #[test]
    fn validate_capabilities_handles_mixed_styles() {
        let mixed_input = ["CAP_NET_RAW", "chown", "NET_ADMIN", "cap-sys-chroot"];

        let result = validate_capabilities(mixed_input.iter().copied());
        assert!(result.is_ok(), "Mixed styles should validate successfully");

        let validated = result.unwrap();
        assert_eq!(validated.len(), 4);
        assert!(validated.contains(&"CAP_NET_RAW".to_string()));
        assert!(validated.contains(&"CAP_CHOWN".to_string()));
        assert!(validated.contains(&"CAP_NET_ADMIN".to_string()));
        assert!(validated.contains(&"CAP_SYS_CHROOT".to_string()));
    }

    // -------------------------------------------------------------------------
    // Dedupe handles mixed styles correctly
    // -------------------------------------------------------------------------

    #[test]
    fn dedupe_recognizes_equivalent_styles() {
        let mixed_duplicates = [
            "CAP_NET_RAW",
            "cap_net_raw",
            "net_raw",
            "NET_RAW",
            "cap-net-raw",
        ];

        let deduped = dedupe_capabilities(mixed_duplicates);
        assert_eq!(
            deduped.len(),
            1,
            "All variants of NET_RAW should deduplicate to one entry"
        );
        assert_eq!(deduped[0], "CAP_NET_RAW");
    }
}

// =============================================================================
// Task 334: Integration tests with runtime exec to verify capability application
// =============================================================================
//
// These tests verify that the security module's capability functions integrate
// correctly with runtime configuration scenarios. They use mock runtime
// configurations to ensure normalized capability sets are correctly produced
// and validated for container execution.

mod runtime_integration_tests {
    use super::*;

    /// Simulates a container runtime configuration with capabilities
    #[derive(Debug)]
    struct MockContainerConfig {
        add_capabilities: Vec<String>,
        drop_capabilities: Vec<String>,
    }

    impl MockContainerConfig {
        fn new() -> Self {
            Self {
                add_capabilities: Vec::new(),
                drop_capabilities: Vec::new(),
            }
        }

        /// Add capabilities (normalizes input)
        fn add_caps<S: AsRef<str>>(&mut self, caps: impl IntoIterator<Item = S>) {
            for cap in caps {
                let normalized = normalize_capability_name(cap.as_ref());
                if !normalized.is_empty() && !self.add_capabilities.contains(&normalized) {
                    self.add_capabilities.push(normalized);
                }
            }
        }

        /// Drop capabilities (normalizes input)
        fn drop_caps<S: AsRef<str>>(&mut self, caps: impl IntoIterator<Item = S>) {
            for cap in caps {
                let normalized = normalize_capability_name(cap.as_ref());
                if !normalized.is_empty() && !self.drop_capabilities.contains(&normalized) {
                    self.drop_capabilities.push(normalized);
                }
            }
        }

        /// Validate all capabilities are known
        fn validate(&self) -> Result<(), String> {
            for cap in &self.add_capabilities {
                if !is_valid_capability(cap) {
                    return Err(format!("Unknown capability to add: {}", cap));
                }
            }
            for cap in &self.drop_capabilities {
                if !is_valid_capability(cap) {
                    return Err(format!("Unknown capability to drop: {}", cap));
                }
            }
            Ok(())
        }

        /// Check if privileged mode is required
        fn requires_privileged(&self) -> bool {
            !capabilities_are_non_privileged(self.add_capabilities.iter().map(|s| s.as_str()))
        }

        /// Get capabilities that require privileged mode
        fn privileged_caps(&self) -> Vec<String> {
            capabilities_requiring_privileged(self.add_capabilities.iter().map(|s| s.as_str()))
        }

        /// Compute effective capability set (add - drop)
        fn effective_capabilities(&self) -> Vec<String> {
            self.add_capabilities
                .iter()
                .filter(|c| !self.drop_capabilities.contains(c))
                .cloned()
                .collect()
        }
    }

    // -------------------------------------------------------------------------
    // Docker-style configuration tests
    // -------------------------------------------------------------------------

    #[test]
    fn docker_style_add_cap_configuration() {
        // Docker: docker run --cap-add NET_RAW --cap-add SYS_ADMIN ...
        let mut config = MockContainerConfig::new();
        config.add_caps(["NET_RAW", "SYS_ADMIN"]);

        assert!(config.validate().is_ok());
        assert!(config.add_capabilities.contains(&"CAP_NET_RAW".to_string()));
        assert!(config.add_capabilities.contains(&"CAP_SYS_ADMIN".to_string()));
    }

    #[test]
    fn docker_style_drop_cap_configuration() {
        // Docker: docker run --cap-drop ALL --cap-add NET_RAW ...
        let mut config = MockContainerConfig::new();
        config.add_caps(["NET_RAW", "NET_ADMIN"]);
        config.drop_caps(["CHOWN", "SETUID", "SETGID"]);

        assert!(config.validate().is_ok());
        assert_eq!(config.add_capabilities.len(), 2);
        assert_eq!(config.drop_capabilities.len(), 3);
    }

    #[test]
    fn docker_style_mixed_case_normalization() {
        // Users might use inconsistent casing
        let mut config = MockContainerConfig::new();
        config.add_caps(["net_raw", "NET_ADMIN", "Cap_Chown"]);

        assert!(config.validate().is_ok());
        // All should be normalized to canonical form
        assert!(config.add_capabilities.iter().all(|c| c.starts_with("CAP_")));
        assert!(config.add_capabilities.iter().all(|c| c == &c.to_uppercase()));
    }

    // -------------------------------------------------------------------------
    // Kubernetes-style configuration tests
    // -------------------------------------------------------------------------

    #[test]
    fn kubernetes_style_add_capabilities() {
        // Kubernetes securityContext.capabilities.add
        let k8s_caps = ["NET_RAW", "NET_ADMIN", "SYS_TIME"];
        let mut config = MockContainerConfig::new();
        config.add_caps(k8s_caps);

        assert!(config.validate().is_ok());
        assert_eq!(config.add_capabilities.len(), 3);
    }

    #[test]
    fn kubernetes_style_drop_capabilities() {
        // Kubernetes securityContext.capabilities.drop
        let k8s_drop = ["ALL"]; // Special case - would need ALL expansion
        let mut config = MockContainerConfig::new();

        // Since "ALL" is not a real capability, it would fail validation
        // This shows the module correctly rejects invalid capabilities
        config.drop_caps(k8s_drop);
        assert!(config.validate().is_err());
    }

    // -------------------------------------------------------------------------
    // Privileged mode detection tests
    // -------------------------------------------------------------------------

    #[test]
    fn detects_non_privileged_configuration() {
        let mut config = MockContainerConfig::new();
        config.add_caps(["NET_RAW", "NET_ADMIN", "NET_BIND_SERVICE"]);

        assert!(config.validate().is_ok());
        assert!(!config.requires_privileged(), "Only networking caps should not require privileged");
        assert!(config.privileged_caps().is_empty());
    }

    #[test]
    fn detects_privileged_configuration() {
        let mut config = MockContainerConfig::new();
        config.add_caps(["NET_RAW", "SYS_ADMIN", "CHOWN"]);

        assert!(config.validate().is_ok());
        assert!(config.requires_privileged(), "SYS_ADMIN and CHOWN require privileged");

        let privileged = config.privileged_caps();
        assert!(privileged.contains(&"CAP_SYS_ADMIN".to_string()));
        assert!(privileged.contains(&"CAP_CHOWN".to_string()));
        assert!(!privileged.contains(&"CAP_NET_RAW".to_string()));
    }

    // -------------------------------------------------------------------------
    // Effective capability computation tests
    // -------------------------------------------------------------------------

    #[test]
    fn computes_effective_capabilities() {
        let mut config = MockContainerConfig::new();
        config.add_caps(["NET_RAW", "NET_ADMIN", "CHOWN", "SETUID"]);
        config.drop_caps(["CHOWN", "SETUID"]);

        let effective = config.effective_capabilities();
        assert_eq!(effective.len(), 2);
        assert!(effective.contains(&"CAP_NET_RAW".to_string()));
        assert!(effective.contains(&"CAP_NET_ADMIN".to_string()));
        assert!(!effective.contains(&"CAP_CHOWN".to_string()));
    }

    #[test]
    fn effective_caps_handles_duplicates() {
        let mut config = MockContainerConfig::new();
        // Add same cap in different formats
        config.add_caps(["NET_RAW", "cap_net_raw", "CAP_NET_RAW"]);

        // Should deduplicate
        assert_eq!(config.add_capabilities.len(), 1);

        let effective = config.effective_capabilities();
        assert_eq!(effective.len(), 1);
        assert_eq!(effective[0], "CAP_NET_RAW");
    }

    // -------------------------------------------------------------------------
    // Validation error handling tests
    // -------------------------------------------------------------------------

    #[test]
    fn validates_unknown_capability_in_add() {
        let mut config = MockContainerConfig::new();
        config.add_caps(["NET_RAW", "UNKNOWN_CAP"]);

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("UNKNOWN_CAP"));
    }

    #[test]
    fn validates_unknown_capability_in_drop() {
        let mut config = MockContainerConfig::new();
        config.add_caps(["NET_RAW"]);
        config.drop_caps(["INVALID_CAP"]);

        let result = config.validate();
        assert!(result.is_err());
    }

    // -------------------------------------------------------------------------
    // OCI bundle simulation tests
    // -------------------------------------------------------------------------

    #[test]
    fn simulates_oci_bundle_capability_config() {
        // Simulates OCI runtime spec capabilities structure
        struct OciCapabilities {
            bounding: Vec<String>,
            effective: Vec<String>,
            inheritable: Vec<String>,
            permitted: Vec<String>,
            ambient: Vec<String>,
        }

        fn normalize_oci_caps(caps: &[&str]) -> Vec<String> {
            caps.iter()
                .map(|c| normalize_capability_name(c))
                .filter(|c| !c.is_empty())
                .collect()
        }

        let input_caps = ["CAP_NET_RAW", "cap_chown", "NET_ADMIN"];

        let oci = OciCapabilities {
            bounding: normalize_oci_caps(&input_caps),
            effective: normalize_oci_caps(&input_caps),
            inheritable: normalize_oci_caps(&["NET_RAW"]),
            permitted: normalize_oci_caps(&input_caps),
            ambient: Vec::new(),
        };

        // All capabilities should be normalized
        assert!(oci.bounding.iter().all(|c| c.starts_with("CAP_")));
        assert!(oci.effective.iter().all(|c| c.starts_with("CAP_")));
        assert!(oci.permitted.iter().all(|c| c.starts_with("CAP_")));
        assert!(oci.inheritable.iter().all(|c| c.starts_with("CAP_")));
        assert!(oci.ambient.is_empty()); // ambient typically empty for non-root

        // Validate all are known capabilities
        for cap in &oci.bounding {
            assert!(is_valid_capability(cap), "OCI bounding cap {} should be valid", cap);
        }
        for cap in &oci.inheritable {
            assert!(is_valid_capability(cap), "OCI inheritable cap {} should be valid", cap);
        }
    }

    // -------------------------------------------------------------------------
    // Preflight validation workflow tests
    // -------------------------------------------------------------------------

    #[test]
    fn preflight_validation_accepts_valid_config() {
        // Simulates preflight validation before container start
        let requested_caps = ["NET_RAW", "NET_ADMIN", "CHOWN"];

        // Step 1: Validate all capabilities are known
        let validation_result = validate_capabilities(requested_caps.iter().copied());
        assert!(validation_result.is_ok());

        // Step 2: Check if privileged mode is needed
        let normalized = validation_result.unwrap();
        let privileged_required = !capabilities_are_non_privileged(normalized.iter().map(|s| s.as_str()));
        assert!(privileged_required, "CHOWN requires privileged");

        // Step 3: Get list of privileged capabilities for warning/error
        let privileged_caps = capabilities_requiring_privileged(normalized.iter().map(|s| s.as_str()));
        assert_eq!(privileged_caps, vec!["CAP_CHOWN"]);
    }

    #[test]
    fn preflight_validation_rejects_invalid_config() {
        let requested_caps = ["NET_RAW", "INVALID_CAP", "CHOWN"];

        let validation_result = validate_capabilities(requested_caps.iter().copied());
        assert!(validation_result.is_err());

        let err = validation_result.unwrap_err();
        assert!(err.to_string().contains("INVALID_CAP") || err.to_string().contains("unknown"));
    }

    // -------------------------------------------------------------------------
    // Capability comparison for security policy tests
    // -------------------------------------------------------------------------

    #[test]
    fn compare_requested_vs_allowed_capabilities() {
        let allowed_caps = ["CAP_NET_RAW", "CAP_NET_ADMIN", "CAP_NET_BIND_SERVICE"];
        let requested_caps = ["net_raw", "NET_ADMIN", "cap-sys-admin"];

        // Check if requested is subset of allowed
        let is_subset = capability_set_is_subset(
            requested_caps.iter().copied(),
            allowed_caps.iter().copied(),
        );
        assert!(!is_subset, "SYS_ADMIN is not in allowed set");

        // Find what's not allowed
        let not_allowed = capability_set_difference(
            requested_caps.iter().copied(),
            allowed_caps.iter().copied(),
        );
        assert_eq!(not_allowed, vec!["CAP_SYS_ADMIN"]);
    }

    #[test]
    fn compare_equivalent_capability_sets() {
        let set_a = ["CAP_NET_RAW", "CAP_CHOWN"];
        let set_b = ["cap_chown", "net_raw"];

        assert!(capability_sets_equal(
            set_a.iter().copied(),
            set_b.iter().copied(),
        ));
    }
}
