use std::collections::HashMap;

use nanocloud::nanocloud::api::schema::{
    format_bundle_error_summary, validate_bundle, BundleSchemaError,
};
use nanocloud::nanocloud::api::types::{
    Bundle, BundleSeccompProfile, BundleSeccompProfileType, BundleSecurityProfile, BundleSpec,
};
use nanocloud::nanocloud::k8s::pod::ObjectMeta;

fn sample_bundle() -> Bundle {
    Bundle {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "Bundle".to_string(),
        metadata: ObjectMeta {
            name: Some("demo".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: BundleSpec {
            service: "demo".to_string(),
            namespace: None,
            options: HashMap::new(),
            profile_key: None,
            snapshot: None,
            start: true,
            update: false,
            security: None,
        },
        status: None,
    }
}

fn expect_validation_error(bundle: &Bundle) -> String {
    match validate_bundle(bundle) {
        Err(BundleSchemaError::Validation(issues)) => format_bundle_error_summary(&issues),
        other => panic!("expected schema validation error, got {:?}", other),
    }
}

#[test]
fn schema_rejects_missing_metadata_name() {
    let mut bundle = sample_bundle();
    bundle.metadata.name = None;

    let summary = expect_validation_error(&bundle);
    assert!(
        summary.contains("metadata.name"),
        "summary should mention metadata.name: {summary}"
    );
    assert!(
        summary.contains("required"),
        "summary should describe missing field: {summary}"
    );
}

#[test]
fn schema_rejects_invalid_option_keys() {
    let mut bundle = sample_bundle();
    bundle
        .spec
        .options
        .insert("UPPERCASE".to_string(), "value".to_string());

    let summary = expect_validation_error(&bundle);
    assert!(
        summary.contains("spec.options.UPPERCASE"),
        "summary should pinpoint the invalid option key: {summary}"
    );
    assert!(
        summary.contains("unknown field"),
        "summary should describe the reason: {summary}"
    );
}

#[test]
fn schema_rejects_privileged_capabilities_without_gate() {
    let mut bundle = sample_bundle();
    bundle.spec.security = Some(BundleSecurityProfile {
        allow_privileged: false,
        extra_capabilities: vec!["CAP_SYS_ADMIN".to_string()],
        ..Default::default()
    });

    let summary = expect_validation_error(&bundle);
    assert!(
        summary.contains("CAP_SYS_ADMIN"),
        "summary should mention the offending capability: {summary}"
    );
    assert!(
        summary.contains("allowPrivileged"),
        "summary should instruct how to resolve the issue: {summary}"
    );
}

#[test]
fn schema_accepts_privileged_capabilities_when_gate_enabled() {
    let mut bundle = sample_bundle();
    bundle.spec.security = Some(BundleSecurityProfile {
        allow_privileged: true,
        extra_capabilities: vec!["cap_sys_admin".to_string()],
        ..Default::default()
    });

    validate_bundle(&bundle).expect("privileged capability should be allowed when gated");
}

#[test]
fn schema_requires_localhost_profile_value() {
    let mut bundle = sample_bundle();
    bundle.spec.security = Some(BundleSecurityProfile {
        seccomp_profile: Some(BundleSeccompProfile {
            profile_type: BundleSeccompProfileType::Localhost,
            localhost_profile: None,
        }),
        ..Default::default()
    });

    let summary = expect_validation_error(&bundle);
    assert!(
        summary.contains("localhostProfile"),
        "summary should mention missing localhost profile: {summary}"
    );
}
