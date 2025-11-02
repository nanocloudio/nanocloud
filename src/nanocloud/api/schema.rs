use std::collections::HashSet;
use std::sync::OnceLock;

use hex::encode as hex_encode;
use jsonschema::{
    error::{TypeKind, ValidationError, ValidationErrorKind},
    JSONSchema,
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::types::{Bundle, BundleSeccompProfileType};
use crate::nanocloud::security::profile::{normalize_capability_name, NON_PRIVILEGED_CAPABILITIES};

const BUNDLE_MANIFEST_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/docs/schema/bundle/manifest.json"
));
const BUNDLE_V1ALPHA1_SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/docs/schema/bundle/v1alpha1.json"
));

#[derive(Debug, Clone, Deserialize)]
struct RawManifest {
    resource: String,
    versions: Vec<RawVersion>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawVersion {
    version: String,
    #[serde(rename = "apiVersion")]
    api_version: String,
    status: String,
    schema: String,
    sha256: String,
    description: String,
    #[serde(rename = "releasedAt")]
    released_at: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct BundleSchemaDocument {
    pub version: String,
    pub api_version: String,
    pub status: String,
    pub description: String,
    pub released_at: String,
    pub sha256: String,
    pub json: &'static str,
}

static BUNDLE_SCHEMA_CACHE: OnceLock<Vec<BundleSchemaDocument>> = OnceLock::new();
struct CompiledBundleSchema {
    version: String,
    validator: JSONSchema,
}

static COMPILED_VALIDATORS: OnceLock<Vec<CompiledBundleSchema>> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct SchemaValidationError {
    pub instance_path: String,
    #[allow(dead_code)]
    pub schema_path: String,
    pub actual_type: Option<String>,
    pub message: String,
    pub kind: SchemaErrorKind,
}

#[derive(Debug, Clone)]
pub enum SchemaErrorKind {
    MissingProperty { property: String },
    AdditionalProperties { properties: Vec<String> },
    Type { expected: String },
    Pattern { pattern: String },
    MinLength { limit: u64 },
    MaxLength { limit: u64 },
    Minimum { limit: String },
    Maximum { limit: String },
    Enum { options: Vec<String> },
    Constraint(String),
}

#[derive(Debug, Clone)]
pub struct FriendlySchemaError {
    pub field: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub enum BundleSchemaError {
    UnsupportedVersion(String),
    Validation(Vec<SchemaValidationError>),
}

fn parse_manifest() -> Vec<BundleSchemaDocument> {
    let manifest: RawManifest =
        serde_json::from_str(BUNDLE_MANIFEST_JSON).expect("bundle schema manifest must be valid");
    if manifest.resource != "Bundle" {
        panic!(
            "expected Bundle resource manifest, got {}",
            manifest.resource
        );
    }

    manifest
        .versions
        .into_iter()
        .map(|entry| {
            let json = resolve_schema(&entry.schema)
                .unwrap_or_else(|| panic!("bundled schema '{}' missing", entry.schema));
            let digest = sha256(json.as_bytes());
            assert_eq!(
                digest, entry.sha256,
                "schema checksum mismatch for {}",
                entry.version
            );
            BundleSchemaDocument {
                version: entry.version,
                api_version: entry.api_version,
                status: entry.status,
                description: entry.description,
                released_at: entry.released_at,
                sha256: entry.sha256,
                json,
            }
        })
        .collect()
}

fn resolve_schema(path: &str) -> Option<&'static str> {
    match path {
        "bundle/v1alpha1.json" => Some(BUNDLE_V1ALPHA1_SCHEMA),
        _ => None,
    }
}

fn sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(hasher.finalize())
}

pub fn bundle_schemas() -> &'static [BundleSchemaDocument] {
    BUNDLE_SCHEMA_CACHE.get_or_init(parse_manifest).as_slice()
}

pub fn bundle_schema(version: &str) -> Option<&'static BundleSchemaDocument> {
    bundle_schemas()
        .iter()
        .find(|document| document.version == version)
}

fn compiled_bundle_validators() -> &'static [CompiledBundleSchema] {
    COMPILED_VALIDATORS.get_or_init(|| {
        bundle_schemas()
            .iter()
            .map(|doc| {
                let schema_value: Value = serde_json::from_str(doc.json)
                    .unwrap_or_else(|err| panic!("invalid schema json for {}: {err}", doc.version));
                let validator =
                    JSONSchema::options()
                        .compile(&schema_value)
                        .unwrap_or_else(|err| {
                            panic!("failed to compile schema {}: {err}", doc.version)
                        });
                CompiledBundleSchema {
                    version: doc.version.clone(),
                    validator,
                }
            })
            .collect()
    })
}

fn validator_for_version(version: &str) -> Option<&'static JSONSchema> {
    compiled_bundle_validators()
        .iter()
        .find(|compiled| compiled.version == version)
        .map(|compiled| &compiled.validator)
}

fn version_for_api(api_version: &str) -> Option<&'static str> {
    match api_version {
        "nanocloud.io/v1" | "" => Some("v1alpha1"),
        other => {
            // allow explicit schema versions later (nanocloud.io/v1alpha2 etc.)
            bundle_schema(other).map(|doc| doc.version.as_str())
        }
    }
}

pub fn validate_bundle(bundle: &Bundle) -> Result<(), BundleSchemaError> {
    let api_version = bundle.api_version.as_str();
    let schema_version = version_for_api(api_version)
        .ok_or_else(|| BundleSchemaError::UnsupportedVersion(api_version.to_string()))?;
    let validator = validator_for_version(schema_version)
        .ok_or_else(|| BundleSchemaError::UnsupportedVersion(api_version.to_string()))?;
    let value = serde_json::to_value(bundle).expect("Bundle should be serialisable");
    let result = validator.validate(&value);
    if let Err(errors) = result {
        let issues = errors.map(schema_error_from_validation).collect::<Vec<_>>();
        if issues.is_empty() {
            return Ok(());
        }
        return Err(BundleSchemaError::Validation(issues));
    }
    validate_security_profile(bundle)?;
    Ok(())
}

fn schema_error_from_validation(error: ValidationError<'_>) -> SchemaValidationError {
    let message = error.to_string();
    SchemaValidationError {
        instance_path: error.instance_path.to_string(),
        schema_path: error.schema_path.to_string(),
        actual_type: describe_actual_type(&error.instance),
        kind: map_error_kind(&error.kind, &message),
        message,
    }
}

fn validate_security_profile(bundle: &Bundle) -> Result<(), BundleSchemaError> {
    let Some(profile) = bundle.spec.security.as_ref() else {
        return Ok(());
    };
    let mut errors = Vec::new();
    let mut seen = HashSet::new();
    for (index, raw_cap) in profile.extra_capabilities.iter().enumerate() {
        let normalized = normalize_capability_name(raw_cap);
        let path = format!("/spec/security/extraCapabilities[{index}]");
        if normalized.is_empty() {
            errors.push(schema_constraint(
                &path,
                "Capability entries cannot be empty".to_string(),
            ));
            continue;
        }
        if !seen.insert(normalized.clone()) {
            errors.push(schema_constraint(
                &path,
                format!("Capability '{normalized}' declared multiple times"),
            ));
        }
        let allowed_without_priv = NON_PRIVILEGED_CAPABILITIES
            .iter()
            .any(|allowed| normalized == *allowed);
        if !profile.allow_privileged && !allowed_without_priv {
            errors.push(schema_constraint(
                &path,
                format!(
                    "Capability '{}' requires spec.security.allowPrivileged=true",
                    normalized
                ),
            ));
        }
    }

    if let Some(seccomp) = profile.seccomp_profile.as_ref() {
        let path = "/spec/security/seccompProfile/localhostProfile";
        match seccomp.profile_type {
            BundleSeccompProfileType::Localhost => {
                let value = seccomp
                    .localhost_profile
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty());
                if value.is_none() {
                    errors.push(schema_constraint(
                        path,
                        "localhostProfile must be set when seccomp type is Localhost",
                    ));
                }
            }
            _ => {
                if seccomp.localhost_profile.is_some() {
                    errors.push(schema_constraint(
                        path,
                        "localhostProfile is only valid when seccomp type is Localhost",
                    ));
                }
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(BundleSchemaError::Validation(errors))
    }
}

fn schema_constraint(path: impl Into<String>, message: impl Into<String>) -> SchemaValidationError {
    let msg = message.into();
    SchemaValidationError {
        instance_path: path.into(),
        schema_path: "".to_string(),
        actual_type: None,
        kind: SchemaErrorKind::Constraint(msg.clone()),
        message: msg,
    }
}

fn describe_actual_type(value: &Value) -> Option<String> {
    Some(
        match value {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(n) => {
                if n.is_i64() {
                    "integer"
                } else {
                    "number"
                }
            }
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        }
        .to_string(),
    )
}

fn literal_to_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.to_string(),
        _ => value.to_string(),
    }
}

fn map_error_kind(kind: &ValidationErrorKind, fallback: &str) -> SchemaErrorKind {
    match kind {
        ValidationErrorKind::Required { property } => SchemaErrorKind::MissingProperty {
            property: literal_to_string(property),
        },
        ValidationErrorKind::AdditionalProperties { unexpected }
        | ValidationErrorKind::UnevaluatedProperties { unexpected } => {
            SchemaErrorKind::AdditionalProperties {
                properties: unexpected.clone(),
            }
        }
        ValidationErrorKind::Type { kind } => SchemaErrorKind::Type {
            expected: describe_type_kind(kind),
        },
        ValidationErrorKind::Pattern { pattern } => SchemaErrorKind::Pattern {
            pattern: pattern.clone(),
        },
        ValidationErrorKind::MinLength { limit } => SchemaErrorKind::MinLength { limit: *limit },
        ValidationErrorKind::MaxLength { limit } => SchemaErrorKind::MaxLength { limit: *limit },
        ValidationErrorKind::Minimum { limit } => SchemaErrorKind::Minimum {
            limit: limit.to_string(),
        },
        ValidationErrorKind::Maximum { limit } => SchemaErrorKind::Maximum {
            limit: limit.to_string(),
        },
        ValidationErrorKind::Enum { options } => SchemaErrorKind::Enum {
            options: options
                .as_array()
                .map(|entries| entries.iter().map(literal_to_string).collect())
                .unwrap_or_else(|| vec![literal_to_string(options)]),
        },
        ValidationErrorKind::Format { format } => {
            SchemaErrorKind::Constraint(format!("must match format {}", format))
        }
        _ => SchemaErrorKind::Constraint(fallback.to_string()),
    }
}

fn describe_type_kind(kind: &TypeKind) -> String {
    match kind {
        TypeKind::Single(primitive) => primitive.to_string(),
        TypeKind::Multiple(types) => {
            let names: Vec<String> = (*types)
                .into_iter()
                .map(|primitive| primitive.to_string())
                .collect();
            names.join(", ")
        }
    }
}

fn decode_pointer(pointer: &str) -> Vec<String> {
    if pointer.is_empty() {
        return Vec::new();
    }
    pointer
        .trim_start_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            segment
                .replace("~1", "/")
                .replace("~0", "~")
                .replace('\"', "")
        })
        .collect()
}

fn pointer_to_field(pointer: &str) -> String {
    let tokens = decode_pointer(pointer);
    if tokens.is_empty() {
        return String::new();
    }
    let mut result = String::new();
    for token in tokens {
        if result.is_empty() {
            result.push_str(&token);
        } else if token.chars().all(|c| c.is_ascii_digit()) {
            result.push_str(&format!("[{token}]"));
        } else {
            result.push('.');
            result.push_str(&token);
        }
    }
    result
}

fn friendly_field(issue: &SchemaValidationError) -> String {
    match &issue.kind {
        SchemaErrorKind::MissingProperty { property } => {
            let base = pointer_to_field(&issue.instance_path);
            if base.is_empty() {
                property.clone()
            } else {
                format!("{base}.{property}")
            }
        }
        _ => {
            let base = pointer_to_field(&issue.instance_path);
            if base.is_empty() {
                "spec".to_string()
            } else {
                base
            }
        }
    }
}

fn friendly_message(issue: &SchemaValidationError) -> String {
    match &issue.kind {
        SchemaErrorKind::MissingProperty { property } => {
            format!("'{property}' is required")
        }
        SchemaErrorKind::AdditionalProperties { properties } => {
            if properties.is_empty() {
                "unknown field(s) present".to_string()
            } else {
                let joined = properties
                    .iter()
                    .map(|prop| format!("'{}'", prop))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("unknown field(s): {joined}")
            }
        }
        SchemaErrorKind::Type { expected } => {
            if let Some(actual) = issue.actual_type.as_deref() {
                format!("expected {expected}, got {actual}")
            } else {
                format!("expected {expected}")
            }
        }
        SchemaErrorKind::Pattern { pattern } => {
            format!("must match pattern {pattern}")
        }
        SchemaErrorKind::MinLength { limit } => {
            let plural = if *limit == 1 { "" } else { "s" };
            format!("must be at least {limit} character{plural}")
        }
        SchemaErrorKind::MaxLength { limit } => {
            let plural = if *limit == 1 { "" } else { "s" };
            format!("must be at most {limit} character{plural}")
        }
        SchemaErrorKind::Minimum { limit } => format!("must be greater than or equal to {limit}"),
        SchemaErrorKind::Maximum { limit } => format!("must be less than or equal to {limit}"),
        SchemaErrorKind::Enum { options } => {
            let joined = options.join(", ");
            format!("must be one of: {joined}")
        }
        SchemaErrorKind::Constraint(details) => {
            if details.is_empty() {
                issue.message.clone()
            } else {
                details.clone()
            }
        }
    }
}

pub fn friendly_bundle_errors(errors: &[SchemaValidationError]) -> Vec<FriendlySchemaError> {
    errors
        .iter()
        .map(|issue| FriendlySchemaError {
            field: friendly_field(issue),
            message: friendly_message(issue),
        })
        .collect()
}

pub fn format_bundle_error_summary(errors: &[SchemaValidationError]) -> String {
    let friendly = friendly_bundle_errors(errors);
    if friendly.is_empty() {
        "Bundle failed schema validation".to_string()
    } else {
        let details = friendly
            .iter()
            .map(|entry| format!("- {}: {}", entry.field, entry.message))
            .collect::<Vec<_>>()
            .join("\n");
        format!("Bundle failed schema validation:\n{details}")
    }
}
