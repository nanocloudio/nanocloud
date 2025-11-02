/// Capabilities that can be requested without toggling `allowPrivileged`.
pub const NON_PRIVILEGED_CAPABILITIES: &[&str] =
    &["CAP_NET_BIND_SERVICE", "CAP_NET_ADMIN", "CAP_NET_RAW"];

use std::collections::HashSet;

/// Normalizes capability names to the canonical `CAP_FOO_BAR` form.
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
