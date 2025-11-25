use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::nanocloud::api::types::{Bundle, BundleSnapshotSource, BundleSpec};
use crate::nanocloud::k8s::pod::ObjectMeta;

/// Common naming helpers for bundle resources.
pub(crate) fn service_display_name(namespace: Option<&str>, service: &str) -> String {
    namespace
        .filter(|value| !value.is_empty())
        .map(|ns| format!("{ns}-{service}"))
        .unwrap_or_else(|| service.to_string())
}

/// Kubernetes workload name for a service, including namespace prefix when provided.
pub(crate) fn workload_name(namespace: Option<&str>, service: &str) -> String {
    namespace
        .filter(|ns| !ns.is_empty())
        .map(|ns| format!("{}-{}", ns, service))
        .unwrap_or_else(|| service.to_string())
}

/// Build a bundle manifest payload with namespace defaults and optional snapshot.
pub(crate) fn bundle_payload(
    namespace: Option<&str>,
    service: &str,
    options: HashMap<String, String>,
    snapshot: Option<&str>,
    start: bool,
    update: bool,
) -> Bundle {
    let namespace_value = namespace.filter(|ns| !ns.is_empty());
    let path_namespace = namespace_value.unwrap_or("default");

    let metadata = ObjectMeta {
        name: Some(service.to_string()),
        namespace: Some(path_namespace.to_string()),
        ..Default::default()
    };

    let spec = BundleSpec {
        service: service.to_string(),
        namespace: namespace_value.map(|ns| ns.to_string()),
        options,
        profile_key: None,
        snapshot: snapshot.map(|path| BundleSnapshotSource {
            source: path.to_string(),
            media_type: None,
        }),
        start,
        update,
        security: None,
        runtime: None,
    };

    Bundle {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "Bundle".to_string(),
        metadata,
        spec,
        status: None,
    }
}

/// Derive a profile export path adjacent to a snapshot or in the working directory.
pub(crate) fn profile_export_path(snapshot_target: &Path, service: &str) -> PathBuf {
    let parent = snapshot_target
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| p.to_path_buf());
    let stem = snapshot_target
        .file_stem()
        .and_then(|s| {
            let value = s.to_string_lossy();
            if value.is_empty() {
                None
            } else {
                Some(value.into_owned())
            }
        })
        .unwrap_or_else(|| service.to_string());
    let file_name = format!("{}.profile.tar", stem);
    if let Some(dir) = parent {
        dir.join(&file_name)
    } else {
        PathBuf::from(file_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn display_and_workload_names_apply_namespace_prefix() {
        assert_eq!(service_display_name(None, "svc"), "svc");
        assert_eq!(service_display_name(Some("demo"), "svc"), "demo-svc");

        assert_eq!(workload_name(None, "svc"), "svc");
        assert_eq!(workload_name(Some("demo"), "svc"), "demo-svc");
    }

    #[test]
    fn bundle_payload_sets_defaults() {
        let mut options = HashMap::new();
        options.insert("size".into(), "small".into());
        let bundle =
            bundle_payload(Some("demo"), "svc", options.clone(), Some("snap.tar"), true, false);

        assert_eq!(bundle.metadata.namespace.as_deref(), Some("demo"));
        assert_eq!(bundle.metadata.name.as_deref(), Some("svc"));
        assert_eq!(bundle.spec.namespace.as_deref(), Some("demo"));
        assert_eq!(bundle.spec.service, "svc");
        assert_eq!(bundle.spec.options, options);
        assert!(bundle.spec.snapshot.is_some());
        assert!(bundle.spec.start);
        assert!(!bundle.spec.update);
    }

    #[test]
    fn bundle_payload_defaults_namespace_to_default_for_metadata() {
        let bundle = bundle_payload(None, "svc", HashMap::new(), None, false, true);
        assert_eq!(bundle.metadata.namespace.as_deref(), Some("default"));
        assert_eq!(bundle.spec.namespace, None);
        assert!(bundle.spec.snapshot.is_none());
        assert!(!bundle.spec.start);
        assert!(bundle.spec.update);
    }

    #[test]
    fn profile_export_path_prefers_snapshot_directory() {
        let path = profile_export_path(Path::new("/tmp/backups/svc.tar"), "svc");
        assert_eq!(path, PathBuf::from("/tmp/backups/svc.profile.tar"));

        let defaulted = profile_export_path(Path::new("svc.tar"), "svc");
        assert_eq!(defaulted, PathBuf::from("svc.profile.tar"));
    }
}
