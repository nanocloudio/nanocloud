use super::error::ApiError;
use super::format::{respond_with, OutputFormat};
use super::jobs::{StatusBody, StatusDetails};
use super::watch::{
    ensure_resource_version_match, parse_resource_version, resource_version_is_newer,
    ResourceVersionMatchPolicy, WatchEventLike, WatchStreamBuilder,
};
use crate::nanocloud::controller::delete::DeletionPropagation;
use crate::nanocloud::controller::replicaset::LABEL_STATEFULSET_NAME;
use crate::nanocloud::controller::statefulset::StatefulSetController;
use crate::nanocloud::controller::watch::{ControllerWatchEvent, ControllerWatchManager};
use crate::nanocloud::k8s::pod::ObjectMeta;
use crate::nanocloud::k8s::statefulset::{
    LabelSelector, PodTemplateSpec, StatefulSet, StatefulSetList, StatefulSetSpec,
    StatefulSetUpdateStrategy,
};
use crate::nanocloud::k8s::store::{
    delete as delete_stateful_set, get_stateful_set, list_stateful_sets_for, normalize_namespace,
    save as save_stateful_set,
};
use crate::nanocloud::kubelet::Kubelet;
use crate::nanocloud::logger::{log_error, log_warn};
use crate::nanocloud::util::KeyspaceEventType;
use axum::extract::{Path, Query};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

#[derive(Default, Deserialize)]
pub struct ListParams {
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub watch: Option<bool>,
    #[serde(rename = "allowWatchBookmarks", default)]
    pub allow_watch_bookmarks: Option<bool>,
    #[serde(rename = "timeoutSeconds", default)]
    pub timeout_seconds: Option<u64>,
    #[serde(rename = "resourceVersion", default)]
    pub resource_version: Option<String>,
    #[serde(rename = "resourceVersionMatch", default)]
    pub resource_version_match: Option<ResourceVersionMatchPolicy>,
}

fn build_list(items: Vec<StatefulSet>) -> StatefulSetList {
    StatefulSetList::from_items(items)
}

#[derive(Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct DeleteOptionsPayload {
    #[serde(rename = "propagationPolicy")]
    propagation_policy: Option<String>,
}

const STATEFULSET_WATCH_COMPONENT: &str = "statefulset-watch";
const STATEFULSET_WATCH_BUFFER: usize = 32;
const STATEFULSET_KEYSPACE_PREFIX: &str = "/statefulsets";

#[derive(Clone)]
struct StatefulSetWatchEvent {
    event_type: String,
    object: StatefulSet,
}

impl WatchEventLike for StatefulSetWatchEvent {
    type Object = StatefulSet;

    fn into_parts(self) -> (String, Self::Object) {
        (self.event_type, self.object)
    }
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/apps/v1/statefulsets",
    params(("format" = Option<String>, Query, description = "Output format (json|yaml)")),
    responses(
        (status = 200, description = "StatefulSet list", body = StatefulSetList),
        (
            status = 500,
            description = "Internal error",
            body = crate::nanocloud::api::types::ErrorBody
        )
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["workloads.read"])
    ),
    tag = "kubernetes"
))]
pub async fn list_all(
    Query(params): Query<ListParams>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if params.watch.unwrap_or(false) {
        return watch_statefulsets(None, &params);
    }
    let output = OutputFormat::negotiate(params.format.as_deref(), &headers);
    let items = list_stateful_sets_for(None).map_err(ApiError::internal_error)?;
    respond_with(build_list(items), output)
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/apps/v1/namespaces/{namespace}/statefulsets",
    params(
        ("namespace" = String, Path, description = "Namespace to query"),
        ("format" = Option<String>, Query, description = "Output format (json|yaml)")
    ),
    responses(
        (status = 200, description = "StatefulSet list", body = StatefulSetList),
        (
            status = 500,
            description = "Internal error",
            body = crate::nanocloud::api::types::ErrorBody
        )
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["workloads.read"])
    ),
    tag = "kubernetes"
))]
pub async fn list_namespaced(
    Path(namespace): Path<String>,
    Query(params): Query<ListParams>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if params.watch.unwrap_or(false) {
        return watch_statefulsets(Some(namespace.as_str()), &params);
    }
    let output = OutputFormat::negotiate(params.format.as_deref(), &headers);
    let items =
        list_stateful_sets_for(Some(namespace.as_str())).map_err(ApiError::internal_error)?;
    respond_with(build_list(items), output)
}

fn watch_statefulsets(namespace: Option<&str>, params: &ListParams) -> Result<Response, ApiError> {
    if params.resource_version_match.is_some() && params.resource_version.is_none() {
        return Err(ApiError::bad_request(
            "resourceVersionMatch requires resourceVersion to be set",
        ));
    }

    let resource_version_threshold = parse_resource_version(params.resource_version.as_deref())?;
    let allow_bookmarks = params.allow_watch_bookmarks.unwrap_or(false);
    let timeout = params.timeout_seconds.map(Duration::from_secs);

    let mut items = list_stateful_sets_for(namespace).map_err(ApiError::internal_error)?;
    let current_resource_version = items
        .iter()
        .filter_map(|statefulset| {
            statefulset
                .metadata
                .resource_version
                .as_deref()
                .and_then(|value| value.parse::<u64>().ok())
        })
        .max();

    ensure_resource_version_match(
        params.resource_version_match,
        resource_version_threshold,
        current_resource_version,
    )?;

    let initial_events: Vec<StatefulSetWatchEvent> = items
        .drain(..)
        .filter(|workload| resource_version_is_newer(workload, resource_version_threshold))
        .map(|mut workload| {
            sanitize_statefulset(&mut workload);
            StatefulSetWatchEvent {
                event_type: "ADDED".to_string(),
                object: workload,
            }
        })
        .collect();

    let mut subscription =
        ControllerWatchManager::shared().subscribe(STATEFULSET_KEYSPACE_PREFIX, namespace);
    let (sender, receiver) = broadcast::channel(STATEFULSET_WATCH_BUFFER);
    tokio::spawn(async move {
        while let Some(event) = subscription.recv().await {
            if let Some(mut watch_event) = statefulset_watch_event_from_controller(event) {
                if resource_version_is_newer(&watch_event.object, resource_version_threshold) {
                    sanitize_statefulset(&mut watch_event.object);
                    if sender.send(watch_event).is_err() {
                        break;
                    }
                }
            }
        }
    });

    let body = WatchStreamBuilder::new(
        STATEFULSET_WATCH_COMPONENT,
        "StatefulSet watch serialization error",
        initial_events,
        receiver,
    )
    .with_bookmarks(allow_bookmarks)
    .with_timeout(timeout)
    .into_body();

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .map_err(|err| ApiError::internal_error(err.into()))
}

fn sanitize_statefulset(workload: &mut StatefulSet) {
    if workload.api_version.is_empty() {
        workload.api_version = "apps/v1".to_string();
    }
    if workload.kind.is_empty() {
        workload.kind = "StatefulSet".to_string();
    }
    if workload.metadata.resource_version.is_none() {
        workload.metadata.resource_version = Some("1".to_string());
    }
}

fn statefulset_watch_event_from_controller(
    event: ControllerWatchEvent,
) -> Option<StatefulSetWatchEvent> {
    let (namespace, name) = match parse_statefulset_key(event.key.as_str()) {
        Some(parts) => parts,
        None => {
            log_warn(
                STATEFULSET_WATCH_COMPONENT,
                "Received StatefulSet event with unrecognized key",
                &[("key", event.key.as_str())],
            );
            return None;
        }
    };

    let event_type = match event.event_type {
        KeyspaceEventType::Added => "ADDED",
        KeyspaceEventType::Modified => "MODIFIED",
        KeyspaceEventType::Deleted => "DELETED",
    };

    let mut workload = if let Some(payload) = event.value {
        match serde_json::from_str::<StatefulSet>(&payload) {
            Ok(mut workload) => {
                sanitize_statefulset(&mut workload);
                workload
            }
            Err(err) => {
                log_error(
                    STATEFULSET_WATCH_COMPONENT,
                    "Failed to deserialize StatefulSet payload for watch event",
                    &[
                        ("key", event.key.as_str()),
                        ("error", err.to_string().as_str()),
                    ],
                );
                return None;
            }
        }
    } else {
        StatefulSet {
            api_version: "apps/v1".to_string(),
            kind: "StatefulSet".to_string(),
            metadata: ObjectMeta {
                name: Some(name.clone()),
                namespace: None,
                labels: Default::default(),
                annotations: Default::default(),
                resource_version: Some(event.resource_version.to_string()),
            },
            spec: StatefulSetSpec {
                service_name: String::new(),
                replicas: 0,
                selector: LabelSelector::default(),
                template: PodTemplateSpec::default(),
                update_strategy: StatefulSetUpdateStrategy::default(),
                volume_claim_templates: Vec::new(),
            },
            status: None,
        }
    };

    if workload.metadata.name.is_none() {
        workload.metadata.name = Some(name.clone());
    }
    let normalized_ns = normalize_namespace(Some(namespace.as_str()));
    workload.metadata.namespace = Some(normalized_ns);
    workload.metadata.resource_version = Some(event.resource_version.to_string());

    Some(StatefulSetWatchEvent {
        event_type: event_type.to_string(),
        object: workload,
    })
}

fn parse_statefulset_key(key: &str) -> Option<(String, String)> {
    let trimmed = key.trim_matches('/');
    let mut parts = trimmed.splitn(3, '/');
    let prefix = parts.next()?;
    if prefix != STATEFULSET_KEYSPACE_PREFIX.trim_matches('/') {
        return None;
    }
    let namespace = parts.next().unwrap_or("default").to_string();
    let name = parts.next()?.to_string();
    Some((namespace, name))
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/apps/v1/namespaces/{namespace}/statefulsets/{name}",
    params(
        ("namespace" = String, Path, description = "Namespace of the StatefulSet"),
        ("name" = String, Path, description = "StatefulSet name"),
        ("format" = Option<String>, Query, description = "Output format (json|yaml)")
    ),
    responses(
        (status = 200, description = "StatefulSet", body = StatefulSet),
        (
            status = 404,
            description = "StatefulSet not found",
            body = crate::nanocloud::api::types::ErrorBody
        ),
        (
            status = 500,
            description = "Internal error",
            body = crate::nanocloud::api::types::ErrorBody
        )
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["workloads.read"])
    ),
    tag = "kubernetes"
))]
pub async fn get(
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<ListParams>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let output = OutputFormat::negotiate(params.format.as_deref(), &headers);
    let resource =
        get_stateful_set(Some(namespace.as_str()), &name).map_err(ApiError::internal_error)?;

    let statefulset = match resource {
        Some(workload) => workload,
        None => {
            return Err(ApiError::new(
                StatusCode::NOT_FOUND,
                format!("StatefulSet '{name}' not found"),
            ))
        }
    };

    respond_with(statefulset, output)
}

#[cfg_attr(feature = "openapi", utoipa::path(
    delete,
    path = "/apis/apps/v1/namespaces/{namespace}/statefulsets/{name}",
    params(
        ("namespace" = String, Path, description = "Namespace of the StatefulSet"),
        ("name" = String, Path, description = "StatefulSet name")
    ),
    request_body = DeleteOptionsPayload,
    responses(
        (status = 200, description = "StatefulSet deletion accepted", body = StatusBody),
        (status = 404, description = "StatefulSet not found", body = crate::nanocloud::api::types::ErrorBody),
        (status = 500, description = "Internal error", body = crate::nanocloud::api::types::ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["workloads.write"])
    ),
    tag = "kubernetes"
))]
pub async fn delete(
    Path((namespace, name)): Path<(String, String)>,
    options: Option<Json<DeleteOptionsPayload>>,
) -> Result<Json<StatusBody>, ApiError> {
    let propagation = match options
        .map(|payload| payload.0)
        .and_then(|payload| payload.propagation_policy)
    {
        Some(policy) => policy
            .parse::<DeletionPropagation>()
            .map_err(ApiError::bad_request)?,
        None => DeletionPropagation::default(),
    };

    let workload =
        get_stateful_set(Some(namespace.as_str()), &name).map_err(ApiError::internal_error)?;
    if workload.is_none() {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("StatefulSet '{name}' not found"),
        ));
    }

    let kubelet = Kubelet::shared();
    let pods_to_delete = if propagation.cascades() {
        collect_statefulset_pods_for_deletion(&kubelet, namespace.as_str(), name.as_str()).await?
    } else {
        Vec::new()
    };

    let controller = StatefulSetController::new(name.clone(), Some(namespace.clone()));
    controller
        .delete_with_propagation(propagation)
        .map_err(|err| ApiError::internal_error(Box::new(err)))?;

    if propagation.cascades() {
        for pod_name in pods_to_delete {
            kubelet
                .delete_pod(Some(namespace.as_str()), pod_name.as_str())
                .await
                .map_err(ApiError::internal_error)?;
        }
    }

    delete_stateful_set(Some(namespace.as_str()), &name).map_err(ApiError::internal_error)?;

    let normalized = normalize_namespace(Some(namespace.as_str()));
    let status = StatusBody {
        api_version: "v1".to_string(),
        kind: "Status".to_string(),
        status: "Success".to_string(),
        message: None,
        details: Some(StatusDetails {
            name,
            namespace: normalized,
        }),
    };

    Ok(Json(status))
}

async fn collect_statefulset_pods_for_deletion(
    kubelet: &Arc<Kubelet>,
    namespace: &str,
    statefulset: &str,
) -> Result<Vec<String>, ApiError> {
    let pods = kubelet
        .list_pods(Some(namespace))
        .await
        .map_err(ApiError::internal_error)?;

    let mut targets: HashSet<String> = HashSet::new();
    for pod in pods {
        if let Some(pod_name) = pod.metadata.name {
            if pod
                .metadata
                .labels
                .get(LABEL_STATEFULSET_NAME)
                .map(|value| value == statefulset)
                .unwrap_or(false)
            {
                targets.insert(pod_name);
            }
        }
    }

    Ok(targets.into_iter().collect())
}

fn ensure_template_selector_alignment(workload: &StatefulSet) -> Result<(), ApiError> {
    for (key, expected) in workload.spec.selector.match_labels.iter() {
        match workload
            .spec
            .template
            .metadata
            .labels
            .get(key)
            .filter(|value| *value == expected)
        {
            Some(_) => continue,
            None => {
                return Err(ApiError::bad_request(format!(
                    "spec.selector.matchLabels['{key}'] must match spec.template.metadata.labels"
                )))
            }
        }
    }
    Ok(())
}

fn ensure_metadata(namespace: &str, workload: &mut StatefulSet) -> Result<String, ApiError> {
    let normalized_ns = normalize_namespace(Some(namespace));
    if let Some(current_ns) = workload
        .metadata
        .namespace
        .clone()
        .filter(|value| !value.is_empty())
    {
        let current_normalized = normalize_namespace(Some(current_ns.as_str()));
        if current_normalized != normalized_ns {
            return Err(ApiError::bad_request(format!(
                "metadata.namespace '{}' does not match request namespace '{}'",
                current_ns, namespace
            )));
        }
    }
    workload.metadata.namespace = Some(normalized_ns);

    let name = workload
        .metadata
        .name
        .clone()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request("metadata.name is required"))?;

    Ok(name)
}

fn sanitize_workload(workload: &mut StatefulSet) -> Result<(), ApiError> {
    if workload.kind.is_empty() {
        workload.kind = "StatefulSet".to_string();
    } else if workload.kind != "StatefulSet" {
        return Err(ApiError::bad_request(format!(
            "kind '{}' is not supported; expected 'StatefulSet'",
            workload.kind
        )));
    }

    if workload.api_version.is_empty() {
        workload.api_version = "apps/v1".to_string();
    }

    if workload.spec.service_name.trim().is_empty() {
        return Err(ApiError::bad_request(
            "spec.serviceName must be a non-empty string",
        ));
    }

    if workload.spec.template.spec.containers.is_empty() {
        return Err(ApiError::bad_request(
            "spec.template.spec.containers must include at least one container",
        ));
    }

    workload.status = None;

    ensure_template_selector_alignment(workload)?;
    Ok(())
}

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/apis/apps/v1/namespaces/{namespace}/statefulsets",
    request_body = StatefulSet,
    params(
        ("namespace" = String, Path, description = "Namespace for the StatefulSet")
    ),
    responses(
        (status = 201, description = "StatefulSet created", body = StatefulSet),
        (
            status = 400,
            description = "Invalid StatefulSet",
            body = crate::nanocloud::api::types::ErrorBody
        ),
        (
            status = 500,
            description = "Internal error",
            body = crate::nanocloud::api::types::ErrorBody
        )
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["workloads.manage"])
    ),
    tag = "kubernetes"
))]
pub async fn create(
    Path(namespace): Path<String>,
    Json(mut payload): Json<StatefulSet>,
) -> Result<(StatusCode, Json<StatefulSet>), ApiError> {
    let name = ensure_metadata(namespace.as_str(), &mut payload)?;
    sanitize_workload(&mut payload)?;

    save_stateful_set(Some(namespace.as_str()), &name, &payload)
        .map_err(ApiError::internal_error)?;

    Ok((StatusCode::CREATED, Json(payload)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::pod::{ContainerSpec, ObjectMeta, PodSpec};
    use crate::nanocloud::k8s::statefulset::{
        LabelSelector, PodTemplateSpec, StatefulSetSpec, StatefulSetStatus,
    };
    use crate::nanocloud::test_support::{keyspace_lock, test_output_dir};
    use axum::extract::Path as AxumPath;
    use axum::http::StatusCode;
    use std::collections::HashMap;
    use std::env;
    use std::fs::{self, File};

    struct EnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: impl AsRef<std::path::Path>) -> Self {
            let previous = env::var(key).ok();
            env::set_var(key, value.as_ref());
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(ref value) = self.previous {
                env::set_var(self.key, value);
            } else {
                env::remove_var(self.key);
            }
        }
    }

    fn configure_keyspace(component: &str) -> (EnvGuard, EnvGuard) {
        let base = test_output_dir(component);
        let keyspace_dir = base.join("keyspace");
        let lock_dir = base.join("lock");
        fs::create_dir_all(&keyspace_dir).expect("keyspace directory");
        fs::create_dir_all(&lock_dir).expect("lock directory");
        let lock_file = lock_dir.join("nanocloud.lock");
        File::create(&lock_file).expect("lock file");

        let keyspace_guard = EnvGuard::set("NANOCLOUD_KEYSPACE", &keyspace_dir);
        let lock_guard = EnvGuard::set("NANOCLOUD_LOCK_FILE", &lock_file);
        (keyspace_guard, lock_guard)
    }

    fn sample_statefulset() -> StatefulSet {
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "block-test".to_string());

        let container = ContainerSpec {
            name: "blocker".to_string(),
            image: Some("registry.nanocloud.io/base".to_string()),
            command: vec!["block.sh".to_string()],
            ..Default::default()
        };

        let pod_spec = PodSpec {
            init_containers: Vec::new(),
            containers: vec![container],
            volumes: Vec::new(),
            restart_policy: None,
            service_account_name: None,
            node_name: None,
            host_network: false,
        };

        StatefulSet {
            api_version: "apps/v1".to_string(),
            kind: "StatefulSet".to_string(),
            metadata: ObjectMeta {
                name: Some("block-test".to_string()),
                namespace: None,
                labels: HashMap::new(),
                annotations: HashMap::new(),
                resource_version: None,
            },
            spec: StatefulSetSpec {
                service_name: "block-test".to_string(),
                replicas: 1,
                selector: LabelSelector {
                    match_labels: labels.clone(),
                },
                template: PodTemplateSpec {
                    metadata: ObjectMeta {
                        name: None,
                        namespace: None,
                        labels,
                        annotations: HashMap::new(),
                        resource_version: None,
                    },
                    spec: pod_spec,
                },
                update_strategy: Default::default(),
                volume_claim_templates: Vec::new(),
            },
            status: Some(StatefulSetStatus::default()),
        }
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)] // Test intentionally holds the keyspace lock to serialize filesystem-backed state.
    async fn create_persists_statefulset() {
        let _lock = keyspace_lock().lock();
        let (_keyspace_guard, _lock_guard) = configure_keyspace("statefulset-handler-create");

        let payload = sample_statefulset();

        let (status, Json(created)) = create(AxumPath("default".to_string()), Json(payload))
            .await
            .expect("create statefulset");

        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(created.metadata.namespace.as_deref(), Some("default"));
        assert!(created.status.is_none());

        let stored = get_stateful_set(Some("default"), "block-test")
            .expect("load stored statefulset")
            .expect("statefulset should exist");

        assert_eq!(stored.metadata.namespace.as_deref(), Some("default"));
        assert_eq!(stored.metadata.name.as_deref(), Some("block-test"));
        assert_eq!(
            stored
                .spec
                .template
                .metadata
                .labels
                .get("app")
                .map(String::as_str),
            Some("block-test")
        );

        let container = stored
            .spec
            .template
            .spec
            .containers
            .first()
            .expect("container");
        assert_eq!(container.name, "blocker");
        assert_eq!(container.command, vec!["block.sh".to_string()]);
    }
}
