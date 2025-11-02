use super::error::ApiError;
use super::format::{respond_with, OutputFormat};
use super::jobs::{StatusBody, StatusDetails};
use super::watch::{
    ensure_resource_version_match, parse_resource_version, resource_version_is_newer,
    ResourceVersionMatchPolicy, WatchEventLike, WatchStreamBuilder,
};
use crate::nanocloud::controller::daemonset::{DaemonSetController, DaemonSetDesiredState};
use crate::nanocloud::controller::delete::DeletionPropagation;
use crate::nanocloud::controller::replicaset::LABEL_REPLICASET_NAME;
use crate::nanocloud::controller::watch::{ControllerWatchEvent, ControllerWatchManager};
use crate::nanocloud::k8s::daemonset::{
    DaemonSet, DaemonSetList, DaemonSetRollingUpdate, DaemonSetSpec, DaemonSetUpdateStrategy,
    DaemonSetUpdateStrategyType,
};
use crate::nanocloud::k8s::deployment::RollingUpdateValue;
use crate::nanocloud::k8s::pod::ObjectMeta;
use crate::nanocloud::k8s::statefulset::{LabelSelector, PodTemplateSpec};
use crate::nanocloud::k8s::store::{
    delete_daemonset, get_daemon_set, list_daemon_sets_for, normalize_namespace,
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

fn build_list(items: Vec<DaemonSet>) -> DaemonSetList {
    DaemonSetList::from_items(items)
}

#[derive(Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct DeleteOptionsPayload {
    #[serde(rename = "propagationPolicy")]
    propagation_policy: Option<String>,
}

const DAEMONSET_WATCH_COMPONENT: &str = "daemonset-watch";
const DAEMONSET_WATCH_BUFFER: usize = 32;
const DAEMONSET_KEYSPACE_PREFIX: &str = "/daemonsets";

#[derive(Clone)]
struct DaemonSetWatchEvent {
    event_type: String,
    object: DaemonSet,
}

impl WatchEventLike for DaemonSetWatchEvent {
    type Object = DaemonSet;

    fn into_parts(self) -> (String, Self::Object) {
        (self.event_type, self.object)
    }
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/apps/v1/daemonsets",
    params(("format" = Option<String>, Query, description = "Output format (json|yaml)")),
    responses(
        (status = 200, description = "DaemonSet list", body = DaemonSetList),
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
        return watch_daemonsets(None, &params);
    }
    let output = OutputFormat::negotiate(params.format.as_deref(), &headers);
    let items = list_daemon_sets_for(None).map_err(ApiError::internal_error)?;
    respond_with(build_list(items), output)
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/apps/v1/namespaces/{namespace}/daemonsets",
    params(
        ("namespace" = String, Path, description = "Namespace to query"),
        ("format" = Option<String>, Query, description = "Output format (json|yaml)")
    ),
    responses(
        (status = 200, description = "DaemonSet list", body = DaemonSetList),
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
        return watch_daemonsets(Some(namespace.as_str()), &params);
    }
    let output = OutputFormat::negotiate(params.format.as_deref(), &headers);
    let items = list_daemon_sets_for(Some(namespace.as_str())).map_err(ApiError::internal_error)?;
    respond_with(build_list(items), output)
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/apps/v1/namespaces/{namespace}/daemonsets/{name}",
    params(
        ("namespace" = String, Path, description = "Namespace of the DaemonSet"),
        ("name" = String, Path, description = "DaemonSet name"),
        ("format" = Option<String>, Query, description = "Output format (json|yaml)")
    ),
    responses(
        (status = 200, description = "DaemonSet", body = DaemonSet),
        (
            status = 404,
            description = "DaemonSet not found",
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
    if params.watch.unwrap_or(false) {
        return Err(ApiError::bad_request(
            "watch is not supported for individual DaemonSet resources",
        ));
    }
    let output = OutputFormat::negotiate(params.format.as_deref(), &headers);
    let resource =
        get_daemon_set(Some(namespace.as_str()), &name).map_err(ApiError::internal_error)?;
    match resource {
        Some(daemonset) => respond_with(daemonset, output),
        None => Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("DaemonSet '{name}' not found"),
        )),
    }
}

#[cfg_attr(feature = "openapi", utoipa::path(
    delete,
    path = "/apis/apps/v1/namespaces/{namespace}/daemonsets/{name}",
    params(
        ("namespace" = String, Path, description = "Namespace of the DaemonSet"),
        ("name" = String, Path, description = "DaemonSet name")
    ),
    request_body = DeleteOptionsPayload,
    responses(
        (status = 200, description = "DaemonSet deletion accepted", body = StatusBody),
        (status = 404, description = "DaemonSet not found", body = crate::nanocloud::api::types::ErrorBody),
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
        get_daemon_set(Some(namespace.as_str()), &name).map_err(ApiError::internal_error)?;
    if workload.is_none() {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("DaemonSet '{name}' not found"),
        ));
    }

    let controller = DaemonSetController::new(name.clone(), Some(namespace.clone()));
    let desired_state = controller
        .desired_state()
        .map_err(|err| ApiError::internal_error(Box::new(err)))?;
    let kubelet = Kubelet::shared();
    let pods_to_delete = if propagation.cascades() {
        collect_daemonset_pods_for_deletion(
            &kubelet,
            namespace.as_str(),
            name.as_str(),
            desired_state.as_ref(),
        )
        .await?
    } else {
        Vec::new()
    };

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

    delete_daemonset(Some(namespace.as_str()), &name).map_err(ApiError::internal_error)?;

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

async fn collect_daemonset_pods_for_deletion(
    kubelet: &Arc<Kubelet>,
    namespace: &str,
    daemonset: &str,
    desired: Option<&DaemonSetDesiredState>,
) -> Result<Vec<String>, ApiError> {
    let mut target_names: HashSet<String> = desired
        .map(|state| {
            state
                .nodes
                .iter()
                .filter_map(|plan| plan.pod_name.clone())
                .collect()
        })
        .unwrap_or_default();

    let pods = kubelet
        .list_pods(Some(namespace))
        .await
        .map_err(ApiError::internal_error)?;

    if target_names.is_empty() {
        for pod in &pods {
            if let Some(label) = pod.metadata.labels.get(LABEL_REPLICASET_NAME) {
                if label.starts_with(daemonset) {
                    if let Some(pod_name) = pod.metadata.name.as_ref() {
                        target_names.insert(pod_name.clone());
                    }
                }
            }
        }
    }

    let mut results: HashSet<String> = HashSet::new();
    for pod in pods {
        if let Some(pod_name) = pod.metadata.name {
            if target_names.contains(&pod_name) {
                results.insert(pod_name);
            }
        }
    }

    Ok(results.into_iter().collect())
}

fn watch_daemonsets(namespace: Option<&str>, params: &ListParams) -> Result<Response, ApiError> {
    if params.resource_version_match.is_some() && params.resource_version.is_none() {
        return Err(ApiError::bad_request(
            "resourceVersionMatch requires resourceVersion to be set",
        ));
    }

    let resource_version_threshold = parse_resource_version(params.resource_version.as_deref())?;
    let allow_bookmarks = params.allow_watch_bookmarks.unwrap_or(false);
    let timeout = params.timeout_seconds.map(Duration::from_secs);

    let mut items = list_daemon_sets_for(namespace).map_err(ApiError::internal_error)?;
    let current_resource_version = items
        .iter()
        .filter_map(|daemonset| {
            daemonset
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

    let initial_events: Vec<DaemonSetWatchEvent> = items
        .drain(..)
        .filter(|workload| resource_version_is_newer(workload, resource_version_threshold))
        .map(|mut workload| {
            sanitize_daemonset(&mut workload);
            DaemonSetWatchEvent {
                event_type: "ADDED".to_string(),
                object: workload,
            }
        })
        .collect();

    let mut subscription =
        ControllerWatchManager::shared().subscribe(DAEMONSET_KEYSPACE_PREFIX, namespace);
    let (sender, receiver) = broadcast::channel(DAEMONSET_WATCH_BUFFER);
    tokio::spawn(async move {
        while let Some(event) = subscription.recv().await {
            if let Some(mut watch_event) = daemonset_watch_event_from_controller(event) {
                if resource_version_is_newer(&watch_event.object, resource_version_threshold) {
                    sanitize_daemonset(&mut watch_event.object);
                    if sender.send(watch_event).is_err() {
                        break;
                    }
                }
            }
        }
    });

    let body = WatchStreamBuilder::new(
        DAEMONSET_WATCH_COMPONENT,
        "DaemonSet watch serialization error",
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

fn sanitize_daemonset(workload: &mut DaemonSet) {
    if workload.api_version.is_empty() {
        workload.api_version = "apps/v1".to_string();
    }
    if workload.kind.is_empty() {
        workload.kind = "DaemonSet".to_string();
    }
    if workload.metadata.resource_version.is_none() {
        workload.metadata.resource_version = Some("1".to_string());
    }
}

fn daemonset_watch_event_from_controller(
    event: ControllerWatchEvent,
) -> Option<DaemonSetWatchEvent> {
    let (namespace, name) = match parse_daemonset_key(event.key.as_str()) {
        Some(parts) => parts,
        None => {
            log_warn(
                DAEMONSET_WATCH_COMPONENT,
                "Received DaemonSet event with unrecognized key",
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
        match serde_json::from_str::<DaemonSet>(&payload) {
            Ok(mut workload) => {
                sanitize_daemonset(&mut workload);
                workload
            }
            Err(err) => {
                log_error(
                    DAEMONSET_WATCH_COMPONENT,
                    "Failed to deserialize DaemonSet payload for watch event",
                    &[
                        ("key", event.key.as_str()),
                        ("error", err.to_string().as_str()),
                    ],
                );
                return None;
            }
        }
    } else {
        DaemonSet {
            api_version: "apps/v1".to_string(),
            kind: "DaemonSet".to_string(),
            metadata: ObjectMeta {
                name: Some(name.clone()),
                namespace: None,
                labels: Default::default(),
                annotations: Default::default(),
                resource_version: Some(event.resource_version.to_string()),
            },
            spec: DaemonSetSpec {
                selector: LabelSelector::default(),
                template: PodTemplateSpec::default(),
                update_strategy: DaemonSetUpdateStrategy {
                    r#type: DaemonSetUpdateStrategyType::RollingUpdate,
                    rolling_update: Some(DaemonSetRollingUpdate {
                        max_unavailable: Some(RollingUpdateValue::Int(1)),
                    }),
                },
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

    Some(DaemonSetWatchEvent {
        event_type: event_type.to_string(),
        object: workload,
    })
}

fn parse_daemonset_key(key: &str) -> Option<(String, String)> {
    let trimmed = key.trim_matches('/');
    let mut parts = trimmed.splitn(3, '/');
    let prefix = parts.next()?;
    if prefix != DAEMONSET_KEYSPACE_PREFIX.trim_matches('/') {
        return None;
    }
    let namespace = parts.next().unwrap_or("default").to_string();
    let name = parts.next()?.to_string();
    Some((namespace, name))
}
