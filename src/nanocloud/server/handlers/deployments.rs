use super::error::ApiError;
use super::format::{respond_with, OutputFormat};
use super::jobs::{StatusBody, StatusDetails};
use super::watch::{
    ensure_resource_version_match, parse_resource_version, resource_version_is_newer,
    ResourceVersionMatchPolicy, WatchEventLike, WatchStreamBuilder,
};
use crate::nanocloud::controller::delete::DeletionPropagation;
use crate::nanocloud::controller::deployment::{DeploymentController, DeploymentDesiredState};
use crate::nanocloud::controller::replicaset::LABEL_REPLICASET_NAME;
use crate::nanocloud::controller::watch::{ControllerWatchEvent, ControllerWatchManager};
use crate::nanocloud::k8s::deployment::{
    Deployment, DeploymentList, DeploymentSpec, DeploymentStrategy, DeploymentStrategyType,
    RollingUpdateDeployment,
};
use crate::nanocloud::k8s::pod::ObjectMeta;
use crate::nanocloud::k8s::statefulset::{LabelSelector, PodTemplateSpec};
use crate::nanocloud::k8s::store::{
    delete_deployment, get_deployment, list_deployments_for, normalize_namespace,
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

fn build_list(items: Vec<Deployment>) -> DeploymentList {
    DeploymentList::from_items(items)
}

#[derive(Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct DeleteOptionsPayload {
    #[serde(rename = "propagationPolicy")]
    propagation_policy: Option<String>,
}

const DEPLOYMENT_WATCH_COMPONENT: &str = "deployment-watch";
const DEPLOYMENT_WATCH_BUFFER: usize = 32;
const DEPLOYMENT_KEYSPACE_PREFIX: &str = "/deployments";

#[derive(Clone)]
struct DeploymentWatchEvent {
    event_type: String,
    object: Deployment,
}

impl WatchEventLike for DeploymentWatchEvent {
    type Object = Deployment;

    fn into_parts(self) -> (String, Self::Object) {
        (self.event_type, self.object)
    }
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/apps/v1/deployments",
    params(("format" = Option<String>, Query, description = "Output format (json|yaml)")),
    responses(
        (status = 200, description = "Deployment list", body = DeploymentList),
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
        return watch_deployments(None, &params);
    }
    let output = OutputFormat::negotiate(params.format.as_deref(), &headers);
    let items = list_deployments_for(None).map_err(ApiError::internal_error)?;
    respond_with(build_list(items), output)
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/apps/v1/namespaces/{namespace}/deployments",
    params(
        ("namespace" = String, Path, description = "Namespace to query"),
        ("format" = Option<String>, Query, description = "Output format (json|yaml)")
    ),
    responses(
        (status = 200, description = "Deployment list", body = DeploymentList),
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
        return watch_deployments(Some(namespace.as_str()), &params);
    }
    let output = OutputFormat::negotiate(params.format.as_deref(), &headers);
    let items = list_deployments_for(Some(namespace.as_str())).map_err(ApiError::internal_error)?;
    respond_with(build_list(items), output)
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/apps/v1/namespaces/{namespace}/deployments/{name}",
    params(
        ("namespace" = String, Path, description = "Namespace of the Deployment"),
        ("name" = String, Path, description = "Deployment name"),
        ("format" = Option<String>, Query, description = "Output format (json|yaml)")
    ),
    responses(
        (status = 200, description = "Deployment", body = Deployment),
        (
            status = 404,
            description = "Deployment not found",
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
            "watch is not supported for individual Deployment resources",
        ));
    }
    let output = OutputFormat::negotiate(params.format.as_deref(), &headers);
    let resource =
        get_deployment(Some(namespace.as_str()), &name).map_err(ApiError::internal_error)?;
    match resource {
        Some(deployment) => respond_with(deployment, output),
        None => Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("Deployment '{name}' not found"),
        )),
    }
}

#[cfg_attr(feature = "openapi", utoipa::path(
    delete,
    path = "/apis/apps/v1/namespaces/{namespace}/deployments/{name}",
    params(
        ("namespace" = String, Path, description = "Namespace of the Deployment"),
        ("name" = String, Path, description = "Deployment name")
    ),
    request_body = DeleteOptionsPayload,
    responses(
        (status = 200, description = "Deployment deletion accepted", body = StatusBody),
        (status = 404, description = "Deployment not found", body = crate::nanocloud::api::types::ErrorBody),
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
        get_deployment(Some(namespace.as_str()), &name).map_err(ApiError::internal_error)?;
    if workload.is_none() {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("Deployment '{name}' not found"),
        ));
    }

    let controller = DeploymentController::new(name.clone(), Some(namespace.clone()));
    let desired_state = controller
        .desired_state()
        .map_err(|err| ApiError::internal_error(Box::new(err)))?;
    let kubelet = Kubelet::shared();
    let pods_to_delete = if propagation.cascades() {
        collect_deployment_pods_for_deletion(
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

    delete_deployment(Some(namespace.as_str()), &name).map_err(ApiError::internal_error)?;

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

async fn collect_deployment_pods_for_deletion(
    kubelet: &Arc<Kubelet>,
    namespace: &str,
    deployment: &str,
    desired: Option<&DeploymentDesiredState>,
) -> Result<Vec<String>, ApiError> {
    let mut replicaset_targets: HashSet<String> = HashSet::new();
    if let Some(state) = desired {
        if !state.active_replicaset.is_empty() {
            replicaset_targets.insert(state.active_replicaset.clone());
        }
        for plan in &state.replica_sets {
            if !plan.name.is_empty() {
                replicaset_targets.insert(plan.name.clone());
            }
        }
        for record in &state.revision_history {
            if !record.replicaset.is_empty() {
                replicaset_targets.insert(record.replicaset.clone());
            }
        }
        for pruned in &state.pruned_replicasets {
            if !pruned.is_empty() {
                replicaset_targets.insert(pruned.clone());
            }
        }
    }

    let pods = kubelet
        .list_pods(Some(namespace))
        .await
        .map_err(ApiError::internal_error)?;

    let mut targets: HashSet<String> = HashSet::new();
    for pod in &pods {
        if let Some(label) = pod.metadata.labels.get(LABEL_REPLICASET_NAME) {
            if !replicaset_targets.is_empty() {
                if replicaset_targets.contains(label) {
                    if let Some(pod_name) = pod.metadata.name.as_ref() {
                        targets.insert(pod_name.clone());
                    }
                }
            } else if label.starts_with(deployment) {
                if let Some(pod_name) = pod.metadata.name.as_ref() {
                    targets.insert(pod_name.clone());
                }
            }
        }
    }

    if replicaset_targets.is_empty() {
        for pod in pods {
            if let Some(label) = pod.metadata.labels.get(LABEL_REPLICASET_NAME) {
                if label.starts_with(deployment) {
                    if let Some(pod_name) = pod.metadata.name {
                        targets.insert(pod_name);
                    }
                }
            }
        }
    }

    Ok(targets.into_iter().collect())
}

fn watch_deployments(namespace: Option<&str>, params: &ListParams) -> Result<Response, ApiError> {
    if params.resource_version_match.is_some() && params.resource_version.is_none() {
        return Err(ApiError::bad_request(
            "resourceVersionMatch requires resourceVersion to be set",
        ));
    }

    let resource_version_threshold = parse_resource_version(params.resource_version.as_deref())?;
    let allow_bookmarks = params.allow_watch_bookmarks.unwrap_or(false);
    let timeout = params.timeout_seconds.map(Duration::from_secs);

    let mut items = list_deployments_for(namespace).map_err(ApiError::internal_error)?;
    let current_resource_version = items
        .iter()
        .filter_map(|deployment| {
            deployment
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

    let initial_events: Vec<DeploymentWatchEvent> = items
        .drain(..)
        .filter(|workload| resource_version_is_newer(workload, resource_version_threshold))
        .map(|mut workload| {
            sanitize_deployment(&mut workload);
            DeploymentWatchEvent {
                event_type: "ADDED".to_string(),
                object: workload,
            }
        })
        .collect();

    let mut subscription =
        ControllerWatchManager::shared().subscribe(DEPLOYMENT_KEYSPACE_PREFIX, namespace);
    let (sender, receiver) = broadcast::channel(DEPLOYMENT_WATCH_BUFFER);
    tokio::spawn(async move {
        while let Some(event) = subscription.recv().await {
            if let Some(mut watch_event) = deployment_watch_event_from_controller(event) {
                if resource_version_is_newer(&watch_event.object, resource_version_threshold) {
                    sanitize_deployment(&mut watch_event.object);
                    if sender.send(watch_event).is_err() {
                        break;
                    }
                }
            }
        }
    });

    let body = WatchStreamBuilder::new(
        DEPLOYMENT_WATCH_COMPONENT,
        "Deployment watch serialization error",
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

fn sanitize_deployment(workload: &mut Deployment) {
    if workload.api_version.is_empty() {
        workload.api_version = "apps/v1".to_string();
    }
    if workload.kind.is_empty() {
        workload.kind = "Deployment".to_string();
    }
    if workload.metadata.resource_version.is_none() {
        workload.metadata.resource_version = Some("1".to_string());
    }
}

fn deployment_watch_event_from_controller(
    event: ControllerWatchEvent,
) -> Option<DeploymentWatchEvent> {
    let (namespace, name) = match parse_deployment_key(event.key.as_str()) {
        Some(parts) => parts,
        None => {
            log_warn(
                DEPLOYMENT_WATCH_COMPONENT,
                "Received Deployment event with unrecognized key",
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
        match serde_json::from_str::<Deployment>(&payload) {
            Ok(mut workload) => {
                sanitize_deployment(&mut workload);
                workload
            }
            Err(err) => {
                log_error(
                    DEPLOYMENT_WATCH_COMPONENT,
                    "Failed to deserialize Deployment payload for watch event",
                    &[
                        ("key", event.key.as_str()),
                        ("error", err.to_string().as_str()),
                    ],
                );
                return None;
            }
        }
    } else {
        Deployment {
            api_version: "apps/v1".to_string(),
            kind: "Deployment".to_string(),
            metadata: ObjectMeta {
                name: Some(name.clone()),
                namespace: None,
                labels: Default::default(),
                annotations: Default::default(),
                resource_version: Some(event.resource_version.to_string()),
            },
            spec: DeploymentSpec {
                replicas: 0,
                selector: LabelSelector::default(),
                template: PodTemplateSpec::default(),
                strategy: DeploymentStrategy {
                    r#type: DeploymentStrategyType::RollingUpdate,
                    rolling_update: Some(RollingUpdateDeployment {
                        max_surge: None,
                        max_unavailable: None,
                    }),
                },
                revision_history_limit: Some(0),
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

    Some(DeploymentWatchEvent {
        event_type: event_type.to_string(),
        object: workload,
    })
}

fn parse_deployment_key(key: &str) -> Option<(String, String)> {
    let trimmed = key.trim_matches('/');
    let mut parts = trimmed.splitn(3, '/');
    let prefix = parts.next()?;
    if prefix != DEPLOYMENT_KEYSPACE_PREFIX.trim_matches('/') {
        return None;
    }
    let namespace = parts.next().unwrap_or("default").to_string();
    let name = parts.next()?.to_string();
    Some((namespace, name))
}
