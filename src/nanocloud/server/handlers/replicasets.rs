use super::error::ApiError;
use super::format::{respond_with, OutputFormat};
use super::watch::{
    ensure_resource_version_match, parse_resource_version, resource_version_is_newer,
    ResourceVersionMatchPolicy, WatchEventLike, WatchStreamBuilder,
};
use crate::nanocloud::controller::replicaset::{ReplicaSetDesiredState, LABEL_REPLICASET_NAME};
use crate::nanocloud::controller::watch::{ControllerWatchEvent, ControllerWatchManager};
use crate::nanocloud::k8s::pod::ObjectMeta;
use crate::nanocloud::k8s::replicaset::{ReplicaSet, ReplicaSetList, ReplicaSetSpec};
use crate::nanocloud::k8s::statefulset::{LabelSelector, PodTemplateSpec};
use crate::nanocloud::k8s::store::{
    get_replica_set, list_replica_sets, normalize_namespace, replicaset_from_desired_state,
};
use crate::nanocloud::logger::{log_error, log_warn};
use crate::nanocloud::util::KeyspaceEventType;
use axum::extract::{Path, Query};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use serde::Deserialize;
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

fn build_list(items: Vec<ReplicaSet>) -> ReplicaSetList {
    ReplicaSetList::from_items(items)
}

const REPLICASET_WATCH_COMPONENT: &str = "replicaset-watch";
const REPLICASET_WATCH_BUFFER: usize = 32;
const REPLICASET_KEYSPACE_PREFIX: &str = "/replicasets";

#[derive(Clone)]
struct ReplicaSetWatchEvent {
    event_type: String,
    object: ReplicaSet,
}

impl WatchEventLike for ReplicaSetWatchEvent {
    type Object = ReplicaSet;

    fn into_parts(self) -> (String, Self::Object) {
        (self.event_type, self.object)
    }
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/apps/v1/replicasets",
    params(("format" = Option<String>, Query, description = "Output format (json|yaml)")),
    responses(
        (status = 200, description = "ReplicaSet list", body = ReplicaSetList),
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
        return watch_replicasets(None, &params);
    }
    let output = OutputFormat::negotiate(params.format.as_deref(), &headers);
    let items = list_replica_sets(None).map_err(ApiError::internal_error)?;
    respond_with(build_list(items), output)
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/apps/v1/namespaces/{namespace}/replicasets",
    params(
        ("namespace" = String, Path, description = "Namespace to query"),
        ("format" = Option<String>, Query, description = "Output format (json|yaml)")
    ),
    responses(
        (status = 200, description = "ReplicaSet list", body = ReplicaSetList),
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
        return watch_replicasets(Some(namespace.as_str()), &params);
    }
    let output = OutputFormat::negotiate(params.format.as_deref(), &headers);
    let items = list_replica_sets(Some(namespace.as_str())).map_err(ApiError::internal_error)?;
    respond_with(build_list(items), output)
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/apps/v1/namespaces/{namespace}/replicasets/{name}",
    params(
        ("namespace" = String, Path, description = "Namespace of the ReplicaSet"),
        ("name" = String, Path, description = "ReplicaSet name"),
        ("format" = Option<String>, Query, description = "Output format (json|yaml)")
    ),
    responses(
        (status = 200, description = "ReplicaSet", body = ReplicaSet),
        (
            status = 404,
            description = "ReplicaSet not found",
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
            "watch is not supported for individual ReplicaSet resources",
        ));
    }
    let output = OutputFormat::negotiate(params.format.as_deref(), &headers);
    let resource =
        get_replica_set(Some(namespace.as_str()), &name).map_err(ApiError::internal_error)?;

    let replicaset = match resource {
        Some(value) => value,
        None => {
            return Err(ApiError::new(
                StatusCode::NOT_FOUND,
                format!("ReplicaSet '{name}' not found"),
            ))
        }
    };

    respond_with(replicaset, output)
}

fn watch_replicasets(namespace: Option<&str>, params: &ListParams) -> Result<Response, ApiError> {
    if params.resource_version_match.is_some() && params.resource_version.is_none() {
        return Err(ApiError::bad_request(
            "resourceVersionMatch requires resourceVersion to be set",
        ));
    }

    let resource_version_threshold = parse_resource_version(params.resource_version.as_deref())?;
    let allow_bookmarks = params.allow_watch_bookmarks.unwrap_or(false);
    let timeout = params.timeout_seconds.map(Duration::from_secs);

    let mut items = list_replica_sets(namespace).map_err(ApiError::internal_error)?;
    let current_resource_version = items
        .iter()
        .filter_map(|replicaset| {
            replicaset
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

    let initial_events: Vec<ReplicaSetWatchEvent> = items
        .drain(..)
        .filter(|workload| resource_version_is_newer(workload, resource_version_threshold))
        .map(|mut workload| {
            sanitize_replicaset(&mut workload);
            ReplicaSetWatchEvent {
                event_type: "ADDED".to_string(),
                object: workload,
            }
        })
        .collect();

    let mut subscription =
        ControllerWatchManager::controllers().subscribe(REPLICASET_KEYSPACE_PREFIX, namespace);
    let (sender, receiver) = broadcast::channel(REPLICASET_WATCH_BUFFER);
    tokio::spawn(async move {
        while let Some(event) = subscription.recv().await {
            if let Some(mut watch_event) = replicaset_watch_event_from_controller(event) {
                if resource_version_is_newer(&watch_event.object, resource_version_threshold) {
                    sanitize_replicaset(&mut watch_event.object);
                    if sender.send(watch_event).is_err() {
                        break;
                    }
                }
            }
        }
    });

    let body = WatchStreamBuilder::new(
        REPLICASET_WATCH_COMPONENT,
        "ReplicaSet watch serialization error",
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

fn sanitize_replicaset(workload: &mut ReplicaSet) {
    if workload.api_version.is_empty() {
        workload.api_version = "apps/v1".to_string();
    }
    if workload.kind.is_empty() {
        workload.kind = "ReplicaSet".to_string();
    }
    if workload.metadata.resource_version.is_none() {
        workload.metadata.resource_version = Some("1".to_string());
    }
}

fn replicaset_watch_event_from_controller(
    event: ControllerWatchEvent,
) -> Option<ReplicaSetWatchEvent> {
    let (namespace, name) = match parse_replicaset_key(event.key.as_str()) {
        Some(parts) => parts,
        None => {
            log_warn(
                REPLICASET_WATCH_COMPONENT,
                "Received ReplicaSet event with unrecognized key",
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
        match serde_json::from_str::<ReplicaSetDesiredState>(&payload) {
            Ok(desired) => match replicaset_from_desired_state(&namespace, &name, desired) {
                Ok(mut workload) => {
                    sanitize_replicaset(&mut workload);
                    workload
                }
                Err(err) => {
                    log_error(
                        REPLICASET_WATCH_COMPONENT,
                        "Failed to project ReplicaSet desired state",
                        &[
                            ("key", event.key.as_str()),
                            ("error", err.to_string().as_str()),
                        ],
                    );
                    return None;
                }
            },
            Err(err) => {
                log_error(
                    REPLICASET_WATCH_COMPONENT,
                    "Failed to deserialize ReplicaSet desired state",
                    &[
                        ("key", event.key.as_str()),
                        ("error", err.to_string().as_str()),
                    ],
                );
                return None;
            }
        }
    } else {
        let mut selector = LabelSelector::default();
        selector
            .match_labels
            .insert(LABEL_REPLICASET_NAME.to_string(), name.clone());
        let mut template = PodTemplateSpec::default();
        template
            .metadata
            .labels
            .insert(LABEL_REPLICASET_NAME.to_string(), name.clone());

        ReplicaSet {
            api_version: "apps/v1".to_string(),
            kind: "ReplicaSet".to_string(),
            metadata: ObjectMeta {
                name: Some(name.clone()),
                namespace: None,
                labels: Default::default(),
                annotations: Default::default(),
                resource_version: Some(event.resource_version.to_string()),
            },
            spec: ReplicaSetSpec {
                replicas: 0,
                selector,
                template,
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

    Some(ReplicaSetWatchEvent {
        event_type: event_type.to_string(),
        object: workload,
    })
}

fn parse_replicaset_key(key: &str) -> Option<(String, String)> {
    let trimmed = key.trim_matches('/');
    let mut parts = trimmed.splitn(3, '/');
    let prefix = parts.next()?;
    if prefix != REPLICASET_KEYSPACE_PREFIX.trim_matches('/') {
        return None;
    }
    let namespace = parts.next().unwrap_or("default").to_string();
    let name = parts.next()?.to_string();
    Some((namespace, name))
}
