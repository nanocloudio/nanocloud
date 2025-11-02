use super::error::ApiError;
use super::format::{respond_with, OutputFormat};
use super::watch::{
    ensure_resource_version_match, parse_resource_version, resource_version_is_newer,
    ResourceVersionMatchPolicy, WatchEventLike, WatchStreamBuilder,
};
use crate::nanocloud::controller::watch::{ControllerWatchEvent, ControllerWatchManager};
use crate::nanocloud::k8s::job::{Job, JobList, JobSpec};
use crate::nanocloud::k8s::pod::ObjectMeta;
use crate::nanocloud::k8s::store::{delete_job, get_job, list_jobs_for, normalize_namespace};
use crate::nanocloud::logger::{log_error, log_warn};
use crate::nanocloud::util::KeyspaceEventType;
use axum::extract::{Path, Query};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use axum::Json;
use serde::Deserialize;
use serde_json;
use tokio::sync::broadcast;
use tokio::time::Duration;

const JOB_PREFIX: &str = "/jobs";
const JOB_WATCH_BUFFER: usize = 32;
const JOB_WATCH_COMPONENT: &str = "jobs-watch";

#[derive(Clone)]
struct JobWatchEvent {
    event_type: String,
    object: Job,
}

impl WatchEventLike for JobWatchEvent {
    type Object = Job;

    fn into_parts(self) -> (String, Self::Object) {
        (self.event_type, self.object)
    }
}

fn build_list(items: Vec<Job>) -> JobList {
    JobList::from_items(items)
}

#[derive(Default, Deserialize)]
pub struct ListParams {
    #[serde(default)]
    pub format: Option<String>,
}

#[derive(Default, Deserialize)]
pub(crate) struct JobWatchParams {
    #[serde(default)]
    watch: Option<bool>,
    #[serde(rename = "resourceVersion")]
    resource_version: Option<String>,
    #[serde(rename = "fieldSelector")]
    field_selector: Option<String>,
    #[serde(rename = "labelSelector")]
    label_selector: Option<String>,
    #[serde(rename = "timeoutSeconds")]
    timeout_seconds: Option<u64>,
    #[serde(rename = "allowWatchBookmarks")]
    allow_watch_bookmarks: Option<bool>,
    #[serde(rename = "limit")]
    limit: Option<u32>,
    #[serde(rename = "continue")]
    continue_token: Option<String>,
    #[serde(rename = "resourceVersionMatch")]
    resource_version_match: Option<ResourceVersionMatchPolicy>,
    #[serde(default)]
    format: Option<String>,
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/batch/v1/jobs",
    params(
        ("watch" = Option<bool>, Query, description = "Watch for changes"),
        ("resourceVersion" = Option<String>, Query, description = "Resource version threshold"),
        ("timeoutSeconds" = Option<u64>, Query, description = "Server-side watch timeout"),
        ("allowWatchBookmarks" = Option<bool>, Query, description = "Emit bookmark events"),
        ("format" = Option<String>, Query, description = "Output format (json|yaml)")
    ),
    responses(
        (status = 200, description = "Job list or watch stream", body = JobList),
        (status = 500, description = "Internal error", body = crate::nanocloud::api::types::ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["workloads.read"])
    ),
    tag = "kubernetes"
))]
pub async fn list_all(
    Query(params): Query<JobWatchParams>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    handle_job_list(None, params, &headers).await
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/batch/v1/namespaces/{namespace}/jobs",
    params(
        ("namespace" = String, Path, description = "Namespace to query"),
        ("watch" = Option<bool>, Query, description = "Watch for changes"),
        ("resourceVersion" = Option<String>, Query, description = "Resource version threshold"),
        ("timeoutSeconds" = Option<u64>, Query, description = "Server-side watch timeout"),
        ("allowWatchBookmarks" = Option<bool>, Query, description = "Emit bookmark events"),
        ("format" = Option<String>, Query, description = "Output format (json|yaml)")
    ),
    responses(
        (status = 200, description = "Job list or watch stream", body = JobList),
        (status = 500, description = "Internal error", body = crate::nanocloud::api::types::ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["workloads.read"])
    ),
    tag = "kubernetes"
))]
pub async fn list_namespaced(
    Path(namespace): Path<String>,
    Query(params): Query<JobWatchParams>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    handle_job_list(Some(namespace.as_str()), params, &headers).await
}

async fn handle_job_list(
    namespace: Option<&str>,
    params: JobWatchParams,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    let JobWatchParams {
        watch,
        resource_version,
        field_selector,
        label_selector,
        timeout_seconds,
        allow_watch_bookmarks,
        limit,
        continue_token,
        resource_version_match,
        format,
    } = params;

    let output = OutputFormat::negotiate(format.as_deref(), headers);
    let watch_requested = watch.unwrap_or(false);

    if !watch_requested {
        let items = list_jobs_for(namespace).map_err(ApiError::internal_error)?;
        return respond_with(build_list(items), output);
    }

    if output != OutputFormat::Json {
        return Err(ApiError::bad_request(
            "watch requests must use JSON output format",
        ));
    }

    if limit.is_some() {
        return Err(ApiError::bad_request(
            "limit cannot be combined with watch=true",
        ));
    }

    if continue_token.is_some() {
        return Err(ApiError::bad_request(
            "continue cannot be combined with watch=true",
        ));
    }

    if field_selector.is_some() {
        return Err(ApiError::bad_request(
            "fieldSelector is not supported for Job watches",
        ));
    }

    if label_selector.is_some() {
        return Err(ApiError::bad_request(
            "labelSelector is not supported for Job watches",
        ));
    }

    let resource_version_threshold = parse_resource_version(resource_version.as_deref())?;
    let allow_bookmarks = allow_watch_bookmarks.unwrap_or(false);
    let timeout = timeout_seconds.map(Duration::from_secs);

    let mut items = list_jobs_for(namespace).map_err(ApiError::internal_error)?;
    let current_resource_version = items
        .iter()
        .filter_map(|job| {
            job.metadata
                .resource_version
                .as_deref()
                .and_then(|value| value.parse::<u64>().ok())
        })
        .max();
    ensure_resource_version_match(
        resource_version_match,
        resource_version_threshold,
        current_resource_version,
    )?;

    let initial_events: Vec<JobWatchEvent> = items
        .drain(..)
        .filter(|job| resource_version_is_newer(job, resource_version_threshold))
        .map(|job| JobWatchEvent {
            event_type: "ADDED".to_string(),
            object: job,
        })
        .collect();

    let mut subscription = ControllerWatchManager::shared().subscribe(JOB_PREFIX, namespace);
    let (sender, receiver) = broadcast::channel(JOB_WATCH_BUFFER);
    tokio::spawn(async move {
        let threshold = resource_version_threshold;
        let sender = sender;
        while let Some(event) = subscription.recv().await {
            if let Some(job_event) = job_watch_event_from_controller(event) {
                if resource_version_is_newer(&job_event.object, threshold)
                    && sender.send(job_event).is_err()
                {
                    break;
                }
            }
        }
    });

    let body = WatchStreamBuilder::new(
        JOB_WATCH_COMPONENT,
        "Job watch serialization error",
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

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/batch/v1/namespaces/{namespace}/jobs/{name}",
    params(
        ("namespace" = String, Path, description = "Namespace of the Job"),
        ("name" = String, Path, description = "Job name"),
        ("format" = Option<String>, Query, description = "Output format (json|yaml)")
    ),
    responses(
        (status = 200, description = "Job", body = Job),
        (status = 404, description = "Job not found", body = crate::nanocloud::api::types::ErrorBody),
        (status = 500, description = "Internal error", body = crate::nanocloud::api::types::ErrorBody)
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
    let resource = get_job(Some(namespace.as_str()), &name).map_err(ApiError::internal_error)?;
    match resource {
        Some(job) => respond_with(job, output),
        None => Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("Job '{name}' not found"),
        )),
    }
}

#[derive(serde::Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct StatusBody {
    #[serde(rename = "apiVersion")]
    pub(crate) api_version: String,
    pub(crate) kind: String,
    pub(crate) status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) details: Option<StatusDetails>,
}

#[derive(serde::Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct StatusDetails {
    pub(crate) name: String,
    pub(crate) namespace: String,
}

fn job_watch_event_from_controller(event: ControllerWatchEvent) -> Option<JobWatchEvent> {
    let (namespace, name) = match parse_job_key(event.key.as_str()) {
        Some(parts) => parts,
        None => {
            log_warn(
                JOB_WATCH_COMPONENT,
                "Received Job event with unrecognized key",
                &[("key", event.key.as_str())],
            );
            return None;
        }
    };

    let event_type = match event.event_type {
        KeyspaceEventType::Added => "ADDED",
        KeyspaceEventType::Modified => "MODIFIED",
        KeyspaceEventType::Deleted => "DELETED",
    }
    .to_string();

    let mut job = match event.value {
        Some(payload) => match serde_json::from_str::<Job>(&payload) {
            Ok(mut job) => {
                if job.api_version.is_empty() {
                    job.api_version = "batch/v1".to_string();
                }
                if job.kind.is_empty() {
                    job.kind = "Job".to_string();
                }
                job
            }
            Err(err) => {
                log_error(
                    JOB_WATCH_COMPONENT,
                    "Failed to decode Job payload for watch event",
                    &[
                        ("key", event.key.as_str()),
                        ("error", err.to_string().as_str()),
                    ],
                );
                return None;
            }
        },
        None => {
            let metadata = ObjectMeta {
                name: Some(name.clone()),
                namespace: Some(namespace.clone()),
                resource_version: Some(event.resource_version.to_string()),
                ..ObjectMeta::default()
            };
            Job::new(metadata, JobSpec::default())
        }
    };

    if job.metadata.name.is_none() {
        job.metadata.name = Some(name);
    }
    if job.metadata.namespace.is_none() {
        job.metadata.namespace = Some(namespace);
    }
    job.metadata.resource_version = Some(event.resource_version.to_string());

    Some(JobWatchEvent {
        event_type,
        object: job,
    })
}

fn parse_job_key(key: &str) -> Option<(String, String)> {
    let trimmed = key.trim_matches('/');
    let mut parts = trimmed.splitn(3, '/');
    let prefix = parts.next()?;
    if prefix != JOB_PREFIX.trim_matches('/') {
        return None;
    }
    let namespace = parts.next().unwrap_or("default").to_string();
    let name = parts.next()?.to_string();
    Some((namespace, name))
}

#[cfg_attr(feature = "openapi", utoipa::path(
    delete,
    path = "/apis/batch/v1/namespaces/{namespace}/jobs/{name}",
    params(
        ("namespace" = String, Path, description = "Namespace of the Job"),
        ("name" = String, Path, description = "Job name")
    ),
    responses(
        (status = 200, description = "Job deletion accepted", body = StatusBody),
        (status = 404, description = "Job not found", body = crate::nanocloud::api::types::ErrorBody),
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
) -> Result<Json<StatusBody>, ApiError> {
    let normalized = normalize_namespace(Some(namespace.as_str()));
    delete_job(Some(namespace.as_str()), &name).map_err(ApiError::internal_error)?;
    let status = StatusBody {
        api_version: "v1".to_string(),
        kind: "Status".to_string(),
        status: "Success".to_string(),
        message: None,
        details: Some(StatusDetails {
            name: name.clone(),
            namespace: normalized,
        }),
    };
    Ok(Json(status))
}
