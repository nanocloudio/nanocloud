/*
 * Copyright (C) 2024 The Nanocloud Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

pub(super) mod bundles;
pub mod certificates;
pub(super) mod configmaps;
pub(super) mod daemonsets;
pub(super) mod deployments;
pub(super) mod devices;
pub(super) mod discovery;
mod error;
pub(super) mod events;
pub(super) mod exec;
pub(super) mod exec_common;
mod format;
pub(super) mod general;
pub(super) mod jobs;
pub(super) mod logs;
pub(super) mod observability;
pub(super) mod pods;
pub(super) mod replicasets;
mod selectors;
pub mod serviceaccounts;
pub(super) mod services;
pub(super) mod statefulsets;
mod watch;

#[allow(unused_imports)]
pub use pods::{list_pods_all, PodTableRow, WatchParams, TABLE_CONTENT_TYPE};

use std::sync::{Arc, OnceLock};

use self::error::ApiError;
use crate::nanocloud::api::types::{
    AsyncStatus, CaRequest, LogQuery, NetworkPolicyChainDebug, NetworkPolicyDebugResponse,
    NetworkPolicyRuleDebug, NetworkPolicySummary, ServiceAccountTokenRequest,
    ServiceAccountTokenResponse,
};
#[cfg(feature = "openapi")]
use crate::nanocloud::api::types::{
    CertificateRequest, CertificateResponse, CertificateStatus, ErrorBody, ServiceActionResponse,
};
use crate::nanocloud::controller::networkpolicy;
#[cfg(feature = "openapi")]
use crate::nanocloud::oci::runtime::{ContainerNetwork, ContainerState, ContainerStatus};
use crate::nanocloud::util::security::JsonTlsInfo;
use axum::extract::{Path, Query};
#[cfg(feature = "openapi")]
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::Response;
use axum::Json;
use serde_json::Value;

#[cfg(feature = "openapi")]
use utoipa::openapi::OpenApi as OpenApiDoc;
#[cfg(feature = "openapi")]
use utoipa::OpenApi;

static OPENAPI_DOC: OnceLock<Option<Arc<Value>>> = OnceLock::new();

#[cfg(feature = "openapi")]
const OPENAPI_SPEC_JSON: &str = include_str!("../../../../docs/openapi.json");

pub fn openapi_document() -> Option<Arc<Value>> {
    OPENAPI_DOC.get_or_init(load_openapi_document).clone()
}

fn load_openapi_document() -> Option<Arc<Value>> {
    #[cfg(feature = "openapi")]
    {
        let value: Value = serde_json::from_str(OPENAPI_SPEC_JSON)
            .expect("embedded OpenAPI specification must be valid JSON");
        Some(Arc::new(value))
    }
    #[cfg(not(feature = "openapi"))]
    {
        None
    }
}

#[cfg(all(test, feature = "openapi"))]
pub fn set_openapi_doc_for_testing(doc: Value) {
    let _ = OPENAPI_DOC.set(Some(Arc::new(doc)));
}

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/setup",
    responses(
        (status = 501, description = "Setup is only available via the CLI", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBootstrap" = ["bootstrap"]),
        (
            "NanocloudBearer" = ["system.setup.execute"]
        )
    ),
    tag = "nanocloud"
))]
pub(super) async fn setup() -> Result<(StatusCode, Json<AsyncStatus>), ApiError> {
    general::setup().await
}

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/ca",
    request_body = CaRequest,
    responses(
        (status = 200, description = "Certificate issued", body = JsonTlsInfo),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBootstrap" = ["bootstrap"]),
        (
            "NanocloudBearer" = ["certificate.issue"]
        )
    ),
    tag = "nanocloud"
))]
pub(super) async fn issue_certificate(
    Json(payload): Json<CaRequest>,
) -> Result<Json<JsonTlsInfo>, ApiError> {
    general::issue_certificate(Json(payload)).await
}

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/serviceaccounts/token",
    request_body = ServiceAccountTokenRequest,
    responses(
        (status = 200, description = "JWT issued", body = ServiceAccountTokenResponse),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 404, description = "Single-use token not found", body = ErrorBody),
        (status = 410, description = "Single-use token expired or replayed", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBootstrap" = ["bootstrap"])
    ),
    tag = "nanocloud"
))]
pub(super) async fn exchange_serviceaccount_token(
    Json(payload): Json<ServiceAccountTokenRequest>,
) -> Result<Json<ServiceAccountTokenResponse>, ApiError> {
    serviceaccounts::exchange_bootstrap_token(Json(payload)).await
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/networkpolicies/debug",
    responses(
        (
            status = 200,
            description = "NetworkPolicy debug snapshot",
            body = NetworkPolicyDebugResponse
        ),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["networkpolicy.debug"])
    ),
    tag = "nanocloud"
))]
pub(super) async fn networkpolicy_debug() -> Result<Json<NetworkPolicyDebugResponse>, ApiError> {
    let (policies, chains) = tokio::task::spawn_blocking(networkpolicy::compile_plan)
        .await
        .map_err(|err| ApiError::internal_error(Box::new(err)))?
        .map_err(ApiError::internal_error)?;

    let policy_summaries: Vec<NetworkPolicySummary> = policies
        .into_iter()
        .map(|stored| {
            let namespace_hint = stored
                .namespace
                .clone()
                .or_else(|| stored.policy.metadata.namespace.clone())
                .unwrap_or_else(|| "default".to_string());
            NetworkPolicySummary {
                namespace: namespace_hint,
                name: stored.name,
                policy_types: stored.policy.spec.policy_types.clone(),
                pod_selector: stored.policy.spec.pod_selector.clone(),
                ingress_rules: stored.policy.spec.ingress.len(),
                egress_rules: stored.policy.spec.egress.len(),
            }
        })
        .collect();

    let chain_summaries: Vec<NetworkPolicyChainDebug> = chains
        .into_iter()
        .map(|chain| {
            let rules = chain
                .rules
                .into_iter()
                .map(|rule| NetworkPolicyRuleDebug {
                    source: rule.cidr,
                    protocol: rule.protocol.map(|p| p.to_lowercase()),
                    port: rule.port,
                })
                .collect();
            NetworkPolicyChainDebug {
                namespace: chain.namespace,
                pod: chain.pod,
                pod_ip: chain.pod_ip,
                direction: chain.direction.as_str().to_string(),
                chain: chain.name,
                rules,
            }
        })
        .collect();

    Ok(Json(NetworkPolicyDebugResponse {
        policies: policy_summaries,
        chains: chain_summaries,
    }))
}

pub(super) async fn metrics() -> Response {
    observability::metrics().await
}

pub(super) async fn healthz() -> Response {
    observability::combined_health().await
}

pub(super) async fn readyz() -> Response {
    observability::readiness().await
}

pub(super) async fn livez() -> Response {
    observability::liveness().await
}

#[cfg(feature = "openapi")]
#[cfg(feature = "openapi")]
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/api/v1/pods",
    params(
        ("watch" = Option<bool>, Query, description = "Stream updates instead of returning a list"),
        ("resourceVersion" = Option<String>, Query, description = "Return items newer than the provided resourceVersion"),
        ("fieldSelector" = Option<String>, Query, description = "Filter by field expressions such as metadata.name=<name>"),
        ("labelSelector" = Option<String>, Query, description = "Filter by Kubernetes label selector expressions"),
        ("timeoutSeconds" = Option<u64>, Query, description = "Terminate a watch after the specified number of seconds"),
        ("allowWatchBookmarks" = Option<bool>, Query, description = "Emit periodic bookmark events when watch=true"),
        ("limit" = Option<u32>, Query, description = "Maximum number of pods to return"),
        ("continue" = Option<String>, Query, description = "Continue token from a previous list response" )
    ),
    responses(
        (
            status = 200,
            description = "List Pods (JSON list or Table projection negotiated via Accept header or format=table)",
            content(
                (
                    crate::nanocloud::server::handlers::pods::PodList
                        = "application/json"
                ),
                (
                    crate::nanocloud::k8s::table::Table
                        = "application/json;as=Table;g=meta.k8s.io;v=v1"
                )
            )
        ),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["pods.read"])
    ),
    tag = "kubernetes"
))]
#[cfg(feature = "openapi")]
#[allow(dead_code)]
pub(super) async fn list_pods_all_openapi(
    Query(params): Query<pods::WatchParams>,
) -> Result<Response, ApiError> {
    pods::list_pods_all(Query(params), HeaderMap::new()).await
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace}/pods",
    params(
        ("namespace" = String, Path, description = "Pod namespace"),
        ("watch" = Option<bool>, Query, description = "Stream updates instead of returning a list"),
        ("resourceVersion" = Option<String>, Query, description = "Return items newer than the provided resourceVersion"),
        ("fieldSelector" = Option<String>, Query, description = "Filter by field expressions such as metadata.name=<name>"),
        ("labelSelector" = Option<String>, Query, description = "Filter by Kubernetes label selector expressions"),
        ("timeoutSeconds" = Option<u64>, Query, description = "Terminate a watch after the specified number of seconds"),
        ("allowWatchBookmarks" = Option<bool>, Query, description = "Emit periodic bookmark events when watch=true"),
        ("limit" = Option<u32>, Query, description = "Maximum number of pods to return"),
        ("continue" = Option<String>, Query, description = "Continue token from a previous list response" )
    ),
    responses(
        (
            status = 200,
            description = "List Pods (JSON list or Table projection negotiated via Accept header or format=table)",
            content(
                (
                    crate::nanocloud::server::handlers::pods::PodList
                        = "application/json"
                ),
                (
                    crate::nanocloud::k8s::table::Table
                        = "application/json;as=Table;g=meta.k8s.io;v=v1"
                )
            )
        ),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["pods.read"])
    ),
    tag = "kubernetes"
))]
#[cfg(feature = "openapi")]
#[allow(dead_code)]
pub(super) async fn list_pods_openapi(
    Path(namespace): Path<String>,
    Query(params): Query<pods::WatchParams>,
) -> Result<Response, ApiError> {
    pods::list_pods(Path(namespace), Query(params), HeaderMap::new()).await
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/api/v1/configmaps",
    params(
        ("watch" = Option<bool>, Query, description = "Stream updates instead of returning a list"),
        ("resourceVersion" = Option<String>, Query, description = "Return items newer than the provided resourceVersion"),
        ("fieldSelector" = Option<String>, Query, description = "Filter by field expressions such as metadata.name=<name>"),
        ("labelSelector" = Option<String>, Query, description = "Filter by Kubernetes label selector expressions"),
        ("timeoutSeconds" = Option<u64>, Query, description = "Terminate a watch after the specified number of seconds"),
        ("allowWatchBookmarks" = Option<bool>, Query, description = "Emit periodic bookmark events when watch=true"),
        ("limit" = Option<u32>, Query, description = "Maximum number of configmaps to return"),
        ("continue" = Option<String>, Query, description = "Continue token from a previous list response" )
    ),
    responses(
        (
            status = 200,
            description = "List ConfigMaps",
            body = crate::nanocloud::k8s::configmap::ConfigMapList
        ),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["configmaps.read"])
    ),
    tag = "kubernetes"
))]
pub(super) async fn list_configmaps_all(
    Query(params): Query<configmaps::WatchParams>,
) -> Result<Response, ApiError> {
    configmaps::list_all(Query(params)).await
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace}/configmaps",
    params(
        ("namespace" = String, Path, description = "ConfigMap namespace"),
        ("watch" = Option<bool>, Query, description = "Stream updates instead of returning a list"),
        ("resourceVersion" = Option<String>, Query, description = "Return items newer than the provided resourceVersion"),
        ("fieldSelector" = Option<String>, Query, description = "Filter by field expressions such as metadata.name=<name>"),
        ("labelSelector" = Option<String>, Query, description = "Filter by Kubernetes label selector expressions"),
        ("timeoutSeconds" = Option<u64>, Query, description = "Terminate a watch after the specified number of seconds"),
        ("allowWatchBookmarks" = Option<bool>, Query, description = "Emit periodic bookmark events when watch=true"),
        ("limit" = Option<u32>, Query, description = "Maximum number of configmaps to return"),
        ("continue" = Option<String>, Query, description = "Continue token from a previous list response" )
    ),
    responses(
        (
            status = 200,
            description = "List ConfigMaps",
            body = crate::nanocloud::k8s::configmap::ConfigMapList
        ),
        (status = 500,
            description = "Internal error",
            body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["configmaps.read"])
    ),
    tag = "kubernetes"
))]
pub(super) async fn list_configmaps(
    Path(namespace): Path<String>,
    Query(params): Query<configmaps::WatchParams>,
) -> Result<Response, ApiError> {
    configmaps::list_namespace(Path(namespace), Query(params)).await
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace}/configmaps/{name}",
    params(
        ("namespace" = String, Path, description = "ConfigMap namespace"),
        ("name" = String, Path, description = "ConfigMap name")
    ),
    responses(
        (
            status = 200,
            description = "Get ConfigMap",
            body = crate::nanocloud::k8s::configmap::ConfigMap
        ),
        (status = 404, description = "ConfigMap not found", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["configmaps.read"])
    ),
    tag = "kubernetes"
))]
pub(super) async fn get_configmap(
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<configmaps::WatchParams>,
) -> Result<Response, ApiError> {
    configmaps::get(Path((namespace, name)), Query(params)).await
}

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/api/v1/namespaces/{namespace}/configmaps",
    request_body = crate::nanocloud::k8s::configmap::ConfigMap,
    responses(
        (
            status = 201,
            description = "ConfigMap created",
            body = crate::nanocloud::k8s::configmap::ConfigMap
        ),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 409, description = "ConfigMap conflict", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["configmaps.write"])
    ),
    tag = "kubernetes"
))]
pub(super) async fn create_configmap(
    Path(namespace): Path<String>,
    Json(payload): Json<crate::nanocloud::k8s::configmap::ConfigMap>,
) -> Result<
    (
        StatusCode,
        Json<crate::nanocloud::k8s::configmap::ConfigMap>,
    ),
    ApiError,
> {
    configmaps::create(Path(namespace), Json(payload)).await
}

#[cfg_attr(feature = "openapi", utoipa::path(
    put,
    path = "/api/v1/namespaces/{namespace}/configmaps/{name}",
    request_body = crate::nanocloud::k8s::configmap::ConfigMap,
    params(
        ("namespace" = String, Path, description = "ConfigMap namespace"),
        ("name" = String, Path, description = "ConfigMap name")
    ),
    responses(
        (
            status = 200,
            description = "ConfigMap updated",
            body = crate::nanocloud::k8s::configmap::ConfigMap
        ),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 404, description = "ConfigMap not found", body = ErrorBody),
        (status = 409, description = "ConfigMap conflict", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["configmaps.write"])
    ),
    tag = "kubernetes"
))]
pub(super) async fn replace_configmap(
    Path((namespace, name)): Path<(String, String)>,
    Json(payload): Json<crate::nanocloud::k8s::configmap::ConfigMap>,
) -> Result<Json<crate::nanocloud::k8s::configmap::ConfigMap>, ApiError> {
    configmaps::replace(Path((namespace, name)), Json(payload)).await
}

#[cfg_attr(feature = "openapi", utoipa::path(
    delete,
    path = "/api/v1/namespaces/{namespace}/configmaps/{name}",
    params(
        ("namespace" = String, Path, description = "ConfigMap namespace"),
        ("name" = String, Path, description = "ConfigMap name")
    ),
    responses(
        (
            status = 200,
            description = "ConfigMap deleted",
            body = crate::nanocloud::k8s::configmap::ConfigMap
        ),
        (status = 404, description = "ConfigMap not found", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["configmaps.write"])
    ),
    tag = "kubernetes"
))]
pub(super) async fn delete_configmap(
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<crate::nanocloud::k8s::configmap::ConfigMap>, ApiError> {
    configmaps::delete(Path((namespace, name))).await
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace}/pods/{name}/log",
    params(
        ("namespace" = String, Path, description = "Service namespace"),
        ("name" = String, Path, description = "Service name"),
        ("follow" = Option<bool>, Query, description = "Stream logs until interrupted"),
        ("container" = Option<String>, Query, description = "Specific container name to stream"),
        ("previous" = Option<bool>, Query, description = "Include logs from the previously terminated container instance"),
        ("tailLines" = Option<u64>, Query, description = "Number of lines from the end of the logs to output"),
        ("sinceSeconds" = Option<u64>, Query, description = "Only return logs newer than N seconds")
    ),
    responses(
        (status = 200, description = "Stream of log lines"),
        (status = 404, description = "Logs not found", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    tag = "nanocloud"
))]
pub(super) async fn service_logs(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<LogQuery>,
) -> Result<Response, ApiError> {
    logs::stream_service_logs(Some(namespace), name, query).await
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/api/v1/pods/{name}/log",
    params(
        ("name" = String, Path, description = "Service name"),
        ("follow" = Option<bool>, Query, description = "Stream logs until interrupted"),
        ("container" = Option<String>, Query, description = "Specific container name to stream"),
        ("previous" = Option<bool>, Query, description = "Include logs from the previously terminated container instance"),
        ("tailLines" = Option<u64>, Query, description = "Number of lines from the end of the logs to output"),
        ("sinceSeconds" = Option<u64>, Query, description = "Only return logs newer than N seconds")
    ),
    responses(
        (status = 200, description = "Stream of log lines"),
        (status = 404, description = "Logs not found", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    tag = "nanocloud"
))]
pub(super) async fn service_logs_no_ns(
    Path(name): Path<String>,
    Query(query): Query<LogQuery>,
) -> Result<Response, ApiError> {
    logs::stream_service_logs(None, name, query).await
}

pub(super) async fn openapi_spec() -> Result<Json<Value>, StatusCode> {
    match openapi_document() {
        Some(doc) => Ok(Json((*doc).clone())),
        None => Err(StatusCode::NOT_FOUND),
    }
}

#[cfg(feature = "openapi")]
#[inline(always)]
#[allow(dead_code)]
fn mark_openapi_handlers_used() {
    let _ = list_pods_all_openapi;
    let _ = list_pods_openapi;
}

#[cfg(feature = "openapi")]
#[allow(dead_code)]
pub fn build_openapi_doc() -> OpenApiDoc {
    mark_openapi_handlers_used();
    ApiDoc::openapi()
}

#[cfg(feature = "openapi")]
#[cfg_attr(not(test), allow(dead_code))]
#[derive(OpenApi)]
#[openapi(
    paths(
        discovery::version,
        discovery::core_api_versions,
        discovery::api_groups,
        discovery::nanocloud_api_group,
        discovery::core_api_resources,
        discovery::nanocloud_api_resources,
        setup,
        issue_certificate,
        certificates::issue_ephemeral_certificate,
        exchange_serviceaccount_token,
        bundles::list,
        bundles::get,
        bundles::create,
        bundles::delete,
        list_pods_all_openapi,
        list_pods_openapi,
        list_configmaps_all,
        list_configmaps,
        services::start_bundle,
        services::stop_bundle,
        services::restart_bundle,
        services::uninstall_bundle,
        services::stream_latest_backup,
        crate::nanocloud::server::handlers::exec::exec_ws_namespaced,
        crate::nanocloud::server::handlers::exec::exec_http_post_namespaced,
        crate::nanocloud::server::handlers::exec::exec_ws_cluster,
        crate::nanocloud::server::handlers::exec::exec_http_post_cluster,
    ),
    components(
        schemas(
            AsyncStatus,
            CaRequest,
            ServiceActionResponse,
            ServiceAccountTokenRequest,
            ServiceAccountTokenResponse,
            CertificateRequest,
            CertificateResponse,
            CertificateStatus,
            crate::nanocloud::api::types::Bundle,
            crate::nanocloud::api::types::BundleList,
            crate::nanocloud::api::types::BundleSpec,
            crate::nanocloud::api::types::BundleStatus,
            crate::nanocloud::api::types::BundleCondition,
            crate::nanocloud::api::types::BundleConditionStatus,
            crate::nanocloud::api::types::BundlePhase,
            crate::nanocloud::api::types::BundleWorkloadRef,
            crate::nanocloud::api::types::BundleSnapshotSource,
            ErrorBody,
            JsonTlsInfo,
            ContainerState,
            ContainerNetwork,
            ContainerStatus,
            crate::nanocloud::k8s::statefulset::StatefulSet,
            crate::nanocloud::k8s::statefulset::StatefulSetList,
            crate::nanocloud::k8s::deployment::Deployment,
            crate::nanocloud::k8s::deployment::DeploymentList,
            crate::nanocloud::k8s::daemonset::DaemonSet,
            crate::nanocloud::k8s::daemonset::DaemonSetList,
            crate::nanocloud::k8s::replicaset::ReplicaSet,
            crate::nanocloud::k8s::replicaset::ReplicaSetList,
            crate::nanocloud::k8s::replicaset::ReplicaSetStatus,
            crate::nanocloud::k8s::pod::Pod,
            crate::nanocloud::k8s::discovery::VersionInfo,
            crate::nanocloud::k8s::discovery::APIVersions,
            crate::nanocloud::k8s::discovery::APIGroupList,
            crate::nanocloud::k8s::discovery::APIGroup,
            crate::nanocloud::k8s::discovery::APIResourceList,
            crate::nanocloud::k8s::discovery::APIResource
        )
    ),
    tags(
        (name = "nanocloud", description = "Nanocloud service management API")
    )
)]
pub(super) struct ApiDoc;
