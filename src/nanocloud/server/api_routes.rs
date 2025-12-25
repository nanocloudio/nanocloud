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

//! API route definitions for the Nanocloud control-plane server.
//!
//! This module defines all HTTP routes for the API server, separate from
//! the server infrastructure. Routes are organized by API group.

use axum::routing::{get, post};
use axum::Router;

use super::handlers;

/// Build the complete API router with all routes.
///
/// This function constructs the router with all API endpoints but does NOT
/// apply middleware. Middleware should be applied by the caller using
/// [`crate::nanocloud::http_middleware::MiddlewareStack`].
pub fn build_api_router() -> Router {
    let mut router = Router::new();

    // Discovery routes
    router = router
        .route("/version", get(handlers::discovery::version))
        .route("/api", get(handlers::discovery::core_api_versions))
        .route("/apis", get(handlers::discovery::api_groups))
        .route(
            "/apis/nanocloud.io",
            get(handlers::discovery::nanocloud_api_group),
        )
        .route("/apis/apps", get(handlers::discovery::apps_api_group))
        .route(
            "/apis/nanocloud.io/v1",
            get(handlers::discovery::nanocloud_api_resources),
        )
        .route(
            "/apis/apps/v1",
            get(handlers::discovery::apps_api_resources),
        )
        .route("/api/v1", get(handlers::discovery::core_api_resources));

    // Nanocloud CRD routes - Bundles
    router = router
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles",
            get(handlers::bundles::list).post(handlers::bundles::create),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}",
            get(handlers::bundles::get)
                .delete(handlers::bundles::delete)
                .patch(handlers::bundles::apply),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/exportProfile",
            post(handlers::bundles::export_profile),
        );

    // Bundle actions
    router = router
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/actions/start",
            post(handlers::services::start_bundle),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/actions/stop",
            post(handlers::services::stop_bundle),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/actions/restart",
            post(handlers::services::restart_bundle),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/actions/uninstall",
            post(handlers::services::uninstall_bundle),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/backups/latest",
            get(handlers::services::stream_latest_backup),
        );

    // Nanocloud CRD routes - Devices
    router = router
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/devices",
            get(handlers::devices::list).post(handlers::devices::create),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/devices/{name}",
            get(handlers::devices::get).delete(handlers::devices::delete),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/devices/certificates",
            post(handlers::devices::issue_certificate),
        );

    // Nanocloud CRD routes - VolumeSnapshots
    router = router
        .route(
            "/apis/nanocloud.io/v1/volumesnapshots",
            get(handlers::volumesnapshots::list_all),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/volumesnapshots",
            get(handlers::volumesnapshots::list_namespaced).post(handlers::volumesnapshots::create),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/volumesnapshots/{name}",
            get(handlers::volumesnapshots::get).delete(handlers::volumesnapshots::delete),
        );

    // Nanocloud CRD routes - RBAC
    router = router
        .route(
            "/apis/nanocloud.io/v1/roles",
            get(handlers::rbac::list_roles),
        )
        .route(
            "/apis/nanocloud.io/v1/roles/{name}",
            get(handlers::rbac::get_role_cluster),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/roles",
            get(handlers::rbac::list_roles_namespaced),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/roles/{name}",
            get(handlers::rbac::get_role),
        )
        .route(
            "/apis/nanocloud.io/v1/rolebindings",
            get(handlers::rbac::list_role_bindings),
        )
        .route(
            "/apis/nanocloud.io/v1/rolebindings/{name}",
            get(handlers::rbac::get_role_binding_cluster),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/rolebindings",
            get(handlers::rbac::list_role_bindings_namespaced),
        )
        .route(
            "/apis/nanocloud.io/v1/namespaces/{namespace}/rolebindings/{name}",
            get(handlers::rbac::get_role_binding),
        );

    // Nanocloud CRD routes - Certificates
    router = router.route(
        "/apis/nanocloud.io/v1/certificates",
        post(handlers::certificates::issue_ephemeral_certificate),
    );

    // Kubernetes core API - Events
    router = router
        .route("/api/v1/events", get(handlers::events::list_all))
        .route(
            "/api/v1/namespaces/{namespace}/events",
            get(handlers::events::list_namespaced),
        );

    // Kubernetes core API - Pods
    router = router
        .route("/api/v1/pods", get(handlers::pods::list_pods_all))
        .route(
            "/api/v1/namespaces/{namespace}/pods",
            get(handlers::pods::list_pods),
        )
        .route(
            "/api/v1/namespaces/{namespace}/pods/{name}",
            get(handlers::pods::get_pod),
        )
        .route(
            "/api/v1/namespaces/{namespace}/pods/{name}/log",
            get(handlers::service_logs),
        )
        .route("/api/v1/pods/{name}/log", get(handlers::service_logs_no_ns))
        .route(
            "/api/v1/namespaces/{namespace}/pods/{name}/exec",
            get(handlers::exec::exec_ws_namespaced).post(handlers::exec::exec_http_post_namespaced),
        )
        .route(
            "/api/v1/pods/{name}/exec",
            get(handlers::exec::exec_ws_cluster).post(handlers::exec::exec_http_post_cluster),
        );

    // Kubernetes core API - Services
    router = router
        .route(
            "/api/v1/services",
            get(handlers::service_resources::list_all),
        )
        .route(
            "/api/v1/namespaces/{namespace}/services",
            get(handlers::service_resources::list_namespaced)
                .post(handlers::service_resources::create),
        )
        .route(
            "/api/v1/namespaces/{namespace}/services/{name}",
            get(handlers::service_resources::get).delete(handlers::service_resources::delete),
        );

    // Kubernetes core API - Endpoints
    router = router
        .route("/api/v1/endpoints", get(handlers::endpoints::list_all))
        .route(
            "/api/v1/namespaces/{namespace}/endpoints",
            get(handlers::endpoints::list_namespaced),
        )
        .route(
            "/api/v1/namespaces/{namespace}/endpoints/{name}",
            get(handlers::endpoints::get),
        );

    // Kubernetes core API - ConfigMaps
    router = router
        .route("/api/v1/configmaps", get(handlers::list_configmaps_all))
        .route(
            "/api/v1/namespaces/{namespace}/configmaps",
            get(handlers::list_configmaps).post(handlers::create_configmap),
        )
        .route(
            "/api/v1/namespaces/{namespace}/configmaps/{name}",
            get(handlers::get_configmap)
                .put(handlers::replace_configmap)
                .delete(handlers::delete_configmap),
        );

    // Kubernetes core API - Secrets
    router = router
        .route("/api/v1/secrets", get(handlers::secrets::list_all))
        .route(
            "/api/v1/namespaces/{namespace}/secrets",
            get(handlers::secrets::list_namespace).post(handlers::secrets::create),
        )
        .route(
            "/api/v1/namespaces/{namespace}/secrets/{name}",
            get(handlers::secrets::get)
                .put(handlers::secrets::replace)
                .delete(handlers::secrets::delete),
        );

    // Kubernetes core API - PersistentVolumeClaims
    router = router
        .route(
            "/api/v1/persistentvolumeclaims",
            get(handlers::pvcs::list_all),
        )
        .route(
            "/api/v1/namespaces/{namespace}/persistentvolumeclaims",
            get(handlers::pvcs::list_namespaced),
        )
        .route(
            "/api/v1/namespaces/{namespace}/persistentvolumeclaims/{name}",
            get(handlers::pvcs::get),
        );

    // Kubernetes apps API - StatefulSets
    router = router
        .route(
            "/apis/apps/v1/statefulsets",
            get(handlers::statefulsets::list_all),
        )
        .route(
            "/apis/apps/v1/namespaces/{namespace}/statefulsets",
            get(handlers::statefulsets::list_namespaced).post(handlers::statefulsets::create),
        )
        .route(
            "/apis/apps/v1/namespaces/{namespace}/statefulsets/{name}",
            get(handlers::statefulsets::get).delete(handlers::statefulsets::delete),
        );

    // Kubernetes apps API - Deployments
    router = router
        .route(
            "/apis/apps/v1/deployments",
            get(handlers::deployments::list_all),
        )
        .route(
            "/apis/apps/v1/namespaces/{namespace}/deployments",
            get(handlers::deployments::list_namespaced),
        )
        .route(
            "/apis/apps/v1/namespaces/{namespace}/deployments/{name}",
            get(handlers::deployments::get).delete(handlers::deployments::delete),
        );

    // Kubernetes apps API - DaemonSets
    router = router
        .route(
            "/apis/apps/v1/daemonsets",
            get(handlers::daemonsets::list_all),
        )
        .route(
            "/apis/apps/v1/namespaces/{namespace}/daemonsets",
            get(handlers::daemonsets::list_namespaced),
        )
        .route(
            "/apis/apps/v1/namespaces/{namespace}/daemonsets/{name}",
            get(handlers::daemonsets::get).delete(handlers::daemonsets::delete),
        );

    // Kubernetes apps API - ReplicaSets
    router = router
        .route(
            "/apis/apps/v1/replicasets",
            get(handlers::replicasets::list_all),
        )
        .route(
            "/apis/apps/v1/namespaces/{namespace}/replicasets",
            get(handlers::replicasets::list_namespaced),
        )
        .route(
            "/apis/apps/v1/namespaces/{namespace}/replicasets/{name}",
            get(handlers::replicasets::get),
        );

    // Kubernetes batch API - Jobs
    router = router
        .route("/apis/batch/v1/jobs", get(handlers::jobs::list_all))
        .route(
            "/apis/batch/v1/namespaces/{namespace}/jobs",
            get(handlers::jobs::list_namespaced),
        )
        .route(
            "/apis/batch/v1/namespaces/{namespace}/jobs/{name}",
            get(handlers::jobs::get).delete(handlers::jobs::delete),
        );

    // Health and observability routes
    router = router
        .route("/metrics", get(handlers::metrics))
        .route("/healthz", get(handlers::healthz))
        .route("/readyz", get(handlers::readyz))
        .route("/livez", get(handlers::livez));

    // Internal/debug routes
    router = router
        .route("/v1/dns/registry", get(handlers::dns::dump_registry))
        .route("/v1/setup", post(handlers::setup))
        .route("/v1/ca", post(handlers::issue_certificate))
        .route(
            "/v1/serviceaccounts/token",
            post(handlers::exchange_serviceaccount_token),
        )
        .route(
            "/v1/networkpolicies/debug",
            get(handlers::networkpolicy_debug),
        )
        .route("/v1/openapi.json", get(handlers::openapi_spec));

    router
}
