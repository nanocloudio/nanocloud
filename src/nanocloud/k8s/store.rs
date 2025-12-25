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

mod bundles;
mod common;
mod configmaps;
mod daemonsets;
mod deployments;
mod devices;
mod endpoints;
mod jobs;
mod network_policies;
mod pagination;
mod pods;
mod replicasets;
#[cfg(feature = "edge")]
mod routes;
mod services;
mod snapshots;
mod statefulsets;
#[cfg(feature = "edge")]
mod webhooks;

pub use bundles::{
    delete_bundle, delete_bundle_field_ownership, list_bundles, load_bundle_field_ownership,
    save_bundle, save_bundle_field_ownership,
};
pub use common::normalize_namespace;
pub use configmaps::{delete_config_map, list_config_maps, load_config_map, save_config_map};
pub use daemonsets::{delete_daemonset, get_daemon_set, list_daemon_sets_for};
pub use deployments::{delete_deployment, get_deployment, list_deployments_for};
pub use devices::{delete_device, list_devices, save_device};
pub use endpoints::{delete_endpoints, list_endpoints, save_endpoints};
pub use jobs::{delete_job, get_job, list_jobs_for};
pub use network_policies::{list_network_policies, StoredNetworkPolicy};
pub use pagination::{
    decode_continue_token, encode_continue_token, paginate_entries, ListCursor, PaginatedResult,
    PaginationError,
};
pub use pods::{
    delete_pod_manifest, list_pod_manifests, load_pod_manifest, save_pod_manifest, StoredPod,
};
pub use replicasets::{get_replica_set, list_replica_sets, replicaset_from_desired_state};
#[cfg(feature = "edge")]
pub use routes::{delete_route, get_route, list_routes, list_routes_for, save_route, StoredRoute};
pub use services::{delete_service, list_services, save_service};
pub use snapshots::{delete_volume_snapshot, list_volume_snapshots, save_volume_snapshot};
pub use statefulsets::{
    delete, get_stateful_set, list_stateful_sets, list_stateful_sets_for, load, save,
    StoredStatefulSet,
};
#[cfg(feature = "edge")]
pub use webhooks::{
    delete_webhook, get_webhook, list_webhooks, list_webhooks_for, save_webhook, StoredWebhook,
};
