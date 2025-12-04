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

#![allow(unused_imports)]

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
mod services;
mod snapshots;
mod statefulsets;

pub use bundles::{
    delete_bundle, delete_bundle_field_ownership, list_bundles, load_bundle_field_ownership,
    save_bundle, save_bundle_field_ownership,
};
pub use common::{
    bump_resource_version, delete_ownership, deserialize_from_store, ensure_resource_version,
    load_ownership, normalize_namespace, save_ownership, serialization_format_for,
    serialize_for_store, write_atomic_files, HotResourceCache, HotResourceCacheMetrics,
    SerializationFormat,
};
pub use configmaps::{delete_config_map, list_config_maps, load_config_map, save_config_map};
pub use daemonsets::{
    delete_daemonset, get_daemon_set, list_daemon_sets, list_daemon_sets_for, StoredDaemonSet,
};
pub use deployments::{
    delete_deployment, get_deployment, list_deployments, list_deployments_for, StoredDeployment,
};
pub use devices::{delete_device, list_devices, save_device};
pub use endpoints::{delete_endpoints, endpoints_cache_metrics, list_endpoints, save_endpoints};
pub use jobs::{delete_job, get_job, list_jobs, list_jobs_for, StoredJob};
pub use network_policies::{list_network_policies, StoredNetworkPolicy};
pub use pagination::{
    decode_continue_token, encode_continue_token, paginate_entries, ListCursor, PaginatedResult,
    PaginationError,
};
pub use pods::{
    delete_pod_manifest, list_pod_manifests, load_pod_manifest, save_pod_manifest, StoredPod,
};
pub use replicasets::{get_replica_set, list_replica_sets, replicaset_from_desired_state};
pub use services::{delete_service, list_services, save_service, service_cache_metrics};
pub use snapshots::{delete_volume_snapshot, list_volume_snapshots, save_volume_snapshot};
pub use statefulsets::{
    delete, get_stateful_set, list_stateful_sets, list_stateful_sets_for, load, save,
    StoredStatefulSet,
};
