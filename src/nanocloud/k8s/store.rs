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

use crate::nanocloud::api::types::{Bundle, Device, VolumeSnapshot};
use crate::nanocloud::controller::replicaset::{
    short_revision_hash, ReplicaSetDesiredState, ReplicaSetPodAction, LABEL_POD_TEMPLATE_HASH,
    LABEL_REPLICASET_NAME, LABEL_STATEFULSET_NAME,
};
use crate::nanocloud::k8s::configmap::ConfigMap;
use crate::nanocloud::k8s::daemonset::DaemonSet;
use crate::nanocloud::k8s::deployment::Deployment;
use crate::nanocloud::k8s::endpoints::Endpoints;
use crate::nanocloud::k8s::job::Job;
use crate::nanocloud::k8s::networkpolicy::NetworkPolicy;
use crate::nanocloud::k8s::ownership::BundleFieldOwnership;
use crate::nanocloud::k8s::pod::{ObjectMeta, Pod};
use crate::nanocloud::k8s::replicaset::{ReplicaSet, ReplicaSetSpec, ReplicaSetStatus};
use crate::nanocloud::k8s::service::Service;
use crate::nanocloud::k8s::statefulset::{LabelSelector, PodTemplateSpec, StatefulSet};
use crate::nanocloud::logger::log_warn;
use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::{is_missing_value_error, Keyspace};
use crate::nanocloud::Config;

use base64::engine::general_purpose::STANDARD as BASE64_ENGINE;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json;
use std::error::Error;
use std::fs;
use std::io::ErrorKind;
use std::path::PathBuf;

const K8S_KEYSPACE: Keyspace = Keyspace::new("k8s");
const STATEFULSET_PREFIX: &str = "/statefulsets";
const DEPLOYMENT_PREFIX: &str = "/deployments";
const DAEMONSET_PREFIX: &str = "/daemonsets";
const POD_PREFIX: &str = "/pods";
const CONFIGMAP_PREFIX: &str = "/configmaps";
const ENDPOINTS_PREFIX: &str = "/endpoints";
const SERVICE_PREFIX: &str = "/services";
const BUNDLE_PREFIX: &str = "/bundles";
const BUNDLE_OWNER_FILE: &str = "_owners.json";
const DEVICE_PREFIX: &str = "/devices";
const SNAPSHOT_PREFIX: &str = "/volumesnapshots";
const JOB_PREFIX: &str = "/jobs";
const KEYSPACE_VALUE_FILE: &str = "_value_";
const CONTROLLER_KEYSPACE_ROOT: &str = "controllers";
const REPLICASET_DIR: &str = "replicasets";
const POD_IP_ANNOTATION: &str = "nanocloud.io/pod-ip";
const NETWORK_POLICY_DIR: &str = "networkpolicies";

#[derive(Debug)]
pub struct StoredStatefulSet {
    pub namespace: Option<String>,
    pub name: String,
    pub workload: StatefulSet,
}

#[derive(Debug)]
pub struct StoredDeployment {
    pub namespace: Option<String>,
    pub name: String,
    pub workload: Deployment,
}

#[derive(Debug)]
pub struct StoredDaemonSet {
    pub namespace: Option<String>,
    pub name: String,
    pub workload: DaemonSet,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paginate_entries_without_limit_returns_all() {
        let entries = vec![
            ("ns-a/item1".to_string(), 1, Some("10".to_string())),
            ("ns-a/item2".to_string(), 2, Some("11".to_string())),
        ];
        let result = paginate_entries(entries, None, None).expect("should succeed");
        assert_eq!(result.items, vec![1, 2]);
        assert!(result.next_cursor.is_none());
        assert_eq!(result.remaining, 0);
    }

    #[test]
    fn paginate_entries_with_limit_sets_cursor() {
        let entries = vec![
            ("a/1".to_string(), 1, Some("1".to_string())),
            ("a/2".to_string(), 2, Some("2".to_string())),
            ("a/3".to_string(), 3, Some("3".to_string())),
        ];
        let result = paginate_entries(entries, None, Some(2)).expect("should succeed");
        assert_eq!(result.items, vec![1, 2]);
        assert_eq!(result.remaining, 1);
        let cursor = result.next_cursor.expect("cursor expected");
        assert_eq!(cursor.key, "a/2");
        assert_eq!(cursor.resource_version.as_deref(), Some("2"));
    }

    #[test]
    fn zero_limit_is_rejected() {
        let entries = vec![("a/1".to_string(), 1, None)];
        let err = paginate_entries(entries, None, Some(0)).unwrap_err();
        match err {
            PaginationError::InvalidLimit(msg) => assert!(msg.contains("greater than 0")),
            _ => panic!("expected InvalidLimit"),
        }
    }

    #[test]
    fn decode_invalid_token_errors() {
        let err = decode_continue_token("not-base64", "pods").unwrap_err();
        match err {
            PaginationError::InvalidContinue(msg) => {
                assert!(msg.contains("base64"));
            }
            PaginationError::InvalidLimit(_) => panic!("expected invalid continue error"),
        }
    }
}

#[derive(Debug)]
pub struct StoredPod {
    pub namespace: Option<String>,
    pub name: String,
    pub workload: Pod,
}

#[derive(Debug)]
pub struct StoredNetworkPolicy {
    pub namespace: Option<String>,
    pub name: String,
    pub policy: NetworkPolicy,
}

#[derive(Debug)]
pub struct StoredJob {
    pub namespace: Option<String>,
    pub name: String,
    pub job: Job,
}

pub(crate) fn normalize_namespace(namespace: Option<&str>) -> String {
    namespace
        .filter(|ns| !ns.is_empty())
        .unwrap_or("default")
        .to_string()
}

fn make_statefulset_key(namespace: Option<&str>, app: &str) -> String {
    let ns = normalize_namespace(namespace);
    format!("{}/{}/{}", STATEFULSET_PREFIX, ns, app)
}

fn make_pod_key(namespace: Option<&str>, app: &str) -> String {
    let ns = normalize_namespace(namespace);
    format!("{}/{}/{}", POD_PREFIX, ns, app)
}

fn make_deployment_key(namespace: Option<&str>, name: &str) -> String {
    let ns = normalize_namespace(namespace);
    format!("{}/{}/{}", DEPLOYMENT_PREFIX, ns, name)
}

fn make_daemonset_key(namespace: Option<&str>, name: &str) -> String {
    let ns = normalize_namespace(namespace);
    format!("{}/{}/{}", DAEMONSET_PREFIX, ns, name)
}

fn make_configmap_key(namespace: Option<&str>, name: &str) -> String {
    let ns = normalize_namespace(namespace);
    format!("{}/{}/{}", CONFIGMAP_PREFIX, ns, name)
}

fn make_endpoints_key(namespace: Option<&str>, name: &str) -> String {
    let ns = normalize_namespace(namespace);
    format!("{}/{}/{}", ENDPOINTS_PREFIX, ns, name)
}

fn make_service_key(namespace: Option<&str>, name: &str) -> String {
    let ns = normalize_namespace(namespace);
    format!("{}/{}/{}", SERVICE_PREFIX, ns, name)
}

fn make_job_key(namespace: Option<&str>, name: &str) -> String {
    let ns = normalize_namespace(namespace);
    format!("{}/{}/{}", JOB_PREFIX, ns, name)
}

fn make_bundle_key(namespace: Option<&str>, name: &str) -> String {
    let ns = normalize_namespace(namespace);
    format!("{}/{}/{}", BUNDLE_PREFIX, ns, name)
}

fn make_device_key(namespace: Option<&str>, name: &str) -> String {
    let ns = normalize_namespace(namespace);
    format!("{}/{}/{}", DEVICE_PREFIX, ns, name)
}

fn make_snapshot_key(namespace: Option<&str>, name: &str) -> String {
    let ns = normalize_namespace(namespace);
    format!("{}/{}/{}", SNAPSHOT_PREFIX, ns, name)
}

fn configmap_root() -> PathBuf {
    Config::Keyspace.get_path().join("k8s").join("configmaps")
}

fn statefulset_root() -> PathBuf {
    Config::Keyspace.get_path().join("k8s").join("statefulsets")
}

fn deployment_root() -> PathBuf {
    Config::Keyspace.get_path().join("k8s").join("deployments")
}

fn daemonset_root() -> PathBuf {
    Config::Keyspace.get_path().join("k8s").join("daemonsets")
}

fn pod_root() -> PathBuf {
    Config::Keyspace.get_path().join("k8s").join("pods")
}

fn bundle_root() -> PathBuf {
    Config::Keyspace.get_path().join("k8s").join("bundles")
}

fn bundle_owner_path(namespace: Option<&str>, name: &str) -> PathBuf {
    bundle_root()
        .join(normalize_namespace(namespace))
        .join(name)
        .join(BUNDLE_OWNER_FILE)
}

fn device_root() -> PathBuf {
    Config::Keyspace.get_path().join("k8s").join("devices")
}

fn snapshot_root() -> PathBuf {
    Config::Keyspace
        .get_path()
        .join("k8s")
        .join("volumesnapshots")
}

fn job_root() -> PathBuf {
    Config::Keyspace.get_path().join("k8s").join("jobs")
}

fn controller_root() -> PathBuf {
    Config::Keyspace.get_path().join(CONTROLLER_KEYSPACE_ROOT)
}

fn replicaset_root() -> PathBuf {
    controller_root().join(REPLICASET_DIR)
}

fn service_root() -> PathBuf {
    Config::Keyspace.get_path().join("k8s").join("services")
}

fn network_policy_root() -> PathBuf {
    Config::Keyspace
        .get_path()
        .join("k8s")
        .join(NETWORK_POLICY_DIR)
}

pub fn list_stateful_sets() -> Result<Vec<StoredStatefulSet>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = statefulset_root();
    let namespace_entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
        Err(err) => {
            return Err(with_context(
                err,
                format!(
                    "Failed to read StatefulSet root directory '{}'",
                    root.display()
                ),
            ))
        }
    };

    for namespace_entry in namespace_entries {
        let namespace_entry = namespace_entry.map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to iterate StatefulSet namespaces in '{}'",
                    root.display()
                ),
            )
        })?;
        let file_type = namespace_entry.file_type().map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to inspect StatefulSet namespace entry '{}'",
                    namespace_entry.path().display()
                ),
            )
        })?;
        if !file_type.is_dir() {
            continue;
        }
        let namespace_name = match namespace_entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };

        let namespace_path = namespace_entry.path();
        let service_entries = fs::read_dir(&namespace_path).map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to read StatefulSet namespace directory '{}'",
                    namespace_path.display()
                ),
            )
        })?;

        for service_entry in service_entries {
            let service_entry = service_entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate StatefulSet directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            let entry_type = service_entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect StatefulSet entry '{}'",
                        service_entry.path().display()
                    ),
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let service_name = match service_entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = service_entry.path().join(KEYSPACE_VALUE_FILE);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!(
                            "Failed to load StatefulSet payload '{}'",
                            value_path.display()
                        ),
                    ))
                }
            };

            let mut workload: StatefulSet = serde_json::from_str(&raw).map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to deserialize StatefulSet '{}' from '{}'",
                        service_name,
                        value_path.display()
                    ),
                )
            })?;

            if workload.metadata.name.is_none() {
                workload.metadata.name = Some(service_name.clone());
            }

            let namespace = workload
                .metadata
                .namespace
                .clone()
                .filter(|ns| !ns.is_empty())
                .or_else(|| {
                    if namespace_name == "default" {
                        None
                    } else {
                        Some(namespace_name.clone())
                    }
                });

            results.push(StoredStatefulSet {
                namespace,
                name: service_name,
                workload,
            });
        }
    }

    Ok(results)
}

pub fn list_deployments() -> Result<Vec<StoredDeployment>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = deployment_root();
    let namespace_entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
        Err(err) => {
            return Err(with_context(
                err,
                format!(
                    "Failed to read Deployment root directory '{}'",
                    root.display()
                ),
            ))
        }
    };

    for namespace_entry in namespace_entries {
        let namespace_entry = namespace_entry.map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to iterate Deployment namespaces in '{}'",
                    root.display()
                ),
            )
        })?;
        let file_type = namespace_entry.file_type().map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to inspect Deployment namespace entry '{}'",
                    namespace_entry.path().display()
                ),
            )
        })?;
        if !file_type.is_dir() {
            continue;
        }
        let namespace_name = match namespace_entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };

        let namespace_path = namespace_entry.path();
        let deployment_entries = fs::read_dir(&namespace_path).map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to read Deployment namespace directory '{}'",
                    namespace_path.display()
                ),
            )
        })?;

        for deployment_entry in deployment_entries {
            let deployment_entry = deployment_entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate Deployment directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            let entry_type = deployment_entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect Deployment entry '{}'",
                        deployment_entry.path().display()
                    ),
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let deployment_name = match deployment_entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = deployment_entry.path().join(KEYSPACE_VALUE_FILE);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) => {
                    log_warn(
                        "store",
                        "Failed to read Deployment payload",
                        &[
                            ("path", value_path.display().to_string().as_str()),
                            ("error", &err.to_string()),
                        ],
                    );
                    continue;
                }
            };

            let mut workload: Deployment = match serde_json::from_str(&raw) {
                Ok(workload) => workload,
                Err(err) => {
                    log_warn(
                        "store",
                        "Failed to deserialize Deployment",
                        &[
                            ("path", value_path.display().to_string().as_str()),
                            ("error", &err.to_string()),
                        ],
                    );
                    continue;
                }
            };

            if workload.api_version.is_empty() {
                workload.api_version = "apps/v1".to_string();
            }
            if workload.kind.is_empty() {
                workload.kind = "Deployment".to_string();
            }

            if workload.metadata.name.is_none() {
                workload.metadata.name = Some(deployment_name.clone());
            }
            let namespace_opt = Some(namespace_name.clone()).filter(|ns| ns != "default");
            workload.metadata.namespace = Some(normalize_namespace(namespace_opt.as_deref()));
            if workload.metadata.resource_version.is_none() {
                workload.metadata.resource_version = Some("1".to_string());
            }

            results.push(StoredDeployment {
                namespace: namespace_opt,
                name: deployment_name,
                workload,
            });
        }
    }

    Ok(results)
}

pub fn list_daemon_sets() -> Result<Vec<StoredDaemonSet>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = daemonset_root();
    let namespace_entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
        Err(err) => {
            return Err(with_context(
                err,
                format!(
                    "Failed to read DaemonSet root directory '{}'",
                    root.display()
                ),
            ))
        }
    };

    for namespace_entry in namespace_entries {
        let namespace_entry = namespace_entry.map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to iterate DaemonSet namespaces in '{}'",
                    root.display()
                ),
            )
        })?;
        let file_type = namespace_entry.file_type().map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to inspect DaemonSet namespace entry '{}'",
                    namespace_entry.path().display()
                ),
            )
        })?;
        if !file_type.is_dir() {
            continue;
        }
        let namespace_name = match namespace_entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };

        let namespace_path = namespace_entry.path();
        let daemonset_entries = fs::read_dir(&namespace_path).map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to read DaemonSet namespace directory '{}'",
                    namespace_path.display()
                ),
            )
        })?;

        for daemonset_entry in daemonset_entries {
            let daemonset_entry = daemonset_entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate DaemonSet directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            let entry_type = daemonset_entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect DaemonSet entry '{}'",
                        daemonset_entry.path().display()
                    ),
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let daemonset_name = match daemonset_entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = daemonset_entry.path().join(KEYSPACE_VALUE_FILE);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) => {
                    log_warn(
                        "store",
                        "Failed to read DaemonSet payload",
                        &[
                            ("path", value_path.display().to_string().as_str()),
                            ("error", &err.to_string()),
                        ],
                    );
                    continue;
                }
            };

            let mut workload: DaemonSet = match serde_json::from_str(&raw) {
                Ok(workload) => workload,
                Err(err) => {
                    log_warn(
                        "store",
                        "Failed to deserialize DaemonSet",
                        &[
                            ("path", value_path.display().to_string().as_str()),
                            ("error", &err.to_string()),
                        ],
                    );
                    continue;
                }
            };

            if workload.api_version.is_empty() {
                workload.api_version = "apps/v1".to_string();
            }
            if workload.kind.is_empty() {
                workload.kind = "DaemonSet".to_string();
            }

            if workload.metadata.name.is_none() {
                workload.metadata.name = Some(daemonset_name.clone());
            }
            let namespace_opt = Some(namespace_name.clone()).filter(|ns| ns != "default");
            workload.metadata.namespace = Some(normalize_namespace(namespace_opt.as_deref()));
            if workload.metadata.resource_version.is_none() {
                workload.metadata.resource_version = Some("1".to_string());
            }

            results.push(StoredDaemonSet {
                namespace: namespace_opt,
                name: daemonset_name,
                workload,
            });
        }
    }

    Ok(results)
}

pub fn list_jobs() -> Result<Vec<StoredJob>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = job_root();
    let namespace_entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
        Err(err) => {
            return Err(with_context(
                err,
                format!("Failed to read Job root directory '{}'", root.display()),
            ))
        }
    };

    for namespace_entry in namespace_entries {
        let namespace_entry = namespace_entry.map_err(|err| {
            with_context(
                err,
                format!("Failed to iterate Job namespaces in '{}'", root.display()),
            )
        })?;
        let file_type = namespace_entry.file_type().map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to inspect Job namespace entry '{}'",
                    namespace_entry.path().display()
                ),
            )
        })?;
        if !file_type.is_dir() {
            continue;
        }

        let namespace_name = match namespace_entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };
        let namespace_path = namespace_entry.path();

        let job_entries = match fs::read_dir(&namespace_path) {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(with_context(
                    err,
                    format!(
                        "Failed to read Jobs in namespace directory '{}'",
                        namespace_path.display()
                    ),
                ))
            }
        };

        for job_entry in job_entries {
            let job_entry = job_entry.map_err(|err| {
                with_context(
                    err,
                    format!("Failed to iterate Jobs in '{}'", namespace_path.display()),
                )
            })?;
            let entry_type = job_entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect Job entry '{}'",
                        job_entry.path().display()
                    ),
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let job_name = match job_entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = job_entry.path().join(KEYSPACE_VALUE_FILE);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!("Failed to load Job payload '{}'", value_path.display()),
                    ))
                }
            };

            let job: Job = serde_json::from_str(&raw).map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to deserialize Job '{}' from '{}'",
                        job_name,
                        value_path.display()
                    ),
                )
            })?;

            let namespace = Some(namespace_name.clone()).filter(|ns| ns != "default");
            results.push(StoredJob {
                namespace,
                name: job_name,
                job,
            });
        }
    }

    Ok(results)
}

pub fn list_pod_manifests() -> Result<Vec<StoredPod>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = pod_root();
    let namespace_entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
        Err(err) => {
            return Err(with_context(
                err,
                format!("Failed to read Pod root directory '{}'", root.display()),
            ))
        }
    };

    for namespace_entry in namespace_entries {
        let namespace_entry = namespace_entry.map_err(|err| {
            with_context(
                err,
                format!("Failed to iterate Pod namespaces in '{}'", root.display()),
            )
        })?;
        let file_type = namespace_entry.file_type().map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to inspect Pod namespace entry '{}'",
                    namespace_entry.path().display()
                ),
            )
        })?;
        if !file_type.is_dir() {
            continue;
        }
        let namespace_name = match namespace_entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };

        let namespace_path = namespace_entry.path();
        let pod_entries = fs::read_dir(&namespace_path).map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to read Pod namespace directory '{}'",
                    namespace_path.display()
                ),
            )
        })?;

        for pod_entry in pod_entries {
            let pod_entry = pod_entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate Pod directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            let entry_type = pod_entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect Pod entry '{}'",
                        pod_entry.path().display()
                    ),
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let pod_name = match pod_entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = pod_entry.path().join(KEYSPACE_VALUE_FILE);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!("Failed to load Pod payload '{}'", value_path.display()),
                    ))
                }
            };

            let mut workload: Pod = serde_json::from_str(&raw).map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to deserialize Pod '{}' from '{}'",
                        pod_name,
                        value_path.display()
                    ),
                )
            })?;
            workload.status = None;
            if workload.metadata.name.is_none() {
                workload.metadata.name = Some(pod_name.clone());
            }
            let namespace = workload
                .metadata
                .namespace
                .clone()
                .filter(|ns| !ns.is_empty())
                .or_else(|| {
                    if namespace_name == "default" {
                        None
                    } else {
                        Some(namespace_name.clone())
                    }
                });

            results.push(StoredPod {
                namespace,
                name: pod_name,
                workload,
            });
        }
    }

    Ok(results)
}

pub fn list_network_policies() -> Result<Vec<StoredNetworkPolicy>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = network_policy_root();
    let namespace_entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
        Err(err) => {
            return Err(with_context(
                err,
                format!(
                    "Failed to read NetworkPolicy root directory '{}'",
                    root.display()
                ),
            ))
        }
    };

    for namespace_entry in namespace_entries {
        let namespace_entry = namespace_entry.map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to iterate NetworkPolicy namespaces in '{}'",
                    root.display()
                ),
            )
        })?;
        let entry_type = namespace_entry.file_type().map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to inspect NetworkPolicy namespace entry '{}'",
                    namespace_entry.path().display()
                ),
            )
        })?;
        if !entry_type.is_dir() {
            continue;
        }
        let namespace_name = match namespace_entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };

        let namespace_path = namespace_entry.path();
        let policy_entries = fs::read_dir(&namespace_path).map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to read NetworkPolicy namespace directory '{}'",
                    namespace_path.display()
                ),
            )
        })?;

        for policy_entry in policy_entries {
            let policy_entry = policy_entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate NetworkPolicy directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            let entry_type = policy_entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect NetworkPolicy entry '{}'",
                        policy_entry.path().display()
                    ),
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let policy_name = match policy_entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = policy_entry.path().join(KEYSPACE_VALUE_FILE);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!(
                            "Failed to read NetworkPolicy value for '{}'",
                            value_path.display()
                        ),
                    ))
                }
            };
            let policy: NetworkPolicy = serde_json::from_str(&raw).map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to parse stored NetworkPolicy '{}'",
                        value_path.display()
                    ),
                )
            })?;

            let namespace = Some(namespace_name.clone()).filter(|ns| ns != "default");
            results.push(StoredNetworkPolicy {
                namespace,
                name: policy_name,
                policy,
            });
        }
    }

    Ok(results)
}

pub fn list_stateful_sets_for(
    namespace: Option<&str>,
) -> Result<Vec<StatefulSet>, Box<dyn Error + Send + Sync>> {
    let filter = namespace.map(|ns| normalize_namespace(Some(ns)));
    let mut filtered = Vec::new();
    for stored in list_stateful_sets()? {
        let namespace_value = normalize_namespace(stored.namespace.as_deref());
        if filter
            .as_ref()
            .is_none_or(|candidate| candidate == &namespace_value)
        {
            let mut workload = stored.workload;
            if workload.metadata.name.is_none() {
                workload.metadata.name = Some(stored.name.clone());
            }
            workload.metadata.namespace = Some(namespace_value.clone());
            if workload.metadata.resource_version.is_none() {
                workload.metadata.resource_version = Some("1".to_string());
            }
            filtered.push(workload);
        }
    }
    Ok(filtered)
}

pub fn list_deployments_for(
    namespace: Option<&str>,
) -> Result<Vec<Deployment>, Box<dyn Error + Send + Sync>> {
    let filter = namespace.map(|ns| normalize_namespace(Some(ns)));
    let mut filtered = Vec::new();
    for stored in list_deployments()? {
        let namespace_value = normalize_namespace(stored.namespace.as_deref());
        if filter
            .as_ref()
            .is_none_or(|candidate| candidate == &namespace_value)
        {
            let mut workload = stored.workload;
            if workload.metadata.name.is_none() {
                workload.metadata.name = Some(stored.name.clone());
            }
            workload.metadata.namespace = Some(namespace_value.clone());
            if workload.metadata.resource_version.is_none() {
                workload.metadata.resource_version = Some("1".to_string());
            }
            filtered.push(workload);
        }
    }
    Ok(filtered)
}

pub fn list_daemon_sets_for(
    namespace: Option<&str>,
) -> Result<Vec<DaemonSet>, Box<dyn Error + Send + Sync>> {
    let filter = namespace.map(|ns| normalize_namespace(Some(ns)));
    let mut filtered = Vec::new();
    for stored in list_daemon_sets()? {
        let namespace_value = normalize_namespace(stored.namespace.as_deref());
        if filter
            .as_ref()
            .is_none_or(|candidate| candidate == &namespace_value)
        {
            let mut workload = stored.workload;
            if workload.metadata.name.is_none() {
                workload.metadata.name = Some(stored.name.clone());
            }
            workload.metadata.namespace = Some(namespace_value.clone());
            if workload.metadata.resource_version.is_none() {
                workload.metadata.resource_version = Some("1".to_string());
            }
            filtered.push(workload);
        }
    }
    Ok(filtered)
}

pub fn list_jobs_for(namespace: Option<&str>) -> Result<Vec<Job>, Box<dyn Error + Send + Sync>> {
    let filter = namespace.map(|ns| normalize_namespace(Some(ns)));
    let mut filtered = Vec::new();
    for stored in list_jobs()? {
        let namespace_value = normalize_namespace(stored.namespace.as_deref());
        if filter
            .as_ref()
            .is_none_or(|candidate| candidate == &namespace_value)
        {
            let mut job = stored.job;
            if job.metadata.name.is_none() {
                job.metadata.name = Some(stored.name.clone());
            }
            job.metadata.namespace = Some(namespace_value.clone());
            if job.metadata.resource_version.is_none() {
                job.metadata.resource_version = Some("1".to_string());
            }
            filtered.push(job);
        }
    }
    Ok(filtered)
}

pub fn get_stateful_set(
    namespace: Option<&str>,
    name: &str,
) -> Result<Option<StatefulSet>, Box<dyn Error + Send + Sync>> {
    let namespace_value = normalize_namespace(namespace);
    let value_path = statefulset_root()
        .join(&namespace_value)
        .join(name)
        .join(KEYSPACE_VALUE_FILE);
    let raw = match fs::read_to_string(&value_path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(with_context(
                err,
                format!(
                    "Failed to load StatefulSet payload '{}'",
                    value_path.display()
                ),
            ))
        }
    };

    let mut workload: StatefulSet = serde_json::from_str(&raw).map_err(|err| {
        with_context(
            err,
            format!(
                "Failed to deserialize StatefulSet '{}' from '{}'",
                name,
                value_path.display()
            ),
        )
    })?;

    if workload.metadata.name.is_none() {
        workload.metadata.name = Some(name.to_string());
    }
    workload.metadata.namespace = Some(namespace_value);

    Ok(Some(workload))
}

pub fn get_deployment(
    namespace: Option<&str>,
    name: &str,
) -> Result<Option<Deployment>, Box<dyn Error + Send + Sync>> {
    let namespace_value = normalize_namespace(namespace);
    let value_path = deployment_root()
        .join(&namespace_value)
        .join(name)
        .join(KEYSPACE_VALUE_FILE);
    let raw = match fs::read_to_string(&value_path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(with_context(
                err,
                format!(
                    "Failed to load Deployment payload '{}'",
                    value_path.display()
                ),
            ))
        }
    };

    let mut workload: Deployment = serde_json::from_str(&raw).map_err(|err| {
        with_context(
            err,
            format!(
                "Failed to deserialize Deployment '{}' from '{}'",
                name,
                value_path.display()
            ),
        )
    })?;

    if workload.api_version.is_empty() {
        workload.api_version = "apps/v1".to_string();
    }
    if workload.kind.is_empty() {
        workload.kind = "Deployment".to_string();
    }

    if workload.metadata.name.is_none() {
        workload.metadata.name = Some(name.to_string());
    }
    workload.metadata.namespace = Some(namespace_value.clone());
    if workload.metadata.resource_version.is_none() {
        workload.metadata.resource_version = Some("1".to_string());
    }

    Ok(Some(workload))
}

pub fn get_daemon_set(
    namespace: Option<&str>,
    name: &str,
) -> Result<Option<DaemonSet>, Box<dyn Error + Send + Sync>> {
    let namespace_value = normalize_namespace(namespace);
    let value_path = daemonset_root()
        .join(&namespace_value)
        .join(name)
        .join(KEYSPACE_VALUE_FILE);
    let raw = match fs::read_to_string(&value_path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(with_context(
                err,
                format!(
                    "Failed to load DaemonSet payload '{}'",
                    value_path.display()
                ),
            ))
        }
    };

    let mut workload: DaemonSet = serde_json::from_str(&raw).map_err(|err| {
        with_context(
            err,
            format!(
                "Failed to deserialize DaemonSet '{}' from '{}'",
                name,
                value_path.display()
            ),
        )
    })?;

    if workload.api_version.is_empty() {
        workload.api_version = "apps/v1".to_string();
    }
    if workload.kind.is_empty() {
        workload.kind = "DaemonSet".to_string();
    }

    if workload.metadata.name.is_none() {
        workload.metadata.name = Some(name.to_string());
    }
    workload.metadata.namespace = Some(namespace_value.clone());
    if workload.metadata.resource_version.is_none() {
        workload.metadata.resource_version = Some("1".to_string());
    }

    Ok(Some(workload))
}

pub fn get_job(
    namespace: Option<&str>,
    name: &str,
) -> Result<Option<Job>, Box<dyn Error + Send + Sync>> {
    let key = make_job_key(namespace, name);
    match K8S_KEYSPACE.get(&key) {
        Ok(raw) => {
            let mut job: Job = serde_json::from_str(&raw).map_err(|err| {
                with_context(err, format!("Failed to parse Job from key '{}'", key))
            })?;
            if job.metadata.name.is_none() {
                job.metadata.name = Some(name.to_string());
            }
            job.metadata.namespace = Some(normalize_namespace(namespace));
            if job.metadata.resource_version.is_none() {
                job.metadata.resource_version = Some("1".to_string());
            }
            Ok(Some(job))
        }
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(None)
            } else {
                Err(with_context(
                    err,
                    format!("Failed to load Job '{}' from keyspace", key),
                ))
            }
        }
    }
}

pub fn list_replica_sets(
    namespace: Option<&str>,
) -> Result<Vec<ReplicaSet>, Box<dyn Error + Send + Sync>> {
    let root = replicaset_root();
    let filter = namespace.map(|ns| normalize_namespace(Some(ns)));
    let namespace_entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(with_context(
                err,
                format!(
                    "Failed to read ReplicaSet root directory '{}'",
                    root.display()
                ),
            ))
        }
    };

    let mut results = Vec::new();
    for namespace_entry in namespace_entries {
        let namespace_entry = namespace_entry.map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to iterate ReplicaSet namespaces in '{}'",
                    root.display()
                ),
            )
        })?;
        let file_type = namespace_entry.file_type().map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to inspect ReplicaSet namespace entry '{}'",
                    namespace_entry.path().display()
                ),
            )
        })?;
        if !file_type.is_dir() {
            continue;
        }

        let namespace_name = match namespace_entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };

        if filter
            .as_ref()
            .is_some_and(|candidate| candidate != &namespace_name)
        {
            continue;
        }

        let namespace_path = namespace_entry.path();
        let replicaset_entries = fs::read_dir(&namespace_path).map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to read ReplicaSet namespace directory '{}'",
                    namespace_path.display()
                ),
            )
        })?;

        for replicaset_entry in replicaset_entries {
            let replicaset_entry = replicaset_entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate ReplicaSet directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            let entry_type = replicaset_entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect ReplicaSet entry '{}'",
                        replicaset_entry.path().display()
                    ),
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let replicaset_name = match replicaset_entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = replicaset_entry.path().join(KEYSPACE_VALUE_FILE);
            if let Some(replica) = load_replicaset(&namespace_name, &replicaset_name, &value_path)?
            {
                results.push(replica);
            }
        }
    }

    Ok(results)
}

pub fn get_replica_set(
    namespace: Option<&str>,
    name: &str,
) -> Result<Option<ReplicaSet>, Box<dyn Error + Send + Sync>> {
    let namespace_value = normalize_namespace(namespace);
    let value_path = replicaset_root()
        .join(&namespace_value)
        .join(name)
        .join(KEYSPACE_VALUE_FILE);
    load_replicaset(&namespace_value, name, &value_path)
}

fn load_replicaset(
    namespace: &str,
    name: &str,
    value_path: &PathBuf,
) -> Result<Option<ReplicaSet>, Box<dyn Error + Send + Sync>> {
    let raw = match fs::read_to_string(value_path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(with_context(
                err,
                format!(
                    "Failed to load ReplicaSet payload '{}'",
                    value_path.display()
                ),
            ))
        }
    };

    let desired: ReplicaSetDesiredState = serde_json::from_str(&raw).map_err(|err| {
        with_context(
            err,
            format!(
                "Failed to deserialize ReplicaSet '{}' from '{}'",
                name,
                value_path.display()
            ),
        )
    })?;

    build_replicaset(namespace.to_string(), name.to_string(), desired).map(Some)
}

fn build_replicaset(
    namespace: String,
    name: String,
    desired: ReplicaSetDesiredState,
) -> Result<ReplicaSet, Box<dyn Error + Send + Sync>> {
    let namespace_option = if namespace == "default" {
        None
    } else {
        Some(namespace.as_str())
    };

    let owning_statefulset: Option<String> = desired
        .owner
        .as_ref()
        .filter(|owner| owner.kind == "StatefulSet")
        .map(|owner| owner.name.clone());

    let statefulset_spec = if let Some(owner_name) = owning_statefulset.as_ref() {
        get_stateful_set(namespace_option, owner_name)?
    } else {
        None
    };

    let (mut selector, mut template) = match statefulset_spec {
        Some(workload) => (
            workload.spec.selector.clone(),
            workload.spec.template.clone(),
        ),
        None => {
            let mut selector = LabelSelector::default();
            selector
                .match_labels
                .insert(LABEL_REPLICASET_NAME.to_string(), name.clone());
            let mut template = PodTemplateSpec::default();
            template
                .metadata
                .labels
                .insert(LABEL_REPLICASET_NAME.to_string(), name.clone());
            (selector, template)
        }
    };

    let template_hash = short_revision_hash(&desired.revision);

    if let Some(first) = desired.pods.first() {
        template
            .metadata
            .labels
            .extend(first.identity.labels.clone());
        template
            .metadata
            .annotations
            .extend(first.identity.annotations.clone());
    }

    template
        .metadata
        .labels
        .insert(LABEL_REPLICASET_NAME.to_string(), name.clone());
    template
        .metadata
        .labels
        .insert(LABEL_POD_TEMPLATE_HASH.to_string(), template_hash.clone());
    if let Some(statefulset_name) = owning_statefulset.clone() {
        template
            .metadata
            .labels
            .insert(LABEL_STATEFULSET_NAME.to_string(), statefulset_name);
    }

    selector
        .match_labels
        .insert(LABEL_REPLICASET_NAME.to_string(), name.clone());
    selector
        .match_labels
        .insert(LABEL_POD_TEMPLATE_HASH.to_string(), template_hash.clone());
    if let Some(statefulset_name) = owning_statefulset.as_ref() {
        selector
            .match_labels
            .entry(LABEL_STATEFULSET_NAME.to_string())
            .or_insert_with(|| statefulset_name.clone());
    }

    let mut metadata = ObjectMeta::default();
    metadata.name = Some(name.clone());
    metadata.namespace = Some(namespace.clone());
    metadata.labels = template.metadata.labels.clone();
    metadata
        .labels
        .entry(LABEL_REPLICASET_NAME.to_string())
        .or_insert_with(|| name.clone());
    metadata
        .labels
        .entry(LABEL_POD_TEMPLATE_HASH.to_string())
        .or_insert_with(|| template_hash.clone());
    if let Some(statefulset_name) = owning_statefulset {
        metadata
            .labels
            .entry(LABEL_STATEFULSET_NAME.to_string())
            .or_insert(statefulset_name);
    }
    metadata.annotations = template.metadata.annotations.clone();

    let spec = ReplicaSetSpec {
        replicas: desired.pods.len() as i32,
        selector,
        template,
    };

    let ready = desired
        .pods
        .iter()
        .filter(|pod| matches!(pod.action, ReplicaSetPodAction::Retain))
        .count() as i32;

    let status = ReplicaSetStatus {
        replicas: Some(desired.pods.len() as i32),
        ready_replicas: Some(ready),
        available_replicas: Some(ready),
        fully_labeled_replicas: Some(ready),
    };

    Ok(ReplicaSet {
        api_version: "apps/v1".to_string(),
        kind: "ReplicaSet".to_string(),
        metadata,
        spec,
        status: Some(status),
    })
}

pub fn replicaset_from_desired_state(
    namespace: &str,
    name: &str,
    desired: ReplicaSetDesiredState,
) -> Result<ReplicaSet, Box<dyn Error + Send + Sync>> {
    build_replicaset(namespace.to_string(), name.to_string(), desired)
}

pub fn list_devices(namespace: Option<&str>) -> Result<Vec<Device>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = device_root();

    let mut namespaces: Vec<String> = Vec::new();
    if let Some(ns) = namespace {
        namespaces.push(normalize_namespace(Some(ns)));
    } else {
        match fs::read_dir(&root) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry.map_err(|err| {
                        with_context(
                            err,
                            format!(
                                "Failed to iterate Device namespaces in '{}'",
                                root.display()
                            ),
                        )
                    })?;
                    if !entry
                        .file_type()
                        .map_err(|err| {
                            with_context(
                                err,
                                format!(
                                    "Failed to inspect Device namespace entry '{}'",
                                    entry.path().display()
                                ),
                            )
                        })?
                        .is_dir()
                    {
                        continue;
                    }
                    if let Ok(name) = entry.file_name().into_string() {
                        namespaces.push(name);
                    }
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
            Err(err) => {
                return Err(with_context(
                    err,
                    format!("Failed to read Device root directory '{}'", root.display()),
                ))
            }
        }
    }

    for ns in namespaces {
        let namespace_path = root.join(&ns);
        let entries = match fs::read_dir(&namespace_path) {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(with_context(
                    err,
                    format!(
                        "Failed to read Device namespace directory '{}'",
                        namespace_path.display()
                    ),
                ))
            }
        };

        for entry in entries {
            let entry = entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate Device directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            let entry_type = entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect Device entry '{}'",
                        entry.path().display()
                    ),
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let name = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = entry.path().join(KEYSPACE_VALUE_FILE);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!("Failed to load Device payload '{}'", value_path.display()),
                    ))
                }
            };

            let mut device: Device = serde_json::from_str(&raw).map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to deserialize Device '{}' from '{}'",
                        name,
                        value_path.display()
                    ),
                )
            })?;

            if device.metadata.name.is_none() {
                device.metadata.name = Some(name.clone());
            }
            if device.metadata.namespace.is_none() {
                device.metadata.namespace = Some(ns.clone());
            }
            if device.metadata.resource_version.is_none() {
                device.metadata.resource_version = Some("1".to_string());
            }
            if device.spec.certificate_subject.trim().is_empty() {
                device.spec.certificate_subject = format!("device:{}", device.spec.hash);
            }

            results.push(device);
        }
    }

    Ok(results)
}

pub fn list_bundles(namespace: Option<&str>) -> Result<Vec<Bundle>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = bundle_root();

    let mut namespaces: Vec<String> = Vec::new();
    if let Some(ns) = namespace {
        namespaces.push(normalize_namespace(Some(ns)));
    } else {
        match fs::read_dir(&root) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry.map_err(|err| {
                        with_context(
                            err,
                            format!(
                                "Failed to iterate Bundle namespaces in '{}'",
                                root.display()
                            ),
                        )
                    })?;
                    if !entry
                        .file_type()
                        .map_err(|err| {
                            with_context(
                                err,
                                format!(
                                    "Failed to inspect Bundle namespace entry '{}'",
                                    entry.path().display()
                                ),
                            )
                        })?
                        .is_dir()
                    {
                        continue;
                    }
                    if let Ok(name) = entry.file_name().into_string() {
                        namespaces.push(name);
                    }
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
            Err(err) => {
                return Err(with_context(
                    err,
                    format!("Failed to read Bundle root directory '{}'", root.display()),
                ))
            }
        }
    }

    for ns in namespaces {
        let namespace_path = root.join(&ns);
        let entries = match fs::read_dir(&namespace_path) {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(with_context(
                    err,
                    format!(
                        "Failed to read Bundle namespace directory '{}'",
                        namespace_path.display()
                    ),
                ))
            }
        };

        for entry in entries {
            let entry = entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate Bundle directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            let entry_type = entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect Bundle entry '{}'",
                        entry.path().display()
                    ),
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let name = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = entry.path().join(KEYSPACE_VALUE_FILE);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!("Failed to load Bundle payload '{}'", value_path.display()),
                    ))
                }
            };

            let mut bundle: Bundle = serde_json::from_str(&raw).map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to deserialize Bundle '{}' from '{}'",
                        name,
                        value_path.display()
                    ),
                )
            })?;

            if bundle.metadata.name.is_none() {
                bundle.metadata.name = Some(name.clone());
            }
            if bundle.metadata.namespace.is_none() {
                bundle.metadata.namespace = Some(ns.clone());
            }
            if bundle.metadata.resource_version.is_none() {
                bundle.metadata.resource_version = Some("1".to_string());
            }

            results.push(bundle);
        }
    }

    Ok(results)
}

pub fn load_bundle_field_ownership(
    namespace: Option<&str>,
    name: &str,
) -> Result<BundleFieldOwnership, Box<dyn Error + Send + Sync>> {
    let path = bundle_owner_path(namespace, name);
    match fs::read_to_string(&path) {
        Ok(contents) => serde_json::from_str(&contents).map_err(|err| {
            with_context(
                err,
                format!("Failed to parse Bundle ownership '{}'", path.display()),
            )
        }),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(BundleFieldOwnership::default()),
        Err(err) => Err(with_context(
            err,
            format!(
                "Failed to read Bundle ownership metadata '{}'",
                path.display()
            ),
        )),
    }
}

pub fn save_bundle_field_ownership(
    namespace: Option<&str>,
    name: &str,
    ownership: &BundleFieldOwnership,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let path = bundle_owner_path(namespace, name);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to create Bundle ownership directory '{}'",
                    parent.display()
                ),
            )
        })?;
    }
    let payload = serde_json::to_string_pretty(ownership).map_err(|err| {
        with_context(
            err,
            format!(
                "Failed to serialize Bundle ownership metadata for '{}/{}'",
                namespace.unwrap_or("default"),
                name
            ),
        )
    })?;
    fs::write(&path, payload).map_err(|err| {
        with_context(
            err,
            format!(
                "Failed to persist Bundle ownership metadata '{}'",
                path.display()
            ),
        )
    })
}

pub fn delete_bundle_field_ownership(
    namespace: Option<&str>,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let path = bundle_owner_path(namespace, name);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(with_context(
            err,
            format!(
                "Failed to delete Bundle ownership metadata '{}'",
                path.display()
            ),
        )),
    }
}

pub fn list_services(
    namespace: Option<&str>,
) -> Result<Vec<Service>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = service_root();

    let mut namespaces: Vec<String> = Vec::new();
    if let Some(ns) = namespace {
        namespaces.push(normalize_namespace(Some(ns)));
    } else {
        match fs::read_dir(&root) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry.map_err(|err| {
                        with_context(
                            err,
                            format!(
                                "Failed to iterate Service namespaces in '{}'",
                                root.display()
                            ),
                        )
                    })?;
                    if !entry
                        .file_type()
                        .map_err(|err| {
                            with_context(
                                err,
                                format!(
                                    "Failed to inspect Service namespace entry '{}'",
                                    entry.path().display()
                                ),
                            )
                        })?
                        .is_dir()
                    {
                        continue;
                    }
                    if let Ok(name) = entry.file_name().into_string() {
                        namespaces.push(name);
                    }
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
            Err(err) => {
                return Err(with_context(
                    err,
                    format!("Failed to read Service root directory '{}'", root.display()),
                ))
            }
        }
    }

    for ns in namespaces {
        let namespace_path = root.join(&ns);
        let entries = match fs::read_dir(&namespace_path) {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(with_context(
                    err,
                    format!(
                        "Failed to read Service namespace directory '{}'",
                        namespace_path.display()
                    ),
                ))
            }
        };

        for entry in entries {
            let entry = entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate Service directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            if !entry
                .file_type()
                .map_err(|err| {
                    with_context(
                        err,
                        format!(
                            "Failed to inspect Service entry '{}'",
                            entry.path().display()
                        ),
                    )
                })?
                .is_dir()
            {
                continue;
            }

            let name = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = entry.path().join(KEYSPACE_VALUE_FILE);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!("Failed to load Service payload '{}'", value_path.display()),
                    ))
                }
            };

            let mut service: Service = serde_json::from_str(&raw).map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to deserialize Service '{}' from '{}'",
                        name,
                        value_path.display()
                    ),
                )
            })?;

            if service.metadata.name.is_none() {
                service.metadata.name = Some(name.clone());
            }
            if service.metadata.namespace.is_none() {
                service.metadata.namespace = Some(ns.clone());
            }
            if service.metadata.resource_version.is_none() {
                service.metadata.resource_version = Some("1".to_string());
            }

            results.push(service);
        }
    }

    Ok(results)
}

pub fn list_volume_snapshots(
    namespace: Option<&str>,
) -> Result<Vec<VolumeSnapshot>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = snapshot_root();

    let mut namespaces: Vec<String> = Vec::new();
    if let Some(ns) = namespace {
        namespaces.push(normalize_namespace(Some(ns)));
    } else {
        match fs::read_dir(&root) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry.map_err(|err| {
                        with_context(
                            err,
                            format!(
                                "Failed to iterate VolumeSnapshot namespaces in '{}'",
                                root.display()
                            ),
                        )
                    })?;
                    if !entry
                        .file_type()
                        .map_err(|err| {
                            with_context(
                                err,
                                format!(
                                    "Failed to inspect VolumeSnapshot namespace entry '{}'",
                                    entry.path().display()
                                ),
                            )
                        })?
                        .is_dir()
                    {
                        continue;
                    }
                    if let Ok(name) = entry.file_name().into_string() {
                        namespaces.push(name);
                    }
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(results),
            Err(err) => {
                return Err(with_context(
                    err,
                    format!(
                        "Failed to read VolumeSnapshot root directory '{}'",
                        root.display()
                    ),
                ))
            }
        }
    }

    for ns in namespaces {
        let namespace_path = root.join(&ns);
        let entries = match fs::read_dir(&namespace_path) {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(with_context(
                    err,
                    format!(
                        "Failed to read VolumeSnapshot namespace directory '{}'",
                        namespace_path.display()
                    ),
                ))
            }
        };

        for entry in entries {
            let entry = entry.map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to iterate VolumeSnapshot directory '{}'",
                        namespace_path.display()
                    ),
                )
            })?;
            let entry_type = entry.file_type().map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to inspect VolumeSnapshot entry '{}'",
                        entry.path().display()
                    ),
                )
            })?;
            if !entry_type.is_dir() {
                continue;
            }

            let name = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let value_path = entry.path().join(KEYSPACE_VALUE_FILE);
            let raw = match fs::read_to_string(&value_path) {
                Ok(contents) => contents,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!(
                            "Failed to load VolumeSnapshot payload '{}'",
                            value_path.display()
                        ),
                    ))
                }
            };

            let mut snapshot: VolumeSnapshot = serde_json::from_str(&raw).map_err(|err| {
                with_context(
                    err,
                    format!(
                        "Failed to deserialize VolumeSnapshot '{}' from '{}'",
                        name,
                        value_path.display()
                    ),
                )
            })?;

            if snapshot.api_version.is_empty() {
                snapshot.api_version = "nanocloud.io/v1".to_string();
            }
            if snapshot.kind.is_empty() {
                snapshot.kind = "VolumeSnapshot".to_string();
            }
            if snapshot.metadata.name.is_none() {
                snapshot.metadata.name = Some(name.clone());
            }
            if snapshot.metadata.namespace.is_none() {
                snapshot.metadata.namespace = Some(ns.clone());
            }
            if snapshot.metadata.resource_version.is_none() {
                snapshot.metadata.resource_version = Some("1".to_string());
            }

            results.push(snapshot);
        }
    }

    Ok(results)
}

fn collect_configmaps(namespace: &str) -> Result<Vec<ConfigMap>, Box<dyn Error + Send + Sync>> {
    let mut items = Vec::new();
    let normalized = normalize_namespace(Some(namespace));
    let dir = configmap_root().join(&normalized);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(items),
        Err(err) => {
            return Err(with_context(
                err,
                format!(
                    "Failed to read ConfigMap namespace directory '{}'",
                    dir.display()
                ),
            ))
        }
    };

    for entry in entries {
        let entry = entry.map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to iterate ConfigMap namespace directory '{}'",
                    dir.display()
                ),
            )
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|err| {
            with_context(
                err,
                format!(
                    "Failed to inspect ConfigMap namespace entry '{}'",
                    path.display()
                ),
            )
        })?;
        if !file_type.is_dir() {
            continue;
        }
        let name = match entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };
        if let Some(config_map) = load_config_map(Some(normalized.as_str()), &name)? {
            items.push(config_map);
        }
    }

    Ok(items)
}

pub fn save(
    namespace: Option<&str>,
    app: &str,
    workload: &StatefulSet,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_statefulset_key(namespace, app);
    let payload = serde_json::to_string(workload).map_err(|err| {
        with_context(
            err,
            format!("Failed to serialize StatefulSet for key '{}'", key),
        )
    })?;
    K8S_KEYSPACE
        .put(&key, &payload)
        .map_err(|err| with_context(err, format!("Failed to persist StatefulSet '{}'", key)))
}

pub fn save_service(
    namespace: Option<&str>,
    name: &str,
    service: &Service,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_service_key(namespace, name);
    let payload = serde_json::to_string(service).map_err(|err| {
        with_context(
            err,
            format!("Failed to serialize Service for key '{}'", key),
        )
    })?;
    K8S_KEYSPACE
        .put(&key, &payload)
        .map_err(|err| with_context(err, format!("Failed to persist Service '{}'", key)))
}

pub fn delete_service(
    namespace: Option<&str>,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_service_key(namespace, name);
    match K8S_KEYSPACE.delete(&key) {
        Ok(_) => Ok(()),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(())
            } else {
                Err(with_context(
                    err,
                    format!("Failed to delete Service '{}' from keyspace", key),
                ))
            }
        }
    }
}

pub fn save_endpoints(
    namespace: Option<&str>,
    name: &str,
    endpoints: &Endpoints,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_endpoints_key(namespace, name);
    let payload = serde_json::to_string(endpoints).map_err(|err| {
        with_context(
            err,
            format!("Failed to serialize Endpoints for key '{}'", key),
        )
    })?;
    K8S_KEYSPACE
        .put(&key, &payload)
        .map_err(|err| with_context(err, format!("Failed to persist Endpoints '{}'", key)))
}

pub fn delete_endpoints(
    namespace: Option<&str>,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_endpoints_key(namespace, name);
    match K8S_KEYSPACE.delete(&key) {
        Ok(_) => Ok(()),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(())
            } else {
                Err(with_context(
                    err,
                    format!("Failed to delete Endpoints '{}' from keyspace", key),
                ))
            }
        }
    }
}

pub fn save_pod_manifest(
    namespace: Option<&str>,
    app: &str,
    workload: &Pod,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_pod_key(namespace, app);
    let mut sanitized = workload.clone();
    if let Some(status) = workload
        .status
        .as_ref()
        .and_then(|status| status.pod_ip.as_deref())
    {
        sanitized
            .metadata
            .annotations
            .insert(POD_IP_ANNOTATION.to_string(), status.to_string());
    } else {
        sanitized.metadata.annotations.remove(POD_IP_ANNOTATION);
    }
    sanitized.status = None;
    sanitized.metadata.resource_version = None;
    let payload = serde_json::to_string(&sanitized)
        .map_err(|err| with_context(err, format!("Failed to serialize Pod for key '{}'", key)))?;
    K8S_KEYSPACE
        .put(&key, &payload)
        .map_err(|err| with_context(err, format!("Failed to persist Pod '{}'", key)))?;
    // Best-effort removal of legacy StatefulSet entries for the same workload.
    if let Err(err) = delete(namespace, app) {
        if !is_missing_value_error(err.as_ref()) {
            return Err(err);
        }
    }
    Ok(())
}

pub fn load(
    namespace: Option<&str>,
    app: &str,
) -> Result<Option<StatefulSet>, Box<dyn Error + Send + Sync>> {
    let key = make_statefulset_key(namespace, app);
    match K8S_KEYSPACE.get(&key) {
        Ok(raw) => {
            let parsed = serde_json::from_str(&raw).map_err(|err| {
                with_context(
                    err,
                    format!("Failed to parse StatefulSet from key '{}'", key),
                )
            })?;
            Ok(Some(parsed))
        }
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(None)
            } else {
                Err(with_context(
                    err,
                    format!("Failed to load StatefulSet '{}' from keyspace", key),
                ))
            }
        }
    }
}

pub fn load_pod_manifest(
    namespace: Option<&str>,
    app: &str,
) -> Result<Option<Pod>, Box<dyn Error + Send + Sync>> {
    let key = make_pod_key(namespace, app);
    match K8S_KEYSPACE.get(&key) {
        Ok(raw) => {
            let mut pod: Pod = serde_json::from_str(&raw).map_err(|err| {
                with_context(err, format!("Failed to parse Pod from key '{}'", key))
            })?;
            pod.status = None;
            Ok(Some(pod))
        }
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                // Attempt to read a legacy StatefulSet entry and convert it to a Pod template.
                match load(namespace, app)? {
                    Some(legacy) => Ok(Some(pod_from_statefulset(&legacy))),
                    None => Ok(None),
                }
            } else {
                Err(with_context(
                    err,
                    format!("Failed to load Pod '{}' from keyspace", key),
                ))
            }
        }
    }
}

pub fn delete(namespace: Option<&str>, app: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_statefulset_key(namespace, app);
    match K8S_KEYSPACE.delete(&key) {
        Ok(()) => Ok(()),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(())
            } else {
                Err(with_context(
                    err,
                    format!("Failed to delete StatefulSet '{}' from keyspace", key),
                ))
            }
        }
    }
}

pub fn delete_deployment(
    namespace: Option<&str>,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_deployment_key(namespace, name);
    match K8S_KEYSPACE.delete(&key) {
        Ok(()) => Ok(()),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(())
            } else {
                Err(with_context(
                    err,
                    format!("Failed to delete Deployment '{}' from keyspace", key),
                ))
            }
        }
    }
}

pub fn delete_daemonset(
    namespace: Option<&str>,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_daemonset_key(namespace, name);
    match K8S_KEYSPACE.delete(&key) {
        Ok(()) => Ok(()),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(())
            } else {
                Err(with_context(
                    err,
                    format!("Failed to delete DaemonSet '{}' from keyspace", key),
                ))
            }
        }
    }
}

pub fn delete_pod_manifest(
    namespace: Option<&str>,
    app: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_pod_key(namespace, app);
    match K8S_KEYSPACE.delete(&key) {
        Ok(()) => {}
        Err(err) => {
            if !is_missing_value_error(err.as_ref()) {
                return Err(with_context(
                    err,
                    format!("Failed to delete Pod '{}' from keyspace", key),
                ));
            }
        }
    }
    // Also remove any legacy StatefulSet manifests.
    if let Err(err) = delete(namespace, app) {
        if !is_missing_value_error(err.as_ref()) {
            return Err(err);
        }
    }
    Ok(())
}

pub fn delete_job(namespace: Option<&str>, name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_job_key(namespace, name);
    match K8S_KEYSPACE.delete(&key) {
        Ok(()) => Ok(()),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(())
            } else {
                Err(with_context(
                    err,
                    format!("Failed to delete Job '{}' from keyspace", key),
                ))
            }
        }
    }
}

fn pod_from_statefulset(workload: &StatefulSet) -> Pod {
    let mut metadata = workload.spec.template.metadata.clone();
    metadata.name = workload.metadata.name.clone();
    metadata.namespace = workload.metadata.namespace.clone();
    Pod {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        metadata,
        spec: workload.spec.template.spec.clone(),
        status: None,
    }
}

pub fn save_config_map(
    namespace: Option<&str>,
    name: &str,
    config_map: &ConfigMap,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_configmap_key(namespace, name);
    let payload = serde_json::to_string(config_map).map_err(|err| {
        with_context(
            err,
            format!("Failed to serialize ConfigMap for key '{}'", key),
        )
    })?;
    K8S_KEYSPACE
        .put(&key, &payload)
        .map_err(|err| with_context(err, format!("Failed to persist ConfigMap '{}'", key)))
}

pub fn load_config_map(
    namespace: Option<&str>,
    name: &str,
) -> Result<Option<ConfigMap>, Box<dyn Error + Send + Sync>> {
    let key = make_configmap_key(namespace, name);
    match K8S_KEYSPACE.get(&key) {
        Ok(raw) => {
            let parsed = serde_json::from_str(&raw).map_err(|err| {
                with_context(err, format!("Failed to parse ConfigMap from key '{}'", key))
            })?;
            Ok(Some(parsed))
        }
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(None)
            } else {
                Err(with_context(
                    err,
                    format!("Failed to load ConfigMap '{}' from keyspace", key),
                ))
            }
        }
    }
}

pub fn delete_config_map(
    namespace: Option<&str>,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_configmap_key(namespace, name);
    match K8S_KEYSPACE.delete(&key) {
        Ok(()) => Ok(()),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(())
            } else {
                Err(with_context(
                    err,
                    format!("Failed to delete ConfigMap '{}' from keyspace", key),
                ))
            }
        }
    }
}

pub fn save_device(
    namespace: Option<&str>,
    name: &str,
    device: &Device,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_device_key(namespace, name);
    let payload = serde_json::to_string(device).map_err(|err| {
        with_context(err, format!("Failed to serialize Device for key '{}'", key))
    })?;
    K8S_KEYSPACE
        .put(&key, &payload)
        .map_err(|err| with_context(err, format!("Failed to persist Device '{}'", key)))
}

pub fn save_bundle(
    namespace: Option<&str>,
    name: &str,
    bundle: &Bundle,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_bundle_key(namespace, name);
    let payload = serde_json::to_string(bundle).map_err(|err| {
        with_context(err, format!("Failed to serialize Bundle for key '{}'", key))
    })?;
    K8S_KEYSPACE
        .put(&key, &payload)
        .map_err(|err| with_context(err, format!("Failed to persist Bundle '{}'", key)))
}

pub fn save_volume_snapshot(
    namespace: Option<&str>,
    name: &str,
    snapshot: &VolumeSnapshot,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_snapshot_key(namespace, name);
    let payload = serde_json::to_string(snapshot).map_err(|err| {
        with_context(
            err,
            format!("Failed to serialize VolumeSnapshot for key '{}'", key),
        )
    })?;
    K8S_KEYSPACE
        .put(&key, &payload)
        .map_err(|err| with_context(err, format!("Failed to persist VolumeSnapshot '{}'", key)))
}

pub fn delete_device(
    namespace: Option<&str>,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_device_key(namespace, name);
    match K8S_KEYSPACE.delete(&key) {
        Ok(()) => Ok(()),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(())
            } else {
                Err(with_context(
                    err,
                    format!("Failed to delete Device '{}' from keyspace", key),
                ))
            }
        }
    }
}

pub fn delete_bundle(
    namespace: Option<&str>,
    name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_bundle_key(namespace, name);
    match K8S_KEYSPACE.delete(&key) {
        Ok(()) => Ok(()),
        Err(err) => {
            if is_missing_value_error(err.as_ref()) {
                Ok(())
            } else {
                Err(with_context(
                    err,
                    format!("Failed to delete Bundle '{}' from keyspace", key),
                ))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListCursor {
    pub key: String,
    #[serde(rename = "resourceVersion", skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
}

#[derive(Debug)]
pub enum PaginationError {
    InvalidContinue(String),
    InvalidLimit(String),
}

impl std::fmt::Display for PaginationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PaginationError::InvalidContinue(msg) => write!(f, "{msg}"),
            PaginationError::InvalidLimit(msg) => write!(f, "{msg}"),
        }
    }
}

impl Error for PaginationError {}

#[derive(Debug)]
pub struct PaginatedResult<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<ListCursor>,
    pub remaining: usize,
}

pub fn encode_continue_token(kind: &str, cursor: &ListCursor) -> String {
    let payload = serde_json::json!({
        "kind": kind,
        "key": cursor.key,
        "resourceVersion": cursor.resource_version,
    });
    BASE64_ENGINE.encode(payload.to_string())
}

pub fn decode_continue_token(
    token: &str,
    expected_kind: &str,
) -> Result<ListCursor, PaginationError> {
    let decoded = BASE64_ENGINE.decode(token).map_err(|_| {
        PaginationError::InvalidContinue("continue token is not valid base64".to_string())
    })?;
    let value: serde_json::Value = serde_json::from_slice(&decoded).map_err(|_| {
        PaginationError::InvalidContinue("continue token payload is not valid JSON".to_string())
    })?;

    let kind = value
        .get("kind")
        .and_then(|kind| kind.as_str())
        .ok_or_else(|| {
            PaginationError::InvalidContinue("continue token missing kind".to_string())
        })?;
    if kind != expected_kind {
        return Err(PaginationError::InvalidContinue(format!(
            "continue token was issued for '{}', not '{}'",
            kind, expected_kind
        )));
    }

    let key = value
        .get("key")
        .and_then(|key| key.as_str())
        .ok_or_else(|| {
            PaginationError::InvalidContinue("continue token missing key".to_string())
        })?;
    let resource_version = value
        .get("resourceVersion")
        .and_then(|rv| rv.as_str())
        .map(|rv| rv.to_string());

    Ok(ListCursor {
        key: key.to_string(),
        resource_version,
    })
}

pub fn paginate_entries<T>(
    mut entries: Vec<(String, T, Option<String>)>,
    cursor: Option<&ListCursor>,
    limit: Option<u32>,
) -> Result<PaginatedResult<T>, PaginationError> {
    if let Some(limit) = limit {
        if limit == 0 {
            return Err(PaginationError::InvalidLimit(
                "limit must be greater than 0".to_string(),
            ));
        }
    }

    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let start_index = if let Some(cursor) = cursor {
        match entries.iter().position(|(key, _, _)| key == &cursor.key) {
            Some(index) => index.saturating_add(1),
            None => {
                return Err(PaginationError::InvalidContinue(
                    "continue token no longer matches available items".to_string(),
                ))
            }
        }
    } else {
        0
    };

    let tail: Vec<(String, T, Option<String>)> = entries.into_iter().skip(start_index).collect();
    let total_after_start = tail.len();

    let take_count = match limit {
        Some(limit) => std::cmp::min(limit as usize, total_after_start),
        None => total_after_start,
    };

    let mut items = Vec::with_capacity(take_count);
    let mut last_key: Option<String> = None;
    let mut last_rv: Option<String> = None;

    for (index, (key, value, rv)) in tail.into_iter().enumerate() {
        if index < take_count {
            last_key = Some(key);
            last_rv = rv;
            items.push(value);
        }
    }

    let remaining = total_after_start.saturating_sub(items.len());
    let next_cursor = if remaining > 0 {
        last_key.map(|key| ListCursor {
            key,
            resource_version: last_rv,
        })
    } else {
        None
    };

    Ok(PaginatedResult {
        items,
        next_cursor,
        remaining,
    })
}

pub fn list_config_maps(
    namespace: Option<&str>,
) -> Result<Vec<ConfigMap>, Box<dyn Error + Send + Sync>> {
    match namespace {
        Some(ns) => collect_configmaps(ns),
        None => {
            let mut items = Vec::new();
            let root = configmap_root();
            let entries = match fs::read_dir(&root) {
                Ok(entries) => entries,
                Err(err) if err.kind() == ErrorKind::NotFound => return Ok(items),
                Err(err) => {
                    return Err(with_context(
                        err,
                        format!(
                            "Failed to read ConfigMap root directory '{}'",
                            root.display()
                        ),
                    ))
                }
            };

            for entry in entries {
                let entry = entry.map_err(|err| {
                    with_context(
                        err,
                        format!(
                            "Failed to iterate ConfigMap root directory '{}'",
                            root.display()
                        ),
                    )
                })?;
                let path = entry.path();
                let file_type = entry.file_type().map_err(|err| {
                    with_context(
                        err,
                        format!(
                            "Failed to inspect ConfigMap namespace directory '{}'",
                            path.display()
                        ),
                    )
                })?;
                if !file_type.is_dir() {
                    continue;
                }
                let namespace = match entry.file_name().into_string() {
                    Ok(ns) => ns,
                    Err(_) => continue,
                };
                items.extend(collect_configmaps(&namespace)?);
            }

            Ok(items)
        }
    }
}
