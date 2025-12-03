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

use super::statefulsets::get_stateful_set;
use crate::nanocloud::controller::replicaset::{
    short_revision_hash, ReplicaSetDesiredState, ReplicaSetPodAction, LABEL_POD_TEMPLATE_HASH,
    LABEL_REPLICASET_NAME, LABEL_STATEFULSET_NAME,
};
use crate::nanocloud::k8s::pod::ObjectMeta;
use crate::nanocloud::k8s::replicaset::{ReplicaSet, ReplicaSetSpec, ReplicaSetStatus};
use crate::nanocloud::k8s::statefulset::{LabelSelector, PodTemplateSpec};
use crate::nanocloud::k8s::store::common::{
    controller_component_root, normalize_namespace, KEYSPACE_VALUE_FILE, REPLICASET_DIR,
};
use crate::nanocloud::util::error::with_context;

use std::error::Error;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;

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

pub fn replicaset_from_desired_state(
    namespace: &str,
    name: &str,
    desired: ReplicaSetDesiredState,
) -> Result<ReplicaSet, Box<dyn Error + Send + Sync>> {
    build_replicaset(namespace.to_string(), name.to_string(), desired)
}

fn load_replicaset(
    namespace: &str,
    name: &str,
    value_path: &Path,
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

fn replicaset_root() -> std::path::PathBuf {
    controller_component_root(REPLICASET_DIR)
}
