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

use crate::nanocloud::k8s::job::Job;
use crate::nanocloud::k8s::store::common::{
    namespaced_key, namespaced_root, normalize_namespace, validate_resource_target,
    value_file_path, with_resource_lock, JOB_PREFIX, K8S_KEYSPACE,
};
use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::is_missing_value_error;

use std::error::Error;
use std::fs;
use std::io::ErrorKind;

#[derive(Debug)]
pub struct StoredJob {
    pub namespace: Option<String>,
    pub name: String,
    pub job: Job,
}

pub fn list_jobs() -> Result<Vec<StoredJob>, Box<dyn Error + Send + Sync>> {
    let mut results = Vec::new();
    let root = namespaced_root(JOB_PREFIX);
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

            let value_path = value_file_path(JOB_PREFIX, &namespace_name, &job_name);
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

pub fn get_job(
    namespace: Option<&str>,
    name: &str,
) -> Result<Option<Job>, Box<dyn Error + Send + Sync>> {
    let key = make_job_key(namespace, name);
    let raw = match K8S_KEYSPACE
        .get_optional(&key)
        .map_err(|err| with_context(err, format!("Failed to load Job '{}' from keyspace", key)))?
    {
        Some(raw) => raw,
        None => return Ok(None),
    };
    let mut job: Job = serde_json::from_str(&raw)
        .map_err(|err| with_context(err, format!("Failed to parse Job from key '{}'", key)))?;
    if job.metadata.name.is_none() {
        job.metadata.name = Some(name.to_string());
    }
    job.metadata.namespace = Some(normalize_namespace(namespace));
    if job.metadata.resource_version.is_none() {
        job.metadata.resource_version = Some("1".to_string());
    }
    Ok(Some(job))
}

pub fn delete_job(namespace: Option<&str>, name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = make_job_key(namespace, name);
    validate_resource_target("Job", name, namespace, None, None)?;
    with_resource_lock(&key, || match K8S_KEYSPACE.delete(&key) {
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
    })
}

fn make_job_key(namespace: Option<&str>, name: &str) -> String {
    namespaced_key(JOB_PREFIX, namespace, name)
}
