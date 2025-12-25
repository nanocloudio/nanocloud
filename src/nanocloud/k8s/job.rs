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

//! Kubernetes Job resource types and builders.
//!
//! This module provides:
//!
//! - `Job`, `JobSpec`, `JobStatus` - Core Kubernetes Job types
//! - `JobBuilder` - Fluent builder for creating Jobs
//! - Helper methods for webhook-triggered job creation
//!
//! # Example
//!
//! ```ignore
//! use nanocloud::k8s::job::JobBuilder;
//!
//! let job = JobBuilder::new("my-job")
//!     .namespace("default")
//!     .container("runner", "myimage:latest")
//!     .env("API_KEY", "secret-value")
//!     .args(&["--verbose", "--config=/etc/config"])
//!     .backoff_limit(3)
//!     .ttl_after_finished(300)
//!     .build();
//! ```

use super::pod::{ListMeta, ObjectMeta};
use super::statefulset::{LabelSelector, PodTemplateSpec};
use serde::{Deserialize, Serialize};

/// Condition describing Job lifecycle state.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct JobCondition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub status: String,
    #[serde(rename = "lastProbeTime", skip_serializing_if = "Option::is_none")]
    pub last_probe_time: Option<String>,
    #[serde(rename = "lastTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Pod execution template and completion policy for a Job.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct JobSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completions: Option<i32>,
    #[serde(rename = "backoffLimit", skip_serializing_if = "Option::is_none")]
    pub backoff_limit: Option<i32>,
    #[serde(
        rename = "activeDeadlineSeconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub active_deadline_seconds: Option<i64>,
    #[serde(
        rename = "ttlSecondsAfterFinished",
        skip_serializing_if = "Option::is_none"
    )]
    pub ttl_seconds_after_finished: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<LabelSelector>,
    #[serde(rename = "manualSelector", skip_serializing_if = "Option::is_none")]
    pub manual_selector: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suspend: Option<bool>,
    pub template: PodTemplateSpec,
}

/// Observed runtime status for a Job.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct JobStatus {
    #[serde(rename = "startTime", skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(rename = "completionTime", skip_serializing_if = "Option::is_none")]
    pub completion_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub succeeded: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed: Option<i32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<JobCondition>,
}

/// Batch Job resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Job {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: JobSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<JobStatus>,
}

impl Job {
    pub fn new(metadata: ObjectMeta, spec: JobSpec) -> Self {
        Self {
            api_version: "batch/v1".to_string(),
            kind: "Job".to_string(),
            metadata,
            spec,
            status: None,
        }
    }
}

/// Aggregated list of Jobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct JobList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ListMeta,
    pub items: Vec<Job>,
}

impl JobList {
    pub fn from_items(items: Vec<Job>) -> Self {
        Self {
            api_version: "batch/v1".to_string(),
            kind: "JobList".to_string(),
            metadata: ListMeta::default(),
            items,
        }
    }
}

// ============================================================================
// Job Builder (edge feature only)
// ============================================================================

#[cfg(feature = "edge")]
mod job_builder {
    use super::super::pod::{ContainerEnvVar, ContainerSpec, PodSpec};
    use super::*;
    use chrono::{SecondsFormat, Utc};
    use rand::Rng;
    use std::collections::HashMap;

    /// Builder for creating Job resources.
    ///
    /// Provides a fluent API for constructing Jobs with templated values,
    /// particularly useful for webhook-triggered job creation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let job = JobBuilder::new("deploy-job")
    ///     .namespace("production")
    ///     .generate_name("deploy-")
    ///     .container("deployer", "deploy:v1.0")
    ///     .env("COMMIT_SHA", "abc123")
    ///     .env("BRANCH", "main")
    ///     .args(&["--environment", "production"])
    ///     .backoff_limit(3)
    ///     .ttl_after_finished(3600)
    ///     .label("app", "deployer")
    ///     .label("triggered-by", "webhook")
    ///     .build();
    /// ```
    #[derive(Debug, Clone)]
    pub struct JobBuilder {
        name: Option<String>,
        generate_name: Option<String>,
        namespace: String,
        labels: HashMap<String, String>,
        annotations: HashMap<String, String>,
        containers: Vec<ContainerSpec>,
        env_vars: HashMap<String, String>,
        args: Vec<String>,
        backoff_limit: Option<i32>,
        active_deadline_seconds: Option<i64>,
        ttl_seconds_after_finished: Option<i32>,
        parallelism: Option<i32>,
        completions: Option<i32>,
        restart_policy: String,
    }

    impl Default for JobBuilder {
        fn default() -> Self {
            Self {
                name: None,
                generate_name: None,
                namespace: "default".to_string(),
                labels: HashMap::new(),
                annotations: HashMap::new(),
                containers: Vec::new(),
                env_vars: HashMap::new(),
                args: Vec::new(),
                backoff_limit: None,
                active_deadline_seconds: None,
                ttl_seconds_after_finished: None,
                parallelism: None,
                completions: None,
                restart_policy: "Never".to_string(),
            }
        }
    }

    impl JobBuilder {
        /// Create a new JobBuilder with the given name.
        pub fn new(name: impl Into<String>) -> Self {
            Self {
                name: Some(name.into()),
                ..Default::default()
            }
        }

        /// Create a new JobBuilder with a generated name prefix.
        ///
        /// The final name will be `prefix` + random suffix.
        pub fn with_generate_name(prefix: impl Into<String>) -> Self {
            Self {
                generate_name: Some(prefix.into()),
                ..Default::default()
            }
        }

        /// Set the namespace.
        #[must_use]
        pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
            self.namespace = namespace.into();
            self
        }

        /// Set the generate name prefix.
        ///
        /// If set, the final job name will be `prefix` + random suffix.
        /// This takes precedence over a name set with `new()`.
        #[must_use]
        pub fn generate_name(mut self, prefix: impl Into<String>) -> Self {
            self.generate_name = Some(prefix.into());
            self
        }

        /// Add a label.
        #[must_use]
        pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
            self.labels.insert(key.into(), value.into());
            self
        }

        /// Add multiple labels.
        #[must_use]
        pub fn labels(mut self, labels: HashMap<String, String>) -> Self {
            self.labels.extend(labels);
            self
        }

        /// Add an annotation.
        #[must_use]
        pub fn annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
            self.annotations.insert(key.into(), value.into());
            self
        }

        /// Add a container to the job.
        #[must_use]
        pub fn container(mut self, name: impl Into<String>, image: impl Into<String>) -> Self {
            self.containers.push(ContainerSpec {
                name: name.into(),
                image: Some(image.into()),
                ..Default::default()
            });
            self
        }

        /// Add a pre-configured container.
        #[must_use]
        pub fn add_container(mut self, container: ContainerSpec) -> Self {
            self.containers.push(container);
            self
        }

        /// Add an environment variable to all containers.
        #[must_use]
        pub fn env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
            self.env_vars.insert(name.into(), value.into());
            self
        }

        /// Add multiple environment variables to all containers.
        #[must_use]
        pub fn envs(mut self, vars: HashMap<String, String>) -> Self {
            self.env_vars.extend(vars);
            self
        }

        /// Add arguments to all containers.
        #[must_use]
        pub fn args(mut self, args: &[&str]) -> Self {
            self.args.extend(args.iter().map(|s| s.to_string()));
            self
        }

        /// Add a single argument to all containers.
        #[must_use]
        pub fn arg(mut self, arg: impl Into<String>) -> Self {
            self.args.push(arg.into());
            self
        }

        /// Set the backoff limit (number of retries).
        #[must_use]
        pub fn backoff_limit(mut self, limit: i32) -> Self {
            self.backoff_limit = Some(limit);
            self
        }

        /// Set the active deadline in seconds.
        #[must_use]
        pub fn active_deadline_seconds(mut self, seconds: i64) -> Self {
            self.active_deadline_seconds = Some(seconds);
            self
        }

        /// Set the TTL after the job finishes (for automatic cleanup).
        #[must_use]
        pub fn ttl_after_finished(mut self, seconds: i32) -> Self {
            self.ttl_seconds_after_finished = Some(seconds);
            self
        }

        /// Set the parallelism (number of pods to run in parallel).
        #[must_use]
        pub fn parallelism(mut self, count: i32) -> Self {
            self.parallelism = Some(count);
            self
        }

        /// Set the number of completions required.
        #[must_use]
        pub fn completions(mut self, count: i32) -> Self {
            self.completions = Some(count);
            self
        }

        /// Set the restart policy (default: "Never").
        #[must_use]
        pub fn restart_policy(mut self, policy: impl Into<String>) -> Self {
            self.restart_policy = policy.into();
            self
        }

        /// Build the Job.
        pub fn build(self) -> Job {
            // Generate the job name
            let name = if let Some(prefix) = self.generate_name {
                format!("{}{}", prefix, generate_random_suffix())
            } else {
                self.name
                    .unwrap_or_else(|| format!("job-{}", generate_random_suffix()))
            };

            // Build containers with env vars and args
            let containers: Vec<ContainerSpec> = if self.containers.is_empty() {
                // Create a default container if none specified
                vec![ContainerSpec {
                    name: "main".to_string(),
                    env: self
                        .env_vars
                        .iter()
                        .map(|(k, v)| ContainerEnvVar {
                            name: k.clone(),
                            value: Some(v.clone()),
                            value_from: None,
                        })
                        .collect(),
                    args: self.args.clone(),
                    ..Default::default()
                }]
            } else {
                self.containers
                    .into_iter()
                    .map(|mut c| {
                        // Add env vars
                        for (k, v) in &self.env_vars {
                            c.env.push(ContainerEnvVar {
                                name: k.clone(),
                                value: Some(v.clone()),
                                value_from: None,
                            });
                        }
                        // Add args
                        c.args.extend(self.args.clone());
                        c
                    })
                    .collect()
            };

            // Build metadata
            let mut metadata = ObjectMeta {
                name: Some(name),
                namespace: Some(self.namespace),
                labels: self.labels,
                annotations: self.annotations,
                creation_timestamp: Some(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)),
                ..Default::default()
            };

            // Ensure UID is set
            metadata.ensure_common_fields(None, None);

            // Build spec
            let spec = JobSpec {
                backoff_limit: self.backoff_limit,
                active_deadline_seconds: self.active_deadline_seconds,
                ttl_seconds_after_finished: self.ttl_seconds_after_finished,
                parallelism: self.parallelism,
                completions: self.completions,
                template: PodTemplateSpec {
                    metadata: ObjectMeta {
                        labels: metadata.labels.clone(),
                        ..Default::default()
                    },
                    spec: PodSpec {
                        containers,
                        restart_policy: Some(self.restart_policy),
                        ..Default::default()
                    },
                },
                ..Default::default()
            };

            Job::new(metadata, spec)
        }
    }

    /// Generate a random 5-character alphanumeric suffix.
    fn generate_random_suffix() -> String {
        let mut rng = rand::thread_rng();
        (0..5)
            .map(|_| {
                let idx = rng.gen_range(0..36);
                if idx < 10 {
                    (b'0' + idx) as char
                } else {
                    (b'a' + idx - 10) as char
                }
            })
            .collect()
    }

    // ============================================================================
    // Template Helpers
    // ============================================================================

    /// Template context for populating job templates.
    ///
    /// Used to substitute values in job templates from webhook payloads.
    #[derive(Debug, Clone, Default)]
    pub struct JobTemplateContext {
        /// Environment variable values to set.
        pub env_vars: HashMap<String, String>,
        /// Additional arguments to append.
        pub args: Vec<String>,
        /// Labels to add to the job.
        pub labels: HashMap<String, String>,
        /// Annotations to add to the job.
        pub annotations: HashMap<String, String>,
    }

    impl JobTemplateContext {
        /// Create a new empty context.
        pub fn new() -> Self {
            Self::default()
        }

        /// Add an environment variable.
        #[must_use]
        pub fn env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
            self.env_vars.insert(name.into(), value.into());
            self
        }

        /// Add an argument.
        #[must_use]
        pub fn arg(mut self, arg: impl Into<String>) -> Self {
            self.args.push(arg.into());
            self
        }

        /// Add a label.
        #[must_use]
        pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
            self.labels.insert(key.into(), value.into());
            self
        }

        /// Add an annotation.
        #[must_use]
        pub fn annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
            self.annotations.insert(key.into(), value.into());
            self
        }
    }

    /// Apply a template context to a JobSpec.
    ///
    /// Modifies the JobSpec in-place, adding environment variables, arguments,
    /// labels, and annotations from the context.
    pub fn apply_template_context(spec: &mut JobSpec, context: &JobTemplateContext) {
        // Add env vars to all containers
        for container in &mut spec.template.spec.containers {
            for (name, value) in &context.env_vars {
                container.env.push(ContainerEnvVar {
                    name: name.clone(),
                    value: Some(value.clone()),
                    value_from: None,
                });
            }

            // Add args
            container.args.extend(context.args.clone());
        }

        // Add labels to pod template
        for (key, value) in &context.labels {
            spec.template
                .metadata
                .labels
                .insert(key.clone(), value.clone());
        }

        // Add annotations to pod template
        for (key, value) in &context.annotations {
            spec.template
                .metadata
                .annotations
                .insert(key.clone(), value.clone());
        }
    }

    /// Create a Job from a template spec with the given context.
    ///
    /// This is the main entry point for creating Jobs from webhook templates.
    pub fn create_job_from_template(
        template_metadata: &ObjectMeta,
        template_spec: &JobSpec,
        namespace: &str,
        context: &JobTemplateContext,
    ) -> Job {
        // Clone and modify the spec
        let mut spec = template_spec.clone();
        apply_template_context(&mut spec, context);

        // Build the metadata
        let name = if let Some(ref prefix) = template_metadata.generate_name {
            format!("{}{}", prefix, generate_random_suffix())
        } else if let Some(ref name) = template_metadata.name {
            name.clone()
        } else {
            format!("job-{}", generate_random_suffix())
        };

        let mut metadata = ObjectMeta {
            name: Some(name),
            namespace: Some(namespace.to_string()),
            labels: template_metadata.labels.clone(),
            annotations: template_metadata.annotations.clone(),
            creation_timestamp: Some(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)),
            ..Default::default()
        };

        // Add context labels and annotations
        for (key, value) in &context.labels {
            metadata.labels.insert(key.clone(), value.clone());
        }
        for (key, value) in &context.annotations {
            metadata.annotations.insert(key.clone(), value.clone());
        }

        // Ensure UID
        metadata.ensure_common_fields(None, None);

        Job::new(metadata, spec)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn job_builder_basic() {
            let job = JobBuilder::new("test-job")
                .namespace("test-ns")
                .container("runner", "myimage:latest")
                .env("FOO", "bar")
                .args(&["--verbose"])
                .backoff_limit(3)
                .build();

            assert_eq!(job.metadata.name, Some("test-job".to_string()));
            assert_eq!(job.metadata.namespace, Some("test-ns".to_string()));
            assert_eq!(job.spec.backoff_limit, Some(3));
            assert_eq!(job.spec.template.spec.containers.len(), 1);
            assert_eq!(job.spec.template.spec.containers[0].name, "runner");
        }

        #[test]
        fn job_builder_generate_name() {
            let job = JobBuilder::with_generate_name("deploy-")
                .namespace("production")
                .container("deployer", "deploy:v1")
                .build();

            let name = job.metadata.name.unwrap();
            assert!(name.starts_with("deploy-"));
            assert_eq!(name.len(), 12); // "deploy-" (7) + suffix (5)
        }

        #[test]
        fn job_builder_multiple_envs() {
            let job = JobBuilder::new("test")
                .container("main", "image:latest")
                .env("VAR1", "value1")
                .env("VAR2", "value2")
                .build();

            let container = &job.spec.template.spec.containers[0];
            assert_eq!(container.env.len(), 2);
        }

        #[test]
        fn job_builder_labels_and_annotations() {
            let job = JobBuilder::new("test")
                .container("main", "image:latest")
                .label("app", "myapp")
                .label("version", "v1")
                .annotation("description", "Test job")
                .build();

            assert_eq!(job.metadata.labels.get("app"), Some(&"myapp".to_string()));
            assert_eq!(job.metadata.labels.get("version"), Some(&"v1".to_string()));
            assert_eq!(
                job.metadata.annotations.get("description"),
                Some(&"Test job".to_string())
            );
        }

        #[test]
        fn template_context_application() {
            let mut spec = JobSpec {
                template: PodTemplateSpec {
                    metadata: ObjectMeta::default(),
                    spec: PodSpec {
                        containers: vec![ContainerSpec {
                            name: "main".to_string(),
                            image: Some("image:latest".to_string()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                },
                ..Default::default()
            };

            let context = JobTemplateContext::new()
                .env("WEBHOOK_ID", "12345")
                .arg("--payload=/tmp/payload.json")
                .label("trigger", "webhook");

            apply_template_context(&mut spec, &context);

            let container = &spec.template.spec.containers[0];
            assert!(container.env.iter().any(|e| e.name == "WEBHOOK_ID"));
            assert!(container
                .args
                .contains(&"--payload=/tmp/payload.json".to_string()));
            assert_eq!(
                spec.template.metadata.labels.get("trigger"),
                Some(&"webhook".to_string())
            );
        }

        #[test]
        fn create_job_from_template_works() {
            let template_metadata = ObjectMeta {
                generate_name: Some("webhook-job-".to_string()),
                labels: [("app".to_string(), "webhook-handler".to_string())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            };

            let template_spec = JobSpec {
                backoff_limit: Some(2),
                template: PodTemplateSpec {
                    metadata: ObjectMeta::default(),
                    spec: PodSpec {
                        containers: vec![ContainerSpec {
                            name: "handler".to_string(),
                            image: Some("handler:latest".to_string()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                },
                ..Default::default()
            };

            let context = JobTemplateContext::new()
                .env("PAYLOAD", "{\"test\": true}")
                .label("source", "github");

            let job =
                create_job_from_template(&template_metadata, &template_spec, "default", &context);

            let name = job.metadata.name.as_ref().unwrap();
            assert!(name.starts_with("webhook-job-"));
            assert_eq!(job.metadata.namespace, Some("default".to_string()));
            assert_eq!(
                job.metadata.labels.get("app"),
                Some(&"webhook-handler".to_string())
            );
            assert_eq!(
                job.metadata.labels.get("source"),
                Some(&"github".to_string())
            );

            let container = &job.spec.template.spec.containers[0];
            assert!(container.env.iter().any(|e| e.name == "PAYLOAD"));
        }

        #[test]
        fn random_suffix_format() {
            let suffix = generate_random_suffix();
            assert_eq!(suffix.len(), 5);
            assert!(suffix.chars().all(|c| c.is_ascii_alphanumeric()));
        }
    }
} // end mod job_builder

#[cfg(feature = "edge")]
pub use job_builder::{
    apply_template_context, create_job_from_template, JobBuilder, JobTemplateContext,
};
