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

use super::pod::{ListMeta, ObjectMeta};
use super::statefulset::{LabelSelector, PodTemplateSpec};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeploymentSpec {
    pub replicas: i32,
    #[serde(default)]
    pub selector: LabelSelector,
    pub template: PodTemplateSpec,
    #[serde(default)]
    pub strategy: DeploymentStrategy,
    #[serde(
        rename = "revisionHistoryLimit",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub revision_history_limit: Option<i32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "camelCase")]
pub struct DeploymentStrategy {
    #[serde(rename = "type", default)]
    pub r#type: DeploymentStrategyType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rolling_update: Option<RollingUpdateDeployment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum DeploymentStrategyType {
    #[serde(rename = "RollingUpdate")]
    #[default]
    RollingUpdate,
    #[serde(rename = "Recreate")]
    Recreate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "camelCase")]
pub struct RollingUpdateDeployment {
    #[serde(rename = "maxUnavailable", skip_serializing_if = "Option::is_none")]
    pub max_unavailable: Option<RollingUpdateValue>,
    #[serde(rename = "maxSurge", skip_serializing_if = "Option::is_none")]
    pub max_surge: Option<RollingUpdateValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(untagged)]
pub enum RollingUpdateValue {
    Int(i32),
    String(String),
}

impl RollingUpdateValue {
    pub fn resolve(&self, replicas: u32, default: u32, field: &'static str) -> Result<u32, String> {
        match self {
            RollingUpdateValue::Int(value) => {
                if *value < 0 {
                    return Err(format!("{field} must be non-negative, received {value}"));
                }
                Ok(*value as u32)
            }
            RollingUpdateValue::String(value) => {
                if let Some(percent) = value.strip_suffix('%') {
                    let percent_value = percent
                        .trim()
                        .parse::<u32>()
                        .map_err(|err| format!("{field} percentage '{value}' is invalid: {err}"))?;
                    if percent_value > 100 {
                        return Err(format!(
                            "{field} percentage '{value}' must be between 0%% and 100%%"
                        ));
                    }
                    let computed =
                        ((replicas as f64) * (percent_value as f64 / 100.0)).ceil() as u32;
                    Ok(computed.max(1))
                } else if value.trim().is_empty() {
                    Ok(default)
                } else {
                    let parsed = value
                        .trim()
                        .parse::<i32>()
                        .map_err(|err| format!("{field} value '{value}' is invalid: {err}"))?;
                    if parsed < 0 {
                        return Err(format!("{field} must be non-negative, received {value}"));
                    }
                    Ok(parsed as u32)
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeploymentStatus {
    #[serde(rename = "observedGeneration", skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub conditions: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Deployment {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: DeploymentSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<DeploymentStatus>,
}

impl Deployment {
    #[cfg(test)]
    pub fn new(metadata: ObjectMeta, spec: DeploymentSpec) -> Self {
        Deployment {
            api_version: "apps/v1".to_string(),
            kind: "Deployment".to_string(),
            metadata,
            spec,
            status: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployment_new_sets_defaults() {
        let metadata = ObjectMeta {
            name: Some("web".to_string()),
            ..Default::default()
        };
        let spec = DeploymentSpec {
            replicas: 1,
            selector: LabelSelector {
                match_labels: HashMap::new(),
            },
            template: PodTemplateSpec::default(),
            strategy: DeploymentStrategy::default(),
            revision_history_limit: None,
        };

        let deployment = Deployment::new(metadata.clone(), spec.clone());

        assert_eq!(deployment.kind, "Deployment");
        assert_eq!(deployment.api_version, "apps/v1");
        assert_eq!(deployment.metadata.name, metadata.name);
        assert_eq!(
            deployment.spec.template.metadata.name,
            spec.template.metadata.name
        );
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeploymentList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ListMeta,
    pub items: Vec<Deployment>,
}

impl DeploymentList {
    pub fn from_items(items: Vec<Deployment>) -> Self {
        DeploymentList {
            api_version: "apps/v1".to_string(),
            kind: "DeploymentList".to_string(),
            metadata: ListMeta::default(),
            items,
        }
    }
}
