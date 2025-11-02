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

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// Minimal ConfigMap resource compatible with kubectl expectations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ConfigMap {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub data: HashMap<String, String>,
    #[serde(
        rename = "binaryData",
        default,
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub binary_data: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub immutable: Option<bool>,
}

impl ConfigMap {
    pub fn new(metadata: ObjectMeta) -> Self {
        Self {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            metadata,
            data: HashMap::new(),
            binary_data: HashMap::new(),
            immutable: None,
        }
    }

    pub fn get_key_bytes(&self, key: &str) -> Option<Vec<u8>> {
        if let Some(value) = self.data.get(key) {
            Some(value.clone().into_bytes())
        } else {
            self.binary_data
                .get(key)
                .and_then(|value| BASE64_STANDARD.decode(value).ok())
        }
    }

    pub fn entries(&self) -> BTreeMap<String, Vec<u8>> {
        let mut merged: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        for (key, value) in &self.data {
            merged.insert(key.clone(), value.clone().into_bytes());
        }
        for (key, value) in &self.binary_data {
            if let Ok(decoded) = BASE64_STANDARD.decode(value) {
                merged.insert(key.clone(), decoded);
            }
        }
        merged
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ConfigMapList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ListMeta,
    pub items: Vec<ConfigMap>,
}

impl ConfigMapList {
    pub fn new(items: Vec<ConfigMap>, resource_version: String) -> Self {
        Self {
            api_version: "v1".to_string(),
            kind: "ConfigMapList".to_string(),
            metadata: ListMeta {
                resource_version: Some(resource_version),
                continue_token: None,
                remaining_item_count: None,
            },
            items,
        }
    }
}
