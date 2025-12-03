use super::pod::{ListMeta, ObjectMeta};

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
/// Secret payload where `data` entries are base64-encoded.
///
/// `string_data` is transient and converted to `data` via [`encode_string_data`]
/// before persistence.
pub struct Secret {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub data: HashMap<String, String>,
    #[serde(rename = "stringData", default, skip_serializing)]
    pub string_data: HashMap<String, String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub secret_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub immutable: Option<bool>,
}

impl Secret {
    pub fn encode_string_data(&mut self) {
        if self.string_data.is_empty() {
            return;
        }
        for (key, value) in self.string_data.drain() {
            self.data
                .insert(key, BASE64_STANDARD.encode(value.as_bytes()));
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SecretList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ListMeta,
    pub items: Vec<Secret>,
}

impl SecretList {
    pub fn new(items: Vec<Secret>, resource_version: String) -> Self {
        SecretList {
            api_version: "v1".to_string(),
            kind: "SecretList".to_string(),
            metadata: ListMeta {
                resource_version: Some(resource_version),
                continue_token: None,
                remaining_item_count: None,
            },
            items,
        }
    }
}
