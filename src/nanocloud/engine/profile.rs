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

use crate::nanocloud::k8s::bundle_manager::BundleRegistry;
use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::util::security::kms;
use crate::nanocloud::util::security::EncryptionKey;

use base64;
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;

#[derive(Serialize, Deserialize)]
struct JsonProfile {
    #[serde(default)]
    pub version: u32,
    pub key: String,
    #[serde(default)]
    pub key_id: Option<String>,
    #[serde(default)]
    pub config: HashMap<String, String>,
}

pub struct Profile {
    pub key: EncryptionKey,
    pub config: HashMap<String, Vec<u8>>,
}

impl Profile {
    pub fn from_options(
        options: &HashMap<String, String>,
    ) -> Result<Profile, Box<dyn Error + Send + Sync>> {
        Ok(Profile {
            key: EncryptionKey::new(None),
            config: options
                .clone()
                .into_iter()
                .map(|(key, value)| (key, value.into_bytes()))
                .collect(),
        })
    }

    #[allow(dead_code)] // Exercised from integration tests; regular builds treat it as unused.
    /// Convenience parser used by integration tests to assert on profile serialization.
    pub fn from_json(buffer: Vec<u8>) -> Result<Profile, Box<dyn Error + Send + Sync>> {
        let json: JsonProfile = serde_json::from_slice(&buffer)?;
        unwrap_profile(&json)
    }

    #[allow(dead_code)] // Exercised from integration tests; regular builds treat it as unused.
    /// Convenience serializer used by integration tests to inspect profile payloads.
    pub fn to_json(&self) -> Result<String, Box<dyn Error + Send + Sync>> {
        let profile = wrap_profile(self)?;
        Ok(serde_json::to_string_pretty(&profile)?)
    }

    pub fn to_serialized_fields(
        &self,
    ) -> Result<(String, HashMap<String, String>), Box<dyn Error + Send + Sync>> {
        let serialized = wrap_profile(self)?;
        Ok((serialized.key, serialized.config))
    }

    pub fn extend(&self, options: &HashMap<String, String>) -> HashMap<String, String> {
        self.config
            .iter()
            .map(|(key, value)| (key.to_string(), String::from_utf8_lossy(value).into_owned()))
            .chain(options.clone())
            .collect()
    }

    pub async fn load(
        namespace: Option<&str>,
        app: &str,
    ) -> Result<Profile, Box<dyn Error + Send + Sync>> {
        let namespace = namespace.unwrap_or("default");
        let registry = BundleRegistry::shared();
        let bundle = registry
            .get(namespace, app)
            .await
            .ok_or_else(|| new_error(format!("Bundle '{namespace}/{app}' not found")))?;
        Profile::from_spec_fields(bundle.spec.profile_key.as_deref(), &bundle.spec.options)
    }

    pub fn from_spec_fields(
        profile_key: Option<&str>,
        options: &HashMap<String, String>,
    ) -> Result<Profile, Box<dyn Error + Send + Sync>> {
        let key = profile_key.ok_or_else(|| new_error("bundle spec is missing profile key"))?;
        let json = JsonProfile {
            version: 0,
            key: key.to_string(),
            key_id: None,
            config: options.clone(),
        };
        unwrap_profile(&json)
    }

    pub fn set_options(&mut self, options: &HashMap<String, String>) {
        self.config = options
            .iter()
            .map(|(key, value)| (key.clone(), value.clone().into_bytes()))
            .collect();
    }
}

fn wrap_profile(profile: &Profile) -> Result<JsonProfile, Box<dyn Error + Send + Sync>> {
    let engine = base64::engine::general_purpose::STANDARD;
    let key = profile
        .key
        .wrap()
        .map_err(|e| with_context(e, "Failed to wrap profile encryption key"))?;

    let mut config = HashMap::with_capacity(profile.config.len());
    for (name, value) in &profile.config {
        let rendered = if name.starts_with("secret_") {
            profile.key.encrypt(value).map_err(|e| {
                with_context(e, format!("Failed to encrypt profile entry '{}'", name))
            })?
        } else if name.starts_with("base64_") {
            engine.encode(value)
        } else {
            String::from_utf8(value.clone()).map_err(|e| {
                with_context(
                    e,
                    format!("Profile entry '{}' contains invalid UTF-8", name),
                )
            })?
        };
        config.insert(name.clone(), rendered);
    }

    let key_id = profile
        .key
        .key_id()
        .map(|id| id.to_string())
        .or_else(|| kms::global_kms().default_key_id())
        .unwrap_or_else(|| "local".to_string());

    Ok(JsonProfile {
        version: 2,
        key,
        key_id: Some(key_id),
        config,
    })
}

fn unwrap_profile(profile: &JsonProfile) -> Result<Profile, Box<dyn Error + Send + Sync>> {
    let engine = base64::engine::general_purpose::STANDARD;
    let key = EncryptionKey::unwrap(&profile.key)
        .map_err(|e| with_context(e, "Failed to unwrap profile encryption key"))?;

    let mut config = HashMap::with_capacity(profile.config.len());
    for (name, value) in &profile.config {
        let bytes = if name.starts_with("secret_") {
            key.decrypt(value).map_err(|e| {
                with_context(e, format!("Failed to decrypt profile entry '{}'", name))
            })?
        } else if name.starts_with("base64_") {
            engine.decode(value).map_err(|e| {
                with_context(
                    e,
                    format!("Failed to decode base64 profile entry '{}'", name),
                )
            })?
        } else {
            value.clone().into_bytes()
        };
        config.insert(name.clone(), bytes);
    }

    Ok(Profile { key, config })
}
