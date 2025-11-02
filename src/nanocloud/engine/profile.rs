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

use crate::nanocloud::api::types::{
    BindingHistoryEntry, BindingHistoryStatus, BundleProfileSecret,
};
use crate::nanocloud::k8s::bundle_manager::BundleRegistry;
use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::util::security::kms;
use crate::nanocloud::util::security::EncryptionKey;

use base64;
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;

type ProfileExportResult = Result<
    (String, HashMap<String, String>, Vec<BundleProfileSecret>),
    Box<dyn Error + Send + Sync>,
>;

pub const RESERVED_BINDINGS_KEY: &str = ".nanocloud.bindings.v1";

pub fn is_reserved_profile_key(key: &str) -> bool {
    key == RESERVED_BINDINGS_KEY
}

/// Status transitions recorded for bindings.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BindingStatus {
    #[default]
    Pending,
    Running,
    Succeeded,
    Failed,
    TimedOut,
}

/// Durable history entry for a single binding invocation chain.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BindingRecord {
    /// Stable identifier advertised by the Dockyard contract.
    pub binding_id: String,
    /// Service associated with the binding (e.g., `database`).
    pub service: String,
    /// Command array invoked for the binding step.
    pub command: Vec<String>,
    /// Total attempts made so far.
    pub attempts: u32,
    /// Last recorded status.
    pub status: BindingStatus,
    /// RFC3339 timestamp for the last start event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_started_at: Option<String>,
    /// RFC3339 timestamp for the last completion.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_finished_at: Option<String>,
    /// Milliseconds spent during the last attempt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Exit code reported by the binding command, when any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Human-readable failure context (e.g., timeout, signal).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Captured stdout snippet (UTF-8, truncated).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdout: Option<String>,
    /// Captured stderr snippet (UTF-8, truncated).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,
    /// Event identifier associated with the most recent outcome.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
}

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
    pub bindings: HashMap<String, BindingRecord>,
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
            bindings: HashMap::new(),
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

    pub fn export_components(&self) -> ProfileExportResult {
        let (profile_key, mut serialized) = self.to_serialized_fields()?;
        let key_id = self.key_identifier();
        let mut options: HashMap<String, String> = HashMap::new();
        let mut secrets = Vec::new();
        for (name, value) in serialized.drain() {
            if is_reserved_profile_key(&name) {
                continue;
            }
            if name.starts_with("secret_") {
                secrets.push(BundleProfileSecret {
                    name,
                    cipher_text: value,
                    key_id: key_id.clone(),
                });
            } else {
                options.insert(name, value);
            }
        }
        secrets.sort_by(|a, b| a.name.cmp(&b.name));
        Ok((profile_key, options, secrets))
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

    #[allow(dead_code)]
    /// Returns the stored binding record for the provided key, if any.
    pub fn binding_record(&self, key: &str) -> Option<&BindingRecord> {
        self.bindings.get(key)
    }

    /// Mutably retrieves or creates a binding record.
    pub fn binding_record_mut(
        &mut self,
        key: &str,
        binding_id: impl Into<String>,
        service: impl Into<String>,
        command: &[String],
    ) -> &mut BindingRecord {
        let entry = self
            .bindings
            .entry(key.to_string())
            .or_insert_with(|| BindingRecord {
                binding_id: binding_id.into(),
                service: service.into(),
                command: command.to_vec(),
                ..BindingRecord::default()
            });
        entry.command = command.to_vec();
        entry
    }

    #[allow(dead_code)]
    /// Replace the underlying bindings map (used when force-resetting state).
    pub fn set_bindings(&mut self, bindings: HashMap<String, BindingRecord>) {
        self.bindings = bindings;
    }

    fn key_identifier(&self) -> String {
        self.key
            .key_id()
            .map(|id| id.to_string())
            .or_else(|| kms::global_kms().default_key_id())
            .unwrap_or_else(|| "local".to_string())
    }

    pub fn binding_history_entries(&self) -> Vec<BindingHistoryEntry> {
        let mut entries: Vec<_> = self
            .bindings
            .values()
            .map(|record| BindingHistoryEntry {
                binding_id: record.binding_id.clone(),
                service: record.service.clone(),
                command: record.command.clone(),
                status: match record.status {
                    BindingStatus::Pending => BindingHistoryStatus::Pending,
                    BindingStatus::Running => BindingHistoryStatus::Running,
                    BindingStatus::Succeeded => BindingHistoryStatus::Succeeded,
                    BindingStatus::Failed => BindingHistoryStatus::Failed,
                    BindingStatus::TimedOut => BindingHistoryStatus::TimedOut,
                },
                attempts: record.attempts,
                last_started_at: record.last_started_at.clone(),
                last_finished_at: record.last_finished_at.clone(),
                duration_ms: record.duration_ms,
                exit_code: record.exit_code,
                message: record.message.clone(),
                stdout: record.stdout.clone(),
                stderr: record.stderr.clone(),
                event_id: record.event_id.clone(),
            })
            .collect();
        entries.sort_by(|a, b| {
            a.service
                .cmp(&b.service)
                .then_with(|| a.binding_id.cmp(&b.binding_id))
        });
        entries
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

    if !profile.bindings.is_empty() {
        let serialized = serde_json::to_string(&profile.bindings).map_err(|e| {
            with_context(
                e,
                "Failed to serialize binding records for profile persistence",
            )
        })?;
        config.insert(RESERVED_BINDINGS_KEY.to_string(), serialized);
    }

    let key_id = profile.key_identifier();

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
    let mut bindings = HashMap::new();
    for (name, value) in &profile.config {
        if is_reserved_profile_key(name) {
            bindings = serde_json::from_str(value).map_err(|e| {
                with_context(e, "Failed to parse stored binding records from profile")
            })?;
            continue;
        }
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

    Ok(Profile {
        key,
        config,
        bindings,
    })
}
