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

use crate::nanocloud::api::types::{Bundle, BundlePhase, BundleStatus};
use crate::nanocloud::k8s::store::{delete_bundle, list_bundles, normalize_namespace, save_bundle};

use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;

#[derive(Debug)]
pub enum BundleError {
    AlreadyExists(String),
    NotFound(String),
    Invalid(String),
    Conflict(String),
    Persistence(Box<dyn Error + Send + Sync>),
}

impl Display for BundleError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BundleError::AlreadyExists(msg)
            | BundleError::NotFound(msg)
            | BundleError::Invalid(msg)
            | BundleError::Conflict(msg) => f.write_str(msg),
            BundleError::Persistence(err) => write!(f, "{err}"),
        }
    }
}

impl Error for BundleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            BundleError::Persistence(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

impl BundleError {
    pub fn persistence_box(err: Box<dyn Error + Send + Sync>) -> Self {
        Self::Persistence(err)
    }
}

fn bundle_key(namespace: &str, name: &str) -> String {
    format!("{}/{}", normalize_namespace(Some(namespace)), name)
}

fn ensure_namespace(namespace: &str, bundle: &mut Bundle) -> String {
    let ns = bundle
        .metadata
        .namespace
        .clone()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| namespace.to_string());
    bundle.metadata.namespace = Some(ns.clone());
    match bundle.spec.namespace.as_deref() {
        Some("") => {
            bundle.spec.namespace = None;
        }
        Some(spec_ns) if spec_ns != ns => {
            bundle.spec.namespace = Some(ns.clone());
        }
        _ => {}
    }
    ns
}

fn ensure_name(name: &str, bundle: &mut Bundle) -> Result<String, BundleError> {
    if bundle.spec.service.trim().is_empty() {
        return Err(BundleError::Invalid("spec.service is required".to_string()));
    }

    let current = bundle
        .metadata
        .name
        .clone()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| name.to_string());
    if current != name {
        return Err(BundleError::Invalid(format!(
            "metadata.name '{}' does not match request name '{}'",
            current, name
        )));
    }
    bundle.metadata.name = Some(current.clone());
    Ok(current)
}

fn normalize_key_new(namespace: &str, bundle: &mut Bundle) -> Result<String, BundleError> {
    let ns = ensure_namespace(namespace, bundle);
    let name = bundle
        .metadata
        .name
        .clone()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| bundle.spec.service.clone());
    if name.trim().is_empty() {
        return Err(BundleError::Invalid(
            "metadata.name is required".to_string(),
        ));
    }
    ensure_name(&name, bundle)?;
    Ok(bundle_key(&ns, &name))
}

pub struct BundleRegistry {
    bundles: RwLock<HashMap<String, Bundle>>,
    resource_counter: AtomicU64,
}

static REGISTRY: OnceLock<Arc<BundleRegistry>> = OnceLock::new();

impl BundleRegistry {
    pub fn shared() -> Arc<Self> {
        REGISTRY
            .get_or_init(|| {
                let (items, counter) = load_initial_bundles();
                Arc::new(BundleRegistry {
                    bundles: RwLock::new(items),
                    resource_counter: AtomicU64::new(counter.max(1)),
                })
            })
            .clone()
    }

    pub async fn list(&self, namespace: Option<&str>) -> Vec<Bundle> {
        let bundles = self.bundles.read().await;
        bundles
            .values()
            .filter(|bundle| match namespace {
                Some(target) => {
                    bundle.metadata.namespace.as_deref().unwrap_or("default")
                        == normalize_namespace(Some(target))
                }
                None => true,
            })
            .cloned()
            .collect()
    }

    pub async fn get(&self, namespace: &str, name: &str) -> Option<Bundle> {
        let key = bundle_key(namespace, name);
        let bundles = self.bundles.read().await;
        bundles.get(&key).cloned()
    }

    pub async fn create(
        &self,
        namespace: &str,
        mut payload: Bundle,
    ) -> Result<Bundle, BundleError> {
        if payload.api_version.is_empty() {
            payload.api_version = "nanocloud.io/v1".to_string();
        }
        if payload.kind.is_empty() {
            payload.kind = "Bundle".to_string();
        }
        if payload.metadata.resource_version.is_some() {
            return Err(BundleError::Invalid(
                "resourceVersion must not be set on create".to_string(),
            ));
        }
        let key = normalize_key_new(namespace, &mut payload)?;

        {
            let bundles = self.bundles.read().await;
            if bundles.contains_key(&key) {
                return Err(BundleError::AlreadyExists(format!(
                    "Bundle '{}' already exists",
                    payload.metadata.name.clone().unwrap()
                )));
            }
        }

        let resource_version = self.next_resource_version();
        payload.metadata.resource_version = Some(resource_version.clone());

        if payload.status.is_none() {
            payload.status = Some(BundleStatus {
                phase: Some(BundlePhase::Pending),
                ..Default::default()
            });
        }

        let stored = payload.clone();
        save_bundle(
            stored.metadata.namespace.as_deref(),
            stored.metadata.name.as_deref().unwrap(),
            &stored,
        )
        .map_err(BundleError::persistence_box)?;

        {
            let mut bundles = self.bundles.write().await;
            bundles.insert(key, stored.clone());
        }

        Ok(stored)
    }

    pub async fn update_status(
        &self,
        namespace: &str,
        name: &str,
        status: BundleStatus,
        expected_resource_version: Option<&str>,
    ) -> Result<Bundle, BundleError> {
        let key = bundle_key(namespace, name);

        let existing = {
            let bundles = self.bundles.read().await;
            bundles.get(&key).cloned()
        };

        let Some(mut existing) = existing else {
            return Err(BundleError::NotFound(format!(
                "Bundle '{}' not found",
                name
            )));
        };

        if let Some(expected) = expected_resource_version {
            if existing.metadata.resource_version.as_deref().unwrap_or("") != expected {
                return Err(BundleError::Conflict(
                    "resourceVersion does not match current Bundle".to_string(),
                ));
            }
        }

        let resource_version = self.next_resource_version();
        let mut status = status;
        if let Ok(parsed) = resource_version.parse::<i64>() {
            status.observed_generation = Some(parsed);
        }
        existing.metadata.resource_version = Some(resource_version);
        existing.status = Some(status);
        save_bundle(
            existing.metadata.namespace.as_deref(),
            existing.metadata.name.as_deref().unwrap(),
            &existing,
        )
        .map_err(BundleError::persistence_box)?;

        {
            let mut bundles = self.bundles.write().await;
            bundles.insert(key, existing.clone());
        }

        Ok(existing)
    }

    pub async fn update_spec_profile(
        &self,
        namespace: &str,
        name: &str,
        profile_key: String,
        options: HashMap<String, String>,
    ) -> Result<Bundle, BundleError> {
        let key = bundle_key(namespace, name);

        let existing = {
            let bundles = self.bundles.read().await;
            bundles.get(&key).cloned()
        };

        let Some(mut existing) = existing else {
            return Err(BundleError::NotFound(format!(
                "Bundle '{}' not found",
                name
            )));
        };

        let noop = existing.spec.profile_key.as_deref() == Some(profile_key.as_str())
            && existing.spec.options == options;
        if noop {
            return Ok(existing);
        }

        existing.spec.profile_key = Some(profile_key);
        existing.spec.options = options;
        let resource_version = self.next_resource_version();
        existing.metadata.resource_version = Some(resource_version);

        save_bundle(
            existing.metadata.namespace.as_deref(),
            existing.metadata.name.as_deref().unwrap(),
            &existing,
        )
        .map_err(BundleError::persistence_box)?;

        {
            let mut bundles = self.bundles.write().await;
            bundles.insert(key, existing.clone());
        }

        Ok(existing)
    }

    pub async fn delete(&self, namespace: &str, name: &str) -> Result<(), BundleError> {
        let key = bundle_key(namespace, name);

        {
            let mut bundles = self.bundles.write().await;
            if bundles.remove(&key).is_none() {
                return Err(BundleError::NotFound(format!(
                    "Bundle '{}' not found",
                    name
                )));
            }
        }

        delete_bundle(Some(namespace), name).map_err(BundleError::persistence_box)
    }

    fn next_resource_version(&self) -> String {
        let next = self.resource_counter.fetch_add(1, Ordering::SeqCst);
        next.to_string()
    }
}

fn load_initial_bundles() -> (HashMap<String, Bundle>, u64) {
    let mut items = HashMap::new();
    let mut max_version = 1u64;

    let existing = list_bundles(None).unwrap_or_default();
    for mut bundle in existing {
        let namespace = bundle
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let name = bundle
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| bundle.spec.service.clone());
        if bundle.metadata.resource_version.is_none() {
            bundle.metadata.resource_version = Some("1".to_string());
        }
        if let Some(rv) = bundle.metadata.resource_version.as_deref() {
            if let Ok(parsed) = rv.parse::<u64>() {
                if parsed > max_version {
                    max_version = parsed;
                }
            }
        }

        let key = bundle_key(&namespace, &name);
        items.insert(key, bundle);
    }

    (items, max_version.saturating_add(1))
}
