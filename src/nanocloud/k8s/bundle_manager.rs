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

use crate::nanocloud::api::schema::{
    format_bundle_error_summary, validate_bundle, BundleSchemaError,
};
use crate::nanocloud::api::types::{Bundle, BundlePhase, BundleStatus};
use crate::nanocloud::k8s::ownership::BundleFieldOwnership;
use crate::nanocloud::k8s::store::{
    delete_bundle, delete_bundle_field_ownership, list_bundles, load_bundle_field_ownership,
    normalize_namespace, save_bundle, save_bundle_field_ownership,
};

use dashmap::DashMap;
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::{Mutex, RwLock};

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

#[derive(Debug, Clone)]
pub struct ApplyFieldConflict {
    pub path: String,
    pub owner: String,
}

#[derive(Debug)]
pub enum BundleApplyError {
    Conflict {
        message: String,
        conflicts: Vec<ApplyFieldConflict>,
    },
    Failure(BundleError),
}

impl BundleApplyError {
    pub fn conflict(message: impl Into<String>, conflicts: Vec<ApplyFieldConflict>) -> Self {
        Self::Conflict {
            message: message.into(),
            conflicts,
        }
    }
}

impl From<BundleError> for BundleApplyError {
    fn from(err: BundleError) -> Self {
        BundleApplyError::Failure(err)
    }
}

pub struct BundleApplyOptions<'a> {
    pub manager: &'a str,
    pub force: bool,
    pub dry_run: bool,
}

fn bundle_key(namespace: &str, name: &str) -> String {
    format!("{}/{}", normalize_namespace(Some(namespace)), name)
}

fn ensure_identity(bundle: &mut Bundle) {
    if bundle.api_version.trim().is_empty() {
        bundle.api_version = "nanocloud.io/v1".to_string();
    }
    if bundle.kind.trim().is_empty() {
        bundle.kind = "Bundle".to_string();
    }
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

fn schema_error(err: BundleSchemaError) -> BundleError {
    match err {
        BundleSchemaError::UnsupportedVersion(version) => BundleError::Invalid(format!(
            "Unsupported bundle apiVersion '{}'; supported: nanocloud.io/v1",
            version
        )),
        BundleSchemaError::Validation(issues) => {
            let detail = format_bundle_error_summary(&issues);
            BundleError::Invalid(detail)
        }
    }
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
    apply_guards: DashMap<String, Arc<Mutex<()>>>,
}

static REGISTRY: OnceLock<Arc<BundleRegistry>> = OnceLock::new();
const BUNDLE_CONTROLLER_MANAGER: &str = "controller/bundle";

impl BundleRegistry {
    pub fn shared() -> Arc<Self> {
        REGISTRY
            .get_or_init(|| {
                let (items, counter) = load_initial_bundles();
                Arc::new(BundleRegistry {
                    bundles: RwLock::new(items),
                    resource_counter: AtomicU64::new(counter.max(1)),
                    apply_guards: DashMap::new(),
                })
            })
            .clone()
    }

    fn guard_for(&self, key: &str) -> Arc<Mutex<()>> {
        self.apply_guards
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
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
        ensure_identity(&mut payload);
        if payload.metadata.resource_version.is_some() {
            return Err(BundleError::Invalid(
                "resourceVersion must not be set on create".to_string(),
            ));
        }
        let key = normalize_key_new(namespace, &mut payload)?;
        validate_bundle(&payload).map_err(schema_error)?;

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
        if status.observed_generation.is_none() {
            if let Ok(parsed) = resource_version.parse::<i64>() {
                status.observed_generation = Some(parsed);
            }
        }
        existing.metadata.resource_version = Some(resource_version);
        existing.status = Some(status);
        save_bundle(
            existing.metadata.namespace.as_deref(),
            existing.metadata.name.as_deref().unwrap(),
            &existing,
        )
        .map_err(BundleError::persistence_box)?;
        self.record_field_owners(namespace, name, BUNDLE_CONTROLLER_MANAGER, &["/status"])
            .await?;

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
        ensure_identity(&mut existing);
        validate_bundle(&existing).map_err(schema_error)?;
        let resource_version = self.next_resource_version();
        existing.metadata.resource_version = Some(resource_version);

        save_bundle(
            existing.metadata.namespace.as_deref(),
            existing.metadata.name.as_deref().unwrap(),
            &existing,
        )
        .map_err(BundleError::persistence_box)?;
        self.record_field_owners(
            namespace,
            name,
            BUNDLE_CONTROLLER_MANAGER,
            &["/spec/options", "/spec/key"],
        )
        .await?;

        {
            let mut bundles = self.bundles.write().await;
            bundles.insert(key, existing.clone());
        }

        Ok(existing)
    }

    pub async fn apply_bundle(
        &self,
        namespace: &str,
        name: &str,
        payload: Value,
        options: BundleApplyOptions<'_>,
    ) -> Result<Bundle, BundleApplyError> {
        let key = bundle_key(namespace, name);
        let guard = self.guard_for(&key);
        let _permit = guard.lock().await;

        let existing = {
            let bundles = self.bundles.read().await;
            bundles.get(&key).cloned()
        };

        let Some(mut bundle) = existing else {
            return Err(BundleApplyError::from(BundleError::NotFound(format!(
                "Bundle '{name}' not found"
            ))));
        };

        let spec_patch = extract_spec_patch(&payload)?;
        let touched = collect_spec_pointers(&spec_patch);
        if touched.is_empty() {
            return Err(BundleApplyError::from(BundleError::Invalid(
                "Apply payload must include at least one spec field".to_string(),
            )));
        }

        let mut ownership = self
            .load_field_ownership(namespace, name)
            .await
            .map_err(BundleApplyError::from)?;

        let mut conflicts = Vec::new();
        for pointer in &touched {
            if let Some(owner) = ownership.manager_for(pointer) {
                if owner != options.manager {
                    conflicts.push(ApplyFieldConflict {
                        path: pointer.clone(),
                        owner: owner.to_string(),
                    });
                }
            }
        }

        if !conflicts.is_empty() && !options.force {
            return Err(BundleApplyError::conflict(
                "Apply would modify fields managed by another actor",
                conflicts,
            ));
        }

        let mut spec_value = serde_json::to_value(&bundle.spec).map_err(|err| {
            BundleApplyError::from(BundleError::Invalid(format!(
                "Failed to serialize Bundle spec: {err}"
            )))
        })?;

        merge_spec(&mut spec_value, &spec_patch);

        bundle.spec = serde_json::from_value(spec_value).map_err(|err| {
            BundleApplyError::from(BundleError::Invalid(format!(
                "Failed to deserialize Bundle spec: {err}"
            )))
        })?;

        ensure_identity(&mut bundle);
        validate_bundle(&bundle)
            .map_err(schema_error)
            .map_err(BundleApplyError::from)?;

        if options.dry_run {
            return Ok(bundle);
        }

        let resource_version = self.next_resource_version();
        bundle.metadata.resource_version = Some(resource_version);

        save_bundle(
            bundle.metadata.namespace.as_deref(),
            bundle.metadata.name.as_deref().unwrap(),
            &bundle,
        )
        .map_err(BundleError::persistence_box)
        .map_err(BundleApplyError::from)?;

        for pointer in &touched {
            ownership.set_owner(pointer, options.manager);
        }
        let bundle_namespace = bundle.metadata.namespace.as_deref().unwrap_or(namespace);
        let bundle_name = bundle.metadata.name.as_deref().unwrap_or(name);
        self.save_field_ownership(bundle_namespace, bundle_name, &ownership)
            .await
            .map_err(BundleApplyError::from)?;

        {
            let mut bundles = self.bundles.write().await;
            bundles.insert(key, bundle.clone());
        }

        Ok(bundle)
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

        delete_bundle(Some(namespace), name).map_err(BundleError::persistence_box)?;
        self.delete_field_ownership(namespace, name).await
    }

    pub async fn load_field_ownership(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<BundleFieldOwnership, BundleError> {
        load_bundle_field_ownership(Some(namespace), name).map_err(BundleError::persistence_box)
    }

    pub async fn save_field_ownership(
        &self,
        namespace: &str,
        name: &str,
        ownership: &BundleFieldOwnership,
    ) -> Result<(), BundleError> {
        save_bundle_field_ownership(Some(namespace), name, ownership)
            .map_err(BundleError::persistence_box)
    }

    pub async fn delete_field_ownership(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<(), BundleError> {
        delete_bundle_field_ownership(Some(namespace), name).map_err(BundleError::persistence_box)
    }

    async fn record_field_owners(
        &self,
        namespace: &str,
        name: &str,
        manager: &str,
        pointers: &[&str],
    ) -> Result<(), BundleError> {
        let mut ownership = self.load_field_ownership(namespace, name).await?;
        for pointer in pointers {
            ownership.set_owner(pointer, manager);
        }
        self.save_field_ownership(namespace, name, &ownership).await
    }

    fn next_resource_version(&self) -> String {
        let next = self.resource_counter.fetch_add(1, Ordering::SeqCst);
        next.to_string()
    }
}

fn extract_spec_patch(payload: &Value) -> Result<Value, BundleApplyError> {
    match payload.get("spec") {
        Some(spec) if spec.is_object() => Ok(spec.clone()),
        Some(_) => Err(BundleApplyError::from(BundleError::Invalid(
            "Apply payload 'spec' must be an object".to_string(),
        ))),
        None => Err(BundleApplyError::from(BundleError::Invalid(
            "Apply payload must include a 'spec' object".to_string(),
        ))),
    }
}

fn collect_spec_pointers(spec_patch: &Value) -> Vec<String> {
    let mut unique = BTreeSet::new();
    if let Value::Object(map) = spec_patch {
        for key in map.keys() {
            unique.insert(format!("/spec/{key}"));
        }
    }
    unique.into_iter().collect()
}

fn merge_spec(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(target_map), Value::Object(patch_map)) => {
            for (key, value) in patch_map {
                if value.is_null() {
                    target_map.remove(key);
                    continue;
                }
                match target_map.get_mut(key) {
                    Some(entry) => merge_spec(entry, value),
                    None => {
                        target_map.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (target_value, patch_value) => {
            *target_value = patch_value.clone();
        }
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
