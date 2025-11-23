use super::secret::Secret;
use super::store::{normalize_namespace, paginate_entries, ListCursor, PaginatedResult};
use crate::nanocloud::k8s::identity::new_uid;
use crate::nanocloud::secrets::{KeyspaceSecretStore, SecretMaterial};

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::{broadcast, RwLock};

const WATCH_BUFFER_SIZE: usize = 32;

#[derive(Debug)]
pub enum SecretError {
    AlreadyExists(String),
    NotFound(String),
    Invalid(String),
    Conflict(String),
    Persistence(Box<dyn Error + Send + Sync>),
}

impl Display for SecretError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SecretError::AlreadyExists(msg)
            | SecretError::NotFound(msg)
            | SecretError::Invalid(msg)
            | SecretError::Conflict(msg) => f.write_str(msg),
            SecretError::Persistence(err) => write!(f, "{err}"),
        }
    }
}

impl Error for SecretError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            SecretError::Persistence(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

impl SecretError {
    pub fn persistence_box(err: Box<dyn Error + Send + Sync>) -> Self {
        SecretError::Persistence(err)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct SecretWatchEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub object: Secret,
}

#[derive(Clone, Hash, Eq, PartialEq)]
enum WatchScope {
    Cluster,
    Namespace(String),
    Secret(String),
}

fn secret_key(namespace: &str, name: &str) -> String {
    format!("{}/{}", normalize_namespace(Some(namespace)), name)
}

fn ensure_namespace(namespace: &str, secret: &mut Secret) -> String {
    let ns = secret
        .metadata
        .namespace
        .clone()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| namespace.to_string());
    secret.metadata.namespace = Some(ns.clone());
    ns
}

fn ensure_name(name: &str, secret: &mut Secret) -> Result<String, SecretError> {
    let current = secret
        .metadata
        .name
        .clone()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| name.to_string());
    if current != name {
        return Err(SecretError::Invalid(format!(
            "metadata.name '{}' does not match request name '{}'",
            current, name
        )));
    }
    secret.metadata.name = Some(current.clone());
    Ok(current)
}

fn normalize_key(namespace: &str, name: &str, secret: &mut Secret) -> Result<String, SecretError> {
    let ns = ensure_namespace(namespace, secret);
    let name = ensure_name(name, secret)?;
    secret.metadata.ensure_common_fields(Some(&ns), Some(&name));
    Ok(secret_key(&ns, &name))
}

fn normalize_key_new(namespace: &str, secret: &mut Secret) -> Result<String, SecretError> {
    let ns = ensure_namespace(namespace, secret);
    let name = secret
        .metadata
        .name
        .clone()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| SecretError::Invalid("metadata.name is required".to_string()))?;
    secret.metadata.ensure_common_fields(Some(&ns), Some(&name));
    Ok(secret_key(&ns, &name))
}

fn decode_data(secret: &Secret) -> Result<BTreeMap<String, String>, SecretError> {
    let mut material = BTreeMap::new();
    for (key, value) in secret.data.iter() {
        let decoded = BASE64_STANDARD
            .decode(value)
            .map_err(|_| SecretError::Invalid(format!("invalid base64 data for key '{key}'")))?;
        let plain = String::from_utf8_lossy(&decoded).to_string();
        material.insert(key.clone(), plain);
    }
    Ok(material)
}

fn secret_from_material(mut material: SecretMaterial) -> Secret {
    let mut data = HashMap::new();
    for (key, value) in material.data.iter() {
        data.insert(key.clone(), BASE64_STANDARD.encode(value.as_bytes()));
    }
    let mut metadata = crate::nanocloud::k8s::pod::ObjectMeta {
        namespace: Some(material.namespace.clone()),
        name: Some(material.name.clone()),
        resource_version: material.resource_version.take(),
        uid: Some(new_uid()),
        ..Default::default()
    };
    let namespace_hint = metadata.namespace.clone();
    let name_hint = metadata.name.clone();
    metadata.ensure_common_fields(namespace_hint.as_deref(), name_hint.as_deref());

    Secret {
        api_version: "v1".to_string(),
        kind: "Secret".to_string(),
        metadata,
        data,
        string_data: HashMap::new(),
        secret_type: Some(material.type_name),
        immutable: Some(material.immutable),
    }
}

pub struct SecretRegistry {
    secrets: RwLock<HashMap<String, Secret>>,
    watchers: RwLock<HashMap<WatchScope, broadcast::Sender<SecretWatchEvent>>>,
    resource_counter: AtomicU64,
    store: KeyspaceSecretStore,
}

static REGISTRY: OnceLock<Arc<SecretRegistry>> = OnceLock::new();

impl SecretRegistry {
    pub fn shared() -> Arc<Self> {
        REGISTRY
            .get_or_init(|| {
                let (items, counter) = load_initial_secrets();
                Arc::new(SecretRegistry {
                    secrets: RwLock::new(items),
                    watchers: RwLock::new(HashMap::new()),
                    resource_counter: AtomicU64::new(counter.max(1)),
                    store: KeyspaceSecretStore::new(),
                })
            })
            .clone()
    }

    pub fn current_resource_version(&self) -> String {
        let current = self.resource_counter.load(Ordering::SeqCst);
        current.saturating_sub(1).to_string()
    }

    pub async fn list_since(
        &self,
        namespace: Option<&str>,
        resource_version: Option<u64>,
    ) -> Vec<Secret> {
        self.list_paginated(namespace, resource_version, None, None)
            .await
            .map(|page| page.items)
            .unwrap_or_default()
    }

    pub async fn list_paginated(
        &self,
        namespace: Option<&str>,
        resource_version: Option<u64>,
        limit: Option<u32>,
        cursor: Option<&ListCursor>,
    ) -> Result<PaginatedResult<Secret>, SecretError> {
        let entries = self.collect_entries(namespace, resource_version).await;
        paginate_entries(entries, cursor, limit)
            .map_err(|err| SecretError::Invalid(err.to_string()))
    }

    pub async fn collect_entries(
        &self,
        namespace: Option<&str>,
        resource_version: Option<u64>,
    ) -> Vec<(String, Secret, Option<String>)> {
        let secrets = self.secrets.read().await;
        let namespace_filter = namespace.map(|ns| normalize_namespace(Some(ns)));
        let mut entries = Vec::new();
        for (key, secret) in secrets.iter() {
            if let Some(filter) = &namespace_filter {
                if !key.starts_with(&format!("{}/", filter)) {
                    continue;
                }
            }

            if resource_version.is_some_and(|threshold| {
                secret
                    .metadata
                    .resource_version
                    .as_deref()
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(|current| current <= threshold)
                    .unwrap_or(false)
            }) {
                continue;
            }

            entries.push((
                key.clone(),
                secret.clone(),
                secret.metadata.resource_version.clone(),
            ));
        }
        entries
    }

    pub async fn get(&self, namespace: &str, name: &str) -> Option<Secret> {
        let key = secret_key(namespace, name);
        self.secrets.read().await.get(&key).cloned()
    }

    pub async fn create(
        &self,
        namespace: &str,
        mut payload: Secret,
    ) -> Result<Secret, SecretError> {
        if payload.metadata.resource_version.is_some() {
            return Err(SecretError::Invalid(
                "resourceVersion must not be set on create".to_string(),
            ));
        }

        payload.encode_string_data();
        let material = decode_data(&payload)?;

        let key = normalize_key_new(namespace, &mut payload)?;
        {
            let secrets = self.secrets.read().await;
            if secrets.contains_key(&key) {
                return Err(SecretError::AlreadyExists(format!(
                    "Secret '{}' already exists",
                    payload.metadata.name.clone().unwrap_or_default()
                )));
            }
        }

        let resource_version = self.next_resource_version();
        payload.metadata.resource_version = Some(resource_version.clone());

        let stored_material = SecretMaterial {
            namespace: payload.metadata.namespace.clone().unwrap_or_default(),
            name: payload.metadata.name.clone().unwrap_or_default(),
            type_name: payload
                .secret_type
                .clone()
                .unwrap_or_else(|| "Opaque".to_string()),
            immutable: payload.immutable.unwrap_or(false),
            data: material,
            resource_version: payload.metadata.resource_version.clone(),
        };
        self.store
            .put(stored_material)
            .map_err(SecretError::persistence_box)?;

        {
            let mut secrets = self.secrets.write().await;
            secrets.insert(key.clone(), payload.clone());
        }

        self.broadcast(&payload, "ADDED").await;
        Ok(payload)
    }

    pub async fn replace(
        &self,
        namespace: &str,
        name: &str,
        mut payload: Secret,
    ) -> Result<Secret, SecretError> {
        payload.encode_string_data();
        let material = decode_data(&payload)?;

        let key = normalize_key(namespace, name, &mut payload)?;

        let existing = {
            let secrets = self.secrets.read().await;
            secrets.get(&key).cloned()
        };

        let Some(existing) = existing else {
            return Err(SecretError::NotFound(format!(
                "Secret '{}' not found",
                name
            )));
        };

        if existing.immutable.unwrap_or(false) && existing.data != payload.data {
            return Err(SecretError::Conflict(format!(
                "Secret '{}' is immutable",
                name
            )));
        }

        if let Some(resource_version) = payload.metadata.resource_version.as_deref() {
            if existing.metadata.resource_version.as_deref().unwrap_or("") != resource_version {
                return Err(SecretError::Conflict(
                    "resourceVersion does not match current Secret".to_string(),
                ));
            }
        }

        let resource_version = self.next_resource_version();
        payload.metadata.resource_version = Some(resource_version.clone());

        let stored_material = SecretMaterial {
            namespace: payload.metadata.namespace.clone().unwrap_or_default(),
            name: payload.metadata.name.clone().unwrap_or_default(),
            type_name: payload
                .secret_type
                .clone()
                .unwrap_or_else(|| "Opaque".to_string()),
            immutable: payload.immutable.unwrap_or(false),
            data: material,
            resource_version: payload.metadata.resource_version.clone(),
        };

        self.store
            .put(stored_material)
            .map_err(SecretError::persistence_box)?;

        {
            let mut secrets = self.secrets.write().await;
            secrets.insert(key.clone(), payload.clone());
        }

        self.broadcast(&payload, "MODIFIED").await;
        Ok(payload)
    }

    pub async fn delete(&self, namespace: &str, name: &str) -> Result<Secret, SecretError> {
        let key = secret_key(namespace, name);
        let removed = {
            let mut secrets = self.secrets.write().await;
            secrets.remove(&key)
        };

        let Some(secret) = removed else {
            return Err(SecretError::NotFound(format!(
                "Secret '{}' not found",
                name
            )));
        };

        self.store
            .delete(
                secret.metadata.namespace.as_deref().unwrap_or("default"),
                secret.metadata.name.as_deref().unwrap_or(name),
            )
            .map_err(SecretError::persistence_box)?;

        self.broadcast(&secret, "DELETED").await;
        Ok(secret)
    }

    pub async fn watch_cluster(&self) -> broadcast::Receiver<SecretWatchEvent> {
        self.ensure_watch(WatchScope::Cluster).await
    }

    pub async fn watch_namespace(&self, namespace: &str) -> broadcast::Receiver<SecretWatchEvent> {
        let scope = WatchScope::Namespace(normalize_namespace(Some(namespace)));
        self.ensure_watch(scope).await
    }

    pub async fn watch_secret(
        &self,
        namespace: &str,
        name: &str,
    ) -> broadcast::Receiver<SecretWatchEvent> {
        let scope = WatchScope::Secret(secret_key(namespace, name));
        self.ensure_watch(scope).await
    }

    fn next_resource_version(&self) -> String {
        self.resource_counter
            .fetch_add(1, Ordering::SeqCst)
            .to_string()
    }

    async fn ensure_watch(&self, scope: WatchScope) -> broadcast::Receiver<SecretWatchEvent> {
        let mut watchers = self.watchers.write().await;
        watchers
            .entry(scope.clone())
            .or_insert_with(|| broadcast::channel(WATCH_BUFFER_SIZE).0)
            .subscribe()
    }

    async fn broadcast(&self, secret: &Secret, event_type: &str) {
        let event = SecretWatchEvent {
            event_type: event_type.to_string(),
            object: secret.clone(),
        };

        let namespace = secret
            .metadata
            .namespace
            .as_deref()
            .map(|ns| normalize_namespace(Some(ns)))
            .unwrap_or_else(|| "default".to_string());
        let key = secret_key(&namespace, secret.metadata.name.as_deref().unwrap());

        let watchers = self.watchers.read().await;
        let mut targets: Vec<broadcast::Sender<SecretWatchEvent>> = Vec::new();

        if let Some(sender) = watchers.get(&WatchScope::Cluster) {
            targets.push(sender.clone());
        }
        if let Some(sender) = watchers.get(&WatchScope::Namespace(namespace.clone())) {
            targets.push(sender.clone());
        }
        if let Some(sender) = watchers.get(&WatchScope::Secret(key)) {
            targets.push(sender.clone());
        }
        drop(watchers);

        for sender in targets {
            let _ = sender.send(event.clone());
        }
    }
}

fn load_initial_secrets() -> (HashMap<String, Secret>, u64) {
    let store = KeyspaceSecretStore::new();
    let mut secrets = HashMap::new();
    let mut counter: u64 = 1;

    match store.list(None) {
        Ok(existing) => {
            for stored in existing.into_iter() {
                let mut secret = secret_from_material(stored.secret);
                let Some(name) = secret.metadata.name.clone() else {
                    continue;
                };
                let namespace = secret
                    .metadata
                    .namespace
                    .clone()
                    .filter(|ns| !ns.is_empty())
                    .unwrap_or_else(|| "default".to_string());

                if secret.metadata.resource_version.is_none() {
                    secret.metadata.resource_version = Some(counter.to_string());
                    counter = counter.saturating_add(1);
                } else if let Some(rv) = secret.metadata.resource_version.as_deref() {
                    if let Ok(parsed) = rv.parse::<u64>() {
                        counter = counter.max(parsed.saturating_add(1));
                    }
                }

                secret
                    .metadata
                    .ensure_common_fields(Some(namespace.as_str()), Some(name.as_str()));

                let key = secret_key(&namespace, &name);
                secrets.insert(key, secret);
            }
        }
        Err(err) => {
            eprintln!("Failed to load persisted Secrets: {err}");
        }
    }

    (secrets, counter)
}
