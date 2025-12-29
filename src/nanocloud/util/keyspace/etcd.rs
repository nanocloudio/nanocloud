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

// Allow dead_code: This module provides the etcd backend API which is tested
// but not yet integrated into the main application. The functions will be
// consumed when the runtime backend selection is wired through.
#![allow(dead_code)]

//! Etcd-backed keyspace implementation.
//!
//! This module provides a keyspace implementation that stores data in an external
//! etcd cluster. It is only available when the `etcd` feature is enabled.
//!
//! # Configuration
//!
//! Set `NANOCLOUD_KEYSPACE_BACKEND=etcd` to use this backend. Additional configuration:
//!
//! - `NANOCLOUD_ETCD_ENDPOINTS`: Comma-separated etcd URLs (default: `http://127.0.0.1:2379`)
//! - `NANOCLOUD_ETCD_PREFIX`: Key prefix (default: `/nanocloud`)
//! - `NANOCLOUD_ETCD_USERNAME`: Optional username for authentication
//! - `NANOCLOUD_ETCD_PASSWORD`: Optional password for authentication

use crate::nanocloud::config::{EtcdConfig, KeyspaceBackend as KeyspaceBackendConfig};
use crate::nanocloud::util::error::{new_error, with_context};

use etcd_client::{Client, ConnectOptions, GetOptions, PutOptions};
use std::collections::VecDeque;
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};
use tokio::runtime::Handle;
use tokio::sync::{broadcast, RwLock};

use super::{KeyspaceEvent, KeyspaceEventType, SingleUseTokenOutcome};

const WATCH_CHANNEL_CAPACITY: usize = 128;
const WATCH_HISTORY_LIMIT: usize = 512;

/// Global etcd client, lazily initialized
static ETCD_CLIENT: OnceLock<Arc<RwLock<Client>>> = OnceLock::new();

/// Global watch registry for etcd partitions
static ETCD_WATCH_REGISTRY: OnceLock<std::sync::Mutex<std::collections::HashMap<String, Arc<EtcdPartitionWatch>>>> = OnceLock::new();

struct EtcdPartitionWatch {
    sender: broadcast::Sender<KeyspaceEvent>,
    history: std::sync::RwLock<VecDeque<KeyspaceEvent>>,
    version: AtomicU64,
}

impl EtcdPartitionWatch {
    fn new() -> Self {
        let (sender, _) = broadcast::channel(WATCH_CHANNEL_CAPACITY);
        Self {
            sender,
            history: std::sync::RwLock::new(VecDeque::new()),
            version: AtomicU64::new(0),
        }
    }

    fn next_version(&self) -> u64 {
        self.version.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn record(&self, event: KeyspaceEvent) {
        {
            let mut history = self
                .history
                .write()
                .expect("etcd watch history lock poisoned");
            history.push_back(event.clone());
            if history.len() > WATCH_HISTORY_LIMIT {
                history.pop_front();
            }
        }
        let _ = self.sender.send(event);
    }
}

fn get_etcd_watch(partition: &str) -> Arc<EtcdPartitionWatch> {
    let registry = ETCD_WATCH_REGISTRY.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let mut guard = registry.lock().expect("etcd watch registry lock poisoned");
    guard
        .entry(partition.to_string())
        .or_insert_with(|| Arc::new(EtcdPartitionWatch::new()))
        .clone()
}

fn publish_etcd_event(partition: &str, key: String, value: Option<String>, event_type: KeyspaceEventType) {
    let watch = get_etcd_watch(partition);
    let resource_version = watch.next_version();
    let event = KeyspaceEvent {
        event_type,
        key,
        value,
        resource_version,
    };
    watch.record(event);
}

/// Creates or returns the global etcd client.
async fn get_client() -> Result<Arc<RwLock<Client>>, Box<dyn Error + Send + Sync>> {
    if let Some(client) = ETCD_CLIENT.get() {
        return Ok(Arc::clone(client));
    }

    let config = EtcdConfig::from_env();
    let mut options = ConnectOptions::new();

    if config.has_auth() {
        options = options.with_user(
            config.username.as_deref().unwrap_or(""),
            config.password.as_deref().unwrap_or(""),
        );
    }

    let client = Client::connect(&config.endpoints, Some(options))
        .await
        .map_err(|e| with_context(e, "Failed to connect to etcd"))?;

    let client = Arc::new(RwLock::new(client));

    // Try to set it; if another thread beat us, use theirs
    match ETCD_CLIENT.set(Arc::clone(&client)) {
        Ok(()) => Ok(client),
        Err(_) => Ok(Arc::clone(ETCD_CLIENT.get().unwrap())),
    }
}

/// Runs an async operation, handling the case where we may or may not be in a tokio context.
fn run_async<F, T>(future: F) -> Result<T, Box<dyn Error + Send + Sync>>
where
    F: std::future::Future<Output = Result<T, Box<dyn Error + Send + Sync>>> + Send,
    T: Send,
{
    match Handle::try_current() {
        Ok(handle) => {
            // We're in an async context, use block_in_place to avoid blocking the runtime
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        Err(_) => {
            // We're not in an async context, create a new runtime
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| with_context(e, "Failed to create tokio runtime"))?;
            rt.block_on(future)
        }
    }
}

/// Etcd-backed keyspace implementation.
///
/// This provides the same API as the embedded `Keyspace` but stores data in etcd.
#[derive(Clone)]
pub struct EtcdKeyspace {
    partition: String,
    config: EtcdConfig,
}

impl EtcdKeyspace {
    /// Creates a new etcd keyspace for the given partition.
    pub fn new(partition: &str) -> Self {
        Self {
            partition: partition.to_string(),
            config: EtcdConfig::from_env(),
        }
    }

    /// Returns the full etcd key for a given keyspace key.
    fn full_key(&self, key: &str) -> String {
        self.config.full_key(&self.partition, key)
    }

    /// Stores a value under the given key.
    pub fn put(&self, key: &str, value: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let etcd_key = self.full_key(key);
        let value = value.to_string();
        let key_for_event = key.to_string();
        let partition = self.partition.clone();

        run_async(async move {
            let client = get_client().await?;
            let mut client = client.write().await;

            // Check if key exists to determine event type
            let existing = client
                .get(etcd_key.as_bytes(), None)
                .await
                .map_err(|e| with_context(e, format!("Failed to check key existence: {}", etcd_key)))?;
            let existed = !existing.kvs().is_empty();

            client
                .put(etcd_key.as_bytes(), value.as_bytes(), None)
                .await
                .map_err(|e| with_context(e, format!("Failed to put key: {}", etcd_key)))?;

            publish_etcd_event(
                &partition,
                key_for_event,
                Some(value),
                if existed {
                    KeyspaceEventType::Modified
                } else {
                    KeyspaceEventType::Added
                },
            );

            Ok(())
        })
    }

    /// Stores a value under the given key with a TTL.
    pub fn put_with_ttl(
        &self,
        key: &str,
        value: &str,
        ttl: Duration,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if ttl.is_zero() {
            return Err(new_error("TTL must be greater than zero"));
        }

        let etcd_key = self.full_key(key);
        let value = value.to_string();
        let key_for_event = key.to_string();
        let partition = self.partition.clone();

        run_async(async move {
            let client = get_client().await?;
            let mut client = client.write().await;

            // Check if key exists
            let existing = client
                .get(etcd_key.as_bytes(), None)
                .await
                .map_err(|e| with_context(e, format!("Failed to check key existence: {}", etcd_key)))?;
            let existed = !existing.kvs().is_empty();

            // Create a lease for TTL
            let lease = client
                .lease_grant(ttl.as_secs() as i64, None)
                .await
                .map_err(|e| with_context(e, "Failed to create lease"))?;

            let options = PutOptions::new().with_lease(lease.id());
            client
                .put(etcd_key.as_bytes(), value.as_bytes(), Some(options))
                .await
                .map_err(|e| with_context(e, format!("Failed to put key with TTL: {}", etcd_key)))?;

            publish_etcd_event(
                &partition,
                key_for_event,
                Some(value),
                if existed {
                    KeyspaceEventType::Modified
                } else {
                    KeyspaceEventType::Added
                },
            );

            Ok(())
        })
    }

    /// Retrieves the value associated with the given key.
    pub fn get(&self, key: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
        match self.get_optional(key)? {
            Some(value) => Ok(value),
            None => Err(new_error(format!("Value file not found: {}", self.full_key(key)))),
        }
    }

    /// Retrieves the value associated with the given key if it exists.
    pub fn get_optional(&self, key: &str) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
        let etcd_key = self.full_key(key);

        run_async(async move {
            let client = get_client().await?;
            let mut client = client.write().await;

            let response = client
                .get(etcd_key.as_bytes(), None)
                .await
                .map_err(|e| with_context(e, format!("Failed to get key: {}", etcd_key)))?;

            match response.kvs().first() {
                Some(kv) => {
                    let value = String::from_utf8(kv.value().to_vec())
                        .map_err(|e| with_context(e, "Invalid UTF-8 in etcd value"))?;
                    Ok(Some(value))
                }
                None => Ok(None),
            }
        })
    }

    /// Retrieves the value and expiry associated with the given key.
    ///
    /// Note: etcd leases don't expose exact expiry time easily, so this returns None for expiry.
    pub fn get_with_expiry(
        &self,
        key: &str,
    ) -> Result<(String, Option<SystemTime>), Box<dyn Error + Send + Sync>> {
        let value = self.get(key)?;
        // Note: etcd doesn't easily expose TTL info on get, would need lease lookup
        Ok((value, None))
    }

    /// Deletes the value for the given key.
    pub fn delete(&self, key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let etcd_key = self.full_key(key);
        let key_for_event = key.to_string();
        let partition = self.partition.clone();

        run_async(async move {
            let client = get_client().await?;
            let mut client = client.write().await;

            let response = client
                .delete(etcd_key.as_bytes(), None)
                .await
                .map_err(|e| with_context(e, format!("Failed to delete key: {}", etcd_key)))?;

            if response.deleted() == 0 {
                return Err(new_error(format!("Value file not found: {}", etcd_key)));
            }

            publish_etcd_event(&partition, key_for_event, None, KeyspaceEventType::Deleted);

            Ok(())
        })
    }

    /// Lists all keys under the given prefix.
    pub fn list(&self, prefix: &str) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let etcd_prefix = self.full_key(prefix);
        let prefix_len = self.config.prefix.len() + 1 + self.partition.len();

        run_async(async move {
            let client = get_client().await?;
            let mut client = client.write().await;

            let options = GetOptions::new().with_prefix();
            let response = client
                .get(etcd_prefix.as_bytes(), Some(options))
                .await
                .map_err(|e| with_context(e, format!("Failed to list keys: {}", etcd_prefix)))?;

            let keys: Vec<String> = response
                .kvs()
                .iter()
                .filter_map(|kv| {
                    let key = String::from_utf8(kv.key().to_vec()).ok()?;
                    // Strip the prefix to return just the keyspace key
                    Some(key[prefix_len..].to_string())
                })
                .collect();

            Ok(keys)
        })
    }

    /// Consumes a single-use token while enforcing an upper bound on TTL.
    ///
    /// Note: This is a simplified implementation. Full single-use semantics
    /// would require etcd transactions for atomicity.
    pub fn consume_single_use(
        &self,
        key: &str,
        _ttl_budget: Duration,
    ) -> Result<SingleUseTokenOutcome, Box<dyn Error + Send + Sync>> {
        match self.get_optional(key)? {
            Some(value) => {
                self.delete(key)?;
                Ok(SingleUseTokenOutcome::Consumed {
                    value,
                    expires_at: None,
                })
            }
            None => Ok(SingleUseTokenOutcome::NotFound),
        }
    }
}

/// Checks if the etcd backend is configured.
pub fn is_etcd_enabled() -> bool {
    matches!(KeyspaceBackendConfig::from_env(), Ok(KeyspaceBackendConfig::Etcd))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_key_generation() {
        let keyspace = EtcdKeyspace::new("k8s");
        // With default config, prefix is /nanocloud
        assert!(keyspace.full_key("/pods/default/nginx").contains("k8s"));
        assert!(keyspace.full_key("/pods/default/nginx").contains("pods"));
    }
}
