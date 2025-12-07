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

//! Optional in-memory cache for decrypted secrets with TTL.
//!
//! This module provides a thread-safe cache for decrypted secrets to reduce
//! the overhead of repeated decryption operations. Each cached entry has a
//! configurable time-to-live (TTL) after which it expires.
//!
//! # Security Considerations
//!
//! - **Memory Exposure**: Cached secrets are held in plaintext in memory.
//!   This trades security for performance. Do not use caching if your
//!   threat model includes memory-scraping attacks.
//! - **TTL**: Short TTLs (seconds to minutes) reduce exposure window.
//!   The default is 60 seconds.
//! - **Capacity Limits**: The cache has a configurable maximum size to
//!   prevent unbounded memory growth.
//! - **No Persistence**: The cache is in-memory only and is cleared on restart.
//!
//! # Usage
//!
//! ```ignore
//! use nanocloud::secrets::cache::{SecretCache, CacheConfig};
//! use std::time::Duration;
//!
//! // Create a cache with custom configuration
//! let config = CacheConfig {
//!     ttl: Duration::from_secs(120),
//!     max_entries: 500,
//!     enabled: true,
//! };
//! let cache = SecretCache::with_config(config);
//!
//! // Or use the default cache (60s TTL, 1000 entries)
//! let cache = SecretCache::default();
//!
//! // Cache a secret
//! cache.put("default", "my-secret", stored_secret.clone());
//!
//! // Retrieve from cache
//! if let Some(secret) = cache.get("default", "my-secret") {
//!     // Use cached secret
//! }
//!
//! // Invalidate on update
//! cache.invalidate("default", "my-secret");
//! ```

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::nanocloud::secrets::metrics;
use crate::nanocloud::secrets::StoredSecret;

/// Default TTL for cached secrets (60 seconds).
pub const DEFAULT_TTL: Duration = Duration::from_secs(60);

/// Default maximum number of cached entries.
pub const DEFAULT_MAX_ENTRIES: usize = 1000;

/// Configuration for the secret cache.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Time-to-live for cached entries.
    pub ttl: Duration,
    /// Maximum number of entries in the cache.
    /// When exceeded, oldest entries are evicted.
    pub max_entries: usize,
    /// Whether caching is enabled.
    pub enabled: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_TTL,
            max_entries: DEFAULT_MAX_ENTRIES,
            enabled: true,
        }
    }
}

impl CacheConfig {
    /// Creates a disabled cache configuration.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Creates a configuration with the specified TTL.
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl,
            ..Default::default()
        }
    }
}

/// A cached secret entry with expiration tracking.
#[derive(Clone)]
struct CacheEntry {
    secret: StoredSecret,
    inserted_at: Instant,
    ttl: Duration,
}

impl CacheEntry {
    fn new(secret: StoredSecret, ttl: Duration) -> Self {
        Self {
            secret,
            inserted_at: Instant::now(),
            ttl,
        }
    }

    fn is_expired(&self) -> bool {
        self.inserted_at.elapsed() >= self.ttl
    }
}

/// Thread-safe in-memory cache for decrypted secrets.
pub struct SecretCache {
    config: CacheConfig,
    entries: RwLock<HashMap<String, CacheEntry>>,
}

impl SecretCache {
    /// Creates a new cache with the given configuration.
    pub fn with_config(config: CacheConfig) -> Self {
        Self {
            config,
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Creates a new cache with default configuration.
    pub fn new() -> Self {
        Self::with_config(CacheConfig::default())
    }

    /// Creates a disabled cache (always misses).
    pub fn disabled() -> Self {
        Self::with_config(CacheConfig::disabled())
    }

    /// Returns whether the cache is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Returns the current TTL configuration.
    pub fn ttl(&self) -> Duration {
        self.config.ttl
    }

    /// Returns the maximum number of entries.
    pub fn max_entries(&self) -> usize {
        self.config.max_entries
    }

    /// Generates a cache key from namespace and name.
    fn cache_key(namespace: &str, name: &str) -> String {
        format!("{}/{}", namespace, name)
    }

    /// Retrieves a secret from the cache if present and not expired.
    ///
    /// Returns `None` if the cache is disabled, the entry doesn't exist,
    /// or the entry has expired.
    pub fn get(&self, namespace: &str, name: &str) -> Option<StoredSecret> {
        if !self.config.enabled {
            return None;
        }

        let key = Self::cache_key(namespace, name);

        // Try read lock first for common case (cache hit or miss without expiry)
        let entries = self.entries.read().ok()?;

        if let Some(entry) = entries.get(&key) {
            if !entry.is_expired() {
                metrics::record_cache_hit(namespace, name);
                return Some(entry.secret.clone());
            }
        }

        // Entry expired or not found
        drop(entries);
        metrics::record_cache_miss(namespace, name);

        // If expired, schedule cleanup (but don't block the get)
        // The next put or explicit cleanup will remove it
        None
    }

    /// Stores a secret in the cache.
    ///
    /// If the cache is at capacity, the oldest entries are evicted.
    /// Does nothing if the cache is disabled.
    pub fn put(&self, namespace: &str, name: &str, secret: StoredSecret) {
        if !self.config.enabled {
            return;
        }

        let key = Self::cache_key(namespace, name);
        let entry = CacheEntry::new(secret, self.config.ttl);

        let mut entries = match self.entries.write() {
            Ok(e) => e,
            Err(_) => return, // Lock poisoned, skip caching
        };

        // Evict expired entries and oldest if at capacity
        if entries.len() >= self.config.max_entries {
            self.evict_expired_and_oldest(&mut entries);
        }

        entries.insert(key, entry);
    }

    /// Invalidates a cached secret.
    ///
    /// Call this when a secret is updated or deleted to ensure
    /// the cache doesn't serve stale data.
    pub fn invalidate(&self, namespace: &str, name: &str) {
        if !self.config.enabled {
            return;
        }

        let key = Self::cache_key(namespace, name);

        if let Ok(mut entries) = self.entries.write() {
            entries.remove(&key);
        }
    }

    /// Invalidates all cached secrets in a namespace.
    pub fn invalidate_namespace(&self, namespace: &str) {
        if !self.config.enabled {
            return;
        }

        let prefix = format!("{}/", namespace);

        if let Ok(mut entries) = self.entries.write() {
            entries.retain(|k, _| !k.starts_with(&prefix));
        }
    }

    /// Clears all cached secrets.
    pub fn clear(&self) {
        if let Ok(mut entries) = self.entries.write() {
            entries.clear();
        }
    }

    /// Returns the number of entries currently in the cache.
    ///
    /// Note: This may include expired entries that haven't been evicted yet.
    pub fn len(&self) -> usize {
        self.entries.read().map(|e| e.len()).unwrap_or(0)
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Removes all expired entries from the cache.
    pub fn cleanup_expired(&self) {
        if let Ok(mut entries) = self.entries.write() {
            entries.retain(|_, entry| !entry.is_expired());
        }
    }

    /// Evicts expired entries and, if still over capacity, the oldest entries.
    fn evict_expired_and_oldest(&self, entries: &mut HashMap<String, CacheEntry>) {
        // First, remove all expired entries
        entries.retain(|_, entry| !entry.is_expired());

        // If still over capacity, remove oldest entries
        while entries.len() >= self.config.max_entries {
            if let Some(oldest_key) = entries
                .iter()
                .min_by_key(|(_, entry)| entry.inserted_at)
                .map(|(k, _)| k.clone())
            {
                entries.remove(&oldest_key);
            } else {
                break;
            }
        }
    }

    /// Returns cache statistics.
    pub fn stats(&self) -> CacheStats {
        let entries = self.entries.read().ok();
        let (total, expired) = entries
            .as_ref()
            .map(|e| {
                let total = e.len();
                let expired = e.values().filter(|entry| entry.is_expired()).count();
                (total, expired)
            })
            .unwrap_or((0, 0));

        CacheStats {
            enabled: self.config.enabled,
            total_entries: total,
            expired_entries: expired,
            active_entries: total.saturating_sub(expired),
            max_entries: self.config.max_entries,
            ttl: self.config.ttl,
        }
    }
}

impl Default for SecretCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Whether the cache is enabled.
    pub enabled: bool,
    /// Total number of entries (including expired).
    pub total_entries: usize,
    /// Number of expired entries.
    pub expired_entries: usize,
    /// Number of active (non-expired) entries.
    pub active_entries: usize,
    /// Maximum number of entries allowed.
    pub max_entries: usize,
    /// Time-to-live for entries.
    pub ttl: Duration,
}

/// A cached secret store that wraps a store with optional caching.
pub struct CachedSecretStore {
    store: Arc<crate::nanocloud::secrets::KeyspaceSecretStore>,
    cache: SecretCache,
}

impl CachedSecretStore {
    /// Creates a new cached store wrapping the given store.
    pub fn new(store: Arc<crate::nanocloud::secrets::KeyspaceSecretStore>) -> Self {
        Self {
            store,
            cache: SecretCache::default(),
        }
    }

    /// Creates a new cached store with custom cache configuration.
    pub fn with_config(
        store: Arc<crate::nanocloud::secrets::KeyspaceSecretStore>,
        config: CacheConfig,
    ) -> Self {
        Self {
            store,
            cache: SecretCache::with_config(config),
        }
    }

    /// Gets a secret, using the cache if available.
    pub fn get(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<StoredSecret>, Box<dyn std::error::Error + Send + Sync>> {
        // Try cache first
        if let Some(cached) = self.cache.get(namespace, name) {
            return Ok(Some(cached));
        }

        // Cache miss - fetch from store
        let result = self.store.get(namespace, name)?;

        // Cache the result if found
        if let Some(ref secret) = result {
            self.cache.put(namespace, name, secret.clone());
        }

        Ok(result)
    }

    /// Puts a secret, invalidating the cache entry.
    pub fn put(
        &self,
        secret: crate::nanocloud::secrets::SecretMaterial,
    ) -> Result<StoredSecret, Box<dyn std::error::Error + Send + Sync>> {
        let namespace = secret.namespace.clone();
        let name = secret.name.clone();

        // Invalidate cache before write
        self.cache.invalidate(&namespace, &name);

        // Write to store
        let result = self.store.put(secret)?;

        // Cache the new value
        self.cache.put(&namespace, &name, result.clone());

        Ok(result)
    }

    /// Deletes a secret, invalidating the cache entry.
    pub fn delete(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Invalidate cache
        self.cache.invalidate(namespace, name);

        // Delete from store
        self.store.delete(namespace, name)
    }

    /// Returns cache statistics.
    pub fn cache_stats(&self) -> CacheStats {
        self.cache.stats()
    }

    /// Clears the cache.
    pub fn clear_cache(&self) {
        self.cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::thread;

    fn sample_stored_secret(namespace: &str, name: &str) -> StoredSecret {
        let mut data = BTreeMap::new();
        data.insert("key".to_string(), format!("value-{}", name));

        StoredSecret {
            secret: crate::nanocloud::secrets::SecretMaterial {
                namespace: namespace.to_string(),
                name: name.to_string(),
                type_name: "Opaque".to_string(),
                immutable: false,
                data,
                resource_version: None,
            },
            digest: "abc123".to_string(),
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn cache_hit_and_miss() {
        let cache = SecretCache::new();

        // Miss
        assert!(cache.get("default", "secret1").is_none());

        // Put
        let secret = sample_stored_secret("default", "secret1");
        cache.put("default", "secret1", secret.clone());

        // Hit
        let cached = cache.get("default", "secret1");
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().secret.name, "secret1");
    }

    #[test]
    fn cache_invalidation() {
        let cache = SecretCache::new();

        let secret = sample_stored_secret("default", "secret1");
        cache.put("default", "secret1", secret);

        // Verify cached
        assert!(cache.get("default", "secret1").is_some());

        // Invalidate
        cache.invalidate("default", "secret1");

        // Now miss
        assert!(cache.get("default", "secret1").is_none());
    }

    #[test]
    fn cache_namespace_invalidation() {
        let cache = SecretCache::new();

        cache.put("ns1", "secret1", sample_stored_secret("ns1", "secret1"));
        cache.put("ns1", "secret2", sample_stored_secret("ns1", "secret2"));
        cache.put("ns2", "secret1", sample_stored_secret("ns2", "secret1"));

        assert_eq!(cache.len(), 3);

        // Invalidate ns1
        cache.invalidate_namespace("ns1");

        // ns1 secrets gone
        assert!(cache.get("ns1", "secret1").is_none());
        assert!(cache.get("ns1", "secret2").is_none());

        // ns2 still there
        assert!(cache.get("ns2", "secret1").is_some());
    }

    #[test]
    fn cache_expiration() {
        let config = CacheConfig {
            ttl: Duration::from_millis(50),
            max_entries: 100,
            enabled: true,
        };
        let cache = SecretCache::with_config(config);

        let secret = sample_stored_secret("default", "expiring");
        cache.put("default", "expiring", secret);

        // Should be cached
        assert!(cache.get("default", "expiring").is_some());

        // Wait for expiration
        thread::sleep(Duration::from_millis(60));

        // Should be expired
        assert!(cache.get("default", "expiring").is_none());
    }

    #[test]
    fn cache_disabled() {
        let cache = SecretCache::disabled();

        let secret = sample_stored_secret("default", "secret1");
        cache.put("default", "secret1", secret);

        // Should not cache
        assert!(cache.get("default", "secret1").is_none());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn cache_capacity_eviction() {
        let config = CacheConfig {
            ttl: Duration::from_secs(60),
            max_entries: 3,
            enabled: true,
        };
        let cache = SecretCache::with_config(config);

        // Add 3 entries
        for i in 0..3 {
            let name = format!("secret{}", i);
            cache.put("default", &name, sample_stored_secret("default", &name));
            thread::sleep(Duration::from_millis(5)); // Ensure different timestamps
        }

        assert_eq!(cache.len(), 3);

        // Add 4th entry - should evict oldest
        cache.put("default", "secret3", sample_stored_secret("default", "secret3"));

        // Should still have 3 entries
        assert!(cache.len() <= 3);

        // Newest should be present
        assert!(cache.get("default", "secret3").is_some());
    }

    #[test]
    fn cache_clear() {
        let cache = SecretCache::new();

        for i in 0..5 {
            let name = format!("secret{}", i);
            cache.put("default", &name, sample_stored_secret("default", &name));
        }

        assert_eq!(cache.len(), 5);

        cache.clear();

        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn cache_stats() {
        let cache = SecretCache::with_config(CacheConfig {
            ttl: Duration::from_millis(50),
            max_entries: 100,
            enabled: true,
        });

        cache.put("default", "secret1", sample_stored_secret("default", "secret1"));
        cache.put("default", "secret2", sample_stored_secret("default", "secret2"));

        let stats = cache.stats();
        assert!(stats.enabled);
        assert_eq!(stats.total_entries, 2);
        assert_eq!(stats.expired_entries, 0);
        assert_eq!(stats.active_entries, 2);

        // Wait for expiration
        thread::sleep(Duration::from_millis(60));

        let stats = cache.stats();
        assert_eq!(stats.total_entries, 2);
        assert_eq!(stats.expired_entries, 2);
        assert_eq!(stats.active_entries, 0);
    }

    #[test]
    fn cache_cleanup_expired() {
        let cache = SecretCache::with_config(CacheConfig {
            ttl: Duration::from_millis(50),
            max_entries: 100,
            enabled: true,
        });

        cache.put("default", "secret1", sample_stored_secret("default", "secret1"));
        cache.put("default", "secret2", sample_stored_secret("default", "secret2"));

        assert_eq!(cache.len(), 2);

        // Wait for expiration
        thread::sleep(Duration::from_millis(60));

        // Cleanup
        cache.cleanup_expired();

        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn cache_thread_safety() {
        use std::sync::Arc;

        let cache = Arc::new(SecretCache::new());
        let mut handles = vec![];

        // Spawn multiple reader/writer threads
        for i in 0..10 {
            let cache = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                let name = format!("secret{}", i % 3);

                // Mix of reads and writes
                for _ in 0..100 {
                    if i % 2 == 0 {
                        cache.put("default", &name, sample_stored_secret("default", &name));
                    } else {
                        let _ = cache.get("default", &name);
                    }
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("thread should complete");
        }

        // Cache should still be functional
        assert!(cache.len() <= 3);
    }

    #[test]
    fn config_defaults() {
        let config = CacheConfig::default();
        assert_eq!(config.ttl, DEFAULT_TTL);
        assert_eq!(config.max_entries, DEFAULT_MAX_ENTRIES);
        assert!(config.enabled);
    }

    #[test]
    fn config_disabled() {
        let config = CacheConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn config_with_ttl() {
        let config = CacheConfig::with_ttl(Duration::from_secs(300));
        assert_eq!(config.ttl, Duration::from_secs(300));
        assert!(config.enabled);
    }
}
