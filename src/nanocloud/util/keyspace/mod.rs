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

//! Pluggable key-value storage for Nanocloud state.
//!
//! This module provides a unified interface for storing small configuration blobs,
//! bootstrap tokens, and Kubernetes resource state. The storage backend can be
//! configured via the `NANOCLOUD_KEYSPACE_BACKEND` environment variable:
//!
//! - `embedded` (default): Filesystem-backed storage under `/var/lib/nanocloud.io/keyspace`
//! - `etcd`: External etcd cluster (requires `etcd` feature)
//!
//! # Backend Configuration
//!
//! ## Embedded Backend (default)
//!
//! The embedded backend stores data on the local filesystem with atomic write
//! semantics. Configuration:
//!
//! - `NANOCLOUD_KEYSPACE`: Base directory (default: `/var/lib/nanocloud.io/keyspace`)
//! - `NANOCLOUD_KEYSPACE_PER_KEY_LOCKS`: Enable per-key locking (`1` to enable)
//! - `NANOCLOUD_KEYSPACE_LOCK_TIMEOUT_SECS`: Lock timeout in seconds (default: 10)
//!
//! ## Etcd Backend
//!
//! When `NANOCLOUD_KEYSPACE_BACKEND=etcd`, state is stored in an external etcd cluster:
//!
//! - `NANOCLOUD_ETCD_ENDPOINTS`: Comma-separated etcd URLs (default: `http://127.0.0.1:2379`)
//! - `NANOCLOUD_ETCD_PREFIX`: Key prefix (default: `/nanocloud`)
//! - `NANOCLOUD_ETCD_USERNAME`: Optional username for authentication
//! - `NANOCLOUD_ETCD_PASSWORD`: Optional password for authentication
//!
//! # Usage
//!
//! The public API is the same regardless of backend:
//!
//! ```no_run
//! use nanocloud::nanocloud::util::Keyspace;
//!
//! let keyspace = Keyspace::new("k8s");
//! keyspace.put("/pods/default/nginx", "{...}")?;
//! let value = keyspace.get("/pods/default/nginx")?;
//! # Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
//! ```

mod embedded;

#[cfg(feature = "etcd")]
mod etcd;

// Re-export the public API from embedded (which is the default and always available)
pub use embedded::{
    is_missing_value_error, Keyspace, KeyspaceEvent, KeyspaceEventType, SingleUseTokenOutcome,
};

// Re-export crate-internal items
pub(crate) use embedded::reset_partition_watch;

// When etcd feature is enabled, re-export the etcd backend
#[cfg(feature = "etcd")]
#[allow(unused_imports)]
pub use etcd::EtcdKeyspace;

// =============================================================================
// Backend Selection Helper
// =============================================================================

use crate::nanocloud::config::KeyspaceBackend as BackendType;
use std::sync::OnceLock;

/// Cached backend type, determined once at startup from environment.
#[allow(dead_code)]
static BACKEND_TYPE: OnceLock<BackendType> = OnceLock::new();

/// Returns the configured keyspace backend type.
///
/// This is determined once from `NANOCLOUD_KEYSPACE_BACKEND` and cached for
/// the lifetime of the process.
///
/// # Usage
///
/// Callers can use this to dispatch to different backends at runtime:
///
/// ```ignore
/// use nanocloud::nanocloud::util::keyspace::{backend_type, Keyspace};
/// use nanocloud::nanocloud::config::KeyspaceBackend;
///
/// match backend_type() {
///     KeyspaceBackend::Embedded => {
///         let ks = Keyspace::new("k8s");
///         ks.put("/key", "value")?;
///     }
///     #[cfg(feature = "etcd")]
///     KeyspaceBackend::Etcd => {
///         let ks = EtcdKeyspace::new("k8s");
///         ks.put("/key", "value")?;
///     }
/// }
/// ```
#[allow(dead_code)]
pub fn backend_type() -> BackendType {
    *BACKEND_TYPE.get_or_init(|| BackendType::from_env().unwrap_or_default())
}

/// Returns true if the etcd backend is configured and available.
#[cfg(feature = "etcd")]
#[allow(dead_code)]
pub fn is_etcd_backend() -> bool {
    matches!(backend_type(), BackendType::Etcd)
}

/// Returns true if the etcd backend is configured and available.
#[cfg(not(feature = "etcd"))]
#[allow(dead_code)]
pub fn is_etcd_backend() -> bool {
    false
}
