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

//! Metrics and observability hooks for secret operations.
//!
//! This module provides a pluggable metrics system for tracking secret store operations.
//! It emits counters and events for CRUD operations without exposing sensitive secret data.
//!
//! # Design Principles
//!
//! - **No sensitive data leakage**: Only namespace/name identifiers are exposed, never secret values
//! - **Pluggable backends**: Default no-op implementation can be replaced with custom backends
//! - **Minimal overhead**: When no metrics backend is configured, operations are essentially no-ops
//! - **Thread-safe**: All operations are safe to call from multiple threads
//!
//! # Usage
//!
//! ```ignore
//! use nanocloud::secrets::metrics::{SecretMetrics, set_metrics_backend};
//!
//! // Implement custom metrics backend
//! struct MyMetrics;
//! impl SecretMetrics for MyMetrics {
//!     fn record_get(&self, namespace: &str, name: &str, success: bool) {
//!         // Send to your metrics system
//!     }
//!     // ... implement other methods
//! }
//!
//! // Install the backend
//! set_metrics_backend(Box::new(MyMetrics));
//! ```

use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

/// Operation types for secret store metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretOperation {
    /// Get/read operation
    Get,
    /// Put/write operation
    Put,
    /// Delete operation
    Delete,
    /// List operation
    List,
    /// Key rotation operation
    Rotate,
}

impl SecretOperation {
    /// Returns the operation name as a string for logging/metrics labels.
    pub fn as_str(&self) -> &'static str {
        match self {
            SecretOperation::Get => "get",
            SecretOperation::Put => "put",
            SecretOperation::Delete => "delete",
            SecretOperation::List => "list",
            SecretOperation::Rotate => "rotate",
        }
    }
}

/// Error categories for metrics purposes.
///
/// These categories help classify errors without exposing internal details.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// Secret not found
    NotFound,
    /// Validation error (bad input)
    Validation,
    /// Integrity check failed (tampering detected)
    Integrity,
    /// Cryptographic operation failed
    Crypto,
    /// IO/storage error
    Io,
    /// Lock contention/timeout
    Lock,
    /// Other/unknown error
    Other,
}

impl ErrorCategory {
    /// Returns the category name as a string for logging/metrics labels.
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCategory::NotFound => "not_found",
            ErrorCategory::Validation => "validation",
            ErrorCategory::Integrity => "integrity",
            ErrorCategory::Crypto => "crypto",
            ErrorCategory::Io => "io",
            ErrorCategory::Lock => "lock",
            ErrorCategory::Other => "other",
        }
    }

    /// Infers the error category from an error message.
    ///
    /// This is a best-effort classification based on common error patterns.
    pub fn from_error_message(msg: &str) -> Self {
        let lower = msg.to_lowercase();
        if lower.contains("not found") {
            ErrorCategory::NotFound
        } else if lower.contains("validation") {
            ErrorCategory::Validation
        } else if lower.contains("integrity") || lower.contains("hmac") || lower.contains("tamper")
        {
            ErrorCategory::Integrity
        } else if lower.contains("crypto") || lower.contains("encrypt") || lower.contains("decrypt")
        {
            ErrorCategory::Crypto
        } else if lower.contains("io") || lower.contains("file") || lower.contains("keyspace") {
            ErrorCategory::Io
        } else if lower.contains("lock") || lower.contains("timeout") {
            ErrorCategory::Lock
        } else {
            ErrorCategory::Other
        }
    }
}

/// Trait for implementing custom metrics backends.
///
/// All methods have default no-op implementations, so you only need to implement
/// the ones you care about.
///
/// # Thread Safety
///
/// Implementations must be thread-safe (`Send + Sync`) as metrics may be recorded
/// from multiple threads concurrently.
///
/// # Sensitive Data
///
/// Implementations should never log or store the actual secret values.
/// Only namespace and name identifiers should be used for labeling.
pub trait SecretMetrics: Send + Sync {
    /// Records a successful operation.
    ///
    /// # Arguments
    ///
    /// * `operation` - The type of operation performed
    /// * `namespace` - The secret's namespace
    /// * `name` - The secret's name (may be None for list operations)
    /// * `duration` - How long the operation took
    fn record_success(
        &self,
        _operation: SecretOperation,
        _namespace: &str,
        _name: Option<&str>,
        _duration: Duration,
    ) {
        // Default no-op
    }

    /// Records a failed operation.
    ///
    /// # Arguments
    ///
    /// * `operation` - The type of operation that failed
    /// * `namespace` - The secret's namespace
    /// * `name` - The secret's name (may be None for list operations)
    /// * `category` - The category of error
    /// * `duration` - How long the operation took before failing
    fn record_failure(
        &self,
        _operation: SecretOperation,
        _namespace: &str,
        _name: Option<&str>,
        _category: ErrorCategory,
        _duration: Duration,
    ) {
        // Default no-op
    }

    /// Records an integrity failure (potential tampering detected).
    ///
    /// This is separate from `record_failure` to allow for special alerting
    /// on security-relevant events.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The secret's namespace
    /// * `name` - The secret's name
    fn record_integrity_failure(&self, _namespace: &str, _name: &str) {
        // Default no-op
    }

    /// Records a cache hit (if caching is enabled).
    fn record_cache_hit(&self, _namespace: &str, _name: &str) {
        // Default no-op
    }

    /// Records a cache miss (if caching is enabled).
    fn record_cache_miss(&self, _namespace: &str, _name: &str) {
        // Default no-op
    }
}

/// Default no-op metrics implementation.
struct NoOpMetrics;

impl SecretMetrics for NoOpMetrics {}

/// Global metrics backend storage.
static METRICS_BACKEND: OnceLock<RwLock<Arc<dyn SecretMetrics>>> = OnceLock::new();

fn get_metrics_lock() -> &'static RwLock<Arc<dyn SecretMetrics>> {
    METRICS_BACKEND.get_or_init(|| RwLock::new(Arc::new(NoOpMetrics)))
}

/// Sets the global metrics backend.
///
/// This can be called at any time to replace the metrics backend.
/// The new backend will be used for all subsequent operations.
///
/// # Example
///
/// ```ignore
/// use nanocloud::secrets::metrics::{SecretMetrics, set_metrics_backend};
///
/// struct PrometheusMetrics;
/// impl SecretMetrics for PrometheusMetrics {
///     // ... implementation
/// }
///
/// set_metrics_backend(Box::new(PrometheusMetrics));
/// ```
pub fn set_metrics_backend(backend: Box<dyn SecretMetrics>) {
    let lock = get_metrics_lock();
    let mut guard = lock.write().expect("metrics backend lock poisoned");
    *guard = Arc::from(backend);
}

/// Gets the current metrics backend.
fn get_metrics() -> Arc<dyn SecretMetrics> {
    let lock = get_metrics_lock();
    let guard = lock.read().expect("metrics backend lock poisoned");
    Arc::clone(&guard)
}

/// Records a successful secret operation.
pub fn record_success(
    operation: SecretOperation,
    namespace: &str,
    name: Option<&str>,
    duration: Duration,
) {
    get_metrics().record_success(operation, namespace, name, duration);
}

/// Records a failed secret operation.
pub fn record_failure(
    operation: SecretOperation,
    namespace: &str,
    name: Option<&str>,
    category: ErrorCategory,
    duration: Duration,
) {
    get_metrics().record_failure(operation, namespace, name, category, duration);
}

/// Records an integrity failure event.
pub fn record_integrity_failure(namespace: &str, name: &str) {
    get_metrics().record_integrity_failure(namespace, name);
}

/// Records a cache hit.
pub fn record_cache_hit(namespace: &str, name: &str) {
    get_metrics().record_cache_hit(namespace, name);
}

/// Records a cache miss.
pub fn record_cache_miss(namespace: &str, name: &str) {
    get_metrics().record_cache_miss(namespace, name);
}

/// RAII guard for timing operations.
///
/// Records the duration when dropped.
pub struct OperationTimer {
    operation: SecretOperation,
    namespace: String,
    name: Option<String>,
    start: std::time::Instant,
    recorded: bool,
}

impl OperationTimer {
    /// Creates a new operation timer.
    pub fn new(operation: SecretOperation, namespace: &str, name: Option<&str>) -> Self {
        Self {
            operation,
            namespace: namespace.to_string(),
            name: name.map(|s| s.to_string()),
            start: std::time::Instant::now(),
            recorded: false,
        }
    }

    /// Records success and consumes the timer.
    pub fn success(mut self) {
        self.recorded = true;
        record_success(
            self.operation,
            &self.namespace,
            self.name.as_deref(),
            self.start.elapsed(),
        );
    }

    /// Records failure and consumes the timer.
    pub fn failure(mut self, category: ErrorCategory) {
        self.recorded = true;
        record_failure(
            self.operation,
            &self.namespace,
            self.name.as_deref(),
            category,
            self.start.elapsed(),
        );
    }

    /// Records failure with automatic category inference.
    pub fn failure_from_error(mut self, error: &dyn std::error::Error) {
        self.recorded = true;
        let category = ErrorCategory::from_error_message(&error.to_string());
        record_failure(
            self.operation,
            &self.namespace,
            self.name.as_deref(),
            category,
            self.start.elapsed(),
        );
    }
}

impl Drop for OperationTimer {
    fn drop(&mut self) {
        if !self.recorded {
            // If not explicitly recorded, record as unknown failure
            record_failure(
                self.operation,
                &self.namespace,
                self.name.as_deref(),
                ErrorCategory::Other,
                self.start.elapsed(),
            );
        }
    }
}

/// A logging metrics backend that writes to the nanocloud logger.
///
/// This is a simple implementation that logs all metrics events.
/// Useful for debugging or when a full metrics system isn't available.
pub struct LoggingMetrics {
    component: &'static str,
}

impl LoggingMetrics {
    /// Creates a new logging metrics backend.
    pub fn new(component: &'static str) -> Self {
        Self { component }
    }
}

impl SecretMetrics for LoggingMetrics {
    fn record_success(
        &self,
        operation: SecretOperation,
        namespace: &str,
        name: Option<&str>,
        duration: Duration,
    ) {
        use crate::nanocloud::logger::log_info;

        let name_str = name.unwrap_or("-");
        log_info(
            self.component,
            &format!("Secret operation succeeded: {}", operation.as_str()),
            &[
                ("namespace", namespace),
                ("name", name_str),
                (
                    "duration_ms",
                    &format!("{:.2}", duration.as_secs_f64() * 1000.0),
                ),
            ],
        );
    }

    fn record_failure(
        &self,
        operation: SecretOperation,
        namespace: &str,
        name: Option<&str>,
        category: ErrorCategory,
        duration: Duration,
    ) {
        use crate::nanocloud::logger::log_warn;

        let name_str = name.unwrap_or("-");
        log_warn(
            self.component,
            &format!("Secret operation failed: {}", operation.as_str()),
            &[
                ("namespace", namespace),
                ("name", name_str),
                ("error_category", category.as_str()),
                (
                    "duration_ms",
                    &format!("{:.2}", duration.as_secs_f64() * 1000.0),
                ),
            ],
        );
    }

    fn record_integrity_failure(&self, namespace: &str, name: &str) {
        use crate::nanocloud::logger::log_error;

        log_error(
            self.component,
            "SECURITY: Secret integrity check failed - possible tampering detected",
            &[("namespace", namespace), ("name", name)],
        );
    }

    fn record_cache_hit(&self, namespace: &str, name: &str) {
        use crate::nanocloud::logger::log_debug;

        log_debug(
            self.component,
            "Secret cache hit",
            &[("namespace", namespace), ("name", name)],
        );
    }

    fn record_cache_miss(&self, namespace: &str, name: &str) {
        use crate::nanocloud::logger::log_debug;

        log_debug(
            self.component,
            "Secret cache miss",
            &[("namespace", namespace), ("name", name)],
        );
    }
}

/// A counting metrics backend for testing.
///
/// Tracks counts of each operation type for verification in tests.
#[cfg(test)]
pub struct CountingMetrics {
    pub success_count: std::sync::atomic::AtomicUsize,
    pub failure_count: std::sync::atomic::AtomicUsize,
    pub integrity_failure_count: std::sync::atomic::AtomicUsize,
    pub cache_hit_count: std::sync::atomic::AtomicUsize,
    pub cache_miss_count: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl Default for CountingMetrics {
    fn default() -> Self {
        Self {
            success_count: std::sync::atomic::AtomicUsize::new(0),
            failure_count: std::sync::atomic::AtomicUsize::new(0),
            integrity_failure_count: std::sync::atomic::AtomicUsize::new(0),
            cache_hit_count: std::sync::atomic::AtomicUsize::new(0),
            cache_miss_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

#[cfg(test)]
impl CountingMetrics {
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(test)]
impl SecretMetrics for CountingMetrics {
    fn record_success(
        &self,
        _operation: SecretOperation,
        _namespace: &str,
        _name: Option<&str>,
        _duration: Duration,
    ) {
        self.success_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn record_failure(
        &self,
        _operation: SecretOperation,
        _namespace: &str,
        _name: Option<&str>,
        _category: ErrorCategory,
        _duration: Duration,
    ) {
        self.failure_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn record_integrity_failure(&self, _namespace: &str, _name: &str) {
        self.integrity_failure_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn record_cache_hit(&self, _namespace: &str, _name: &str) {
        self.cache_hit_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn record_cache_miss(&self, _namespace: &str, _name: &str) {
        self.cache_miss_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_names() {
        assert_eq!(SecretOperation::Get.as_str(), "get");
        assert_eq!(SecretOperation::Put.as_str(), "put");
        assert_eq!(SecretOperation::Delete.as_str(), "delete");
        assert_eq!(SecretOperation::List.as_str(), "list");
        assert_eq!(SecretOperation::Rotate.as_str(), "rotate");
    }

    #[test]
    fn error_category_names() {
        assert_eq!(ErrorCategory::NotFound.as_str(), "not_found");
        assert_eq!(ErrorCategory::Validation.as_str(), "validation");
        assert_eq!(ErrorCategory::Integrity.as_str(), "integrity");
        assert_eq!(ErrorCategory::Crypto.as_str(), "crypto");
        assert_eq!(ErrorCategory::Io.as_str(), "io");
        assert_eq!(ErrorCategory::Lock.as_str(), "lock");
        assert_eq!(ErrorCategory::Other.as_str(), "other");
    }

    #[test]
    fn error_category_inference() {
        assert_eq!(
            ErrorCategory::from_error_message("Secret not found"),
            ErrorCategory::NotFound
        );
        assert_eq!(
            ErrorCategory::from_error_message("Validation error: empty namespace"),
            ErrorCategory::Validation
        );
        assert_eq!(
            ErrorCategory::from_error_message("Integrity check failed: HMAC mismatch"),
            ErrorCategory::Integrity
        );
        assert_eq!(
            ErrorCategory::from_error_message("Crypto operation failed"),
            ErrorCategory::Crypto
        );
        assert_eq!(
            ErrorCategory::from_error_message("IO error reading file"),
            ErrorCategory::Io
        );
        assert_eq!(
            ErrorCategory::from_error_message("Lock timeout"),
            ErrorCategory::Lock
        );
        assert_eq!(
            ErrorCategory::from_error_message("Unknown error"),
            ErrorCategory::Other
        );
    }

    #[test]
    fn counting_metrics() {
        let metrics = CountingMetrics::new();

        metrics.record_success(
            SecretOperation::Get,
            "default",
            Some("test"),
            Duration::from_millis(10),
        );
        metrics.record_success(
            SecretOperation::Put,
            "default",
            Some("test"),
            Duration::from_millis(20),
        );
        metrics.record_failure(
            SecretOperation::Get,
            "default",
            Some("missing"),
            ErrorCategory::NotFound,
            Duration::from_millis(5),
        );
        metrics.record_integrity_failure("default", "tampered");
        metrics.record_cache_hit("default", "cached");
        metrics.record_cache_miss("default", "not-cached");

        assert_eq!(
            metrics
                .success_count
                .load(std::sync::atomic::Ordering::SeqCst),
            2
        );
        assert_eq!(
            metrics
                .failure_count
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            metrics
                .integrity_failure_count
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            metrics
                .cache_hit_count
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            metrics
                .cache_miss_count
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    #[test]
    fn operation_timer_success() {
        let timer = OperationTimer::new(SecretOperation::Get, "default", Some("test"));
        std::thread::sleep(Duration::from_millis(1));
        timer.success(); // Should not panic
    }

    #[test]
    fn operation_timer_failure() {
        let timer = OperationTimer::new(SecretOperation::Put, "default", Some("test"));
        timer.failure(ErrorCategory::Validation); // Should not panic
    }

    #[test]
    fn no_op_metrics_does_not_panic() {
        let metrics = NoOpMetrics;

        // All of these should be no-ops and not panic
        metrics.record_success(
            SecretOperation::Get,
            "default",
            Some("test"),
            Duration::from_millis(10),
        );
        metrics.record_failure(
            SecretOperation::Get,
            "default",
            Some("test"),
            ErrorCategory::NotFound,
            Duration::from_millis(5),
        );
        metrics.record_integrity_failure("default", "test");
        metrics.record_cache_hit("default", "test");
        metrics.record_cache_miss("default", "test");
    }
}
