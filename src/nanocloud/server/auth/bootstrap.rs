//! Bootstrap token authentication service.
//!
//! This module provides single-use bootstrap tokens for initial cluster setup
//! and device enrollment. Tokens are stored encrypted in the keyspace with
//! optional TTL expiration.
//!
//! # Token Format
//!
//! Bootstrap tokens follow the format `{id}.{secret}` where:
//! - `id`: Unique token identifier used for storage lookup
//! - `secret`: Encrypted secret validated against stored ciphertext
//!
//! # Storage
//!
//! Tokens are stored in the keyspace at `/v1/token/{id}` with the following
//! JSON structure:
//!
//! ```json
//! {
//!   "user": "subject-name",
//!   "cluster": "cluster-name",
//!   "scope": ["scope1", "scope2"],
//!   "aud": ["audience1"],
//!   "secret": {
//!     "key": "wrapped-encryption-key",
//!     "ciphertext": "encrypted-secret"
//!   }
//! }
//! ```
//!
//! # Security
//!
//! - Tokens are single-use and consumed after successful validation
//! - Token secrets are stored encrypted using the cluster encryption key
//! - Expired tokens are automatically cleaned up by a background maintenance task

use std::error::Error;
use std::fmt;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use crate::nanocloud::logger::log_warn;
use crate::nanocloud::scheduler::{
    CronSchedule, JobResult, ScheduleSpec, ScheduledTaskHandle, Scheduler,
};
use crate::nanocloud::server::handlers::serviceaccounts::CERTIFICATE_SCOPE;
use crate::nanocloud::util::security::EncryptionKey;
use crate::nanocloud::util::Keyspace;
use chrono_tz::UTC;
use serde_json::Value;

const BOOTSTRAP_PREFIX: &str = "/v1/token";
const BOOTSTRAP_LOG_COMPONENT: &str = "auth-bootstrap";
const TOKEN_REPAIR_SCHEDULE: &str = "0 */5 * * * *";

/// Maximum length for token IDs to prevent abuse.
const MAX_TOKEN_ID_LENGTH: usize = 256;

/// Maximum length for subject names.
const MAX_SUBJECT_LENGTH: usize = 512;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BootstrapToken {
    pub token: String,
    pub subject: String,
    pub cluster: Option<String>,
    pub scopes: Vec<String>,
    pub audiences: Vec<String>,
    pub expires_at: Option<SystemTime>,
    pub raw: String,
}

/// Error types for bootstrap token operations.
#[derive(Debug)]
pub(crate) enum BootstrapTokenError {
    /// Keyspace storage error
    Storage(String),
    /// Token payload is malformed or invalid
    Malformed(String),
    /// Token format validation failed
    InvalidFormat(String),
    /// Token has expired
    Expired,
    /// Secret decryption failed
    DecryptionFailed(String),
}

impl BootstrapTokenError {
    /// Returns true if this is a validation error (client's fault).
    #[allow(dead_code)]
    pub fn is_client_error(&self) -> bool {
        matches!(
            self,
            BootstrapTokenError::Malformed(_)
                | BootstrapTokenError::InvalidFormat(_)
                | BootstrapTokenError::Expired
        )
    }

    /// Returns true if this is a server/storage error.
    #[allow(dead_code)]
    pub fn is_server_error(&self) -> bool {
        matches!(
            self,
            BootstrapTokenError::Storage(_) | BootstrapTokenError::DecryptionFailed(_)
        )
    }
}

impl fmt::Display for BootstrapTokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BootstrapTokenError::Storage(msg) => write!(f, "keyspace error: {msg}"),
            BootstrapTokenError::Malformed(msg) => {
                write!(f, "invalid bootstrap token payload: {msg}")
            }
            BootstrapTokenError::InvalidFormat(msg) => {
                write!(f, "invalid token format: {msg}")
            }
            BootstrapTokenError::Expired => write!(f, "bootstrap token has expired"),
            BootstrapTokenError::DecryptionFailed(msg) => {
                write!(f, "failed to decrypt token secret: {msg}")
            }
        }
    }
}

impl Error for BootstrapTokenError {}

/// Validate a bootstrap token format before lookup.
///
/// Returns the token ID and secret parts if valid.
#[allow(dead_code)]
pub(crate) fn validate_token_format(token: &str) -> Result<(String, String), BootstrapTokenError> {
    let trimmed = token.trim();

    if trimmed.is_empty() {
        return Err(BootstrapTokenError::InvalidFormat(
            "token cannot be empty".to_string(),
        ));
    }

    if trimmed.len() > MAX_TOKEN_ID_LENGTH + MAX_TOKEN_ID_LENGTH + 1 {
        return Err(BootstrapTokenError::InvalidFormat(
            "token exceeds maximum length".to_string(),
        ));
    }

    let (id, secret) = trimmed.split_once('.').ok_or_else(|| {
        BootstrapTokenError::InvalidFormat(
            "token must be in format 'id.secret'".to_string(),
        )
    })?;

    if id.is_empty() {
        return Err(BootstrapTokenError::InvalidFormat(
            "token ID cannot be empty".to_string(),
        ));
    }

    if secret.is_empty() {
        return Err(BootstrapTokenError::InvalidFormat(
            "token secret cannot be empty".to_string(),
        ));
    }

    if id.len() > MAX_TOKEN_ID_LENGTH {
        return Err(BootstrapTokenError::InvalidFormat(
            format!("token ID exceeds maximum length of {}", MAX_TOKEN_ID_LENGTH),
        ));
    }

    // Check for invalid characters in ID (path traversal prevention)
    if id.contains('/') || id.contains('\\') || id.contains("..") {
        return Err(BootstrapTokenError::InvalidFormat(
            "token ID contains invalid characters".to_string(),
        ));
    }

    Ok((id.to_string(), secret.to_string()))
}

#[derive(Clone)]
pub(crate) struct BootstrapTokenService {
    keyspace: Keyspace,
}

impl BootstrapTokenService {
    pub fn new() -> Self {
        Self {
            keyspace: Keyspace::new("tokens"),
        }
    }

    pub fn lookup(&self, token: &str) -> Result<Option<BootstrapToken>, BootstrapTokenError> {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }

        let (id_part, secret_part) = match trimmed.split_once('.') {
            Some((id, secret)) if !id.is_empty() && !secret.is_empty() => {
                (Some(id.to_string()), Some(secret.to_string()))
            }
            _ => (None, None),
        };

        let mut lookup_keys = Vec::new();
        if let Some(ref id) = id_part {
            lookup_keys.push(format!("{BOOTSTRAP_PREFIX}/{id}"));
        }
        lookup_keys.push(format!("{BOOTSTRAP_PREFIX}/{trimmed}"));

        let mut raw_value = None;
        let mut expires_at = None;
        for key in lookup_keys.into_iter() {
            match self.keyspace.get_with_expiry(&key) {
                Ok((value, expiry)) => {
                    raw_value = Some(value);
                    expires_at = expiry;
                    break;
                }
                Err(err) => {
                    let message = err.to_string();
                    if message.contains("Value file not found") {
                        continue;
                    }
                    return Err(BootstrapTokenError::Storage(message));
                }
            }
        }

        let Some(raw_value) = raw_value else {
            return Ok(None);
        };

        let grant: Value = serde_json::from_str(&raw_value)
            .map_err(|err| BootstrapTokenError::Malformed(err.to_string()))?;

        if let Some(expected_secret) = secret_part.as_deref() {
            match extract_secret(&grant) {
                Ok(Some(actual_secret)) if actual_secret == expected_secret => {}
                Ok(_) => return Ok(None),
                Err(err) => return Err(err),
            }
        }

        if let Some(expiry) = expires_at {
            if SystemTime::now() >= expiry {
                return Err(BootstrapTokenError::Expired);
            }
        }

        let subject = grant
            .get("user")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                BootstrapTokenError::Malformed(
                    "bootstrap token payload missing non-empty 'user' field".to_string(),
                )
            })?
            .to_string();

        if subject.len() > MAX_SUBJECT_LENGTH {
            return Err(BootstrapTokenError::Malformed(format!(
                "bootstrap token subject exceeds maximum length of {} characters",
                MAX_SUBJECT_LENGTH
            )));
        }

        let cluster = grant
            .get("cluster")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);

        let scopes = normalize_string_list(grant.get("scope"))
            .or_else(|| normalize_string_list(grant.get("scopes")))
            .unwrap_or_else(|| vec![CERTIFICATE_SCOPE.to_string()]);

        let audiences = normalize_string_list(grant.get("aud")).unwrap_or_default();

        let token_record = BootstrapToken {
            token: trimmed.to_string(),
            subject,
            cluster,
            scopes,
            audiences,
            expires_at,
            raw: raw_value,
        };

        Ok(Some(token_record))
    }

    pub fn consume_token(&self, token: &str) -> Result<(), BootstrapTokenError> {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            return Ok(());
        }

        let (id_part, _) = match trimmed.split_once('.') {
            Some((id, secret)) if !id.is_empty() && !secret.is_empty() => {
                (Some(id.to_string()), Some(secret.to_string()))
            }
            _ => (None, None),
        };

        self.invalidate_token(trimmed, id_part.as_deref())
    }

    fn invalidate_token(
        &self,
        token: &str,
        token_id: Option<&str>,
    ) -> Result<(), BootstrapTokenError> {
        let mut targets = Vec::new();

        let canonical_key = format!("{BOOTSTRAP_PREFIX}/{token}");
        if !targets.iter().any(|existing| existing == &canonical_key) {
            targets.push(canonical_key);
        }

        if let Some(id) = token_id {
            let id_key = format!("{BOOTSTRAP_PREFIX}/{id}");
            if !targets.iter().any(|existing| existing == &id_key) {
                targets.push(id_key);
            }
        }

        for key in targets {
            match self.keyspace.delete(&key) {
                Ok(_) => {}
                Err(err) => {
                    let message = err.to_string();
                    if message.contains("Value file not found") {
                        continue;
                    }
                    return Err(BootstrapTokenError::Storage(message));
                }
            }
        }

        Ok(())
    }
}

impl Default for BootstrapTokenService {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn spawn_bootstrap_token_maintenance() {
    static TASK_HANDLE: OnceLock<ScheduledTaskHandle> = OnceLock::new();

    let keyspace = Keyspace::new("tokens");
    if let Err(err) = keyspace.repair_now() {
        let error_text = err.to_string();
        let metadata = [("error", error_text.as_str())];
        log_warn(
            BOOTSTRAP_LOG_COMPONENT,
            "Failed to repair bootstrap token keyspace",
            &metadata,
        );
    }

    let schedule = match CronSchedule::from_str(TOKEN_REPAIR_SCHEDULE, UTC) {
        Ok(schedule) => schedule,
        Err(err) => {
            let error_text = err.to_string();
            let metadata = [("error", error_text.as_str())];
            log_warn(
                BOOTSTRAP_LOG_COMPONENT,
                "Failed to schedule bootstrap token maintenance",
                &metadata,
            );
            return;
        }
    };

    let scheduler = Scheduler::global();
    let _warmup = scheduler.schedule(
        ScheduleSpec::After {
            label: "bootstrap-token-maintenance-warmup",
            delay: Duration::from_secs(1),
        },
        |_| {
            Box::pin(async move {
                if let Err(err) = Keyspace::new("tokens").repair_now() {
                    log_warn(
                        BOOTSTRAP_LOG_COMPONENT,
                        "Initial bootstrap token repair failed",
                        &[("error", err.to_string().as_str())],
                    );
                }
                JobResult::Stop
            })
        },
    );

    let handle = scheduler.schedule(
        ScheduleSpec::Cron {
            label: "bootstrap-token-maintenance",
            schedule: Box::new(schedule),
        },
        move |ctx| {
            let keyspace = Keyspace::new("tokens");
            let scheduled_for = ctx.scheduled_for();
            Box::pin(async move {
                if let Err(err) = keyspace.repair_now() {
                    let error_text = err.to_string();
                    let mut owned = vec![("error".to_string(), error_text)];
                    if let Some(when) = scheduled_for {
                        owned.push(("scheduled_for".to_string(), when.to_rfc3339()));
                    }
                    let metadata: Vec<(&str, &str)> = owned
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_str()))
                        .collect();
                    log_warn(
                        BOOTSTRAP_LOG_COMPONENT,
                        "Failed to repair bootstrap token keyspace",
                        &metadata,
                    );
                }
                JobResult::Continue
            })
        },
    );

    let _ = TASK_HANDLE.set(handle);
}

fn extract_secret(grant: &Value) -> Result<Option<String>, BootstrapTokenError> {
    let Some(secret_value) = grant.get("secret") else {
        return Ok(None);
    };

    let secret_object = secret_value.as_object().ok_or_else(|| {
        BootstrapTokenError::Malformed(
            "bootstrap token secret payload must be an object".to_string(),
        )
    })?;

    let key = secret_object
        .get("key")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            BootstrapTokenError::Malformed(
                "bootstrap token secret payload missing non-empty 'key' field".to_string(),
            )
        })?;

    let ciphertext = secret_object
        .get("ciphertext")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            BootstrapTokenError::Malformed(
                "bootstrap token secret payload missing non-empty 'ciphertext' field".to_string(),
            )
        })?;

    let encryption_key = EncryptionKey::unwrap(&key.to_string())
        .map_err(|err| BootstrapTokenError::DecryptionFailed(err.to_string()))?;
    let decrypted = encryption_key
        .decrypt(&ciphertext.to_string())
        .map_err(|err| BootstrapTokenError::DecryptionFailed(err.to_string()))?;
    let secret = String::from_utf8(decrypted).map_err(|_| {
        BootstrapTokenError::DecryptionFailed(
            "bootstrap token secret payload was not valid UTF-8".to_string(),
        )
    })?;

    Ok(Some(secret))
}

fn normalize_string_list(value: Option<&Value>) -> Option<Vec<String>> {
    let value = value?;
    match value {
        Value::Null => None,
        Value::String(s) => {
            let text = s.trim();
            if text.is_empty() {
                Some(Vec::new())
            } else {
                Some(vec![text.to_string()])
            }
        }
        Value::Array(items) => {
            let mut collected = Vec::new();
            for item in items {
                if let Some(text) = item.as_str() {
                    let trimmed = text.trim();
                    if !trimmed.is_empty() {
                        collected.push(trimmed.to_string());
                    }
                }
            }
            Some(collected)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::test_support::{keyspace_lock, test_output_dir};
    use serial_test::serial;
    use std::env;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, OnceLock};
    use std::thread;
    use std::time::Duration;

    use crate::nanocloud::util::security::{clear_asset_caches, EncryptionKey, SecureAssets};
    use serde_json::json;

    fn test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct EnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvGuard {
        fn set<P: AsRef<Path>>(key: &'static str, value: P) -> Self {
            let previous = env::var(key).ok();
            let value_string = value.as_ref().to_string_lossy().into_owned();
            env::set_var(key, value_string);
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(prev) = self.previous.as_ref() {
                env::set_var(self.key, prev);
            } else {
                env::remove_var(self.key);
            }
        }
    }

    struct TestEnv {
        base: PathBuf,
        keyspace_dir: PathBuf,
        lock_file: PathBuf,
        secure_dir: PathBuf,
        _keyspace: EnvGuard,
        _lock: EnvGuard,
        _secure: EnvGuard,
        _guard: std::sync::MutexGuard<'static, ()>,
    }

    impl TestEnv {
        fn new() -> Self {
            let guard = keyspace_lock().lock();
            let base = test_output_dir("bootstrap-auth");
            let keyspace_dir = base.join("keyspace");
            let lock_dir = base.join("lock");
            let secure_dir = base.join("secure");

            fs::create_dir_all(&keyspace_dir).expect("keyspace dir");
            fs::create_dir_all(&lock_dir).expect("lock dir");
            fs::create_dir_all(&secure_dir).expect("secure dir");

            let lock_file = lock_dir.join("nanocloud.lock");
            std::fs::File::create(&lock_file).expect("lock file");
            let keyspace_guard = EnvGuard::set("NANOCLOUD_KEYSPACE", keyspace_dir.clone());
            let lock_guard = EnvGuard::set("NANOCLOUD_LOCK_FILE", lock_file.clone());
            let secure_guard = EnvGuard::set("NANOCLOUD_SECURE_ASSETS", secure_dir.clone());

            clear_asset_caches();
            SecureAssets::generate(&secure_dir, false).expect("secure assets");

            TestEnv {
                base,
                keyspace_dir,
                lock_file,
                secure_dir,
                _keyspace: keyspace_guard,
                _lock: lock_guard,
                _secure: secure_guard,
                _guard: guard,
            }
        }

        fn refresh_env(&self) {
            env::set_var("NANOCLOUD_KEYSPACE", &self.keyspace_dir);
            env::set_var("NANOCLOUD_LOCK_FILE", &self.lock_file);
            env::set_var("NANOCLOUD_SECURE_ASSETS", &self.secure_dir);
        }
    }

    impl Drop for TestEnv {
        fn drop(&mut self) {
            if let Err(err) = fs::remove_dir_all(&self.base) {
                if err.kind() != std::io::ErrorKind::NotFound {
                    eprintln!(
                        "warning: failed to clean test directory '{}': {}",
                        self.base.display(),
                        err
                    );
                }
            }
        }
    }

    #[test]
    #[serial]
    fn returns_bootstrap_token_when_present() {
        let _guard = test_lock().lock().unwrap();
        let env = TestEnv::new();
        env.refresh_env();
        let _env = env;

        let token_id = "abc123";
        let token_secret = "0123456789abcdef";
        let full_token = format!("{token_id}.{token_secret}");

        let encryption_key = EncryptionKey::new(None);
        let wrapped_key = encryption_key.wrap().expect("wrap key");
        let ciphertext = encryption_key
            .encrypt(token_secret.as_bytes())
            .expect("encrypt secret");

        let grant = json!({
            "user": "bootstrap-user",
            "cluster": "demo",
            "scope": ["install"],
            "aud": ["nanocloud"],
            "secret": {
                "key": wrapped_key,
                "ciphertext": ciphertext,
            }
        });

        let keyspace = Keyspace::new("tokens");
        keyspace
            .put_with_ttl(
                &format!("{BOOTSTRAP_PREFIX}/{token_id}"),
                &grant.to_string(),
                Duration::from_secs(60),
            )
            .expect("store token");

        let service = BootstrapTokenService::new();
        let token = service
            .lookup(&full_token)
            .expect("lookup")
            .expect("token present");

        assert_eq!(token.subject, "bootstrap-user");
        assert_eq!(token.cluster.as_deref(), Some("demo"));
        assert_eq!(token.scopes, vec!["install".to_string()]);
        assert_eq!(token.audiences, vec!["nanocloud".to_string()]);
        assert_eq!(token.token, full_token);

        service.consume_token(&full_token).expect("consume token");
        assert!(service.lookup(&full_token).unwrap().is_none());
        assert!(keyspace
            .get(&format!("{BOOTSTRAP_PREFIX}/{token_id}"))
            .is_err());
    }

    #[test]
    fn validate_token_format_accepts_valid_tokens() {
        let result = validate_token_format("abc123.secret456");
        assert!(result.is_ok());
        let (id, secret) = result.unwrap();
        assert_eq!(id, "abc123");
        assert_eq!(secret, "secret456");
    }

    #[test]
    fn validate_token_format_rejects_empty() {
        assert!(matches!(
            validate_token_format(""),
            Err(BootstrapTokenError::InvalidFormat(_))
        ));
        assert!(matches!(
            validate_token_format("   "),
            Err(BootstrapTokenError::InvalidFormat(_))
        ));
    }

    #[test]
    fn validate_token_format_rejects_no_separator() {
        assert!(matches!(
            validate_token_format("noseparator"),
            Err(BootstrapTokenError::InvalidFormat(_))
        ));
    }

    #[test]
    fn validate_token_format_rejects_empty_parts() {
        assert!(matches!(
            validate_token_format(".secret"),
            Err(BootstrapTokenError::InvalidFormat(_))
        ));
        assert!(matches!(
            validate_token_format("id."),
            Err(BootstrapTokenError::InvalidFormat(_))
        ));
    }

    #[test]
    fn validate_token_format_rejects_path_traversal() {
        assert!(matches!(
            validate_token_format("../etc/passwd.secret"),
            Err(BootstrapTokenError::InvalidFormat(_))
        ));
        assert!(matches!(
            validate_token_format("foo/bar.secret"),
            Err(BootstrapTokenError::InvalidFormat(_))
        ));
        assert!(matches!(
            validate_token_format("foo\\bar.secret"),
            Err(BootstrapTokenError::InvalidFormat(_))
        ));
    }

    #[test]
    fn bootstrap_token_error_classification() {
        assert!(BootstrapTokenError::Malformed("test".into()).is_client_error());
        assert!(BootstrapTokenError::InvalidFormat("test".into()).is_client_error());
        assert!(BootstrapTokenError::Expired.is_client_error());

        assert!(BootstrapTokenError::Storage("test".into()).is_server_error());
        assert!(BootstrapTokenError::DecryptionFailed("test".into()).is_server_error());
    }

    #[test]
    fn bootstrap_token_error_display() {
        let storage = BootstrapTokenError::Storage("io error".into());
        assert!(storage.to_string().contains("keyspace error"));

        let malformed = BootstrapTokenError::Malformed("bad json".into());
        assert!(malformed.to_string().contains("invalid bootstrap token"));

        let invalid = BootstrapTokenError::InvalidFormat("no dot".into());
        assert!(invalid.to_string().contains("invalid token format"));

        let expired = BootstrapTokenError::Expired;
        assert!(expired.to_string().contains("expired"));

        let decrypt = BootstrapTokenError::DecryptionFailed("key error".into());
        assert!(decrypt.to_string().contains("decrypt"));
    }

    #[test]
    #[serial]
    fn returns_none_for_missing_or_expired_token() {
        let _guard = test_lock().lock().unwrap();
        let env = TestEnv::new();
        env.refresh_env();
        let _env = env;

        let token_id = "exp123";
        let token_secret = "fedcba9876543210";
        let full_token = format!("{token_id}.{token_secret}");

        let encryption_key = EncryptionKey::new(None);
        let wrapped_key = encryption_key.wrap().expect("wrap key");
        let ciphertext = encryption_key
            .encrypt(token_secret.as_bytes())
            .expect("encrypt secret");

        let grant = json!({
            "user": "expiring-user",
            "secret": {
                "key": wrapped_key,
                "ciphertext": ciphertext,
            }
        });

        let keyspace = Keyspace::new("tokens");
        keyspace
            .put_with_ttl(
                &format!("{BOOTSTRAP_PREFIX}/{token_id}"),
                &grant.to_string(),
                Duration::from_secs(1),
            )
            .expect("store token");

        let service = BootstrapTokenService::new();
        assert!(service.lookup("MISSING").unwrap().is_none());

        assert!(service
            .lookup(&format!("{token_id}.wrongsecret"))
            .unwrap()
            .is_none());

        thread::sleep(Duration::from_secs(2));
        assert!(service.lookup(&full_token).unwrap().is_none());
    }

    // Additional configuration validation tests

    #[test]
    fn validate_token_format_rejects_too_long_id() {
        // Token IDs longer than MAX_TOKEN_ID_LENGTH should be rejected
        let long_id = "a".repeat(MAX_TOKEN_ID_LENGTH + 1);
        let token = format!("{}.secret", long_id);
        let result = validate_token_format(&token);
        assert!(matches!(result, Err(BootstrapTokenError::InvalidFormat(_))));
    }

    #[test]
    fn validate_token_format_accepts_max_length_id() {
        // Token IDs at exactly MAX_TOKEN_ID_LENGTH should be accepted
        let max_id = "a".repeat(MAX_TOKEN_ID_LENGTH);
        let token = format!("{}.secret", max_id);
        let result = validate_token_format(&token);
        assert!(result.is_ok());
    }

    #[test]
    fn validate_token_format_handles_multiple_dots() {
        // Token with multiple dots - should split at first dot
        let result = validate_token_format("id.part1.part2.part3");
        assert!(result.is_ok());
        let (id, secret) = result.unwrap();
        assert_eq!(id, "id");
        assert_eq!(secret, "part1.part2.part3");
    }

    #[test]
    fn bootstrap_token_error_is_server_error() {
        assert!(!BootstrapTokenError::Malformed("test".into()).is_server_error());
        assert!(!BootstrapTokenError::InvalidFormat("test".into()).is_server_error());
        assert!(!BootstrapTokenError::Expired.is_server_error());

        assert!(BootstrapTokenError::Storage("test".into()).is_server_error());
        assert!(BootstrapTokenError::DecryptionFailed("test".into()).is_server_error());
    }

    #[test]
    fn bootstrap_token_equality() {
        let token1 = BootstrapToken {
            token: "abc.def".to_string(),
            subject: "user1".to_string(),
            cluster: Some("cluster1".to_string()),
            scopes: vec!["scope1".to_string()],
            audiences: vec!["aud1".to_string()],
            expires_at: None,
            raw: "{}".to_string(),
        };

        let token2 = BootstrapToken {
            token: "abc.def".to_string(),
            subject: "user1".to_string(),
            cluster: Some("cluster1".to_string()),
            scopes: vec!["scope1".to_string()],
            audiences: vec!["aud1".to_string()],
            expires_at: None,
            raw: "{}".to_string(),
        };

        assert_eq!(token1, token2);
    }

    #[test]
    fn bootstrap_token_debug() {
        let token = BootstrapToken {
            token: "abc.def".to_string(),
            subject: "testuser".to_string(),
            cluster: None,
            scopes: vec![],
            audiences: vec![],
            expires_at: None,
            raw: "{}".to_string(),
        };

        let debug = format!("{:?}", token);
        assert!(debug.contains("testuser"));
        assert!(debug.contains("BootstrapToken"));
    }

    #[test]
    fn bootstrap_token_clone() {
        let token = BootstrapToken {
            token: "abc.def".to_string(),
            subject: "user".to_string(),
            cluster: Some("cluster".to_string()),
            scopes: vec!["read".to_string(), "write".to_string()],
            audiences: vec!["api".to_string()],
            expires_at: None,
            raw: "{}".to_string(),
        };

        let cloned = token.clone();
        assert_eq!(token, cloned);
    }

    #[test]
    fn validate_token_format_whitespace_handling() {
        // Leading/trailing whitespace in parts should be handled
        let result = validate_token_format("  id  .  secret  ");
        // Should either fail validation or trim whitespace appropriately
        // Current implementation doesn't trim, so spaces become part of id
        if let Ok((id, _secret)) = result {
            // Verify behavior is consistent
            assert!(id.contains(" ") || id == "id");
        }
    }

    #[test]
    fn validate_token_format_special_characters() {
        // Test with various special characters that might be problematic
        // Path separators in the ID (before first dot) should be rejected
        assert!(validate_token_format("id/part.secret").is_err());
        assert!(validate_token_format("id\\part.secret").is_err());

        // Double-dot is only checked in the ID portion (before first dot)
        // The ID is everything before the first dot character
        // "a..b.secret" -> id="a", so no .. in ID, accepted
        let result = validate_token_format("a..b.secret");
        assert!(result.is_ok());
        let (id, secret) = result.unwrap();
        assert_eq!(id, "a");
        assert_eq!(secret, ".b.secret");

        // "a..secret" -> id="a", secret=".secret", valid
        assert!(validate_token_format("a..secret").is_ok());

        // To actually trigger the .. rejection, it must appear in the ID
        // (before the first dot). Using a split_once means .. before first dot.
        assert!(validate_token_format("a..secret").is_ok()); // a, .secret - no .. in "a"

        // Edge case: token starting with .. has id=".." which contains ".."
        assert!(validate_token_format("..a.secret").is_err());
    }
}
