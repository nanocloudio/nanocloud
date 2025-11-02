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

#[derive(Debug)]
pub(crate) enum BootstrapTokenError {
    Storage(String),
    Malformed(String),
}

impl fmt::Display for BootstrapTokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BootstrapTokenError::Storage(msg) => write!(f, "keyspace error: {msg}"),
            BootstrapTokenError::Malformed(msg) => {
                write!(f, "invalid bootstrap token payload: {msg}")
            }
        }
    }
}

impl Error for BootstrapTokenError {}

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
        .map_err(|err| BootstrapTokenError::Malformed(err.to_string()))?;
    let decrypted = encryption_key
        .decrypt(&ciphertext.to_string())
        .map_err(|err| BootstrapTokenError::Malformed(err.to_string()))?;
    let secret = String::from_utf8(decrypted).map_err(|_| {
        BootstrapTokenError::Malformed(
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
}
