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

use std::time::{Duration, SystemTime};

use axum::http::StatusCode;
use axum::Json;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, SecondsFormat, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use openssl::rand::rand_bytes;
use serde::{Deserialize, Serialize};

use super::error::ApiError;
use crate::nanocloud::api::types::{ServiceAccountTokenRequest, ServiceAccountTokenResponse};
use crate::nanocloud::logger::{log_info, log_warn};
use crate::nanocloud::util::security::load_service_secret_key;
use crate::nanocloud::util::{Keyspace, SingleUseTokenOutcome};

const COMPONENT: &str = "serviceaccounts";
const TOKENS_KEYSPACE: Keyspace = Keyspace::new("tokens");
const BOOTSTRAP_TOKEN_PREFIX: &str = "/v1/token";
const MAX_BOOTSTRAP_TOKEN_TTL: Duration = Duration::from_secs(600);
const JWT_TTL: Duration = Duration::from_secs(300);
const ISSUER: &str = "nanocloud.io/control-plane";
pub(crate) const DEFAULT_AUDIENCE: &str = "nanocloud.io/certificate";
pub(crate) const CERTIFICATE_SCOPE: &str = "certificate.issue";

#[derive(Debug, Deserialize)]
struct BootstrapGrant {
    user: String,
    #[serde(default)]
    cluster: Option<String>,
    #[serde(default)]
    audience: Vec<String>,
    #[serde(default)]
    scope: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ServiceAccountClaims {
    pub(crate) iss: String,
    pub(crate) sub: String,
    #[serde(default)]
    pub(crate) aud: Vec<String>,
    #[serde(default)]
    pub(crate) scope: Vec<String>,
    pub(crate) iat: i64,
    pub(crate) exp: i64,
    pub(crate) jti: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cluster: Option<String>,
}

pub async fn exchange_bootstrap_token(
    Json(payload): Json<ServiceAccountTokenRequest>,
) -> Result<Json<ServiceAccountTokenResponse>, ApiError> {
    let token = payload.single_use_token.trim();
    if token.is_empty() {
        return Err(ApiError::bad_request("singleUseToken must not be empty"));
    }

    let key = format!("{}/{}", BOOTSTRAP_TOKEN_PREFIX, token);
    let outcome = TOKENS_KEYSPACE
        .consume_single_use(&key, MAX_BOOTSTRAP_TOKEN_TTL)
        .map_err(ApiError::internal_error)?;

    let bootstrap = match outcome {
        SingleUseTokenOutcome::Consumed { value, expires_at } => {
            if let Some(expiry) = expires_at {
                let max_expiry = SystemTime::now()
                    .checked_add(MAX_BOOTSTRAP_TOKEN_TTL)
                    .ok_or_else(|| ApiError::internal_message("TTL budget overflow"))?;
                if expiry > max_expiry {
                    log_warn(
                        COMPONENT,
                        "Bootstrap token TTL exceeds configured budget",
                        &[("token", token)],
                    );
                    return Err(ApiError::bad_request(
                        "single-use token TTL exceeds configured budget",
                    ));
                }
            }
            serde_json::from_str::<BootstrapGrant>(&value)
                .map_err(|err| ApiError::internal_error(err.into()))?
        }
        SingleUseTokenOutcome::Expired { .. } => {
            log_warn(COMPONENT, "Bootstrap token expired before exchange", &[]);
            return Err(ApiError::new(StatusCode::GONE, "single-use token expired"));
        }
        SingleUseTokenOutcome::Replay => {
            log_warn(COMPONENT, "Bootstrap token replay blocked", &[]);
            return Err(ApiError::new(
                StatusCode::GONE,
                "single-use token already consumed",
            ));
        }
        SingleUseTokenOutcome::NotFound => {
            return Err(ApiError::new(
                StatusCode::NOT_FOUND,
                "single-use token not found",
            ));
        }
        SingleUseTokenOutcome::MissingExpiry => {
            return Err(ApiError::internal_message(
                "single-use token missing expiry metadata",
            ));
        }
        SingleUseTokenOutcome::TtlBudgetExceeded { .. } => {
            return Err(ApiError::bad_request(
                "single-use token TTL exceeds configured budget",
            ));
        }
    };

    if bootstrap.user.trim().is_empty() {
        return Err(ApiError::internal_message(
            "bootstrap payload is missing user identity",
        ));
    }

    let issued_at = SystemTime::now();
    let expires_at = issued_at
        .checked_add(JWT_TTL)
        .ok_or_else(|| ApiError::internal_message("JWT duration overflow"))?;

    let claims = ServiceAccountClaims {
        iss: ISSUER.to_string(),
        sub: bootstrap.user.clone(),
        aud: normalize_audience(&bootstrap),
        scope: normalize_scopes(&bootstrap),
        iat: to_epoch_seconds(issued_at)?,
        exp: to_epoch_seconds(expires_at)?,
        jti: random_jti()?,
        cluster: bootstrap.cluster.clone(),
    };

    let encoding_key = encoding_key()?;
    let token = jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &encoding_key)
        .map_err(|err| ApiError::internal_error(err.into()))?;

    log_info(
        COMPONENT,
        "Issued short-lived service account JWT",
        &[
            ("subject", &bootstrap.user),
            (
                "audience",
                &claims
                    .aud
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<&str>>()
                    .join(","),
            ),
        ],
    );

    Ok(Json(ServiceAccountTokenResponse {
        jwt: token,
        issued_at: format_timestamp(issued_at),
        expires_at: format_timestamp(expires_at),
    }))
}

fn normalize_audience(grant: &BootstrapGrant) -> Vec<String> {
    if grant.audience.is_empty() {
        return vec![DEFAULT_AUDIENCE.to_string()];
    }

    let filtered = grant
        .audience
        .iter()
        .map(|aud| aud.trim().to_string())
        .filter(|aud| !aud.is_empty())
        .collect::<Vec<_>>();

    if filtered.is_empty() {
        vec![DEFAULT_AUDIENCE.to_string()]
    } else {
        filtered
    }
}

fn normalize_scopes(grant: &BootstrapGrant) -> Vec<String> {
    let mut scopes = if grant.scope.is_empty() {
        vec![CERTIFICATE_SCOPE.to_string()]
    } else {
        grant
            .scope
            .iter()
            .map(|scope| scope.trim().to_string())
            .filter(|scope| !scope.is_empty())
            .collect::<Vec<_>>()
    };
    if scopes.is_empty() {
        scopes.push(CERTIFICATE_SCOPE.to_string());
    }
    scopes
}

fn format_timestamp(time: SystemTime) -> String {
    let datetime: DateTime<Utc> = time.into();
    datetime.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn to_epoch_seconds(time: SystemTime) -> Result<i64, ApiError> {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .map_err(|err| ApiError::internal_error(err.into()))
}

fn random_jti() -> Result<String, ApiError> {
    let mut bytes = [0u8; 18];
    rand_bytes(&mut bytes).map_err(|err| ApiError::internal_error(err.into()))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn encoding_key() -> Result<EncodingKey, ApiError> {
    let key = load_service_secret_key().map_err(ApiError::internal_error)?;
    let pem = key
        .private_key_to_pem_pkcs8()
        .map_err(|err| ApiError::internal_error(err.into()))?;
    EncodingKey::from_rsa_pem(&pem).map_err(|err| ApiError::internal_error(err.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::test_support::keyspace_lock;
    use crate::nanocloud::util::security::SecureAssets;
    use axum::response::IntoResponse;
    use axum::Json;
    use chrono::{DateTime, Utc};
    use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
    use serde_json::json;
    use serial_test::serial;
    use std::env;
    use std::fs;
    use std::path::Path;
    use std::sync::{MutexGuard, OnceLock};
    use tempfile::TempDir;
    use tokio::sync::Mutex;

    fn test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct EnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvGuard {
        fn set<K: AsRef<Path>>(key: &'static str, value: K) -> Self {
            let previous = env::var(key).ok();
            env::set_var(key, value.as_ref());
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
        _base: TempDir,
        _keyspace: EnvGuard,
        _lock: EnvGuard,
        _secure: EnvGuard,
        _guard: MutexGuard<'static, ()>,
    }

    impl TestEnv {
        fn new() -> Self {
            let guard = keyspace_lock().lock();
            let base = TempDir::new().expect("tempdir");
            let keyspace_dir = base.path().join("keyspace");
            let lock_dir = base.path().join("lock");
            let secure_dir = base.path().join("secure");

            fs::create_dir_all(&keyspace_dir).expect("create keyspace dir");
            fs::create_dir_all(&lock_dir).expect("create lock dir");
            fs::create_dir_all(&secure_dir).expect("create secure dir");

            let lock_file = lock_dir.join("nanocloud.lock");
            let keyspace_guard = EnvGuard::set("NANOCLOUD_KEYSPACE", &keyspace_dir);
            let lock_guard = EnvGuard::set("NANOCLOUD_LOCK_FILE", &lock_file);
            let secure_guard = EnvGuard::set("NANOCLOUD_SECURE_ASSETS", &secure_dir);

            SecureAssets::generate(&secure_dir, false).expect("secure assets");

            TestEnv {
                _base: base,
                _keyspace: keyspace_guard,
                _lock: lock_guard,
                _secure: secure_guard,
                _guard: guard,
            }
        }
    }

    fn store_bootstrap_token(token: &str, grant: serde_json::Value, ttl: Duration) {
        let keyspace = Keyspace::new("tokens");
        let key = format!("/v1/token/{}", token);
        keyspace
            .put_with_ttl(&key, &grant.to_string(), ttl)
            .expect("store bootstrap token");
    }

    fn decode_claims(jwt: &str) -> ServiceAccountClaims {
        let key = load_service_secret_key().expect("load service key");
        let pem = key
            .public_key_to_pem()
            .expect("export public key for decoding");
        let decoding_key = DecodingKey::from_rsa_pem(&pem).expect("decoding key");
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[DEFAULT_AUDIENCE.to_string()]);
        decode::<ServiceAccountClaims>(jwt, &decoding_key, &validation)
            .expect("decode jwt")
            .claims
    }

    #[tokio::test]
    #[serial]
    async fn jwt_claims_include_expected_defaults() {
        let _guard = test_lock().lock().await;
        let _env = TestEnv::new();

        let token = "CLAIMS123TOKEN";
        let user = "claims-user";
        let cluster = "integration-cluster";
        store_bootstrap_token(
            token,
            json!({ "user": user, "cluster": cluster }),
            Duration::from_secs(60),
        );

        let Json(response) = exchange_bootstrap_token(Json(ServiceAccountTokenRequest {
            single_use_token: token.to_string(),
        }))
        .await
        .expect("token exchange succeeds");

        assert!(
            !response.jwt.is_empty(),
            "exchange should return a signed JWT"
        );

        let issued_at = DateTime::parse_from_rfc3339(&response.issued_at)
            .expect("parse issued_at")
            .with_timezone(&Utc);
        let expires_at = DateTime::parse_from_rfc3339(&response.expires_at)
            .expect("parse expires_at")
            .with_timezone(&Utc);
        let ttl_seconds = (expires_at - issued_at).num_seconds();
        assert_eq!(
            ttl_seconds,
            JWT_TTL.as_secs() as i64,
            "JWT TTL should match configured duration"
        );

        let claims = decode_claims(&response.jwt);
        assert_eq!(claims.iss, ISSUER);
        assert_eq!(claims.sub, user);
        assert_eq!(claims.cluster.as_deref(), Some(cluster));
        assert_eq!(
            claims.scope,
            vec![CERTIFICATE_SCOPE.to_string()],
            "scope should default to certificate issuance"
        );
        assert_eq!(
            claims.aud,
            vec![DEFAULT_AUDIENCE.to_string()],
            "audience should default to certificate workflow"
        );
        assert_eq!(
            claims.exp - claims.iat,
            JWT_TTL.as_secs() as i64,
            "claim TTL should match configured duration"
        );
    }

    #[tokio::test]
    async fn rejects_tokens_exceeding_ttl_budget() {
        let _guard = test_lock().lock().await;
        let _env = TestEnv::new();

        let token = "TTL-OVERFLOW";
        let ttl = MAX_BOOTSTRAP_TOKEN_TTL
            .checked_add(Duration::from_secs(60))
            .expect("ttl overflow");
        store_bootstrap_token(token, json!({ "user": "ttl-user" }), ttl);

        let err = exchange_bootstrap_token(Json(ServiceAccountTokenRequest {
            single_use_token: token.to_string(),
        }))
        .await
        .expect_err("TTL budget violation should fail");

        let status = err.into_response().status();
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
