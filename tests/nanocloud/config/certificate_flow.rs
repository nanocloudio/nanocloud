use std::env;
use std::fs;
use std::path::Path;
use std::time::Duration;

use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use nanocloud::nanocloud::api::types::{
    CertificateRequest, CertificateSpec, ServiceAccountTokenRequest,
};
use nanocloud::nanocloud::server::handlers::{
    certificates::issue_ephemeral_certificate, serviceaccounts::exchange_bootstrap_token,
};
use nanocloud::nanocloud::test_support::keyspace_lock;
use nanocloud::nanocloud::util::security::SecureAssets;
use nanocloud::nanocloud::util::Keyspace;
use openssl::ec::{EcGroup, EcKey};
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::PKey;
use openssl::x509::{X509NameBuilder, X509ReqBuilder, X509};
use serde_json::json;
use tempfile::TempDir;
use tokio::time::sleep;

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
}

impl TestEnv {
    fn new() -> Self {
        let base = TempDir::new().expect("tempdir");
        let keyspace_dir = base.path().join("keyspace");
        let lock_dir = base.path().join("lock");
        let secure_dir = base.path().join("secure");

        fs::create_dir_all(&keyspace_dir).expect("create keyspace dir");
        fs::create_dir_all(&lock_dir).expect("create lock dir");
        fs::create_dir_all(&secure_dir).expect("create secure assets dir");

        let lock_file = lock_dir.join("nanocloud.lock");
        let keyspace_guard = EnvGuard::set("NANOCLOUD_KEYSPACE", &keyspace_dir);
        let lock_guard = EnvGuard::set("NANOCLOUD_LOCK_FILE", &lock_file);
        let secure_guard = EnvGuard::set("NANOCLOUD_SECURE_ASSETS", &secure_dir);

        SecureAssets::generate(&secure_dir, false).expect("generate secure assets");

        TestEnv {
            _base: base,
            _keyspace: keyspace_guard,
            _lock: lock_guard,
            _secure: secure_guard,
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

fn generate_csr(common_name: &str) -> String {
    let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).expect("curve");
    let ec_key = EcKey::generate(&group).expect("generate key");
    let key_pair = PKey::from_ec_key(ec_key).expect("build signing key");

    let mut name_builder = X509NameBuilder::new().expect("name builder");
    name_builder
        .append_entry_by_text("CN", common_name)
        .expect("set common name");
    let name = name_builder.build();

    let mut csr_builder = X509ReqBuilder::new().expect("csr builder");
    csr_builder
        .set_subject_name(&name)
        .expect("attach subject name");
    csr_builder
        .set_pubkey(&key_pair)
        .expect("attach public key");
    csr_builder
        .sign(&key_pair, MessageDigest::sha256())
        .expect("sign csr");
    let csr = csr_builder.build();
    let pem = csr.to_pem().expect("encode csr");
    String::from_utf8(pem).expect("csr utf8")
}

#[tokio::test(flavor = "current_thread")]
async fn certificate_flow_succeeds() {
    let _env = {
        let _guard = keyspace_lock().lock();
        TestEnv::new()
    };

    let token = "FLOWTOKEN123";
    let user = "bootstrap-user";
    {
        let _guard = keyspace_lock().lock();
        store_bootstrap_token(
            token,
            json!({"user": user, "cluster": "integration"}),
            Duration::from_secs(60),
        );
    }

    let Json(token_response) = exchange_bootstrap_token(Json(ServiceAccountTokenRequest {
        single_use_token: token.to_string(),
    }))
    .await
    .expect("token exchange");

    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", token_response.jwt)).expect("header value"),
    );

    let csr_pem = generate_csr(user);
    let Json(cert_response) = issue_ephemeral_certificate(
        headers,
        Json(CertificateRequest {
            api_version: "nanocloud.io/v1".to_string(),
            kind: "Certificate".to_string(),
            spec: CertificateSpec { csr_pem },
        }),
    )
    .await
    .expect("certificate issuance");

    assert_eq!(cert_response.kind, "Certificate");
    assert!(cert_response
        .status
        .certificate_pem
        .contains("BEGIN CERTIFICATE"));
    assert!(cert_response
        .status
        .ca_bundle_pem
        .contains("BEGIN CERTIFICATE"));
    assert!(
        cert_response.status.expiration_timestamp.is_some(),
        "certificate should include expiration metadata"
    );

    let cert =
        X509::from_pem(cert_response.status.certificate_pem.as_bytes()).expect("parse certificate");
    let subject = cert
        .subject_name()
        .entries_by_nid(Nid::COMMONNAME)
        .next()
        .expect("common name entry")
        .data()
        .as_utf8()
        .expect("utf8 common name")
        .to_string();
    assert_eq!(subject, user);
}

#[tokio::test(flavor = "current_thread")]
async fn exchange_fails_for_expired_token() {
    let _env = {
        let _guard = keyspace_lock().lock();
        TestEnv::new()
    };

    let token = "EXPIREDTOKEN42";
    {
        let _guard = keyspace_lock().lock();
        store_bootstrap_token(
            token,
            json!({ "user": "expired-user" }),
            Duration::from_secs(1),
        );
    }

    sleep(Duration::from_secs(2)).await;

    let err = exchange_bootstrap_token(Json(ServiceAccountTokenRequest {
        single_use_token: token.to_string(),
    }))
    .await
    .expect_err("expired token should fail");

    let status = err.into_response().status();
    assert_eq!(status, StatusCode::GONE);
}

#[tokio::test(flavor = "current_thread")]
async fn exchange_rejects_replay_attempts() {
    let _env = {
        let _guard = keyspace_lock().lock();
        TestEnv::new()
    };

    let token = "REPLAYTOKEN77";
    {
        let _guard = keyspace_lock().lock();
        store_bootstrap_token(
            token,
            json!({ "user": "replay-user" }),
            Duration::from_secs(60),
        );
    }

    let Json(_) = exchange_bootstrap_token(Json(ServiceAccountTokenRequest {
        single_use_token: token.to_string(),
    }))
    .await
    .expect("first exchange should succeed");

    let err = exchange_bootstrap_token(Json(ServiceAccountTokenRequest {
        single_use_token: token.to_string(),
    }))
    .await
    .expect_err("replay should be rejected");

    let status = err.into_response().status();
    assert_eq!(status, StatusCode::NOT_FOUND);
}
