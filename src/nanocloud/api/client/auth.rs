//! Authentication and TLS helpers for `NanocloudClient`.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use openssl::nid::Nid;
use openssl::pkcs12::Pkcs12;
use openssl::pkey::PKey;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use openssl::x509::X509;
use reqwest::tls::Identity;
use serde::Deserialize;
use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) const SERVER_ENV: &str = "NANOCLOUD_SERVER";
pub(super) const DEFAULT_SERVER_ENDPOINT: &str = "https://127.0.0.1:6443";

#[derive(Debug)]
pub struct KubeAuthError {
    context: String,
    source: Option<Box<dyn Error + Send + Sync>>,
}

impl KubeAuthError {
    fn new(context: impl Into<String>) -> Self {
        KubeAuthError {
            context: context.into(),
            source: None,
        }
    }

    fn with_source(
        context: impl Into<String>,
        source: impl Into<Box<dyn Error + Send + Sync>>,
    ) -> Self {
        KubeAuthError {
            context: context.into(),
            source: Some(source.into()),
        }
    }

    fn with_io(context: impl Into<String>, err: io::Error) -> Self {
        KubeAuthError {
            context: context.into(),
            source: Some(Box::new(err)),
        }
    }
}

impl fmt::Display for KubeAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(source) = &self.source {
            write!(f, "{}: {}", self.context, source)
        } else {
            write!(f, "{}", self.context)
        }
    }
}

impl Error for KubeAuthError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| &**source as &(dyn Error + 'static))
    }
}

#[derive(Clone)]
pub struct CurlAuthData {
    pub kubeconfig_path: PathBuf,
    pub kubeconfig_dir: PathBuf,
    pub cluster_name: String,
    pub user_name: String,
    pub ca_source: Option<KubeFieldSource>,
    pub ca_path: Option<PathBuf>,
    pub ca_data: Option<Vec<u8>>,
    pub cert_source: KubeFieldSource,
    pub cert_path: Option<PathBuf>,
    pub cert_data: Vec<u8>,
    pub key_source: KubeFieldSource,
    pub key_path: Option<PathBuf>,
    pub key_data: Vec<u8>,
}

#[derive(Clone, Copy, Debug)]
pub enum KubeFieldSource {
    InlineData,
    FilePath,
}

#[derive(Clone, Debug)]
pub struct EphemeralCertificate {
    pub certificate: Vec<u8>,
    pub ca_bundle: Vec<u8>,
    pub expiration: Option<std::time::SystemTime>,
}

#[derive(Clone)]
pub(super) struct ResolvedData {
    pub(super) bytes: Vec<u8>,
    pub(super) from_data_field: bool,
    pub(super) source_path: Option<PathBuf>,
}

#[derive(Clone)]
pub(super) struct KubeAuth {
    pub(super) kubeconfig_path: PathBuf,
    pub(super) kubeconfig_dir: PathBuf,
    pub(super) cluster_name: String,
    pub(super) user_name: String,
    pub(super) server: String,
    pub(super) server_override: Option<String>,
    pub(super) ca: Option<ResolvedData>,
    pub(super) cert: ResolvedData,
    pub(super) key: ResolvedData,
}

#[derive(Clone)]
pub(super) struct ClientTls {
    pub(super) client_certificate: Vec<u8>,
    pub(super) client_key: Vec<u8>,
    pub(super) ca_bundle: Option<Vec<u8>>,
}

#[derive(Clone)]
pub(super) struct CertificateAuth {
    pub(super) tls: ClientTls,
    pub(super) owner: String,
    pub(super) curl: CurlAuthData,
}

#[derive(Clone)]
pub(super) enum AuthContext {
    ClientCertificate(Box<CertificateAuth>),
    BearerToken { token: String },
}

pub(super) fn load_kube_auth() -> Result<Option<KubeAuth>, KubeAuthError> {
    let path = match kubeconfig_path() {
        Ok(path) => path,
        Err(_) => return Ok(None),
    };

    let raw = match read_kubeconfig(&path) {
        Ok(Some(raw)) => raw,
        Ok(None) => return Ok(None),
        Err(err) => return Err(err),
    };
    let config = parse_kubeconfig(&raw, &path)?;
    let (context, context_name) = select_context(&config)?;
    let cluster = find_cluster(&config, context, &context_name)?;
    let user = find_user(&config, context, &context_name)?;

    let config_dir = config_dir(&path);
    let ca = resolve_ca(&cluster.cluster, &config_dir)?;
    let cert = resolve_client_certificate(&user.user, &config_dir)?;
    let key = resolve_client_key(&user.user, &config_dir)?;

    let server_override = env::var(SERVER_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    Ok(Some(KubeAuth {
        kubeconfig_path: path,
        kubeconfig_dir: config_dir,
        cluster_name: cluster.name.clone(),
        user_name: user.name.clone(),
        server: cluster.cluster.server.clone(),
        server_override,
        ca,
        cert,
        key,
    }))
}

pub(super) fn select_endpoint(host: Option<&str>) -> String {
    if let Ok(value) = env::var(SERVER_ENV) {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }

    if let Some(host) = host.map(str::trim) {
        if !host.is_empty() {
            return normalize_host_to_url(host);
        }
    }

    DEFAULT_SERVER_ENDPOINT.to_string()
}

pub(super) fn build_reqwest_identity(cert_pem: &[u8], key_pem: &[u8]) -> Result<Identity, String> {
    let cert = X509::from_pem(cert_pem)
        .map_err(|err| format!("failed to parse client certificate: {err}"))?;
    let key = PKey::private_key_from_pem(key_pem)
        .map_err(|err| format!("failed to parse client key: {err}"))?;
    let pkcs12 = Pkcs12::builder()
        .name("nanocloud-client")
        .pkey(&key)
        .cert(&cert)
        .build2("")
        .map_err(|err| format!("failed to build client PKCS#12 bundle: {err}"))?;
    let pkcs12_der = pkcs12
        .to_der()
        .map_err(|err| format!("failed to encode client PKCS#12 bundle: {err}"))?;
    Identity::from_pkcs12_der(&pkcs12_der, "")
        .map_err(|err| format!("failed to load client identity: {err}"))
}

pub(super) fn extract_certificate_owner(pem: &[u8]) -> Result<String, KubeAuthError> {
    let certificates = X509::stack_from_pem(pem).map_err(|err| {
        KubeAuthError::with_source("invalid client certificate PEM", io::Error::other(err))
    })?;
    let leaf = certificates.first().ok_or_else(|| {
        KubeAuthError::new("client certificate bundle is empty; expected a leaf certificate")
    })?;
    let common_name = leaf
        .subject_name()
        .entries_by_nid(Nid::COMMONNAME)
        .next()
        .and_then(|entry| entry.data().as_utf8().ok().map(|data| data.to_string()))
        .ok_or_else(|| KubeAuthError::new("client certificate is missing a common name"))?;
    Ok(common_name)
}

pub(super) fn build_exec_ssl_connector(tls: &ClientTls) -> Result<SslConnector, KubeAuthError> {
    #[cfg(test)]
    EXEC_SSL_BUILD_COUNT.fetch_add(1, Ordering::SeqCst);
    let mut builder =
        SslConnector::builder(SslMethod::tls_client()).map_err(|err| KubeAuthError {
            context: "build exec SSL connector".to_string(),
            source: Some(Box::new(err)),
        })?;

    let certificates = X509::stack_from_pem(&tls.client_certificate).map_err(|err| {
        KubeAuthError::with_source("invalid client certificate PEM", io::Error::other(err))
    })?;
    let (leaf, chain) = certificates.split_first().ok_or_else(|| {
        KubeAuthError::new("client certificate PEM did not contain any certificates")
    })?;
    builder.set_certificate(leaf).map_err(|err| {
        KubeAuthError::with_source("set client certificate", io::Error::other(err))
    })?;
    for cert in chain {
        builder
            .add_extra_chain_cert(cert.to_owned())
            .map_err(|err| KubeAuthError::with_source("add client certificate chain entry", err))?;
    }

    let private_key = PKey::private_key_from_pem(&tls.client_key).map_err(|err| {
        KubeAuthError::with_source("invalid client key PEM", io::Error::other(err))
    })?;
    builder
        .set_private_key(&private_key)
        .map_err(|err| KubeAuthError::with_source("set client key", io::Error::other(err)))?;
    builder
        .check_private_key()
        .map_err(|err| KubeAuthError::with_source("verify client key", io::Error::other(err)))?;

    if let Some(ca_bundle) = &tls.ca_bundle {
        let store = builder.cert_store_mut();
        let ca_chain = X509::stack_from_pem(ca_bundle).map_err(|err| {
            KubeAuthError::with_source("invalid CA bundle PEM", io::Error::other(err))
        })?;
        for cert in ca_chain {
            store.add_cert(cert).map_err(|err| {
                KubeAuthError::with_source("add CA certificate to connector", err)
            })?;
        }
    }

    builder.set_verify(SslVerifyMode::PEER);
    Ok(builder.build())
}

#[cfg(test)]
static EXEC_SSL_BUILD_COUNT: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub fn reset_exec_ssl_build_count() {
    EXEC_SSL_BUILD_COUNT.store(0, Ordering::SeqCst);
}

#[cfg(test)]
pub fn exec_ssl_build_count() -> usize {
    EXEC_SSL_BUILD_COUNT.load(Ordering::SeqCst)
}

fn kubeconfig_path() -> Result<PathBuf, KubeAuthError> {
    if let Ok(path) = env::var("KUBECONFIG") {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            return Ok(PathBuf::from(trimmed));
        }
    }

    let home = env::var("HOME").map_err(|_| {
        KubeAuthError::new("HOME environment variable is not set; cannot resolve kubeconfig path")
    })?;
    Ok(PathBuf::from(home).join(".kube").join("config"))
}

fn read_kubeconfig(path: &Path) -> Result<Option<String>, KubeAuthError> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(KubeAuthError::with_io(
            format!("failed to read kubeconfig {}", path.display()),
            err,
        )),
    }
}

fn parse_kubeconfig(raw: &str, path: &Path) -> Result<KubeConfig, KubeAuthError> {
    serde_yaml::from_str(raw).map_err(|err| {
        KubeAuthError::with_source(
            format!("failed to parse kubeconfig {}", path.display()),
            io::Error::other(err),
        )
    })
}

fn select_context(config: &KubeConfig) -> Result<(&NamedContext, String), KubeAuthError> {
    let context_name = config
        .current_context
        .as_deref()
        .map(str::to_string)
        .or_else(|| config.contexts.first().map(|ctx| ctx.name.clone()))
        .ok_or_else(|| KubeAuthError::new("kubeconfig does not define any contexts"))?;

    let context = config
        .contexts
        .iter()
        .find(|ctx| ctx.name == context_name)
        .ok_or_else(|| {
            KubeAuthError::new(format!("kubeconfig missing context '{context_name}'"))
        })?;

    Ok((context, context_name))
}

fn find_cluster<'a>(
    config: &'a KubeConfig,
    context: &NamedContext,
    context_name: &str,
) -> Result<&'a NamedCluster, KubeAuthError> {
    config
        .clusters
        .iter()
        .find(|cl| cl.name == context.context.cluster)
        .ok_or_else(|| {
            KubeAuthError::new(format!(
                "kubeconfig missing cluster '{}' referenced by context '{}'",
                context.context.cluster, context_name
            ))
        })
}

fn find_user<'a>(
    config: &'a KubeConfig,
    context: &NamedContext,
    context_name: &str,
) -> Result<&'a NamedUser, KubeAuthError> {
    config
        .users
        .iter()
        .find(|usr| usr.name == context.context.user)
        .ok_or_else(|| {
            KubeAuthError::new(format!(
                "kubeconfig missing user '{}' referenced by context '{}'",
                context.context.user, context_name
            ))
        })
}

fn config_dir(path: &Path) -> PathBuf {
    path.parent()
        .map(|dir| dir.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."))
}

fn resolve_ca(cluster: &Cluster, config_dir: &Path) -> Result<Option<ResolvedData>, KubeAuthError> {
    let explicit_ca =
        cluster.certificate_authority_data.is_some() || cluster.certificate_authority.is_some();
    if let Some(data) = resolve_data_field(
        cluster.certificate_authority_data.as_ref(),
        cluster.certificate_authority.as_ref(),
        config_dir,
        "certificate authority",
        explicit_ca,
    )? {
        return Ok(Some(data));
    }

    let fallback = read_default_pem_required("ca.pem", "certificate authority")?;
    Ok(Some(fallback))
}

fn resolve_client_certificate(
    user: &UserEntry,
    config_dir: &Path,
) -> Result<ResolvedData, KubeAuthError> {
    let explicit_cert = user.client_certificate_data.is_some() || user.client_certificate.is_some();
    if let Some(data) = resolve_data_field(
        user.client_certificate_data.as_ref(),
        user.client_certificate.as_ref(),
        config_dir,
        "client certificate",
        true,
    )? {
        return Ok(data);
    }

    if explicit_cert {
        Err(KubeAuthError::new(
            "client certificate is required in kubeconfig",
        ))
    } else {
        read_default_pem_required("admin_cert.pem", "client certificate")
    }
}

fn resolve_client_key(user: &UserEntry, config_dir: &Path) -> Result<ResolvedData, KubeAuthError> {
    let explicit_key = user.client_key_data.is_some() || user.client_key.is_some();
    if let Some(data) = resolve_data_field(
        user.client_key_data.as_ref(),
        user.client_key.as_ref(),
        config_dir,
        "client key",
        true,
    )? {
        return Ok(data);
    }

    if explicit_key {
        Err(KubeAuthError::new("client key is required in kubeconfig"))
    } else {
        read_default_pem_required("admin_key.pem", "client key")
    }
}

fn resolve_data_field(
    data_field: Option<&String>,
    path_field: Option<&String>,
    config_dir: &Path,
    field_name: &str,
    required: bool,
) -> Result<Option<ResolvedData>, KubeAuthError> {
    if let Some(raw) = data_field {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            if required {
                return Err(KubeAuthError::new(format!(
                    "kubeconfig contains empty {field_name}"
                )));
            }
            return Ok(None);
        }
        let bytes = decode_base64_field(trimmed, field_name)?;
        return Ok(Some(ResolvedData {
            bytes,
            from_data_field: true,
            source_path: None,
        }));
    }

    if let Some(path) = path_field {
        let resolved = resolve_path(path, config_dir)?;
        let bytes = read_file_field(&resolved, field_name)?;
        return Ok(Some(ResolvedData {
            bytes,
            from_data_field: false,
            source_path: Some(resolved),
        }));
    }

    if required {
        Err(KubeAuthError::new(format!(
            "kubeconfig missing required {field_name}"
        )))
    } else {
        Ok(None)
    }
}

fn decode_base64_field(raw: &str, field_name: &str) -> Result<Vec<u8>, KubeAuthError> {
    BASE64
        .decode(raw)
        .map_err(|_| KubeAuthError::new(format!("failed to decode base64 for {field_name}")))
}

fn read_file_field(path: &Path, field_name: &str) -> Result<Vec<u8>, KubeAuthError> {
    fs::read(path).map_err(|err| {
        KubeAuthError::with_io(
            format!("failed to read {field_name} from {}", path.display()),
            err,
        )
    })
}

fn kube_default_pem_path(file_name: &str) -> Result<PathBuf, KubeAuthError> {
    let home = env::var("HOME").map_err(|_| {
        KubeAuthError::new("HOME environment variable is not set; cannot resolve default kube PEM")
    })?;
    Ok(PathBuf::from(home).join(".kube").join(file_name))
}

fn read_default_pem_optional(
    file_name: &str,
    description: &str,
) -> Result<(PathBuf, Option<ResolvedData>), KubeAuthError> {
    let path = kube_default_pem_path(file_name)?;
    match fs::read(&path) {
        Ok(bytes) => {
            let resolved_path = path.clone();
            Ok((
                path,
                Some(ResolvedData {
                    bytes,
                    from_data_field: false,
                    source_path: Some(resolved_path),
                }),
            ))
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok((path, None)),
        Err(err) => Err(KubeAuthError::with_io(
            format!(
                "failed to read default {description} from {}",
                path.display()
            ),
            err,
        )),
    }
}

fn read_default_pem_required(
    file_name: &str,
    description: &str,
) -> Result<ResolvedData, KubeAuthError> {
    let (path, data) = read_default_pem_optional(file_name, description)?;
    data.ok_or_else(|| {
        KubeAuthError::new(format!(
            "default {description} is missing at {}",
            path.display()
        ))
    })
}

fn resolve_path(path: &str, base_dir: &Path) -> Result<PathBuf, KubeAuthError> {
    let trimmed = path.trim();
    let expanded = if let Some(stripped) = trimmed.strip_prefix("~/") {
        let home = env::var("HOME").map_err(|_| {
            KubeAuthError::new(
                "HOME environment variable is not set; cannot expand '~' in kubeconfig path",
            )
        })?;
        PathBuf::from(home).join(stripped)
    } else if trimmed == "~" {
        let home = env::var("HOME").map_err(|_| {
            KubeAuthError::new(
                "HOME environment variable is not set; cannot expand '~' in kubeconfig path",
            )
        })?;
        PathBuf::from(home)
    } else {
        PathBuf::from(trimmed)
    };

    if expanded.is_absolute() {
        Ok(expanded)
    } else {
        Ok(base_dir.join(expanded))
    }
}

fn normalize_host_to_url(host: &str) -> String {
    if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("https://{}", host)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tempfile::TempDir;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        vars: HashMap<String, Option<String>>,
    }

    impl EnvGuard {
        fn new(keys: &[&str]) -> Self {
            let mut vars = HashMap::new();
            for key in keys {
                vars.insert(key.to_string(), env::var(key).ok());
                env::remove_var(key);
            }
            EnvGuard { vars }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.vars.drain() {
                if let Some(val) = value {
                    env::set_var(&key, val);
                } else {
                    env::remove_var(&key);
                }
            }
        }
    }

    fn write_kubeconfig(dir: &TempDir, ca_data: &str, cert_data: &str, key_data: &str) -> PathBuf {
        let config = format!(
            r#"
apiVersion: v1
clusters:
  - name: demo
    cluster:
      server: https://demo.example.test
      certificate-authority-data: "{}"
contexts:
  - name: demo
    context:
      cluster: demo
      user: demo-user
current-context: demo
users:
  - name: demo-user
    user:
      client-certificate-data: "{}"
      client-key-data: "{}"
"#,
            ca_data, cert_data, key_data
        );
        let path = dir.path().join("kubeconfig");
        fs::write(&path, config).expect("write kubeconfig");
        path
    }

    #[test]
    fn select_endpoint_prefers_env_over_host() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = EnvGuard::new(&[SERVER_ENV]);
        env::set_var(SERVER_ENV, "https://override.example");
        assert_eq!(env::var(SERVER_ENV).unwrap(), "https://override.example");
        let result = select_endpoint(Some("example.com"));
        assert_eq!(result, "https://override.example");
    }

    #[test]
    fn select_endpoint_normalizes_host() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = EnvGuard::new(&[SERVER_ENV]);
        let result = select_endpoint(Some("example.com"));
        assert_eq!(result, "https://example.com");
        let defaulted = select_endpoint(None);
        assert_eq!(defaulted, DEFAULT_SERVER_ENDPOINT.to_string());
    }

    #[test]
    fn load_kube_auth_reads_inline_data() {
        let guard = EnvGuard::new(&["KUBECONFIG", SERVER_ENV]);
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().expect("temp dir");
        let ca = BASE64.encode("ca-bytes");
        let cert = BASE64.encode("cert-bytes");
        let key = BASE64.encode("key-bytes");
        let kubeconfig = write_kubeconfig(&dir, &ca, &cert, &key);
        env::set_var("KUBECONFIG", &kubeconfig);

        let auth = load_kube_auth()
            .expect("load kubeauth")
            .expect("kube auth present");
        assert_eq!(auth.server, "https://demo.example.test");
        assert_eq!(auth.cluster_name, "demo");
        assert_eq!(auth.user_name, "demo-user");
        assert_eq!(auth.cert.bytes, b"cert-bytes");
        assert_eq!(auth.key.bytes, b"key-bytes");
        assert!(auth.server_override.is_none());
        drop(guard);
    }

    #[test]
    fn load_kube_auth_errors_on_empty_cert_field() {
        let guard = EnvGuard::new(&["KUBECONFIG", SERVER_ENV]);
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().expect("temp dir");
        let ca = BASE64.encode("ca");
        let cert = "%%%";
        let kubeconfig = write_kubeconfig(&dir, &ca, cert, "ZGVtbw==");
        env::set_var("KUBECONFIG", &kubeconfig);

        let err = match load_kube_auth() {
            Ok(_) => panic!("expected kube auth error"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("failed to decode base64 for client certificate"),
            "unexpected error: {err}"
        );
        drop(guard);
    }
}

#[derive(Deserialize)]
struct KubeConfig {
    #[serde(default)]
    clusters: Vec<NamedCluster>,
    #[serde(default)]
    users: Vec<NamedUser>,
    #[serde(default)]
    contexts: Vec<NamedContext>,
    #[serde(rename = "current-context")]
    current_context: Option<String>,
}

#[derive(Deserialize)]
struct NamedCluster {
    name: String,
    cluster: Cluster,
}

#[derive(Deserialize)]
struct Cluster {
    server: String,
    #[serde(rename = "certificate-authority-data")]
    certificate_authority_data: Option<String>,
    #[serde(rename = "certificate-authority")]
    certificate_authority: Option<String>,
}

#[derive(Deserialize)]
struct NamedUser {
    name: String,
    user: UserEntry,
}

#[derive(Deserialize)]
struct UserEntry {
    #[serde(rename = "client-certificate-data")]
    client_certificate_data: Option<String>,
    #[serde(rename = "client-certificate")]
    client_certificate: Option<String>,
    #[serde(rename = "client-key-data")]
    client_key_data: Option<String>,
    #[serde(rename = "client-key")]
    client_key: Option<String>,
}

#[derive(Deserialize)]
struct NamedContext {
    name: String,
    context: ContextEntry,
}

#[derive(Deserialize)]
struct ContextEntry {
    cluster: String,
    user: String,
}
