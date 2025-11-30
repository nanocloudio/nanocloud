//! Nanocloud API client surface split into focused auth, exec, and resource submodules.
//
// The public API remains the same while the implementation is decomposed for clarity.

mod auth;
mod exec;
mod resources;
#[doc(hidden)]
pub mod test_support;

pub use auth::{CurlAuthData, EphemeralCertificate, KubeAuthError, KubeFieldSource};
pub use exec::ExecRequest;
pub use resources::{ApplyBundleOptions, BundleExportOptions, EventLevel, EventQuery, HttpError};

use auth::{
    build_reqwest_identity, extract_certificate_owner, load_kube_auth, select_endpoint,
    AuthContext, CertificateAuth,
};
use openssl::ssl::SslConnector;
use reqwest::tls::Certificate;
use reqwest::{Client, Url};
use std::error::Error;
use std::io;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct NanocloudClient {
    pub(super) client: Client,
    pub(super) base_url: Url,
    auth: AuthContext,
    exec_connector: Arc<Mutex<Option<Arc<SslConnector>>>>,
}

impl NanocloudClient {
    pub fn new() -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::from_kube_auth(load_kube_auth().map_err(to_boxed_error)?)
    }

    pub fn with_bearer(host: &str, token: String) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let token_trimmed = token.trim();
        if token_trimmed.is_empty() {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "bootstrap token must not be empty",
            )) as Box<dyn Error + Send + Sync>);
        }
        let endpoint = select_endpoint(Some(host));
        let base_url = Url::parse(&endpoint)?;
        let builder = Client::builder()
            .http1_only()
            .danger_accept_invalid_certs(true);

        let client = builder.build().map_err(|err| {
            io::Error::other(format!("failed to construct Nanocloud HTTP client: {err}"))
        })?;

        Ok(NanocloudClient {
            client,
            base_url,
            auth: AuthContext::BearerToken {
                token: token_trimmed.to_string(),
            },
            exec_connector: Arc::new(Mutex::new(None)),
        })
    }

    fn from_kube_auth(
        kube_auth: Option<auth::KubeAuth>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let kube_auth = kube_auth.ok_or_else(|| {
            Box::new(io::Error::new(
                io::ErrorKind::NotFound,
                "kubeconfig is required for this command",
            )) as Box<dyn Error + Send + Sync>
        })?;

        let endpoint = kube_auth
            .server_override
            .unwrap_or_else(|| kube_auth.server.clone());
        let base_url = Url::parse(&endpoint)?;

        let identity = build_reqwest_identity(&kube_auth.cert.bytes, &kube_auth.key.bytes)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;

        let ca_bytes = kube_auth.ca.as_ref().map(|data| data.bytes.clone());

        let mut client_builder = Client::builder().identity(identity).http1_only();

        if let Some(bytes) = ca_bytes.as_ref() {
            let ca_certificate = Certificate::from_pem(bytes)?;
            client_builder = client_builder.add_root_certificate(ca_certificate);
        }

        let client = client_builder.build().map_err(|err| {
            io::Error::other(format!("failed to construct Nanocloud HTTP client: {err}"))
        })?;

        let tls = auth::ClientTls {
            client_certificate: kube_auth.cert.bytes.clone(),
            client_key: kube_auth.key.bytes.clone(),
            ca_bundle: ca_bytes.clone(),
        };

        let owner = extract_certificate_owner(&kube_auth.cert.bytes).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to derive owner from client certificate: {err}"),
            )
        })?;

        let curl_auth = CurlAuthData {
            kubeconfig_path: kube_auth.kubeconfig_path.clone(),
            kubeconfig_dir: kube_auth.kubeconfig_dir.clone(),
            cluster_name: kube_auth.cluster_name.clone(),
            user_name: kube_auth.user_name.clone(),
            ca_source: kube_auth.ca.as_ref().map(|ca| {
                if ca.from_data_field {
                    KubeFieldSource::InlineData
                } else {
                    KubeFieldSource::FilePath
                }
            }),
            ca_path: kube_auth.ca.as_ref().and_then(|ca| ca.source_path.clone()),
            ca_data: kube_auth.ca.as_ref().map(|ca| ca.bytes.clone()),
            cert_source: if kube_auth.cert.from_data_field {
                KubeFieldSource::InlineData
            } else {
                KubeFieldSource::FilePath
            },
            cert_path: kube_auth.cert.source_path.clone(),
            cert_data: kube_auth.cert.bytes.clone(),
            key_source: if kube_auth.key.from_data_field {
                KubeFieldSource::InlineData
            } else {
                KubeFieldSource::FilePath
            },
            key_path: kube_auth.key.source_path.clone(),
            key_data: kube_auth.key.bytes.clone(),
        };

        Ok(NanocloudClient {
            client,
            base_url,
            auth: AuthContext::ClientCertificate(Box::new(CertificateAuth {
                tls,
                owner,
                curl: curl_auth,
            })),
            exec_connector: Arc::new(Mutex::new(None)),
        })
    }

    pub fn curl_identity(&self) -> Option<&CurlAuthData> {
        match &self.auth {
            AuthContext::ClientCertificate(ctx) => Some(&ctx.curl),
            _ => None,
        }
    }

    pub fn owner(&self) -> Option<&str> {
        match &self.auth {
            AuthContext::ClientCertificate(ctx) => Some(ctx.owner.as_str()),
            _ => None,
        }
    }

    pub fn bearer_token(&self) -> Option<&str> {
        match &self.auth {
            AuthContext::BearerToken { token } => Some(token.as_str()),
            _ => None,
        }
    }

    fn apply_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            AuthContext::ClientCertificate(ctx) => request.header("X-Nanocloud-Owner", &ctx.owner),
            AuthContext::BearerToken { token } => request.bearer_auth(token),
        }
    }

    fn certificate_auth(&self) -> Result<&CertificateAuth, Box<dyn Error + Send + Sync>> {
        match &self.auth {
            AuthContext::ClientCertificate(ctx) => Ok(ctx.as_ref()),
            _ => Err(Box::new(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "client certificate is required for this operation",
            )) as Box<dyn Error + Send + Sync>),
        }
    }

    pub fn url_from_segments(
        &self,
        segments: &[&str],
    ) -> Result<Url, Box<dyn Error + Send + Sync>> {
        let mut url = self.base_url.clone();
        {
            let mut parts = url
                .path_segments_mut()
                .map_err(|_| "base URL cannot be base for segments")?;
            parts.clear();
            for segment in segments {
                if !segment.is_empty() {
                    parts.push(segment);
                }
            }
        }
        Ok(url)
    }

    #[inline]
    pub(super) fn bool_query_value(value: bool) -> &'static str {
        if value {
            "true"
        } else {
            "false"
        }
    }

    pub(super) fn exec_tls_connector(
        &self,
    ) -> Result<Arc<SslConnector>, Box<dyn Error + Send + Sync>> {
        {
            if let Some(connector) = self.exec_connector.lock().unwrap().as_ref() {
                return Ok(Arc::clone(connector));
            }
        }

        let certificate_auth = self.certificate_auth()?;
        let tls = certificate_auth.tls.clone();
        let connector = auth::build_exec_ssl_connector(&tls)
            .map(Arc::new)
            .map_err(to_boxed_error)?;

        let mut guard = self.exec_connector.lock().unwrap();
        guard.get_or_insert_with(|| Arc::clone(&connector));
        Ok(connector)
    }
}

fn to_boxed_error(err: KubeAuthError) -> Box<dyn Error + Send + Sync> {
    Box::new(err)
}
