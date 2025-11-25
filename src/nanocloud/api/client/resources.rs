//! Resource HTTP APIs for `NanocloudClient`.

use super::auth::EphemeralCertificate;
use super::NanocloudClient;
use crate::nanocloud::api::types::{
    ApplyConflict, Bundle, BundleList, BundleSnapshotSource, BundleSpec, CaRequest,
    CertificateRequest, CertificateResponse, CertificateSpec, Device, DeviceList, DeviceSpec,
    NetworkPolicyDebugResponse, PodTable, ServiceActionResponse,
};
use crate::nanocloud::k8s::event::EventList;
use crate::nanocloud::k8s::pod::{ObjectMeta, Pod};
use crate::nanocloud::util::security::JsonTlsInfo;
use bytes::Bytes;
use reqwest::header::{ACCEPT_ENCODING, CONTENT_TYPE};
use reqwest::{StatusCode, Url};
use serde::de::DeserializeOwned;
use serde_json;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io;
use std::time::{Duration, SystemTime};
use tokio::time::sleep;

const POD_TABLE_ACCEPT: &str = "application/json;as=Table;g=meta.k8s.io;v=v1";
const RETRY_ATTEMPTS: usize = 3;
const RETRY_BACKOFF: Duration = Duration::from_millis(200);
const MAX_BACKOFF: Duration = Duration::from_secs(2);
const CERTIFICATE_API_VERSION: &str = "nanocloud.io/v1";
const CERTIFICATE_KIND: &str = "Certificate";
const BUNDLE_API_VERSION: &str = "nanocloud.io/v1";
const BUNDLE_KIND: &str = "Bundle";
const DEFAULT_NAMESPACE: &str = "default";
const DEFAULT_STREAM_TIMEOUT: Duration = Duration::from_secs(30);

/// Options that tweak bundle profile export behavior.
#[derive(Clone, Copy, Debug, Default)]
pub struct BundleExportOptions {
    /// Include encrypted secret payloads in the export artifact when supported.
    pub include_secrets: bool,
}

pub struct ApplyBundleOptions<'a> {
    pub payload: Bytes,
    pub content_type: &'a str,
    pub field_manager: &'a str,
    pub force: bool,
    pub dry_run: bool,
}

#[derive(Debug)]
pub struct HttpError {
    pub status: StatusCode,
    pub message: String,
    pub conflicts: Option<Vec<ApplyConflict>>,
}

impl HttpError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        HttpError {
            status,
            message: message.into(),
            conflicts: None,
        }
    }

    fn with_conflicts(
        status: StatusCode,
        message: impl Into<String>,
        conflicts: Vec<ApplyConflict>,
    ) -> Self {
        HttpError {
            status,
            message: message.into(),
            conflicts: Some(conflicts),
        }
    }

    pub fn conflicts(&self) -> Option<&[ApplyConflict]> {
        self.conflicts.as_deref()
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (status {})", self.message, self.status)
    }
}

impl Error for HttpError {}

impl NanocloudClient {
    fn default_namespace(namespace: Option<&str>) -> &str {
        namespace
            .filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT_NAMESPACE)
    }

    fn namespaced_segments<'a>(namespace: Option<&'a str>, tail: &[&'a str]) -> Vec<&'a str> {
        let ns = Self::default_namespace(namespace);
        let mut segments = Vec::with_capacity(2 + tail.len());
        segments.extend_from_slice(&["namespaces", ns]);
        segments.extend_from_slice(tail);
        segments
    }

    pub fn logs_segments<'a>(namespace: Option<&'a str>, service: &'a str) -> Vec<&'a str> {
        let mut segments = vec!["api", "v1"];
        if let Some(ns) = namespace.filter(|s| !s.is_empty()) {
            segments.extend(Self::namespaced_segments(Some(ns), &["pods"]));
        } else {
            segments.push("pods");
        }
        segments.push(service);
        segments.push("log");
        segments
    }

    pub fn pod_collection_segments(namespace: Option<&str>) -> Vec<&str> {
        let mut segments = vec!["api", "v1"];
        if let Some(ns) = namespace.filter(|s| !s.is_empty()) {
            segments.extend(Self::namespaced_segments(Some(ns), &["pods"]));
        } else {
            segments.push("pods");
        }
        segments
    }

    pub fn event_collection_segments(namespace: Option<&str>) -> Vec<&str> {
        let mut segments = vec!["api", "v1"];
        if let Some(ns) = namespace.filter(|s| !s.is_empty()) {
            segments.extend(Self::namespaced_segments(Some(ns), &["events"]));
        } else {
            segments.push("events");
        }
        segments
    }

    pub async fn list_pods_table(
        &self,
        namespace: Option<&str>,
    ) -> Result<PodTable, Box<dyn Error + Send + Sync>> {
        let segments = Self::pod_collection_segments(namespace);
        let url = self.url_from_segments(&segments)?;
        let request = self
            .client
            .get(url.clone())
            .query(&[("format", "table")])
            .header(reqwest::header::ACCEPT, POD_TABLE_ACCEPT);
        self.send_json("list pods table", &url, request).await
    }

    pub async fn issue_certificate(
        &self,
        common_name: &str,
        additional: Option<Vec<String>>,
    ) -> Result<JsonTlsInfo, Box<dyn Error + Send + Sync>> {
        let url = self.url_from_segments(&["v1", "ca"])?;
        let payload = CaRequest {
            common_name: common_name.to_string(),
            additional,
        };
        let response = self.client.post(url.clone()).json(&payload);
        self.send_json("issue certificate", &url, response).await
    }

    pub async fn request_ephemeral_certificate(
        &self,
        csr_pem: &str,
    ) -> Result<EphemeralCertificate, Box<dyn Error + Send + Sync>> {
        if self.bearer_token().is_none() {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "bootstrap token authentication is required to request an ephemeral certificate",
            )) as Box<dyn Error + Send + Sync>);
        }

        let csr = csr_pem.trim();
        if csr.is_empty() {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "certificate signing request must not be empty",
            )));
        }

        let url = self.url_from_segments(&["apis", "nanocloud.io", "v1", "certificates"])?;
        let payload = CertificateRequest {
            api_version: CERTIFICATE_API_VERSION.to_string(),
            kind: CERTIFICATE_KIND.to_string(),
            spec: CertificateSpec {
                csr_pem: csr.to_string(),
            },
        };

        let mut backoff = RETRY_BACKOFF;
        let mut last_err: Option<Box<dyn Error + Send + Sync>> = None;

        for attempt in 0..RETRY_ATTEMPTS {
            let request = self.client.post(url.clone()).json(&payload);
            let response = self.apply_auth(request).send().await;

            match response {
                Ok(resp) => match self
                    .handle_json::<CertificateResponse>("request ephemeral certificate", &url, resp)
                    .await
                {
                    Ok(body) => return convert_certificate_response(body),
                    Err(err) => match err.downcast::<HttpError>() {
                        Ok(http_err) => {
                            let status = http_err.status;
                            let boxed: Box<dyn Error + Send + Sync> = http_err;
                            if should_retry_status(status) && attempt + 1 < RETRY_ATTEMPTS {
                                last_err = Some(boxed);
                            } else {
                                return Err(boxed);
                            }
                        }
                        Err(other) => {
                            if attempt + 1 < RETRY_ATTEMPTS {
                                last_err = Some(other);
                            } else {
                                return Err(other);
                            }
                        }
                    },
                },
                Err(err) => {
                    if is_retryable_reqwest(&err) && attempt + 1 < RETRY_ATTEMPTS {
                        last_err = Some(Box::new(err));
                    } else {
                        return Err(Box::new(err));
                    }
                }
            }

            if attempt + 1 < RETRY_ATTEMPTS {
                sleep(backoff).await;
                backoff = next_backoff(backoff);
            }
        }

        Err(last_err.unwrap_or_else(|| {
            Box::new(HttpError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("request certificate {} failed", url),
            ))
        }))
    }

    #[allow(dead_code)] // TODO(api-devices): wire into device management CLI (see docs/refactor.md).
    pub async fn list_devices(
        &self,
        namespace: Option<&str>,
    ) -> Result<DeviceList, Box<dyn Error + Send + Sync>> {
        let ns = Self::default_namespace(namespace);
        let url =
            self.url_from_segments(&["apis", "nanocloud.io", "v1", "namespaces", ns, "devices"])?;
        let request = self.client.get(url.clone());
        self.send_json("list devices", &url, request).await
    }

    #[allow(dead_code)] // TODO(api-devices): wire into device management CLI (see docs/refactor.md).
    pub async fn get_device(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Device, Box<dyn Error + Send + Sync>> {
        let ns = Self::default_namespace(Some(namespace));
        let url = self.url_from_segments(&[
            "apis",
            "nanocloud.io",
            "v1",
            "namespaces",
            ns,
            "devices",
            name,
        ])?;
        let request = self.client.get(url.clone());
        self.send_json("get device", &url, request).await
    }

    #[allow(dead_code)] // TODO(api-devices): wire into device management CLI (see docs/refactor.md).
    pub async fn create_device(
        &self,
        namespace: Option<&str>,
        hash: &str,
        description: Option<&str>,
    ) -> Result<Device, Box<dyn Error + Send + Sync>> {
        let trimmed = hash.trim();
        if trimmed.is_empty() {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "device hash must not be empty",
            )) as Box<dyn Error + Send + Sync>);
        }

        let ns = namespace.filter(|s| !s.is_empty()).unwrap_or("default");
        let metadata = ObjectMeta {
            name: Some(format!("device-{trimmed}")),
            namespace: Some(ns.to_string()),
            ..Default::default()
        };
        let spec = DeviceSpec {
            hash: trimmed.to_string(),
            certificate_subject: format!("device:{trimmed}"),
            description: description.map(|value| value.to_string()),
        };
        let payload = Device {
            api_version: "nanocloud.io/v1".to_string(),
            kind: "Device".to_string(),
            metadata,
            spec,
            status: None,
        };

        let url =
            self.url_from_segments(&["apis", "nanocloud.io", "v1", "namespaces", ns, "devices"])?;
        let request = self.client.post(url.clone()).json(&payload);
        self.send_json("create device", &url, request).await
    }

    #[allow(dead_code)] // TODO(api-devices): wire into device management CLI (see docs/refactor.md).
    pub async fn delete_device(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Device, Box<dyn Error + Send + Sync>> {
        let ns = Self::default_namespace(Some(namespace));
        let url = self.url_from_segments(&[
            "apis",
            "nanocloud.io",
            "v1",
            "namespaces",
            ns,
            "devices",
            name,
        ])?;
        let request = self.client.delete(url.clone());
        self.send_json("delete device", &url, request).await
    }

    #[allow(dead_code)] // TODO(api-devices): wire into device management CLI (see docs/refactor.md).
    pub async fn issue_device_certificate(
        &self,
        namespace: &str,
        csr_pem: &str,
    ) -> Result<CertificateResponse, Box<dyn Error + Send + Sync>> {
        let csr = csr_pem.trim();
        if csr.is_empty() {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "certificate signing request must not be empty",
            )) as Box<dyn Error + Send + Sync>);
        }

        let ns = if namespace.is_empty() {
            "default"
        } else {
            namespace
        };
        let url = self.url_from_segments(&[
            "apis",
            "nanocloud.io",
            "v1",
            "namespaces",
            ns,
            "devices",
            "certificates",
        ])?;

        let payload = CertificateRequest {
            api_version: CERTIFICATE_API_VERSION.to_string(),
            kind: CERTIFICATE_KIND.to_string(),
            spec: CertificateSpec {
                csr_pem: csr.to_string(),
            },
        };

        let request = self.client.post(url.clone()).json(&payload);
        self.send_json("issue device certificate", &url, request)
            .await
    }

    pub async fn create_bundle(
        &self,
        namespace: Option<&str>,
        service: &str,
        options: HashMap<String, String>,
        snapshot: Option<&str>,
        start: bool,
        update: bool,
    ) -> Result<Bundle, Box<dyn Error + Send + Sync>> {
        let namespace_value = namespace.filter(|s| !s.is_empty());
        let path_namespace = Self::default_namespace(namespace);
        let metadata = ObjectMeta {
            name: Some(service.to_string()),
            namespace: Some(path_namespace.to_string()),
            ..Default::default()
        };

        let spec = BundleSpec {
            service: service.to_string(),
            namespace: namespace_value.map(|ns| ns.to_string()),
            options,
            profile_key: None,
            snapshot: snapshot.map(|path| BundleSnapshotSource {
                source: path.to_string(),
                media_type: None,
            }),
            start,
            update,
            security: None,
            runtime: None,
        };

        let payload = Bundle {
            api_version: BUNDLE_API_VERSION.to_string(),
            kind: BUNDLE_KIND.to_string(),
            metadata,
            spec,
            status: None,
        };

        let url = self.url_from_segments(&[
            "apis",
            "nanocloud.io",
            "v1",
            "namespaces",
            path_namespace,
            "bundles",
        ])?;
        let request = self.client.post(url.clone()).json(&payload);
        self.send_json("create bundle", &url, request).await
    }

    /// Apply a bundle manifest to the cluster, respecting default namespace and field manager.
    pub async fn apply_bundle(
        &self,
        namespace: Option<&str>,
        service: &str,
        options: ApplyBundleOptions<'_>,
    ) -> Result<Bundle, Box<dyn Error + Send + Sync>> {
        let trimmed_manager = options.field_manager.trim();
        if trimmed_manager.is_empty() {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "field manager is required for apply operations",
            )));
        }

        let segments = Self::bundle_segments(namespace, service);
        let mut url = self.url_from_segments(&segments)?;
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("fieldManager", trimmed_manager);
            if options.force {
                pairs.append_pair("force", "true");
            }
            if options.dry_run {
                pairs.append_pair("dryRun", "true");
            }
        }

        let request = self
            .client
            .patch(url.clone())
            .header(CONTENT_TYPE, options.content_type)
            .body(options.payload.clone());
        self.send_json("apply bundle", &url, request).await
    }

    pub async fn list_bundles(
        &self,
        namespace: Option<&str>,
    ) -> Result<BundleList, Box<dyn Error + Send + Sync>> {
        let segments = Self::bundle_collection_segments(namespace);
        let url = self.url_from_segments(&segments)?;
        let request = self.client.get(url.clone());
        self.send_json("list bundles", &url, request).await
    }

    pub async fn get_bundle(
        &self,
        namespace: Option<&str>,
        service: &str,
    ) -> Result<Option<Bundle>, Box<dyn Error + Send + Sync>> {
        let segments = Self::bundle_segments(namespace, service);
        let url = self.url_from_segments(&segments)?;
        let response = self.apply_auth(self.client.get(url.clone())).send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = self
            .handle_stream_error("get bundle", &url, response)
            .await?;
        let bundle = response.json::<Bundle>().await?;
        Ok(Some(bundle))
    }

    pub async fn uninstall_bundle(
        &self,
        namespace: Option<&str>,
        service: &str,
    ) -> Result<ServiceActionResponse, Box<dyn Error + Send + Sync>> {
        self.bundle_action(namespace, service, "uninstall").await
    }

    pub async fn delete_bundle(
        &self,
        namespace: Option<&str>,
        service: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let ns = Self::default_namespace(namespace);
        let url = self.url_from_segments(&[
            "apis",
            "nanocloud.io",
            "v1",
            "namespaces",
            ns,
            "bundles",
            service,
        ])?;
        let response = self
            .apply_auth(self.client.delete(url.clone()))
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        self.handle_stream_error("delete bundle", &url, response)
            .await
            .map(|_| ())
    }

    pub async fn start_bundle(
        &self,
        namespace: Option<&str>,
        service: &str,
    ) -> Result<ServiceActionResponse, Box<dyn Error + Send + Sync>> {
        self.bundle_action(namespace, service, "start").await
    }

    pub async fn stop_bundle(
        &self,
        namespace: Option<&str>,
        service: &str,
    ) -> Result<ServiceActionResponse, Box<dyn Error + Send + Sync>> {
        self.bundle_action(namespace, service, "stop").await
    }

    pub async fn restart_bundle(
        &self,
        namespace: Option<&str>,
        service: &str,
    ) -> Result<ServiceActionResponse, Box<dyn Error + Send + Sync>> {
        self.bundle_action(namespace, service, "restart").await
    }

    async fn bundle_action(
        &self,
        namespace: Option<&str>,
        service: &str,
        action: &str,
    ) -> Result<ServiceActionResponse, Box<dyn Error + Send + Sync>> {
        let ns = Self::default_namespace(namespace);
        let action_segment = action.to_string();
        let url = self.url_from_segments(&[
            "apis",
            "nanocloud.io",
            "v1",
            "namespaces",
            ns,
            "bundles",
            service,
            "actions",
            action_segment.as_str(),
        ])?;
        let request = self.client.post(url.clone());
        self.send_json("bundle action", &url, request).await
    }

    pub async fn latest_bundle_backup(
        &self,
        namespace: Option<&str>,
        service: &str,
    ) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
        let ns = Self::default_namespace(namespace);
        let url = self.url_from_segments(&[
            "apis",
            "nanocloud.io",
            "v1",
            "namespaces",
            ns,
            "bundles",
            service,
            "backups",
            "latest",
        ])?;
        let response = self
            .send_stream(
                "latest bundle backup",
                &url,
                self.client.get(url.clone()),
                Some(DEFAULT_STREAM_TIMEOUT),
            )
            .await?;
        Ok(response)
    }

    pub async fn export_bundle_profile(
        &self,
        namespace: Option<&str>,
        service: &str,
        options: BundleExportOptions,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let owned_segments = Self::bundle_export_segments(namespace, service);
        let segment_refs: Vec<&str> = owned_segments
            .iter()
            .map(|segment| segment.as_str())
            .collect();
        let mut url = self.url_from_segments(&segment_refs)?;
        if options.include_secrets {
            url.query_pairs_mut()
                .append_pair("includeSecrets", Self::bool_query_value(true));
        }
        let response = self
            .send_stream(
                "export bundle profile",
                &url,
                self.client.post(url.clone()),
                Some(DEFAULT_STREAM_TIMEOUT),
            )
            .await?;
        let body = response.bytes().await?;
        Ok(body.to_vec())
    }

    pub async fn network_policy_debug(
        &self,
    ) -> Result<NetworkPolicyDebugResponse, Box<dyn Error + Send + Sync>> {
        let url = self.url_from_segments(&["v1", "networkpolicies", "debug"])?;
        let request = self.client.get(url.clone());
        self.send_json("network policy debug", &url, request).await
    }

    /// Stream logs for a pod/service with optional long-running follow and bounded timeout.
    pub async fn logs_stream(
        &self,
        namespace: Option<&str>,
        service: &str,
        follow: bool,
    ) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
        let segments = Self::logs_segments(namespace, service);
        let url = self.url_from_segments(&segments)?;
        let mut request = self.client.get(url.clone());
        let timeout = if follow {
            request = request.query(&[("follow", follow)]);
            Some(Duration::from_secs(60 * 60 * 24 * 365))
        } else {
            Some(DEFAULT_STREAM_TIMEOUT)
        };
        self.send_stream("logs stream", &url, request, timeout)
            .await
    }

    pub async fn get_pod(
        &self,
        namespace: Option<&str>,
        pod: &str,
    ) -> Result<Option<Pod>, Box<dyn Error + Send + Sync>> {
        let segments = Self::pod_segments(namespace, pod);
        let url = self.url_from_segments(&segments)?;
        let response = self.apply_auth(self.client.get(url.clone())).send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let pod = self.handle_json("get pod", &url, response).await?;
        Ok(Some(pod))
    }

    pub async fn watch_pod(
        &self,
        namespace: Option<&str>,
        pod: &str,
        timeout_seconds: Option<u64>,
        resource_version: Option<&str>,
    ) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
        let segments = Self::pod_collection_segments(namespace);
        let url = self.url_from_segments(&segments)?;

        let mut query: Vec<(&str, String)> = Vec::new();
        query.push(("watch", "true".to_string()));
        query.push(("fieldSelector", format!("metadata.name={pod}")));
        if let Some(timeout) = timeout_seconds {
            query.push(("timeoutSeconds", timeout.to_string()));
        }
        if let Some(rv) = resource_version {
            query.push(("resourceVersion", rv.to_string()));
        }

        let query_pairs: Vec<(&str, &str)> = query.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let request = self.client.get(url.clone()).query(&query_pairs);
        // Use only the server-side watch timeout; a client deadline can abort chunked bodies and
        // surface decode errors even when the server is behaving correctly.
        self.send_stream("watch pod", &url, request, None).await
    }

    pub async fn list_events(
        &self,
        namespace: Option<&str>,
        query: &EventQuery,
    ) -> Result<EventList, Box<dyn Error + Send + Sync>> {
        let segments = Self::event_collection_segments(namespace);
        let url = self.url_from_segments(&segments)?;
        let mut params: Vec<(String, String)> = Vec::new();

        if let Some(limit) = query.limit {
            if limit > 0 {
                params.push(("limit".to_string(), limit.to_string()));
            }
        }

        if let Some(since) = query
            .since
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            params.push(("since".to_string(), since.to_string()));
        }

        if let Some(level) = query.level {
            params.push(("level".to_string(), level.as_str().to_string()));
        }

        if !query.reasons.is_empty() {
            let joined = query
                .reasons
                .iter()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
                .join(",");
            if !joined.is_empty() {
                params.push(("reason".to_string(), joined));
            }
        }

        if let Some(selector) = query.build_field_selector() {
            params.push(("fieldSelector".to_string(), selector));
        }

        let request = if params.is_empty() {
            self.client.get(url.clone())
        } else {
            let pairs: Vec<(&str, &str)> = params
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            self.client.get(url.clone()).query(&pairs)
        };

        let response = self.apply_auth(request).send().await?;
        self.handle_json("list events", &url, response).await
    }

    /// Watch events with server-side field selectors and bounded timeout to avoid hangs.
    pub async fn watch_events(
        &self,
        namespace: Option<&str>,
        query: &EventQuery,
    ) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
        let segments = Self::event_collection_segments(namespace);
        let url = self.url_from_segments(&segments)?;

        let mut params: Vec<(String, String)> = Vec::new();
        params.push(("watch".to_string(), "true".to_string()));
        if let Some(selector) = query.build_field_selector() {
            params.push(("fieldSelector".to_string(), selector));
        }

        if let Some(limit) = query.limit {
            if limit > 0 {
                params.push(("limit".to_string(), limit.to_string()));
            }
        }

        if let Some(since) = query
            .since
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            params.push(("since".to_string(), since.to_string()));
        }

        if let Some(rv) = query.resource_version.as_deref() {
            params.push(("resourceVersion".to_string(), rv.to_string()));
            params.push((
                "resourceVersionMatch".to_string(),
                "NotOlderThan".to_string(),
            ));
        }

        params.push(("allowWatchBookmarks".to_string(), "true".to_string()));
        let timeout = query.timeout_seconds.unwrap_or(30);
        params.push(("timeoutSeconds".to_string(), timeout.to_string()));

        let pairs: Vec<(&str, &str)> = params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let request = self.client.get(url.clone()).query(&pairs);
        // Avoid client-side timeouts for watches; rely on the API server to end the stream.
        self.send_stream("watch events", &url, request, None).await
    }
}

impl NanocloudClient {
    pub fn pod_segments<'a>(namespace: Option<&'a str>, pod: &'a str) -> Vec<&'a str> {
        let mut segments = vec!["api", "v1"];
        segments.extend(Self::namespaced_segments(namespace, &["pods"]));
        segments.push(pod);
        segments
    }

    #[allow(clippy::needless_lifetimes)]
    pub fn bundle_collection_segments<'a>(namespace: Option<&'a str>) -> Vec<&'a str> {
        let ns = Self::default_namespace(namespace);
        vec!["apis", "nanocloud.io", "v1", "namespaces", ns, "bundles"]
    }

    #[allow(clippy::needless_lifetimes)]
    pub fn bundle_segments<'a>(namespace: Option<&'a str>, name: &'a str) -> Vec<&'a str> {
        let mut segments = Self::bundle_collection_segments(namespace);
        segments.push(name);
        segments
    }

    pub fn bundle_export_segments(namespace: Option<&str>, name: &str) -> Vec<String> {
        let mut segments = Self::bundle_collection_segments(namespace)
            .into_iter()
            .map(|segment| segment.to_string())
            .collect::<Vec<_>>();
        segments.push(name.to_string());
        segments.push("exportProfile".to_string());
        segments
    }

    async fn handle_json<T>(
        &self,
        operation: &str,
        url: &Url,
        response: reqwest::Response,
    ) -> Result<T, Box<dyn Error + Send + Sync>>
    where
        T: DeserializeOwned,
    {
        let status = response.status();
        if status.is_success() {
            let body = response.json::<T>().await?;
            return Ok(body);
        }

        let text = response.text().await.unwrap_or_default();
        if let Ok(parsed) = serde_json::from_str::<crate::nanocloud::api::types::ErrorBody>(&text) {
            let message = parsed.message.or(parsed.reason).unwrap_or_else(|| {
                status
                    .canonical_reason()
                    .unwrap_or("request failed")
                    .to_string()
            });
            let err = match parsed.conflicts {
                Some(conflicts) if !conflicts.is_empty() => HttpError::with_conflicts(
                    status,
                    format!("{operation} {}: {message}", url),
                    conflicts,
                ),
                _ => HttpError::new(status, format!("{operation} {}: {message}", url)),
            };
            return Err(Box::new(err));
        }

        let message = if text.is_empty() {
            status
                .canonical_reason()
                .unwrap_or("request failed")
                .to_string()
        } else {
            text
        };

        Err(Box::new(HttpError::new(
            status,
            format!("{operation} {}: {message}", url),
        )))
    }

    async fn send_json<T>(
        &self,
        operation: &str,
        url: &Url,
        request: reqwest::RequestBuilder,
    ) -> Result<T, Box<dyn Error + Send + Sync>>
    where
        T: DeserializeOwned,
    {
        let response = self.apply_auth(request).send().await?;
        self.handle_json(operation, url, response).await
    }

    async fn handle_stream_error(
        &self,
        operation: &str,
        url: &Url,
        response: reqwest::Response,
    ) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }

        let text = response.text().await.unwrap_or_default();
        if let Ok(parsed) = serde_json::from_str::<crate::nanocloud::api::types::ErrorBody>(&text) {
            let message = parsed.message.or(parsed.reason).unwrap_or_else(|| {
                status
                    .canonical_reason()
                    .unwrap_or("request failed")
                    .to_string()
            });
            let err = match parsed.conflicts {
                Some(conflicts) if !conflicts.is_empty() => HttpError::with_conflicts(
                    status,
                    format!("{operation} {}: {message}", url),
                    conflicts,
                ),
                _ => HttpError::new(status, format!("{operation} {}: {message}", url)),
            };
            return Err(Box::new(err));
        }

        let message = if text.is_empty() {
            status
                .canonical_reason()
                .unwrap_or("request failed")
                .to_string()
        } else {
            text
        };

        Err(Box::new(HttpError::new(
            status,
            format!("{operation} {}: {message}", url),
        )))
    }

    async fn send_stream(
        &self,
        operation: &str,
        url: &Url,
        request: reqwest::RequestBuilder,
        timeout: Option<Duration>,
    ) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
        // Disable HTTP compression for streaming endpoints. Some proxies mark watch/log streams as
        // gzip-encoded but terminate them early when the watch times out, which surfaces as
        // `error decoding response body` on the client. Streams are small enough that skipping
        // compression avoids that failure mode.
        let request = request.header(ACCEPT_ENCODING, "identity");
        let request = if let Some(duration) = timeout {
            request.timeout(duration)
        } else {
            request
        };
        let response = self.apply_auth(request).send().await?;
        self.handle_stream_error(operation, url, response).await
    }
}

fn should_retry_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn is_retryable_reqwest(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect()
}

fn next_backoff(current: Duration) -> Duration {
    current
        .checked_mul(2)
        .unwrap_or(MAX_BACKOFF)
        .min(MAX_BACKOFF)
}

fn parse_timestamp(value: &str) -> Result<SystemTime, chrono::ParseError> {
    chrono::DateTime::parse_from_rfc3339(value).map(|dt| dt.with_timezone(&chrono::Utc).into())
}

fn sanitize_pem(pem: &str, label: &str) -> Result<Vec<u8>, io::Error> {
    let trimmed = pem.trim();
    if trimmed.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label} payload is empty"),
        ));
    }
    if !trimmed.starts_with("-----BEGIN") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label} is not PEM encoded"),
        ));
    }
    Ok(trimmed.as_bytes().to_vec())
}

fn convert_certificate_response(
    response: CertificateResponse,
) -> Result<EphemeralCertificate, Box<dyn Error + Send + Sync>> {
    let certificate = sanitize_pem(&response.status.certificate_pem, "certificate")
        .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
    let ca_bundle = sanitize_pem(&response.status.ca_bundle_pem, "CA bundle")
        .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
    let expiration = match response.status.expiration_timestamp.as_deref() {
        Some(ts) if !ts.trim().is_empty() => {
            Some(parse_timestamp(ts).map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?)
        }
        _ => None,
    };
    Ok(EphemeralCertificate {
        certificate,
        ca_bundle,
        expiration,
    })
}

#[derive(Clone, Debug, Default)]
pub struct EventQuery {
    pub bundle: Option<String>,
    pub limit: Option<u32>,
    pub since: Option<String>,
    pub resource_version: Option<String>,
    pub timeout_seconds: Option<u64>,
    pub level: Option<EventLevel>,
    pub reasons: Vec<String>,
}

impl EventQuery {
    fn build_field_selector(&self) -> Option<String> {
        let mut selectors: Vec<String> = Vec::new();
        if let Some(bundle) = self
            .bundle
            .as_ref()
            .map(|value| value.trim())
            .filter(|v| !v.is_empty())
        {
            selectors.push(format!("involvedObject.name={bundle}"));
        }
        if selectors.is_empty() {
            None
        } else {
            Some(selectors.join(","))
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum EventLevel {
    Normal,
    Warning,
}

impl EventLevel {
    fn as_str(&self) -> &'static str {
        match self {
            EventLevel::Normal => "Normal",
            EventLevel::Warning => "Warning",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::api::client::auth::AuthContext;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    fn test_client(base_url: Url) -> NanocloudClient {
        NanocloudClient {
            client: reqwest::Client::new(),
            base_url,
            auth: AuthContext::BearerToken {
                token: "test".to_string(),
            },
            exec_connector: Arc::new(Mutex::new(None)),
        }
    }

    async fn spawn_server(
        status: StatusCode,
        body: &'static str,
    ) -> (SocketAddr, oneshot::Sender<()>) {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("local addr");
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let status_line = format!(
            "{} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("UNKNOWN")
        );
        let body_owned = body.to_string();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => {
                        break;
                    }
                    accept_result = listener.accept() => {
                        let Ok((mut stream, _)) = accept_result else {
                            break;
                        };
                        let response = format!(
                            "HTTP/1.1 {status_line}\r\ncontent-length: {}\r\ncontent-type: application/json\r\n\r\n{body}",
                            body_owned.len()
                        );
                        let _ = stream.write_all(response.as_bytes()).await;
                    }
                }
            }
        });

        (addr, shutdown_tx)
    }

    #[tokio::test]
    async fn stream_error_returns_http_error_with_conflicts() {
        let (addr, shutdown) = spawn_server(
            StatusCode::CONFLICT,
            r#"{"apiVersion":"v1","kind":"Status","status":"Failure","message":"boom","conflicts":[{"path":"spec","existingManager":"test"}]}"#,
        )
        .await;
        let url = Url::parse(&format!("http://{addr}/test")).unwrap();
        let client = test_client(url.clone());

        let err = client
            .send_stream("test op", &url, client.client.get(url.clone()), None)
            .await
            .expect_err("expected conflict error");
        let http_err = err
            .downcast::<HttpError>()
            .expect("should downcast to HttpError");
        assert_eq!(http_err.status, StatusCode::CONFLICT);
        assert!(http_err.conflicts().is_some());

        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn stream_error_defaults_message_on_empty_body() {
        let (addr, shutdown) = spawn_server(StatusCode::INTERNAL_SERVER_ERROR, "").await;
        let url = Url::parse(&format!("http://{addr}/test")).unwrap();
        let client = test_client(url.clone());

        let err = client
            .send_stream("empty body op", &url, client.client.get(url.clone()), None)
            .await
            .expect_err("expected error");
        let http_err = err
            .downcast::<HttpError>()
            .expect("should downcast to HttpError");
        assert_eq!(http_err.status, StatusCode::INTERNAL_SERVER_ERROR);

        let _ = shutdown.send(());
    }

    #[test]
    fn path_helpers_apply_default_namespace() {
        let bundles = NanocloudClient::bundle_collection_segments(None);
        assert_eq!(
            bundles,
            vec![
                "apis",
                "nanocloud.io",
                "v1",
                "namespaces",
                "default",
                "bundles"
            ]
        );
        let logs = NanocloudClient::logs_segments(None, "svc");
        assert_eq!(logs, vec!["api", "v1", "pods", "svc", "log"]);
    }

    #[test]
    fn event_query_builds_field_selector() {
        let mut query = EventQuery::default();
        assert!(query.build_field_selector().is_none());
        query.bundle = Some("svc ".into());
        assert_eq!(
            query.build_field_selector().as_deref(),
            Some("involvedObject.name=svc")
        );
    }
}
