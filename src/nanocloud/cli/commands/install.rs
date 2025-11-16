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

use std::collections::HashMap;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use futures_util::StreamExt;
use reqwest::StatusCode;
use tokio::fs;
use tokio::io::{self as tokio_io, AsyncWriteExt};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use crate::nanocloud::api::client::{BundleExportOptions, HttpError, NanocloudClient};
use crate::nanocloud::api::types::{Bundle, BundleSnapshotSource, BundleSpec};
use crate::nanocloud::cli::args::{InstallArgs, UninstallArgs};
use crate::nanocloud::cli::curl::print_curl_request;
use crate::nanocloud::cli::output::service_display_name;
use crate::nanocloud::cli::Terminal;
use crate::nanocloud::k8s::pod::{ObjectMeta, Pod};
use crate::nanocloud::kubelet::WatchEvent;
use crate::nanocloud::util::security::{SecureAssets, VolumeKeyMetadata};
use crate::nanocloud::Config;

pub(super) async fn handle_install(
    client: &NanocloudClient,
    args: &InstallArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let namespace = args.namespace.as_deref();
    let namespace = namespace.filter(|value| !value.is_empty());
    let display = service_display_name(namespace, &args.service);
    let options: HashMap<String, String> = args.option.iter().cloned().collect();

    if args.curl {
        let request = bundle_payload(
            namespace,
            &args.service,
            options.clone(),
            args.snapshot.as_deref(),
            true,
            args.update,
        );
        let body = serde_json::to_string_pretty(&request)?;
        let ns_segment = namespace.unwrap_or("default");
        let url = client
            .url_from_segments(&[
                "apis",
                "nanocloud.io",
                "v1",
                "namespaces",
                ns_segment,
                "bundles",
            ])?
            .to_string();
        print_curl_request(client, "POST", &url, Some(&body))?;

        let workload_name = workload_name(namespace, &args.service);

        let log_segments = NanocloudClient::logs_segments(namespace, &args.service);
        let mut logs_url = client.url_from_segments(&log_segments)?;
        {
            let mut pairs = logs_url.query_pairs_mut();
            pairs.append_pair("follow", "true");
        }
        print_curl_request(client, "GET", logs_url.as_ref(), None)?;

        let mut watch_segments = vec!["api", "v1"];
        if let Some(ns) = namespace {
            watch_segments.extend(["namespaces", ns, "pods"]);
        } else {
            watch_segments.push("pods");
        }
        let mut watch_url = client.url_from_segments(&watch_segments)?;
        {
            let mut pairs = watch_url.query_pairs_mut();
            pairs.append_pair("fieldSelector", &format!("metadata.name={}", workload_name));
            pairs.append_pair("watch", "true");
            pairs.append_pair("timeoutSeconds", "120");
        }
        print_curl_request(client, "GET", watch_url.as_ref(), None)?;
        return Ok(());
    }

    if !args.volume_keys.is_empty() {
        let secure_dir = Config::SecureAssets.get_path();
        let volume_keys: Vec<VolumeKeyMetadata> =
            SecureAssets::ensure_volume_keys(&secure_dir, &args.volume_keys, false)?;
        for meta in volume_keys {
            let status = if meta.created {
                "Generated"
            } else {
                "Using existing"
            };
            Terminal::stdout(format_args!(
                "{status} volume key '{}' (fingerprint: {}) at {}",
                meta.volume,
                meta.fingerprint,
                meta.path.display()
            ));
        }
    }

    Terminal::stdout(format_args!("Installing {}", display));

    client
        .create_bundle(
            namespace,
            &args.service,
            options,
            args.snapshot.as_deref(),
            true,
            args.update,
        )
        .await?;

    let pod_name = workload_name(namespace, &args.service);
    Terminal::stdout(format_args!("Waiting for {} to become ready", display));
    wait_for_pod_ready(client, namespace, &pod_name).await?;
    Terminal::stdout(format_args!("✅ {} is ready", display));
    Ok(())
}

pub(super) async fn handle_uninstall(
    client: &NanocloudClient,
    args: &UninstallArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let namespace = args.namespace.as_deref();
    let display = service_display_name(namespace, &args.service);

    if args.curl {
        let ns = namespace
            .filter(|value| !value.is_empty())
            .unwrap_or("default");
        let action_segment = format!("{}:uninstall", args.service);
        let url = client.url_from_segments(&[
            "apis",
            "nanocloud.io",
            "v1",
            "namespaces",
            ns,
            "bundles",
            action_segment.as_str(),
        ])?;
        print_curl_request(client, "POST", url.as_ref(), None)?;
        if let Some(path) = args.snapshot.as_deref() {
            let profile_target = profile_export_path(Path::new(path), &args.service);
            let export_segments =
                NanocloudClient::bundle_export_segments(args.namespace.as_deref(), &args.service);
            let segment_refs: Vec<&str> = export_segments
                .iter()
                .map(|segment| segment.as_str())
                .collect();
            let mut export_url = client.url_from_segments(&segment_refs)?;
            {
                let mut pairs = export_url.query_pairs_mut();
                pairs.append_pair("includeSecrets", "true");
            }
            Terminal::stdout(format_args!(
                "# Export the bundle profile (redirect output to save locally, e.g. > {})",
                profile_target.display()
            ));
            print_curl_request(client, "POST", export_url.as_ref(), None)?;
            let backup_url = client.url_from_segments(&[
                "apis",
                "nanocloud.io",
                "v1",
                "namespaces",
                ns,
                "bundles",
                &args.service,
                "backups",
                "latest",
            ])?;
            Terminal::stdout(format_args!(
                "# Download the latest backup (redirect output to save locally, e.g. > {})",
                path
            ));
            print_curl_request(client, "GET", backup_url.as_ref(), None)?;
        }
        return Ok(());
    }

    Terminal::stdout(format_args!("Uninstalling {}", display));
    if let Some(target) = args.snapshot.as_deref() {
        export_profile_snapshot(
            client,
            namespace,
            &args.service,
            &display,
            Path::new(target),
        )
        .await?;
    }

    let mut service_missing = false;
    match client.uninstall_bundle(namespace, &args.service).await {
        Ok(_) => {}
        Err(err) => match err.downcast::<HttpError>() {
            Ok(http_err) if http_err.status == StatusCode::NOT_FOUND => {
                service_missing = true;
                Terminal::stderr(format_args!(
                    "Service '{}' is not running; removing bundle manifest only",
                    display
                ));
            }
            Ok(http_err) => return Err(Box::new(http_err)),
            Err(other) => return Err(other),
        },
    }

    if !service_missing {
        Terminal::stdout(format_args!("Waiting for {} to terminate", display));
        wait_for_service_termination(client, namespace, &args.service).await?;
    }
    if let Err(err) = client.delete_bundle(namespace, &args.service).await {
        Terminal::stderr(format_args!(
            "Warning: failed to remove bundle manifest for '{}': {}",
            args.service, err
        ));
    }
    if let Some(target) = args.snapshot.as_deref() {
        Terminal::stdout(format_args!("Fetching backup for {}", display));
        match client.latest_bundle_backup(namespace, &args.service).await {
            Ok(response) => {
                let path = Path::new(target);
                if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                    fs::create_dir_all(parent).await?;
                }
                let mut file = fs::File::create(path).await?;
                let mut stream = response.bytes_stream();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    file.write_all(&chunk).await?;
                }
                file.flush().await?;
                Terminal::stdout(format_args!("Saved backup to {}", path.display()));
            }
            Err(err) => match err.downcast::<HttpError>() {
                Ok(http_err) if http_err.status == StatusCode::NOT_FOUND => {
                    Terminal::stderr(format_args!(
                        "Warning: no backup available for '{}'",
                        display
                    ));
                }
                Ok(http_err) => return Err(Box::new(http_err)),
                Err(other) => return Err(other),
            },
        }
    }

    Terminal::stdout(format_args!("✅ {} uninstalled", display));
    Ok(())
}

async fn export_profile_snapshot(
    client: &NanocloudClient,
    namespace: Option<&str>,
    service: &str,
    display: &str,
    snapshot_target: &Path,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let profile_path = profile_export_path(snapshot_target, service);
    Terminal::stdout(format_args!(
        "Exporting profile for {} to {}",
        display,
        profile_path.display()
    ));
    match client
        .export_bundle_profile(
            namespace,
            service,
            BundleExportOptions {
                include_secrets: true,
            },
        )
        .await
    {
        Ok(payload) => {
            if let Some(parent) = profile_path.parent().filter(|p| !p.as_os_str().is_empty()) {
                fs::create_dir_all(parent).await?;
            }
            let mut file = fs::File::create(&profile_path).await?;
            file.write_all(&payload).await?;
            file.flush().await?;
            Terminal::stdout(format_args!(
                "Saved profile export to {}",
                profile_path.display()
            ));
        }
        Err(err) => match err.downcast::<HttpError>() {
            Ok(http_err) if http_err.status == StatusCode::NOT_FOUND => {
                Terminal::stderr(format_args!(
                    "Warning: no profile export available for '{}'",
                    display
                ));
            }
            Ok(http_err) => return Err(Box::new(http_err)),
            Err(other) => return Err(other),
        },
    }
    Ok(())
}

fn profile_export_path(snapshot_target: &Path, service: &str) -> PathBuf {
    let parent = snapshot_target
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| p.to_path_buf());
    let stem = snapshot_target
        .file_stem()
        .and_then(|s| {
            let value = s.to_string_lossy();
            if value.is_empty() {
                None
            } else {
                Some(value.into_owned())
            }
        })
        .unwrap_or_else(|| service.to_string());
    let file_name = format!("{}.profile.tar", stem);
    if let Some(dir) = parent {
        dir.join(&file_name)
    } else {
        PathBuf::from(file_name)
    }
}

async fn wait_for_pod_ready(
    client: &NanocloudClient,
    namespace: Option<&str>,
    pod_name: &str,
) -> Result<Pod, Box<dyn Error + Send + Sync>> {
    const OVERALL_TIMEOUT: Duration = Duration::from_secs(120);
    let deadline = Instant::now() + OVERALL_TIMEOUT;

    let mut resource_version: Option<String> = None;

    if let Some(pod) = client.get_pod(namespace, pod_name).await? {
        if is_pod_ready(&pod) {
            return Ok(pod);
        }
        if let Some(reason) = pod_failure_message(&pod) {
            return Err(reason.into());
        }
        resource_version = pod.metadata.resource_version.clone();
    }

    loop {
        if Instant::now() >= deadline {
            return Err("Timed out waiting for pod to become Ready".into());
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        let watch_timeout = remaining
            .min(Duration::from_secs(30))
            .max(Duration::from_secs(1));

        let response = client
            .watch_pod(
                namespace,
                pod_name,
                Some(watch_timeout.as_secs()),
                resource_version.as_deref(),
            )
            .await?;

        let mut stream = response.bytes_stream();
        let mut buffer = BytesMut::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            buffer.extend_from_slice(&chunk);

            loop {
                let Some(pos) = buffer.iter().position(|b| *b == b'\n') else {
                    break;
                };
                let line = buffer.split_to(pos + 1);
                if line.len() <= 1 {
                    continue;
                }
                let mut line_vec = line[..line.len() - 1].to_vec();
                if let Some(last) = line_vec.last() {
                    if *last == b'\r' {
                        line_vec.pop();
                    }
                }
                if line_vec.is_empty() {
                    continue;
                }
                let parsed = match std::str::from_utf8(&line_vec) {
                    Ok(text) => text.trim(),
                    Err(_) => continue,
                };
                if parsed.is_empty() {
                    continue;
                }

                let event: WatchEvent<Pod> = match serde_json::from_str(parsed) {
                    Ok(value) => value,
                    Err(err) => {
                        Terminal::error(format_args!("Failed to parse pod watch event: {}", err));
                        continue;
                    }
                };

                let pod = event.object;
                resource_version = pod.metadata.resource_version.clone();

                if is_pod_ready(&pod) {
                    return Ok(pod);
                }
                if let Some(reason) = pod_failure_message(&pod) {
                    return Err(reason.into());
                }
            }

            if Instant::now() >= deadline {
                return Err("Timed out waiting for pod to become Ready".into());
            }
        }

        // If the stream closed without delivering readiness, poll once more before retrying.
        if let Some(pod) = client.get_pod(namespace, pod_name).await? {
            resource_version = pod.metadata.resource_version.clone();
            if is_pod_ready(&pod) {
                return Ok(pod);
            }
            if let Some(reason) = pod_failure_message(&pod) {
                return Err(reason.into());
            }
        }
    }
}

async fn wait_for_service_termination(
    client: &NanocloudClient,
    namespace: Option<&str>,
    service: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    const OVERALL_TIMEOUT: Duration = Duration::from_secs(120);
    let deadline = Instant::now() + OVERALL_TIMEOUT;

    loop {
        if Instant::now() >= deadline {
            return Err("Timed out waiting for service to terminate".into());
        }

        let pod_name = workload_name(namespace, service);
        match client.get_pod(namespace, &pod_name).await? {
            Some(_) => sleep(Duration::from_secs(2)).await,
            None => return Ok(()),
        }
    }
}

fn is_pod_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| {
            status
                .conditions
                .iter()
                .find(|condition| condition.condition_type == "Ready")
        })
        .map(|condition| condition.status == "True")
        .unwrap_or(false)
}

fn pod_failure_message(pod: &Pod) -> Option<String> {
    let status = pod.status.as_ref()?;
    match status.phase.as_deref() {
        Some("Failed") => Some(format!(
            "Pod {} failed before becoming ready",
            pod.metadata.name.as_deref().unwrap_or("unknown")
        )),
        _ => None,
    }
}

fn workload_name(namespace: Option<&str>, service: &str) -> String {
    namespace
        .filter(|ns| !ns.is_empty())
        .map(|ns| format!("{}-{}", ns, service))
        .unwrap_or_else(|| service.to_string())
}

fn bundle_payload(
    namespace: Option<&str>,
    service: &str,
    options: HashMap<String, String>,
    snapshot: Option<&str>,
    start: bool,
    update: bool,
) -> Bundle {
    let namespace_value = namespace.filter(|ns| !ns.is_empty());
    let path_namespace = namespace_value.unwrap_or("default");

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

    Bundle {
        api_version: "nanocloud.io/v1".to_string(),
        kind: "Bundle".to_string(),
        metadata,
        spec,
        status: None,
    }
}

pub(super) async fn stream_logs_to_terminal(
    client: NanocloudClient,
    namespace: Option<String>,
    service: String,
    follow: bool,
    cancel: CancellationToken,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut retry_backoff = Duration::from_millis(500);
    let max_backoff = Duration::from_secs(5);

    loop {
        match client
            .logs_stream(namespace.as_deref(), &service, follow)
            .await
        {
            Ok(response) => {
                let mut stdout = tokio_io::stdout();
                let mut stream = response.bytes_stream();
                loop {
                    tokio::select! {
                        biased;
                        chunk = stream.next() => {
                            match chunk {
                                Some(data) => {
                                    let bytes = data?;
                                    stdout.write_all(&bytes).await?;
                                    stdout.flush().await?;
                                }
                                None => return Ok(()),
                            }
                        }
                        _ = cancel.cancelled() => {
                            let mut drain_deadline = Instant::now() + Duration::from_secs(2);
                            loop {
                                let now = Instant::now();
                                if now >= drain_deadline {
                                    break;
                                }
                                let remaining = drain_deadline.saturating_duration_since(now);
                                match timeout(remaining, stream.next()).await {
                                    Ok(Some(chunk)) => {
                                        let bytes = chunk?;
                                        stdout.write_all(&bytes).await?;
                                        stdout.flush().await?;
                                        drain_deadline = Instant::now() + Duration::from_secs(2);
                                    }
                                    Ok(None) => return Ok(()),
                                    Err(_) => break,
                                }
                            }
                            return Ok(());
                        }
                    }
                }
            }
            Err(err) => match err.downcast::<HttpError>() {
                Ok(http_err) => {
                    if http_err.status == StatusCode::NOT_FOUND && follow {
                        sleep(retry_backoff).await;
                        retry_backoff = (retry_backoff * 2).min(max_backoff);
                        continue;
                    }
                    return Err(http_err);
                }
                Err(err) => match err.downcast::<reqwest::Error>() {
                    Ok(req_err) => {
                        if follow && (req_err.is_connect() || req_err.is_timeout()) {
                            sleep(retry_backoff).await;
                            retry_backoff = (retry_backoff * 2).min(max_backoff);
                            continue;
                        }
                        return Err(req_err.into());
                    }
                    Err(other) => return Err(other),
                },
            },
        }
    }
}
