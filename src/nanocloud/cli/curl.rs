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

use std::path::Path;

use crate::nanocloud::api::client::{CurlAuthData, KubeFieldSource, NanocloudClient};
use crate::nanocloud::cli::Terminal;

pub(super) fn curl_prefix(
    client: &NanocloudClient,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut parts = vec!["curl --fail --silent --show-error".to_string()];

    if let Some(identity) = client.curl_identity() {
        if let Some(source) = identity.ca_source {
            let field = match source {
                KubeFieldSource::InlineData => "certificate-authority-data",
                KubeFieldSource::FilePath => "certificate-authority",
            };
            let selector = cluster_field_selector(identity, field);
            parts.push(format!(
                "--cacert {}",
                jq_process_substitution(identity, &selector, source)
            ));
        }

        let cert_field = match identity.cert_source {
            KubeFieldSource::InlineData => "client-certificate-data",
            KubeFieldSource::FilePath => "client-certificate",
        };
        let cert_selector = user_field_selector(identity, cert_field);
        parts.push(format!(
            "--cert {}",
            jq_process_substitution(identity, &cert_selector, identity.cert_source)
        ));

        let key_field = match identity.key_source {
            KubeFieldSource::InlineData => "client-key-data",
            KubeFieldSource::FilePath => "client-key",
        };
        let key_selector = user_field_selector(identity, key_field);
        parts.push(format!(
            "--key {}",
            jq_process_substitution(identity, &key_selector, identity.key_source)
        ));
    }

    if let Some(token) = client.bearer_token() {
        parts.push(format!(
            "-H {}",
            shell_quote_str(&format!("Authorization: Bearer {}", token))
        ));
    }

    Ok(parts.join(" "))
}

pub(super) fn print_curl_request(
    client: &NanocloudClient,
    method: &str,
    url: &str,
    body: Option<&str>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    print_curl_request_with_type(client, method, url, body, "application/json")
}

pub(super) fn print_curl_request_with_type(
    client: &NanocloudClient,
    method: &str,
    url: &str,
    body: Option<&str>,
    content_type: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let prefix = curl_prefix(client)?;
    let owner_header = client
        .owner()
        .map(|owner| {
            format!(
                "-H {}",
                shell_quote_str(&format!("X-Nanocloud-Owner: {}", owner))
            )
        })
        .unwrap_or_default();
    match body {
        Some(payload) => {
            Terminal::stdout(format_args!(
                "cat <<'EOF' | {} -X {} '{}' {} -H 'Content-Type: {}' --data-binary @-",
                prefix, method, url, owner_header, content_type
            ));
            Terminal::stdout(format_args!("{}", payload));
            Terminal::stdout(format_args!("EOF"));
        }
        None => {
            if method.eq_ignore_ascii_case("GET") {
                Terminal::stdout(format_args!("{} {} '{}'", prefix, owner_header, url));
            } else {
                Terminal::stdout(format_args!(
                    "{} {} -X {} '{}'",
                    prefix, owner_header, method, url
                ));
            }
        }
    }
    Terminal::stdout(format_args!(""));
    Ok(())
}

fn jq_process_substitution(auth: &CurlAuthData, selector: &str, source: KubeFieldSource) -> String {
    let kubeconfig = shell_quote(&auth.kubeconfig_path);
    let jq_command = format!("jq -r '({}) // empty' {}", selector, kubeconfig);
    match source {
        KubeFieldSource::InlineData => format!("<({} | base64 --decode)", jq_command),
        KubeFieldSource::FilePath => {
            let config_dir = shell_quote(&auth.kubeconfig_dir);
            format!(
                "<(CONFIG_DIR={config_dir}; export CONFIG_DIR; {jq} | while IFS= read -r path; do \
                    if [ -z \"$path\" ]; then continue; fi; \
                    case \"$path\" in \
                        ~/*) path=\"$HOME/${{path:2}}\" ;; \
                        ~) path=\"$HOME\" ;; \
                    esac; \
                    if [ \"${{path:0:1}}\" != \"/\" ]; then path=\"$CONFIG_DIR/$path\"; fi; \
                    cat \"$path\"; \
                done)",
                config_dir = config_dir,
                jq = jq_command,
            )
        }
    }
}

fn cluster_field_selector(auth: &CurlAuthData, field: &str) -> String {
    format!(
        ".clusters[] | select(.name == {}) | .cluster[{}]",
        jq_string_literal(&auth.cluster_name),
        jq_string_literal(field)
    )
}

fn user_field_selector(auth: &CurlAuthData, field: &str) -> String {
    format!(
        ".users[] | select(.name == {}) | .user[{}]",
        jq_string_literal(&auth.user_name),
        jq_string_literal(field)
    )
}

fn jq_string_literal(value: &str) -> String {
    serde_json::to_string(value).expect("failed to encode jq string literal")
}

fn shell_quote(path: &Path) -> String {
    let raw = path.to_string_lossy();
    if raw.contains('\'') {
        let escaped = raw.replace('\'', "'\"'\"'");
        format!("'{}'", escaped)
    } else {
        format!("'{}'", raw)
    }
}

fn shell_quote_str(value: &str) -> String {
    if value.contains('\'') {
        let escaped = value.replace('\'', "'\"'\"'");
        format!("'{}'", escaped)
    } else {
        format!("'{}'", value)
    }
}
