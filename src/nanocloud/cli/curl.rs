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

use std::env;
use std::path::Path;
use std::process::Command;

use crate::nanocloud::api::client::{CurlAuthData, KubeFieldSource, NanocloudClient};
use crate::nanocloud::cli::Terminal;

#[derive(Clone, Copy)]
struct CurlTooling {
    has_jq: bool,
    has_base64: bool,
    process_substitution: bool,
}

struct CurlContext<'a> {
    owner: Option<String>,
    bearer_token: Option<String>,
    auth: Option<&'a CurlAuthData>,
}

struct AuthPlan {
    notes: Vec<String>,
    setup: Vec<String>,
    prefix: String,
    cleanup: Vec<String>,
}

struct FileAuthPlan {
    setup: Vec<String>,
    cleanup: Vec<String>,
    cert_var: String,
    key_var: String,
    ca_var: Option<String>,
}

pub(super) fn print_curl_request(
    client: &NanocloudClient,
    method: &str,
    url: &str,
    body: Option<&str>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    print_curl_request_with_type(client, method, url, body, "application/json")
}

/// Render a curl command for a request, auto-detecting shell/tooling support and emitting portable fallbacks when needed.
pub(super) fn print_curl_request_with_type(
    client: &NanocloudClient,
    method: &str,
    url: &str,
    body: Option<&str>,
    content_type: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let context = CurlContext {
        owner: client.owner().map(|value| value.to_string()),
        bearer_token: client.bearer_token().map(|value| value.to_string()),
        auth: client.curl_identity(),
    };
    let lines =
        build_curl_request_lines(&context, method, url, body, content_type, detect_tooling())?;
    for line in lines {
        Terminal::stdout(format_args!("{}", line));
    }
    Ok(())
}

fn build_curl_request_lines(
    context: &CurlContext<'_>,
    method: &str,
    url: &str,
    body: Option<&str>,
    content_type: &str,
    tooling: CurlTooling,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let auth_plan = build_auth_plan(context, tooling)?;
    let owner_header = context.owner.as_deref().map(|owner| {
        format!(
            "-H {}",
            shell_quote_str(&format!("X-Nanocloud-Owner: {}", owner))
        )
    });

    let mut lines = Vec::new();
    lines.extend(auth_plan.notes);
    lines.extend(auth_plan.setup);
    lines.extend(render_curl_command(
        &auth_plan.prefix,
        owner_header.as_deref(),
        method,
        url,
        body,
        content_type,
    ));
    lines.extend(auth_plan.cleanup);
    lines.push(String::new());
    Ok(lines)
}

fn build_auth_plan(
    context: &CurlContext<'_>,
    tooling: CurlTooling,
) -> Result<AuthPlan, Box<dyn std::error::Error + Send + Sync>> {
    let mut notes = Vec::new();
    let base_parts = base_curl_parts(context);

    if let Some(auth) = context.auth {
        if tooling.process_substitution && tooling.has_jq && tooling.has_base64 {
            notes.push(
                "# Requires bash-compatible shells with process substitution, jq, and base64."
                    .to_string(),
            );
            let prefix = process_substitution_prefix(base_parts, auth);
            return Ok(AuthPlan {
                notes,
                setup: Vec::new(),
                prefix,
                cleanup: Vec::new(),
            });
        }

        let missing = tooling.missing_labels();
        if !missing.is_empty() {
            notes.push(format!(
                "# Missing {} — using portable auth files instead.",
                missing.join(", ")
            ));
        } else {
            notes.push("# Using portable auth files to avoid shell-specific features.".to_string());
        }

        let file_plan = build_file_auth_plan(auth);
        let prefix = file_auth_prefix(base_parts, &file_plan, auth.ca_source.is_some());
        return Ok(AuthPlan {
            notes,
            setup: file_plan.setup,
            prefix,
            cleanup: file_plan.cleanup,
        });
    }

    Ok(AuthPlan {
        notes,
        setup: Vec::new(),
        prefix: base_parts.join(" "),
        cleanup: Vec::new(),
    })
}

fn render_curl_command(
    prefix: &str,
    owner_header: Option<&str>,
    method: &str,
    url: &str,
    body: Option<&str>,
    content_type: &str,
) -> Vec<String> {
    let mut headers = Vec::new();
    if let Some(owner) = owner_header {
        if !owner.is_empty() {
            headers.push(owner.to_string());
        }
    }

    match body {
        Some(payload) => {
            let mut parts = vec![format!("cat <<'EOF' | {} -X {} '{}'", prefix, method, url)];
            if !headers.is_empty() {
                parts.push(headers.join(" "));
            }
            parts.push(format!(
                "-H 'Content-Type: {}' --data-binary @-",
                content_type
            ));
            let mut lines = vec![parts.join(" ")];
            lines.push(payload.to_string());
            lines.push("EOF".to_string());
            lines
        }
        None => {
            let mut parts = vec![prefix.to_string()];
            if let Some(owner) = owner_header {
                if !owner.is_empty() {
                    parts.push(owner.to_string());
                }
            }
            if method.eq_ignore_ascii_case("GET") {
                parts.push(format!("'{}'", url));
            } else {
                parts.push(format!("-X {} '{}'", method, url));
            }
            vec![parts.join(" ")]
        }
    }
}

fn base_curl_parts(context: &CurlContext<'_>) -> Vec<String> {
    let mut parts = vec!["curl --fail --silent --show-error".to_string()];
    if let Some(token) = context.bearer_token.as_deref() {
        parts.push(format!(
            "-H {}",
            shell_quote_str(&format!("Authorization: Bearer {}", token))
        ));
    }
    parts
}

fn process_substitution_prefix(base_parts: Vec<String>, auth: &CurlAuthData) -> String {
    let mut parts = base_parts;

    if let Some(source) = auth.ca_source {
        let field = match source {
            KubeFieldSource::InlineData => "certificate-authority-data",
            KubeFieldSource::FilePath => "certificate-authority",
        };
        let selector = cluster_field_selector(auth, field);
        parts.push(format!(
            "--cacert {}",
            jq_process_substitution(auth, &selector, source)
        ));
    }

    let cert_field = match auth.cert_source {
        KubeFieldSource::InlineData => "client-certificate-data",
        KubeFieldSource::FilePath => "client-certificate",
    };
    let cert_selector = user_field_selector(auth, cert_field);
    parts.push(format!(
        "--cert {}",
        jq_process_substitution(auth, &cert_selector, auth.cert_source)
    ));

    let key_field = match auth.key_source {
        KubeFieldSource::InlineData => "client-key-data",
        KubeFieldSource::FilePath => "client-key",
    };
    let key_selector = user_field_selector(auth, key_field);
    parts.push(format!(
        "--key {}",
        jq_process_substitution(auth, &key_selector, auth.key_source)
    ));

    parts.join(" ")
}

fn file_auth_prefix(mut parts: Vec<String>, plan: &FileAuthPlan, include_ca: bool) -> String {
    if include_ca {
        if let Some(ca_var) = &plan.ca_var {
            parts.push(format!("--cacert {}", ca_var));
        }
    }
    parts.push(format!("--cert {}", plan.cert_var));
    parts.push(format!("--key {}", plan.key_var));
    parts.join(" ")
}

fn build_file_auth_plan(auth: &CurlAuthData) -> FileAuthPlan {
    let mut setup = Vec::new();
    let mut cleanup = Vec::new();
    let needs_inline_cert = auth.cert_path.is_none();
    let needs_inline_key = auth.key_path.is_none();
    let needs_inline_ca = auth.ca_path.is_none() && auth.ca_data.is_some();
    let use_tmp_dir = needs_inline_cert || needs_inline_key || needs_inline_ca;

    if use_tmp_dir {
        setup
            .push("NC_TMP_DIR=$(mktemp -d 2>/dev/null || mktemp -d -t nanocloud-curl)".to_string());
        cleanup.push("# Cleanup credentials after running: rm -rf \"$NC_TMP_DIR\"".to_string());
    }

    setup.extend(assign_or_write(
        "NC_CERT_FILE",
        auth.cert_path.as_deref(),
        use_tmp_dir,
        "nanocloud-cert.pem",
        &auth.cert_data,
        "CERT",
    ));

    setup.extend(assign_or_write(
        "NC_KEY_FILE",
        auth.key_path.as_deref(),
        use_tmp_dir,
        "nanocloud-key.pem",
        &auth.key_data,
        "KEY",
    ));

    let ca_var = if auth.ca_source.is_some() {
        let lines = assign_or_write(
            "NC_CA_FILE",
            auth.ca_path.as_deref(),
            use_tmp_dir,
            "nanocloud-ca.pem",
            auth.ca_data.as_deref().unwrap_or_else(|| "".as_bytes()),
            "CA",
        );
        setup.extend(lines);
        Some("$NC_CA_FILE".to_string())
    } else {
        None
    };

    FileAuthPlan {
        setup,
        cleanup,
        cert_var: "$NC_CERT_FILE".to_string(),
        key_var: "$NC_KEY_FILE".to_string(),
        ca_var,
    }
}

fn assign_or_write(
    var_name: &str,
    path: Option<&Path>,
    use_tmp_dir: bool,
    file_name: &str,
    data: &[u8],
    label: &str,
) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(path) = path {
        lines.push(format!("{var_name}={}", shell_quote(path)));
        return lines;
    }

    if !use_tmp_dir {
        lines.push(format!(
            "# {var_name} missing and no temporary directory available; skipping."
        ));
        return lines;
    }

    let target = format!("$NC_TMP_DIR/{file_name}");
    lines.push(format!("{var_name}=\"{target}\""));
    lines.extend(write_inline_file(&target, data, label));
    lines
}

fn write_inline_file(target: &str, data: &[u8], label: &str) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!("cat > \"{target}\" <<'EOF_{label}'"));
    let text = String::from_utf8_lossy(data);
    for line in text.split_inclusive('\n') {
        let trimmed = line.strip_suffix('\n').unwrap_or(line);
        lines.push(trimmed.to_string());
    }
    lines.push(format!("EOF_{label}"));
    lines
}

fn detect_tooling() -> CurlTooling {
    CurlTooling {
        has_jq: command_available("jq"),
        has_base64: command_available("base64"),
        process_substitution: shell_supports_process_substitution(),
    }
}

impl CurlTooling {
    fn missing_labels(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();
        if !self.process_substitution {
            missing.push("bash/process substitution");
        }
        if !self.has_jq {
            missing.push("jq");
        }
        if !self.has_base64 {
            missing.push("base64");
        }
        missing
    }
}

fn command_available(command: &str) -> bool {
    Command::new(command)
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn shell_supports_process_substitution() -> bool {
    if let Ok(shell) = env::var("SHELL") {
        if shell.contains("bash") || shell.contains("zsh") {
            return true;
        }
    }
    env::var("BASH_VERSION").is_ok()
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn inline_auth() -> CurlAuthData {
        CurlAuthData {
            kubeconfig_path: PathBuf::from("/tmp/kubeconfig"),
            kubeconfig_dir: PathBuf::from("/tmp"),
            cluster_name: "demo".into(),
            user_name: "demo-user".into(),
            ca_source: Some(KubeFieldSource::InlineData),
            ca_path: None,
            ca_data: Some(
                "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----\n"
                    .as_bytes()
                    .to_vec(),
            ),
            cert_source: KubeFieldSource::InlineData,
            cert_path: None,
            cert_data: "-----BEGIN CERTIFICATE-----\nCERT\n-----END CERTIFICATE-----\n"
                .as_bytes()
                .to_vec(),
            key_source: KubeFieldSource::InlineData,
            key_path: None,
            key_data: "-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----\n"
                .as_bytes()
                .to_vec(),
        }
    }

    #[test]
    fn process_substitution_used_when_tooling_available() {
        let auth = inline_auth();
        let context = CurlContext {
            owner: Some("owner".into()),
            bearer_token: None,
            auth: Some(&auth),
        };

        let lines = build_curl_request_lines(
            &context,
            "GET",
            "https://example.test",
            None,
            "application/json",
            CurlTooling {
                has_jq: true,
                has_base64: true,
                process_substitution: true,
            },
        )
        .expect("curl lines");

        assert!(
            lines
                .iter()
                .any(|line| line.contains("process substitution")),
            "expected prerequisite note in output"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.contains("jq -r '(.clusters[]") && line.contains("<(")),
            "expected jq process substitution in command"
        );
    }

    #[test]
    fn falls_back_to_portable_files_without_tooling() {
        let auth = inline_auth();
        let context = CurlContext {
            owner: None,
            bearer_token: Some("token".into()),
            auth: Some(&auth),
        };

        let lines = build_curl_request_lines(
            &context,
            "POST",
            "https://example.test",
            Some("{\"value\":1}"),
            "application/json",
            CurlTooling {
                has_jq: false,
                has_base64: false,
                process_substitution: false,
            },
        )
        .expect("curl lines");

        assert!(
            lines
                .iter()
                .any(|line| line.contains("portable auth files")),
            "expected fallback note"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.contains("NC_TMP_DIR=$(mktemp")),
            "expected temporary directory setup"
        );
        assert!(
            lines.iter().any(|line| line.contains("BEGIN PRIVATE KEY")),
            "expected inline credential payload"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.contains("--cert $NC_CERT_FILE")),
            "expected portable curl prefix"
        );
    }

    #[test]
    fn uses_existing_paths_when_available() {
        let mut auth = inline_auth();
        auth.cert_source = KubeFieldSource::FilePath;
        auth.key_source = KubeFieldSource::FilePath;
        auth.ca_source = Some(KubeFieldSource::FilePath);
        auth.cert_path = Some(PathBuf::from("/etc/certs/client.pem"));
        auth.key_path = Some(PathBuf::from("/etc/certs/client-key.pem"));
        auth.ca_path = Some(PathBuf::from("/etc/certs/ca.pem"));

        let context = CurlContext {
            owner: None,
            bearer_token: None,
            auth: Some(&auth),
        };

        let lines = build_curl_request_lines(
            &context,
            "GET",
            "https://example.test",
            None,
            "application/json",
            CurlTooling {
                has_jq: false,
                has_base64: false,
                process_substitution: false,
            },
        )
        .expect("curl lines");

        assert!(
            lines
                .iter()
                .any(|line| line.contains("NC_CERT_FILE='/etc/certs/client.pem'")),
            "expected direct assignment to existing cert path"
        );
        assert!(
            !lines.iter().any(|line| line.contains("NC_TMP_DIR")),
            "should not need temporary directory when paths exist"
        );
    }

    #[test]
    fn renders_bearer_only_requests() {
        let context = CurlContext {
            owner: None,
            bearer_token: Some("token-123".into()),
            auth: None,
        };

        let lines = build_curl_request_lines(
            &context,
            "GET",
            "https://example.test",
            None,
            "application/json",
            CurlTooling {
                has_jq: false,
                has_base64: false,
                process_substitution: false,
            },
        )
        .expect("curl lines");

        let command = lines
            .iter()
            .find(|line| line.contains("curl --fail"))
            .expect("expected curl command");
        assert!(
            command.contains("Authorization: Bearer token-123"),
            "expected bearer token header in command"
        );
    }
}
