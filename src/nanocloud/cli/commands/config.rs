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

use std::error::Error;
use std::io::{self, Read};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use openssl::ec::{EcGroup, EcKey};
use openssl::error::ErrorStack;
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::PKey;
use openssl::x509::{X509NameBuilder, X509ReqBuilder};
use reqwest::Url;

use crate::nanocloud::api::client::{EphemeralCertificate, NanocloudClient};
use crate::nanocloud::cli::args::KubeConfigArgs;
use crate::nanocloud::cli::Terminal;

pub(super) async fn handle_config(
    args: &KubeConfigArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let user = args.user.trim();
    if user.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "user must not be empty").into());
    }

    let server = args.server.trim();
    if server.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "server must not be empty").into());
    }

    let cluster = args.cluster.trim();
    if cluster.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cluster must not be empty when issuing credentials",
        )
        .into());
    }

    let raw_token_input = read_token_input(args)?;
    let single_use_token = extract_token(&raw_token_input)?;

    let client = NanocloudClient::with_bearer(&args.host, single_use_token)?;

    if args.curl {
        emit_certificate_curl(&client)?;
        return Ok(());
    }

    let (private_key_pem, csr_pem) = generate_key_and_csr(user)?;
    let certificate = client.request_ephemeral_certificate(&csr_pem).await?;
    let EphemeralCertificate {
        certificate: client_certificate,
        ca_bundle,
        expiration: _expiration,
    } = certificate;

    let certificate_b64 = encode_base64(&client_certificate);
    let ca_bundle_b64 = encode_base64(&ca_bundle);
    let key_b64 = encode_base64(&private_key_pem);

    let context_name = format!("{user}@{cluster}");
    let kubeconfig = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [
            {
                "name": cluster,
                "cluster": {
                    "certificate-authority-data": ca_bundle_b64,
                    "server": server,
                }
            }
        ],
        "users": [
            {
                "name": user,
                "user": {
                    "client-certificate-data": certificate_b64,
                    "client-key-data": key_b64,
                }
            }
        ],
        "contexts": [
            {
                "name": context_name,
                "context": {
                    "cluster": cluster,
                    "user": user,
                }
            }
        ],
        "current-context": context_name,
    });

    let kubeconfig_text = serde_json::to_string_pretty(&kubeconfig)?;
    emit_kubeconfig_script(&kubeconfig_text);
    Ok(())
}

fn emit_certificate_curl(client: &NanocloudClient) -> Result<(), Box<dyn Error + Send + Sync>> {
    let token_value = client.bearer_token().ok_or_else(|| {
        Box::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            "token is required to render the curl request",
        )) as Box<dyn Error + Send + Sync>
    })?;
    let url = client
        .url_from_segments(&["apis", "nanocloud.io", "v1", "certificates"])?
        .to_string();
    let payload = serde_json::json!({
        "apiVersion": "nanocloud.io/v1",
        "kind": "CertificateRequest",
        "spec": {
            "csrPem": "__REPLACE_WITH_CSR_PEM__",
        }
    });
    let body = serde_json::to_string_pretty(&payload)?;

    Terminal::stdout(format_args!(
        "SINGLE_USE_TOKEN='{}'",
        token_value.replace('\'', "'\"'\"'")
    ));
    Terminal::stdout(format_args!(
        r#"curl --fail --silent --show-error \
-H "Authorization: Bearer $SINGLE_USE_TOKEN" \
-H 'Content-Type: application/json' \
'{}' \
--data-binary @- <<'EOF'"#,
        url
    ));
    Terminal::stdout(format_args!("{}", body));
    Terminal::stdout(format_args!("EOF"));
    Ok(())
}

fn read_token_input(args: &KubeConfigArgs) -> Result<String, Box<dyn Error + Send + Sync>> {
    match args.token.as_deref() {
        Some(raw) if raw.trim() == "-" => {
            let mut buffer = String::new();
            io::stdin()
                .read_to_string(&mut buffer)
                .map_err(|err| -> Box<dyn Error + Send + Sync> { Box::new(err) })?;
            if buffer.trim().is_empty() {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "token read from stdin was empty",
                )
                .into())
            } else {
                Ok(buffer)
            }
        }
        Some(raw) => Ok(raw.to_string()),
        None => Err(io::Error::new(io::ErrorKind::InvalidInput, "--token is required").into()),
    }
}

fn extract_token(raw: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "token payload was empty").into());
    }

    if let Ok(url) = Url::parse(trimmed) {
        if let Some(token) = url.query_pairs().find_map(|(key, value)| {
            if key == "token" || key == "singleUseToken" {
                Some(value.into_owned())
            } else {
                None
            }
        }) {
            return sanitize_token(&token);
        }

        if let Some(segment) = url.path_segments().and_then(|segments| {
            segments
                .rev()
                .find(|s| !s.is_empty())
                .map(|s| s.to_string())
        }) {
            return sanitize_token(&segment);
        }
    }

    if let Some(index) = trimmed.find("singleUseToken=") {
        let candidate = &trimmed[index + "singleUseToken=".len()..];
        return sanitize_token(candidate);
    }

    if let Some(index) = trimmed.find("token=") {
        let candidate = &trimmed[index + "token=".len()..];
        return sanitize_token(candidate);
    }

    if let Some(pos) = trimmed.rfind('/') {
        return sanitize_token(&trimmed[pos + 1..]);
    }

    sanitize_token(trimmed)
}

fn sanitize_token(candidate: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
    let token = candidate.split(['?', '#', '&']).next().unwrap_or("").trim();
    if token.is_empty() {
        Err(io::Error::new(io::ErrorKind::InvalidInput, "token payload was empty").into())
    } else {
        Ok(token.to_string())
    }
}

fn generate_key_and_csr(
    common_name: &str,
) -> Result<(Vec<u8>, String), Box<dyn Error + Send + Sync>> {
    let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1)
        .map_err(|err| wrap_openssl_error(err, "failed to select elliptic curve"))?;
    let ec_key = EcKey::generate(&group)
        .map_err(|err| wrap_openssl_error(err, "failed to generate EC private key"))?;
    let key_pair = PKey::from_ec_key(ec_key)
        .map_err(|err| wrap_openssl_error(err, "failed to convert EC key into signing key"))?;

    let mut name_builder = X509NameBuilder::new()
        .map_err(|err| wrap_openssl_error(err, "failed to create subject builder"))?;
    name_builder
        .append_entry_by_text("CN", common_name)
        .map_err(|err| wrap_openssl_error(err, "failed to set CSR common name"))?;
    let name = name_builder.build();

    let mut csr_builder = X509ReqBuilder::new()
        .map_err(|err| wrap_openssl_error(err, "failed to create CSR builder"))?;
    csr_builder
        .set_subject_name(&name)
        .map_err(|err| wrap_openssl_error(err, "failed to attach CSR subject name"))?;
    csr_builder
        .set_pubkey(&key_pair)
        .map_err(|err| wrap_openssl_error(err, "failed to attach CSR public key"))?;
    csr_builder
        .sign(&key_pair, MessageDigest::sha256())
        .map_err(|err| wrap_openssl_error(err, "failed to sign CSR"))?;
    let csr = csr_builder.build();

    let csr_pem = csr
        .to_pem()
        .map_err(|err| wrap_openssl_error(err, "failed to encode CSR as PEM"))?;
    let key_pem = key_pair
        .private_key_to_pem_pkcs8()
        .map_err(|err| wrap_openssl_error(err, "failed to encode private key as PKCS#8"))?;

    let csr_text = String::from_utf8(csr_pem).map_err(|err| {
        Box::new(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("CSR PEM is not valid UTF-8: {err}"),
        )) as Box<dyn Error + Send + Sync>
    })?;

    Ok((key_pem, csr_text))
}

fn wrap_openssl_error(err: ErrorStack, context: &str) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(format!("{context}: {err}")))
}

fn encode_base64(bytes: &[u8]) -> String {
    BASE64.encode(bytes)
}

fn emit_kubeconfig_script(kubeconfig_text: &str) {
    Terminal::stdout(format_args!("#!/bin/sh"));
    Terminal::stdout(format_args!("set -eu"));
    Terminal::stdout(format_args!("umask 077"));
    Terminal::stdout(format_args!("mkdir -p \"$HOME/.kube\""));
    Terminal::stdout(format_args!("cat > \"$HOME/.kube/config\" <<'EOF'"));
    Terminal::stdout(format_args!("{}", kubeconfig_text));
    Terminal::stdout(format_args!("EOF"));
    Terminal::stdout(format_args!("chmod 600 \"$HOME/.kube/config\""));
}
