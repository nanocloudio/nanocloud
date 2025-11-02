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
use std::time::Duration;

use openssl::rand::rand_bytes;
use qrcode::{Color, EcLevel, QrCode};
use serde::Serialize;

use crate::nanocloud::cli::args::TokenArgs;
use crate::nanocloud::cli::Terminal;
use crate::nanocloud::util::security::EncryptionKey;
use crate::nanocloud::util::Keyspace;

const TOKENS_KEYSPACE: Keyspace = Keyspace::new("tokens");
const QUIET_ZONE: usize = 0;
const TOKEN_PREFIX: &str = "/v1/token";
const TOKEN_TTL_SECONDS: u64 = 300;
const TOKEN_ID_LENGTH: usize = 6;
const TOKEN_SECRET_LENGTH: usize = 16;
const TOKEN_CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

#[derive(Serialize)]
struct PersistedTokenGrant<'a> {
    user: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cluster: Option<&'a str>,
    secret: EncryptedSecret<'a>,
}

#[derive(Serialize)]
struct EncryptedSecret<'a> {
    key: &'a str,
    ciphertext: &'a str,
}

struct GeneratedToken {
    id: String,
    secret: String,
}

impl GeneratedToken {
    fn render(&self) -> String {
        format!("{}.{}", self.id, self.secret)
    }
}

pub(crate) fn handle_token(args: &TokenArgs) -> Result<(), Box<dyn Error + Send + Sync>> {
    if args.curl {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "--curl is not supported for token because it runs locally",
        )
        .into());
    }

    let user = args.user.trim();
    if user.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "user must not be empty",
        )
        .into());
    }

    let cluster = args
        .cluster
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let token = generate_token()?;
    persist_token(&token, user, cluster)?;

    let encoded = token.render();
    if args.qr {
        render_ansi_qr(&encoded)?;
    }

    Terminal::stdout(format_args!("{}", encoded));
    Ok(())
}

fn generate_token() -> Result<GeneratedToken, Box<dyn Error + Send + Sync>> {
    let id = random_component(TOKEN_ID_LENGTH)?;
    let secret = random_component(TOKEN_SECRET_LENGTH)?;
    Ok(GeneratedToken { id, secret })
}

fn persist_token(
    token: &GeneratedToken,
    user: &str,
    cluster: Option<&str>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let encryption_key = EncryptionKey::new(None);
    let wrapped_key = encryption_key.wrap()?;
    let encrypted_secret = encryption_key.encrypt(token.secret.as_bytes())?;

    let secret = EncryptedSecret {
        key: &wrapped_key,
        ciphertext: &encrypted_secret,
    };
    let grant = PersistedTokenGrant {
        user,
        cluster,
        secret,
    };

    let payload = serde_json::to_string(&grant)?;
    let key = format!("{TOKEN_PREFIX}/{}", token.id);
    TOKENS_KEYSPACE.put_with_ttl(&key, &payload, Duration::from_secs(TOKEN_TTL_SECONDS))
}

fn random_component(length: usize) -> Result<String, Box<dyn Error + Send + Sync>> {
    let mut output = String::with_capacity(length);
    let mut buffer = [0u8; 32];
    let modulus = TOKEN_CHARSET.len();
    let threshold = 256 - (256 % modulus);

    while output.len() < length {
        rand_bytes(&mut buffer).map_err(|err| {
            Box::new(std::io::Error::other(format!(
                "failed to generate token: {err}"
            ))) as Box<dyn Error + Send + Sync>
        })?;
        for byte in buffer {
            if (byte as usize) < threshold {
                let index = (byte as usize) % modulus;
                output.push(TOKEN_CHARSET[index] as char);
                if output.len() == length {
                    break;
                }
            }
        }
    }

    Ok(output)
}

fn render_ansi_qr(url: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let code = QrCode::with_error_correction_level(url.as_bytes(), EcLevel::L).map_err(
        |err| -> Box<dyn Error + Send + Sync> {
            Box::new(std::io::Error::other(format!(
                "failed to encode QR code: {err}"
            )))
        },
    )?;
    let width = code.width();
    let padded_width = width + QUIET_ZONE * 2;
    let mut y = 0;
    while y < padded_width {
        let mut line = String::with_capacity(padded_width);
        for x in 0..padded_width {
            let upper = module_is_dark(&code, x, y);
            let lower = if y + 1 < padded_width {
                module_is_dark(&code, x, y + 1)
            } else {
                false
            };
            let ch = match (upper, lower) {
                (true, true) => '█',
                (true, false) => '▀',
                (false, true) => '▄',
                (false, false) => ' ',
            };
            line.push(ch);
        }
        Terminal::stdout(format_args!("{}", line));
        y += 2;
    }
    Ok(())
}

fn module_is_dark(code: &QrCode, x: usize, y: usize) -> bool {
    let width = code.width();
    if QUIET_ZONE == 0 {
        if x >= width || y >= width {
            return false;
        }
        return code[(x, y)] == Color::Dark;
    }

    let quiet_zone = QUIET_ZONE;
    if x < quiet_zone || y < quiet_zone {
        return false;
    }
    let inner_x = x - quiet_zone;
    let inner_y = y - quiet_zone;
    if inner_x >= width || inner_y >= width {
        return false;
    }
    code[(inner_x, inner_y)] == Color::Dark
}
