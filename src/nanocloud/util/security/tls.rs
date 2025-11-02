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

use base64::Engine;
use serde::{Deserialize, Serialize};

use super::assets::load_ca;
use super::cert::{gen_ecc_key, gen_x509_cert};
use crate::nanocloud::util::error::with_context;

#[derive(Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct JsonTlsInfo {
    pub key: String,
    pub cert: String,
    pub ca: String,
}

#[derive(Clone)]
pub struct TlsInfo {
    pub key: Vec<u8>,
    pub cert: Vec<u8>,
    pub ca: Vec<u8>,
}

impl TlsInfo {
    pub fn create(
        common_name: &str,
        san_list: Option<&Vec<String>>,
    ) -> Result<TlsInfo, Box<dyn Error + Send + Sync>> {
        let (ca_cert, ca_key) =
            load_ca().map_err(|e| with_context(e, "Failed to load cluster CA assets"))?;
        let key =
            gen_ecc_key().map_err(|e| with_context(e, "Failed to generate TLS private key"))?;
        let cert = gen_x509_cert(common_name, &key, Some((&ca_cert, &ca_key)), san_list)
            .map_err(|e| with_context(e, "Failed to generate TLS certificate"))?;

        let key_pem = key
            .private_key_to_pem_pkcs8()
            .map_err(|e| with_context(e, "Failed to encode TLS private key"))?;
        let cert_pem = cert
            .to_pem()
            .map_err(|e| with_context(e, "Failed to encode TLS certificate"))?;
        let ca_pem = ca_cert
            .to_pem()
            .map_err(|e| with_context(e, "Failed to encode cluster CA certificate"))?;

        Ok(TlsInfo {
            key: key_pem,
            cert: cert_pem,
            ca: ca_pem,
        })
    }

    pub fn wrap(&self) -> JsonTlsInfo {
        let engine = base64::engine::general_purpose::STANDARD;
        JsonTlsInfo {
            key: engine.encode(&self.key),
            cert: engine.encode(&self.cert),
            ca: engine.encode(&self.ca),
        }
    }
}
