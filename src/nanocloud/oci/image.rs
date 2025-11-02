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

use crate::nanocloud::util::error::with_context;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OciImage {
    pub created: String,
    pub architecture: String,
    pub variant: Option<String>,
    pub os: String,
    pub config: Config,
    pub rootfs: RootFs,
    pub history: Vec<HistoryEntry>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    #[serde(rename = "User")]
    pub user: Option<String>,
    #[serde(rename = "Env")]
    pub env: Option<Vec<String>>,
    #[serde(rename = "Entrypoint")]
    pub entrypoint: Option<Vec<String>>,
    #[serde(rename = "Cmd")]
    pub cmd: Option<Vec<String>>,
    #[serde(rename = "WorkingDir")]
    pub working_dir: Option<String>,
    #[serde(rename = "Labels")]
    pub labels: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RootFs {
    #[serde(rename = "type")]
    pub type_field: String,
    #[serde(rename = "diff_ids")]
    pub diff_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HistoryEntry {
    pub created: String,
    #[serde(rename = "created_by")]
    pub created_by: String,
    pub comment: Option<String>,
    #[serde(rename = "empty_layer")]
    pub empty_layer: Option<bool>,
}

impl OciImage {
    pub fn load(digest: &str) -> Result<OciImage, Box<dyn Error + Send + Sync>> {
        let path = format!("/var/lib/nanocloud.io/image/blobs/sha256/{}", &digest[7..]);
        let file = File::open(&path)
            .map_err(|e| with_context(e, format!("Failed to open image config at {path}")))?;
        let reader = BufReader::new(file);
        let oci_image: OciImage = serde_json::from_reader(reader)
            .map_err(|e| with_context(e, format!("Failed to parse image config at {path}")))?;

        Ok(oci_image)
    }
}
