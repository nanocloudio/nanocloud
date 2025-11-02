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

use super::network::{CniReconciliationReport, CniResult, Network};
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, OnceLock};

type DynError = Box<dyn Error + Send + Sync>;
type DynResult<T> = Result<T, DynError>;

/// Interface for pluggable CNI implementations.
pub trait CniPlugin: Send + Sync {
    fn reconcile_cni_artifacts(&self) -> DynResult<CniReconciliationReport>;
    fn bridge(&self, name: &str, cidr: &str) -> DynResult<()>;
    fn add(&self, env: &HashMap<String, String>, config: Vec<u8>) -> DynResult<CniResult>;
    fn delete(&self, env: &HashMap<String, String>) -> DynResult<()>;
}

struct LocalCniPlugin;

impl CniPlugin for LocalCniPlugin {
    fn reconcile_cni_artifacts(&self) -> DynResult<CniReconciliationReport> {
        Network::reconcile_cni_artifacts()
    }

    fn bridge(&self, name: &str, cidr: &str) -> DynResult<()> {
        Network::bridge(name, cidr)
    }

    fn add(&self, env: &HashMap<String, String>, config: Vec<u8>) -> DynResult<CniResult> {
        Network::add(env, std::io::Cursor::new(config))
    }

    fn delete(&self, env: &HashMap<String, String>) -> DynResult<()> {
        Network::delete(env)
    }
}

static GLOBAL_CNI_PLUGIN: OnceLock<Arc<dyn CniPlugin>> = OnceLock::new();

#[allow(dead_code)] // Integration tests register custom plugins to probe behavior.
pub fn register_cni_plugin(provider: Arc<dyn CniPlugin>) -> Result<(), Arc<dyn CniPlugin>> {
    GLOBAL_CNI_PLUGIN.set(provider)
}

pub fn cni_plugin() -> Arc<dyn CniPlugin> {
    GLOBAL_CNI_PLUGIN
        .get_or_init(|| Arc::new(LocalCniPlugin) as Arc<dyn CniPlugin>)
        .clone()
}
