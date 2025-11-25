/*
 * Copyright (C) 2025 The Nanocloud Authors
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

pub mod config;
pub mod registry;
pub mod resolver;
pub mod server;

use std::sync::Arc;

#[allow(unused_imports)]
pub use config::DnsConfig;
#[allow(unused_imports)]
pub use registry::{
    ClusterDnsSnapshot, DnsProtocol, DnsRegistry, EndpointDescription, EndpointId, EndpointPatch,
    EndpointSnapshot, RegistryError, ServiceDescription, ServicePortDescription,
};
#[allow(unused_imports)]
pub use resolver::{
    DnsQuestion, DnsRecord, DnsResolver, DnsResponse, QueryType, Resolution, ResponseCode,
};

#[derive(Clone)]
pub struct DnsService {
    config: DnsConfig,
    registry: Arc<DnsRegistry>,
}

impl DnsService {
    pub fn new(config: DnsConfig) -> Self {
        Self {
            registry: Arc::new(DnsRegistry::new()),
            config,
        }
    }

    pub fn config(&self) -> &DnsConfig {
        &self.config
    }

    #[allow(dead_code)]
    pub fn registry(&self) -> Arc<DnsRegistry> {
        Arc::clone(&self.registry)
    }

    pub fn resolver(&self) -> DnsResolver {
        DnsResolver::new(self.config.clone(), Arc::clone(&self.registry))
    }
}
