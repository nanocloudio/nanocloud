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

use super::driver::{CsiDriver, StoredVolume};
use super::types::{
    CreateSnapshotRequest, CreateSnapshotResponse, CreateVolumeRequest, CreateVolumeResponse,
    DeleteSnapshotRequest, DeleteVolumeRequest, NodePublishVolumeRequest,
    NodePublishVolumeResponse, NodeUnpublishVolumeRequest,
};
use std::error::Error;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

type DynError = Box<dyn Error + Send + Sync>;
type DynResult<T> = Result<T, DynError>;
type CsiFuture<T> = Pin<Box<dyn Future<Output = DynResult<T>> + Send>>;

/// Interface for pluggable CSI implementations.
pub trait CsiPlugin: Send + Sync {
    fn publish_root(&self) -> PathBuf;

    fn create_volume(&self, request: CreateVolumeRequest) -> CsiFuture<CreateVolumeResponse>;

    fn delete_volume(&self, request: DeleteVolumeRequest) -> CsiFuture<()>;

    fn node_publish_volume(
        &self,
        request: NodePublishVolumeRequest,
    ) -> CsiFuture<NodePublishVolumeResponse>;

    fn node_unpublish_volume(&self, request: NodeUnpublishVolumeRequest) -> CsiFuture<()>;

    fn create_snapshot(&self, request: CreateSnapshotRequest) -> CsiFuture<CreateSnapshotResponse>;

    fn delete_snapshot(&self, request: DeleteSnapshotRequest) -> CsiFuture<()>;

    fn list_service_volumes(&self, namespace: &str, service: &str) -> DynResult<Vec<StoredVolume>>;
}

struct LocalCsiPlugin;

impl CsiPlugin for LocalCsiPlugin {
    fn publish_root(&self) -> PathBuf {
        CsiDriver::shared().publish_root().to_path_buf()
    }

    fn create_volume(&self, request: CreateVolumeRequest) -> CsiFuture<CreateVolumeResponse> {
        Box::pin(async move { CsiDriver::shared().create_volume(request).await })
    }

    fn delete_volume(&self, request: DeleteVolumeRequest) -> CsiFuture<()> {
        Box::pin(async move { CsiDriver::shared().delete_volume(request).await })
    }

    fn node_publish_volume(
        &self,
        request: NodePublishVolumeRequest,
    ) -> CsiFuture<NodePublishVolumeResponse> {
        Box::pin(async move { CsiDriver::shared().node_publish_volume(request).await })
    }

    fn node_unpublish_volume(&self, request: NodeUnpublishVolumeRequest) -> CsiFuture<()> {
        Box::pin(async move { CsiDriver::shared().node_unpublish_volume(request).await })
    }

    fn create_snapshot(&self, request: CreateSnapshotRequest) -> CsiFuture<CreateSnapshotResponse> {
        Box::pin(async move { CsiDriver::shared().create_snapshot(request).await })
    }

    fn delete_snapshot(&self, request: DeleteSnapshotRequest) -> CsiFuture<()> {
        Box::pin(async move { CsiDriver::shared().delete_snapshot(request).await })
    }

    fn list_service_volumes(&self, namespace: &str, service: &str) -> DynResult<Vec<StoredVolume>> {
        CsiDriver::shared().list_service_volumes(namespace, service)
    }
}

static GLOBAL_CSI_PLUGIN: OnceLock<Arc<dyn CsiPlugin>> = OnceLock::new();

#[allow(dead_code)] // Integration tests register fake providers to assert override behaviour.
pub fn register_csi_plugin(provider: Arc<dyn CsiPlugin>) -> Result<(), Arc<dyn CsiPlugin>> {
    GLOBAL_CSI_PLUGIN.set(provider)
}

pub fn csi_plugin() -> Arc<dyn CsiPlugin> {
    GLOBAL_CSI_PLUGIN
        .get_or_init(|| Arc::new(LocalCsiPlugin) as Arc<dyn CsiPlugin>)
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_provider_exposes_local_publish_root() {
        let plugin = csi_plugin();
        let driver_root = CsiDriver::shared().publish_root().to_path_buf();
        assert_eq!(plugin.publish_root(), driver_root);
    }
}
