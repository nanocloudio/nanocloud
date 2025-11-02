use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use nanocloud::nanocloud::csi::provider::{csi_plugin, register_csi_plugin, CsiPlugin};
use nanocloud::nanocloud::csi::types::{
    CreateSnapshotRequest,
    CreateSnapshotResponse,
    CreateVolumeRequest,
    CreateVolumeResponse,
    DeleteSnapshotRequest,
    DeleteVolumeRequest,
    NodePublishVolumeRequest,
    NodePublishVolumeResponse,
    NodeUnpublishVolumeRequest,
    Volume as CsiVolume,
};
use nanocloud::nanocloud::csi::driver::StoredVolume;

struct StubCsiPlugin;

impl CsiPlugin for StubCsiPlugin {
    fn publish_root(&self) -> std::path::PathBuf {
        std::path::PathBuf::from("/tmp/csi-stub")
    }

    fn create_volume(
        &self,
        _request: CreateVolumeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CreateVolumeResponse, Box<dyn std::error::Error + Send + Sync>>> + Send>> {
        Box::pin(async { Ok(CreateVolumeResponse { volume: None }) })
    }

    fn delete_volume(
        &self,
        _request: DeleteVolumeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>> {
        Box::pin(async { Ok(()) })
    }

    fn node_publish_volume(
        &self,
        _request: NodePublishVolumeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<NodePublishVolumeResponse, Box<dyn std::error::Error + Send + Sync>>> + Send>> {
        Box::pin(async { Ok(NodePublishVolumeResponse {}) })
    }

    fn node_unpublish_volume(
        &self,
        _request: NodeUnpublishVolumeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>> {
        Box::pin(async { Ok(()) })
    }

    fn create_snapshot(
        &self,
        _request: CreateSnapshotRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CreateSnapshotResponse, Box<dyn std::error::Error + Send + Sync>>> + Send>> {
        Box::pin(async { Ok(CreateSnapshotResponse { snapshot: None }) })
    }

    fn delete_snapshot(
        &self,
        _request: DeleteSnapshotRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>> {
        Box::pin(async { Ok(()) })
    }

    fn list_service_volumes(
        &self,
        _namespace: &str,
        _service: &str,
    ) -> Result<Vec<StoredVolume>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Vec::new())
    }
}

#[test]
fn register_csi_plugin_overrides_default_driver() {
    let plugin = Arc::new(StubCsiPlugin);
    let _ = register_csi_plugin(plugin);

    let plugin = csi_plugin();
    assert_eq!(plugin.publish_root(), std::path::PathBuf::from("/tmp/csi-stub"));
}
