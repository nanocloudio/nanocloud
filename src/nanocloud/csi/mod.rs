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

mod driver;
mod provider;
mod types;

#[allow(unused_imports)]
// Re-exported for downstream crates/tests that depend on the public CSI API.
pub use driver::{CsiDriver, StoredVolume};
#[allow(unused_imports)]
// Re-exported for downstream crates/tests that depend on the public CSI API.
pub use provider::{csi_plugin, register_csi_plugin, CsiPlugin};
#[allow(unused_imports)]
// Re-exported so external callers can build CSI requests without reaching into submodules.
pub use types::{
    CreateSnapshotRequest, CreateVolumeRequest, DeleteSnapshotRequest, DeleteVolumeRequest,
    NodePublishVolumeRequest, NodeUnpublishVolumeRequest, Volume as CsiVolume,
};
