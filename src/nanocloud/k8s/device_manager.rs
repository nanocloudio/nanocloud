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

use crate::nanocloud::api::types::{Device, DeviceStatus};
use crate::nanocloud::k8s::store::{delete_device, list_devices, normalize_namespace, save_device};

use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;

const DEVICE_API_VERSION: &str = "nanocloud.io/v1";
const DEVICE_KIND: &str = "Device";

#[derive(Debug)]
pub enum DeviceError {
    AlreadyExists(String),
    NotFound(String),
    Invalid(String),
    Persistence(Box<dyn Error + Send + Sync>),
}

impl Display for DeviceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DeviceError::AlreadyExists(msg)
            | DeviceError::NotFound(msg)
            | DeviceError::Invalid(msg) => f.write_str(msg),
            DeviceError::Persistence(err) => write!(f, "{err}"),
        }
    }
}

impl Error for DeviceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DeviceError::Persistence(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

impl DeviceError {
    pub fn persistence_box(err: Box<dyn Error + Send + Sync>) -> Self {
        Self::Persistence(err)
    }
}

fn device_key(namespace: &str, name: &str) -> String {
    format!("{}/{}", normalize_namespace(Some(namespace)), name)
}

fn ensure_namespace(namespace: &str, device: &mut Device) -> String {
    let resolved = device
        .metadata
        .namespace
        .clone()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| namespace.to_string());
    device.metadata.namespace = Some(resolved.clone());
    resolved
}

fn ensure_device_payload(device: &mut Device) -> Result<(), DeviceError> {
    let hash = device.spec.hash.trim().to_string();
    if hash.is_empty() {
        return Err(DeviceError::Invalid("spec.hash is required".to_string()));
    }
    device.spec.hash = hash.clone();

    let expected_subject = format!("device:{hash}");
    let subject = device.spec.certificate_subject.trim();
    if subject.is_empty() {
        device.spec.certificate_subject = expected_subject.clone();
    } else if subject != expected_subject {
        return Err(DeviceError::Invalid(format!(
            "spec.certificateSubject must be '{expected_subject}'"
        )));
    }

    let expected_name = format!("device-{hash}");
    let current_name = device
        .metadata
        .name
        .clone()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| expected_name.clone());
    if current_name != expected_name {
        return Err(DeviceError::Invalid(format!(
            "metadata.name '{}' does not match expected device name '{}'",
            current_name, expected_name
        )));
    }
    device.metadata.name = Some(current_name);

    Ok(())
}

fn normalize_key_new(namespace: &str, device: &mut Device) -> Result<String, DeviceError> {
    let ns = ensure_namespace(namespace, device);
    ensure_device_payload(device)?;
    let name = device
        .metadata
        .name
        .clone()
        .expect("device name ensured to be present");
    Ok(device_key(&ns, &name))
}

pub struct DeviceRegistry {
    devices: RwLock<HashMap<String, Device>>,
    resource_counter: AtomicU64,
}

static REGISTRY: OnceLock<Arc<DeviceRegistry>> = OnceLock::new();

impl DeviceRegistry {
    pub fn shared() -> Arc<Self> {
        REGISTRY
            .get_or_init(|| {
                let (items, counter) = load_initial_devices();
                Arc::new(DeviceRegistry {
                    devices: RwLock::new(items),
                    resource_counter: AtomicU64::new(counter.max(1)),
                })
            })
            .clone()
    }

    pub async fn list(&self, namespace: Option<&str>) -> Vec<Device> {
        let devices = self.devices.read().await;
        devices
            .values()
            .filter(|device| match namespace {
                Some(target) => {
                    device.metadata.namespace.as_deref().unwrap_or("default")
                        == normalize_namespace(Some(target))
                }
                None => true,
            })
            .cloned()
            .collect()
    }

    pub async fn get(&self, namespace: &str, name: &str) -> Option<Device> {
        let key = device_key(namespace, name);
        let devices = self.devices.read().await;
        devices.get(&key).cloned()
    }

    pub async fn create(
        &self,
        namespace: &str,
        mut payload: Device,
    ) -> Result<Device, DeviceError> {
        if payload.api_version.is_empty() {
            payload.api_version = DEVICE_API_VERSION.to_string();
        }
        if payload.kind.is_empty() {
            payload.kind = DEVICE_KIND.to_string();
        }
        if payload.metadata.resource_version.is_some() {
            return Err(DeviceError::Invalid(
                "resourceVersion must not be set on create".to_string(),
            ));
        }

        let key = normalize_key_new(namespace, &mut payload)?;

        {
            let devices = self.devices.read().await;
            if devices.contains_key(&key) {
                return Err(DeviceError::AlreadyExists(format!(
                    "Device '{}' already exists",
                    payload.metadata.name.clone().unwrap_or_default()
                )));
            }
        }

        if payload.status.is_none() {
            payload.status = Some(DeviceStatus::default());
        }

        let resource_version = self.next_resource_version();
        payload.metadata.resource_version = Some(resource_version.clone());

        {
            let mut devices = self.devices.write().await;
            devices.insert(key.clone(), payload.clone());
        }

        let namespace = payload
            .metadata
            .namespace
            .clone()
            .expect("device namespace assigned during normalization");
        let name = payload
            .metadata
            .name
            .clone()
            .expect("device name assigned during normalization");

        save_device(Some(namespace.as_str()), &name, &payload)
            .map_err(DeviceError::persistence_box)?;

        Ok(payload)
    }

    pub async fn delete(&self, namespace: &str, name: &str) -> Result<Device, DeviceError> {
        let key = device_key(namespace, name);
        let removed = {
            let mut devices = self.devices.write().await;
            devices.remove(&key)
        };

        let device = match removed {
            Some(device) => device,
            None => {
                return Err(DeviceError::NotFound(format!(
                    "Device '{}' not found",
                    name
                )))
            }
        };

        delete_device(
            device.metadata.namespace.as_deref(),
            device.metadata.name.as_deref().unwrap_or(name),
        )
        .map_err(DeviceError::persistence_box)?;

        Ok(device)
    }

    pub async fn update_status(
        &self,
        namespace: &str,
        name: &str,
        status: DeviceStatus,
    ) -> Result<Device, DeviceError> {
        let key = device_key(namespace, name);
        let new_version = self.next_resource_version();

        {
            let mut devices = self.devices.write().await;
            let device = devices
                .get_mut(&key)
                .ok_or_else(|| DeviceError::NotFound(format!("Device '{name}' not found")))?;
            device.metadata.resource_version = Some(new_version.clone());
            device.status = Some(status.clone());
        }

        let device = self
            .devices
            .read()
            .await
            .get(&key)
            .cloned()
            .expect("device must exist after status update");

        let namespace = device
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let name = device
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| format!("device-{}", device.spec.hash));

        save_device(Some(namespace.as_str()), &name, &device)
            .map_err(DeviceError::persistence_box)?;

        Ok(device)
    }

    fn next_resource_version(&self) -> String {
        let next = self.resource_counter.fetch_add(1, Ordering::SeqCst);
        next.to_string()
    }
}

fn load_initial_devices() -> (HashMap<String, Device>, u64) {
    let mut items = HashMap::new();
    let mut max_version = 1u64;

    let existing = list_devices(None).unwrap_or_default();
    for mut device in existing {
        let namespace = device
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let name = device
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| format!("device-{}", device.spec.hash));

        if device.metadata.resource_version.is_none() {
            device.metadata.resource_version = Some("1".to_string());
        }
        if let Some(rv) = device.metadata.resource_version.as_deref() {
            if let Ok(parsed) = rv.parse::<u64>() {
                if parsed > max_version {
                    max_version = parsed;
                }
            }
        }

        let key = device_key(&namespace, &name);
        items.insert(key, device);
    }

    (items, max_version.saturating_add(1))
}
