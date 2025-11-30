use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;

use crate::nanocloud::csi::types::CapacityRange;
use crate::nanocloud::util::error::new_error;
use crate::nanocloud::util::security::volume::sanitize_identifier;

#[derive(Debug, Clone)]
pub struct VolumeParams {
    pub namespace: String,
    pub service: String,
    pub claim: String,
}

#[derive(Debug, Clone)]
pub struct VolumeIdentity {
    pub params: VolumeParams,
    pub volume_id: String,
}

pub fn namespace_from_params(parameters: &HashMap<String, String>) -> String {
    parameters
        .get("namespace")
        .map(|value| sanitize_identifier(value, "default"))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "default".to_string())
}

pub fn service_from_params(parameters: &HashMap<String, String>) -> Option<String> {
    parameters
        .get("service")
        .map(|value| sanitize_identifier(value, "service"))
        .filter(|value| !value.is_empty())
}

pub fn claim_from_params(parameters: &HashMap<String, String>) -> Option<String> {
    parameters
        .get("claim")
        .map(|value| sanitize_identifier(value, "claim"))
        .filter(|value| !value.is_empty())
}

pub fn sanitize_name(value: &str, fallback: &str) -> String {
    sanitize_identifier(value, fallback)
}

pub fn parse_volume_params(
    parameters: &HashMap<String, String>,
    request_name: &str,
) -> Result<VolumeIdentity, Box<dyn Error + Send + Sync>> {
    let namespace = namespace_from_params(parameters);
    let service = service_from_params(parameters)
        .ok_or_else(|| new_error("CreateVolumeRequest.parameters must include 'service'"))?;
    let claim = claim_from_params(parameters)
        .ok_or_else(|| new_error("CreateVolumeRequest.parameters must include 'claim'"))?;

    let base_name = if request_name.trim().is_empty() {
        format!("{}-{}-{}", namespace, service, claim)
    } else {
        request_name.trim().to_string()
    };
    let volume_id = sanitize_identifier(&base_name, "volume");
    if volume_id.is_empty() {
        return Err(new_error("Volume name is empty after sanitization"));
    }

    Ok(VolumeIdentity {
        params: VolumeParams {
            namespace,
            service,
            claim,
        },
        volume_id,
    })
}

pub fn validate_target_path(path: &str) -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    if path.trim().is_empty() {
        return Err(new_error("Target path is required"));
    }
    Ok(PathBuf::from(path))
}

pub fn required_capacity(range: &Option<CapacityRange>) -> u64 {
    range
        .as_ref()
        .and_then(|r| r.required_bytes)
        .or_else(|| range.as_ref().and_then(|r| r.limit_bytes))
        .unwrap_or(0)
}
