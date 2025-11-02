#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::pod::{ListMeta, ObjectMeta};

/// Describes a single Service port mapping.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ServicePort {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub port: u16,
    #[serde(rename = "targetPort", skip_serializing_if = "Option::is_none")]
    pub target_port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ServiceSpec {
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub selector: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<ServicePort>,
    #[serde(rename = "clusterIP", skip_serializing_if = "Option::is_none")]
    pub cluster_ip: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,
}

impl Default for ServiceSpec {
    fn default() -> Self {
        ServiceSpec {
            selector: HashMap::new(),
            ports: Vec::new(),
            cluster_ip: None,
            type_name: Some("ClusterIP".to_string()),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ServiceStatus {
    #[serde(rename = "clusterIP", skip_serializing_if = "Option::is_none")]
    pub cluster_ip: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Service {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: ServiceSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ServiceStatus>,
}

impl Default for Service {
    fn default() -> Self {
        Service {
            api_version: "nanocloud.io/v1".to_string(),
            kind: "Service".to_string(),
            metadata: ObjectMeta::default(),
            spec: ServiceSpec::default(),
            status: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ServiceList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ListMeta,
    pub items: Vec<Service>,
}

impl Default for ServiceList {
    fn default() -> Self {
        ServiceList {
            api_version: "nanocloud.io/v1".to_string(),
            kind: "ServiceList".to_string(),
            metadata: ListMeta::default(),
            items: Vec::new(),
        }
    }
}
