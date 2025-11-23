use crate::nanocloud::k8s::pod::{ListMeta, ObjectMeta};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PolicyRule {
    pub verbs: Vec<String>,
    #[serde(default, rename = "apiGroups", skip_serializing_if = "Vec::is_empty")]
    pub api_groups: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Role {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub rules: Vec<PolicyRule>,
}

impl Role {
    pub fn new(name: &str, namespace: Option<&str>, rules: Vec<PolicyRule>) -> Self {
        let mut metadata = ObjectMeta {
            name: Some(name.to_string()),
            namespace: namespace.map(|ns| ns.to_string()),
            ..Default::default()
        };
        metadata.ensure_common_fields(namespace, Some(name));
        Role {
            api_version: "nanocloud.io/v1".to_string(),
            kind: "Role".to_string(),
            metadata,
            rules,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RoleList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ListMeta,
    pub items: Vec<Role>,
}

impl RoleList {
    pub fn new(items: Vec<Role>, resource_version: String) -> Self {
        RoleList {
            api_version: "nanocloud.io/v1".to_string(),
            kind: "RoleList".to_string(),
            metadata: ListMeta {
                resource_version: Some(resource_version),
                continue_token: None,
                remaining_item_count: None,
            },
            items,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RoleRef {
    #[serde(rename = "apiGroup")]
    pub api_group: String,
    pub kind: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Subject {
    pub kind: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RoleBinding {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub subjects: Vec<Subject>,
    #[serde(rename = "roleRef")]
    pub role_ref: RoleRef,
}

impl RoleBinding {
    pub fn new(
        name: &str,
        namespace: Option<&str>,
        role_ref: RoleRef,
        subjects: Vec<Subject>,
    ) -> Self {
        let mut metadata = ObjectMeta {
            name: Some(name.to_string()),
            namespace: namespace.map(|ns| ns.to_string()),
            ..Default::default()
        };
        metadata.ensure_common_fields(namespace, Some(name));
        RoleBinding {
            api_version: "nanocloud.io/v1".to_string(),
            kind: "RoleBinding".to_string(),
            metadata,
            subjects,
            role_ref,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RoleBindingList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ListMeta,
    pub items: Vec<RoleBinding>,
}

impl RoleBindingList {
    pub fn new(items: Vec<RoleBinding>, resource_version: String) -> Self {
        RoleBindingList {
            api_version: "nanocloud.io/v1".to_string(),
            kind: "RoleBindingList".to_string(),
            metadata: ListMeta {
                resource_version: Some(resource_version),
                continue_token: None,
                remaining_item_count: None,
            },
            items,
        }
    }
}
