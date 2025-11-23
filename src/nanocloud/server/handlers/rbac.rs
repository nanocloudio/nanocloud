use super::error::ApiError;
use super::selectors::{matches_metadata_filter, parse_object_selector};
use super::watch::parse_resource_version;
use crate::nanocloud::k8s::rbac::{
    PolicyRule, Role, RoleBinding, RoleBindingList, RoleList, RoleRef, Subject,
};
use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Default, Deserialize)]
pub struct ListParams {
    #[serde(default)]
    watch: Option<bool>,
    #[serde(rename = "fieldSelector")]
    field_selector: Option<String>,
    #[serde(rename = "labelSelector")]
    label_selector: Option<String>,
    #[serde(rename = "resourceVersion")]
    resource_version: Option<String>,
}

pub async fn list_roles(Query(params): Query<ListParams>) -> Result<impl IntoResponse, ApiError> {
    list_roles_inner(None, params).await
}

pub async fn list_roles_namespaced(
    Path(namespace): Path<String>,
    Query(params): Query<ListParams>,
) -> Result<impl IntoResponse, ApiError> {
    list_roles_inner(Some(namespace.as_str()), params).await
}

pub async fn get_role(
    Path((namespace, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let roles = static_roles(Some(namespace.as_str()));
    let role = roles
        .into_iter()
        .find(|role| role.metadata.name.as_deref() == Some(name.as_str()))
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Role not found"))?;
    Ok(Json(role))
}

pub async fn get_role_cluster(Path(name): Path<String>) -> Result<impl IntoResponse, ApiError> {
    let roles = static_roles(None);
    let role = roles
        .into_iter()
        .find(|role| role.metadata.name.as_deref() == Some(name.as_str()))
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Role not found"))?;
    Ok(Json(role))
}

pub async fn list_role_bindings(
    Query(params): Query<ListParams>,
) -> Result<impl IntoResponse, ApiError> {
    list_bindings_inner(None, params).await
}

pub async fn list_role_bindings_namespaced(
    Path(namespace): Path<String>,
    Query(params): Query<ListParams>,
) -> Result<impl IntoResponse, ApiError> {
    list_bindings_inner(Some(namespace.as_str()), params).await
}

pub async fn get_role_binding(
    Path((namespace, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let bindings = static_role_bindings(Some(namespace.as_str()));
    let binding = bindings
        .into_iter()
        .find(|binding| binding.metadata.name.as_deref() == Some(name.as_str()))
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "RoleBinding not found"))?;
    Ok(Json(binding))
}

pub async fn get_role_binding_cluster(
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let bindings = static_role_bindings(None);
    let binding = bindings
        .into_iter()
        .find(|binding| binding.metadata.name.as_deref() == Some(name.as_str()))
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "RoleBinding not found"))?;
    Ok(Json(binding))
}

async fn list_roles_inner(
    namespace: Option<&str>,
    params: ListParams,
) -> Result<impl IntoResponse, ApiError> {
    if params.watch.unwrap_or(false) {
        return Err(ApiError::bad_request("watch is not supported for roles"));
    }
    let filter = parse_object_selector(
        params.field_selector.as_deref(),
        params.label_selector.as_deref(),
    )?
    .map(Arc::new);
    let _ = parse_resource_version(params.resource_version.as_deref())?; // accept but ignore for static roles

    let roles = static_roles(namespace);
    let selector_ref = filter.as_deref();
    let filtered: Vec<Role> = roles
        .into_iter()
        .filter(|role| matches_metadata_filter(selector_ref, &role.metadata))
        .collect();
    Ok(Json(RoleList::new(filtered, "1".to_string())))
}

async fn list_bindings_inner(
    namespace: Option<&str>,
    params: ListParams,
) -> Result<impl IntoResponse, ApiError> {
    if params.watch.unwrap_or(false) {
        return Err(ApiError::bad_request(
            "watch is not supported for rolebindings",
        ));
    }
    let filter = parse_object_selector(
        params.field_selector.as_deref(),
        params.label_selector.as_deref(),
    )?
    .map(Arc::new);
    let _ = parse_resource_version(params.resource_version.as_deref())?;

    let bindings = static_role_bindings(namespace);
    let selector_ref = filter.as_deref();
    let filtered: Vec<RoleBinding> = bindings
        .into_iter()
        .filter(|binding| matches_metadata_filter(selector_ref, &binding.metadata))
        .collect();
    Ok(Json(RoleBindingList::new(filtered, "1".to_string())))
}

fn static_roles(namespace: Option<&str>) -> Vec<Role> {
    let admin_rules = vec![PolicyRule {
        verbs: vec!["*".into()],
        api_groups: vec!["*".into()],
        resources: vec!["*".into()],
    }];
    let viewer_rules = vec![PolicyRule {
        verbs: vec!["get".into(), "list".into(), "watch".into()],
        api_groups: vec!["*".into()],
        resources: vec![
            "pods".into(),
            "services".into(),
            "endpoints".into(),
            "configmaps".into(),
            "secrets".into(),
            "persistentvolumeclaims".into(),
            "volumesnapshots".into(),
            "events".into(),
            "roles".into(),
            "rolebindings".into(),
        ],
    }];
    let device_rules = vec![PolicyRule {
        verbs: vec!["get".into(), "list".into()],
        api_groups: vec!["nanocloud.io".into()],
        resources: vec!["devices".into()],
    }];
    let service_account_rules = vec![PolicyRule {
        verbs: vec!["get".into(), "list".into()],
        api_groups: vec!["*".into()],
        resources: vec![
            "pods".into(),
            "configmaps".into(),
            "secrets".into(),
            "events".into(),
        ],
    }];

    vec![
        Role::new("admin", namespace, admin_rules),
        Role::new("viewer", namespace, viewer_rules),
        Role::new("device", namespace, device_rules),
        Role::new("service-account", namespace, service_account_rules),
    ]
}

fn static_role_bindings(namespace: Option<&str>) -> Vec<RoleBinding> {
    let mut bindings = Vec::new();
    for role in static_roles(namespace) {
        let role_name = role.metadata.name.clone().unwrap_or_default();
        let ns = role.metadata.namespace.clone();
        let binding_name = format!("{}-binding", role_name);
        let role_ref = RoleRef {
            api_group: "nanocloud.io".to_string(),
            kind: "Role".to_string(),
            name: role_name.clone(),
        };
        let subjects = vec![Subject {
            kind: "Group".to_string(),
            name: role_name.clone(),
            namespace: ns.clone(),
        }];
        let mut binding = RoleBinding::new(&binding_name, namespace, role_ref, subjects);
        binding.metadata.namespace = ns;
        bindings.push(binding);
    }
    bindings
}
