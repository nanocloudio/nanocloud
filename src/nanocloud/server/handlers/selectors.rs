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

//! Centralized request validation and selector parsing.
//!
//! This module provides utilities for parsing Kubernetes-style selectors
//! (field selectors and label selectors) and validating common request parameters.
//!
//! # Usage
//!
//! ```ignore
//! use crate::nanocloud::server::handlers::selectors::{
//!     parse_object_selector, validate_namespace, validate_resource_name
//! };
//!
//! // Parse selectors
//! let selector = parse_object_selector(
//!     Some("metadata.name=my-pod"),
//!     Some("app=web,env=prod")
//! )?;
//!
//! // Validate namespace
//! let ns = validate_namespace(&namespace)?;
//!
//! // Validate resource name
//! let name = validate_resource_name(&name, "Pod")?;
//! ```

use super::error::ApiError;
use crate::nanocloud::k8s::{
    configmap::ConfigMap,
    endpoints::Endpoints,
    persistentvolumeclaim::PersistentVolumeClaim,
    pod::{ObjectMeta, Pod},
    secret::Secret,
    service::Service,
};
use axum::http::StatusCode;
use std::collections::HashMap;

#[derive(Clone, Debug, Eq, PartialEq)]
struct LabelRequirement {
    key: String,
    operator: LabelOperator,
}

impl LabelRequirement {
    fn new(key: impl Into<String>, operator: LabelOperator) -> Self {
        Self {
            key: key.into(),
            operator,
        }
    }

    fn matches(&self, labels: &HashMap<String, String>) -> bool {
        let actual = labels.get(&self.key);
        match &self.operator {
            LabelOperator::Equals(expected) => actual == Some(expected),
            LabelOperator::NotEquals(expected) => actual != Some(expected),
            LabelOperator::In(allowed) => actual
                .map(|value| allowed.iter().any(|candidate| candidate == value))
                .unwrap_or(false),
            LabelOperator::NotIn(disallowed) => actual
                .map(|value| !disallowed.iter().any(|candidate| candidate == value))
                .unwrap_or(true),
            LabelOperator::Exists => actual.is_some(),
            LabelOperator::NotExists => actual.is_none(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LabelOperator {
    Equals(String),
    NotEquals(String),
    In(Vec<String>),
    NotIn(Vec<String>),
    Exists,
    NotExists,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectSelector {
    metadata_name: Option<String>,
    metadata_namespace: Option<String>,
    spec_node_name: Option<String>,
    status_phase: Option<String>,
    label_requirements: Vec<LabelRequirement>,
}

impl ObjectSelector {
    pub fn matches_config_map(&self, config_map: &ConfigMap) -> bool {
        self.matches_metadata(&config_map.metadata)
    }

    pub fn matches_pod(&self, pod: &Pod) -> bool {
        if !self.matches_metadata(&pod.metadata) {
            return false;
        }

        if let Some(expected) = &self.spec_node_name {
            if pod.spec.node_name.as_deref() != Some(expected.as_str()) {
                return false;
            }
        }

        if let Some(expected) = &self.status_phase {
            let actual = pod
                .status
                .as_ref()
                .and_then(|status| status.phase.as_deref());
            if actual != Some(expected.as_str()) {
                return false;
            }
        }

        true
    }

    pub fn matches_secret(&self, secret: &Secret) -> bool {
        self.matches_metadata(&secret.metadata)
    }

    pub fn matches_service(&self, service: &Service) -> bool {
        self.matches_metadata(&service.metadata)
    }

    pub fn matches_endpoints(&self, endpoints: &Endpoints) -> bool {
        self.matches_metadata(&endpoints.metadata)
    }

    pub fn matches_pvc(&self, pvc: &PersistentVolumeClaim) -> bool {
        self.matches_metadata(&pvc.metadata)
    }

    pub fn matches_object(&self, metadata: &ObjectMeta) -> bool {
        self.matches_metadata(metadata)
    }

    pub fn is_empty(&self) -> bool {
        self.metadata_name.is_none()
            && self.metadata_namespace.is_none()
            && self.spec_node_name.is_none()
            && self.status_phase.is_none()
            && self.label_requirements.is_empty()
    }

    #[cfg(test)]
    fn spec_node_name(&self) -> Option<&str> {
        self.spec_node_name.as_deref()
    }

    #[cfg(test)]
    fn status_phase(&self) -> Option<&str> {
        self.status_phase.as_deref()
    }

    fn matches_metadata(&self, metadata: &ObjectMeta) -> bool {
        if let Some(expected) = &self.metadata_name {
            let actual = metadata.name.as_deref().unwrap_or_default();
            if actual != expected {
                return false;
            }
        }

        if let Some(expected) = &self.metadata_namespace {
            let actual = metadata.namespace.as_deref().unwrap_or_default();
            if actual != expected {
                return false;
            }
        }

        self.label_requirements
            .iter()
            .all(|requirement| requirement.matches(&metadata.labels))
    }
}

pub fn parse_object_selector(
    field_selector: Option<&str>,
    label_selector: Option<&str>,
) -> Result<Option<ObjectSelector>, ApiError> {
    let mut selector = ObjectSelector::default();

    if let Some(field_selector) = field_selector {
        parse_field_selector(field_selector, &mut selector)?;
    }

    if let Some(label_selector) = label_selector {
        parse_label_selector(label_selector, &mut selector)?;
    }

    if selector.is_empty() {
        Ok(None)
    } else {
        Ok(Some(selector))
    }
}

pub fn ensure_named_resource<T, F>(
    resource: Option<T>,
    filter: Option<&ObjectSelector>,
    matcher: F,
    not_found_message: impl Into<String>,
) -> Result<T, ApiError>
where
    F: Fn(&T, &ObjectSelector) -> bool,
{
    match resource {
        Some(value) if filter.is_none_or(|selector| matcher(&value, selector)) => Ok(value),
        Some(_) | None => Err(ApiError::new(
            StatusCode::NOT_FOUND,
            not_found_message.into(),
        )),
    }
}

pub fn matches_pod_filter(filter: Option<&ObjectSelector>, pod: &Pod) -> bool {
    filter.is_none_or(|selector| selector.matches_pod(pod))
}

pub fn matches_config_map_filter(filter: Option<&ObjectSelector>, config_map: &ConfigMap) -> bool {
    filter
        .map(|selector| selector.matches_config_map(config_map))
        .unwrap_or(true)
}

pub fn matches_secret_filter(filter: Option<&ObjectSelector>, secret: &Secret) -> bool {
    filter
        .map(|selector| selector.matches_secret(secret))
        .unwrap_or(true)
}

pub fn matches_service_filter(filter: Option<&ObjectSelector>, service: &Service) -> bool {
    filter
        .map(|selector| selector.matches_service(service))
        .unwrap_or(true)
}

pub fn matches_endpoints_filter(filter: Option<&ObjectSelector>, endpoints: &Endpoints) -> bool {
    filter
        .map(|selector| selector.matches_endpoints(endpoints))
        .unwrap_or(true)
}

pub fn matches_pvc_filter(filter: Option<&ObjectSelector>, pvc: &PersistentVolumeClaim) -> bool {
    filter
        .map(|selector| selector.matches_pvc(pvc))
        .unwrap_or(true)
}

pub fn matches_metadata_filter(filter: Option<&ObjectSelector>, metadata: &ObjectMeta) -> bool {
    filter
        .map(|selector| selector.matches_object(metadata))
        .unwrap_or(true)
}

fn parse_field_selector(raw: &str, selector: &mut ObjectSelector) -> Result<(), ApiError> {
    for expr in split_selector_terms(raw) {
        let (left, right) = parse_field_equality(expr).ok_or_else(|| {
            ApiError::bad_request("Unsupported fieldSelector expression; expected key=value")
        })?;

        let normalized_key = left.trim();
        if normalized_key.is_empty() {
            return Err(ApiError::bad_request(
                "Unsupported fieldSelector expression; missing key",
            ));
        }

        let normalized_value = normalize_value(right);
        match normalized_key {
            "metadata.name" => selector.metadata_name = Some(normalized_value),
            "metadata.namespace" => selector.metadata_namespace = Some(normalized_value),
            "spec.nodeName" => selector.spec_node_name = Some(normalized_value),
            "status.phase" => selector.status_phase = Some(normalized_value),
            _ => {
                return Err(ApiError::bad_request(format!(
                    "Unsupported fieldSelector key '{}'",
                    normalized_key
                )))
            }
        }
    }

    Ok(())
}

fn parse_label_selector(raw: &str, selector: &mut ObjectSelector) -> Result<(), ApiError> {
    for expr in split_selector_terms(raw) {
        if expr.is_empty() {
            continue;
        }

        let requirement = parse_label_requirement(expr).map_err(ApiError::bad_request)?;
        selector.label_requirements.push(requirement);
    }

    Ok(())
}

fn parse_label_requirement(expr: &str) -> Result<LabelRequirement, String> {
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        return Err("Unsupported labelSelector requirement: empty expression".to_string());
    }

    if trimmed.starts_with('!') {
        let key = trimmed.trim_start_matches('!').trim();
        if key.is_empty() {
            return Err(format!(
                "Unsupported labelSelector requirement '{}'; missing key",
                expr
            ));
        }
        return Ok(LabelRequirement::new(key, LabelOperator::NotExists));
    }

    if let Some(requirement) = parse_set_requirement(trimmed)? {
        return Ok(requirement);
    }

    if let Some((operator, left, right)) = parse_label_equality(trimmed) {
        let key = left.trim();
        if key.is_empty() {
            return Err(format!(
                "Unsupported labelSelector requirement '{}'; missing key",
                expr
            ));
        }

        let value = normalize_value(right);
        match operator {
            EqualityOperator::Equals => {
                return Ok(LabelRequirement::new(key, LabelOperator::Equals(value)))
            }
            EqualityOperator::NotEquals => {
                return Ok(LabelRequirement::new(key, LabelOperator::NotEquals(value)))
            }
        }
    }

    let key = trimmed;
    if key.is_empty() {
        return Err("Unsupported labelSelector requirement: empty expression".to_string());
    }

    if key.contains(' ') {
        return Err(format!(
            "Unsupported labelSelector requirement '{}'; expected operator",
            expr
        ));
    }

    Ok(LabelRequirement::new(key, LabelOperator::Exists))
}

fn parse_set_requirement(expr: &str) -> Result<Option<LabelRequirement>, String> {
    let Some(start) = expr.find('(') else {
        return Ok(None);
    };

    let Some(end) = expr.rfind(')') else {
        return Err(format!(
            "Unsupported labelSelector requirement '{}'; missing closing ')'",
            expr
        ));
    };

    if end < start {
        return Err(format!(
            "Unsupported labelSelector requirement '{}'; mismatched parentheses",
            expr
        ));
    }

    if !expr[end + 1..].trim().is_empty() {
        return Err(format!(
            "Unsupported labelSelector requirement '{}'; unexpected trailing characters",
            expr
        ));
    }

    let head = expr[..start].trim();
    let mut parts = head.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(format!(
            "Unsupported labelSelector requirement '{}'; expected '<key> <operator> (...)'",
            expr
        ));
    }

    let key = parts.remove(0);
    let operator = parts.remove(0);
    let values_segment = &expr[start + 1..end];
    let values = parse_value_list(values_segment)?;

    let requirement = match operator {
        "in" => LabelRequirement::new(key, LabelOperator::In(values)),
        "notin" => LabelRequirement::new(key, LabelOperator::NotIn(values)),
        _ => {
            return Err(format!(
                "Unsupported labelSelector requirement '{}'; unknown set operator '{}'",
                expr, operator
            ))
        }
    };

    Ok(Some(requirement))
}

fn parse_value_list(segment: &str) -> Result<Vec<String>, String> {
    let values = split_selector_terms(segment)
        .map(normalize_value)
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Err("Unsupported labelSelector requirement; empty set".to_string());
    }
    Ok(values)
}

fn split_selector_terms(raw: &str) -> impl Iterator<Item = &str> {
    let mut terms = Vec::new();
    let mut start = 0;
    let mut depth = 0;
    for (idx, ch) in raw.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            ',' if depth == 0 => {
                let slice = raw[start..idx].trim();
                if !slice.is_empty() {
                    terms.push(slice);
                }
                start = idx + 1;
            }
            _ => {}
        }
    }

    let slice = raw[start..].trim();
    if !slice.is_empty() {
        terms.push(slice);
    }

    terms.into_iter()
}

#[derive(Clone, Copy)]
enum EqualityOperator {
    Equals,
    NotEquals,
}

fn parse_label_equality(expr: &str) -> Option<(EqualityOperator, &str, &str)> {
    if let Some((left, right)) = expr.split_once("!=") {
        return Some((EqualityOperator::NotEquals, left, right));
    }
    if let Some((left, right)) = expr.split_once("==") {
        return Some((EqualityOperator::Equals, left, right));
    }
    expr.split_once('=')
        .map(|(left, right)| (EqualityOperator::Equals, left, right))
}

fn parse_field_equality(expr: &str) -> Option<(&str, &str)> {
    if let Some((left, right)) = expr.split_once("==") {
        Some((left, right))
    } else {
        expr.split_once('=')
    }
}

fn normalize_value(value: &str) -> String {
    let trimmed = value.trim();
    if let Some(stripped) = trimmed
        .strip_prefix('"')
        .and_then(|inner| inner.strip_suffix('"'))
    {
        return stripped.to_string();
    }
    if let Some(stripped) = trimmed
        .strip_prefix('\'')
        .and_then(|inner| inner.strip_suffix('\''))
    {
        return stripped.to_string();
    }
    trimmed.to_string()
}

// ============================================================================
// Request Validation Helpers
// ============================================================================

/// Maximum length for Kubernetes resource names (DNS subdomain).
const MAX_NAME_LENGTH: usize = 253;

/// Maximum length for Kubernetes namespace names (DNS label).
const MAX_NAMESPACE_LENGTH: usize = 63;

/// Validate a Kubernetes namespace name.
///
/// Kubernetes namespace names must:
/// - Be non-empty
/// - Be at most 63 characters
/// - Contain only lowercase alphanumeric characters or '-'
/// - Start with an alphabetic character
/// - End with an alphanumeric character
#[allow(dead_code)]
pub fn validate_namespace(namespace: &str) -> Result<&str, ApiError> {
    if namespace.is_empty() {
        return Err(ApiError::bad_request("namespace cannot be empty"));
    }

    if namespace.len() > MAX_NAMESPACE_LENGTH {
        return Err(ApiError::bad_request(format!(
            "namespace cannot exceed {} characters",
            MAX_NAMESPACE_LENGTH
        )));
    }

    if !is_valid_dns_label(namespace) {
        return Err(ApiError::bad_request(
            "namespace must be a valid DNS label (lowercase alphanumeric with hyphens, starting with letter)",
        ));
    }

    Ok(namespace)
}

/// Validate a Kubernetes resource name.
///
/// Kubernetes resource names must:
/// - Be non-empty
/// - Be at most 253 characters (DNS subdomain)
/// - Contain only lowercase alphanumeric characters, '-' or '.'
/// - Start and end with an alphanumeric character
#[allow(dead_code)]
pub fn validate_resource_name<'a>(name: &'a str, resource_kind: &str) -> Result<&'a str, ApiError> {
    if name.is_empty() {
        return Err(ApiError::bad_request(format!(
            "{} name cannot be empty",
            resource_kind
        )));
    }

    if name.len() > MAX_NAME_LENGTH {
        return Err(ApiError::bad_request(format!(
            "{} name cannot exceed {} characters",
            resource_kind, MAX_NAME_LENGTH
        )));
    }

    if !is_valid_dns_subdomain(name) {
        return Err(ApiError::bad_request(format!(
            "{} name must be a valid DNS subdomain (lowercase alphanumeric with hyphens/dots, \
             starting and ending with alphanumeric)",
            resource_kind
        )));
    }

    Ok(name)
}

/// Validate that a string is a valid positive integer for limit/timeout parameters.
#[allow(dead_code)]
pub fn validate_positive_integer(value: &str, param_name: &str) -> Result<u64, ApiError> {
    value.parse::<u64>().map_err(|_| {
        ApiError::bad_request(format!(
            "{} must be a positive integer",
            param_name
        ))
    }).and_then(|n| {
        if n == 0 {
            Err(ApiError::bad_request(format!(
                "{} must be greater than 0",
                param_name
            )))
        } else {
            Ok(n)
        }
    })
}

/// Validate that a limit value is within bounds.
#[allow(dead_code)]
pub fn validate_limit(limit: u32, max_allowed: u32) -> Result<u32, ApiError> {
    if limit == 0 {
        return Err(ApiError::bad_request("limit must be greater than 0"));
    }
    if limit > max_allowed {
        return Err(ApiError::bad_request(format!(
            "limit cannot exceed {}",
            max_allowed
        )));
    }
    Ok(limit)
}

/// Validate a container name.
#[allow(dead_code)]
pub fn validate_container_name(name: &str) -> Result<&str, ApiError> {
    if name.is_empty() {
        return Err(ApiError::bad_request("container name cannot be empty"));
    }

    if name.len() > MAX_NAMESPACE_LENGTH {
        return Err(ApiError::bad_request(format!(
            "container name cannot exceed {} characters",
            MAX_NAMESPACE_LENGTH
        )));
    }

    // Container names follow DNS label rules
    if !is_valid_dns_label(name) {
        return Err(ApiError::bad_request(
            "container name must be a valid DNS label",
        ));
    }

    Ok(name)
}

/// Check if a string is a valid DNS label (RFC 1123).
fn is_valid_dns_label(s: &str) -> bool {
    if s.is_empty() || s.len() > 63 {
        return false;
    }

    let chars: Vec<char> = s.chars().collect();

    // Must start with a letter
    if !chars[0].is_ascii_lowercase() {
        return false;
    }

    // Must end with alphanumeric
    if !chars.last().unwrap().is_ascii_alphanumeric() {
        return false;
    }

    // All characters must be lowercase alphanumeric or hyphen
    chars.iter().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '-')
}

/// Check if a string is a valid DNS subdomain (RFC 1123).
fn is_valid_dns_subdomain(s: &str) -> bool {
    if s.is_empty() || s.len() > 253 {
        return false;
    }

    let chars: Vec<char> = s.chars().collect();

    // Must start with alphanumeric
    if !chars[0].is_ascii_alphanumeric() {
        return false;
    }

    // Must end with alphanumeric
    if !chars.last().unwrap().is_ascii_alphanumeric() {
        return false;
    }

    // All characters must be lowercase alphanumeric, hyphen, or dot
    chars.iter().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '-' || *c == '.')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::pod::{Pod, PodSecurityContext, PodSpec, PodStatus};
    use axum::response::IntoResponse;
    use std::collections::HashMap;

    fn metadata(name: &str, namespace: &str, labels: &[(&str, &str)]) -> ObjectMeta {
        ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            annotations: HashMap::new(),
            ..Default::default()
        }
    }

    fn pod(
        name: &str,
        namespace: &str,
        labels: &[(&str, &str)],
        node_name: Option<&str>,
        phase: Option<&str>,
    ) -> Pod {
        Pod {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            metadata: metadata(name, namespace, labels),
            spec: PodSpec {
                init_containers: Vec::new(),
                containers: Vec::new(),
                volumes: Vec::new(),
                restart_policy: None,
                service_account_name: None,
                node_name: node_name.map(|value| value.to_string()),
                host_network: false,
                security: PodSecurityContext::default(),
                node_selector: HashMap::new(),
            },
            status: phase.map(|value| PodStatus {
                phase: Some(value.to_string()),
                ..PodStatus::default()
            }),
        }
    }

    #[test]
    fn empty_selectors_return_none() {
        assert_eq!(parse_object_selector(None, None).unwrap(), None);
        assert_eq!(parse_object_selector(Some("   "), Some("")).unwrap(), None);
    }

    #[test]
    fn parse_field_selector_for_name_and_namespace() {
        let selector =
            parse_object_selector(Some("metadata.name=web,metadata.namespace=default"), None)
                .unwrap()
                .unwrap();
        let meta = metadata("web", "default", &[]);
        assert!(selector.matches_metadata(&meta));
        let wrong_name = metadata("worker", "default", &[]);
        assert!(!selector.matches_metadata(&wrong_name));
        let wrong_namespace = metadata("web", "prod", &[]);
        assert!(!selector.matches_metadata(&wrong_namespace));
    }

    #[test]
    fn parse_field_selector_with_quotes() {
        let selector = parse_object_selector(Some("metadata.name=\"web\""), None)
            .unwrap()
            .unwrap();
        let meta = metadata("web", "ns", &[]);
        assert!(selector.matches_metadata(&meta));
    }

    #[test]
    fn parse_field_selector_accepts_node_and_phase() {
        let selector = parse_object_selector(
            Some("spec.nodeName = \"node-a\", status.phase=Running"),
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!(selector.spec_node_name(), Some("node-a"));
        assert_eq!(selector.status_phase(), Some("Running"));
    }

    #[test]
    fn pod_selector_evaluates_spec_and_status_fields() {
        let selector = parse_object_selector(
            Some("spec.nodeName=node-a,status.phase=Running"),
            Some("role=api"),
        )
        .unwrap()
        .unwrap();

        let matching = pod(
            "pod-a",
            "default",
            &[("role", "api")],
            Some("node-a"),
            Some("Running"),
        );
        assert!(selector.matches_pod(&matching));

        let wrong_node = pod(
            "pod-a",
            "default",
            &[("role", "api")],
            Some("node-b"),
            Some("Running"),
        );
        assert!(!selector.matches_pod(&wrong_node));

        let wrong_phase = pod(
            "pod-a",
            "default",
            &[("role", "api")],
            Some("node-a"),
            Some("Pending"),
        );
        assert!(!selector.matches_pod(&wrong_phase));

        let missing_phase = pod("pod-a", "default", &[("role", "api")], Some("node-a"), None);
        assert!(!selector.matches_pod(&missing_phase));
    }

    #[test]
    fn parse_label_selector_equality() {
        let selector = parse_object_selector(None, Some("app=web,env=prod"))
            .unwrap()
            .unwrap();
        let meta = metadata("web", "default", &[("app", "web"), ("env", "prod")]);
        assert!(selector.matches_metadata(&meta));
        let missing_label = metadata("web", "default", &[("app", "web")]);
        assert!(!selector.matches_metadata(&missing_label));
    }

    #[test]
    fn parse_mixed_selectors() {
        let selector =
            parse_object_selector(Some("metadata.name = web"), Some("tier=frontend")).unwrap();
        let selector = selector.unwrap();
        let meta = metadata(
            "web",
            "default",
            &[("tier", "frontend"), ("component", "nginx")],
        );
        assert!(selector.matches_metadata(&meta));
        let meta = metadata("web", "default", &[("tier", "backend")]);
        assert!(!selector.matches_metadata(&meta));
    }

    #[test]
    fn unsupported_field_selector_errors() {
        let err = parse_object_selector(Some("status.reason=Evicted"), None).unwrap_err();
        let debug = format!("{:?}", err);
        assert!(debug.contains("Unsupported fieldSelector key"));
    }

    #[test]
    fn unsupported_label_selector_operator_errors() {
        let err = parse_object_selector(None, Some("app ~~ web")).unwrap_err();
        let debug = format!("{:?}", err);
        assert!(debug.contains("Unsupported labelSelector requirement"));
    }

    #[test]
    fn parse_label_selector_with_set_operators() {
        let selector =
            parse_object_selector(None, Some("app in (web,api),tier notin (backend),track"))
                .unwrap()
                .unwrap();
        let matching = metadata(
            "pod",
            "ns",
            &[("app", "api"), ("tier", "frontend"), ("track", "stable")],
        );
        assert!(selector.matches_metadata(&matching));
        let failing_in = metadata(
            "pod",
            "ns",
            &[("app", "worker"), ("tier", "frontend"), ("track", "stable")],
        );
        assert!(!selector.matches_metadata(&failing_in));
        let failing_notin = metadata(
            "pod",
            "ns",
            &[("app", "api"), ("tier", "backend"), ("track", "stable")],
        );
        assert!(!selector.matches_metadata(&failing_notin));
        let failing_exists = metadata("pod", "ns", &[("app", "api"), ("tier", "frontend")]);
        assert!(!selector.matches_metadata(&failing_exists));
    }

    #[test]
    fn parse_label_selector_not_exists_expression() {
        let selector = parse_object_selector(None, Some("!debug"))
            .unwrap()
            .unwrap();
        let matching = metadata("pod", "ns", &[("app", "api")]);
        assert!(selector.matches_metadata(&matching));
        let failing = metadata("pod", "ns", &[("debug", "true")]);
        assert!(!selector.matches_metadata(&failing));
    }

    #[test]
    fn parse_label_selector_inequality_expression() {
        let selector = parse_object_selector(None, Some("env!=prod"))
            .unwrap()
            .unwrap();
        let matching_without_label = metadata("pod", "ns", &[]);
        assert!(selector.matches_metadata(&matching_without_label));
        let matching_with_different_value = metadata("pod", "ns", &[("env", "staging")]);
        assert!(selector.matches_metadata(&matching_with_different_value));
        let failing = metadata("pod", "ns", &[("env", "prod")]);
        assert!(!selector.matches_metadata(&failing));
    }

    #[test]
    fn parse_label_selector_rejects_empty_set() {
        let err = parse_object_selector(None, Some("app in ()")).unwrap_err();
        let debug = format!("{:?}", err);
        assert!(debug.contains("empty set"));
    }

    #[test]
    fn ensure_named_resource_accepts_matching_selector() {
        #[derive(Clone)]
        struct Wrapper {
            metadata: ObjectMeta,
        }

        let selector = parse_object_selector(Some("metadata.name=web"), None)
            .unwrap()
            .unwrap();
        let resource = Wrapper {
            metadata: metadata("web", "default", &[("app", "demo")]),
        };

        let unwrapped = ensure_named_resource(
            Some(resource),
            Some(&selector),
            |item: &Wrapper, selector: &ObjectSelector| selector.matches_metadata(&item.metadata),
            "not found",
        )
        .expect("selector should accept matching resource");
        assert_eq!(
            unwrapped.metadata.name.as_deref(),
            Some("web"),
            "resource should be returned intact"
        );
    }

    #[test]
    fn ensure_named_resource_rejects_mismatched_selector() {
        #[derive(Clone)]
        struct Wrapper {
            metadata: ObjectMeta,
        }

        let selector = parse_object_selector(Some("metadata.name=worker"), None)
            .unwrap()
            .unwrap();
        let resource = Wrapper {
            metadata: metadata("web", "default", &[]),
        };

        let error = ensure_named_resource(
            Some(resource),
            Some(&selector),
            |item: &Wrapper, selector: &ObjectSelector| selector.matches_metadata(&item.metadata),
            "pod not found",
        )
        .err()
        .expect("selector mismatch should be rejected");
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // ========================================================================
    // Validation Helper Tests
    // ========================================================================

    #[test]
    fn validate_namespace_accepts_valid_names() {
        assert!(validate_namespace("default").is_ok());
        assert!(validate_namespace("kube-system").is_ok());
        assert!(validate_namespace("my-app-prod").is_ok());
        assert!(validate_namespace("a1b2c3").is_ok());
    }

    #[test]
    fn validate_namespace_rejects_invalid_names() {
        assert!(validate_namespace("").is_err());
        assert!(validate_namespace("Default").is_err()); // uppercase
        assert!(validate_namespace("-invalid").is_err()); // starts with hyphen
        assert!(validate_namespace("invalid-").is_err()); // ends with hyphen
        assert!(validate_namespace("has_underscore").is_err());
        assert!(validate_namespace("has.dot").is_err());
        assert!(validate_namespace("123numeric").is_err()); // starts with number
    }

    #[test]
    fn validate_namespace_rejects_too_long() {
        let long_name = "a".repeat(64);
        assert!(validate_namespace(&long_name).is_err());
    }

    #[test]
    fn validate_resource_name_accepts_valid_names() {
        assert!(validate_resource_name("my-pod", "Pod").is_ok());
        assert!(validate_resource_name("web.server.v1", "Pod").is_ok());
        assert!(validate_resource_name("a", "Pod").is_ok());
    }

    #[test]
    fn validate_resource_name_rejects_invalid_names() {
        assert!(validate_resource_name("", "Pod").is_err());
        assert!(validate_resource_name("My-Pod", "Pod").is_err()); // uppercase
        assert!(validate_resource_name("-pod", "Pod").is_err()); // starts with hyphen
        assert!(validate_resource_name("pod-", "Pod").is_err()); // ends with hyphen
        assert!(validate_resource_name(".pod", "Pod").is_err()); // starts with dot
        assert!(validate_resource_name("pod.", "Pod").is_err()); // ends with dot
    }

    #[test]
    fn validate_positive_integer_accepts_valid() {
        assert_eq!(validate_positive_integer("1", "limit").unwrap(), 1);
        assert_eq!(validate_positive_integer("100", "limit").unwrap(), 100);
        assert_eq!(validate_positive_integer("999999", "timeout").unwrap(), 999999);
    }

    #[test]
    fn validate_positive_integer_rejects_invalid() {
        assert!(validate_positive_integer("0", "limit").is_err());
        assert!(validate_positive_integer("-1", "limit").is_err());
        assert!(validate_positive_integer("abc", "limit").is_err());
        assert!(validate_positive_integer("", "limit").is_err());
    }

    #[test]
    fn validate_limit_checks_bounds() {
        assert!(validate_limit(1, 100).is_ok());
        assert!(validate_limit(100, 100).is_ok());
        assert!(validate_limit(0, 100).is_err());
        assert!(validate_limit(101, 100).is_err());
    }

    #[test]
    fn validate_container_name_accepts_valid() {
        assert!(validate_container_name("nginx").is_ok());
        assert!(validate_container_name("my-sidecar").is_ok());
        assert!(validate_container_name("init-container1").is_ok());
    }

    #[test]
    fn validate_container_name_rejects_invalid() {
        assert!(validate_container_name("").is_err());
        assert!(validate_container_name("NGINX").is_err());
        assert!(validate_container_name("-nginx").is_err());
        assert!(validate_container_name("nginx-").is_err());
    }

    #[test]
    fn is_valid_dns_label_handles_edge_cases() {
        assert!(is_valid_dns_label("a"));
        assert!(is_valid_dns_label("abc123"));
        assert!(!is_valid_dns_label("")); // empty
        assert!(!is_valid_dns_label("1abc")); // starts with digit
        assert!(!is_valid_dns_label("abc-")); // ends with hyphen
    }

    #[test]
    fn is_valid_dns_subdomain_handles_edge_cases() {
        assert!(is_valid_dns_subdomain("a"));
        assert!(is_valid_dns_subdomain("a.b.c"));
        assert!(is_valid_dns_subdomain("my-app.example.com"));
        assert!(!is_valid_dns_subdomain("")); // empty
        assert!(!is_valid_dns_subdomain(".abc")); // starts with dot
        assert!(!is_valid_dns_subdomain("abc.")); // ends with dot
    }

    // Performance regression tests to ensure selector parsing scales well

    #[test]
    fn parse_label_selector_many_requirements() {
        // Test parsing a selector with many label requirements
        let parts: Vec<String> = (0..50)
            .map(|i| format!("label{}=value{}", i, i))
            .collect();
        let selector_str = parts.join(",");

        let result = parse_object_selector(None, Some(&selector_str));
        assert!(result.is_ok());
    }

    #[test]
    fn parse_label_selector_long_values() {
        // Test parsing labels with long key/value strings
        let long_key = "a".repeat(63); // max DNS label length
        let long_value = "v".repeat(63);
        let selector_str = format!("{}={}", long_key, long_value);

        let result = parse_object_selector(None, Some(&selector_str));
        assert!(result.is_ok());
    }

    #[test]
    fn parse_label_selector_set_with_many_values() {
        // Test In operator with many values
        let values: Vec<String> = (0..100).map(|i| format!("val{}", i)).collect();
        let selector_str = format!("env in ({})", values.join(","));

        let result = parse_object_selector(None, Some(&selector_str));
        assert!(result.is_ok());
    }

    #[test]
    fn parse_combined_selectors_stress() {
        // Test combined field and label selectors with multiple requirements
        let field_selector = "metadata.name=my-pod,metadata.namespace=default";
        let label_parts: Vec<String> = (0..20)
            .map(|i| format!("app-label-{}=value-{}", i, i))
            .collect();
        let label_selector = label_parts.join(",");

        let result = parse_object_selector(Some(field_selector), Some(&label_selector));
        assert!(result.is_ok());
    }

    #[test]
    fn matches_labels_many_requirements() {
        // Test matching against many label requirements
        let label_parts: Vec<String> = (0..50)
            .map(|i| format!("label{}=value{}", i, i))
            .collect();
        let selector_str = label_parts.join(",");
        let selector = parse_object_selector(None, Some(&selector_str)).unwrap().unwrap();

        // Build matching labels
        let mut labels: HashMap<String, String> = HashMap::new();
        for i in 0..50 {
            labels.insert(format!("label{}", i), format!("value{}", i));
        }

        let metadata = ObjectMeta {
            name: Some("test-pod".to_string()),
            namespace: Some("default".to_string()),
            labels,
            ..Default::default()
        };

        assert!(selector.matches_object(&metadata));
    }

    #[test]
    fn matches_labels_large_label_set() {
        // Test matching when object has many labels
        let selector = parse_object_selector(None, Some("target=match")).unwrap().unwrap();

        // Build object with many labels
        let mut labels: HashMap<String, String> = HashMap::new();
        for i in 0..1000 {
            labels.insert(format!("label{}", i), format!("value{}", i));
        }
        labels.insert("target".to_string(), "match".to_string());

        let metadata = ObjectMeta {
            name: Some("test-pod".to_string()),
            namespace: Some("default".to_string()),
            labels,
            ..Default::default()
        };

        assert!(selector.matches_object(&metadata));
    }

    #[test]
    fn parse_selector_repeated_iterations() {
        // Verify parsing is consistent across many iterations
        let field_selector = "metadata.name=test-pod";
        let label_selector = "app=web,env=prod,tier=frontend";

        for _ in 0..100 {
            let result = parse_object_selector(Some(field_selector), Some(label_selector));
            assert!(result.is_ok());
            let selector = result.unwrap().unwrap();
            assert!(selector.metadata_name.is_some());
        }
    }

    #[test]
    fn validate_namespace_repeated() {
        // Stress test namespace validation
        for _ in 0..1000 {
            assert!(validate_namespace("default").is_ok());
            assert!(validate_namespace("kube-system").is_ok());
            assert!(validate_namespace("my-namespace-123").is_ok());
        }
    }

    #[test]
    fn validate_resource_name_repeated() {
        // Stress test resource name validation
        for _ in 0..1000 {
            assert!(validate_resource_name("my-pod", "Pod").is_ok());
            assert!(validate_resource_name("nginx-deployment-abc123", "Deployment").is_ok());
        }
    }

    #[test]
    fn parse_complex_set_expression() {
        // Test complex set expressions
        let selector = "env in (prod,staging,dev),tier notin (backend),!disabled,enabled";
        let result = parse_object_selector(None, Some(selector));
        assert!(result.is_ok());
    }

    #[test]
    fn label_requirement_matching_performance() {
        // Test many matches against same selector
        let selector = parse_object_selector(
            None,
            Some("app=web,env=prod,tier=frontend,version=v1"),
        )
        .unwrap()
        .unwrap();

        let mut labels: HashMap<String, String> = HashMap::new();
        labels.insert("app".to_string(), "web".to_string());
        labels.insert("env".to_string(), "prod".to_string());
        labels.insert("tier".to_string(), "frontend".to_string());
        labels.insert("version".to_string(), "v1".to_string());

        let metadata = ObjectMeta {
            name: Some("test".to_string()),
            namespace: Some("default".to_string()),
            labels,
            ..Default::default()
        };

        // Run many matches
        for _ in 0..1000 {
            assert!(selector.matches_object(&metadata));
        }
    }

    #[test]
    fn parse_deeply_nested_quoted_values() {
        // Test quoted values with special characters
        let selector = r#"annotation="value with spaces",tag="key=value""#;
        let result = parse_object_selector(None, Some(selector));
        // This may fail depending on quote handling, but shouldn't panic
        let _ = result;
    }

    #[test]
    fn empty_selectors_parse_quickly() {
        // Empty selectors should be fast
        for _ in 0..1000 {
            let result = parse_object_selector(None, None);
            assert!(result.is_ok());
            let result = parse_object_selector(Some(""), Some(""));
            assert!(result.is_ok());
        }
    }
}
