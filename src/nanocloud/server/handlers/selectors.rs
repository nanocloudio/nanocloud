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

use super::error::ApiError;
use crate::nanocloud::k8s::{
    configmap::ConfigMap,
    pod::{ObjectMeta, Pod},
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
            resource_version: None,
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
}
