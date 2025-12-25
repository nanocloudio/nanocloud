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

//! Route CRD types for edge ingress routing.
//!
//! Routes define how incoming HTTP requests are forwarded to backend Services
//! based on host and path matching rules.
//!
//! # Example Route
//!
//! ```yaml
//! apiVersion: nanocloud.io/v1
//! kind: Route
//! metadata:
//!   name: my-app
//!   namespace: default
//! spec:
//!   host: myapp.example.com
//!   pathPrefix: /api
//!   service:
//!     name: my-backend
//!     namespace: default
//!     port: 8080
//!   stripPrefix: true
//!   timeoutSeconds: 30
//! ```

use serde::{Deserialize, Serialize};

use super::pod::{ListMeta, ObjectMeta};

/// API version for Route resources.
pub const API_VERSION: &str = "nanocloud.io/v1";

/// Kind for Route resources.
pub const KIND: &str = "Route";

/// Reference to a backend Service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ServiceRef {
    /// Name of the target Service.
    pub name: String,

    /// Namespace of the target Service.
    /// Defaults to the Route's namespace if not specified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// Port on the target Service to forward traffic to.
    pub port: u16,
}

impl ServiceRef {
    /// Create a new ServiceRef with the given name and port.
    pub fn new(name: impl Into<String>, port: u16) -> Self {
        Self {
            name: name.into(),
            namespace: None,
            port,
        }
    }

    /// Set the namespace for this ServiceRef.
    #[must_use]
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Resolve the namespace, using the provided default if not set.
    pub fn resolved_namespace<'a>(&'a self, default: &'a str) -> &'a str {
        self.namespace.as_deref().unwrap_or(default)
    }
}

/// Specification for a Route.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RouteSpec {
    /// Hostname to match for incoming requests.
    /// Required field.
    pub host: String,

    /// Optional path prefix to match.
    /// If specified, only requests with paths starting with this prefix
    /// will be routed to the backend.
    #[serde(rename = "pathPrefix", skip_serializing_if = "Option::is_none")]
    pub path_prefix: Option<String>,

    /// Reference to the backend Service.
    pub service: ServiceRef,

    /// Whether to strip the path prefix before forwarding.
    /// Only applies when `path_prefix` is set.
    #[serde(rename = "stripPrefix", default, skip_serializing_if = "is_false")]
    pub strip_prefix: bool,

    /// Request timeout in seconds.
    /// Defaults to 30 seconds if not specified.
    #[serde(rename = "timeoutSeconds", skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,

    /// Response timeout in seconds (time to first byte).
    /// Defaults to the request timeout if not specified.
    #[serde(
        rename = "responseTimeoutSeconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub response_timeout_seconds: Option<u64>,
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl Default for RouteSpec {
    fn default() -> Self {
        Self {
            host: String::new(),
            path_prefix: None,
            service: ServiceRef {
                name: String::new(),
                namespace: None,
                port: 80,
            },
            strip_prefix: false,
            timeout_seconds: None,
            response_timeout_seconds: None,
        }
    }
}

impl RouteSpec {
    /// Create a new RouteSpec with the given host and service.
    pub fn new(host: impl Into<String>, service: ServiceRef) -> Self {
        Self {
            host: host.into(),
            service,
            ..Default::default()
        }
    }

    /// Set the path prefix.
    #[must_use]
    pub fn with_path_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.path_prefix = Some(prefix.into());
        self
    }

    /// Enable prefix stripping.
    #[must_use]
    pub fn with_strip_prefix(mut self, strip: bool) -> Self {
        self.strip_prefix = strip;
        self
    }

    /// Set the request timeout.
    #[must_use]
    pub fn with_timeout(mut self, seconds: u64) -> Self {
        self.timeout_seconds = Some(seconds);
        self
    }

    /// Get the effective timeout in seconds.
    pub fn effective_timeout(&self) -> u64 {
        self.timeout_seconds.unwrap_or(30)
    }

    /// Get the effective response timeout in seconds.
    pub fn effective_response_timeout(&self) -> u64 {
        self.response_timeout_seconds
            .or(self.timeout_seconds)
            .unwrap_or(30)
    }

    /// Validate the RouteSpec.
    pub fn validate(&self) -> Result<(), RouteValidationError> {
        if self.host.is_empty() {
            return Err(RouteValidationError::MissingHost);
        }

        if self.service.name.is_empty() {
            return Err(RouteValidationError::MissingServiceName);
        }

        if self.service.port == 0 {
            return Err(RouteValidationError::InvalidServicePort);
        }

        // Validate path prefix format if present
        if let Some(ref prefix) = self.path_prefix {
            if !prefix.starts_with('/') {
                return Err(RouteValidationError::InvalidPathPrefix(
                    "path prefix must start with '/'".to_string(),
                ));
            }
        }

        Ok(())
    }
}

/// Condition for Route status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RouteCondition {
    /// Type of condition (e.g., "Ready", "ServiceResolved").
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status of the condition ("True", "False", "Unknown").
    pub status: String,

    /// Last time the condition transitioned.
    #[serde(rename = "lastTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,

    /// Human-readable reason for the condition.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Human-readable message with details.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl RouteCondition {
    /// Create a new condition with the given type and status.
    pub fn new(condition_type: impl Into<String>, status: impl Into<String>) -> Self {
        Self {
            condition_type: condition_type.into(),
            status: status.into(),
            last_transition_time: None,
            reason: None,
            message: None,
        }
    }

    /// Create a "Ready" condition set to True.
    pub fn ready() -> Self {
        Self::new("Ready", "True")
    }

    /// Create a "Ready" condition set to False with reason.
    pub fn not_ready(reason: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            condition_type: "Ready".to_string(),
            status: "False".to_string(),
            last_transition_time: None,
            reason: Some(reason.into()),
            message: Some(message.into()),
        }
    }
}

/// Status of a Route.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RouteStatus {
    /// Conditions describing the current state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<RouteCondition>,

    /// Resolved backend endpoint (IP:port).
    #[serde(rename = "resolvedEndpoint", skip_serializing_if = "Option::is_none")]
    pub resolved_endpoint: Option<String>,

    /// Number of active connections through this route.
    #[serde(rename = "activeConnections", skip_serializing_if = "Option::is_none")]
    pub active_connections: Option<u64>,
}

impl RouteStatus {
    /// Check if the Route is ready.
    pub fn is_ready(&self) -> bool {
        self.conditions
            .iter()
            .any(|c| c.condition_type == "Ready" && c.status == "True")
    }

    /// Set the Ready condition.
    pub fn set_ready(&mut self, ready: bool, reason: Option<&str>, message: Option<&str>) {
        // Remove existing Ready condition
        self.conditions.retain(|c| c.condition_type != "Ready");

        let condition = if ready {
            RouteCondition::ready()
        } else {
            RouteCondition::not_ready(
                reason.unwrap_or("Unknown"),
                message.unwrap_or("Route is not ready"),
            )
        };

        self.conditions.push(condition);
    }
}

/// Route resource for edge ingress routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Route {
    /// API version (always "nanocloud.io/v1").
    #[serde(rename = "apiVersion")]
    pub api_version: String,

    /// Kind (always "Route").
    pub kind: String,

    /// Standard object metadata.
    pub metadata: ObjectMeta,

    /// Desired state of the Route.
    pub spec: RouteSpec,

    /// Observed state of the Route.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<RouteStatus>,
}

impl Default for Route {
    fn default() -> Self {
        Self {
            api_version: API_VERSION.to_string(),
            kind: KIND.to_string(),
            metadata: ObjectMeta::default(),
            spec: RouteSpec::default(),
            status: None,
        }
    }
}

impl Route {
    /// Create a new Route with the given name and spec.
    pub fn new(name: impl Into<String>, spec: RouteSpec) -> Self {
        Self {
            metadata: ObjectMeta {
                name: Some(name.into()),
                ..Default::default()
            },
            spec,
            ..Default::default()
        }
    }

    /// Get the Route name.
    pub fn name(&self) -> &str {
        self.metadata.name.as_deref().unwrap_or("")
    }

    /// Get the Route namespace.
    pub fn namespace(&self) -> &str {
        self.metadata.namespace.as_deref().unwrap_or("default")
    }

    /// Validate the Route.
    pub fn validate(&self) -> Result<(), RouteValidationError> {
        if self.name().is_empty() {
            return Err(RouteValidationError::MissingName);
        }
        self.spec.validate()
    }

    /// Check if the Route is ready.
    pub fn is_ready(&self) -> bool {
        self.status.as_ref().is_some_and(|s| s.is_ready())
    }

    /// Check if this route matches the given host and path.
    pub fn matches(&self, host: &str, path: &str) -> bool {
        if self.spec.host != host {
            return false;
        }

        match &self.spec.path_prefix {
            Some(prefix) => path.starts_with(prefix),
            None => true,
        }
    }

    /// Transform the path according to strip_prefix setting.
    pub fn transform_path<'a>(&self, path: &'a str) -> &'a str {
        if !self.spec.strip_prefix {
            return path;
        }

        match &self.spec.path_prefix {
            Some(prefix) if path.starts_with(prefix) => {
                let stripped = &path[prefix.len()..];
                if stripped.is_empty() || !stripped.starts_with('/') {
                    // Ensure we always have a leading slash
                    if stripped.is_empty() {
                        "/"
                    } else {
                        stripped
                    }
                } else {
                    stripped
                }
            }
            _ => path,
        }
    }
}

/// List of Route resources.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RouteList {
    /// API version.
    #[serde(rename = "apiVersion")]
    pub api_version: String,

    /// Kind (always "RouteList").
    pub kind: String,

    /// List metadata.
    pub metadata: ListMeta,

    /// List of Routes.
    pub items: Vec<Route>,
}

impl Default for RouteList {
    fn default() -> Self {
        Self {
            api_version: API_VERSION.to_string(),
            kind: "RouteList".to_string(),
            metadata: ListMeta::default(),
            items: Vec::new(),
        }
    }
}

impl RouteList {
    /// Create a new RouteList with the given items.
    pub fn new(items: Vec<Route>) -> Self {
        Self {
            items,
            ..Default::default()
        }
    }
}

/// Validation errors for Route resources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteValidationError {
    /// Route name is missing.
    MissingName,
    /// Host is required but missing.
    MissingHost,
    /// Service name is required but missing.
    MissingServiceName,
    /// Service port must be non-zero.
    InvalidServicePort,
    /// Path prefix is invalid.
    InvalidPathPrefix(String),
}

impl std::fmt::Display for RouteValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingName => write!(f, "route name is required"),
            Self::MissingHost => write!(f, "host is required"),
            Self::MissingServiceName => write!(f, "service name is required"),
            Self::InvalidServicePort => write!(f, "service port must be non-zero"),
            Self::InvalidPathPrefix(msg) => write!(f, "invalid path prefix: {}", msg),
        }
    }
}

impl std::error::Error for RouteValidationError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_spec_validation() {
        // Valid spec
        let spec = RouteSpec::new("example.com", ServiceRef::new("backend", 8080));
        assert!(spec.validate().is_ok());

        // Missing host
        let spec = RouteSpec {
            host: String::new(),
            service: ServiceRef::new("backend", 8080),
            ..Default::default()
        };
        assert_eq!(spec.validate(), Err(RouteValidationError::MissingHost));

        // Missing service name
        let spec = RouteSpec {
            host: "example.com".to_string(),
            service: ServiceRef::new("", 8080),
            ..Default::default()
        };
        assert_eq!(
            spec.validate(),
            Err(RouteValidationError::MissingServiceName)
        );

        // Invalid port
        let spec = RouteSpec {
            host: "example.com".to_string(),
            service: ServiceRef::new("backend", 0),
            ..Default::default()
        };
        assert_eq!(
            spec.validate(),
            Err(RouteValidationError::InvalidServicePort)
        );
    }

    #[test]
    fn route_spec_path_prefix_validation() {
        // Valid path prefix
        let spec = RouteSpec::new("example.com", ServiceRef::new("backend", 8080))
            .with_path_prefix("/api");
        assert!(spec.validate().is_ok());

        // Invalid path prefix (no leading slash)
        let spec = RouteSpec {
            host: "example.com".to_string(),
            path_prefix: Some("api".to_string()),
            service: ServiceRef::new("backend", 8080),
            ..Default::default()
        };
        assert!(matches!(
            spec.validate(),
            Err(RouteValidationError::InvalidPathPrefix(_))
        ));
    }

    #[test]
    fn route_matching() {
        let route = Route::new(
            "test",
            RouteSpec::new("example.com", ServiceRef::new("backend", 8080))
                .with_path_prefix("/api"),
        );

        // Matches host and path
        assert!(route.matches("example.com", "/api/users"));
        assert!(route.matches("example.com", "/api"));

        // Wrong host
        assert!(!route.matches("other.com", "/api/users"));

        // Wrong path
        assert!(!route.matches("example.com", "/web/users"));
    }

    #[test]
    fn route_path_transformation() {
        // With strip_prefix enabled
        let route = Route::new(
            "test",
            RouteSpec::new("example.com", ServiceRef::new("backend", 8080))
                .with_path_prefix("/api")
                .with_strip_prefix(true),
        );

        assert_eq!(route.transform_path("/api/users"), "/users");
        assert_eq!(route.transform_path("/api"), "/");
        assert_eq!(route.transform_path("/other"), "/other");

        // Without strip_prefix
        let route = Route::new(
            "test",
            RouteSpec::new("example.com", ServiceRef::new("backend", 8080))
                .with_path_prefix("/api")
                .with_strip_prefix(false),
        );

        assert_eq!(route.transform_path("/api/users"), "/api/users");
    }

    #[test]
    fn route_status_ready() {
        let mut status = RouteStatus::default();
        assert!(!status.is_ready());

        status.set_ready(true, None, None);
        assert!(status.is_ready());

        status.set_ready(false, Some("ServiceNotFound"), Some("Backend not found"));
        assert!(!status.is_ready());
    }

    #[test]
    fn service_ref_namespace_resolution() {
        let sref = ServiceRef::new("backend", 8080);
        assert_eq!(sref.resolved_namespace("default"), "default");

        let sref = ServiceRef::new("backend", 8080).with_namespace("other");
        assert_eq!(sref.resolved_namespace("default"), "other");
    }

    #[test]
    fn route_serialization() {
        let route = Route::new(
            "my-route",
            RouteSpec::new("example.com", ServiceRef::new("backend", 8080))
                .with_path_prefix("/api")
                .with_strip_prefix(true)
                .with_timeout(60),
        );

        let json = serde_json::to_string_pretty(&route).unwrap();
        let parsed: Route = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.name(), "my-route");
        assert_eq!(parsed.spec.host, "example.com");
        assert_eq!(parsed.spec.path_prefix, Some("/api".to_string()));
        assert!(parsed.spec.strip_prefix);
        assert_eq!(parsed.spec.timeout_seconds, Some(60));
    }
}
