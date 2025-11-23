use crate::nanocloud::k8s::event::{Event, EventRegistry, EventSource, ObjectReference};
use crate::nanocloud::k8s::pod::ObjectMeta;
use chrono::{SecondsFormat, Utc};
use std::sync::Arc;

/// Lightweight helper to emit Kubernetes-style Events for controller actions.
#[derive(Clone)]
pub struct EventRecorder {
    component: String,
    registry: Arc<EventRegistry>,
    node: Option<String>,
}

#[derive(Clone)]
pub struct InvolvedObjectRef {
    pub api_version: String,
    pub kind: String,
    pub name: String,
    pub uid: Option<String>,
    pub namespace: Option<String>,
}

impl EventRecorder {
    pub fn new(component: impl Into<String>) -> Self {
        Self {
            component: component.into(),
            registry: EventRegistry::shared(),
            node: None,
        }
    }

    pub async fn record(
        &self,
        namespace: Option<&str>,
        involved: &InvolvedObjectRef,
        reason: &str,
        event_type: &str,
        message: impl Into<String>,
    ) {
        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        let resolved_namespace = namespace
            .map(|ns| ns.to_string())
            .or_else(|| involved.namespace.clone());

        let event = Event {
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            metadata: ObjectMeta {
                namespace: resolved_namespace.clone(),
                ..Default::default()
            },
            involved_object: ObjectReference {
                api_version: Some(involved.api_version.clone()),
                kind: Some(involved.kind.clone()),
                name: Some(involved.name.clone()),
                namespace: resolved_namespace.or_else(|| involved.namespace.clone()),
                uid: involved.uid.clone(),
                ..Default::default()
            },
            reason: Some(reason.to_string()),
            message: Some(message.into()),
            event_type: Some(event_type.to_string()),
            event_time: Some(now.clone()),
            first_timestamp: Some(now.clone()),
            last_timestamp: Some(now),
            reporting_component: Some(self.component.clone()),
            reporting_instance: self.node.clone(),
            deprecated_source: Some(EventSource {
                component: Some(self.component.clone()),
                host: self.node.clone(),
            }),
            source: Some(EventSource {
                component: Some(self.component.clone()),
                host: self.node.clone(),
            }),
            count: Some(1),
            action: None,
            related: None,
            series: None,
            deprecated_first_timestamp: None,
            deprecated_last_timestamp: None,
            deprecated_count: None,
        };

        let _ = self.registry.record(event).await;
    }
}
