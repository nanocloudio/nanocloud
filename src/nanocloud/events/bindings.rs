use serde::{Deserialize, Serialize};

/// Serialized payload emitted for binding lifecycle events.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BindingEventPayload {
    pub status: BindingEventStatus,
    pub bundle: String,
    pub namespace: String,
    pub service: String,
    pub binding_id: String,
    pub attempt: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,
}

impl BindingEventPayload {
    pub fn started(
        bundle: &str,
        namespace: &str,
        service: &str,
        binding_id: &str,
        attempt: u32,
    ) -> Self {
        Self {
            status: BindingEventStatus::Started,
            bundle: bundle.to_string(),
            namespace: namespace.to_string(),
            service: service.to_string(),
            binding_id: binding_id.to_string(),
            attempt,
            duration_ms: None,
            exit_code: None,
            message: None,
            stdout: None,
            stderr: None,
        }
    }

    pub fn with_completion_meta(
        mut self,
        status: BindingEventStatus,
        duration_ms: Option<u64>,
        exit_code: Option<i32>,
        message: Option<String>,
        stdout: Option<String>,
        stderr: Option<String>,
    ) -> Self {
        self.status = status;
        self.duration_ms = duration_ms;
        self.exit_code = exit_code;
        self.message = message;
        self.stdout = stdout;
        self.stderr = stderr;
        self
    }
}

/// Enumerates binding lifecycle stages pushed to the event bus.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BindingEventStatus {
    Started,
    Completed,
    Failed,
    TimedOut,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn started_payload_sets_defaults() {
        let payload = BindingEventPayload::started("bundle-a", "ns", "svc", "binding-1", 1);
        let serialized = serde_json::to_value(&payload).expect("serialize payload");

        assert_eq!(payload.status, BindingEventStatus::Started);
        assert_eq!(payload.duration_ms, None);
        assert_eq!(payload.exit_code, None);
        assert_eq!(payload.message, None);

        assert_eq!(serialized["bundle"], "bundle-a");
        assert!(
            serialized.get("duration_ms").is_none(),
            "optional fields should be omitted when None"
        );
    }

    #[test]
    fn completion_metadata_round_trips() {
        let payload = BindingEventPayload::started("bundle-a", "ns", "svc", "binding-1", 2)
            .with_completion_meta(
                BindingEventStatus::Failed,
                Some(1200),
                Some(1),
                Some("boom".to_string()),
                Some("out".to_string()),
                Some("err".to_string()),
            );

        let encoded = serde_json::to_string(&payload).expect("encode payload");
        let decoded: BindingEventPayload =
            serde_json::from_str(&encoded).expect("decode payload back");

        assert_eq!(decoded.status, BindingEventStatus::Failed);
        assert_eq!(decoded.duration_ms, Some(1200));
        assert_eq!(decoded.exit_code, Some(1));
        assert_eq!(decoded.message.as_deref(), Some("boom"));
        assert_eq!(decoded.stdout.as_deref(), Some("out"));
        assert_eq!(decoded.stderr.as_deref(), Some("err"));
    }

    #[test]
    fn deserialization_fails_when_required_fields_missing() {
        let invalid: Result<BindingEventPayload, _> =
            serde_json::from_value(json!({ "namespace": "ns-only" }));
        assert!(invalid.is_err(), "missing required fields should error");
    }
}
