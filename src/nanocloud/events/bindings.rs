use serde::Serialize;

/// Serialized payload emitted for binding lifecycle events.
#[derive(Clone, Debug, Serialize)]
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
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BindingEventStatus {
    Started,
    Completed,
    Failed,
    TimedOut,
}
