use chrono::{TimeZone, Utc};
use nanocloud::nanocloud::events::{
    EventEnvelope, EventKey, EventTopic, EventType, EventValidationError,
};

fn envelope() -> EventEnvelope {
    EventEnvelope::new(
        EventTopic::new("tests", "metadata"),
        EventKey::new("partition", "id"),
        EventType::Updated,
        br#"{"status":"ok"}"#.to_vec(),
        "application/json",
    )
}

#[test]
fn metadata_helpers_round_trip() {
    let envelope = envelope()
        .with_trace_id("trace-1")
        .with_span_id("span-1")
        .with_request_id("req-1")
        .with_user_id("user-1")
        .with_attribute("custom", "value");

    assert_eq!(envelope.trace_id(), Some("trace-1"));
    assert_eq!(envelope.span_id(), Some("span-1"));
    assert_eq!(envelope.request_id(), Some("req-1"));
    assert_eq!(envelope.user_id(), Some("user-1"));
    assert_eq!(
        envelope.attributes.get("custom").map(String::as_str),
        Some("value")
    );
    assert!(envelope.validate().is_ok());
}

#[test]
fn validation_rejects_missing_content_type_or_payload() {
    let empty_payload = EventEnvelope::new(
        EventTopic::new("tests", "invalid"),
        EventKey::new("p1", "id"),
        EventType::Updated,
        Vec::new(),
        "application/json",
    );
    assert_eq!(
        empty_payload.validate(),
        Err(EventValidationError::EmptyPayload)
    );

    let mut missing_content_type = envelope();
    missing_content_type.content_type = "  ";
    assert_eq!(
        missing_content_type.validate(),
        Err(EventValidationError::MissingContentType)
    );
}

#[test]
fn validation_rejects_invalid_trace_and_timestamp() {
    let default_timestamp = envelope().with_timestamp(Utc.timestamp_opt(0, 0).unwrap());
    assert_eq!(
        default_timestamp.validate(),
        Err(EventValidationError::DefaultTimestamp)
    );

    let invalid_trace = envelope().with_trace_id("invalid trace id");
    assert_eq!(
        invalid_trace.validate(),
        Err(EventValidationError::InvalidTraceId(
            "invalid trace id".to_string()
        ))
    );
}

#[test]
fn validation_bounds_attribute_volume_and_size() {
    let oversized_attribute = envelope().with_attribute("key", "x".repeat(300));
    let attr_error = oversized_attribute.validate();
    assert!(
        matches!(
            attr_error,
            Err(EventValidationError::AttributeTooLarge { .. })
        ),
        "expected oversize attributes to be rejected but got {attr_error:?}"
    );

    let mut too_many = envelope();
    for idx in 0..70 {
        too_many = too_many.with_attribute(format!("k{idx}"), "v");
    }
    let count_error = too_many.validate();
    assert!(
        matches!(
            count_error,
            Err(EventValidationError::TooManyAttributes { count, .. }) if count > 64
        ),
        "expected attribute count rejection but got {count_error:?}"
    );
}
