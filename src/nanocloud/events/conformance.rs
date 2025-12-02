#![cfg_attr(not(test), allow(dead_code))]
//! Lightweight conformance helpers for event backends.
//!
//! These assertions provide a shared checklist for new implementations to validate behavior without
//! copying test logic. Call [`run_smoke_suite`] or individual assertions from backend-specific
//! tests:
//!
//! ```ignore
//! #[tokio::test]
//! async fn my_backend_conforms() {
//!     let backend = MyBackend::new();
//!     nanocloud::nanocloud::events::conformance::run_smoke_suite(&backend).await;
//! }
//! ```

use std::time::Duration;

use futures_util::StreamExt;
use tokio::time::timeout;

use super::{
    DropPolicy, EventEnvelope, EventError, EventKey, EventPublisher, EventQos, EventSubscriber,
    EventTopic, EventType, OrderingScope, SubscriptionOptions,
};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

/// Trait alias for backends that can be exercised by the conformance helpers.
pub trait ConformanceBackend: EventPublisher + EventSubscriber + Send + Sync + 'static {}

impl<T> ConformanceBackend for T where T: EventPublisher + EventSubscriber + Send + Sync + 'static {}

fn envelope(topic: &EventTopic, partition: &str, id: &str) -> EventEnvelope {
    EventEnvelope::new(
        topic.clone(),
        EventKey::new(partition, id),
        EventType::Updated,
        format!(r#"{{"id":"{id}"}}"#).into_bytes(),
        "application/json",
    )
}

/// Returns the checklist of expectations enforced by the helpers.
pub fn checklist() -> &'static [&'static str] {
    &[
        "Envelopes are validated before publish (content type, payload, timestamp, trace id, attributes).",
        "Ordering is preserved per partition.",
        "Cancellation signals terminate subscription streams with EventError::Canceled.",
        "Drop/backpressure surfaces either SubscriberLagged or Backpressure errors instead of panicking.",
    ]
}

/// Runs a basic suite of conformance assertions against the provided backend.
pub async fn run_smoke_suite<B: ConformanceBackend>(backend: &B) {
    assert_validation_errors(backend).await;
    assert_partition_ordering(backend).await;
    assert_cancellation_behavior(backend).await;
    assert_backpressure_visibility(backend).await;
}

/// Ensures invalid envelopes are rejected.
pub async fn assert_validation_errors<B: ConformanceBackend>(backend: &B) {
    let topic = EventTopic::new("conformance", "validation");
    let invalid = EventEnvelope::new(
        topic.clone(),
        EventKey::new("p1", "invalid"),
        EventType::Updated,
        Vec::new(),
        "",
    );

    let err = backend
        .publish(invalid)
        .await
        .expect_err("invalid envelope should be rejected");
    assert!(
        matches!(err, EventError::Validation(_)),
        "invalid envelopes should return validation errors"
    );
}

/// Ensures ordering is maintained for events published to the same partition.
pub async fn assert_partition_ordering<B: ConformanceBackend>(backend: &B) {
    let topic = EventTopic::new("conformance", "ordering");
    let mut subscription = backend
        .subscribe(&topic, SubscriptionOptions::default())
        .expect("subscribe");

    backend
        .publish(envelope(&topic, "partition", "first"))
        .await
        .expect("publish first");
    backend
        .publish(envelope(&topic, "partition", "second"))
        .await
        .expect("publish second");

    let first = next_event(&mut subscription).await;
    let second = next_event(&mut subscription).await;
    assert_eq!(
        [&first, &second],
        ["first", "second"],
        "ordering should be preserved within a partition"
    );
}

/// Ensures subscription cancellation surfaces [`EventError::Canceled`] and ends the stream.
pub async fn assert_cancellation_behavior<B: ConformanceBackend>(backend: &B) {
    let topic = EventTopic::new("conformance", "cancel");
    let mut subscription = backend
        .subscribe(&topic, SubscriptionOptions::default())
        .expect("subscribe");

    backend
        .publish(envelope(&topic, "p1", "before-cancel"))
        .await
        .expect("publish");
    let _ = next_event(&mut subscription).await;

    subscription.cancel();
    let canceled = timeout(DEFAULT_TIMEOUT, subscription.stream.next())
        .await
        .expect("stream timeout after cancel");
    match canceled {
        Some(Err(EventError::Canceled)) => {}
        other => panic!("expected canceled marker, got {other:?}"),
    }
}

/// Verifies drop/backpressure signals are surfaced instead of panicking.
pub async fn assert_backpressure_visibility<B: ConformanceBackend>(backend: &B) {
    let qos = EventQos {
        buffer_capacity: 1,
        drop_policy: DropPolicy::RejectPublish,
        ordering: OrderingScope::PerPartition,
    };
    let topic = EventTopic::new("conformance", "backpressure");

    let _sub = backend
        .subscribe(&topic, SubscriptionOptions::default().with_qos(qos.clone()))
        .expect("subscribe");

    backend
        .publish(envelope(&topic, "p1", "first"))
        .await
        .expect("first publish");
    let err = backend
        .publish(envelope(&topic, "p1", "second"))
        .await
        .expect_err("second publish should backpressure");

    assert!(
        matches!(
            err,
            EventError::Backpressure {
                policy: DropPolicy::RejectPublish,
                ..
            }
        ),
        "expected publish-time backpressure to be surfaced"
    );
}

async fn next_event(subscription: &mut super::Subscription) -> String {
    timeout(DEFAULT_TIMEOUT, subscription.stream.next())
        .await
        .expect("stream timeout")
        .expect("stream closed")
        .expect("event error")
        .key
        .id
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::events::in_memory::InMemoryEventBus;

    #[tokio::test(flavor = "current_thread")]
    async fn smoke_suite_runs_against_in_memory_backend() {
        assert!(
            !checklist().is_empty(),
            "checklist should enumerate expectations"
        );
        let backend = InMemoryEventBus::default();
        run_smoke_suite(&backend).await;
    }
}
