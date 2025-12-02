use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use nanocloud::nanocloud::events::in_memory::{InMemoryEventBus, InMemoryEventBusConfig};
use nanocloud::nanocloud::events::{
    DropPolicy, EventEnvelope, EventError, EventKey, EventPublisher, EventQos, EventSubscriber,
    EventTopic, EventType, OrderingScope, SubscriptionOptions,
};
use tokio::time::timeout;

fn build_envelope(topic: &EventTopic, partition: &str, id: &str) -> EventEnvelope {
    EventEnvelope::new(
        topic.clone(),
        EventKey::new(partition, id),
        EventType::Updated,
        format!(r#"{{"id":"{id}"}}"#).into_bytes(),
        "application/json",
    )
}

fn qos(buffer_capacity: usize, drop_policy: DropPolicy) -> EventQos {
    EventQos {
        buffer_capacity,
        drop_policy,
        ordering: OrderingScope::PerPartition,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ordering_preserves_sequence_single_partition() {
    let bus = InMemoryEventBus::default();
    let topic = EventTopic::new("tests", "ordering.single");
    let mut subscription = bus
        .subscribe(&topic, SubscriptionOptions::default())
        .expect("subscribe");

    for idx in 0..5 {
        bus.publish(build_envelope(&topic, "p1", &format!("e{idx}")))
            .await
            .expect("publish");
    }

    let mut received = Vec::new();
    while received.len() < 5 {
        let envelope = timeout(Duration::from_secs(1), subscription.stream.next())
            .await
            .expect("stream timeout")
            .expect("stream closed")
            .expect("event error");
        received.push(envelope.key.id);
    }

    assert_eq!(
        received,
        vec!["e0", "e1", "e2", "e3", "e4"],
        "events preserve order for a single partition"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn ordering_per_partition_filters_and_preserves() {
    let bus = InMemoryEventBus::default();
    let topic = EventTopic::new("tests", "ordering.partitioned");
    let mut sub_a = bus
        .subscribe(
            &topic,
            SubscriptionOptions::for_partition("partition-a").with_qos(EventQos::default()),
        )
        .expect("subscribe partition a");
    let mut sub_b = bus
        .subscribe(&topic, SubscriptionOptions::for_partition("partition-b"))
        .expect("subscribe partition b");

    let sequence = [
        ("partition-a", "a1"),
        ("partition-b", "b1"),
        ("partition-a", "a2"),
        ("partition-b", "b2"),
        ("partition-a", "a3"),
    ];
    for (partition, id) in sequence {
        bus.publish(build_envelope(&topic, partition, id))
            .await
            .expect("publish");
    }

    let mut received_a = Vec::new();
    while received_a.len() < 3 {
        let envelope = timeout(Duration::from_secs(1), sub_a.stream.next())
            .await
            .expect("stream timeout")
            .expect("stream closed")
            .expect("event error");
        received_a.push(envelope.key.id);
    }
    let mut received_b = Vec::new();
    while received_b.len() < 2 {
        let envelope = timeout(Duration::from_secs(1), sub_b.stream.next())
            .await
            .expect("stream timeout")
            .expect("stream closed")
            .expect("event error");
        received_b.push(envelope.key.id);
    }

    assert_eq!(received_a, ["a1", "a2", "a3"]);
    assert_eq!(received_b, ["b1", "b2"]);
}

#[tokio::test(flavor = "current_thread")]
async fn multiple_publishers_interleave_per_partition_ordering() {
    let bus = Arc::new(InMemoryEventBus::default());
    let topic = EventTopic::new("tests", "ordering.interleave");
    let mut subscription = bus
        .subscribe(&topic, SubscriptionOptions::default())
        .expect("subscribe");

    let publisher_a = bus.clone();
    let topic_a = topic.clone();
    let task_a = tokio::spawn(async move {
        publisher_a
            .publish(build_envelope(&topic_a, "p-a", "a1"))
            .await
            .expect("publish a1");
        tokio::time::sleep(Duration::from_millis(5)).await;
        publisher_a
            .publish(build_envelope(&topic_a, "p-a", "a2"))
            .await
            .expect("publish a2");
    });

    let publisher_b = bus.clone();
    let topic_b = topic.clone();
    let task_b = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1)).await;
        publisher_b
            .publish(build_envelope(&topic_b, "p-b", "b1"))
            .await
            .expect("publish b1");
        tokio::time::sleep(Duration::from_millis(1)).await;
        publisher_b
            .publish(build_envelope(&topic_b, "p-b", "b2"))
            .await
            .expect("publish b2");
    });

    task_a.await.expect("publisher a failed");
    task_b.await.expect("publisher b failed");

    let mut partitions: Vec<(String, String)> = Vec::new();
    while partitions.len() < 4 {
        let envelope = timeout(Duration::from_secs(1), subscription.stream.next())
            .await
            .expect("stream timeout")
            .expect("stream closed")
            .expect("event error");
        partitions.push((envelope.key.partition, envelope.key.id));
    }

    let mut p_a_ids = Vec::new();
    let mut p_b_ids = Vec::new();
    for (partition, id) in partitions {
        match partition.as_str() {
            "p-a" => p_a_ids.push(id),
            "p-b" => p_b_ids.push(id),
            unexpected => panic!("unexpected partition {unexpected}"),
        }
    }

    assert_eq!(p_a_ids, vec!["a1", "a2"], "partition a ordering");
    assert_eq!(p_b_ids, vec!["b1", "b2"], "partition b ordering");
}

#[tokio::test(flavor = "current_thread")]
async fn reject_publish_policy_surfaces_backpressure() {
    let qos = qos(1, DropPolicy::RejectPublish);
    let bus = InMemoryEventBus::with_config(InMemoryEventBusConfig::new(qos));
    let topic = EventTopic::new("tests", "backpressure.reject");

    let _subscription = bus
        .subscribe(&topic, SubscriptionOptions::default())
        .expect("subscribe");

    bus.publish(build_envelope(&topic, "p1", "first"))
        .await
        .expect("first publish succeeds");
    let err = bus
        .publish(build_envelope(&topic, "p1", "second"))
        .await
        .expect_err("second publish backpressures");

    assert!(
        matches!(
            err,
            EventError::Backpressure {
                policy: DropPolicy::RejectPublish,
                ..
            }
        ),
        "expected publish-time backpressure error"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn subscriber_lag_reports_drop() {
    let qos = qos(2, DropPolicy::DropOldest);
    let bus = InMemoryEventBus::with_config(InMemoryEventBusConfig::new(qos));
    let topic = EventTopic::new("tests", "backpressure.drop_oldest");
    let mut subscription = bus
        .subscribe(&topic, SubscriptionOptions::default())
        .expect("subscribe");

    for idx in 0..5 {
        bus.publish(build_envelope(&topic, "p1", &format!("msg-{idx}")))
            .await
            .expect("publish");
    }

    let first = timeout(Duration::from_secs(1), subscription.stream.next())
        .await
        .expect("stream timeout");
    match first {
        Some(Err(EventError::SubscriberLagged { skipped })) => {
            assert!(
                skipped >= 1,
                "expected to skip at least one message when lagging"
            );
        }
        other => panic!("expected lag error, got {other:?}"),
    }

    let next_ok = timeout(Duration::from_secs(1), subscription.stream.next())
        .await
        .expect("stream timeout")
        .expect("stream closed")
        .expect("expected message after lag");
    assert!(
        next_ok.key.id.starts_with("msg-"),
        "subscriber resumes after drop"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_subscription_stops_stream() {
    let bus = InMemoryEventBus::default();
    let topic = EventTopic::new("tests", "cancellation");
    let mut subscription = bus
        .subscribe(&topic, SubscriptionOptions::default())
        .expect("subscribe");

    bus.publish(build_envelope(&topic, "p1", "before-cancel"))
        .await
        .expect("publish");

    let _ = timeout(Duration::from_secs(1), subscription.stream.next())
        .await
        .expect("stream timeout")
        .expect("stream closed")
        .expect("event error");

    subscription.cancel();

    let canceled = timeout(Duration::from_secs(1), subscription.stream.next())
        .await
        .expect("stream timeout");
    match canceled {
        Some(Err(EventError::Canceled)) => {}
        other => panic!("expected cancellation marker, got {other:?}"),
    }

    let end = timeout(Duration::from_millis(250), subscription.stream.next())
        .await
        .ok()
        .flatten();
    assert!(
        end.is_none(),
        "stream should terminate cleanly after cancellation"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_publishers_and_subscribers_do_not_drop() {
    let bus = Arc::new(InMemoryEventBus::default());
    let topic = EventTopic::new("tests", "concurrency");

    let mut sub_one = bus
        .subscribe(&topic, SubscriptionOptions::default())
        .expect("subscribe");
    let mut sub_two = bus
        .subscribe(&topic, SubscriptionOptions::default())
        .expect("subscribe");

    let publisher_a = bus.clone();
    let topic_a = topic.clone();
    let send_a = tokio::spawn(async move {
        for idx in 0..10 {
            publisher_a
                .publish(build_envelope(&topic_a, "p-a", &format!("a{idx}")))
                .await
                .expect("publish");
        }
    });

    let publisher_b = bus.clone();
    let topic_b = topic.clone();
    let send_b = tokio::spawn(async move {
        for idx in 0..10 {
            publisher_b
                .publish(build_envelope(&topic_b, "p-b", &format!("b{idx}")))
                .await
                .expect("publish");
        }
    });

    send_a.await.expect("publisher a failed");
    send_b.await.expect("publisher b failed");

    let mut received_one = Vec::new();
    let mut received_two = Vec::new();
    while received_one.len() < 20 || received_two.len() < 20 {
        if received_one.len() < 20 {
            let envelope = timeout(Duration::from_secs(1), sub_one.stream.next())
                .await
                .expect("stream timeout")
                .expect("stream closed")
                .expect("event error");
            received_one.push(envelope.key.id);
        }
        if received_two.len() < 20 {
            let envelope = timeout(Duration::from_secs(1), sub_two.stream.next())
                .await
                .expect("stream timeout")
                .expect("stream closed")
                .expect("event error");
            received_two.push(envelope.key.id);
        }
    }

    assert_eq!(received_one.len(), 20);
    assert_eq!(received_two.len(), 20);
}
