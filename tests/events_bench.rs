#![cfg(feature = "events-bench")]

use std::time::Instant;

use futures_util::StreamExt;
use nanocloud::nanocloud::events::in_memory::InMemoryEventBus;
use nanocloud::nanocloud::events::{
    EventEnvelope, EventKey, EventPublisher, EventSubscriber, EventTopic, EventType,
    SubscriptionOptions,
};

#[tokio::test(flavor = "multi_thread")]
async fn publish_subscribe_throughput_smoke() {
    let bus = InMemoryEventBus::default();
    let topic = EventTopic::new("bench", "throughput");
    let mut subscription = bus
        .subscribe(&topic, SubscriptionOptions::default())
        .expect("subscribe");

    let iterations = 1_000usize;
    let start = Instant::now();

    for idx in 0..iterations {
        let envelope = EventEnvelope::new(
            topic.clone(),
            EventKey::new("bench", format!("id-{idx}")),
            EventType::Updated,
            b"{}".to_vec(),
            "application/json",
        );
        bus.publish(envelope).await.expect("publish");
    }

    let mut received = 0;
    while received < iterations {
        let _ = subscription
            .stream
            .next()
            .await
            .expect("stream closed")
            .expect("event error");
        received += 1;
    }

    let elapsed = start.elapsed();
    let per_sec = iterations as f64 / elapsed.as_secs_f64();
    eprintln!(
        "In-memory publish/subscribe throughput: {per_sec:.2} events/sec over {iterations} events"
    );
}
