use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};

use crate::nanocloud::observability::metrics;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use super::{
    EventEnvelope, EventPublisher, EventSubscriber, EventTopic, PublishFuture, Subscription,
    SubscriptionOptions,
};

#[derive(Debug)]
pub enum InMemoryBusError {
    ChannelClosed,
    Lagged(u64),
}

impl fmt::Display for InMemoryBusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InMemoryBusError::ChannelClosed => write!(f, "event channel closed"),
            InMemoryBusError::Lagged(count) => {
                write!(f, "subscriber lagged by {count} messages")
            }
        }
    }
}

impl std::error::Error for InMemoryBusError {}

pub struct InMemoryEventBus {
    topics: Mutex<HashMap<String, broadcast::Sender<EventEnvelope>>>,
    capacity: usize,
}

impl InMemoryEventBus {
    pub fn new(capacity: usize) -> Self {
        Self {
            topics: Mutex::new(HashMap::new()),
            capacity,
        }
    }

    pub fn global() -> Arc<Self> {
        static INSTANCE: OnceLock<Arc<InMemoryEventBus>> = OnceLock::new();
        INSTANCE
            .get_or_init(|| Arc::new(InMemoryEventBus::new(1024)))
            .clone()
    }

    fn sender_for(&self, topic: &EventTopic) -> broadcast::Sender<EventEnvelope> {
        let mut topics = self
            .topics
            .lock()
            .expect("in-memory event topics lock poisoned");
        let key = topic.full_name();
        topics
            .entry(key)
            .or_insert_with(|| broadcast::channel(self.capacity).0)
            .clone()
    }
}

impl EventPublisher for InMemoryEventBus {
    type Error = InMemoryBusError;

    fn publish<'a>(&'a self, event: EventEnvelope) -> PublishFuture<'a, Self::Error> {
        let topic_label = event.topic.full_name();
        let status_label = event
            .attributes
            .get("status")
            .map(|value| value.as_str())
            .unwrap_or("unknown")
            .to_string();
        let sender = self.sender_for(&event.topic);
        Box::pin(async move {
            sender
                .send(event)
                .map(|_| metrics::record_event_emit(&topic_label, &status_label))
                .map_err(|_| InMemoryBusError::ChannelClosed)
        })
    }
}

impl EventSubscriber for InMemoryEventBus {
    type Error = InMemoryBusError;

    fn subscribe(
        &self,
        topic: &EventTopic,
        _options: SubscriptionOptions,
    ) -> Result<Subscription<Self::Error>, Self::Error> {
        let sender = self.sender_for(topic);
        let topic_label = topic.full_name();
        let receiver = sender.subscribe();

        let stream = BroadcastStream::new(receiver).map(move |result| match result {
            Ok(envelope) => Ok(envelope),
            Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                metrics::record_event_stream_error(&topic_label, "lagged");
                Err(InMemoryBusError::Lagged(skipped))
            }
        });

        Ok(Subscription {
            stream: Box::pin(stream),
        })
    }
}
