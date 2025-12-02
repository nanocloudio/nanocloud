//! In-memory event bus implementing [`EventPublisher`] and [`EventSubscriber`].
//!
//! This backend is intentionally simple and non-durable. Buffer sizing and drop behavior are
//! configured through [`EventQos`] on the bus or per-subscription:
//! - [`DropPolicy::DropOldest`] (default) keeps publishers fast but will skip messages for lagging
//!   subscribers, surfacing [`EventError::SubscriberLagged`] plus instrumentation hooks.
//! - [`DropPolicy::RejectPublish`] applies backpressure at publish-time instead of dropping, letting
//!   callers choose between loss and error handling.
//! - `buffer_capacity` controls how many events can be buffered per topic before either policy
//!   triggers. Use small values in tests to simulate pressure and larger values for bursty topics.
//!
//! The bus is best-effort only; use it for in-process fan-out rather than durable delivery.
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use crate::nanocloud::logger::{log_debug, log_warn};
use crate::nanocloud::observability::metrics;
use futures_util::{stream, StreamExt};
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;

use super::{
    DropPolicy, EventEnvelope, EventError, EventInstrumentation, EventPublisher, EventQos,
    EventSubscriber, EventTopic, PublishFuture, Subscription, SubscriptionOptions,
};

const COMPONENT: &str = "events::in_memory";

#[derive(Clone)]
/// Shared configuration for in-memory topics (QoS and instrumentation).
pub struct InMemoryEventBusConfig {
    pub qos: EventQos,
    pub instrumentation: Arc<dyn EventInstrumentation>,
}

impl InMemoryEventBusConfig {
    pub fn new(qos: EventQos) -> Self {
        Self {
            qos,
            instrumentation: Arc::new(MetricsEventInstrumentation),
        }
    }
}

impl Default for InMemoryEventBusConfig {
    fn default() -> Self {
        Self::new(EventQos::default())
    }
}

struct MetricsEventInstrumentation;

impl EventInstrumentation for MetricsEventInstrumentation {
    fn on_backpressure(&self, topic: &EventTopic, partition: &str, policy: DropPolicy) {
        let topic_label = topic.full_name();
        log_warn(
            COMPONENT,
            "In-memory event backpressure",
            &[
                ("topic", topic_label.as_str()),
                ("partition", partition),
                ("policy", policy.as_str()),
            ],
        );
        metrics::record_event_stream_error(&topic_label, "backpressure");
    }

    fn on_lagged(&self, topic: &EventTopic, skipped: u64) {
        let topic_label = topic.full_name();
        log_warn(
            COMPONENT,
            "Subscriber lagged and dropped events",
            &[
                ("topic", topic_label.as_str()),
                ("skipped", skipped.to_string().as_str()),
            ],
        );
        metrics::record_event_stream_error(&topic_label, "lagged");
    }

    fn on_drop(&self, topic: &EventTopic, partition: &str, policy: DropPolicy) {
        let topic_label = topic.full_name();
        log_debug(
            COMPONENT,
            "Dropped events for subscriber",
            &[
                ("topic", topic_label.as_str()),
                ("partition", partition),
                ("policy", policy.as_str()),
            ],
        );
        metrics::record_event_stream_error(&topic_label, "dropped");
    }
}

#[derive(Clone)]
struct TopicState {
    sender: broadcast::Sender<EventEnvelope>,
    qos: EventQos,
}

pub struct InMemoryEventBus {
    topics: Mutex<HashMap<String, TopicState>>,
    config: InMemoryEventBusConfig,
}

impl Default for InMemoryEventBus {
    fn default() -> Self {
        Self::with_config(InMemoryEventBusConfig::default())
    }
}

impl InMemoryEventBus {
    #[allow(dead_code)]
    pub fn new(qos: EventQos) -> Self {
        Self::with_config(InMemoryEventBusConfig::new(qos))
    }

    /// Builds a bus with explicit QoS and instrumentation configuration.
    pub fn with_config(config: InMemoryEventBusConfig) -> Self {
        Self {
            topics: Mutex::new(HashMap::new()),
            config,
        }
    }

    pub fn global() -> Arc<Self> {
        static INSTANCE: OnceLock<Arc<InMemoryEventBus>> = OnceLock::new();
        INSTANCE
            .get_or_init(|| Arc::new(InMemoryEventBus::default()))
            .clone()
    }

    fn sender_for(
        &self,
        topic: &EventTopic,
        qos_hint: Option<&EventQos>,
    ) -> (broadcast::Sender<EventEnvelope>, EventQos) {
        let mut topics = self
            .topics
            .lock()
            .expect("in-memory event topics lock poisoned");
        let key = topic.full_name();
        let default_qos = qos_hint.cloned().unwrap_or_else(|| self.config.qos.clone());
        let entry = topics.entry(key).or_insert_with(|| TopicState {
            sender: broadcast::channel(default_qos.buffer_capacity).0,
            qos: default_qos.clone(),
        });
        (entry.sender.clone(), entry.qos.clone())
    }
}

impl EventPublisher for InMemoryEventBus {
    fn publish<'a>(&'a self, event: EventEnvelope) -> PublishFuture<'a> {
        let topic = event.topic.clone();
        let partition = event.key.partition.clone();
        let topic_label = topic.full_name();
        let status_label = event
            .attributes
            .get("status")
            .map(|value| value.as_str())
            .unwrap_or("unknown")
            .to_string();
        let instrumentation = Arc::clone(&self.config.instrumentation);
        let (sender, qos) = self.sender_for(&topic, None);

        Box::pin(async move {
            event.validate()?;

            if qos.drop_policy == DropPolicy::RejectPublish && sender.len() >= qos.buffer_capacity {
                instrumentation.on_backpressure(&topic, &partition, qos.drop_policy);
                return Err(EventError::Backpressure {
                    topic: topic_label,
                    partition,
                    policy: qos.drop_policy,
                });
            }

            sender
                .send(event)
                .map(|_| metrics::record_event_emit(&topic_label, &status_label))
                .map_err(|_| EventError::ChannelClosed)
        })
    }
}

impl EventSubscriber for InMemoryEventBus {
    fn subscribe(
        &self,
        topic: &EventTopic,
        options: SubscriptionOptions,
    ) -> Result<Subscription, EventError> {
        let (sender, qos) = self.sender_for(topic, options.qos.as_ref());
        let receiver = sender.subscribe();
        let instrumentation = Arc::clone(&self.config.instrumentation);

        let topic_for_stream = topic.clone();
        let partition_filter = options.partition.clone();
        let drop_policy = qos.drop_policy;

        let stream = BroadcastStream::new(receiver).filter_map(move |result| {
            let instrumentation = Arc::clone(&instrumentation);
            let topic_for_log = topic_for_stream.clone();
            let partition_filter = partition_filter.clone();
            async move {
                let mapped: Option<Result<EventEnvelope, EventError>> = match result {
                    Ok(envelope) => {
                        if let Some(filter) = partition_filter.as_ref() {
                            if envelope.key.partition != *filter {
                                return None;
                            }
                        }
                        Some(Ok::<EventEnvelope, EventError>(envelope))
                    }
                    Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                        instrumentation.on_lagged(&topic_for_log, skipped);
                        instrumentation.on_drop(
                            &topic_for_log,
                            partition_filter.as_deref().unwrap_or("*"),
                            drop_policy,
                        );
                        Some(Err(EventError::SubscriberLagged { skipped }))
                    }
                };
                mapped
            }
        });

        let cancel_token = CancellationToken::new();
        let cancel_notifier = cancel_token.clone();
        let cancel_stream = stream::once(async move {
            cancel_notifier.cancelled().await;
            Err(EventError::Canceled)
        });

        let cancel_future = cancel_token.clone().cancelled_owned();
        let stream = stream.take_until(cancel_future).chain(cancel_stream);

        Ok(Subscription::new(Box::pin(stream), cancel_token))
    }
}
