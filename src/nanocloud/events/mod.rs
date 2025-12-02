//! Event abstractions and in-process delivery semantics.
//!
//! - Delivery is **best-effort and at-most-once**. The in-memory backend does not persist events
//!   and drops messages when buffers overflow or subscribers lag.
//! - Ordering is scoped to a topic/partition (`EventKey::partition`) and is only guaranteed for
//!   events published sequentially on the same thread. Concurrent publishers may interleave.
//! - Backpressure/drop behavior is controlled through [`EventQos`]. Defaults mirror the previous
//!   unbounded behavior by using a generous buffer and dropping the oldest events when lagging.
//!   Use [`EventQos::with_drop_policy`] to switch to publish-time backpressure when desired and
//!   [`EventQos::with_buffer_capacity`] to tune buffers for tests or latency-sensitive topics.
//! - Envelopes must provide a content type and non-default timestamp. Trace metadata helpers are
//!   available to encourage consistent correlation identifiers. Validation also bounds attribute
//!   count and size to defend against malformed metadata.
//!
//! # Example
//! ```no_run
//! use nanocloud::nanocloud::events::{
//!     EventEnvelope, EventKey, EventPublisher, EventTopic, EventType, SubscriptionOptions,
//! };
//! use nanocloud::nanocloud::events::in_memory::InMemoryEventBus;
//! use futures_util::StreamExt;
//!
//! # async fn demo() -> Result<(), nanocloud::nanocloud::events::EventError> {
//! let bus = InMemoryEventBus::global();
//! let topic = EventTopic::new("controller", "example");
//! let envelope = EventEnvelope::new(
//!     topic,
//!     EventKey::new("partition-a", "example-id"),
//!     EventType::Updated,
//!     br#"{"status":"ok"}"#.to_vec(),
//!     "application/json",
//! )
//! .with_request_id("req-1234")
//! .with_trace_id("trace-1234");
//! bus.publish(envelope).await?;
//!
//! let subscription = bus.subscribe(&EventTopic::new("controller", "example"), SubscriptionOptions::default())?;
//! let mut stream = subscription.stream;
//! while let Some(next) = stream.next().await {
//!     let _ = next?;
//!     break;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Implementing a backend
//! A minimal backend should validate envelopes, honor [`EventQos`] (buffer sizing and
//! [`DropPolicy`]), and translate transport errors into [`EventError`] so callers see consistent
//! semantics. Cancellation should be surfaced using [`EventError::Canceled`] or by ending the
//! stream when resources are dropped.
//!
//! ```ignore
//! use nanocloud::nanocloud::events::{EventEnvelope, EventError, EventPublisher, PublishFuture};
//! struct MyBackend;
//! impl EventPublisher for MyBackend {
//!     fn publish<'a>(&'a self, event: EventEnvelope) -> PublishFuture<'a> {
//!         Box::pin(async move {
//!             event.validate()?;
//!             // forward to backend transport here
//!             Ok(())
//!         })
//!     }
//! }
//! ```

use chrono::{DateTime, Utc};
use futures_util::stream::BoxStream;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use tokio_util::sync::CancellationToken;

pub type PublishFuture<'a> = Pin<Box<dyn Future<Output = Result<(), EventError>> + Send + 'a>>;
pub type EventStream = BoxStream<'static, Result<EventEnvelope, EventError>>;

pub mod bindings;
pub mod conformance;
pub mod in_memory;

const MAX_ATTRIBUTE_PAIRS: usize = 64;
const MAX_ATTRIBUTE_ENTRY_LEN: usize = 256;

/// Controls how a backend handles overflow and ordering.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropPolicy {
    /// Drop the oldest buffered events when subscribers lag.
    DropOldest,
    /// Reject new publishes once the buffer is full.
    RejectPublish,
}

impl DropPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            DropPolicy::DropOldest => "drop_oldest",
            DropPolicy::RejectPublish => "reject_publish",
        }
    }
}

/// Defines how ordering should be interpreted for a topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub enum OrderingScope {
    /// No ordering is provided; events may arrive in any order.
    Unordered,
    /// Ordering is maintained per topic regardless of partition.
    PerTopic,
    /// Ordering is maintained per partition within a topic.
    PerPartition,
}

/// Quality-of-service options for publishers and subscribers.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct EventQos {
    /// Maximum buffered events per topic/partition.
    pub buffer_capacity: usize,
    #[allow(dead_code)]
    /// Behavior when the buffer is full.
    pub drop_policy: DropPolicy,
    /// Ordering guarantees expected by the caller.
    pub ordering: OrderingScope,
}

impl EventQos {
    #[allow(dead_code)]
    pub fn with_buffer_capacity(mut self, capacity: usize) -> Self {
        self.buffer_capacity = capacity;
        self
    }

    #[allow(dead_code)]
    pub fn with_drop_policy(mut self, drop_policy: DropPolicy) -> Self {
        self.drop_policy = drop_policy;
        self
    }

    #[allow(dead_code)]
    pub fn with_ordering(mut self, ordering: OrderingScope) -> Self {
        self.ordering = ordering;
        self
    }
}

impl Default for EventQos {
    fn default() -> Self {
        Self {
            buffer_capacity: 1024,
            drop_policy: DropPolicy::DropOldest,
            ordering: OrderingScope::PerPartition,
        }
    }
}

/// Hook interface for recording drops, lag, and backpressure events.
pub trait EventInstrumentation: Send + Sync {
    fn on_backpressure(&self, _topic: &EventTopic, _partition: &str, _policy: DropPolicy) {}
    fn on_lagged(&self, _topic: &EventTopic, _skipped: u64) {}
    fn on_drop(&self, _topic: &EventTopic, _partition: &str, _policy: DropPolicy) {}
}

/// Default no-op instrumentation.
#[allow(dead_code)]
pub struct NoopEventInstrumentation;

impl EventInstrumentation for NoopEventInstrumentation {}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EventTopic {
    /// Logical scope used to group related streams (e.g. "controller").
    pub scope: String,
    /// Stream name within the scope (e.g. "bundles.reconcile").
    pub name: String,
}

impl EventTopic {
    pub fn new<S>(scope: S, name: S) -> Self
    where
        S: Into<String> + Clone,
    {
        Self {
            scope: scope.clone().into(),
            name: name.into(),
        }
    }

    /// Returns the fully qualified name `<scope>.<name>`.
    pub fn full_name(&self) -> String {
        format!("{}.{}", self.scope, self.name)
    }
}

/// Uniquely identifies an event within a topic and provides ordering partitioning.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EventKey {
    /// Partition controls ordering scope. Events with the same partition are emitted in order.
    pub partition: String,
    /// Caller-defined identifier for the event payload.
    pub id: String,
}

impl EventKey {
    pub fn new<P, I>(partition: P, id: I) -> Self
    where
        P: Into<String>,
        I: Into<String>,
    {
        Self {
            partition: partition.into(),
            id: id.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventType {
    Updated,
    Custom(&'static str),
}

/// Attribute keys for typed metadata helpers.
#[allow(dead_code)]
pub const ATTR_SPAN_ID: &str = "span_id";
#[allow(dead_code)]
pub const ATTR_REQUEST_ID: &str = "request_id";
#[allow(dead_code)]
pub const ATTR_USER_ID: &str = "user_id";

#[derive(Clone, Debug)]
pub struct EventEnvelope {
    pub topic: EventTopic,
    pub key: EventKey,
    pub event_type: EventType,
    pub payload: Vec<u8>,
    pub content_type: &'static str,
    pub timestamp: DateTime<Utc>,
    pub trace_id: Option<String>,
    pub attributes: HashMap<String, String>,
}

impl EventEnvelope {
    /// Creates a new envelope with the current timestamp.
    ///
    /// This constructor does not perform validation; callers should rely on the publisher to
    /// validate or call [`EventEnvelope::validate`] before publishing.
    pub fn new(
        topic: EventTopic,
        key: EventKey,
        event_type: EventType,
        payload: Vec<u8>,
        content_type: &'static str,
    ) -> Self {
        Self {
            topic,
            key,
            event_type,
            payload,
            content_type,
            timestamp: Utc::now(),
            trace_id: None,
            attributes: HashMap::new(),
        }
    }

    #[allow(dead_code)]
    pub fn with_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.timestamp = timestamp;
        self
    }

    #[allow(dead_code)]
    pub fn with_trace_id<T>(mut self, trace_id: T) -> Self
    where
        T: Into<String>,
    {
        self.trace_id = Some(trace_id.into());
        self
    }

    #[allow(dead_code)]
    pub fn with_span_id<T>(mut self, span_id: T) -> Self
    where
        T: Into<String>,
    {
        self.attributes
            .insert(ATTR_SPAN_ID.to_string(), span_id.into());
        self
    }

    #[allow(dead_code)]
    pub fn with_request_id<T>(mut self, request_id: T) -> Self
    where
        T: Into<String>,
    {
        self.attributes
            .insert(ATTR_REQUEST_ID.to_string(), request_id.into());
        self
    }

    #[allow(dead_code)]
    pub fn with_user_id<T>(mut self, user_id: T) -> Self
    where
        T: Into<String>,
    {
        self.attributes
            .insert(ATTR_USER_ID.to_string(), user_id.into());
        self
    }

    pub fn with_attribute<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.attributes.insert(key.into(), value.into());
        self
    }

    #[allow(dead_code)]
    pub fn trace_id(&self) -> Option<&str> {
        self.trace_id.as_deref()
    }

    #[allow(dead_code)]
    pub fn span_id(&self) -> Option<&str> {
        self.attributes
            .get(ATTR_SPAN_ID)
            .map(|value| value.as_str())
    }

    #[allow(dead_code)]
    pub fn request_id(&self) -> Option<&str> {
        self.attributes
            .get(ATTR_REQUEST_ID)
            .map(|value| value.as_str())
    }

    #[allow(dead_code)]
    pub fn user_id(&self) -> Option<&str> {
        self.attributes
            .get(ATTR_USER_ID)
            .map(|value| value.as_str())
    }

    /// Ensures the envelope contains a content type, payload, non-default timestamp, and a valid
    /// trace id plus bounded metadata attributes.
    pub fn validate(&self) -> Result<(), EventValidationError> {
        if self.content_type.trim().is_empty() {
            return Err(EventValidationError::MissingContentType);
        }
        if self.payload.is_empty() {
            return Err(EventValidationError::EmptyPayload);
        }
        if self.timestamp.timestamp() == 0 && self.timestamp.timestamp_subsec_nanos() == 0 {
            return Err(EventValidationError::DefaultTimestamp);
        }
        if self.attributes.len() > MAX_ATTRIBUTE_PAIRS {
            return Err(EventValidationError::TooManyAttributes {
                count: self.attributes.len(),
                max: MAX_ATTRIBUTE_PAIRS,
            });
        }
        for (key, value) in &self.attributes {
            let combined_len = key.len() + value.len();
            if combined_len > MAX_ATTRIBUTE_ENTRY_LEN {
                return Err(EventValidationError::AttributeTooLarge {
                    key: key.clone(),
                    length: combined_len,
                    max: MAX_ATTRIBUTE_ENTRY_LEN,
                });
            }
        }
        if let Some(trace_id) = self.trace_id.as_ref() {
            if !is_valid_trace_id(trace_id) {
                return Err(EventValidationError::InvalidTraceId(trace_id.clone()));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventValidationError {
    MissingContentType,
    EmptyPayload,
    DefaultTimestamp,
    InvalidTraceId(String),
    TooManyAttributes {
        count: usize,
        max: usize,
    },
    AttributeTooLarge {
        key: String,
        length: usize,
        max: usize,
    },
}

impl fmt::Display for EventValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EventValidationError::MissingContentType => write!(f, "event is missing content type"),
            EventValidationError::EmptyPayload => {
                write!(f, "event payload must not be empty")
            }
            EventValidationError::DefaultTimestamp => {
                write!(f, "event timestamp must be set to a non-default value")
            }
            EventValidationError::InvalidTraceId(value) => {
                write!(f, "trace id '{value}' is not valid")
            }
            EventValidationError::TooManyAttributes { count, max } => write!(
                f,
                "event has {count} attributes which exceeds the limit of {max}"
            ),
            EventValidationError::AttributeTooLarge { key, length, max } => write!(
                f,
                "event attribute '{key}' is too large ({length} bytes, limit {max})"
            ),
        }
    }
}

impl std::error::Error for EventValidationError {}

#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub enum EventError {
    ChannelClosed,
    SubscriberLagged {
        skipped: u64,
    },
    Backpressure {
        topic: String,
        partition: String,
        policy: DropPolicy,
    },
    Dropped {
        topic: String,
        partition: Option<String>,
        policy: DropPolicy,
    },
    Validation(EventValidationError),
    Canceled,
}

impl fmt::Display for EventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EventError::ChannelClosed => write!(f, "event channel closed"),
            EventError::SubscriberLagged { skipped } => {
                write!(f, "subscriber lagged by {skipped} messages")
            }
            EventError::Backpressure {
                topic,
                partition,
                policy,
            } => write!(
                f,
                "backpressure on topic '{topic}' partition '{partition}' ({})",
                policy.as_str()
            ),
            EventError::Dropped {
                topic,
                partition,
                policy,
            } => {
                if let Some(partition) = partition {
                    write!(
                        f,
                        "event dropped for topic '{topic}' partition '{partition}' ({})",
                        policy.as_str()
                    )
                } else {
                    write!(f, "event dropped for topic '{topic}' ({})", policy.as_str())
                }
            }
            EventError::Validation(err) => err.fmt(f),
            EventError::Canceled => write!(f, "subscription canceled"),
        }
    }
}

impl std::error::Error for EventError {}

impl From<EventValidationError> for EventError {
    fn from(value: EventValidationError) -> Self {
        EventError::Validation(value)
    }
}

#[derive(Clone, Debug, Default)]
pub struct SubscriptionOptions {
    pub partition: Option<String>,
    pub qos: Option<EventQos>,
}

impl SubscriptionOptions {
    #[allow(dead_code)]
    pub fn for_partition<P>(partition: P) -> Self
    where
        P: Into<String>,
    {
        Self {
            partition: Some(partition.into()),
            qos: None,
        }
    }

    #[allow(dead_code)]
    pub fn with_qos(mut self, qos: EventQos) -> Self {
        self.qos = Some(qos);
        self
    }
}

pub struct Subscription {
    pub stream: EventStream,
    #[allow(dead_code)]
    cancel_token: CancellationToken,
}

impl Subscription {
    #[allow(dead_code)]
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }

    pub(crate) fn new(stream: EventStream, cancel_token: CancellationToken) -> Self {
        Self {
            stream,
            cancel_token,
        }
    }
}

/// Publishes events to subscribers using at-most-once semantics.
///
/// Publishers must not assume durability. Ordering is constrained by the backend and the
/// configured [`OrderingScope`]. Backpressure handling is backend-specific but always expressed
/// using [`EventError`] variants rather than opaque errors.
pub trait EventPublisher {
    fn publish<'a>(&'a self, event: EventEnvelope) -> PublishFuture<'a>;
}

/// Subscribes to events for a topic.
///
/// Subscription lifetimes are controlled by the returned [`Subscription`] and can be explicitly
/// canceled via [`Subscription::cancel`]. Backends may drop messages when subscribers lag; such
/// drops are surfaced as [`EventError::SubscriberLagged`].
pub trait EventSubscriber {
    fn subscribe(
        &self,
        topic: &EventTopic,
        options: SubscriptionOptions,
    ) -> Result<Subscription, EventError>;
}

fn is_valid_trace_id(trace_id: &str) -> bool {
    const MAX_TRACE_ID_LENGTH: usize = 128;
    !trace_id.is_empty()
        && trace_id.len() <= MAX_TRACE_ID_LENGTH
        && trace_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.')
}
