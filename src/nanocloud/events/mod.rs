use chrono::{DateTime, Utc};
use futures_util::stream::BoxStream;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

pub type PublishFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;
pub type EventStream<E> = BoxStream<'static, Result<EventEnvelope, E>>;

pub mod in_memory;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EventTopic {
    pub scope: String,
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

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.scope, self.name)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EventKey {
    pub partition: String,
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

    pub fn with_attribute<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.attributes.insert(key.into(), value.into());
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct SubscriptionOptions;

pub struct Subscription<E> {
    pub stream: EventStream<E>,
}

pub trait EventPublisher {
    type Error;

    fn publish<'a>(&'a self, event: EventEnvelope) -> PublishFuture<'a, Self::Error>;
}

pub trait EventSubscriber {
    type Error;

    fn subscribe(
        &self,
        topic: &EventTopic,
        options: SubscriptionOptions,
    ) -> Result<Subscription<Self::Error>, Self::Error>;
}
