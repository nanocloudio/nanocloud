/*
 * Copyright (C) 2024 The Nanocloud Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use super::error::ApiError;
use axum::body::Body;
use axum::http::StatusCode;
use bytes::Bytes;
use futures_util::future::pending;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{interval, MissedTickBehavior};
use tokio_stream::wrappers::ReceiverStream;

use crate::nanocloud::k8s::configmap::ConfigMap;
use crate::nanocloud::k8s::configmap_manager::ConfigMapWatchEvent;
use crate::nanocloud::k8s::daemonset::DaemonSet;
use crate::nanocloud::k8s::deployment::Deployment;
use crate::nanocloud::k8s::event::{Event, EventWatchEvent};
use crate::nanocloud::k8s::job::Job;
use crate::nanocloud::k8s::pod::{ObjectMeta, Pod};
use crate::nanocloud::k8s::replicaset::ReplicaSet;
use crate::nanocloud::k8s::statefulset::StatefulSet;
use crate::nanocloud::kubelet::WatchEvent;
use crate::nanocloud::logger::log_warn;

const CHANNEL_BUFFER: usize = 32;
const DEFAULT_BOOKMARK_INTERVAL: Duration = Duration::from_secs(15);

pub(super) type WatchPredicate<T> = dyn Fn(&T) -> bool + Send + Sync;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ResourceVersionMatchPolicy {
    Exact,
    NotOlderThan,
}

pub(super) fn parse_resource_version(
    resource_version: Option<&str>,
) -> Result<Option<u64>, ApiError> {
    resource_version
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| ApiError::bad_request("resourceVersion must be a positive integer"))
        })
        .transpose()
}

pub(super) fn resource_version_is_newer<T: WatchItem>(object: &T, since: Option<u64>) -> bool {
    since
        .map(|threshold| {
            object
                .metadata()
                .resource_version
                .as_deref()
                .and_then(|value| value.parse::<u64>().ok())
                .map(|current| current > threshold)
                .unwrap_or(true)
        })
        .unwrap_or(true)
}

pub(super) fn ensure_resource_version_match(
    policy: Option<ResourceVersionMatchPolicy>,
    requested: Option<u64>,
    current: Option<u64>,
) -> Result<(), ApiError> {
    let Some(policy) = policy else {
        return Ok(());
    };

    let requested = requested.ok_or_else(|| {
        ApiError::bad_request("resourceVersionMatch requires resourceVersion to be set")
    })?;
    let current = current.ok_or_else(|| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server resourceVersion is unavailable",
        )
    })?;

    match policy {
        ResourceVersionMatchPolicy::Exact => match requested.cmp(&current) {
            std::cmp::Ordering::Equal => Ok(()),
            std::cmp::Ordering::Less => Err(ApiError::new(
                StatusCode::GONE,
                "requested resourceVersion has expired",
            )),
            std::cmp::Ordering::Greater => Err(ApiError::new(
                StatusCode::GONE,
                "requested resourceVersion is ahead of current state",
            )),
        },
        ResourceVersionMatchPolicy::NotOlderThan => {
            if requested <= current {
                Ok(())
            } else {
                Err(ApiError::new(
                    StatusCode::GONE,
                    "requested resourceVersion is ahead of current state",
                ))
            }
        }
    }
}

pub(super) trait WatchItem: Serialize + Send + 'static {
    fn metadata(&self) -> &ObjectMeta;
}

impl WatchItem for Pod {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}

impl WatchItem for ConfigMap {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}

impl WatchItem for Job {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}

impl WatchItem for Event {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}

impl WatchItem for StatefulSet {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}

impl WatchItem for ReplicaSet {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}

impl WatchItem for Deployment {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}

impl WatchItem for DaemonSet {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}

pub(super) trait WatchEventLike: Send + Clone + 'static {
    type Object: WatchItem;

    fn into_parts(self) -> (String, Self::Object);
}

impl<T> WatchEventLike for WatchEvent<T>
where
    T: WatchItem + Clone,
{
    type Object = T;

    fn into_parts(self) -> (String, Self::Object) {
        (self.event_type, self.object)
    }
}

impl WatchEventLike for ConfigMapWatchEvent {
    type Object = ConfigMap;

    fn into_parts(self) -> (String, Self::Object) {
        (self.event_type, self.object)
    }
}

impl WatchEventLike for EventWatchEvent {
    type Object = Event;

    fn into_parts(self) -> (String, Self::Object) {
        (self.event_type, self.object)
    }
}

#[derive(Serialize)]
struct GenericEvent<T> {
    #[serde(rename = "type")]
    event_type: String,
    object: T,
}

#[derive(Serialize)]
struct BookmarkEvent<'a> {
    #[serde(rename = "type")]
    event_type: &'static str,
    object: BookmarkObject<'a>,
}

#[derive(Serialize)]
struct BookmarkObject<'a> {
    metadata: BookmarkMetadata<'a>,
}

#[derive(Serialize)]
struct BookmarkMetadata<'a> {
    #[serde(rename = "resourceVersion")]
    resource_version: &'a str,
}

pub(super) struct WatchStreamBuilder<E>
where
    E: WatchEventLike,
{
    log_target: &'static str,
    serialization_error: &'static str,
    initial: Vec<E>,
    receiver: broadcast::Receiver<E>,
    filter: Option<Arc<WatchPredicate<E::Object>>>,
    allow_bookmarks: bool,
    timeout: Option<Duration>,
    bookmark_interval: Duration,
}

impl<E> WatchStreamBuilder<E>
where
    E: WatchEventLike,
{
    pub fn new(
        log_target: &'static str,
        serialization_error: &'static str,
        initial: Vec<E>,
        receiver: broadcast::Receiver<E>,
    ) -> Self {
        Self {
            log_target,
            serialization_error,
            initial,
            receiver,
            filter: None,
            allow_bookmarks: false,
            timeout: None,
            bookmark_interval: DEFAULT_BOOKMARK_INTERVAL,
        }
    }

    pub fn with_filter(mut self, filter: Option<Arc<WatchPredicate<E::Object>>>) -> Self {
        self.filter = filter;
        self
    }

    pub fn with_bookmarks(mut self, allow: bool) -> Self {
        self.allow_bookmarks = allow;
        self
    }

    pub fn with_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    #[cfg(test)]
    pub fn with_bookmark_interval(mut self, interval: Duration) -> Self {
        self.bookmark_interval = interval;
        self
    }

    pub fn into_body(self) -> Body {
        Body::from_stream(self.into_stream())
    }

    fn into_stream(self) -> ReceiverStream<Result<Bytes, Infallible>> {
        let Self {
            log_target,
            serialization_error,
            initial,
            receiver,
            filter,
            allow_bookmarks,
            timeout,
            bookmark_interval,
        } = self;

        let (tx, rx) = mpsc::channel(CHANNEL_BUFFER);
        tokio::spawn(async move {
            watch_loop(
                initial,
                receiver,
                WatchLoopConfig {
                    log_target,
                    serialization_error,
                    filter,
                    allow_bookmarks,
                    timeout,
                    bookmark_interval,
                },
                tx,
            )
            .await;
        });

        ReceiverStream::new(rx)
    }
}

struct WatchLoopConfig<E: WatchEventLike> {
    log_target: &'static str,
    serialization_error: &'static str,
    filter: Option<Arc<WatchPredicate<E::Object>>>,
    allow_bookmarks: bool,
    timeout: Option<Duration>,
    bookmark_interval: Duration,
}

async fn watch_loop<E>(
    initial: Vec<E>,
    mut receiver: broadcast::Receiver<E>,
    config: WatchLoopConfig<E>,
    tx: mpsc::Sender<Result<Bytes, Infallible>>,
) where
    E: WatchEventLike,
{
    let WatchLoopConfig {
        log_target,
        serialization_error,
        filter,
        allow_bookmarks,
        timeout,
        bookmark_interval,
    } = config;

    let mut sender = tx;
    let mut last_seen: Option<String> = None;

    for event in initial {
        let continue_stream = process_event(
            &mut sender,
            event,
            &filter,
            &mut last_seen,
            log_target,
            serialization_error,
            "initial",
        )
        .await;
        if !continue_stream {
            return;
        }
    }

    let mut timeout_fut: Option<Pin<Box<tokio::time::Sleep>>> =
        timeout.map(|duration| Box::pin(tokio::time::sleep(duration)));

    let mut maybe_interval = if allow_bookmarks {
        let mut interval = interval(bookmark_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Some((interval, false))
    } else {
        None
    };

    loop {
        tokio::select! {
            result = receiver.recv() => {
                match result {
                    Ok(event) => {
                        let continue_stream = process_event(
                            &mut sender,
                            event,
                            &filter,
                            &mut last_seen,
                            log_target,
                            serialization_error,
                            "update",
                        ).await;
                        if !continue_stream {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        log_warn(
                            log_target,
                            "Watch channel lagged",
                            &[("skipped", &skipped.to_string())],
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = async {
                if let Some((interval, _)) = maybe_interval.as_mut() {
                    interval.tick().await;
                } else {
                    pending::<()>().await;
                }
            }, if maybe_interval.is_some() => {
                if let Some((_, primed)) = maybe_interval.as_mut() {
                    if *primed {
                        let continue_stream = send_bookmark(
                            &mut sender,
                            &last_seen,
                            log_target,
                            serialization_error,
                        )
                        .await;
                        if !continue_stream {
                            return;
                        }
                    } else {
                        *primed = true;
                    }
                }
            }
            _ = async {
                if let Some(timeout) = timeout_fut.as_mut() {
                    timeout.await;
                } else {
                    pending::<()>().await;
                }
            }, if timeout_fut.is_some() => {
                if allow_bookmarks {
                    let continue_stream = send_bookmark(
                        &mut sender,
                        &last_seen,
                        log_target,
                        serialization_error,
                    ).await;
                    if !continue_stream {
                        return;
                    }
                }
                break;
            }
        }
    }
}

async fn process_event<E>(
    sender: &mut mpsc::Sender<Result<Bytes, Infallible>>,
    event: E,
    filter: &Option<Arc<WatchPredicate<E::Object>>>,
    last_seen: &mut Option<String>,
    log_target: &'static str,
    serialization_error: &'static str,
    phase: &'static str,
) -> bool
where
    E: WatchEventLike,
{
    let (event_type, object) = event.into_parts();
    if let Some(predicate) = filter {
        if !(predicate)(&object) {
            return true;
        }
    }

    let metadata = object.metadata();
    let namespace = metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let name = metadata
        .name
        .clone()
        .unwrap_or_else(|| "<unnamed>".to_string());
    let resource_version = metadata.resource_version.clone();

    match serde_json::to_vec(&GenericEvent { event_type, object }) {
        Ok(mut json) => {
            json.push(b'\n');
            if sender.send(Ok(Bytes::from(json))).await.is_err() {
                return false;
            }
            if let Some(rv) = resource_version {
                *last_seen = Some(rv);
            }
        }
        Err(err) => {
            let error_text = err.to_string();
            log_warn(
                log_target,
                serialization_error,
                &[
                    ("phase", phase),
                    ("namespace", namespace.as_str()),
                    ("name", name.as_str()),
                    ("error", error_text.as_str()),
                ],
            );
        }
    }

    true
}

async fn send_bookmark(
    sender: &mut mpsc::Sender<Result<Bytes, Infallible>>,
    last_seen: &Option<String>,
    log_target: &'static str,
    serialization_error: &'static str,
) -> bool {
    let Some(resource_version) = last_seen.as_deref() else {
        return true;
    };

    let bookmark = BookmarkEvent {
        event_type: "BOOKMARK",
        object: BookmarkObject {
            metadata: BookmarkMetadata { resource_version },
        },
    };

    match serde_json::to_vec(&bookmark) {
        Ok(mut json) => {
            json.push(b'\n');
            if sender.send(Ok(Bytes::from(json))).await.is_err() {
                return false;
            }
        }
        Err(err) => {
            let error_text = err.to_string();
            log_warn(
                log_target,
                serialization_error,
                &[
                    ("phase", "bookmark"),
                    ("namespace", ""),
                    ("name", ""),
                    ("error", error_text.as_str()),
                ],
            );
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::pod::PodSpec;
    use axum::response::IntoResponse;
    use futures_util::StreamExt;
    use tokio::sync::broadcast;

    fn pod_event(name: &str, namespace: &str, resource_version: &str) -> WatchEvent<Pod> {
        let metadata = ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Default::default(),
            annotations: Default::default(),
            resource_version: Some(resource_version.to_string()),
        };
        WatchEvent {
            event_type: "ADDED".to_string(),
            object: Pod::new(metadata, PodSpec::default()),
        }
    }

    #[tokio::test]
    async fn emits_initial_events() {
        let (tx, _) = broadcast::channel(4);
        let stream = WatchStreamBuilder::new(
            "test_pods",
            "Pod watch serialization error",
            vec![pod_event("pod-a", "default", "1")],
            tx.subscribe(),
        )
        .into_stream();

        let mut messages = stream.take(1);
        let first = messages.next().await.unwrap().unwrap();
        let text = String::from_utf8(first.to_vec()).unwrap();
        assert!(text.contains("\"type\":\"ADDED\""));
        assert!(text.contains("\"resourceVersion\":\"1\""));
        assert!(text.ends_with('\n'));
    }

    #[tokio::test]
    async fn emits_bookmark_after_interval() {
        let (tx, _) = broadcast::channel(4);
        let stream = WatchStreamBuilder::new(
            "test_pods",
            "Pod watch serialization error",
            vec![pod_event("pod-a", "default", "1")],
            tx.subscribe(),
        )
        .with_bookmarks(true)
        .with_bookmark_interval(Duration::from_millis(20))
        .into_stream();

        let mut messages = stream;
        let first = messages.next().await.unwrap().unwrap();
        let _ = String::from_utf8(first.to_vec()).unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        let bookmark = messages.next().await.unwrap().unwrap();
        let text = String::from_utf8(bookmark.to_vec()).unwrap();
        assert!(text.contains("\"type\":\"BOOKMARK\""));
        assert!(text.contains("\"resourceVersion\":\"1\""));
    }

    #[tokio::test]
    async fn stops_after_timeout() {
        let (tx, _) = broadcast::channel(4);
        let stream = WatchStreamBuilder::new(
            "test_pods",
            "Pod watch serialization error",
            vec![pod_event("pod-a", "default", "1")],
            tx.subscribe(),
        )
        .with_timeout(Some(Duration::from_millis(20)))
        .into_stream();

        let mut messages = stream;
        let _ = messages.next().await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(messages.next().await.is_none());
    }

    #[test]
    fn parse_resource_version_validates_input() {
        assert_eq!(parse_resource_version(None).unwrap(), None);
        assert_eq!(parse_resource_version(Some("42")).unwrap(), Some(42));
        assert!(parse_resource_version(Some("abc")).is_err());
    }

    #[test]
    fn resource_version_newer_detection() {
        let pod = pod_event("pod-a", "default", "5").object;
        assert!(resource_version_is_newer(&pod, Some(4)));
        assert!(!resource_version_is_newer(&pod, Some(5)));
        assert!(!resource_version_is_newer(&pod, Some(6)));
        assert!(resource_version_is_newer(&pod, None));
    }

    #[test]
    fn resource_version_match_exact_requires_equal() {
        ensure_resource_version_match(Some(ResourceVersionMatchPolicy::Exact), Some(5), Some(5))
            .expect("matching resourceVersion should succeed");
    }

    #[test]
    fn resource_version_match_exact_rejects_stale_or_future() {
        let stale = ensure_resource_version_match(
            Some(ResourceVersionMatchPolicy::Exact),
            Some(4),
            Some(6),
        )
        .expect_err("stale resourceVersion should error");
        assert_eq!(stale.into_response().status(), StatusCode::GONE);

        let future = ensure_resource_version_match(
            Some(ResourceVersionMatchPolicy::Exact),
            Some(7),
            Some(5),
        )
        .expect_err("future resourceVersion should error");
        assert_eq!(future.into_response().status(), StatusCode::GONE);
    }

    #[test]
    fn resource_version_match_not_older_than_checks_upper_bound() {
        ensure_resource_version_match(
            Some(ResourceVersionMatchPolicy::NotOlderThan),
            Some(3),
            Some(5),
        )
        .expect("older resourceVersion should be accepted");

        let err = ensure_resource_version_match(
            Some(ResourceVersionMatchPolicy::NotOlderThan),
            Some(9),
            Some(5),
        )
        .expect_err("newer resourceVersion should fail");
        assert_eq!(err.into_response().status(), StatusCode::GONE);
    }
}
