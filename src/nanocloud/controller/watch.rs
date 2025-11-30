#![allow(dead_code)]

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

use crate::nanocloud::logger::log_warn;
use crate::nanocloud::observability::metrics;
use crate::nanocloud::scheduler::{JobResult, ScheduleSpec, ScheduledTaskHandle, Scheduler};
use crate::nanocloud::util::{Keyspace, KeyspaceEvent};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

const DEFAULT_WATCH_BUFFER_SIZE: usize = 64;
const DEFAULT_BACKOFF_INITIAL_MS: u64 = 200;
const DEFAULT_BACKOFF_MAX_MS: u64 = 10_000;
const COMPONENT: &str = "controller-watch";

pub type ControllerWatchEvent = KeyspaceEvent;

#[derive(Clone, Default)]
/// Optional callbacks that surface watch lag/backoff for metrics or tests.
pub struct WatchHooks {
    pub on_backoff: Option<WatchBackoffHook>,
    pub on_lagged: Option<WatchLagHook>,
}

type WatchBackoffHook = Arc<dyn Fn(&str, Duration) + Send + Sync>;
type WatchLagHook = Arc<dyn Fn(&str, u64) + Send + Sync>;

#[derive(Clone)]
/// Tuning knobs for watch buffer sizing, backoff, and observability.
pub struct WatchConfig {
    pub buffer_size: usize,
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
    pub hooks: WatchHooks,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            buffer_size: DEFAULT_WATCH_BUFFER_SIZE,
            backoff_initial: Duration::from_millis(DEFAULT_BACKOFF_INITIAL_MS),
            backoff_max: Duration::from_millis(DEFAULT_BACKOFF_MAX_MS),
            hooks: WatchHooks::default(),
        }
    }
}

#[derive(Clone)]
pub struct ControllerWatchManager {
    inner: Arc<Inner>,
}

struct Inner {
    keyspace: Keyspace,
    watches: Mutex<HashMap<WatchKey, Arc<WatchState>>>,
    config: Arc<WatchConfig>,
    shutdown: CancellationToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WatchKey {
    prefix: String,
    namespace: Option<String>,
}

impl Hash for WatchKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.prefix.hash(state);
        self.namespace.hash(state);
    }
}

impl WatchKey {
    fn new(prefix: &str, namespace: Option<&str>) -> Self {
        let mut normalized = prefix.trim_end_matches('/').to_string();
        if normalized.is_empty() || !normalized.starts_with('/') {
            normalized = format!("/{}", normalized.trim_start_matches('/'));
        }

        let namespace = namespace
            .map(|ns| ns.trim_matches('/').to_string())
            .filter(|ns| !ns.is_empty());

        Self {
            prefix: normalized,
            namespace,
        }
    }

    fn watch_path(&self) -> String {
        match &self.namespace {
            Some(ns) => format!("{}/{}", self.prefix, ns),
            None => self.prefix.clone(),
        }
    }
}

struct WatchState {
    sender: broadcast::Sender<ControllerWatchEvent>,
    subscribers: AtomicUsize,
    shutdown: CancellationToken,
    task: Mutex<Option<ScheduledTaskHandle>>,
    path: String,
    config: Arc<WatchConfig>,
}

impl WatchState {
    fn new(path: String, config: Arc<WatchConfig>, manager_shutdown: CancellationToken) -> Self {
        let (sender, _) = broadcast::channel(config.buffer_size.max(1));
        Self {
            sender,
            subscribers: AtomicUsize::new(0),
            shutdown: manager_shutdown.child_token(),
            task: Mutex::new(None),
            path,
            config,
        }
    }

    fn start(self: &Arc<Self>, keyspace: Keyspace) {
        let state = Arc::clone(self);
        let scheduler = Scheduler::global();
        let handle = scheduler.schedule(
            ScheduleSpec::Immediate {
                label: "controller.watch-loop",
            },
            move |ctx| {
                let state = Arc::clone(&state);
                Box::pin(async move {
                    let cancellation = ctx.cancellation_token();
                    let shutdown = state.shutdown.clone();
                    let mut loop_future = Box::pin(run_watch_loop(Arc::clone(&state), keyspace));
                    let mut cancel_applied = false;

                    loop {
                        tokio::select! {
                            _ = cancellation.cancelled(), if !cancel_applied => {
                                cancel_applied = true;
                                shutdown.cancel();
                            }
                            _ = &mut loop_future => {
                                break;
                            }
                        }
                    }

                    JobResult::Stop
                })
            },
        );
        *self.task.lock().expect("watch task lock poisoned") = Some(handle);
    }

    fn subscribe(&self) -> broadcast::Receiver<ControllerWatchEvent> {
        self.subscribers.fetch_add(1, Ordering::SeqCst);
        self.sender.subscribe()
    }

    fn release(&self) -> bool {
        self.subscribers.fetch_sub(1, Ordering::SeqCst) == 1
    }
}

pub struct ControllerWatchSubscription {
    key: WatchKey,
    receiver: broadcast::Receiver<ControllerWatchEvent>,
    inner: Arc<Inner>,
    state: Arc<WatchState>,
    shutdown: CancellationToken,
}

impl ControllerWatchSubscription {
    /// Receives the next watch event, exiting when the subscription is shut down.
    pub async fn recv(&mut self) -> Option<ControllerWatchEvent> {
        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => return None,
                result = self.receiver.recv() => match result {
                    Ok(event) => return Some(event),
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        self.report_lag(skipped);
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        }
    }

    fn report_lag(&self, skipped: u64) {
        if skipped == 0 {
            return;
        }

        metrics::record_controller_watch_lagged(&self.state.path, skipped);

        if let Some(callback) = self.state.config.hooks.on_lagged.as_ref() {
            callback(&self.state.path, skipped);
        }

        let metadata = [
            ("path".to_string(), self.state.path.clone()),
            ("skipped".to_string(), skipped.to_string()),
        ];
        let metadata_refs = [
            (metadata[0].0.as_str(), metadata[0].1.as_str()),
            (metadata[1].0.as_str(), metadata[1].1.as_str()),
        ];
        log_warn(
            COMPONENT,
            "controller watch subscriber lagged",
            &metadata_refs,
        );
    }
}

impl Drop for ControllerWatchSubscription {
    fn drop(&mut self) {
        if self.state.release() {
            self.state.shutdown.cancel();
            if let Some(handle) = self.state.task.lock().unwrap().take() {
                handle.cancel_and_abort();
            }
            let mut watches = self.inner.watches.lock().unwrap();
            watches.remove(&self.key);
        }
    }
}

impl ControllerWatchManager {
    pub fn shared() -> Self {
        static INSTANCE: OnceLock<ControllerWatchManager> = OnceLock::new();
        INSTANCE
            .get_or_init(|| {
                ControllerWatchManager::create(Keyspace::new("k8s"), WatchConfig::default())
            })
            .clone()
    }

    pub fn controllers() -> Self {
        static INSTANCE: OnceLock<ControllerWatchManager> = OnceLock::new();
        INSTANCE
            .get_or_init(|| {
                ControllerWatchManager::create(Keyspace::new("controllers"), WatchConfig::default())
            })
            .clone()
    }

    fn create(keyspace: Keyspace, config: WatchConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                keyspace,
                watches: Mutex::new(HashMap::new()),
                config: Arc::new(config),
                shutdown: CancellationToken::new(),
            }),
        }
    }

    pub fn with_config(keyspace: Keyspace, config: WatchConfig) -> Self {
        Self::create(keyspace, config)
    }

    /// Subscribes to a prefix within the watch keyspace.
    ///
    /// Subscriptions are lightweight and should be dropped when no longer needed to free
    /// background tasks. Example:
    ///
    /// ```ignore
    /// let manager = ControllerWatchManager::shared();
    /// let mut sub = manager.subscribe("/statefulsets", Some("default"));
    /// while let Some(event) = sub.recv().await {
    ///     handle(event);
    /// }
    /// ```
    pub fn subscribe(&self, prefix: &str, namespace: Option<&str>) -> ControllerWatchSubscription {
        let key = WatchKey::new(prefix, namespace);
        let state = self.inner.get_or_create_state(&key);
        let receiver = state.subscribe();
        let shutdown = state.shutdown.child_token();
        ControllerWatchSubscription {
            key,
            receiver,
            inner: Arc::clone(&self.inner),
            state,
            shutdown,
        }
    }

    /// Cancels all active watches and closes subscriptions.
    pub fn shutdown(&self) {
        self.inner.shutdown.cancel();
        let mut watches = self.inner.watches.lock().unwrap();
        for state in watches.values() {
            state.shutdown.cancel();
            if let Some(handle) = state.task.lock().unwrap().take() {
                handle.cancel_and_abort();
            }
        }
        watches.clear();
    }

    #[cfg(test)]
    pub fn with_keyspace(keyspace: Keyspace) -> Self {
        Self::create(keyspace, WatchConfig::default())
    }

    #[cfg(test)]
    pub fn active_watches(&self) -> usize {
        self.inner.watches.lock().unwrap().len()
    }
}

impl Inner {
    fn get_or_create_state(&self, key: &WatchKey) -> Arc<WatchState> {
        let mut watches = self.watches.lock().expect("watch registry lock poisoned");
        match watches.entry(key.clone()) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let state = Arc::new(WatchState::new(
                    key.watch_path(),
                    Arc::clone(&self.config),
                    self.shutdown.clone(),
                ));
                state.start(self.keyspace);
                entry.insert(state.clone());
                state
            }
        }
    }
}

async fn run_watch_loop(state: Arc<WatchState>, keyspace: Keyspace) {
    let mut last_version = 0u64;
    let mut backoff = state.config.backoff_initial;

    loop {
        let mut stream = keyspace.watch(
            &state.path,
            if last_version == 0 {
                None
            } else {
                Some(last_version)
            },
        );

        loop {
            tokio::select! {
                _ = state.shutdown.cancelled() => return,
                event = stream.next() => match event {
                    Some(event) => {
                        backoff = state.config.backoff_initial;
                        last_version = event.resource_version;
                        if let Err(err) = state.sender.send(event) {
                        let metadata = [
                            ("path".to_string(), state.path.clone()),
                            ("error".to_string(), err.to_string()),
                        ];
                        let metadata_refs = [
                            (metadata[0].0.as_str(), metadata[0].1.as_str()),
                            (metadata[1].0.as_str(), metadata[1].1.as_str()),
                        ];
                            log_warn(
                                COMPONENT,
                                "controller watch fanout failed",
                                &metadata_refs,
                            );
                        }
                    }
                    None => break,
                }
            }
        }

        tokio::select! {
            _ = state.shutdown.cancelled() => return,
            _ = sleep(backoff) => {}
        }
        metrics::record_controller_watch_backoff(&state.path, backoff);
        if let Some(callback) = state.config.hooks.on_backoff.as_ref() {
            callback(&state.path, backoff);
        }
        let next = backoff.saturating_mul(2);
        backoff = if next > state.config.backoff_max {
            state.config.backoff_max
        } else {
            next
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::test_support::keyspace_lock;
    use crate::nanocloud::util::KeyspaceEventType;
    use futures_core::Stream;
    use std::env;
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{MutexGuard, OnceLock};
    use tokio::sync::Mutex;
    use tokio::time::{sleep, timeout, Duration};
    use tokio_stream::{iter, StreamExt};

    struct EnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvGuard {
        fn set_path(key: &'static str, value: &std::path::Path) -> Self {
            let previous = env::var(key).ok();
            env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(prev) = self.previous.as_ref() {
                env::set_var(self.key, prev);
            } else {
                env::remove_var(self.key);
            }
        }
    }

    struct TestEnv {
        _dir: tempfile::TempDir,
        _lock: MutexGuard<'static, ()>,
        _keyspace: EnvGuard,
        _lock_file: EnvGuard,
    }

    impl TestEnv {
        fn new() -> Self {
            let guard = keyspace_lock().lock();
            let dir = tempfile::tempdir().expect("tempdir");
            let base = dir.path();
            let keyspace_dir = base.join("keyspace");
            let lock_dir = base.join("lock");
            fs::create_dir_all(&keyspace_dir).expect("keyspace dir");
            fs::create_dir_all(&lock_dir).expect("lock dir");
            let lock_file = lock_dir.join("nanocloud.lock");
            fs::File::create(&lock_file).expect("lock file");

            let keyspace_guard = EnvGuard::set_path("NANOCLOUD_KEYSPACE", &keyspace_dir);
            let lock_guard = EnvGuard::set_path("NANOCLOUD_LOCK_FILE", &lock_file);

            Self {
                _dir: dir,
                _lock: guard,
                _keyspace: keyspace_guard,
                _lock_file: lock_guard,
            }
        }
    }

    fn test_guard() -> &'static Mutex<()> {
        static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
        GUARD.get_or_init(|| Mutex::new(()))
    }

    #[tokio::test]
    async fn manager_fans_out_events() {
        let _env = TestEnv::new();
        let _lock = test_guard().lock().await;
        let keyspace = Keyspace::new("controller-watch");
        let manager = ControllerWatchManager::with_keyspace(keyspace);

        let mut first = manager.subscribe("/statefulsets", Some("default"));
        let mut second = manager.subscribe("/statefulsets", Some("default"));

        Keyspace::new("controller-watch")
            .put("/statefulsets/default/demo", "value")
            .expect("put demo");

        let event1 = timeout(Duration::from_secs(1), first.recv())
            .await
            .expect("event1 timeout")
            .expect("event1");
        let event2 = timeout(Duration::from_secs(1), second.recv())
            .await
            .expect("event2 timeout")
            .expect("event2");

        assert_eq!(event1.key, "/statefulsets/default/demo");
        assert_eq!(event2.key, "/statefulsets/default/demo");
    }

    #[tokio::test]
    async fn namespace_filtering_excludes_other_scopes() {
        let _env = TestEnv::new();
        let _lock = test_guard().lock().await;
        let manager = ControllerWatchManager::with_keyspace(Keyspace::new("controller-watch-ns"));
        let mut default = manager.subscribe("/statefulsets", Some("default"));
        let mut other = manager.subscribe("/statefulsets", Some("other"));

        Keyspace::new("controller-watch-ns")
            .put("/statefulsets/default/demo", "value")
            .expect("put demo");

        let received = timeout(Duration::from_secs(1), default.recv())
            .await
            .expect("default timeout")
            .expect("default event");
        assert_eq!(received.key, "/statefulsets/default/demo");

        let other_result = timeout(Duration::from_millis(200), other.recv()).await;
        assert!(
            other_result.is_err(),
            "other namespace should not receive event"
        );
    }

    #[tokio::test]
    async fn dropping_subscriptions_cleans_up_watch() {
        let _env = TestEnv::new();
        let _lock = test_guard().lock().await;
        let manager = ControllerWatchManager::with_keyspace(Keyspace::new("controller-watch-drop"));
        let sub_one = manager.subscribe("/replicasets", Some("default"));
        let sub_two = manager.subscribe("/replicasets", Some("default"));

        assert_eq!(manager.active_watches(), 1);

        drop(sub_one);
        assert_eq!(manager.active_watches(), 1);

        drop(sub_two);
        // Allow the cleanup to run.
        sleep(Duration::from_millis(50)).await;
        assert_eq!(manager.active_watches(), 0);
    }

    #[tokio::test]
    async fn lagged_subscribers_report_skips() {
        let _env = TestEnv::new();
        let _lock = test_guard().lock().await;
        let lagged = Arc::new(AtomicUsize::new(0));
        let config = WatchConfig {
            buffer_size: 1,
            backoff_initial: Duration::from_millis(10),
            backoff_max: Duration::from_millis(10),
            hooks: WatchHooks {
                on_lagged: Some({
                    let lagged = lagged.clone();
                    Arc::new(move |_, skipped| {
                        lagged.fetch_add(skipped as usize, Ordering::SeqCst);
                    })
                }),
                ..Default::default()
            },
        };
        let manager =
            ControllerWatchManager::with_config(Keyspace::new("controller-watch-lag"), config);

        let mut sub = manager.subscribe("/deployments", Some("default"));
        let keyspace = Keyspace::new("controller-watch-lag");
        for idx in 0..3 {
            keyspace
                .put(&format!("/deployments/default/demo-{idx}"), "value")
                .expect("put deployment");
        }

        let _ = timeout(Duration::from_secs(1), sub.recv())
            .await
            .expect("recv should complete");

        assert!(
            lagged.load(Ordering::SeqCst) > 0,
            "lag hook should be invoked when receiver skips events"
        );
    }

    #[tokio::test]
    async fn watch_backoff_hook_fires_on_stream_restart() {
        let backoff_calls = Arc::new(AtomicUsize::new(0));
        let config = Arc::new(WatchConfig {
            buffer_size: 4,
            backoff_initial: Duration::from_millis(5),
            backoff_max: Duration::from_millis(5),
            hooks: WatchHooks {
                on_backoff: Some({
                    let backoff_calls = backoff_calls.clone();
                    Arc::new(move |_, _| {
                        backoff_calls.fetch_add(1, Ordering::SeqCst);
                    })
                }),
                ..Default::default()
            },
        });
        let state = Arc::new(WatchState::new(
            "/backoff".to_string(),
            Arc::clone(&config),
            CancellationToken::new(),
        ));
        let mut receiver = state.subscribe();

        let event = ControllerWatchEvent {
            event_type: KeyspaceEventType::Added,
            key: "/backoff/default/demo".to_string(),
            value: Some("value".to_string()),
            resource_version: 1,
        };

        let shutdown = state.shutdown.clone();
        let state_clone = Arc::clone(&state);
        let handle = tokio::spawn(async move {
            run_state_with_stream(state_clone, iter(vec![event])).await;
        });

        let received = timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("receive event")
            .expect("event present");
        assert_eq!(received.key, "/backoff/default/demo");

        // Allow one backoff cycle to execute, then stop the loop.
        tokio::time::sleep(Duration::from_millis(20)).await;
        shutdown.cancel();
        handle.await.expect("watch loop");

        assert!(
            backoff_calls.load(Ordering::SeqCst) > 0,
            "backoff hook should be invoked after stream completion"
        );
    }

    #[tokio::test]
    async fn manager_shutdown_cleans_active_watches() {
        let _env = TestEnv::new();
        let _lock = test_guard().lock().await;
        let manager =
            ControllerWatchManager::with_keyspace(Keyspace::new("controller-watch-shutdown"));
        let _sub = manager.subscribe("/pods", Some("default"));
        assert_eq!(manager.active_watches(), 1);
        manager.shutdown();
        sleep(Duration::from_millis(50)).await;
        assert_eq!(manager.active_watches(), 0);
    }

    async fn run_state_with_stream<S>(state: Arc<WatchState>, mut stream: S)
    where
        S: Stream<Item = ControllerWatchEvent> + Unpin + Send + 'static,
    {
        let mut backoff = state.config.backoff_initial;
        loop {
            loop {
                tokio::select! {
                    _ = state.shutdown.cancelled() => return,
                    maybe_event = stream.next() => match maybe_event {
                        Some(event) => {
                            backoff = state.config.backoff_initial;
                            let _ = state.sender.send(event);
                        }
                        None => break,
                    }
                }
            }

            tokio::select! {
                _ = state.shutdown.cancelled() => return,
                _ = sleep(backoff) => {}
            }
            metrics::record_controller_watch_backoff(&state.path, backoff);
            if let Some(callback) = state.config.hooks.on_backoff.as_ref() {
                callback(&state.path, backoff);
            }
            let next = backoff.saturating_mul(2);
            backoff = if next > state.config.backoff_max {
                state.config.backoff_max
            } else {
                next
            };
        }
    }
}
