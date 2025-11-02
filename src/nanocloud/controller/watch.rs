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

const WATCH_BUFFER_SIZE: usize = 64;
const BACKOFF_INITIAL_MS: u64 = 200;
const BACKOFF_MAX_MS: u64 = 10_000;

pub type ControllerWatchEvent = KeyspaceEvent;

#[derive(Clone)]
pub struct ControllerWatchManager {
    inner: Arc<Inner>,
}

struct Inner {
    keyspace: Keyspace,
    watches: Mutex<HashMap<WatchKey, Arc<WatchState>>>,
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
}

impl WatchState {
    fn new(path: String) -> Self {
        let (sender, _) = broadcast::channel(WATCH_BUFFER_SIZE);
        Self {
            sender,
            subscribers: AtomicUsize::new(0),
            shutdown: CancellationToken::new(),
            task: Mutex::new(None),
            path,
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
}

impl ControllerWatchSubscription {
    pub async fn recv(&mut self) -> Option<ControllerWatchEvent> {
        loop {
            match self.receiver.recv().await {
                Ok(event) => return Some(event),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
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
            .get_or_init(|| ControllerWatchManager::create(Keyspace::new("k8s")))
            .clone()
    }

    pub fn controllers() -> Self {
        static INSTANCE: OnceLock<ControllerWatchManager> = OnceLock::new();
        INSTANCE
            .get_or_init(|| ControllerWatchManager::create(Keyspace::new("controllers")))
            .clone()
    }

    fn create(keyspace: Keyspace) -> Self {
        Self {
            inner: Arc::new(Inner {
                keyspace,
                watches: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn subscribe(&self, prefix: &str, namespace: Option<&str>) -> ControllerWatchSubscription {
        let key = WatchKey::new(prefix, namespace);
        let state = self.inner.get_or_create_state(&key);
        let receiver = state.subscribe();
        ControllerWatchSubscription {
            key,
            receiver,
            inner: Arc::clone(&self.inner),
            state,
        }
    }

    #[cfg(test)]
    pub fn with_keyspace(keyspace: Keyspace) -> Self {
        Self::create(keyspace)
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
                let state = Arc::new(WatchState::new(key.watch_path()));
                state.start(self.keyspace);
                entry.insert(state.clone());
                state
            }
        }
    }
}

async fn run_watch_loop(state: Arc<WatchState>, keyspace: Keyspace) {
    let mut last_version = 0u64;
    let mut backoff = Duration::from_millis(BACKOFF_INITIAL_MS);

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
                        backoff = Duration::from_millis(BACKOFF_INITIAL_MS);
                        last_version = event.resource_version;
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
        let next = backoff * 2;
        backoff = if next > Duration::from_millis(BACKOFF_MAX_MS) {
            Duration::from_millis(BACKOFF_MAX_MS)
        } else {
            next
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::test_support::keyspace_lock;
    use std::env;
    use std::fs;
    use std::sync::{MutexGuard, OnceLock};
    use tokio::sync::Mutex;
    use tokio::time::{sleep, timeout, Duration};

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
}
