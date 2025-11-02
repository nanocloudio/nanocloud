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
use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::Config;

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::error::Error;
use std::fmt::Display;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::RuntimeFlavor;
use tokio::sync::broadcast;

const VALUE_FILE_NAME: &str = "_value_";
const EXPIRY_FILE_NAME: &str = "_expiry_";
const KEYSPACE_COMPONENT: &str = "keyspace";
const MAX_KEY_DEPTH: usize = 16;
const MAX_KEY_LENGTH: usize = 512;
const WATCH_HISTORY_LIMIT: usize = 512;
const WATCH_CHANNEL_CAPACITY: usize = 128;
const KEYSPACE_BLOCKING_WORKERS: usize = 4;

type KeyParser<K> = Arc<dyn Fn(&str) -> Result<K, Box<dyn Error + Send + Sync>> + Send + Sync>;

struct BlockingExecutor {
    sender: mpsc::Sender<Job>,
    queue_depth: Arc<AtomicUsize>,
}

struct Job {
    queued_at: Instant,
    run: Box<dyn FnOnce(Duration) + Send + 'static>,
}

impl BlockingExecutor {
    fn new(workers: usize) -> Self {
        let (sender, receiver) = mpsc::channel::<Job>();
        let queue_depth = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let shared_receiver = Arc::new(Mutex::new(receiver));

        for index in 0..workers {
            let receiver = Arc::clone(&shared_receiver);
            let queue_depth = Arc::clone(&queue_depth);
            let active = Arc::clone(&active);
            thread::Builder::new()
                .name(format!("keyspace-blocking-{index}"))
                .spawn(move || worker_loop(receiver, queue_depth, active))
                .expect("failed to spawn keyspace blocking worker");
        }

        BlockingExecutor {
            sender,
            queue_depth,
        }
    }

    fn submit<R, E, F>(&self, operation: &'static str, work: F) -> Result<R, E>
    where
        F: FnOnce() -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let job = Job {
            queued_at: Instant::now(),
            run: Box::new(move |queue_wait| {
                let start = Instant::now();
                let result = work();
                let run_duration = start.elapsed();
                metrics::observe_keyspace_blocking(operation, queue_wait, run_duration);
                let _ = result_tx.send(result);
            }),
        };

        self.queue_depth.fetch_add(1, Ordering::SeqCst);
        metrics::set_keyspace_blocking_queue(self.queue_depth.load(Ordering::SeqCst));

        if self.sender.send(job).is_err() {
            self.queue_depth.fetch_sub(1, Ordering::SeqCst);
            metrics::set_keyspace_blocking_queue(self.queue_depth.load(Ordering::SeqCst));
            panic!("keyspace blocking executor shut down unexpectedly");
        }

        result_rx
            .recv()
            .expect("keyspace blocking worker dropped result")
    }
}

fn worker_loop(
    receiver: Arc<Mutex<mpsc::Receiver<Job>>>,
    queue_depth: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
) {
    loop {
        let job = {
            let guard = receiver
                .lock()
                .expect("keyspace blocking receiver lock poisoned");
            guard.recv()
        };

        match job {
            Ok(job) => {
                queue_depth.fetch_sub(1, Ordering::SeqCst);
                metrics::set_keyspace_blocking_queue(queue_depth.load(Ordering::SeqCst));
                active.fetch_add(1, Ordering::SeqCst);
                metrics::set_keyspace_blocking_active(active.load(Ordering::SeqCst));
                let queue_wait = job.queued_at.elapsed();
                (job.run)(queue_wait);
                active.fetch_sub(1, Ordering::SeqCst);
                metrics::set_keyspace_blocking_active(active.load(Ordering::SeqCst));
            }
            Err(_) => break,
        }
    }
}

fn blocking_executor() -> &'static BlockingExecutor {
    static EXECUTOR: OnceLock<BlockingExecutor> = OnceLock::new();
    EXECUTOR.get_or_init(|| BlockingExecutor::new(KEYSPACE_BLOCKING_WORKERS))
}

fn run_blocking<R, E, F>(operation: &'static str, work: F) -> Result<R, E>
where
    F: FnOnce() -> Result<R, E> + Send + 'static,
    R: Send + 'static,
    E: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => match handle.runtime_flavor() {
            RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| blocking_executor().submit(operation, work))
            }
            RuntimeFlavor::CurrentThread => blocking_executor().submit(operation, work),
            _ => blocking_executor().submit(operation, work),
        },
        Err(_) => blocking_executor().submit(operation, work),
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum SingleUseTokenOutcome {
    Consumed {
        value: String,
        expires_at: Option<SystemTime>,
    },
    Expired {
        expires_at: Option<SystemTime>,
    },
    Replay,
    NotFound,
    MissingExpiry,
    TtlBudgetExceeded {
        expires_at: SystemTime,
        max_ttl: Duration,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum KeyspaceEventType {
    Added,
    Modified,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KeyspaceEvent {
    #[serde(rename = "type")]
    pub event_type: KeyspaceEventType,
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(rename = "resourceVersion")]
    pub resource_version: u64,
}

impl KeyspaceEvent {
    fn matches_prefix(&self, prefix: &str) -> bool {
        if prefix == "/" {
            true
        } else {
            self.key.starts_with(prefix)
        }
    }
}

struct PartitionWatch {
    sender: broadcast::Sender<KeyspaceEvent>,
    history: RwLock<VecDeque<KeyspaceEvent>>,
    version: AtomicU64,
}

impl PartitionWatch {
    fn new() -> Self {
        let (sender, _) = broadcast::channel(WATCH_CHANNEL_CAPACITY);
        Self {
            sender,
            history: RwLock::new(VecDeque::new()),
            version: AtomicU64::new(0),
        }
    }

    fn next_version(&self) -> u64 {
        self.version.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn record(&self, event: KeyspaceEvent) {
        {
            let mut history = self
                .history
                .write()
                .expect("keyspace watch history lock poisoned");
            history.push_back(event.clone());
            if history.len() > WATCH_HISTORY_LIMIT {
                history.pop_front();
            }
        }
        let _ = self.sender.send(event);
    }

    fn snapshot_since(&self, since: u64) -> VecDeque<KeyspaceEvent> {
        let history = self
            .history
            .read()
            .expect("keyspace watch history lock poisoned");
        history
            .iter()
            .filter(|event| event.resource_version > since)
            .cloned()
            .collect()
    }

    fn subscribe(&self) -> broadcast::Receiver<KeyspaceEvent> {
        self.sender.subscribe()
    }
}

fn watch_registry() -> &'static Mutex<HashMap<&'static str, Arc<PartitionWatch>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<&'static str, Arc<PartitionWatch>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn get_partition_watch(partition: &'static str) -> Arc<PartitionWatch> {
    let registry = watch_registry();
    let mut guard = registry
        .lock()
        .expect("keyspace watch registry lock poisoned");
    guard
        .entry(partition)
        .or_insert_with(|| Arc::new(PartitionWatch::new()))
        .clone()
}

fn normalize_watch_prefix(prefix: &str) -> String {
    if prefix.is_empty() || prefix == "/" {
        "/".to_string()
    } else if prefix.starts_with('/') {
        prefix.to_string()
    } else {
        format!("/{}", prefix)
    }
}

fn publish_partition_event(
    partition: &'static str,
    key: String,
    value: Option<String>,
    event_type: KeyspaceEventType,
) {
    let watch = get_partition_watch(partition);
    let resource_version = watch.next_version();
    let event = KeyspaceEvent {
        event_type,
        key,
        value,
        resource_version,
    };
    watch.record(event);
}

pub struct KeyspaceWatchStream {
    prefix: String,
    receiver: broadcast::Receiver<KeyspaceEvent>,
    partition: Arc<PartitionWatch>,
    backlog: VecDeque<KeyspaceEvent>,
    last_version: u64,
}

impl KeyspaceWatchStream {
    fn new(partition: Arc<PartitionWatch>, prefix: String, since: u64) -> Self {
        let receiver = partition.subscribe();
        let mut stream = Self {
            prefix,
            receiver,
            partition,
            backlog: VecDeque::new(),
            last_version: since,
        };
        stream.refill_backlog();
        stream
    }

    fn refill_backlog(&mut self) {
        let events = self.partition.snapshot_since(self.last_version);
        for event in events {
            if event.matches_prefix(&self.prefix) {
                self.backlog.push_back(event);
            }
        }
    }

    pub async fn next(&mut self) -> Option<KeyspaceEvent> {
        if let Some(event) = self.backlog.pop_front() {
            self.last_version = event.resource_version;
            return Some(event);
        }

        loop {
            match self.receiver.recv().await {
                Ok(event) => {
                    if event.resource_version <= self.last_version {
                        continue;
                    }

                    if event.matches_prefix(&self.prefix) {
                        self.last_version = event.resource_version;
                        return Some(event);
                    } else {
                        self.last_version = event.resource_version;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    self.refill_backlog();
                    if let Some(event) = self.backlog.pop_front() {
                        self.last_version = event.resource_version;
                        return Some(event);
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

/// A file-based lock used for concurrent access control.
/// Read paths acquire the shared guard, while writers take the exclusive guard.
/// When an upgrade is required (e.g., pruning expired entries), callers must
/// drop the shared guard before requesting the exclusive variant to preserve
/// lock ordering guarantees relied on by concurrent access tests.
struct FileLock {
    file: File,
}

impl FileLock {
    fn new(shared: bool) -> std::io::Result<Self> {
        let lockfile_path = Config::LockFile.get_path();
        if let Some(parent) = lockfile_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(lockfile_path)?;
        if shared {
            file.lock_shared()?;
        } else {
            file.lock_exclusive()?;
        }
        Ok(Self { file })
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[derive(Clone, Copy)]
pub struct Keyspace {
    partition: &'static str,
}

impl Keyspace {
    pub const fn new(partition: &'static str) -> Keyspace {
        Keyspace { partition }
    }

    /// Returns a stream of keyspace events filtered by prefix starting after an optional resource version.
    pub fn watch(&self, prefix: &str, since: Option<u64>) -> KeyspaceWatchStream {
        let normalized = normalize_watch_prefix(prefix);
        let partition = get_partition_watch(self.partition);
        KeyspaceWatchStream::new(partition, normalized, since.unwrap_or(0))
    }

    fn execute_blocking<R, F>(
        &self,
        operation: &'static str,
        work: F,
    ) -> Result<R, Box<dyn Error + Send + Sync>>
    where
        F: FnOnce() -> Result<R, Box<dyn Error + Send + Sync>> + Send + 'static,
        R: Send + 'static,
    {
        run_blocking(operation, work)
    }

    fn ensure_repaired(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        static REPAIRED_PARTITIONS: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
        let tracker = REPAIRED_PARTITIONS.get_or_init(|| Mutex::new(HashSet::new()));
        {
            let repaired = tracker
                .lock()
                .map_err(|_| new_error("Keyspace repair tracking lock poisoned"))?;
            if repaired.contains(&self.partition) {
                return Ok(());
            }
        }

        self.repair_partition()?;

        let mut repaired = tracker
            .lock()
            .map_err(|_| new_error("Keyspace repair tracking lock poisoned"))?;
        repaired.insert(self.partition);
        Ok(())
    }

    fn repair_partition(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let _lock = FileLock::new(false).map_err(|e| {
            with_context(
                e,
                "Failed to acquire exclusive keyspace lock for repair pass",
            )
        })?;

        let partition_root = Config::Keyspace.get_path().join(self.partition);
        if !partition_root.exists() {
            return Ok(());
        }

        repair_directory(self.partition, &partition_root, true)?;

        Ok(())
    }

    /// Stores a value under the given key.
    pub fn put(&self, key: &str, value: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let keyspace = *self;
        let key = key.to_string();
        let value = value.to_string();
        self.execute_blocking("put", move || keyspace.put_blocking(&key, &value))
    }

    fn put_blocking(&self, key: &str, value: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.ensure_repaired()?;
        let key_path = resolve_path(self.partition, key)?;
        let _lock = FileLock::new(false)
            .map_err(|e| with_context(e, "Failed to acquire exclusive keyspace lock"))?;
        let value_path = key_path.join(VALUE_FILE_NAME);
        let existed = value_path.exists();

        put_value(&key_path, value, None)?;
        publish_partition_event(
            self.partition,
            key.to_string(),
            Some(value.to_string()),
            if existed {
                KeyspaceEventType::Modified
            } else {
                KeyspaceEventType::Added
            },
        );

        Ok(())
    }

    /// Stores a value under the given key with a TTL.
    pub fn put_with_ttl(
        &self,
        key: &str,
        value: &str,
        ttl: Duration,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if ttl.is_zero() {
            return Err(new_error("TTL must be greater than zero"));
        }

        let keyspace = *self;
        let key = key.to_string();
        let value = value.to_string();
        self.execute_blocking("put_with_ttl", move || {
            keyspace.put_with_ttl_blocking(&key, &value, ttl)
        })
    }

    fn put_with_ttl_blocking(
        &self,
        key: &str,
        value: &str,
        ttl: Duration,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if ttl.is_zero() {
            return Err(new_error("TTL must be greater than zero"));
        }

        self.ensure_repaired()?;
        let key_path = resolve_path(self.partition, key)?;
        let _lock = FileLock::new(false)
            .map_err(|e| with_context(e, "Failed to acquire exclusive keyspace lock"))?;

        let mut ttl_secs = ttl.as_secs();
        if ttl.subsec_nanos() > 0 {
            ttl_secs = ttl_secs
                .checked_add(1)
                .ok_or_else(|| new_error("TTL exceeds supported range"))?;
        }

        if ttl_secs == 0 {
            ttl_secs = 1;
        }

        let expiry = SystemTime::now()
            .checked_add(Duration::from_secs(ttl_secs))
            .ok_or_else(|| new_error("TTL exceeds supported range"))?;

        let value_path = key_path.join(VALUE_FILE_NAME);
        let existed = value_path.exists();
        put_value(&key_path, value, Some(expiry))?;
        publish_partition_event(
            self.partition,
            key.to_string(),
            Some(value.to_string()),
            if existed {
                KeyspaceEventType::Modified
            } else {
                KeyspaceEventType::Added
            },
        );

        Ok(())
    }

    /// Retrieves the value associated with the given key.
    pub fn get(&self, key: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
        let keyspace = *self;
        let key = key.to_string();
        self.execute_blocking("get", move || keyspace.get_blocking(&key))
    }

    fn get_blocking(&self, key: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
        self.ensure_repaired()?;
        let key_path = resolve_path(self.partition, key)?;
        let value_path = key_path.join(VALUE_FILE_NAME);
        let missing_msg = || format!("Value file not found: {}", value_path.display());

        let shared_lock = FileLock::new(true)
            .map_err(|e| with_context(e, "Failed to acquire shared keyspace lock"))?;

        if is_expired(&key_path)? {
            drop(shared_lock);
            let exclusive_lock = FileLock::new(false)
                .map_err(|e| with_context(e, "Failed to acquire exclusive keyspace lock"))?;
            if is_expired(&key_path)? {
                prune_expired(self.partition, &key_path)?;
                drop(exclusive_lock);
                return Err(new_error(missing_msg()));
            }
            let value = match read_value_if_exists(&key_path)? {
                Some(value) => value,
                None => {
                    drop(exclusive_lock);
                    return Err(new_error(missing_msg()));
                }
            };
            drop(exclusive_lock);
            return Ok(value);
        }

        let value = match read_value_if_exists(&key_path)? {
            Some(value) => value,
            None => {
                drop(shared_lock);
                return Err(new_error(missing_msg()));
            }
        };
        drop(shared_lock);
        Ok(value)
    }

    /// Retrieves the value and expiry associated with the given key.
    pub fn get_with_expiry(
        &self,
        key: &str,
    ) -> Result<(String, Option<SystemTime>), Box<dyn Error + Send + Sync>> {
        let keyspace = *self;
        let key = key.to_string();
        self.execute_blocking("get_with_expiry", move || {
            keyspace.get_with_expiry_blocking(&key)
        })
    }

    fn get_with_expiry_blocking(
        &self,
        key: &str,
    ) -> Result<(String, Option<SystemTime>), Box<dyn Error + Send + Sync>> {
        let value = self.get_blocking(key)?;
        let key_path = resolve_path(self.partition, key)?;
        let expiry = read_expiry(&key_path)?;
        Ok((value, expiry))
    }

    /// Consumes a single-use token while enforcing an upper bound on TTL.
    pub fn consume_single_use(
        &self,
        key: &str,
        ttl_budget: Duration,
    ) -> Result<SingleUseTokenOutcome, Box<dyn Error + Send + Sync>> {
        if ttl_budget.is_zero() {
            return Err(new_error("TTL budget must be greater than zero"));
        }

        let keyspace = *self;
        let key = key.to_string();
        self.execute_blocking("consume_single_use", move || {
            keyspace.consume_single_use_blocking(&key, ttl_budget)
        })
    }

    fn consume_single_use_blocking(
        &self,
        key: &str,
        ttl_budget: Duration,
    ) -> Result<SingleUseTokenOutcome, Box<dyn Error + Send + Sync>> {
        if ttl_budget.is_zero() {
            return Err(new_error("TTL budget must be greater than zero"));
        }

        self.ensure_repaired()?;
        let key_path = resolve_path(self.partition, key)?;
        let exclusive_lock = FileLock::new(false)
            .map_err(|e| with_context(e, "Failed to acquire exclusive keyspace lock"))?;

        if !key_path.exists() {
            drop(exclusive_lock);
            return Ok(SingleUseTokenOutcome::NotFound);
        }

        let expiry = read_expiry(&key_path)?;
        let expiry = match expiry {
            Some(expiry) => expiry,
            None => {
                drop(exclusive_lock);
                log_warn(
                    KEYSPACE_COMPONENT,
                    "Single-use token is missing its expiry metadata",
                    &[("partition", self.partition)],
                );
                return Ok(SingleUseTokenOutcome::MissingExpiry);
            }
        };

        let now = SystemTime::now();
        if now >= expiry {
            prune_expired(self.partition, &key_path)?;
            drop(exclusive_lock);
            log_warn(
                KEYSPACE_COMPONENT,
                "Expired single-use token rejected",
                &[("partition", self.partition)],
            );
            return Ok(SingleUseTokenOutcome::Expired {
                expires_at: Some(expiry),
            });
        }

        let remaining = expiry
            .duration_since(now)
            .map_err(|e| with_context(e, "Expiry precedes current time"))?;
        if remaining > ttl_budget {
            drop(exclusive_lock);
            log_warn(
                KEYSPACE_COMPONENT,
                "Single-use token exceeds TTL budget",
                &[
                    ("partition", self.partition),
                    ("max_ttl_secs", &ttl_budget.as_secs().to_string()),
                ],
            );
            return Ok(SingleUseTokenOutcome::TtlBudgetExceeded {
                expires_at: expiry,
                max_ttl: ttl_budget,
            });
        }

        match read_value_if_exists(&key_path)? {
            Some(value) => {
                delete(self.partition, &key_path)?;
                drop(exclusive_lock);
                Ok(SingleUseTokenOutcome::Consumed {
                    value,
                    expires_at: Some(expiry),
                })
            }
            None => {
                drop(exclusive_lock);
                log_warn(
                    KEYSPACE_COMPONENT,
                    "Single-use token replay detected",
                    &[("partition", self.partition)],
                );
                Ok(SingleUseTokenOutcome::Replay)
            }
        }
    }

    /// Deletes the value and associated directory for the given key.
    pub fn delete(&self, key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let keyspace = *self;
        let key = key.to_string();
        self.execute_blocking("delete", move || keyspace.delete_blocking(&key))
    }

    fn delete_blocking(&self, key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.ensure_repaired()?;
        let key_path = resolve_path(self.partition, key)?;
        let _lock = FileLock::new(false)
            .map_err(|e| with_context(e, "Failed to acquire exclusive keyspace lock"))?;

        delete(self.partition, &key_path)
    }

    /// Forces a repair pass for this partition.
    pub fn repair_now(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let keyspace = *self;
        self.execute_blocking("repair_now", move || keyspace.repair_partition())
    }

    // /// Lists all keys directly under the given key (non-recursive).
    // pub fn list(&self, key: &str) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
    //     let base_path = resolve_path(&self.partition, key)?;
    //     let _lock = FileLock::new(true)
    //         .map_err(|e| format!("Failed to acquire exclusive lock: {}", e))?;

    //     list(self.partition, &base_path, |s| s.parse::<std::net::IpAddr>().map_err(|e| e.into()))
    // }

    /// Lists all keys directly under the given key (non-recursive).
    pub fn put_first_fit<K, P, S>(
        &self,
        base: &str,
        value: &str,
        key_parser: P,
        key_sequence: S,
    ) -> Result<String, Box<dyn Error + Send + Sync>>
    where
        K: Display + Ord + Send + 'static,
        P: Fn(&str) -> Result<K, Box<dyn Error + Send + Sync>> + Send + Sync + 'static,
        S: IntoIterator<Item = K>,
    {
        let keyspace = *self;
        let base = base.to_string();
        let value = value.to_string();
        let parser_fn = move |input: &str| key_parser(input);
        let parser: KeyParser<K> = Arc::new(parser_fn);
        let candidates: Vec<K> = key_sequence.into_iter().collect();
        self.execute_blocking("put_first_fit", move || {
            keyspace.put_first_fit_blocking(&base, &value, Arc::clone(&parser), candidates)
        })
    }

    fn put_first_fit_blocking<K>(
        &self,
        base: &str,
        value: &str,
        key_parser: KeyParser<K>,
        key_sequence: Vec<K>,
    ) -> Result<String, Box<dyn Error + Send + Sync>>
    where
        K: Display + Ord,
    {
        self.ensure_repaired()?;
        let base_path = resolve_path(self.partition, base)?;
        let _lock = FileLock::new(false)
            .map_err(|e| with_context(e, "Failed to acquire exclusive keyspace lock"))?;

        fs::create_dir_all(&base_path).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to prepare keyspace directory '{}'",
                    base_path.display()
                ),
            )
        })?;

        let mut allocated: Vec<K> = Vec::new();
        for entry in fs::read_dir(&base_path).map_err(|e| {
            with_context(
                e,
                format!(
                    "Failed to read keyspace directory '{}'",
                    base_path.display()
                ),
            )
        })? {
            let entry = entry.map_err(|e| {
                with_context(
                    e,
                    format!(
                        "Failed to iterate keyspace directory '{}'",
                        base_path.display()
                    ),
                )
            })?;
            let path = entry.path();
            if !path.is_dir() || !path.join("_value_").exists() {
                continue;
            }
            let name = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => {
                    return Err(new_error(format!(
                        "Encountered non-UTF8 keyspace entry under '{}'",
                        base_path.display()
                    )));
                }
            };
            allocated.push(key_parser(&name)?);
        }

        allocated.sort();
        let mut allocated = allocated.into_iter();

        for candidate in key_sequence.into_iter() {
            match allocated.next() {
                Some(allocated_key) if allocated_key == candidate => continue,
                _ => {
                    let candidate_str = candidate.to_string();
                    let key = if base == "/" {
                        format!("/{}", candidate_str)
                    } else {
                        format!("{}/{}", base.trim_end_matches('/'), candidate_str)
                    };
                    let key_path = resolve_path(self.partition, &key)?;
                    return put_value(&key_path, value, None).map(|_| candidate_str);
                }
            }
        }

        Err(new_error("No fit found"))
    }
}

/// Resolves a key path into a full, canonicalized path within the data root.
fn resolve_path(partition: &str, key: &str) -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    // Validate the provided key path to ensure it is safe and well-formed.
    if key.is_empty() {
        return Err(new_error("Key path is empty"));
    }

    if key.len() > MAX_KEY_LENGTH {
        return Err(new_error(format!(
            "Key path \"{}\" exceeds max length of {} characters",
            key, MAX_KEY_LENGTH
        )));
    }

    if !is_valid_key_path(key) {
        return Err(new_error(format!(
            "Key path \"{}\" must start with '/' and contain only alphanumeric segments",
            key
        )));
    }

    if key != "/" {
        let mut depth = 0usize;
        for segment in key.split('/').filter(|segment| !segment.is_empty()) {
            if segment == "." || segment == ".." {
                return Err(new_error(format!(
                    "Key path \"{}\" must not contain '.' or '..' segments",
                    key
                )));
            }
            depth += 1;
        }
        if depth > MAX_KEY_DEPTH {
            return Err(new_error(format!(
                "Key path \"{}\" exceeds max depth of {} segments",
                key, MAX_KEY_DEPTH
            )));
        }
    }

    let partition_root = Config::Keyspace.get_path().join(partition);
    fs::create_dir_all(&partition_root).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to prepare keyspace partition '{}'",
                partition_root.display()
            ),
        )
    })?;

    if key == "/" {
        Ok(partition_root)
    } else {
        Ok(partition_root.join(&key[1..]))
    }
}

fn is_valid_key_path(key: &str) -> bool {
    if !key.starts_with('/') {
        return false;
    }
    if key == "/" {
        return true;
    }

    key.split('/').skip(1).all(|segment| {
        !segment.is_empty()
            && segment
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
    })
}

/// Resolves a canonicalized path within the data root into a key.
fn resolve_key(partition: &str, path: &Path) -> Result<String, Box<dyn Error + Send + Sync>> {
    path.strip_prefix(Config::Keyspace.get_path().join(partition))
        .map(|key| key.to_string_lossy().into_owned())
        .map(|key| format!("/{}", key))
        .map_err(|_| {
            new_error(format!(
                "Path {} is not part of keyspace partition {}",
                path.display(),
                partition
            ))
        })
}

/// Stores a value under the given key.
fn put_value(
    key_path: &Path,
    value: &str,
    expiry: Option<SystemTime>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    fs::create_dir_all(key_path).map_err(|e| {
        with_context(
            e,
            format!("Failed to create directories for '{}'", key_path.display()),
        )
    })?;

    let value_file = key_path.join(VALUE_FILE_NAME);
    persist_atomically(&value_file, value.as_bytes())?;

    write_expiry(key_path, expiry)?;

    Ok(())
}

/// Reads the value associated with the given key if it exists.
fn read_value_if_exists(key_path: &Path) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
    let value_file = key_path.join(VALUE_FILE_NAME);
    if !value_file.exists() {
        return Ok(None);
    }

    let mut file = File::open(&value_file).map_err(|e| {
        with_context(
            e,
            format!("Failed to open value file '{}'", value_file.display()),
        )
    })?;

    let mut contents = String::new();
    file.read_to_string(&mut contents).map_err(|e| {
        with_context(
            e,
            format!("Failed to read value file '{}'", value_file.display()),
        )
    })?;

    Ok(Some(contents))
}

/// Deletes the value and associated directory for the given key.
pub fn delete(
    partition: &'static str,
    key_path: &Path,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let value_file = key_path.join(VALUE_FILE_NAME);
    if !value_file.exists() {
        return Err(new_error(format!(
            "Value file not found: {}",
            value_file.display()
        )));
    }

    let key = resolve_key(partition, key_path)?;

    let expiry_file = key_path.join(EXPIRY_FILE_NAME);
    if expiry_file.exists() {
        if let Err(e) = fs::remove_file(&expiry_file) {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(with_context(
                    e,
                    format!("Failed to remove expiry file '{}'", expiry_file.display()),
                ));
            }
        }
    }

    fs::remove_file(&value_file).map_err(|e| {
        with_context(
            e,
            format!("Failed to remove value file '{}'", value_file.display()),
        )
    })?;

    cleanup_empty_dirs(partition, key_path);

    publish_partition_event(partition, key, None, KeyspaceEventType::Deleted);

    Ok(())
}

fn write_expiry(
    key_path: &Path,
    expiry: Option<SystemTime>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let expiry_file = key_path.join(EXPIRY_FILE_NAME);
    match expiry {
        Some(expiry_time) => {
            let duration = expiry_time
                .duration_since(UNIX_EPOCH)
                .map_err(|e| with_context(e, "Expiry precedes UNIX_EPOCH"))?;
            let secs = duration.as_secs();
            persist_atomically(&expiry_file, secs.to_string().as_bytes())?;
        }
        None => {
            if expiry_file.exists() {
                let removed = fs::remove_file(&expiry_file);
                if let Err(e) = removed {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        return Err(with_context(
                            e,
                            format!("Failed to remove expiry file '{}'", expiry_file.display()),
                        ));
                    }
                } else {
                    sync_parent(&expiry_file)?;
                }
            }
        }
    }
    Ok(())
}

fn read_expiry(key_path: &Path) -> Result<Option<SystemTime>, Box<dyn Error + Send + Sync>> {
    let expiry_file = key_path.join(EXPIRY_FILE_NAME);
    if !expiry_file.exists() {
        return Ok(None);
    }

    let contents = fs::read_to_string(&expiry_file).map_err(|e| {
        with_context(
            e,
            format!("Failed to read expiry file '{}'", expiry_file.display()),
        )
    })?;

    let secs: u64 = contents.trim().parse().map_err(|e| {
        with_context(
            e,
            format!("Invalid expiry value in '{}'", expiry_file.display()),
        )
    })?;

    Ok(Some(UNIX_EPOCH + Duration::from_secs(secs)))
}

fn is_expired(key_path: &Path) -> Result<bool, Box<dyn Error + Send + Sync>> {
    match read_expiry(key_path)? {
        Some(expiry) => Ok(SystemTime::now() >= expiry),
        None => Ok(false),
    }
}

fn prune_expired(
    partition: &'static str,
    key_path: &Path,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    write_expiry(key_path, None)?;
    if key_path.join(VALUE_FILE_NAME).exists() {
        delete(partition, key_path)?;
    } else {
        cleanup_empty_dirs(partition, key_path);
    }
    Ok(())
}

fn cleanup_empty_dirs(partition: &str, key_path: &Path) {
    let data_root = Config::Keyspace.get_path().join(partition);
    let mut dir = key_path.to_path_buf();
    while dir != data_root && dir.starts_with(&data_root) {
        match fs::remove_dir(&dir) {
            Ok(_) => {
                if let Some(parent) = dir.parent() {
                    dir = parent.to_path_buf();
                } else {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

fn repair_directory(
    partition: &str,
    dir: &Path,
    is_root: bool,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let read_dir = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(true),
        Err(e) => {
            return Err(with_context(
                e,
                format!("Failed to read keyspace directory '{}'", dir.display()),
            ))
        }
    };

    let mut has_value_file = false;
    let mut descendant_retained = false;

    for entry in read_dir {
        let entry = entry.map_err(|e| {
            with_context(
                e,
                format!("Failed to iterate keyspace directory '{}'", dir.display()),
            )
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|e| {
            with_context(
                e,
                format!("Failed to determine entry type for '{}'", path.display()),
            )
        })?;

        if file_type.is_dir() {
            let removed = repair_directory(partition, &path, false)?;
            if !removed {
                descendant_retained = true;
            }
            continue;
        }

        if file_type.is_file() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.ends_with(".tmp") {
                fs::remove_file(&path).map_err(|e| {
                    with_context(
                        e,
                        format!("Failed to remove orphaned file '{}'", path.display()),
                    )
                })?;
                let path_str = path.display().to_string();
                log_warn(
                    KEYSPACE_COMPONENT,
                    "Removed orphaned keyspace temp file",
                    &[("partition", partition), ("path", &path_str)],
                );
                continue;
            }

            if name_str == VALUE_FILE_NAME {
                has_value_file = true;
                continue;
            }

            if name_str == EXPIRY_FILE_NAME {
                continue;
            }
        }

        descendant_retained = true;
    }

    if !has_value_file {
        let expiry_path = dir.join(EXPIRY_FILE_NAME);
        if expiry_path.exists() {
            fs::remove_file(&expiry_path).map_err(|e| {
                with_context(
                    e,
                    format!(
                        "Failed to remove orphaned expiry file '{}'",
                        expiry_path.display()
                    ),
                )
            })?;
            let path_str = expiry_path.display().to_string();
            log_warn(
                KEYSPACE_COMPONENT,
                "Removed orphaned keyspace expiry file",
                &[("partition", partition), ("path", &path_str)],
            );
        }
    }

    if has_value_file || descendant_retained {
        return Ok(false);
    }

    if is_root {
        return Ok(false);
    }

    match fs::read_dir(dir) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                fs::remove_dir_all(dir).map_err(|e| {
                    with_context(
                        e,
                        format!("Failed to remove partial directory '{}'", dir.display()),
                    )
                })?;
            } else {
                fs::remove_dir(dir).map_err(|e| {
                    with_context(
                        e,
                        format!("Failed to remove empty directory '{}'", dir.display()),
                    )
                })?;
            }
        }
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(true),
        Err(e) => {
            return Err(with_context(
                e,
                format!(
                    "Failed to verify directory contents for '{}'",
                    dir.display()
                ),
            ))
        }
    }

    let dir_str = dir.display().to_string();
    log_warn(
        KEYSPACE_COMPONENT,
        "Removed partial keyspace directory",
        &[("partition", partition), ("path", &dir_str)],
    );

    Ok(true)
}

/// Returns true when a keyspace error indicates a missing value file.
pub fn is_missing_value_error(err: &dyn Error) -> bool {
    let msg = err.to_string();
    msg.contains("No such file or directory") || msg.contains("Value file not found")
}

fn persist_atomically(target: &Path, data: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
    let parent = target.parent().ok_or_else(|| {
        new_error(format!(
            "Target '{}' does not have a parent directory",
            target.display()
        ))
    })?;

    fs::create_dir_all(parent).map_err(|e| {
        with_context(
            e,
            format!("Failed to create parent directory '{}'", parent.display()),
        )
    })?;

    let tmpfile_path = target.with_extension("tmp");
    let mut tmp_guard = TempFileGuard::new(tmpfile_path.clone());
    let mut tmpfile = File::create(&tmpfile_path).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to create temporary file '{}'",
                tmpfile_path.display()
            ),
        )
    })?;

    tmpfile.write_all(data).map_err(|e| {
        with_context(
            e,
            format!(
                "Failed to write to temporary file '{}'",
                tmpfile_path.display()
            ),
        )
    })?;
    tmpfile.sync_all().map_err(|e| {
        with_context(
            e,
            format!("Failed to sync temporary file '{}'", tmpfile_path.display()),
        )
    })?;
    drop(tmpfile);

    if env::var_os("NANOCLOUD_FAIL_KEYSPACE_RENAME").is_some() {
        return Err(new_error("Simulated rename failure"));
    }

    fs::rename(&tmpfile_path, target)
        .map_err(|e| with_context(e, format!("Failed to replace file '{}'", target.display())))?;
    tmp_guard.keep();

    sync_parent(target)?;

    Ok(())
}

fn sync_parent(path: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Some(dir) = path.parent() {
        let dir_file = File::open(dir).map_err(|e| {
            with_context(e, format!("Failed to open directory '{}'", dir.display()))
        })?;
        dir_file.sync_all().map_err(|e| {
            with_context(e, format!("Failed to sync directory '{}'", dir.display()))
        })?;
    }
    Ok(())
}

struct TempFileGuard {
    path: PathBuf,
    keep: bool,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, keep: false }
    }

    fn keep(&mut self) {
        self.keep = true;
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if !self.keep {
            let _ = fs::remove_file(&self.path);
        }
    }
}
