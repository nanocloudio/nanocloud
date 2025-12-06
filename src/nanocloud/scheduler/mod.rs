//! Async scheduler with lifecycle controls, rate limits, and observability.
//!
//! # Examples
//! ```rust
//! use nanocloud::scheduler::{JobResult, ScheduleSpec, Scheduler, SchedulerConfig};
//! use std::time::Duration;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let scheduler = Scheduler::new(SchedulerConfig::default());
//! let handle = scheduler.schedule(
//!     ScheduleSpec::After {
//!         label: "example",
//!         delay: Duration::from_millis(10),
//!     },
//!     |_ctx| Box::pin(async { JobResult::Stop }),
//! );
//! handle.join().await.unwrap();
//! scheduler.shutdown_and_join().await;
//! # });
//! ```
#![allow(dead_code)]

use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use futures_util::future::FutureExt;
use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time;
use tokio_util::sync::CancellationToken;

static GLOBAL_SCHEDULER: OnceLock<Mutex<Scheduler>> = OnceLock::new();

pub type JobFuture = Pin<Box<dyn Future<Output = JobResult> + Send>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobResult {
    Stop,
    Continue,
    Backoff(Duration),
}

impl JobResult {}

#[derive(Clone)]
pub struct ScheduleContext {
    task_id: TaskId,
    label: &'static str,
    cancellation: CancellationToken,
    scheduled_for: Option<DateTime<Utc>>,
}

impl ScheduleContext {
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    #[cfg(test)]
    pub fn label(&self) -> &'static str {
        self.label
    }

    pub fn scheduled_for(&self) -> Option<DateTime<Utc>> {
        self.scheduled_for
    }

    #[cfg(test)]
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TaskId(u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskOutcome {
    Completed(JobResult),
    Cancelled,
    Panicked,
}

#[derive(Clone, Debug)]
pub struct TaskReport {
    pub task_id: TaskId,
    pub label: &'static str,
    pub scheduled_for: Option<DateTime<Utc>>,
    pub outcome: TaskOutcome,
}

pub trait SchedulerHooks: Send + Sync {
    fn on_scheduled(&self, _task_id: TaskId, _label: &'static str) {}
    fn on_started(&self, _task_id: TaskId, _label: &'static str) {}
    fn on_completed(&self, _task_id: TaskId, _label: &'static str, _outcome: &TaskOutcome) {}
}

#[derive(Debug)]
struct NoopSchedulerHooks;

impl SchedulerHooks for NoopSchedulerHooks {}

#[derive(Clone)]
pub struct SchedulerConfig {
    pub timezone: Tz,
    pub max_in_flight: Option<usize>,
    pub max_queue_depth: Option<usize>,
    pub shutdown_timeout: Duration,
    pub reporter: Option<mpsc::UnboundedSender<TaskReport>>,
    pub hooks: Arc<dyn SchedulerHooks>,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        SchedulerConfig {
            timezone: chrono_tz::UTC,
            max_in_flight: None,
            max_queue_depth: None,
            shutdown_timeout: Duration::from_secs(2),
            reporter: None,
            hooks: Arc::new(NoopSchedulerHooks),
        }
    }
}

impl SchedulerConfig {
    pub fn with_reporter(mut self, reporter: mpsc::UnboundedSender<TaskReport>) -> Self {
        self.reporter = Some(reporter);
        self
    }

    pub fn with_hooks(mut self, hooks: Arc<dyn SchedulerHooks>) -> Self {
        self.hooks = hooks;
        self
    }

    pub fn with_limits(
        mut self,
        max_in_flight: Option<usize>,
        max_queue_depth: Option<usize>,
    ) -> Self {
        self.max_in_flight = max_in_flight;
        self.max_queue_depth = max_queue_depth;
        self
    }
}

#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<SchedulerInner>,
}

struct SchedulerInner {
    next_id: AtomicU64,
    tasks: Mutex<HashMap<TaskId, TaskState>>,
    config: SchedulerConfig,
    shutdown: CancellationToken,
    hooks: Arc<dyn SchedulerHooks>,
    reporter: Option<mpsc::UnboundedSender<TaskReport>>,
    concurrency: Option<Arc<Semaphore>>,
    queue_budget: Option<Arc<Semaphore>>,
    is_shutdown: AtomicBool,
}

struct TaskState {
    cancellation: CancellationToken,
    join: Arc<Mutex<Option<JoinHandle<()>>>>,
    _queue_permit: Option<OwnedSemaphorePermit>,
}

#[derive(Clone)]
pub struct SchedulerHandle {
    scheduler: Scheduler,
}

#[derive(Clone)]
pub struct CronSchedule {
    expression: Schedule,
    timezone: Tz,
}

#[derive(Clone, Debug)]
pub struct ExponentialBackoff {
    current: Duration,
    factor: f64,
    max: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cancelled;

#[derive(Debug)]
pub enum ScheduleError {
    ShuttingDown,
    AtCapacity,
    InvalidSpec(String),
}

#[derive(Clone, Debug)]
pub enum ScheduleSpec {
    Immediate {
        label: &'static str,
    },
    After {
        label: &'static str,
        delay: Duration,
    },
    Cron {
        label: &'static str,
        schedule: Box<CronSchedule>,
    },
}

type JobFn = dyn Fn(ScheduleContext) -> JobFuture + Send + Sync + 'static;

pub struct ScheduledTaskHandle {
    task_id: TaskId,
    cancellation: CancellationToken,
    join: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Scheduler {
    pub fn new(config: SchedulerConfig) -> Scheduler {
        let queue_budget = config
            .max_queue_depth
            .map(|limit| Arc::new(Semaphore::new(limit)));
        let concurrency = config
            .max_in_flight
            .map(|limit| Arc::new(Semaphore::new(limit)));

        Scheduler {
            inner: Arc::new(SchedulerInner {
                next_id: AtomicU64::new(1),
                tasks: Mutex::new(HashMap::new()),
                hooks: config.hooks.clone(),
                reporter: config.reporter.clone(),
                config,
                shutdown: CancellationToken::new(),
                concurrency,
                queue_budget,
                is_shutdown: AtomicBool::new(false),
            }),
        }
    }

    pub fn handle(&self) -> SchedulerHandle {
        SchedulerHandle {
            scheduler: self.clone(),
        }
    }

    pub fn global() -> Scheduler {
        let lock =
            GLOBAL_SCHEDULER.get_or_init(|| Mutex::new(Scheduler::new(SchedulerConfig::default())));
        let guard = lock.lock().expect("global scheduler lock poisoned");
        guard.clone()
    }

    #[cfg(test)]
    pub async fn reset_global_for_tests() {
        if let Some(lock) = GLOBAL_SCHEDULER.get() {
            let replacement = Scheduler::new(SchedulerConfig::default());
            let old = {
                let guard = lock.lock().expect("global scheduler lock poisoned");
                guard.clone()
            };

            old.shutdown_and_join().await;

            let mut guard = lock.lock().expect("global scheduler lock poisoned");
            *guard = replacement;
        }
    }

    pub fn schedule<F>(&self, spec: ScheduleSpec, job: F) -> ScheduledTaskHandle
    where
        F: Fn(ScheduleContext) -> JobFuture + Send + Sync + 'static,
    {
        self.try_schedule(spec, job)
            .expect("schedule should succeed with default config")
    }

    pub fn try_schedule<F>(
        &self,
        spec: ScheduleSpec,
        job: F,
    ) -> Result<ScheduledTaskHandle, ScheduleError>
    where
        F: Fn(ScheduleContext) -> JobFuture + Send + Sync + 'static,
    {
        if self.inner.is_shutdown.load(Ordering::SeqCst) {
            return Err(ScheduleError::ShuttingDown);
        }

        let queue_permit = self
            .inner
            .queue_budget
            .as_ref()
            .map(|sem| {
                sem.clone()
                    .try_acquire_owned()
                    .map_err(|_| ScheduleError::AtCapacity)
            })
            .transpose()?;

        let task_id = TaskId(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
        self.inner.hooks.on_scheduled(task_id, spec.label());

        let cancellation = self.inner.shutdown.child_token();
        let join_handle = self.spawn_for_spec(
            task_id,
            &cancellation,
            spec.clone(),
            Arc::new(job) as Arc<JobFn>,
        );
        let handle = ScheduledTaskHandle {
            task_id,
            cancellation: cancellation.clone(),
            join: Arc::new(Mutex::new(Some(join_handle))),
        };

        let state = TaskState {
            cancellation,
            join: handle.join.clone(),
            _queue_permit: queue_permit,
        };

        let mut tasks = self
            .inner
            .tasks
            .lock()
            .expect("scheduler task map poisoned");
        tasks.insert(task_id, state);

        Ok(handle)
    }

    pub fn cron_schedule(&self, expression: &str) -> Result<CronSchedule, ScheduleError> {
        CronSchedule::from_str_with_validation(expression, self.inner.config.timezone)
    }

    fn spawn_for_spec(
        &self,
        task_id: TaskId,
        cancellation: &CancellationToken,
        spec: ScheduleSpec,
        job: Arc<JobFn>,
    ) -> JoinHandle<()> {
        let scheduler = self.clone();
        let cancellation = cancellation.clone();
        match spec {
            ScheduleSpec::Immediate { label } => {
                let ctx = ScheduleContext::new(task_id, label, cancellation.clone(), None);
                tokio::spawn(async move {
                    let _ = scheduler.run_once(job.clone(), ctx).await;
                    scheduler.finish_task(task_id);
                })
            }
            ScheduleSpec::After { label, delay } => {
                let scheduled_for = compute_scheduled_time(delay);
                tokio::spawn(async move {
                    let sleeper = time::sleep(delay);
                    tokio::pin!(sleeper);
                    tokio::select! {
                        _ = scheduler.inner.shutdown.cancelled() => {
                            scheduler.record_outcome(task_id, label, scheduled_for, TaskOutcome::Cancelled);
                            scheduler.finish_task(task_id);
                            return;
                        }
                        _ = cancellation.cancelled() => {
                            scheduler.record_outcome(task_id, label, scheduled_for, TaskOutcome::Cancelled);
                            scheduler.finish_task(task_id);
                            return;
                        }
                        _ = sleeper.as_mut() => {}
                    }

                    let ctx =
                        ScheduleContext::new(task_id, label, cancellation.clone(), scheduled_for);
                    let _ = scheduler.run_once(job.clone(), ctx).await;
                    scheduler.finish_task(task_id);
                })
            }
            ScheduleSpec::Cron { label, schedule } => {
                self.spawn_cron(task_id, cancellation, label, *schedule, job)
            }
        }
    }

    fn spawn_cron(
        &self,
        task_id: TaskId,
        cancellation: CancellationToken,
        label: &'static str,
        schedule: CronSchedule,
        job: Arc<JobFn>,
    ) -> JoinHandle<()> {
        let scheduler = self.clone();
        tokio::spawn(async move {
            let mut next = schedule.next_after(Utc::now());
            let mut last_outcome: Option<TaskOutcome> = None;
            let mut recorded = false;

            while let Some(run_at) = next {
                let delay = duration_until(run_at);
                let sleeper = time::sleep(delay);
                tokio::pin!(sleeper);
                tokio::select! {
                    _ = scheduler.inner.shutdown.cancelled() => {
                        last_outcome = Some(TaskOutcome::Cancelled);
                        break;
                    }
                    _ = cancellation.cancelled() => {
                        last_outcome = Some(TaskOutcome::Cancelled);
                        break;
                    }
                    _ = sleeper.as_mut() => {}
                }

                if scheduler.inner.shutdown.is_cancelled() || cancellation.is_cancelled() {
                    last_outcome = Some(TaskOutcome::Cancelled);
                    break;
                }

                let ctx = ScheduleContext::new(task_id, label, cancellation.clone(), Some(run_at));
                let run_result = scheduler.run_once(job.clone(), ctx).await;
                last_outcome = Some(run_result.as_outcome());
                recorded = true;

                match run_result {
                    RunResult::Completed(JobResult::Continue) => {
                        let after = run_at + ChronoDuration::seconds(1);
                        next = schedule.next_after(after);
                    }
                    RunResult::Completed(JobResult::Backoff(backoff)) => {
                        let backoff = ChronoDuration::from_std(backoff)
                            .unwrap_or_else(|_| ChronoDuration::seconds(0));
                        next = Some(Utc::now() + backoff);
                    }
                    _ => break,
                }
            }

            if !recorded {
                if let Some(outcome) = last_outcome {
                    scheduler.record_outcome(task_id, label, None, outcome);
                }
            } else if matches!(last_outcome, Some(TaskOutcome::Cancelled)) {
                scheduler.record_outcome(task_id, label, None, TaskOutcome::Cancelled);
            }
            scheduler.finish_task(task_id);
        })
    }

    async fn run_once(&self, job: Arc<JobFn>, ctx: ScheduleContext) -> RunResult {
        if ctx.cancellation.is_cancelled() || self.inner.shutdown.is_cancelled() {
            self.record_outcome(
                ctx.task_id,
                ctx.label,
                ctx.scheduled_for,
                TaskOutcome::Cancelled,
            );
            return RunResult::Cancelled;
        }

        let _permit = if let Some(semaphore) = &self.inner.concurrency {
            match semaphore.clone().acquire_owned().await {
                Ok(permit) => Some(permit),
                Err(_) => {
                    self.record_outcome(
                        ctx.task_id,
                        ctx.label,
                        ctx.scheduled_for,
                        TaskOutcome::Cancelled,
                    );
                    return RunResult::Cancelled;
                }
            }
        } else {
            None
        };

        self.inner.hooks.on_started(ctx.task_id, ctx.label);
        let job_future = AssertUnwindSafe((job.clone())(ctx.clone()))
            .catch_unwind()
            .await;

        match job_future {
            Ok(result) => {
                let outcome = TaskOutcome::Completed(result);
                self.record_outcome(ctx.task_id, ctx.label, ctx.scheduled_for, outcome.clone());
                RunResult::Completed(result)
            }
            Err(_) => {
                log::error!(
                    "Task {:?} ({}) panicked; stopping task",
                    ctx.task_id,
                    ctx.label
                );
                self.record_outcome(
                    ctx.task_id,
                    ctx.label,
                    ctx.scheduled_for,
                    TaskOutcome::Panicked,
                );
                RunResult::Panicked
            }
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.inner.is_shutdown.load(Ordering::SeqCst)
    }

    pub fn shutdown(&self) {
        if self
            .inner
            .is_shutdown
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        self.inner.shutdown.cancel();
        let tasks = self
            .inner
            .tasks
            .lock()
            .expect("scheduler task map poisoned");
        for state in tasks.values() {
            state.cancellation.cancel();
        }
    }

    pub async fn join(&self) {
        let handles = self.drain_handles();
        for handle in handles {
            tokio::pin!(handle);
            tokio::select! {
                _ = time::sleep(self.inner.config.shutdown_timeout) => {
                    handle.as_mut().abort();
                }
                _ = &mut handle => {}
            }
        }
    }

    pub async fn shutdown_and_join(&self) {
        self.shutdown();
        self.join().await;
    }

    fn finish_task(&self, task_id: TaskId) {
        let mut tasks = self
            .inner
            .tasks
            .lock()
            .expect("scheduler task map poisoned");
        if let Some(state) = tasks.remove(&task_id) {
            drop(state);
        }
    }

    fn drain_handles(&self) -> Vec<JoinHandle<()>> {
        let mut tasks = self
            .inner
            .tasks
            .lock()
            .expect("scheduler task map poisoned");
        let mut handles = Vec::with_capacity(tasks.len());
        for (_, state) in tasks.drain() {
            if let Ok(mut join) = state.join.lock() {
                if let Some(handle) = join.take() {
                    handles.push(handle);
                }
            }
        }

        handles
    }

    fn record_outcome(
        &self,
        task_id: TaskId,
        label: &'static str,
        scheduled_for: Option<DateTime<Utc>>,
        outcome: TaskOutcome,
    ) {
        self.inner.hooks.on_completed(task_id, label, &outcome);
        if let Some(sender) = &self.inner.reporter {
            let _ = sender.send(TaskReport {
                task_id,
                label,
                scheduled_for,
                outcome,
            });
        }
    }
}

impl SchedulerHandle {
    pub fn scheduler(&self) -> Scheduler {
        self.scheduler.clone()
    }

    pub fn shutdown(&self) {
        self.scheduler.shutdown()
    }

    pub async fn join(&self) {
        self.scheduler.join().await
    }

    pub async fn shutdown_and_join(&self) {
        self.scheduler.shutdown_and_join().await
    }
}

impl CronSchedule {
    pub fn new(expression: Schedule, timezone: Tz) -> Self {
        Self {
            expression,
            timezone,
        }
    }

    pub fn from_str(expression: &str, timezone: Tz) -> Result<Self, cron::error::Error> {
        Schedule::from_str(expression).map(|schedule| Self::new(schedule, timezone))
    }

    pub fn from_str_with_validation(expression: &str, timezone: Tz) -> Result<Self, ScheduleError> {
        let parsed = Schedule::from_str(expression)
            .map_err(|err| ScheduleError::InvalidSpec(err.to_string()))?;
        let candidate = Self::new(parsed, timezone);
        if candidate.next_after(Utc::now()).is_none() {
            return Err(ScheduleError::InvalidSpec(
                "schedule produced no upcoming times".to_string(),
            ));
        }
        Ok(candidate)
    }

    pub fn timezone(&self) -> Tz {
        self.timezone
    }

    pub fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let tz_after = self.timezone.from_utc_datetime(&after.naive_utc());
        self.expression
            .after(&tz_after)
            .next()
            .map(|dt| dt.with_timezone(&Utc))
    }
}

impl std::fmt::Debug for CronSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CronSchedule")
            .field("expression", &self.expression.to_string())
            .field("timezone", &self.timezone)
            .finish()
    }
}

impl ScheduleSpec {
    fn label(&self) -> &'static str {
        match self {
            ScheduleSpec::Immediate { label } => label,
            ScheduleSpec::After { label, .. } => label,
            ScheduleSpec::Cron { label, .. } => label,
        }
    }
}

impl ScheduledTaskHandle {
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn abort(&self) {
        if let Some(handle) = self
            .join
            .lock()
            .expect("scheduler join lock poisoned")
            .take()
        {
            handle.abort();
        }
    }

    pub async fn join(&self) -> Result<(), tokio::task::JoinError> {
        let maybe_handle = self
            .join
            .lock()
            .expect("scheduler join lock poisoned")
            .take();
        if let Some(handle) = maybe_handle {
            handle.await.map(|_| ())
        } else {
            Ok(())
        }
    }

    pub fn cancel_and_abort(&self) {
        self.cancel();
        self.abort();
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }
}

impl Drop for ScheduledTaskHandle {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl ExponentialBackoff {
    pub fn new(base: Duration, factor: f64, max: Duration) -> Self {
        ExponentialBackoff {
            current: base,
            factor,
            max,
        }
    }

    pub fn on_error(&mut self, error: &impl Display) -> JobResult {
        log::warn!("retrying after error: {}", error);
        let delay = self.current.min(self.max);
        self.current = Duration::from_secs_f64(
            (self.current.as_secs_f64() * self.factor).min(self.max.as_secs_f64()),
        );
        JobResult::Backoff(delay)
    }

    pub fn reset(&mut self, base: Duration) {
        self.current = base;
    }
}

pub fn ensure_not_cancelled(token: &CancellationToken) -> Result<(), Cancelled> {
    if token.is_cancelled() {
        Err(Cancelled)
    } else {
        Ok(())
    }
}

pub async fn cancellation_point(token: &CancellationToken) -> Result<(), Cancelled> {
    if token.is_cancelled() {
        return Err(Cancelled);
    }

    tokio::select! {
        _ = token.cancelled() => Err(Cancelled),
        _ = tokio::time::sleep(Duration::from_millis(0)) => Ok(()),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RunResult {
    Completed(JobResult),
    Cancelled,
    Panicked,
}

impl RunResult {
    fn as_outcome(&self) -> TaskOutcome {
        match self {
            RunResult::Completed(result) => TaskOutcome::Completed(*result),
            RunResult::Cancelled => TaskOutcome::Cancelled,
            RunResult::Panicked => TaskOutcome::Panicked,
        }
    }
}

fn compute_scheduled_time(delay: Duration) -> Option<DateTime<Utc>> {
    if delay.is_zero() {
        Some(Utc::now())
    } else {
        ChronoDuration::from_std(delay)
            .ok()
            .map(|delta| Utc::now() + delta)
    }
}

impl ScheduleContext {
    fn new(
        task_id: TaskId,
        label: &'static str,
        cancellation: CancellationToken,
        scheduled_for: Option<DateTime<Utc>>,
    ) -> Self {
        ScheduleContext {
            task_id,
            label,
            cancellation,
            scheduled_for,
        }
    }
}

fn duration_until(run_at: DateTime<Utc>) -> Duration {
    let now = Utc::now();
    if run_at <= now {
        Duration::from_secs(0)
    } else {
        (run_at - now)
            .to_std()
            .unwrap_or_else(|_| Duration::from_secs(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time;

    #[tokio::test]
    async fn immediate_task_executes() {
        let scheduler = Scheduler::new(SchedulerConfig::default());
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let handle =
            scheduler.schedule(ScheduleSpec::Immediate { label: "immediate" }, move |ctx| {
                let counter = counter_clone.clone();
                Box::pin(async move {
                    assert_eq!(ctx.label(), "immediate");
                    let _ = ctx.task_id();
                    counter.fetch_add(1, Ordering::SeqCst);
                    JobResult::Stop
                })
            });
        let task_id = handle.task_id();
        handle.join().await.unwrap();
        assert_eq!(task_id, handle.task_id());

        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn delayed_task_executes() {
        let scheduler = Scheduler::new(SchedulerConfig::default());
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        scheduler
            .schedule(
                ScheduleSpec::After {
                    label: "delayed",
                    delay: Duration::from_millis(25),
                },
                move |ctx| {
                    let counter = counter_clone.clone();
                    Box::pin(async move {
                        assert_eq!(ctx.label(), "delayed");
                        assert!(ctx.scheduled_for().is_some());
                        counter.fetch_add(1, Ordering::SeqCst);
                        JobResult::Stop
                    })
                },
            )
            .join()
            .await
            .unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cron_task_repeats_until_stopped() {
        let scheduler = Scheduler::new(SchedulerConfig::default());
        let executions = Arc::new(AtomicUsize::new(0));
        let executions_clone = executions.clone();
        let tz: Tz = chrono_tz::UTC;
        let schedule = CronSchedule::from_str("*/1 * * * * *", tz).expect("cron");

        let handle = scheduler.schedule(
            ScheduleSpec::Cron {
                label: "cron",
                schedule: Box::new(schedule),
            },
            move |ctx| {
                let executions = executions_clone.clone();
                Box::pin(async move {
                    assert!(ctx.scheduled_for().is_some());
                    let count = executions.fetch_add(1, Ordering::SeqCst) + 1;
                    if count >= 2 {
                        JobResult::Stop
                    } else {
                        JobResult::Continue
                    }
                })
            },
        );

        time::sleep(Duration::from_secs(3)).await;
        handle.cancel_and_abort();
        assert!(executions.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn shutdown_cancels_tasks() {
        let scheduler = Scheduler::new(SchedulerConfig::default());
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let handle = scheduler.schedule(
            ScheduleSpec::After {
                label: "long",
                delay: Duration::from_secs(5),
            },
            move |_ctx| {
                let counter = counter_clone.clone();
                Box::pin(async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    JobResult::Stop
                })
            },
        );

        handle.cancel();
        scheduler.shutdown_and_join().await;
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn rate_limits_reject_when_full() {
        let scheduler = Scheduler::new(SchedulerConfig::default().with_limits(Some(1), Some(1)));
        let blocker = CancellationToken::new();
        let blocker_clone = blocker.clone();

        let _handle = scheduler.schedule(ScheduleSpec::Immediate { label: "first" }, move |_ctx| {
            let blocker = blocker_clone.clone();
            Box::pin(async move {
                blocker.cancelled().await;
                JobResult::Stop
            })
        });

        let result = scheduler.try_schedule(ScheduleSpec::Immediate { label: "second" }, |_ctx| {
            Box::pin(async { JobResult::Stop })
        });

        assert!(matches!(result, Err(ScheduleError::AtCapacity)));
        blocker.cancel();
        scheduler.shutdown_and_join().await;
    }

    #[tokio::test]
    async fn panic_is_reported() {
        let scheduler = Scheduler::new(SchedulerConfig::default());
        let handle = scheduler.schedule(ScheduleSpec::Immediate { label: "panic" }, |_ctx| {
            Box::pin(async move {
                panic!("boom");
            })
        });

        let _ = handle.join().await;
        scheduler.shutdown_and_join().await;
    }

    #[tokio::test]
    async fn backoff_delays_cron() {
        let scheduler = Scheduler::new(SchedulerConfig::default());
        let events = Arc::new(AtomicUsize::new(0));
        let events_clone = events.clone();
        let schedule =
            CronSchedule::from_str("*/1 * * * * *", chrono_tz::UTC).expect("cron schedule");

        let handle = scheduler.schedule(
            ScheduleSpec::Cron {
                label: "backoff",
                schedule: Box::new(schedule),
            },
            move |_ctx| {
                let events = events_clone.clone();
                Box::pin(async move {
                    let count = events.fetch_add(1, Ordering::SeqCst);
                    if count == 0 {
                        JobResult::Backoff(Duration::from_millis(500))
                    } else {
                        JobResult::Stop
                    }
                })
            },
        );

        time::sleep(Duration::from_secs(2)).await;
        handle.cancel_and_abort();
        scheduler.shutdown_and_join().await;
        assert!(events.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn cancellation_race_is_safe() {
        let scheduler = Scheduler::new(SchedulerConfig::default());
        let handle = scheduler.schedule(ScheduleSpec::Immediate { label: "race" }, move |_ctx| {
            Box::pin(async move { JobResult::Stop })
        });
        handle.cancel();
        scheduler.shutdown_and_join().await;
    }

    #[tokio::test]
    async fn reset_global_creates_new_instance() {
        Scheduler::reset_global_for_tests().await;
        let scheduler = Scheduler::global();
        scheduler.shutdown_and_join().await;
    }

    #[tokio::test]
    async fn dst_boundary_next_after() {
        let tz: Tz = chrono_tz::US::Eastern;
        let schedule = CronSchedule::from_str("0 0 2 * * *", tz).unwrap();
        let before_dst = tz
            .with_ymd_and_hms(2023, 3, 12, 0, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let next = schedule.next_after(before_dst).unwrap();
        assert!(next.timestamp() > before_dst.timestamp());
    }

    #[tokio::test]
    async fn job_result_conformance() {
        let scheduler = Scheduler::new(SchedulerConfig::default());
        let runs = Arc::new(AtomicUsize::new(0));
        let runs_clone = runs.clone();
        let handle = scheduler.schedule(
            ScheduleSpec::Cron {
                label: "conformance",
                schedule: Box::new(
                    CronSchedule::from_str("*/1 * * * * *", chrono_tz::UTC).unwrap(),
                ),
            },
            move |_ctx| {
                let runs = runs_clone.clone();
                Box::pin(async move {
                    let count = runs.fetch_add(1, Ordering::SeqCst);
                    if count == 0 {
                        JobResult::Continue
                    } else {
                        JobResult::Stop
                    }
                })
            },
        );

        time::sleep(Duration::from_secs(3)).await;
        handle.cancel_and_abort();
        scheduler.shutdown_and_join().await;
        assert!(runs.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn high_volume_stress() {
        let scheduler = Scheduler::new(SchedulerConfig::default().with_limits(Some(32), Some(256)));
        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..100 {
            let counter_clone = counter.clone();
            let handle =
                scheduler.schedule(ScheduleSpec::Immediate { label: "stress" }, move |_ctx| {
                    let counter = counter_clone.clone();
                    Box::pin(async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        JobResult::Stop
                    })
                });
            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.join().await;
        }

        assert_eq!(counter.load(Ordering::SeqCst), 100);
        scheduler.shutdown_and_join().await;
    }

    #[tokio::test]
    async fn cancellation_point_helper_respects_token() {
        let token = CancellationToken::new();
        assert!(ensure_not_cancelled(&token).is_ok());
        assert!(cancellation_point(&token).await.is_ok());
        token.cancel();
        assert_eq!(cancellation_point(&token).await, Err(Cancelled));
        assert_eq!(ensure_not_cancelled(&token), Err(Cancelled));
    }

    #[tokio::test]
    async fn exponential_backoff_moves_forward() {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(10), 2.0, Duration::from_millis(40));
        let first = backoff.on_error(&"err");
        assert_eq!(first, JobResult::Backoff(Duration::from_millis(10)));
        let second = backoff.on_error(&"err");
        assert_eq!(second, JobResult::Backoff(Duration::from_millis(20)));
        backoff.reset(Duration::from_millis(5));
        let third = backoff.on_error(&"err");
        assert_eq!(third, JobResult::Backoff(Duration::from_millis(5)));
    }

    #[tokio::test]
    async fn reporting_channel_receives_outcome() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let scheduler = Scheduler::new(SchedulerConfig::default().with_reporter(tx));
        let handle = scheduler.schedule(ScheduleSpec::Immediate { label: "report" }, |_ctx| {
            Box::pin(async { JobResult::Stop })
        });
        let _ = handle.join().await;
        scheduler.shutdown_and_join().await;
        let outcome = rx.recv().await.expect("report");
        assert_eq!(outcome.label, "report");
        assert!(matches!(
            outcome.outcome,
            TaskOutcome::Completed(JobResult::Stop)
        ));
        assert!(outcome.task_id != TaskId(0));
        assert!(outcome.scheduled_for.is_none());
    }

    #[tokio::test]
    async fn cron_schedule_uses_config_timezone() {
        let config = SchedulerConfig {
            timezone: chrono_tz::Europe::Paris,
            ..SchedulerConfig::default()
        };
        let scheduler = Scheduler::new(config);
        let schedule = scheduler
            .cron_schedule("*/1 * * * * *")
            .expect("cron schedule");
        assert_eq!(schedule.timezone(), chrono_tz::Europe::Paris);
    }

    #[tokio::test]
    async fn scheduler_handle_controls_shutdown() {
        let scheduler = Scheduler::new(SchedulerConfig::default());
        let handle = scheduler.handle();
        assert!(!scheduler.is_shutdown());
        handle.shutdown();
        assert!(scheduler.is_shutdown());
        handle.shutdown_and_join().await;
    }

    #[tokio::test]
    async fn hooks_receive_lifecycle_events() {
        struct CountingHooks {
            scheduled: Arc<AtomicUsize>,
            started: Arc<AtomicUsize>,
            completed: Arc<AtomicUsize>,
        }

        impl SchedulerHooks for CountingHooks {
            fn on_scheduled(&self, _task_id: TaskId, _label: &'static str) {
                self.scheduled.fetch_add(1, Ordering::SeqCst);
            }

            fn on_started(&self, _task_id: TaskId, _label: &'static str) {
                self.started.fetch_add(1, Ordering::SeqCst);
            }

            fn on_completed(&self, _task_id: TaskId, _label: &'static str, _outcome: &TaskOutcome) {
                self.completed.fetch_add(1, Ordering::SeqCst);
            }
        }

        let hooks = Arc::new(CountingHooks {
            scheduled: Arc::new(AtomicUsize::new(0)),
            started: Arc::new(AtomicUsize::new(0)),
            completed: Arc::new(AtomicUsize::new(0)),
        });

        let scheduler = Scheduler::new(SchedulerConfig::default().with_hooks(hooks.clone()));
        let handle = scheduler.schedule(ScheduleSpec::Immediate { label: "hooks" }, |_ctx| {
            Box::pin(async { JobResult::Stop })
        });

        let _ = handle.join().await;
        scheduler.shutdown_and_join().await;

        assert_eq!(hooks.scheduled.load(Ordering::SeqCst), 1);
        assert_eq!(hooks.started.load(Ordering::SeqCst), 1);
        assert_eq!(hooks.completed.load(Ordering::SeqCst), 1);
    }
}
