use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use futures_util::future::FutureExt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time;
use tokio_util::sync::CancellationToken;

pub type JobFuture = Pin<Box<dyn Future<Output = JobResult> + Send>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobResult {
    Stop,
    Continue,
}

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

#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<SchedulerInner>,
}

struct SchedulerInner {
    next_id: AtomicU64,
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
    cancellation: CancellationToken,
    join: Arc<Mutex<Option<JoinHandle<()>>>>,
}

#[derive(Clone)]
pub struct CronSchedule {
    expression: Schedule,
    timezone: Tz,
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

impl Scheduler {
    pub fn global() -> Scheduler {
        static INSTANCE: OnceLock<Scheduler> = OnceLock::new();
        INSTANCE
            .get_or_init(|| Scheduler {
                inner: Arc::new(SchedulerInner {
                    next_id: AtomicU64::new(1),
                }),
            })
            .clone()
    }

    pub fn schedule<F>(&self, spec: ScheduleSpec, job: F) -> ScheduledTaskHandle
    where
        F: Fn(ScheduleContext) -> JobFuture + Send + Sync + 'static,
    {
        let job = Arc::new(job) as Arc<JobFn>;
        let task_id = TaskId(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
        let cancellation = CancellationToken::new();
        let join_handle = self.spawn_for_spec(task_id, &cancellation, spec, job);
        ScheduledTaskHandle {
            cancellation,
            join: Arc::new(Mutex::new(Some(join_handle))),
        }
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
                    scheduler.run_job(job, ctx).await;
                })
            }
            ScheduleSpec::After { label, delay } => self.spawn_after(
                task_id,
                cancellation,
                label,
                delay,
                compute_scheduled_time(delay),
                job,
            ),
            ScheduleSpec::Cron { label, schedule } => {
                self.spawn_cron(task_id, cancellation, label, *schedule, job)
            }
        }
    }

    async fn run_job(&self, job: Arc<JobFn>, ctx: ScheduleContext) -> JobResult {
        if ctx.cancellation.is_cancelled() {
            return JobResult::Stop;
        }

        AssertUnwindSafe((job.clone())(ctx.clone()))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| {
                log::error!(
                    "Task {:?} ({}) panicked; stopping task",
                    ctx.task_id,
                    ctx.label
                );
                JobResult::Stop
            })
    }

    fn spawn_after(
        &self,
        task_id: TaskId,
        cancellation: CancellationToken,
        label: &'static str,
        delay: Duration,
        scheduled_for: Option<DateTime<Utc>>,
        job: Arc<JobFn>,
    ) -> JoinHandle<()> {
        let scheduler = self.clone();
        tokio::spawn(async move {
            let sleeper = time::sleep(delay);
            tokio::pin!(sleeper);
            tokio::select! {
                _ = cancellation.cancelled() => return,
                _ = sleeper.as_mut() => {}
            }

            if cancellation.is_cancelled() {
                return;
            }

            let ctx = ScheduleContext::new(task_id, label, cancellation.clone(), scheduled_for);
            let _ = scheduler.run_job(job, ctx).await;
        })
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
            while let Some(run_at) = next {
                let delay = duration_until(run_at);
                let sleeper = time::sleep(delay);
                tokio::pin!(sleeper);
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    _ = sleeper.as_mut() => {}
                }

                if cancellation.is_cancelled() {
                    break;
                }

                let ctx = ScheduleContext::new(task_id, label, cancellation.clone(), Some(run_at));
                let result = scheduler.run_job(job.clone(), ctx).await;
                if matches!(result, JobResult::Stop) {
                    break;
                }

                let after = run_at + ChronoDuration::seconds(1);
                next = schedule.next_after(after);
            }
        })
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

    #[cfg(test)]
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
    use std::time::Duration;
    use tokio::time;

    #[tokio::test]
    async fn immediate_task_executes() {
        let scheduler = Scheduler::global();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        scheduler.schedule(ScheduleSpec::Immediate { label: "immediate" }, move |ctx| {
            let counter = counter_clone.clone();
            Box::pin(async move {
                assert_eq!(ctx.label(), "immediate");
                // Ensure task_id accessor is usable.
                let _ = ctx.task_id();
                counter.fetch_add(1, Ordering::SeqCst);
                JobResult::Stop
            })
        });

        time::sleep(Duration::from_millis(50)).await;

        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn delayed_task_executes() {
        let scheduler = Scheduler::global();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        scheduler.schedule(
            ScheduleSpec::After {
                label: "delayed",
                delay: Duration::from_millis(50),
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
        );

        time::sleep(Duration::from_millis(80)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cron_task_repeats_until_stopped() {
        let scheduler = Scheduler::global();
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
    async fn join_waits_for_completion() {
        let scheduler = Scheduler::global();
        let flag = Arc::new(AtomicUsize::new(0));
        let flag_clone = flag.clone();

        let handle = scheduler.schedule(ScheduleSpec::Immediate { label: "join" }, move |ctx| {
            let flag = flag_clone.clone();
            Box::pin(async move {
                assert_eq!(ctx.label(), "join");
                flag.store(1, Ordering::SeqCst);
                JobResult::Stop
            })
        });

        handle.join().await.expect("join should succeed");
        assert_eq!(flag.load(Ordering::SeqCst), 1);
    }
}
