#![cfg(feature = "scheduler-bench")]

use chrono::Utc;
use chrono_tz::UTC;
use criterion::{criterion_group, criterion_main, Criterion};
use nanocloud::nanocloud::scheduler::{
    CronSchedule, JobResult, ScheduleSpec, Scheduler, SchedulerConfig,
};

fn bench_schedule_immediate(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    c.bench_function("scheduler_immediate", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let scheduler = Scheduler::new(SchedulerConfig::default());
                let handle = scheduler
                    .schedule(ScheduleSpec::Immediate { label: "bench" }, |_ctx| {
                        Box::pin(async { JobResult::Stop })
                    });
                let _ = handle.join().await;
                scheduler.shutdown_and_join().await;
            })
        })
    });
}

fn bench_cron_iteration(c: &mut Criterion) {
    let schedule = CronSchedule::from_str("*/1 * * * * *", UTC).expect("cron");
    c.bench_function("cron_next_after", |b| {
        b.iter(|| {
            let mut cursor = Utc::now();
            for _ in 0..100 {
                cursor = schedule.next_after(cursor).expect("next");
            }
            cursor
        })
    });
}

criterion_group!(scheduler, bench_schedule_immediate, bench_cron_iteration);
criterion_main!(scheduler);
