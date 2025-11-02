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

use std::error::Error;
use std::future::Future;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use prometheus::core::Collector;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
    TextEncoder,
};

const DEFAULT_NAMESPACE: &str = "default";

static REGISTRY: OnceLock<Registry> = OnceLock::new();
static CONTAINER_OPERATION_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static CONTAINER_OPERATION_DURATION: OnceLock<HistogramVec> = OnceLock::new();
static CONTAINER_READY: OnceLock<IntGaugeVec> = OnceLock::new();
static EXEC_HANDSHAKE_FAILURES_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static STATEFULSET_READY: OnceLock<IntGaugeVec> = OnceLock::new();
static STATEFULSET_CURRENT: OnceLock<IntGaugeVec> = OnceLock::new();
static STATEFULSET_PROGRESSING: OnceLock<IntGaugeVec> = OnceLock::new();
static AUTH_BOOTSTRAP_ATTEMPTS_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static EVENTS_EMITTED_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static EVENTS_CONSUMED_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static EVENTS_STREAM_ERRORS_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static CONTROLLER_RECONCILES_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static BINDING_EXECUTIONS_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static IMAGE_PULLS_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static RESTARTS_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static BUNDLE_STATE_GAUGE: OnceLock<IntGaugeVec> = OnceLock::new();
static POD_COUNTS_GAUGE: OnceLock<IntGaugeVec> = OnceLock::new();
static SNAPSHOT_OPERATION_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static SNAPSHOT_OPERATION_DURATION: OnceLock<HistogramVec> = OnceLock::new();
static PROXY_OPERATION_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static PROXY_OPERATION_DURATION: OnceLock<HistogramVec> = OnceLock::new();
static BACKUP_STREAM_BYTES_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static BACKUP_STREAM_DURATION: OnceLock<HistogramVec> = OnceLock::new();
static KEYSPACE_BLOCKING_QUEUE_DEPTH: OnceLock<IntGauge> = OnceLock::new();
static KEYSPACE_BLOCKING_ACTIVE: OnceLock<IntGauge> = OnceLock::new();
static KEYSPACE_BLOCKING_WAIT: OnceLock<HistogramVec> = OnceLock::new();
static KEYSPACE_BLOCKING_RUN: OnceLock<HistogramVec> = OnceLock::new();
static BACKUP_CAPTURE_BYTES_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static BACKUP_CAPTURE_DURATION: OnceLock<HistogramVec> = OnceLock::new();
static BACKUP_RESTORE_BYTES_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static BACKUP_RESTORE_DURATION: OnceLock<HistogramVec> = OnceLock::new();

fn registry() -> &'static Registry {
    REGISTRY.get_or_init(|| {
        Registry::new_custom(Some("nanocloud".to_string()), None)
            .expect("failed to initialise nanocloud metrics registry")
    })
}

fn register_collector<C>(collector: C) -> C
where
    C: Clone + Collector + Send + Sync + 'static,
{
    registry()
        .register(Box::new(collector.clone()))
        .expect("failed to register nanocloud metric collector");
    collector
}

fn controller_reconciles_total() -> &'static IntCounterVec {
    CONTROLLER_RECONCILES_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "controller_reconciles_total",
            "Controller reconciliation attempts grouped by result",
        );
        let counter = IntCounterVec::new(opts, &["controller", "result"])
            .expect("failed to build controller reconcile counter");
        register_collector(counter)
    })
}

fn binding_executions_total() -> &'static IntCounterVec {
    BINDING_EXECUTIONS_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "binding_executions_total",
            "Binding envelope executions grouped by service and result",
        );
        let counter = IntCounterVec::new(opts, &["service", "result"])
            .expect("failed to build binding execution counter");
        register_collector(counter)
    })
}

fn image_pulls_total() -> &'static IntCounterVec {
    IMAGE_PULLS_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "image_pulls_total",
            "Image pulls grouped by cache hit status",
        );
        let counter =
            IntCounterVec::new(opts, &["cache_hit"]).expect("failed to build image pulls counter");
        register_collector(counter)
    })
}

fn restarts_total() -> &'static IntCounterVec {
    RESTARTS_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "restarts_total",
            "Kubelet-triggered restarts grouped by namespace, service, and reason",
        );
        let counter = IntCounterVec::new(opts, &["namespace", "service", "reason"])
            .expect("failed to build restarts counter");
        register_collector(counter)
    })
}

fn bundle_state_gauge() -> &'static IntGaugeVec {
    BUNDLE_STATE_GAUGE.get_or_init(|| {
        let opts = Opts::new(
            "bundles",
            "Number of bundles grouped by high-level readiness state",
        );
        let gauge = IntGaugeVec::new(opts, &["state"]).expect("failed to build bundle state gauge");
        register_collector(gauge)
    })
}

fn pod_counts_gauge() -> &'static IntGaugeVec {
    POD_COUNTS_GAUGE.get_or_init(|| {
        let opts = Opts::new(
            "pods",
            "Number of Nanocloud-managed pods grouped by namespace",
        );
        let gauge =
            IntGaugeVec::new(opts, &["namespace"]).expect("failed to build pod count gauge");
        register_collector(gauge)
    })
}

fn container_operation_total() -> &'static IntCounterVec {
    CONTAINER_OPERATION_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "operations_total",
            "Container lifecycle operations aggregated by result",
        )
        .subsystem("container");
        let counter = IntCounterVec::new(opts, &["operation", "result", "namespace", "workload"])
            .expect("failed to build container operations counter");
        register_collector(counter)
    })
}

fn container_operation_duration() -> &'static HistogramVec {
    CONTAINER_OPERATION_DURATION.get_or_init(|| {
        let opts = HistogramOpts::new(
            "operation_duration_seconds",
            "Latency distribution for container lifecycle operations",
        )
        .subsystem("container")
        .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]);
        let histogram = HistogramVec::new(opts, &["operation", "result", "namespace", "workload"])
            .expect("failed to build container operation histogram");
        register_collector(histogram)
    })
}

fn container_ready() -> &'static IntGaugeVec {
    CONTAINER_READY.get_or_init(|| {
        let opts = Opts::new(
            "status_ready",
            "Container readiness status aligned with kube_pod_container_status_ready",
        )
        .subsystem("container");
        let gauge = IntGaugeVec::new(opts, &["namespace", "workload"])
            .expect("failed to build container readiness gauge");
        register_collector(gauge)
    })
}

fn exec_handshake_failures_total() -> &'static IntCounterVec {
    EXEC_HANDSHAKE_FAILURES_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "handshake_failures_total",
            "Exec handshake failures grouped by transport and reason",
        )
        .subsystem("exec");
        let counter = IntCounterVec::new(opts, &["transport", "reason"])
            .expect("failed to build exec handshake failures counter");
        register_collector(counter)
    })
}

fn statefulset_ready() -> &'static IntGaugeVec {
    STATEFULSET_READY.get_or_init(|| {
        let opts = Opts::new(
            "ready_replicas",
            "StatefulSet ready replica count aligned with kubernetes_statefulset_status_ready_replicas",
        )
        .subsystem("controller_statefulset");
        let gauge =
            IntGaugeVec::new(opts, &["namespace", "statefulset"])
                .expect("failed to build statefulset ready gauge");
        register_collector(gauge)
    })
}

fn statefulset_current() -> &'static IntGaugeVec {
    STATEFULSET_CURRENT.get_or_init(|| {
        let opts = Opts::new(
            "current_replicas",
            "StatefulSet current replica count aligned with kubernetes_statefulset_status_current_replicas",
        )
        .subsystem("controller_statefulset");
        let gauge =
            IntGaugeVec::new(opts, &["namespace", "statefulset"])
                .expect("failed to build statefulset current gauge");
        register_collector(gauge)
    })
}

fn statefulset_progressing() -> &'static IntGaugeVec {
    STATEFULSET_PROGRESSING.get_or_init(|| {
        let opts = Opts::new(
            "progressing",
            "Boolean indicator (0/1) that the StatefulSet has pending reconciliation work",
        )
        .subsystem("controller_statefulset");
        let gauge = IntGaugeVec::new(opts, &["namespace", "statefulset"])
            .expect("failed to build statefulset progressing gauge");
        register_collector(gauge)
    })
}

fn auth_bootstrap_attempts_total() -> &'static IntCounterVec {
    AUTH_BOOTSTRAP_ATTEMPTS_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "bootstrap_attempts_total",
            "Bootstrap token authentication fallback attempts grouped by outcome",
        )
        .subsystem("auth");
        let counter = IntCounterVec::new(opts, &["outcome"])
            .expect("failed to build auth bootstrap attempts counter");
        register_collector(counter)
    })
}

fn events_emitted_total() -> &'static IntCounterVec {
    EVENTS_EMITTED_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "emitted_total",
            "Events published grouped by topic and status",
        )
        .subsystem("events");
        let counter = IntCounterVec::new(opts, &["topic", "status"])
            .expect("failed to build events emitted counter");
        register_collector(counter)
    })
}

fn events_consumed_total() -> &'static IntCounterVec {
    EVENTS_CONSUMED_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "consumed_total",
            "Events consumed by subscribers grouped by topic and status",
        )
        .subsystem("events");
        let counter = IntCounterVec::new(opts, &["topic", "status"])
            .expect("failed to build events consumed counter");
        register_collector(counter)
    })
}

fn events_stream_errors_total() -> &'static IntCounterVec {
    EVENTS_STREAM_ERRORS_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "stream_errors_total",
            "Event stream errors grouped by topic and cause",
        )
        .subsystem("events");
        let counter = IntCounterVec::new(opts, &["topic", "cause"])
            .expect("failed to build events stream errors counter");
        register_collector(counter)
    })
}

fn snapshot_operation_total() -> &'static IntCounterVec {
    SNAPSHOT_OPERATION_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "operations_total",
            "Volume snapshot controller operations grouped by outcome",
        )
        .subsystem("controller_snapshot");
        let counter = IntCounterVec::new(opts, &["operation", "result", "namespace", "snapshot"])
            .expect("failed to build snapshot operations counter");
        register_collector(counter)
    })
}

fn snapshot_operation_duration() -> &'static HistogramVec {
    SNAPSHOT_OPERATION_DURATION.get_or_init(|| {
        let opts = HistogramOpts::new(
            "operation_duration_seconds",
            "Volume snapshot controller operation latency distribution",
        )
        .subsystem("controller_snapshot")
        .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]);
        let histogram = HistogramVec::new(opts, &["operation", "result", "namespace", "snapshot"])
            .expect("failed to build snapshot operation histogram");
        register_collector(histogram)
    })
}

fn proxy_operation_total() -> &'static IntCounterVec {
    PROXY_OPERATION_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "operations_total",
            "Network proxy programming operations grouped by outcome",
        )
        .subsystem("proxy");
        let counter = IntCounterVec::new(opts, &["operation", "result", "namespace", "service"])
            .expect("failed to build proxy operations counter");
        register_collector(counter)
    })
}

fn proxy_operation_duration() -> &'static HistogramVec {
    PROXY_OPERATION_DURATION.get_or_init(|| {
        let opts = HistogramOpts::new(
            "operation_duration_seconds",
            "Network proxy programming operation latency distribution",
        )
        .subsystem("proxy")
        .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0]);
        let histogram = HistogramVec::new(opts, &["operation", "result", "namespace", "service"])
            .expect("failed to build proxy operation histogram");
        register_collector(histogram)
    })
}

fn backup_stream_bytes_total() -> &'static IntCounterVec {
    BACKUP_STREAM_BYTES_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "bytes_total",
            "Total bytes streamed for backups grouped by owner and workload",
        )
        .subsystem("backup_stream");
        let counter = IntCounterVec::new(opts, &["owner", "namespace", "service"])
            .expect("failed to build backup stream bytes counter");
        register_collector(counter)
    })
}

fn backup_stream_duration() -> &'static HistogramVec {
    BACKUP_STREAM_DURATION.get_or_init(|| {
        let opts = HistogramOpts::new(
            "duration_seconds",
            "Duration of backup streaming requests grouped by owner and workload",
        )
        .subsystem("backup_stream")
        .buckets(vec![0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]);
        let histogram = HistogramVec::new(opts, &["owner", "namespace", "service"])
            .expect("failed to build backup stream duration histogram");
        register_collector(histogram)
    })
}

fn keyspace_blocking_queue_depth() -> &'static IntGauge {
    KEYSPACE_BLOCKING_QUEUE_DEPTH.get_or_init(|| {
        let gauge = IntGauge::with_opts(
            Opts::new(
                "queue_depth",
                "Number of pending keyspace blocking tasks awaiting execution",
            )
            .subsystem("keyspace_blocking"),
        )
        .expect("failed to build keyspace blocking queue depth gauge");
        register_collector(gauge)
    })
}

fn keyspace_blocking_active() -> &'static IntGauge {
    KEYSPACE_BLOCKING_ACTIVE.get_or_init(|| {
        let gauge = IntGauge::with_opts(
            Opts::new(
                "active_tasks",
                "Number of keyspace blocking tasks currently executing",
            )
            .subsystem("keyspace_blocking"),
        )
        .expect("failed to build keyspace blocking active gauge");
        register_collector(gauge)
    })
}

fn keyspace_blocking_wait() -> &'static HistogramVec {
    KEYSPACE_BLOCKING_WAIT.get_or_init(|| {
        let opts = HistogramOpts::new(
            "wait_duration_seconds",
            "Queue wait time for keyspace blocking tasks grouped by operation",
        )
        .subsystem("keyspace_blocking")
        .buckets(vec![
            0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
        ]);
        let histogram = HistogramVec::new(opts, &["operation"])
            .expect("failed to build keyspace wait histogram");
        register_collector(histogram)
    })
}

fn keyspace_blocking_run() -> &'static HistogramVec {
    KEYSPACE_BLOCKING_RUN.get_or_init(|| {
        let opts = HistogramOpts::new(
            "run_duration_seconds",
            "Execution time for keyspace blocking tasks grouped by operation",
        )
        .subsystem("keyspace_blocking")
        .buckets(vec![
            0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
        ]);
        let histogram = HistogramVec::new(opts, &["operation"])
            .expect("failed to build keyspace run histogram");
        register_collector(histogram)
    })
}

fn backup_capture_bytes_total() -> &'static IntCounterVec {
    BACKUP_CAPTURE_BYTES_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "capture_bytes_total",
            "Total bytes captured during service backups grouped by namespace and service",
        )
        .subsystem("backup");
        let counter = IntCounterVec::new(opts, &["namespace", "service"])
            .expect("failed to build backup capture bytes counter");
        register_collector(counter)
    })
}

fn backup_capture_duration() -> &'static HistogramVec {
    BACKUP_CAPTURE_DURATION.get_or_init(|| {
        let opts = HistogramOpts::new(
            "capture_duration_seconds",
            "Duration of service backup capture operations",
        )
        .subsystem("backup")
        .buckets(vec![0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0]);
        let histogram = HistogramVec::new(opts, &["namespace", "service"])
            .expect("failed to build backup capture duration histogram");
        register_collector(histogram)
    })
}

fn backup_restore_bytes_total() -> &'static IntCounterVec {
    BACKUP_RESTORE_BYTES_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "restore_bytes_total",
            "Total bytes restored during service snapshot recovery",
        )
        .subsystem("backup");
        let counter = IntCounterVec::new(opts, &["namespace", "service"])
            .expect("failed to build backup restore bytes counter");
        register_collector(counter)
    })
}

fn backup_restore_duration() -> &'static HistogramVec {
    BACKUP_RESTORE_DURATION.get_or_init(|| {
        let opts = HistogramOpts::new(
            "restore_duration_seconds",
            "Duration of service snapshot restore operations",
        )
        .subsystem("backup")
        .buckets(vec![0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0]);
        let histogram = HistogramVec::new(opts, &["namespace", "service"])
            .expect("failed to build backup restore duration histogram");
        register_collector(histogram)
    })
}

#[derive(Clone, Copy, Debug)]
pub enum ContainerOperation {
    Install,
    Start,
    Stop,
    Uninstall,
}

impl ContainerOperation {
    fn as_label(self) -> &'static str {
        match self {
            ContainerOperation::Install => "install",
            ContainerOperation::Start => "start",
            ContainerOperation::Stop => "stop",
            ContainerOperation::Uninstall => "uninstall",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum SnapshotOperation {
    Reconcile,
}

impl SnapshotOperation {
    fn as_label(self) -> &'static str {
        match self {
            SnapshotOperation::Reconcile => "reconcile",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ProxyOperation {
    Program,
    Remove,
}

impl ProxyOperation {
    fn as_label(self) -> &'static str {
        match self {
            ProxyOperation::Program => "program",
            ProxyOperation::Remove => "remove",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum OperationOutcome {
    Success,
    Error,
}

impl OperationOutcome {
    fn as_label(self) -> &'static str {
        match self {
            OperationOutcome::Success => "success",
            OperationOutcome::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum BootstrapAuthOutcome {
    Success,
    NotFound,
    Invalid,
    Error,
}

impl BootstrapAuthOutcome {
    fn as_label(self) -> &'static str {
        match self {
            BootstrapAuthOutcome::Success => "success",
            BootstrapAuthOutcome::NotFound => "not_found",
            BootstrapAuthOutcome::Invalid => "invalid",
            BootstrapAuthOutcome::Error => "error",
        }
    }
}

fn namespace_label(namespace: Option<&str>) -> &str {
    match namespace {
        Some(value) if !value.is_empty() => value,
        _ => DEFAULT_NAMESPACE,
    }
}

fn owner_label(owner: &str) -> &str {
    if owner.is_empty() {
        "unknown"
    } else {
        owner
    }
}

fn record_operation(
    namespace: Option<&str>,
    workload: &str,
    operation: ContainerOperation,
    outcome: OperationOutcome,
    duration: Duration,
) {
    let labels = [
        operation.as_label(),
        outcome.as_label(),
        namespace_label(namespace),
        workload,
    ];

    container_operation_total().with_label_values(&labels).inc();
    container_operation_duration()
        .with_label_values(&labels)
        .observe(duration.as_secs_f64());
}

/// Wraps a future representing a container lifecycle operation and records
/// Kubernetes-style Prometheus metrics for the outcome and latency.
pub async fn observe_container_operation<F, T, E>(
    namespace: Option<&str>,
    workload: &str,
    operation: ContainerOperation,
    future: F,
) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    let start = Instant::now();
    match future.await {
        Ok(value) => {
            record_operation(
                namespace,
                workload,
                operation,
                OperationOutcome::Success,
                start.elapsed(),
            );
            Ok(value)
        }
        Err(err) => {
            record_operation(
                namespace,
                workload,
                operation,
                OperationOutcome::Error,
                start.elapsed(),
            );
            Err(err)
        }
    }
}

fn record_proxy_operation(
    namespace: Option<&str>,
    service: &str,
    operation: ProxyOperation,
    outcome: OperationOutcome,
    duration: Duration,
) {
    let labels = [
        operation.as_label(),
        outcome.as_label(),
        namespace_label(namespace),
        service,
    ];

    proxy_operation_total().with_label_values(&labels).inc();
    proxy_operation_duration()
        .with_label_values(&labels)
        .observe(duration.as_secs_f64());
}

/// Records metrics for a completed backup streaming transfer.
pub fn record_backup_stream(
    owner: &str,
    namespace: Option<&str>,
    service: &str,
    bytes: u64,
    duration: Duration,
) {
    let labels = [owner_label(owner), namespace_label(namespace), service];
    backup_stream_bytes_total()
        .with_label_values(&labels)
        .inc_by(bytes);
    backup_stream_duration()
        .with_label_values(&labels)
        .observe(duration.as_secs_f64());
}

/// Records metrics for service backup capture (creation) operations.
pub fn record_backup_capture(
    namespace: Option<&str>,
    service: &str,
    _volumes: usize,
    bytes: u64,
    duration: Duration,
) {
    let labels = [namespace_label(namespace), service];
    backup_capture_bytes_total()
        .with_label_values(&labels)
        .inc_by(bytes);
    backup_capture_duration()
        .with_label_values(&labels)
        .observe(duration.as_secs_f64());
}

/// Records metrics for service snapshot restore operations.
pub fn record_backup_restore(
    namespace: Option<&str>,
    service: &str,
    _volumes: usize,
    bytes: u64,
    duration: Duration,
) {
    let labels = [namespace_label(namespace), service];
    backup_restore_bytes_total()
        .with_label_values(&labels)
        .inc_by(bytes);
    backup_restore_duration()
        .with_label_values(&labels)
        .observe(duration.as_secs_f64());
}

/// Updates gauges tracking the keyspace blocking executor queue depth.
pub fn set_keyspace_blocking_queue(depth: usize) {
    keyspace_blocking_queue_depth().set(depth as i64);
}

/// Updates gauges tracking active keyspace blocking tasks.
pub fn set_keyspace_blocking_active(active: usize) {
    keyspace_blocking_active().set(active as i64);
}

/// Records queue wait and execution durations for keyspace blocking tasks.
pub fn observe_keyspace_blocking(operation: &str, queue_wait: Duration, run: Duration) {
    keyspace_blocking_wait()
        .with_label_values(&[operation])
        .observe(queue_wait.as_secs_f64());
    keyspace_blocking_run()
        .with_label_values(&[operation])
        .observe(run.as_secs_f64());
}

/// Records metrics for synchronous proxy operations (program/remove).
pub fn observe_proxy_operation<F, T, E>(
    namespace: Option<&str>,
    service: &str,
    operation: ProxyOperation,
    action: F,
) -> Result<T, E>
where
    F: FnOnce() -> Result<T, E>,
{
    let start = Instant::now();
    match action() {
        Ok(value) => {
            record_proxy_operation(
                namespace,
                service,
                operation,
                OperationOutcome::Success,
                start.elapsed(),
            );
            Ok(value)
        }
        Err(err) => {
            record_proxy_operation(
                namespace,
                service,
                operation,
                OperationOutcome::Error,
                start.elapsed(),
            );
            Err(err)
        }
    }
}

fn record_snapshot_operation(
    namespace: Option<&str>,
    snapshot: &str,
    operation: SnapshotOperation,
    outcome: OperationOutcome,
    duration: Duration,
) {
    let labels = [
        operation.as_label(),
        outcome.as_label(),
        namespace_label(namespace),
        snapshot,
    ];

    snapshot_operation_total().with_label_values(&labels).inc();
    snapshot_operation_duration()
        .with_label_values(&labels)
        .observe(duration.as_secs_f64());
}

/// Wraps a future representing a snapshot controller operation and records metrics.
pub async fn observe_snapshot_operation<F, T, E>(
    namespace: Option<&str>,
    snapshot: &str,
    operation: SnapshotOperation,
    future: F,
) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    let start = Instant::now();
    match future.await {
        Ok(value) => {
            record_snapshot_operation(
                namespace,
                snapshot,
                operation,
                OperationOutcome::Success,
                start.elapsed(),
            );
            Ok(value)
        }
        Err(err) => {
            record_snapshot_operation(
                namespace,
                snapshot,
                operation,
                OperationOutcome::Error,
                start.elapsed(),
            );
            Err(err)
        }
    }
}

pub fn record_bootstrap_token_attempt(outcome: BootstrapAuthOutcome) {
    auth_bootstrap_attempts_total()
        .with_label_values(&[outcome.as_label()])
        .inc();
}

pub fn record_event_emit(topic: &str, status: &str) {
    events_emitted_total()
        .with_label_values(&[topic, status])
        .inc();
}

pub fn record_event_consume(topic: &str, status: &str) {
    events_consumed_total()
        .with_label_values(&[topic, status])
        .inc();
}

pub fn record_event_stream_error(topic: &str, cause: &str) {
    events_stream_errors_total()
        .with_label_values(&[topic, cause])
        .inc();
}

pub fn record_controller_reconcile(controller: &str, result: ControllerReconcileResult) {
    controller_reconciles_total()
        .with_label_values(&[controller, result.as_label()])
        .inc();
}

pub fn record_binding_execution(service: &str, result: BindingExecutionResult) {
    binding_executions_total()
        .with_label_values(&[service, result.as_label()])
        .inc();
}

pub fn record_image_pull(cache_hit: bool) {
    let label = if cache_hit { "true" } else { "false" };
    image_pulls_total().with_label_values(&[label]).inc();
}

pub fn record_restart(namespace: Option<&str>, service: &str, reason: &str) {
    let ns = namespace.unwrap_or(DEFAULT_NAMESPACE);
    restarts_total()
        .with_label_values(&[ns, service, reason])
        .inc();
}

pub fn set_bundle_gauges(ready: i64, degraded: i64) {
    let gauge = bundle_state_gauge();
    gauge.with_label_values(&["ready"]).set(ready);
    gauge.with_label_values(&["degraded"]).set(degraded);
}

pub fn set_pod_gauges(counts: &[(String, i64)]) {
    let gauge = pod_counts_gauge();
    gauge.reset();
    for (namespace, count) in counts {
        gauge.with_label_values(&[namespace.as_str()]).set(*count);
    }
}

/// Publishes gauges that describe the latest StatefulSet reconciliation status.
pub fn record_statefulset_status(
    namespace: Option<&str>,
    name: &str,
    ready: i32,
    current: i32,
    progressing: bool,
) {
    let labels = [namespace_label(namespace), name];
    statefulset_ready()
        .with_label_values(&labels)
        .set(i64::from(ready));
    statefulset_current()
        .with_label_values(&labels)
        .set(i64::from(current));
    statefulset_progressing()
        .with_label_values(&labels)
        .set(if progressing { 1 } else { 0 });
}

/// Marks a container as ready (1) or not ready (0) in the Prometheus gauge
/// mirroring `kube_pod_container_status_ready`.
pub fn set_container_ready(namespace: Option<&str>, workload: &str, ready: bool) {
    let gauge = container_ready().with_label_values(&[namespace_label(namespace), workload]);
    gauge.set(if ready { 1 } else { 0 });
}

/// Removes per-container gauges once a workload is fully deprovisioned to
/// prevent stale time series.
pub fn clear_container(namespace: Option<&str>, workload: &str) {
    let labels = [namespace_label(namespace), workload];
    if container_ready().remove_label_values(&labels).is_err() {
        container_ready().with_label_values(&labels).set(0);
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ExecTransport {
    WebSocket,
}

impl ExecTransport {
    fn as_label(self) -> &'static str {
        match self {
            ExecTransport::WebSocket => "websocket",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ExecHandshakeFailure {
    HttpVersion,
    MissingProtocol,
}

impl ExecHandshakeFailure {
    fn as_label(self) -> &'static str {
        match self {
            ExecHandshakeFailure::HttpVersion => "http_version",
            ExecHandshakeFailure::MissingProtocol => "missing_protocol",
        }
    }
}

pub fn record_exec_handshake_failure(transport: ExecTransport, reason: ExecHandshakeFailure) {
    exec_handshake_failures_total()
        .with_label_values(&[transport.as_label(), reason.as_label()])
        .inc();
}

/// Encodes all registered metrics using the Prometheus text exposition
/// format.
pub fn gather() -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let metric_families = registry().gather();
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    encoder
        .encode(&metric_families, &mut buffer)
        .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn records_success_and_failure_outcomes() {
        observe_container_operation(Some("ns"), "svc", ContainerOperation::Start, async {
            Ok::<_, ()>(())
        })
        .await
        .unwrap();

        let _ = observe_container_operation(None, "svc", ContainerOperation::Stop, async {
            Err::<(), _>(())
        })
        .await;

        let body = gather().expect("metrics encoded");
        let text = String::from_utf8(body).expect("utf8");
        assert!(text.contains("nanocloud_container_operations_total"));
        assert!(text.contains("operation=\"start\""));
        assert!(text.contains("operation=\"stop\""));
    }

    #[test]
    fn records_exec_handshake_metrics() {
        record_exec_handshake_failure(
            ExecTransport::WebSocket,
            ExecHandshakeFailure::MissingProtocol,
        );
        record_exec_handshake_failure(ExecTransport::WebSocket, ExecHandshakeFailure::HttpVersion);

        let body = gather().expect("metrics encoded");
        let text = String::from_utf8(body).expect("utf8");
        assert!(text.contains("nanocloud_exec_handshake_failures_total"));
        assert!(text.contains("transport=\"websocket\""));
        assert!(text.contains("reason=\"missing_protocol\""));
        assert!(text.contains("reason=\"http_version\""));
    }

    #[test]
    fn record_backup_stream_updates_metrics() {
        let counter =
            backup_stream_bytes_total().with_label_values(&["owner-test", "default", "svc"]);
        let before_bytes = counter.get();
        let histogram =
            backup_stream_duration().with_label_values(&["owner-test", "default", "svc"]);
        let before_count = histogram.get_sample_count();

        record_backup_stream("owner-test", None, "svc", 2048, Duration::from_millis(500));

        let after_bytes = counter.get();
        let after_count = histogram.get_sample_count();
        assert_eq!(after_bytes, before_bytes + 2048);
        assert_eq!(after_count, before_count + 1);
    }

    #[test]
    fn record_backup_capture_updates_metrics() {
        let counter = backup_capture_bytes_total().with_label_values(&["default", "svc"]);
        let before_bytes = counter.get();
        let histogram = backup_capture_duration().with_label_values(&["default", "svc"]);
        let before_count = histogram.get_sample_count();

        record_backup_capture(None, "svc", 2, 4096, Duration::from_secs(2));

        let after_bytes = counter.get();
        let after_count = histogram.get_sample_count();
        assert_eq!(after_bytes, before_bytes + 4096);
        assert_eq!(after_count, before_count + 1);
    }

    #[test]
    fn record_backup_restore_updates_metrics() {
        let counter = backup_restore_bytes_total().with_label_values(&["default", "svc"]);
        let before_bytes = counter.get();
        let histogram = backup_restore_duration().with_label_values(&["default", "svc"]);
        let before_count = histogram.get_sample_count();

        record_backup_restore(None, "svc", 2, 8192, Duration::from_secs(3));

        let after_bytes = counter.get();
        let after_count = histogram.get_sample_count();
        assert_eq!(after_bytes, before_bytes + 8192);
        assert_eq!(after_count, before_count + 1);
    }

    #[test]
    fn keyspace_blocking_metrics_update() {
        set_keyspace_blocking_queue(3);
        set_keyspace_blocking_active(2);
        observe_keyspace_blocking("put", Duration::from_millis(5), Duration::from_millis(2));

        let body = gather().expect("metrics encoded");
        let text = String::from_utf8(body).expect("utf8");
        assert!(text.contains("nanocloud_keyspace_blocking_queue_depth 3"));
        assert!(text.contains("nanocloud_keyspace_blocking_active_tasks 2"));
        assert!(text.contains("nanocloud_keyspace_blocking_wait_duration_seconds_sum"));
        assert!(text.contains("operation=\"put\""));
    }
}
#[derive(Copy, Clone, Debug)]
pub enum ControllerReconcileResult {
    Success,
    Error,
}

impl ControllerReconcileResult {
    fn as_label(self) -> &'static str {
        match self {
            ControllerReconcileResult::Success => "success",
            ControllerReconcileResult::Error => "error",
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum BindingExecutionResult {
    Success,
    Failed,
    TimedOut,
}

impl BindingExecutionResult {
    fn as_label(self) -> &'static str {
        match self {
            BindingExecutionResult::Success => "success",
            BindingExecutionResult::Failed => "failed",
            BindingExecutionResult::TimedOut => "timeout",
        }
    }
}
