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

use crate::nanocloud::observability::{MetricsConfig, TelemetryError};
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use prometheus::core::Collector;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
    TextEncoder,
};

// Kubernetes-aligned metric label conventions:
// - namespace defaults to "default" when unspecified
// - empty resource identifiers fall back to "unknown"
// - duration histograms always use `_seconds` suffix for units.
const DEFAULT_NAMESPACE: &str = "default";
const UNKNOWN_LABEL: &str = "unknown";

#[derive(Clone)]
struct MetricsState {
    registry: Registry,
    enabled: bool,
}

static METRICS_STATE: OnceLock<MetricsState> = OnceLock::new();
static TELEMETRY_FAILURES_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();

fn metrics_state() -> &'static MetricsState {
    METRICS_STATE.get_or_init(|| {
        MetricsState::from_config(MetricsConfig::from_env())
            .unwrap_or_else(|err| panic!("failed to initialize metrics registry: {err}"))
    })
}

impl MetricsState {
    fn from_config(config: MetricsConfig) -> Result<Self, TelemetryError> {
        let labels = if config.default_labels.is_empty() {
            None
        } else {
            Some(
                config
                    .default_labels
                    .clone()
                    .into_iter()
                    .collect::<HashMap<_, _>>(),
            )
        };

        let registry = Registry::new_custom(Some(config.namespace.clone()), labels)
            .map_err(|err| TelemetryError::InitializationFailed("metrics", err.to_string()))?;

        Ok(MetricsState {
            registry,
            enabled: config.is_enabled(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct MetricsHandle;

impl MetricsHandle {
    pub fn shutdown(&self) {}
}

pub fn init(config: MetricsConfig) -> Result<MetricsHandle, TelemetryError> {
    if METRICS_STATE.get().is_some() {
        return Err(TelemetryError::AlreadyInitialized("metrics"));
    }

    let namespace = config.namespace.clone();
    let enabled = config.is_enabled();
    let default_labels = config.default_labels.clone();
    let state = MetricsState::from_config(config)?;
    METRICS_STATE
        .set(state)
        .map_err(|_| TelemetryError::AlreadyInitialized("metrics"))?;
    tracing::info!(
        target: "nanocloud::telemetry",
        enabled,
        namespace = namespace.as_str(),
        default_labels = ?default_labels,
        "initialized metrics registry"
    );
    Ok(MetricsHandle)
}

static CONTAINER_OPERATION_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static CONTAINER_OPERATION_DURATION: OnceLock<HistogramVec> = OnceLock::new();
static CONTAINER_READY: OnceLock<IntGaugeVec> = OnceLock::new();
static EXEC_HANDSHAKE_FAILURES_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static CNI_OPERATION_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static CNI_OPERATION_DURATION: OnceLock<HistogramVec> = OnceLock::new();
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
static OCI_RUNTIME_EVENTS_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static OCI_REGISTRY_EVENTS_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static RESTARTS_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static BUNDLE_STATE_GAUGE: OnceLock<IntGaugeVec> = OnceLock::new();
static POD_COUNTS_GAUGE: OnceLock<IntGaugeVec> = OnceLock::new();
static SNAPSHOT_OPERATION_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static SNAPSHOT_OPERATION_DURATION: OnceLock<HistogramVec> = OnceLock::new();
static POLICY_OPERATION_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static POLICY_OPERATION_DURATION: OnceLock<HistogramVec> = OnceLock::new();
static POLICY_ERROR_CLASSIFICATION_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static PROXY_OPERATION_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static PROXY_OPERATION_DURATION: OnceLock<HistogramVec> = OnceLock::new();
static PROXY_ERROR_CLASSIFICATION_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
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
static DNS_QUERIES_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static DNS_RESPONSES_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static DNS_DROPS_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static DNS_UPSTREAM_ATTEMPTS_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static CONTROLLER_DISPATCHER_QUEUE_DEPTH: OnceLock<IntGauge> = OnceLock::new();
static CONTROLLER_DISPATCHER_HANDLER_ERRORS_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static CONTROLLER_WATCH_BACKOFF_SECONDS: OnceLock<HistogramVec> = OnceLock::new();
static CONTROLLER_WATCH_LAGGED_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();

fn registry() -> &'static Registry {
    &metrics_state().registry
}

fn register_collector<C>(collector: C) -> C
where
    C: Clone + Collector + Send + Sync + 'static,
{
    let state = metrics_state();
    if state.enabled {
        state
            .registry
            .register(Box::new(collector.clone()))
            .expect("failed to register nanocloud metric collector");
    }
    collector
}

fn telemetry_failures_total(state: &MetricsState) -> &'static IntCounterVec {
    TELEMETRY_FAILURES_TOTAL.get_or_init(|| {
        let counter = IntCounterVec::new(
            Opts::new(
                "telemetry_failures_total",
                "Telemetry failures grouped by component and kind",
            ),
            &["component", "kind"],
        )
        .expect("failed to build telemetry failures counter");

        if state.enabled {
            state
                .registry
                .register(Box::new(counter.clone()))
                .expect("failed to register telemetry failures counter");
        }

        counter
    })
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

fn oci_runtime_events_total() -> &'static IntCounterVec {
    OCI_RUNTIME_EVENTS_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "oci_runtime_events_total",
            "OCI runtime events grouped by event name",
        );
        let counter =
            IntCounterVec::new(opts, &["event"]).expect("failed to build OCI runtime counter");
        register_collector(counter)
    })
}

fn oci_registry_events_total() -> &'static IntCounterVec {
    OCI_REGISTRY_EVENTS_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "oci_registry_events_total",
            "OCI registry events grouped by event name",
        );
        let counter =
            IntCounterVec::new(opts, &["event"]).expect("failed to build OCI registry counter");
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

fn controller_dispatcher_queue_depth() -> &'static IntGauge {
    CONTROLLER_DISPATCHER_QUEUE_DEPTH.get_or_init(|| {
        let gauge = IntGauge::new(
            "controller_dispatcher_queue_depth",
            "Pending controller work items in the dispatcher queue",
        )
        .expect("controller_dispatcher_queue_depth");
        register_collector(gauge)
    })
}

fn controller_dispatcher_handler_errors_total() -> &'static IntCounterVec {
    CONTROLLER_DISPATCHER_HANDLER_ERRORS_TOTAL.get_or_init(|| {
        let counter = IntCounterVec::new(
            Opts::new(
                "controller_dispatcher_handler_errors_total",
                "Total controller handler errors grouped by target",
            ),
            &["target"],
        )
        .expect("controller_dispatcher_handler_errors_total");
        register_collector(counter)
    })
}

fn controller_watch_backoff_seconds() -> &'static HistogramVec {
    CONTROLLER_WATCH_BACKOFF_SECONDS.get_or_init(|| {
        let histogram = HistogramVec::new(
            HistogramOpts::new(
                "controller_watch_backoff_seconds",
                "Backoff delays applied when watch streams restart",
            )
            .buckets(prometheus::exponential_buckets(0.1, 2.0, 8).expect("watch backoff buckets")),
            &["path"],
        )
        .expect("controller_watch_backoff_seconds");
        register_collector(histogram)
    })
}

fn controller_watch_lagged_total() -> &'static IntCounterVec {
    CONTROLLER_WATCH_LAGGED_TOTAL.get_or_init(|| {
        let counter = IntCounterVec::new(
            Opts::new(
                "controller_watch_lagged_total",
                "Total dropped watch events due to lagging subscribers",
            ),
            &["path"],
        )
        .expect("controller_watch_lagged_total");
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

fn cni_operation_total() -> &'static IntCounterVec {
    CNI_OPERATION_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "cni_operations_total",
            "CNI provisioning operations grouped by outcome",
        );
        let counter =
            IntCounterVec::new(opts, &["operation", "result"]).expect("cni_operations_total");
        register_collector(counter)
    })
}

fn cni_operation_duration() -> &'static HistogramVec {
    CNI_OPERATION_DURATION.get_or_init(|| {
        let opts = HistogramOpts::new(
            "cni_operation_duration_seconds",
            "Latency distribution for CNI provisioning and teardown",
        )
        .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0]);
        let histogram =
            HistogramVec::new(opts, &["operation", "result"]).expect("cni_operation_duration");
        register_collector(histogram)
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

fn policy_operation_total() -> &'static IntCounterVec {
    POLICY_OPERATION_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "operations_total",
            "Network policy programming operations grouped by outcome",
        )
        .subsystem("policy");
        let counter = IntCounterVec::new(opts, &["operation", "result", "namespace", "pod"])
            .expect("failed to build policy operations counter");
        register_collector(counter)
    })
}

fn policy_operation_duration() -> &'static HistogramVec {
    POLICY_OPERATION_DURATION.get_or_init(|| {
        let opts = HistogramOpts::new(
            "operation_duration_seconds",
            "Network policy programming operation latency distribution",
        )
        .subsystem("policy")
        .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0]);
        let histogram = HistogramVec::new(opts, &["operation", "result", "namespace", "pod"])
            .expect("failed to build policy operation histogram");
        register_collector(histogram)
    })
}

fn policy_error_classification_total() -> &'static IntCounterVec {
    POLICY_ERROR_CLASSIFICATION_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "error_classifications_total",
            "Network policy failures grouped by classification",
        )
        .subsystem("policy");
        let counter = IntCounterVec::new(opts, &["classification", "namespace", "pod"])
            .expect("failed to build policy error classification counter");
        register_collector(counter)
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

fn proxy_error_classification_total() -> &'static IntCounterVec {
    PROXY_ERROR_CLASSIFICATION_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "error_classifications_total",
            "Network proxy failures grouped by classification",
        )
        .subsystem("proxy");
        let counter = IntCounterVec::new(opts, &["classification", "namespace", "service"])
            .expect("failed to build proxy error classification counter");
        register_collector(counter)
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

fn dns_queries_total() -> &'static IntCounterVec {
    DNS_QUERIES_TOTAL.get_or_init(|| {
        let opts = Opts::new("dns_queries_total", "DNS queries grouped by query type");
        let counter =
            IntCounterVec::new(opts, &["qtype"]).expect("failed to build dns queries counter");
        register_collector(counter)
    })
}

fn dns_responses_total() -> &'static IntCounterVec {
    DNS_RESPONSES_TOTAL.get_or_init(|| {
        let opts = Opts::new("dns_responses_total", "DNS responses grouped by rcode");
        let counter =
            IntCounterVec::new(opts, &["rcode"]).expect("failed to build dns responses counter");
        register_collector(counter)
    })
}

fn dns_drops_total() -> &'static IntCounterVec {
    DNS_DROPS_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "dns_drops_total",
            "Dropped DNS queries grouped by reason (rate_limit, saturated, malformed, too_short)",
        );
        let counter =
            IntCounterVec::new(opts, &["reason"]).expect("failed to build dns drops counter");
        register_collector(counter)
    })
}

fn dns_upstream_attempts_total() -> &'static IntCounterVec {
    DNS_UPSTREAM_ATTEMPTS_TOTAL.get_or_init(|| {
        let opts = Opts::new(
            "dns_upstream_attempts_total",
            "Upstream DNS forwarding attempts grouped by outcome",
        );
        let counter = IntCounterVec::new(opts, &["outcome"])
            .expect("failed to build dns upstream attempts counter");
        register_collector(counter)
    })
}

#[derive(Clone, Copy, Debug)]
pub enum TelemetryComponent {
    Tracing,
    Metrics,
    Health,
}

impl TelemetryComponent {
    fn as_label(self) -> &'static str {
        match self {
            TelemetryComponent::Tracing => "tracing",
            TelemetryComponent::Metrics => "metrics",
            TelemetryComponent::Health => "health",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum TelemetryFailureKind {
    Init,
    Exporter,
    Check,
}

impl TelemetryFailureKind {
    fn as_label(self) -> &'static str {
        match self {
            TelemetryFailureKind::Init => "init",
            TelemetryFailureKind::Exporter => "exporter",
            TelemetryFailureKind::Check => "check",
        }
    }
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
pub enum CniOperation {
    Add,
    Delete,
}

impl CniOperation {
    fn as_label(self) -> &'static str {
        match self {
            CniOperation::Add => "add",
            CniOperation::Delete => "delete",
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
pub enum PolicyOperation {
    Sync,
}

impl PolicyOperation {
    fn as_label(self) -> &'static str {
        match self {
            PolicyOperation::Sync => "sync",
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
        UNKNOWN_LABEL
    } else {
        owner
    }
}

fn resource_label(resource: &str) -> &str {
    if resource.is_empty() {
        UNKNOWN_LABEL
    } else {
        resource
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
        resource_label(workload),
    ];

    container_operation_total().with_label_values(&labels).inc();
    container_operation_duration()
        .with_label_values(&labels)
        .observe(duration.as_secs_f64());
}

fn record_cni_operation(operation: CniOperation, outcome: OperationOutcome, duration: Duration) {
    let labels = [operation.as_label(), outcome.as_label()];
    cni_operation_total().with_label_values(&labels).inc();
    cni_operation_duration()
        .with_label_values(&labels)
        .observe(duration.as_secs_f64());
}

fn record_policy_operation(
    namespace: Option<&str>,
    pod: &str,
    operation: PolicyOperation,
    outcome: OperationOutcome,
    duration: Duration,
) {
    let labels = [
        operation.as_label(),
        outcome.as_label(),
        namespace_label(namespace),
        resource_label(pod),
    ];
    policy_operation_total().with_label_values(&labels).inc();
    policy_operation_duration()
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

/// Wraps a synchronous CNI operation and records its latency and outcome.
pub fn observe_cni_operation<F, T, E>(operation: CniOperation, f: F) -> Result<T, E>
where
    F: FnOnce() -> Result<T, E>,
{
    let start = Instant::now();
    match f() {
        Ok(value) => {
            record_cni_operation(operation, OperationOutcome::Success, start.elapsed());
            Ok(value)
        }
        Err(err) => {
            record_cni_operation(operation, OperationOutcome::Error, start.elapsed());
            Err(err)
        }
    }
}

/// Wraps a synchronous network policy operation and records its latency/outcome.
pub fn observe_policy_operation<F, T, E>(
    namespace: Option<&str>,
    pod: &str,
    operation: PolicyOperation,
    f: F,
) -> Result<T, E>
where
    F: FnOnce() -> Result<T, E>,
{
    let start = Instant::now();
    match f() {
        Ok(value) => {
            record_policy_operation(
                namespace,
                pod,
                operation,
                OperationOutcome::Success,
                start.elapsed(),
            );
            Ok(value)
        }
        Err(err) => {
            record_policy_operation(
                namespace,
                pod,
                operation,
                OperationOutcome::Error,
                start.elapsed(),
            );
            Err(err)
        }
    }
}

/// Records a policy error classification without emitting latency metrics.
pub fn record_policy_error_classification(
    namespace: Option<&str>,
    pod: &str,
    classification: impl Into<String>,
) {
    let classification = classification.into();
    let namespace = namespace_label(namespace);
    policy_error_classification_total()
        .with_label_values(&[classification.as_str(), namespace, resource_label(pod)])
        .inc();
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
        resource_label(service),
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
    let labels = [
        owner_label(owner),
        namespace_label(namespace),
        resource_label(service),
    ];
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
    let labels = [namespace_label(namespace), resource_label(service)];
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
    let labels = [namespace_label(namespace), resource_label(service)];
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

/// Records a proxy error classification without emitting latency metrics.
pub fn record_proxy_error_classification(
    namespace: Option<&str>,
    service: &str,
    classification: impl Into<String>,
) {
    let classification = classification.into();
    let namespace = namespace_label(namespace);
    proxy_error_classification_total()
        .with_label_values(&[classification.as_str(), namespace, resource_label(service)])
        .inc();
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
        resource_label(snapshot),
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

pub fn record_dns_query(qtype: &str) {
    dns_queries_total().with_label_values(&[qtype]).inc();
}

pub fn record_dns_response(rcode: &str) {
    dns_responses_total().with_label_values(&[rcode]).inc();
}

pub fn record_dns_drop(reason: &str) {
    dns_drops_total().with_label_values(&[reason]).inc();
}

pub fn record_dns_upstream_attempt(outcome: &str) {
    dns_upstream_attempts_total()
        .with_label_values(&[outcome])
        .inc();
}

/// Records telemetry failures (initialization, exporter, or health check errors).
pub fn record_telemetry_failure(component: TelemetryComponent, kind: TelemetryFailureKind) {
    if let Some(state) = METRICS_STATE.get() {
        telemetry_failures_total(state)
            .with_label_values(&[component.as_label(), kind.as_label()])
            .inc();
    }
}

pub fn record_controller_reconcile(controller: &str, result: ControllerReconcileResult) {
    controller_reconciles_total()
        .with_label_values(&[controller, result.as_label()])
        .inc();
}

/// Updates the dispatcher queue depth gauge.
pub fn set_controller_dispatcher_queue_depth(depth: i64) {
    controller_dispatcher_queue_depth().set(depth);
}

/// Records a controller handler error for the given target identifier.
pub fn record_controller_handler_error(target: &str) {
    controller_dispatcher_handler_errors_total()
        .with_label_values(&[target])
        .inc();
}

/// Records a watch backoff duration for a given path.
pub fn record_controller_watch_backoff(path: &str, delay: Duration) {
    controller_watch_backoff_seconds()
        .with_label_values(&[path])
        .observe(delay.as_secs_f64());
}

/// Records watch lag/drop occurrences for a given path.
pub fn record_controller_watch_lagged(path: &str, skipped: u64) {
    controller_watch_lagged_total()
        .with_label_values(&[path])
        .inc_by(skipped);
}

pub fn record_binding_execution(service: &str, result: BindingExecutionResult) {
    binding_executions_total()
        .with_label_values(&[resource_label(service), result.as_label()])
        .inc();
}

pub fn record_image_pull(cache_hit: bool) {
    let label = if cache_hit { "true" } else { "false" };
    image_pulls_total().with_label_values(&[label]).inc();
}

pub fn record_oci_runtime_event(event: &str) {
    oci_runtime_events_total()
        .with_label_values(&[event])
        .inc();
}

pub fn record_oci_registry_event(event: &str) {
    oci_registry_events_total()
        .with_label_values(&[event])
        .inc();
}

pub fn record_restart(namespace: Option<&str>, service: &str, reason: &str) {
    let ns = namespace.unwrap_or(DEFAULT_NAMESPACE);
    restarts_total()
        .with_label_values(&[ns, resource_label(service), reason])
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
        gauge
            .with_label_values(&[namespace_label(Some(namespace.as_str()))])
            .set(*count);
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
    let labels = [namespace_label(namespace), resource_label(name)];
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
    let gauge = container_ready()
        .with_label_values(&[namespace_label(namespace), resource_label(workload)]);
    gauge.set(if ready { 1 } else { 0 });
}

/// Removes per-container gauges once a workload is fully deprovisioned to
/// prevent stale time series.
pub fn clear_container(namespace: Option<&str>, workload: &str) {
    let labels = [namespace_label(namespace), resource_label(workload)];
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
    if !metrics_state().enabled {
        return Ok(Vec::new());
    }
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

    #[test]
    fn telemetry_failure_metric_records_counts() {
        // Ensure the metrics registry is initialized for this binary.
        let _ = metrics_state();
        record_telemetry_failure(TelemetryComponent::Tracing, TelemetryFailureKind::Init);

        let snapshot = gather().expect("metrics encoded");
        let text = String::from_utf8(snapshot).expect("utf8");
        assert!(text.contains("nanocloud_telemetry_failures_total"));
        assert!(text.contains("component=\"tracing\""));
        assert!(text.contains("kind=\"init\""));
    }

    #[test]
    fn metrics_handle_shutdown_is_noop() {
        MetricsHandle.shutdown();
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
