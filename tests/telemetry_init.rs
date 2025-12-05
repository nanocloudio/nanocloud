use nanocloud::nanocloud::observability::{
    metrics, tracing, MetricsConfig, TelemetryConfig, TelemetryError, TracingConfig,
};
use serial_test::serial;

#[test]
#[serial]
fn tracing_init_is_idempotent() {
    let config = TracingConfig::disabled();
    tracing::init_with_config(config.clone()).expect("first tracing init succeeds");

    let err = tracing::init_with_config(config).unwrap_err();
    assert!(matches!(err, TelemetryError::AlreadyInitialized("tracing")));
}

#[test]
#[serial]
fn metrics_init_reports_double_install_and_noop_support() {
    let config = MetricsConfig::disabled();
    metrics::init(config.clone()).expect("first metrics init succeeds");

    let err = metrics::init(config).unwrap_err();
    assert!(matches!(err, TelemetryError::AlreadyInitialized("metrics")));

    assert_eq!(
        metrics::gather().expect("gather should succeed"),
        Vec::<u8>::new()
    );
}

#[test]
#[serial]
fn noop_telemetry_config_disables_all_exporters() {
    let telemetry = TelemetryConfig::noop();
    assert!(!telemetry.metrics.is_enabled());
    assert!(!telemetry.tracing.is_enabled());
}
