use nanocloud::nanocloud::observability::config::{MetricsConfig, MetricsExporter};
use nanocloud::nanocloud::observability::{TracingConfig, TracingFormat, TracingOutput};
use serial_test::serial;
use std::env;

#[test]
#[serial]
fn tracing_config_reads_env_overrides() {
    env::set_var("NANOCLOUD_TRACING_OUTPUT", "stderr");
    env::set_var("NANOCLOUD_TRACING_FORMAT", "json");
    env::set_var("NANOCLOUD_TRACING_SAMPLE_RATE", "0.25");
    env::set_var("NANOCLOUD_TRACING_RATE_LIMIT_PER_SEC", "5");

    let cfg = TracingConfig::from_env();
    assert_eq!(cfg.output, TracingOutput::Stderr);
    assert_eq!(cfg.format, TracingFormat::Json);
    assert_eq!(cfg.sample_rate, 0.25);
    assert_eq!(cfg.rate_limit_per_sec, Some(5));

    env::remove_var("NANOCLOUD_TRACING_OUTPUT");
    env::remove_var("NANOCLOUD_TRACING_FORMAT");
    env::remove_var("NANOCLOUD_TRACING_SAMPLE_RATE");
    env::remove_var("NANOCLOUD_TRACING_RATE_LIMIT_PER_SEC");
}

#[test]
#[serial]
fn tracing_config_supports_otlp_selection() {
    env::set_var("NANOCLOUD_TRACING_OUTPUT", "otlp");
    env::set_var("NANOCLOUD_TRACING_OTLP_ENDPOINT", "http://collector:4317");

    let cfg = TracingConfig::from_env();
    assert_eq!(cfg.output, TracingOutput::Otlp);
    assert_eq!(cfg.otlp_endpoint.as_deref(), Some("http://collector:4317"));

    env::remove_var("NANOCLOUD_TRACING_OUTPUT");
    env::remove_var("NANOCLOUD_TRACING_OTLP_ENDPOINT");
}

#[test]
#[serial]
fn metrics_config_respects_none_exporter() {
    env::set_var("NANOCLOUD_METRICS_EXPORTER", "none");
    let cfg = MetricsConfig::from_env();
    assert_eq!(cfg.exporter, MetricsExporter::None);

    env::set_var("NANOCLOUD_METRICS_EXPORTER", "prometheus");
    let cfg = MetricsConfig::from_env();
    assert_eq!(cfg.exporter, MetricsExporter::Prometheus);

    env::remove_var("NANOCLOUD_METRICS_EXPORTER");
}
