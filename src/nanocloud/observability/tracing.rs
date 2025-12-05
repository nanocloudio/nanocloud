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

//! Minimal tracing utilities for propagating span identifiers across the
//! Nanocloud control-plane. Spans are backed by the `tracing` crate but we
//! additionally maintain a task-local [`TraceContext`] so the existing logger
//! can attach `trace_id` / `span_id` pairs to every log line without forcing
//! a wholesale logging rewrite.

use crate::nanocloud::observability::{
    TelemetryError, TracingConfig, TracingFormat, TracingOutput,
};
#[cfg(feature = "telemetry-otlp")]
use opentelemetry::trace::TracerProvider;
use rand::{rngs::OsRng, RngCore};
use std::fmt::Write;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::SystemTime;
use tokio::task_local;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::layer::{Context as LayerContext, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::Registry as SubscriberRegistry;

type BoxedSubscriber = Box<dyn tracing::Subscriber + Send + Sync>;

#[cfg(feature = "telemetry-otlp")]
type OtlpLayer = tracing_opentelemetry::OpenTelemetryLayer<
    tracing_subscriber::layer::Layered<
        SamplingLayer,
        tracing_subscriber::layer::Layered<EnvFilter, SubscriberRegistry>,
    >,
    opentelemetry_sdk::trace::Tracer,
>;

struct BuiltSubscriber {
    subscriber: BoxedSubscriber,
    #[cfg(feature = "telemetry-otlp")]
    otlp_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

#[derive(Clone, Debug)]
pub struct TraceContext {
    trace_id: Arc<str>,
    span_id: Arc<str>,
}

impl TraceContext {
    pub fn trace_id(&self) -> &str {
        &self.trace_id
    }

    pub fn span_id(&self) -> &str {
        &self.span_id
    }
}

task_local! {
    static ACTIVE_TRACE: TraceContext;
}

static TRACING_STATE: OnceLock<TracingHandle> = OnceLock::new();
const DEFAULT_OTLP_ENDPOINT: &str = "http://localhost:4317";

#[derive(Clone, Debug)]
pub struct TracingHandle {
    otlp_installed: bool,
    #[cfg(feature = "telemetry-otlp")]
    otlp_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

impl TracingHandle {
    /// Flush or teardown any tracing exporters. The current implementation uses
    /// synchronous writers so there is nothing to drain, but the hook is kept
    /// for future async exporters.
    pub fn shutdown(&self) {
        #[cfg(feature = "telemetry-otlp")]
        if let Some(ref provider) = self.otlp_provider {
            // Flush OTLP exporters before exit.
            let _ = provider.shutdown();
        }
        tracing::debug!(
            target: "nanocloud::telemetry",
            otlp = self.otlp_installed,
            "tracing shutdown requested"
        );
    }
}

/// Initialize the global tracing subscriber exactly once with the provided
/// configuration. Returns an error if called multiple times.
pub fn init_with_config(config: TracingConfig) -> Result<TracingHandle, TelemetryError> {
    config.validate()?;

    if TRACING_STATE.get().is_some() {
        return Err(TelemetryError::AlreadyInitialized("tracing"));
    }

    let handle = if config.is_enabled() {
        let built = build_subscriber(&config)?;
        tracing::subscriber::set_global_default(built.subscriber)
            .map_err(|err| TelemetryError::InitializationFailed("tracing", err.to_string()))?;
        tracing::debug!(
            target: "nanocloud::telemetry",
            format = ?config.format,
            output = ?config.output,
            sample_rate = config.sample_rate,
            rate_limit_per_sec = config.rate_limit_per_sec,
            otlp_endpoint = config
                .otlp_endpoint
                .as_deref()
                .unwrap_or(DEFAULT_OTLP_ENDPOINT),
            "installed tracing subscriber"
        );
        TracingHandle {
            otlp_installed: matches!(config.output, TracingOutput::Otlp),
            #[cfg(feature = "telemetry-otlp")]
            otlp_provider: built.otlp_provider,
        }
    } else {
        let subscriber = SubscriberRegistry::default();
        tracing::subscriber::set_global_default(subscriber)
            .map_err(|err| TelemetryError::InitializationFailed("tracing", err.to_string()))?;
        tracing::info!(
            target: "nanocloud::telemetry",
            "tracing disabled via configuration; using no-op subscriber"
        );
        TracingHandle {
            otlp_installed: false,
            #[cfg(feature = "telemetry-otlp")]
            otlp_provider: None,
        }
    };

    TRACING_STATE
        .set(handle.clone())
        .map_err(|_| TelemetryError::AlreadyInitialized("tracing"))?;

    Ok(handle)
}

fn build_subscriber(
    config: &TracingConfig,
) -> Result<BuiltSubscriber, TelemetryError> {
    let filter = EnvFilter::builder()
        .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
        .parse(config.filter_directives.clone())
        .map_err(|err| {
            TelemetryError::InvalidConfig(format!("invalid tracing filter directives: {err}"))
        })?;

    let sampler = SamplingLayer::new(config.sample_rate, config.rate_limit_per_sec);
    let registry = SubscriberRegistry::default().with(filter).with(sampler);

    match config.output {
        TracingOutput::Otlp => {
            #[cfg(feature = "telemetry-otlp")]
            {
                let (otlp_layer, provider) = build_otlp_layer(config)?;
                Ok(BuiltSubscriber {
                    subscriber: Box::new(registry.with(otlp_layer)),
                    otlp_provider: Some(provider),
                })
            }
            #[cfg(not(feature = "telemetry-otlp"))]
            {
                Err(TelemetryError::InitializationFailed(
                    "tracing",
                    "OTLP output requested but feature `telemetry-otlp` is not enabled".to_string(),
                ))
            }
        }
        _ => {
            let fmt_layer = match config.format {
                TracingFormat::Json => fmt::layer()
                    .with_target(false)
                    .with_writer(make_writer(&config.output))
                    .with_ansi(false)
                    .json()
                    .boxed(),
                TracingFormat::Pretty => fmt::layer()
                    .with_target(false)
                    .with_writer(make_writer(&config.output))
                    .with_ansi(false)
                    .pretty()
                    .boxed(),
            };
            Ok(BuiltSubscriber {
                subscriber: Box::new(registry.with(fmt_layer)),
                #[cfg(feature = "telemetry-otlp")]
                otlp_provider: None,
            })
        }
    }
}

/// Returns the currently active [`TraceContext`], if any.
pub fn current_context() -> Option<TraceContext> {
    ACTIVE_TRACE.try_with(|ctx| ctx.clone()).ok()
}

#[cfg(feature = "telemetry-otlp")]
fn build_otlp_layer(
    config: &TracingConfig,
) -> Result<
    (
        OtlpLayer,
        opentelemetry_sdk::trace::SdkTracerProvider,
    ),
    TelemetryError,
> {
    use opentelemetry_otlp::WithExportConfig;

    let endpoint = config
        .otlp_endpoint
        .clone()
        .unwrap_or_else(|| DEFAULT_OTLP_ENDPOINT.to_string());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|err| {
            TelemetryError::InitializationFailed(
                "tracing",
                format!("failed to build OTLP exporter: {err}"),
            )
        })?;

    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build();

    let tracer = tracer_provider.tracer("nanocloud");
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    Ok((tracing_opentelemetry::layer().with_tracer(tracer), tracer_provider))
}

/// Execute `fut` while publishing a tracing span whose identifiers are
/// propagated through the [`TraceContext`].
pub async fn with_span<T>(
    component: &'static str,
    span_name: impl Into<String>,
    fut: impl Future<Output = T>,
) -> T {
    let existing = current_context();
    let trace_id = existing
        .as_ref()
        .map(|ctx| ctx.trace_id.clone())
        .unwrap_or_else(|| Arc::<str>::from(generate_trace_id()));
    let span_id = Arc::<str>::from(generate_span_id());
    let context = TraceContext {
        trace_id: trace_id.clone(),
        span_id: span_id.clone(),
    };
    let name = span_name.into();
    let span = tracing::info_span!(
        "nanocloud",
        component = component,
        span = name.as_str(),
        trace_id = trace_id.as_ref(),
        span_id = span_id.as_ref(),
    );

    ACTIVE_TRACE
        .scope(context, async move {
            let _guard = span.enter();
            fut.await
        })
        .await
}

fn generate_trace_id() -> String {
    random_hex(16)
}

fn generate_span_id() -> String {
    random_hex(8)
}

fn random_hex(bytes: usize) -> String {
    let mut data = vec![0u8; bytes];
    OsRng.fill_bytes(&mut data);
    let mut output = String::with_capacity(bytes * 2);
    for byte in data {
        let _ = write!(&mut output, "{:02x}", byte);
    }
    output
}

fn make_writer(output: &TracingOutput) -> BoxMakeWriter {
    match output {
        TracingOutput::Stdout => BoxMakeWriter::new(std::io::stdout),
        TracingOutput::Stderr => BoxMakeWriter::new(std::io::stderr),
        TracingOutput::Otlp => BoxMakeWriter::new(std::io::sink),
        TracingOutput::Disabled => BoxMakeWriter::new(std::io::sink),
    }
}

#[derive(Clone, Debug)]
struct SamplingLayer {
    rate: f64,
    rate_limiter: Option<RateLimiter>,
}

impl SamplingLayer {
    fn new(rate: f64, rate_limit_per_sec: Option<u64>) -> Self {
        SamplingLayer {
            rate,
            rate_limiter: rate_limit_per_sec.map(RateLimiter::new),
        }
    }

    fn allow(&self) -> bool {
        if let Some(ref limiter) = self.rate_limiter {
            if !limiter.allow() {
                return false;
            }
        }
        if self.rate >= 1.0 {
            return true;
        }
        if self.rate <= 0.0 {
            return false;
        }
        rand::random::<f64>() <= self.rate
    }
}

impl<S> Layer<S> for SamplingLayer
where
    S: tracing::Subscriber,
{
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _ctx: LayerContext<'_, S>) -> bool {
        // Always allow span creation so downstream instrumentation can attach identifiers,
        // but probabilistically drop events to reduce overhead on hot paths.
        if metadata.is_span() {
            return true;
        }
        self.allow()
    }
}

#[derive(Clone, Debug)]
struct RateLimiter {
    limit: u64,
    state: Arc<RateLimiterState>,
}

#[derive(Debug)]
struct RateLimiterState {
    window_start: AtomicU64,
    count: AtomicU64,
}

impl RateLimiter {
    fn new(limit: u64) -> Self {
        RateLimiter {
            limit,
            state: Arc::new(RateLimiterState {
                window_start: AtomicU64::new(current_second()),
                count: AtomicU64::new(0),
            }),
        }
    }

    fn allow(&self) -> bool {
        let now = current_second();
        let window_start = self.state.window_start.load(Ordering::Relaxed);
        if now != window_start {
            self.state.window_start.store(now, Ordering::Relaxed);
            self.state.count.store(0, Ordering::Relaxed);
        }

        let previous = self.state.count.fetch_add(1, Ordering::Relaxed);
        previous < self.limit
    }
}

fn current_second() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampling_layer_honors_rate_limit() {
        let layer = SamplingLayer::new(1.0, Some(2));
        assert!(layer.allow());
        assert!(layer.allow());
        assert!(!layer.allow());
    }

    #[test]
    #[cfg(not(feature = "telemetry-otlp"))]
    fn otlp_output_without_feature_is_rejected() {
        let config = TracingConfig {
            filter_directives: "info".to_string(),
            format: TracingFormat::Pretty,
            output: TracingOutput::Otlp,
            sample_rate: 1.0,
            rate_limit_per_sec: None,
            otlp_endpoint: Some("http://localhost:4317".to_string()),
        };

        let result = build_subscriber(&config);
        assert!(matches!(
            result,
            Err(TelemetryError::InitializationFailed("tracing", _))
        ));
    }

    #[test]
    fn invalid_sample_rate_is_rejected() {
        let mut config = TracingConfig::disabled();
        config.sample_rate = 1.5;
        assert!(config.validate().is_err());

        config.sample_rate = 0.5;
        config.rate_limit_per_sec = Some(0);
        assert!(config.validate().is_err());
    }

    #[test]
    fn invalid_filter_directives_error() {
        let mut config = TracingConfig::disabled();
        config.sample_rate = 1.0;
        config.output = TracingOutput::Stdout;
        config.filter_directives = "nanocloud=notalevel".to_string();

        let err = build_subscriber(&config);
        assert!(matches!(err, Err(TelemetryError::InvalidConfig(_))));
    }

    #[test]
    fn shutdown_is_noop_without_otlp() {
        TracingHandle {
            otlp_installed: false,
            #[cfg(feature = "telemetry-otlp")]
            otlp_provider: None,
        }
        .shutdown();
    }
}
