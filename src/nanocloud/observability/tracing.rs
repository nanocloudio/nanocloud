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
use rand::{rngs::OsRng, RngCore};
use std::fmt::Write;
use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::task_local;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::layer::{Context as LayerContext, Layer};
use tracing_subscriber::registry::Registry as SubscriberRegistry;
use tracing_subscriber::prelude::*;

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

#[derive(Clone, Debug)]
pub struct TracingHandle;

impl TracingHandle {
    /// Flush or teardown any tracing exporters. The current implementation uses
    /// synchronous writers so there is nothing to drain, but the hook is kept
    /// for future async exporters.
    pub fn shutdown(&self) {
        tracing::debug!(
            target: "nanocloud::telemetry",
            "tracing shutdown requested; no buffered exporters to flush"
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
        let subscriber = build_subscriber(&config)?;
        tracing::subscriber::set_global_default(subscriber).map_err(|err| {
            TelemetryError::InitializationFailed("tracing", err.to_string())
        })?;
        tracing::info!(
            target: "nanocloud::telemetry",
            format = ?config.format,
            sample_rate = config.sample_rate,
            "installed tracing subscriber"
        );
        TracingHandle
    } else {
        let subscriber = SubscriberRegistry::default();
        tracing::subscriber::set_global_default(subscriber).map_err(|err| {
            TelemetryError::InitializationFailed("tracing", err.to_string())
        })?;
        tracing::info!(
            target: "nanocloud::telemetry",
            "tracing disabled via configuration; using no-op subscriber"
        );
        TracingHandle
    };

    TRACING_STATE
        .set(handle.clone())
        .map_err(|_| TelemetryError::AlreadyInitialized("tracing"))?;

    Ok(handle)
}

fn build_subscriber(
    config: &TracingConfig,
) -> Result<impl tracing::Subscriber + Send + Sync, TelemetryError> {
    let filter = EnvFilter::builder()
        .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
        .parse(config.filter_directives.clone())
        .map_err(|err| {
            TelemetryError::InvalidConfig(format!("invalid tracing filter directives: {err}"))
        })?;

    let fmt_layer = match config.format {
        TracingFormat::Json => fmt::layer()
            .with_target(false)
            .with_writer(make_writer(&config.output))
            .json()
            .boxed(),
        TracingFormat::Pretty => fmt::layer()
            .with_target(false)
            .with_writer(make_writer(&config.output))
            .pretty()
            .boxed(),
    };

    let sampler = SamplingLayer::new(config.sample_rate);

    Ok(SubscriberRegistry::default().with(filter).with(sampler).with(fmt_layer))
}

/// Returns the currently active [`TraceContext`], if any.
pub fn current_context() -> Option<TraceContext> {
    ACTIVE_TRACE.try_with(|ctx| ctx.clone()).ok()
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
        TracingOutput::Disabled => BoxMakeWriter::new(std::io::sink),
    }
}

#[derive(Clone, Debug)]
struct SamplingLayer {
    rate: f64,
}

impl SamplingLayer {
    fn new(rate: f64) -> Self {
        SamplingLayer { rate }
    }

    fn allow(&self) -> bool {
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
