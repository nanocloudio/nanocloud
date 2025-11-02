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

use rand::{rngs::OsRng, RngCore};
use std::fmt::Write;
use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::task_local;
use tracing_subscriber::registry::Registry;

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

static TRACING_INIT: OnceLock<()> = OnceLock::new();

/// Initialize the global tracing subscriber exactly once.
pub fn init() {
    TRACING_INIT.get_or_init(|| {
        let subscriber = Registry::default();
        // It's fine if another component already installed a subscriber.
        let _ = tracing::subscriber::set_global_default(subscriber);
    });
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
