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

use crate::nanocloud::observability::tracing;
use chrono::{SecondsFormat, Utc};
use serde_json::Value;
#[cfg(not(test))]
use std::io::{self, Write};
use std::sync::atomic::{AtomicU8, Ordering};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

const SERVICE_NAME: &str = "nanocloud";

#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum LogFormat {
    Text = 0,
    Json = 1,
}

static LOG_FORMAT: AtomicU8 = AtomicU8::new(LogFormat::Text as u8);

pub fn set_log_format(format: LogFormat) {
    LOG_FORMAT.store(format as u8, Ordering::Relaxed);
}

pub fn current_log_format() -> LogFormat {
    match LOG_FORMAT.load(Ordering::Relaxed) {
        1 => LogFormat::Json,
        _ => LogFormat::Text,
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EngineLogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl EngineLogLevel {
    fn as_str(self) -> &'static str {
        match self {
            EngineLogLevel::Debug => "DEBUG",
            EngineLogLevel::Info => "INFO",
            EngineLogLevel::Warn => "WARN",
            EngineLogLevel::Error => "ERROR",
        }
    }

    fn is_stderr(self) -> bool {
        matches!(self, EngineLogLevel::Warn | EngineLogLevel::Error)
    }
}

fn encode_field_value(value: &str) -> String {
    let needs_quotes = value.chars().any(|c| {
        c.is_whitespace()
            || matches!(
                c,
                '"' | '\\' | '=' | '[' | ']' | '{' | '}' | ',' | '\n' | '\r' | '\t'
            )
    });

    if !needs_quotes {
        return value.to_string();
    }

    let mut encoded = String::with_capacity(value.len() + 2);
    encoded.push('"');
    for ch in value.chars() {
        match ch {
            '"' => encoded.push_str("\\\""),
            '\\' => encoded.push_str("\\\\"),
            '\n' => encoded.push_str("\\n"),
            '\r' => encoded.push_str("\\r"),
            '\t' => encoded.push_str("\\t"),
            _ => encoded.push(ch),
        }
    }
    encoded.push('"');
    encoded
}

fn push_field(buffer: &mut String, key: &str, value: &str) {
    if !buffer.is_empty() {
        buffer.push(' ');
    }
    buffer.push_str(key);
    buffer.push('=');
    buffer.push_str(&encode_field_value(value));
}

pub fn log_event(level: EngineLogLevel, component: &str, message: &str, metadata: &[(&str, &str)]) {
    let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let pid = std::process::id().to_string();
    let trace_context = tracing::current_context();

    match current_log_format() {
        LogFormat::Text => {
            let mut line = String::new();
            push_field(&mut line, "ts", &timestamp);
            push_field(&mut line, "level", level.as_str());
            push_field(&mut line, "service", SERVICE_NAME);
            push_field(&mut line, "component", component);
            push_field(&mut line, "pid", &pid);
            push_field(&mut line, "msg", message);
            if let Some(ctx) = trace_context.as_ref() {
                push_field(&mut line, "trace_id", ctx.trace_id());
                push_field(&mut line, "span_id", ctx.span_id());
            }

            for (key, value) in metadata {
                if key.is_empty() {
                    continue;
                }
                push_field(&mut line, key, value);
            }

            write_line(level, &line);
        }
        LogFormat::Json => {
            let mut payload = serde_json::Map::new();
            payload.insert("ts".into(), Value::String(timestamp));
            payload.insert("level".into(), Value::String(level.as_str().to_string()));
            payload.insert("service".into(), Value::String(SERVICE_NAME.to_string()));
            payload.insert("component".into(), Value::String(component.to_string()));
            payload.insert("pid".into(), Value::String(pid));
            payload.insert("msg".into(), Value::String(message.to_string()));
            if let Some(ctx) = trace_context {
                payload.insert("trace_id".into(), Value::String(ctx.trace_id().to_string()));
                payload.insert("span_id".into(), Value::String(ctx.span_id().to_string()));
            }
            for (key, value) in metadata {
                if key.is_empty() {
                    continue;
                }
                payload.insert((*key).to_string(), Value::String((*value).to_string()));
            }
            let line = Value::Object(payload).to_string();
            write_line(level, &line);
        }
    }
}

pub fn log_debug(component: &str, message: &str, metadata: &[(&str, &str)]) {
    log_event(EngineLogLevel::Debug, component, message, metadata);
}

pub fn log_info(component: &str, message: &str, metadata: &[(&str, &str)]) {
    log_event(EngineLogLevel::Info, component, message, metadata);
}

pub fn log_warn(component: &str, message: &str, metadata: &[(&str, &str)]) {
    log_event(EngineLogLevel::Warn, component, message, metadata);
}

pub fn log_error(component: &str, message: &str, metadata: &[(&str, &str)]) {
    log_event(EngineLogLevel::Error, component, message, metadata);
}

#[cfg(not(test))]
fn write_line(level: EngineLogLevel, line: &str) {
    let write_result = if level.is_stderr() {
        let mut stderr = io::stderr().lock();
        writeln!(stderr, "{}", line)
    } else {
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "{}", line)
    };

    if let Err(error) = write_result {
        let mut stderr = io::stderr().lock();
        let _ = writeln!(
            stderr,
            "nanocloud: failed to write log line: {} (original: {})",
            error, line
        );
    }
}

#[cfg(test)]
fn write_line(level: EngineLogLevel, line: &str) {
    let _ = level.is_stderr();
    let store = test_log_store();
    let mut guard = store.lock().unwrap();
    guard.push((level, line.to_string()));
}

#[cfg(test)]
fn test_log_store() -> &'static Mutex<Vec<(EngineLogLevel, String)>> {
    static STORE: OnceLock<Mutex<Vec<(EngineLogLevel, String)>>> = OnceLock::new();
    STORE.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn take_test_logs() -> Vec<(EngineLogLevel, String)> {
    let store = test_log_store();
    let mut guard = store.lock().unwrap();
    guard.drain(..).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::observability::tracing;

    #[tokio::test]
    async fn text_logs_include_trace_ids() {
        tracing::init();
        set_log_format(LogFormat::Text);
        take_test_logs();
        tracing::with_span("test.component", "text-log", async {
            log_info("unit", "testing", &[("namespace", "default")]);
        })
        .await;

        let logs = take_test_logs();
        assert_eq!(logs.len(), 1);
        let (_, line) = &logs[0];
        assert!(
            line.contains("trace_id=") && line.contains("span_id="),
            "trace metadata missing: {line}"
        );
        assert!(
            line.contains("namespace=\"default\"") || line.contains("namespace=default"),
            "metadata not propagated: {line}"
        );
    }

    #[tokio::test]
    async fn json_logs_include_trace_ids() {
        tracing::init();
        set_log_format(LogFormat::Json);
        take_test_logs();
        tracing::with_span("test.component", "json-log", async {
            log_warn("unit", "testing-json", &[("key", "value")]);
        })
        .await;

        let logs = take_test_logs();
        assert_eq!(logs.len(), 1);
        let payload: Value = serde_json::from_str(&logs[0].1).expect("valid json log");
        let trace_id = payload
            .get("trace_id")
            .and_then(|v| v.as_str())
            .expect("trace_id present");
        let span_id = payload
            .get("span_id")
            .and_then(|v| v.as_str())
            .expect("span_id present");
        let component = payload.get("component").and_then(|v| v.as_str());
        assert_eq!(component, Some("unit"));
        assert_eq!(payload.get("key").and_then(|v| v.as_str()), Some("value"));
        assert!(!trace_id.is_empty());
        assert!(!span_id.is_empty());
    }
}
