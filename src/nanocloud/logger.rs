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

use chrono::{SecondsFormat, Utc};
use std::io::{self, Write};

const SERVICE_NAME: &str = "nanocloud";

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

    let mut line = String::new();
    push_field(&mut line, "ts", &timestamp);
    push_field(&mut line, "level", level.as_str());
    push_field(&mut line, "service", SERVICE_NAME);
    push_field(&mut line, "component", component);
    push_field(&mut line, "pid", &pid);
    push_field(&mut line, "msg", message);

    for (key, value) in metadata {
        if key.is_empty() {
            continue;
        }
        push_field(&mut line, key, value);
    }

    if level.is_stderr() {
        let _ = writeln!(io::stderr(), "{}", line);
    } else {
        let _ = writeln!(io::stdout(), "{}", line);
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
