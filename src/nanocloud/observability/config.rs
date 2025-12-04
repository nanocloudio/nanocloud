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

use std::collections::BTreeMap;
use std::env;
use std::fmt::{Display, Formatter};

/// Telemetry configuration shared between tracing and metrics.
#[derive(Clone, Debug)]
pub struct TelemetryConfig {
    pub tracing: TracingConfig,
    pub metrics: MetricsConfig,
}

impl TelemetryConfig {
    pub fn from_env() -> Self {
        TelemetryConfig {
            tracing: TracingConfig::from_env(),
            metrics: MetricsConfig::from_env(),
        }
    }

    pub fn noop() -> Self {
        TelemetryConfig {
            tracing: TracingConfig::disabled(),
            metrics: MetricsConfig::disabled(),
        }
    }
}

/// Describes where tracing output should be written.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TracingOutput {
    Stdout,
    Stderr,
    Disabled,
}

/// Format to use for tracing output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TracingFormat {
    Pretty,
    Json,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TracingConfig {
    pub filter_directives: String,
    pub format: TracingFormat,
    pub output: TracingOutput,
    /// Probability (0.0 - 1.0) that an event/span is recorded.
    pub sample_rate: f64,
}

impl TracingConfig {
    pub fn from_env() -> Self {
        let filter_directives =
            env::var("NANOCLOUD_TRACING_FILTER").unwrap_or_else(|_| "info".to_string());
        let format = env::var("NANOCLOUD_TRACING_FORMAT")
            .ok()
            .and_then(|value| match value.to_ascii_lowercase().as_str() {
                "json" => Some(TracingFormat::Json),
                "pretty" | "text" => Some(TracingFormat::Pretty),
                _ => None,
            })
            .unwrap_or(TracingFormat::Pretty);
        let output = env::var("NANOCLOUD_TRACING_OUTPUT")
            .ok()
            .and_then(|value| match value.to_ascii_lowercase().as_str() {
                "stderr" => Some(TracingOutput::Stderr),
                "stdout" => Some(TracingOutput::Stdout),
                "off" | "disabled" | "none" => Some(TracingOutput::Disabled),
                _ => None,
            })
            .unwrap_or(TracingOutput::Stdout);
        let sample_rate = env::var("NANOCLOUD_TRACING_SAMPLE_RATE")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(1.0);

        TracingConfig {
            filter_directives,
            format,
            output,
            sample_rate,
        }
    }

    pub fn disabled() -> Self {
        TracingConfig {
            filter_directives: "off".to_string(),
            format: TracingFormat::Pretty,
            output: TracingOutput::Disabled,
            sample_rate: 0.0,
        }
    }

    pub fn validate(&self) -> Result<(), TelemetryError> {
        if !(0.0..=1.0).contains(&self.sample_rate) {
            return Err(TelemetryError::InvalidConfig(
                "tracing sample rate must be between 0.0 and 1.0".to_string(),
            ));
        }
        Ok(())
    }

    pub fn is_enabled(&self) -> bool {
        !matches!(self.output, TracingOutput::Disabled) && self.sample_rate > 0.0
    }
}

/// Identifies how metrics should be exported.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetricsExporter {
    Prometheus,
    None,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricsConfig {
    pub exporter: MetricsExporter,
    pub namespace: String,
    pub default_labels: BTreeMap<String, String>,
}

impl MetricsConfig {
    pub fn from_env() -> Self {
        let exporter = env::var("NANOCLOUD_METRICS_EXPORTER")
            .ok()
            .and_then(|value| match value.to_ascii_lowercase().as_str() {
                "none" | "disabled" | "off" => Some(MetricsExporter::None),
                "prometheus" | "" => Some(MetricsExporter::Prometheus),
                _ => None,
            })
            .unwrap_or(MetricsExporter::Prometheus);
        let namespace =
            env::var("NANOCLOUD_METRICS_NAMESPACE").unwrap_or_else(|_| "nanocloud".to_string());

        MetricsConfig {
            exporter,
            namespace,
            default_labels: BTreeMap::new(),
        }
    }

    pub fn disabled() -> Self {
        MetricsConfig {
            exporter: MetricsExporter::None,
            namespace: "nanocloud".to_string(),
            default_labels: BTreeMap::new(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        !matches!(self.exporter, MetricsExporter::None)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TelemetryError {
    AlreadyInitialized(&'static str),
    InvalidConfig(String),
    InitializationFailed(&'static str, String),
}

impl Display for TelemetryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TelemetryError::AlreadyInitialized(component) => {
                write!(f, "{component} telemetry already initialized")
            }
            TelemetryError::InvalidConfig(reason) => write!(f, "{reason}"),
            TelemetryError::InitializationFailed(component, reason) => {
                write!(f, "{component} telemetry initialization failed: {reason}")
            }
        }
    }
}

impl std::error::Error for TelemetryError {}
