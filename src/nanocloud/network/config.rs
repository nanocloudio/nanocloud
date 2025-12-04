//! Shared configuration helpers for the network module.
//!
//! This module centralizes instrumentation knobs so both the policy and proxy
//! helpers can respect the same logging/metrics preferences.

use log::{Level, LevelFilter};
use std::env;
use std::str::FromStr;

/// Lightweight classification used by policy/proxy errors for reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkErrorClass {
    Validation,
    Command,
    Io,
}

impl NetworkErrorClass {
    /// Returns a short label suitable for logs/metrics.
    pub fn as_str(&self) -> &'static str {
        match self {
            NetworkErrorClass::Validation => "validation",
            NetworkErrorClass::Command => "command",
            NetworkErrorClass::Io => "io",
        }
    }
}

/// Controls how much the network module logs and whether metrics should be
/// emitted.
#[derive(Debug, Clone, Copy)]
pub struct NetworkInstrumentation {
    pub metrics_enabled: bool,
    pub log_level: LevelFilter,
}

impl Default for NetworkInstrumentation {
    fn default() -> Self {
        Self {
            metrics_enabled: true,
            log_level: LevelFilter::Info,
        }
    }
}

impl NetworkInstrumentation {
    /// Loads instrumentation settings from environment variables.
    ///
    /// - `NANOCLOUD_NETWORK_METRICS` can disable metrics emission (`false`).
    /// - `NANOCLOUD_NETWORK_LOG_LEVEL` controls module logging verbosity.
    pub fn from_env() -> Result<Self, String> {
        let metrics_enabled = match env::var("NANOCLOUD_NETWORK_METRICS") {
            Ok(raw) => raw
                .parse::<bool>()
                .map_err(|err| format!("invalid NANOCLOUD_NETWORK_METRICS value `{raw}`: {err}"))?,
            Err(_) => true,
        };

        let log_level = match env::var("NANOCLOUD_NETWORK_LOG_LEVEL") {
            Ok(raw) => LevelFilter::from_str(&raw).map_err(|err| {
                format!("invalid NANOCLOUD_NETWORK_LOG_LEVEL value `{raw}`: {err}")
            })?,
            Err(_) => LevelFilter::Info,
        };

        Ok(Self {
            metrics_enabled,
            log_level,
        })
    }

    /// Checks whether the given log level should be emitted given the current
    /// instrumentation settings and the global logger configuration.
    pub fn should_log(&self, level: Level) -> bool {
        level.to_level_filter() <= self.log_level && log::log_enabled!(level)
    }
}
