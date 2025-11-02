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

use chrono::DateTime;
use serde_json::Value;

use crate::nanocloud::api::types::{
    BindingHistoryEntry, BindingHistoryStatus, Bundle, BundleCondition, BundleConditionKind,
    BundleConditionStatus, PodTable,
};
use crate::nanocloud::cli::Terminal;

const CONDITION_DISPLAY_ORDER: [BundleConditionKind; 4] = [
    BundleConditionKind::Ready,
    BundleConditionKind::Bound,
    BundleConditionKind::SecretsProvisioned,
    BundleConditionKind::ProfilePrepared,
];

pub(super) fn service_display_name(namespace: Option<&str>, service: &str) -> String {
    namespace
        .filter(|value| !value.is_empty())
        .map(|ns| format!("{ns}-{service}"))
        .unwrap_or_else(|| service.to_string())
}

pub(super) fn print_pod_table(table: &PodTable) {
    if table.column_definitions.is_empty() {
        Terminal::stdout(format_args!("No pods found."));
        return;
    }

    let headers: Vec<String> = table
        .column_definitions
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let column_count = headers.len();
    let mut widths: Vec<usize> = headers.iter().map(|header| header.len()).collect();
    let mut formatted_rows: Vec<Vec<String>> = Vec::with_capacity(table.rows.len());

    for row in &table.rows {
        let mut cells = Vec::with_capacity(column_count);
        for (index, width) in widths.iter_mut().enumerate().take(column_count) {
            let value = row.cells.get(index);
            let text = value.map(table_cell_to_string).unwrap_or_else(String::new);
            if text.len() > *width {
                *width = text.len();
            }
            cells.push(text);
        }
        formatted_rows.push(cells);
    }

    let header_line = headers
        .iter()
        .enumerate()
        .map(|(idx, header)| format!("{:<width$}", header, width = widths[idx]))
        .collect::<Vec<_>>()
        .join("  ");
    Terminal::stdout(format_args!("{}", header_line));

    if formatted_rows.is_empty() {
        Terminal::stdout(format_args!("(no pods found)"));
        return;
    }

    for row in formatted_rows {
        let line = row
            .iter()
            .enumerate()
            .map(|(idx, cell)| format!("{:<width$}", cell, width = widths[idx]))
            .collect::<Vec<_>>()
            .join("  ");
        Terminal::stdout(format_args!("{}", line));
    }
}

pub(super) fn print_bundle_table(bundles: &[Bundle]) {
    if bundles.is_empty() {
        Terminal::stdout(format_args!("No bundles found."));
        return;
    }

    let headers = [
        "NAMESPACE",
        "BUNDLE",
        "READY",
        "BOUND",
        "SECRETS",
        "PROFILE",
        "UPDATED",
    ];
    let mut widths: Vec<usize> = headers.iter().map(|header| header.len()).collect();
    let mut rows: Vec<Vec<String>> = Vec::with_capacity(bundles.len());

    for bundle in bundles {
        let namespace = bundle
            .metadata
            .namespace
            .as_deref()
            .filter(|value| !value.is_empty())
            .unwrap_or("default")
            .to_string();
        let name = bundle
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| bundle.spec.service.clone());
        let ready = summarize_condition(bundle, BundleConditionKind::Ready);
        let bound = summarize_condition(bundle, BundleConditionKind::Bound);
        let secrets = summarize_condition(bundle, BundleConditionKind::SecretsProvisioned);
        let profile = summarize_condition(bundle, BundleConditionKind::ProfilePrepared);
        let updated = bundle
            .status
            .as_ref()
            .and_then(|status| status.last_reconciled_time.as_deref())
            .map(|value| format_timestamp(Some(value)))
            .unwrap_or_else(|| "-".to_string());

        let row = vec![namespace, name, ready, bound, secrets, profile, updated];
        for (idx, cell) in row.iter().enumerate() {
            if cell.len() > widths[idx] {
                widths[idx] = cell.len();
            }
        }
        rows.push(row);
    }

    let header_line = headers
        .iter()
        .enumerate()
        .map(|(idx, header)| format!("{:<width$}", header, width = widths[idx]))
        .collect::<Vec<_>>()
        .join("  ");
    Terminal::stdout(format_args!("{}", header_line));

    for row in rows {
        let line = row
            .iter()
            .enumerate()
            .map(|(idx, cell)| format!("{:<width$}", cell, width = widths[idx]))
            .collect::<Vec<_>>()
            .join("  ");
        Terminal::stdout(format_args!("{}", line));
    }
}

pub(super) fn print_bundle_state(bundle: &Bundle) {
    let namespace = bundle
        .metadata
        .namespace
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or("default");
    let name = bundle
        .metadata
        .name
        .as_deref()
        .unwrap_or(bundle.spec.service.as_str());
    Terminal::stdout(format_args!("Bundle: {}/{}", namespace, name));

    match bundle.status.as_ref() {
        Some(status) => {
            let phase = status
                .phase
                .as_ref()
                .map(|p| format!("{p:?}"))
                .unwrap_or_else(|| "Unknown".to_string());
            Terminal::stdout(format_args!("  Phase: {}", phase));
            let observed = status
                .observed_generation
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string());
            Terminal::stdout(format_args!("  Observed Generation: {}", observed));
            Terminal::stdout(format_args!(
                "  Last Reconciled: {}",
                format_timestamp(status.last_reconciled_time.as_deref())
            ));
            Terminal::stdout(format_args!("  Conditions:"));
            for kind in CONDITION_DISPLAY_ORDER {
                if let Some(condition) = find_condition(status, kind) {
                    let status_text = format!("{:?}", condition.status);
                    let timestamp = format_timestamp(condition.last_transition_time.as_deref());
                    let mut line = format!(
                        "    - {:<20} {:<8} {}",
                        kind.as_str(),
                        status_text,
                        timestamp
                    );
                    if let Some(reason) = condition.reason.as_deref() {
                        line.push_str(&format!(" ({reason})"));
                    }
                    Terminal::stdout(format_args!("{}", line));
                    if let Some(message) = condition.message.as_deref() {
                        Terminal::stdout(format_args!("        {}", message));
                    }
                } else {
                    Terminal::stdout(format_args!("    - {:<20} UNKNOWN", kind.as_str()));
                }
            }
            if !status.binding_history.is_empty() {
                Terminal::stdout(format_args!("  Bindings:"));
                print_binding_history(&status.binding_history);
            }
        }
        None => {
            Terminal::stdout(format_args!("  Status: not yet reported"));
        }
    }
}

fn format_timestamp(raw: Option<&str>) -> String {
    raw.and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%:z").to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn table_cell_to_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn summarize_condition(bundle: &Bundle, kind: BundleConditionKind) -> String {
    match bundle
        .status
        .as_ref()
        .and_then(|status| find_condition(status, kind))
    {
        Some(condition) => {
            let status = format!("{:?}", condition.status);
            match (&condition.status, condition.reason.as_deref()) {
                (BundleConditionStatus::True, _) => status,
                (_, Some(reason)) => format!("{} ({})", status, reason),
                _ => status,
            }
        }
        None => "UNKNOWN".to_string(),
    }
}

fn find_condition(
    status: &crate::nanocloud::api::types::BundleStatus,
    kind: BundleConditionKind,
) -> Option<&BundleCondition> {
    status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == kind)
}

fn print_binding_history(entries: &[BindingHistoryEntry]) {
    for entry in entries {
        let identifier = format!("{}/{}", entry.service, entry.binding_id);
        let status = format_history_status(&entry.status);
        let finished = format_timestamp(entry.last_finished_at.as_deref());
        let duration = entry
            .duration_ms
            .map(|ms| format!("{ms} ms"))
            .unwrap_or_else(|| "-".to_string());
        let exit_code = entry
            .exit_code
            .map(|code| code.to_string())
            .unwrap_or_else(|| "-".to_string());
        Terminal::stdout(format_args!(
            "    - {:<32} {:<10} attempts={} exit={} finished={} duration={}",
            identifier, status, entry.attempts, exit_code, finished, duration
        ));
        if let Some(message) = entry.message.as_deref() {
            Terminal::stdout(format_args!("      message: {}", message));
        }
        if let Some(stdout) = entry.stdout.as_deref() {
            Terminal::stdout(format_args!("      stdout: {}", stdout));
        }
        if let Some(stderr) = entry.stderr.as_deref() {
            Terminal::stdout(format_args!("      stderr: {}", stderr));
        }
    }
}

fn format_history_status(status: &BindingHistoryStatus) -> &'static str {
    match status {
        BindingHistoryStatus::Pending => "PENDING",
        BindingHistoryStatus::Running => "RUNNING",
        BindingHistoryStatus::Succeeded => "SUCCEEDED",
        BindingHistoryStatus::Failed => "FAILED",
        BindingHistoryStatus::TimedOut => "TIMED_OUT",
    }
}
