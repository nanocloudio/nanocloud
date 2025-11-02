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

use crate::nanocloud::api::types::PodTable;
use crate::nanocloud::cli::Terminal;
use crate::nanocloud::k8s::pod::Pod;

pub(super) fn service_display_name(namespace: Option<&str>, service: &str) -> String {
    namespace
        .filter(|value| !value.is_empty())
        .map(|ns| format!("{ns}-{service}"))
        .unwrap_or_else(|| service.to_string())
}

pub(super) fn print_pod_state(namespace: Option<&str>, service: &str, pod: &Pod) {
    let display = pod
        .metadata
        .name
        .clone()
        .unwrap_or_else(|| service_display_name(namespace, service));
    Terminal::stdout(format_args!("Pod: {}", display));

    if let Some(status) = pod.status.as_ref() {
        let phase = status
            .phase
            .clone()
            .unwrap_or_else(|| "Unknown".to_string());
        Terminal::stdout(format_args!("  Phase: {}", phase));

        let pod_ip = status.pod_ip.clone().unwrap_or_else(|| "-".to_string());
        Terminal::stdout(format_args!("  Pod IP: {}", pod_ip));

        if let Some(start_time) = status.start_time.as_deref() {
            Terminal::stdout(format_args!(
                "  Started: {}",
                format_timestamp(Some(start_time))
            ));
        }

        let container_statuses = &status.container_statuses;
        if !container_statuses.is_empty() {
            let ready_count = container_statuses.iter().filter(|cs| cs.ready).count();
            let total = container_statuses.len();
            Terminal::stdout(format_args!(
                "  Containers: {}/{} ready",
                ready_count, total
            ));
            for cs in container_statuses {
                Terminal::stdout(format_args!(
                    "    - {}: ready={} restarts={}",
                    cs.name, cs.ready, cs.restart_count
                ));
            }
        }
    } else {
        Terminal::stdout(format_args!("  Phase: Unknown"));
    }

    if !pod.spec.containers.is_empty() {
        Terminal::stdout(format_args!("  Images:"));
        for container in &pod.spec.containers {
            let image = container
                .image
                .clone()
                .unwrap_or_else(|| "<unknown>".to_string());
            Terminal::stdout(format_args!("    - {}", image));
        }
    }
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
