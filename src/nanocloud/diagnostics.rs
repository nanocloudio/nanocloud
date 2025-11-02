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

pub mod loopback;

use std::error::Error;

use tokio::task;

use crate::nanocloud::cni::{cni_plugin, CniContainerCleanup, CniReconciliationReport};
use crate::nanocloud::logger::{log_info, log_warn};
use crate::nanocloud::util::error::with_context;

pub async fn reconcile_cni_artifacts_on_startup() -> Result<(), Box<dyn Error + Send + Sync>> {
    log_info("diagnostics", "Starting CNI artifact reconciliation", &[]);

    let plugin = cni_plugin();
    let report = task::spawn_blocking(move || plugin.reconcile_cni_artifacts())
        .await
        .map_err(|err| with_context(err, "Failed to join CNI reconciliation task"))??;

    log_report(&report);

    Ok(())
}

fn log_report(report: &CniReconciliationReport) {
    let stale_count = report.stale_containers.len();
    let released_ip_count: usize = report
        .stale_containers
        .iter()
        .map(|cleanup| cleanup.released_ips.len())
        .sum();
    let removed_rule_count: usize = report
        .stale_containers
        .iter()
        .map(|cleanup| cleanup.removed_nat_rules.len())
        .sum();

    if stale_count == 0 {
        log_info("diagnostics", "No stale CNI artifacts detected", &[]);
    } else {
        let metadata = vec![
            ("stale_containers".to_string(), stale_count.to_string()),
            ("released_ips".to_string(), released_ip_count.to_string()),
            (
                "removed_nat_rules".to_string(),
                removed_rule_count.to_string(),
            ),
        ];
        let metadata_refs = metadata_to_refs(&metadata);
        log_warn(
            "diagnostics",
            "Reconciled stale CNI artifacts",
            &metadata_refs,
        );
        for cleanup in &report.stale_containers {
            log_cleanup(cleanup);
        }
    }

    for warning in &report.warnings {
        log_warn("diagnostics", warning, &[]);
    }

    for cleanup in &report.stale_containers {
        for err in &cleanup.errors {
            let metadata = vec![("container_id".to_string(), cleanup.container_id.clone())];
            let metadata_refs = metadata_to_refs(&metadata);
            log_warn("diagnostics", err, &metadata_refs);
        }
    }

    log_info(
        "diagnostics",
        "Operators can rerun reconciliation via 'nanocloud diagnostics'",
        &[],
    );
}

fn log_cleanup(cleanup: &CniContainerCleanup) {
    let mut metadata = Vec::new();
    metadata.push(("container_id".to_string(), cleanup.container_id.clone()));
    let ip_list = if cleanup.released_ips.is_empty() {
        "none".to_string()
    } else {
        cleanup.released_ips.join(",")
    };
    metadata.push(("released_ips".to_string(), ip_list));
    metadata.push((
        "host_interface".to_string(),
        cleanup
            .host_interface
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
    ));
    metadata.push((
        "host_if_present".to_string(),
        cleanup.host_interface_was_present.to_string(),
    ));
    metadata.push((
        "host_if_removed".to_string(),
        cleanup.host_interface_removed.to_string(),
    ));
    metadata.push((
        "port_forward_present".to_string(),
        cleanup.had_port_forward_entry.to_string(),
    ));
    metadata.push((
        "port_forward_removed".to_string(),
        cleanup.port_forward_entry_removed.to_string(),
    ));
    metadata.push((
        "nat_rules_removed".to_string(),
        cleanup.removed_nat_rules.len().to_string(),
    ));
    if !cleanup.removed_nat_rules.is_empty() {
        let rule_list = cleanup
            .removed_nat_rules
            .iter()
            .map(|rule| format!("{}:{}", rule.chain, rule.comment))
            .collect::<Vec<_>>()
            .join(",");
        metadata.push(("nat_rule_comments".to_string(), rule_list));
    }

    let metadata_refs = metadata_to_refs(&metadata);
    log_info(
        "diagnostics",
        "CNI container cleanup applied",
        &metadata_refs,
    );
}

fn metadata_to_refs(metadata: &[(String, String)]) -> Vec<(&str, &str)> {
    metadata
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect()
}
