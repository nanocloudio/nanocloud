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

use humantime::parse_duration;
use std::error::Error;

use crate::nanocloud::cli::args::DiagnosticsArgs;
use crate::nanocloud::cli::Terminal;
use crate::nanocloud::cni::{
    cni_plugin, CniContainerCleanup, CniReconciliationReport, NftRuleCleanup,
};
use crate::nanocloud::diagnostics::loopback::{
    run_loopback_probe, LoopbackProbeConfig, LoopbackProbeResult,
};

pub(super) async fn handle_diagnostics(
    args: &DiagnosticsArgs,
) -> Result<i32, Box<dyn Error + Send + Sync>> {
    let report = cni_plugin().reconcile_cni_artifacts()?;
    print_report(&report);

    let mut exit_code = 0;
    let loopback_enabled = if args.no_loopback {
        false
    } else {
        args.loopback
    };

    if loopback_enabled {
        let config = build_loopback_config(args)?;
        match run_loopback_probe(config.clone()).await {
            Ok(result) => {
                let outcome = summarize_loopback_outcome(&result);
                print_loopback_summary(&config.image, &result, &outcome);
                print_loopback_hints(&outcome);
                exit_code = exit_code.max(outcome.exit_code);
            }
            Err(err) => {
                Terminal::stderr(format_args!("Loopback probe failed: {}", err));
                exit_code = 2;
            }
        }
    } else {
        Terminal::stdout(format_args!(
            "Loopback probe skipped (enable with --loopback)."
        ));
    }

    Ok(exit_code)
}

fn print_report(report: &CniReconciliationReport) {
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
        Terminal::stdout(format_args!("No stale CNI artifacts detected."));
    } else {
        Terminal::stdout(format_args!(
            "Reconciled {} stale container{}; released {} IP{} and removed {} NAT rule{}.",
            stale_count,
            plural(stale_count),
            released_ip_count,
            plural(released_ip_count),
            removed_rule_count,
            plural(removed_rule_count),
        ));
        for cleanup in &report.stale_containers {
            print_cleanup(cleanup);
        }
    }

    if !report.warnings.is_empty() {
        for warning in &report.warnings {
            Terminal::stderr(format_args!("Warning: {}", warning));
        }
    }

    for cleanup in &report.stale_containers {
        for err in &cleanup.errors {
            Terminal::stderr(format_args!("Warning ({}): {}", cleanup.container_id, err));
        }
    }

    Terminal::stdout(format_args!(
        "You can rerun this reconciliation anytime with `nanocloud diagnostics`."
    ));
}

fn print_cleanup(cleanup: &CniContainerCleanup) {
    Terminal::stdout(format_args!("- {}", cleanup.container_id));

    let ips = if cleanup.released_ips.is_empty() {
        "none".to_string()
    } else {
        cleanup.released_ips.join(", ")
    };
    Terminal::stdout(format_args!("    released IPs: {}", ips));

    let host_if = cleanup
        .host_interface
        .as_deref()
        .unwrap_or("unknown (not recorded)");
    Terminal::stdout(format_args!(
        "    host interface: {} (present: {}, removed: {})",
        host_if,
        yes_no(cleanup.host_interface_was_present),
        yes_no(cleanup.host_interface_removed)
    ));

    let nat_rules = format_rule_list(&cleanup.removed_nat_rules);
    Terminal::stdout(format_args!("    NAT rules removed: {}", nat_rules));

    let port_forward_status = if cleanup.port_forward_entry_removed {
        "removed"
    } else if cleanup.had_port_forward_entry {
        "not removed (see warnings)"
    } else {
        "not present"
    };
    Terminal::stdout(format_args!(
        "    port-forward record: {}",
        port_forward_status
    ));
}

fn build_loopback_config(
    args: &DiagnosticsArgs,
) -> Result<LoopbackProbeConfig, Box<dyn Error + Send + Sync>> {
    let mut config = LoopbackProbeConfig::default();
    if let Some(image) = args.loopback_image.as_deref() {
        config.image = image.to_string();
    }
    if let Some(raw_timeout) = args.loopback_timeout.as_deref() {
        let duration = parse_duration(raw_timeout)
            .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
        config.timeout = duration;
    }
    Ok(config)
}

fn print_loopback_summary(image: &str, result: &LoopbackProbeResult, outcome: &LoopbackOutcome) {
    Terminal::stdout(format_args!("Loopback probe image: {}", image));
    if result.skipped {
        for note in &result.notes {
            Terminal::stdout(format_args!("Loopback probe note: {}", note));
        }
        Terminal::stdout(format_args!("  Summary: SKIPPED"));
        return;
    }
    Terminal::stdout(format_args!("  DNS check: {}", status_label(result.dns_ok)));
    Terminal::stdout(format_args!(
        "  Volume check: {}",
        status_label(result.volumes_ok)
    ));
    Terminal::stdout(format_args!("  Duration: {:?}", result.duration));
    if let Some(path) = result.log_path.as_deref() {
        Terminal::stdout(format_args!("  Logs: {}", path.display()));
    }
    if !result.notes.is_empty() {
        for note in &result.notes {
            Terminal::stdout(format_args!("  Note: {}", note));
        }
    }
    Terminal::stdout(format_args!(
        "  Summary: {}",
        if outcome.exit_code == 0 {
            "PASS"
        } else {
            "CHECK FAILURE (see hints)"
        }
    ));
}

fn status_label(ok: bool) -> &'static str {
    if ok {
        "OK"
    } else {
        "FAIL"
    }
}

fn format_rule_list(entries: &[NftRuleCleanup]) -> String {
    if entries.is_empty() {
        return "none".to_string();
    }
    entries
        .iter()
        .map(|rule| format!("{}:{}", rule.chain, rule.comment))
        .collect::<Vec<_>>()
        .join(", ")
}

fn plural(count: usize) -> &'static str {
    if count == 1 {
        ""
    } else {
        "s"
    }
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

#[derive(Debug, PartialEq, Eq)]
struct LoopbackOutcome {
    exit_code: i32,
    hints: Vec<&'static str>,
}

impl LoopbackOutcome {
    fn success() -> Self {
        Self {
            exit_code: 0,
            hints: Vec::new(),
        }
    }
}

const DNS_HINT: &str =
    "DNS check failed; confirm /etc/resolv.conf has reachable nameservers and nanocloud0 routes traffic.";
const VOLUME_HINT: &str =
    "Volume check failed; inspect CSI logs and ensure the loopback mount path is writable.";

fn summarize_loopback_outcome(result: &LoopbackProbeResult) -> LoopbackOutcome {
    if result.skipped {
        return LoopbackOutcome::success();
    }

    let mut outcome = LoopbackOutcome::success();
    if !result.dns_ok {
        outcome.exit_code = 1;
        outcome.hints.push(DNS_HINT);
    }
    if !result.volumes_ok {
        outcome.exit_code = 1;
        outcome.hints.push(VOLUME_HINT);
    }
    outcome
}

fn print_loopback_hints(outcome: &LoopbackOutcome) {
    if outcome.hints.is_empty() {
        return;
    }
    Terminal::stderr(format_args!("Loopback probe hints:"));
    for hint in &outcome.hints {
        Terminal::stderr(format_args!("  - {}", hint));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::diagnostics::loopback::LoopbackProbeResult;
    use std::time::Duration;

    fn base_result() -> LoopbackProbeResult {
        LoopbackProbeResult {
            dns_ok: true,
            volumes_ok: true,
            duration: Duration::from_secs(1),
            log_path: None,
            notes: Vec::new(),
            skipped: false,
        }
    }

    #[test]
    fn outcome_success_when_all_checks_pass() {
        let result = base_result();
        let outcome = summarize_loopback_outcome(&result);
        assert_eq!(
            outcome,
            LoopbackOutcome {
                exit_code: 0,
                hints: vec![]
            }
        );
    }

    #[test]
    fn outcome_sets_dns_hint() {
        let mut result = base_result();
        result.dns_ok = false;
        let outcome = summarize_loopback_outcome(&result);
        assert_eq!(
            outcome,
            LoopbackOutcome {
                exit_code: 1,
                hints: vec![DNS_HINT]
            }
        );
    }

    #[test]
    fn outcome_sets_volume_hint() {
        let mut result = base_result();
        result.volumes_ok = false;
        let outcome = summarize_loopback_outcome(&result);
        assert_eq!(
            outcome,
            LoopbackOutcome {
                exit_code: 1,
                hints: vec![VOLUME_HINT]
            }
        );
    }

    #[test]
    fn outcome_handles_both_failures() {
        let mut result = base_result();
        result.dns_ok = false;
        result.volumes_ok = false;
        let outcome = summarize_loopback_outcome(&result);
        assert_eq!(
            outcome,
            LoopbackOutcome {
                exit_code: 1,
                hints: vec![DNS_HINT, VOLUME_HINT]
            }
        );
    }
}
