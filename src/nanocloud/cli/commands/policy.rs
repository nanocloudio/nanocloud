use std::error::Error;

use crate::nanocloud::api::client::NanocloudClient;
use crate::nanocloud::cli::args::{PolicyArgs, PolicyCommands};
use crate::nanocloud::cli::Terminal;

pub(super) async fn handle_policy(
    client: &NanocloudClient,
    args: &PolicyArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    match args.command {
        PolicyCommands::Debug => {
            let snapshot = client.network_policy_debug().await?;
            if snapshot.policies.is_empty() {
                Terminal::stdout(format_args!("Policies: none"));
            } else {
                Terminal::stdout(format_args!("Policies:"));
                for summary in &snapshot.policies {
                    let policy_types = if summary.policy_types.is_empty() {
                        let mut inferred = vec!["Ingress".to_string()];
                        if summary.egress_rules > 0 {
                            inferred.push("Egress".to_string());
                        }
                        inferred
                    } else {
                        summary.policy_types.clone()
                    };
                    let selector = if summary.pod_selector.is_empty() {
                        "<all>".to_string()
                    } else {
                        summary
                            .pod_selector
                            .iter()
                            .map(|(k, v)| format!("{k}={v}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    };
                    Terminal::stdout(format_args!(
                        "- {}/{} types [{}] selector [{}] ingress={} egress={}",
                        summary.namespace,
                        summary.name,
                        policy_types.join(", "),
                        selector,
                        summary.ingress_rules,
                        summary.egress_rules
                    ));
                }
            }

            if snapshot.chains.is_empty() {
                Terminal::stdout(format_args!("Chains: none"));
            } else {
                Terminal::stdout(format_args!("Chains:"));
                for chain in &snapshot.chains {
                    Terminal::stdout(format_args!(
                        "- {}/{} [{}] {} ({})",
                        chain.namespace, chain.pod, chain.direction, chain.pod_ip, chain.chain
                    ));
                    if chain.rules.is_empty() {
                        Terminal::stdout(format_args!("    (no allow rules; default drop)"));
                    } else {
                        for rule in &chain.rules {
                            let source = rule.source.as_deref().unwrap_or("<any>");
                            let protocol = rule.protocol.as_deref().unwrap_or("<any>");
                            match rule.port {
                                Some(port) => Terminal::stdout(format_args!(
                                    "    allow {source} proto={protocol} port={port}"
                                )),
                                None => Terminal::stdout(format_args!(
                                    "    allow {source} proto={protocol}"
                                )),
                            }
                        }
                    }
                }
            }
            Ok(())
        }
    }
}
