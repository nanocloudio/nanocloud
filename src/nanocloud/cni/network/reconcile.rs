use super::*;
use crate::nanocloud::cni::network::nat::{delete_rule_by_handle, list_nat_chain_rules};

enum CachedNatRules {
    Ok(Vec<NatRule>),
    Err(String),
}

struct NatRuleCache {
    rules: HashMap<String, CachedNatRules>,
}

impl NatRuleCache {
    fn load(runner: &dyn CommandRunner) -> Self {
        let mut rules = HashMap::new();
        for chain in ["PREROUTING", "OUTPUT"] {
            let entry = match list_nat_chain_rules(runner, chain) {
                Ok(rules) => CachedNatRules::Ok(rules),
                Err(err) => CachedNatRules::Err(err.to_string()),
            };
            rules.insert(chain.to_string(), entry);
        }
        let prerouting_count = rules
            .get("PREROUTING")
            .and_then(|value| match value {
                CachedNatRules::Ok(rules) => Some(rules.len()),
                CachedNatRules::Err(_) => None,
            })
            .unwrap_or(0)
            .to_string();
        let output_count = rules
            .get("OUTPUT")
            .and_then(|value| match value {
                CachedNatRules::Ok(rules) => Some(rules.len()),
                CachedNatRules::Err(_) => None,
            })
            .unwrap_or(0)
            .to_string();
        log_info(
            "cni",
            "Cached nat chains for reconciliation",
            &[
                ("prerouting_entries", prerouting_count.as_str()),
                ("output_entries", output_count.as_str()),
            ],
        );
        Self { rules }
    }

    fn rules_with_prefix(&self, chain: &str, prefix: &str) -> Result<Vec<NatRule>, String> {
        match self.rules.get(chain) {
            Some(CachedNatRules::Ok(rules)) => Ok(rules
                .iter()
                .filter(|(_, comment)| {
                    comment
                        .as_ref()
                        .map(|value| value.starts_with(prefix))
                        .unwrap_or(false)
                })
                .cloned()
                .collect()),
            Some(CachedNatRules::Err(err)) => Err(err.clone()),
            None => Err(format!("No cached rules for chain {chain}")),
        }
    }

    fn remove_handles(&mut self, chain: &str, handles: &[u64]) {
        if let Some(CachedNatRules::Ok(rules)) = self.rules.get_mut(chain) {
            let handle_set: HashSet<u64> = handles.iter().copied().collect();
            rules.retain(|(handle, _)| !handle_set.contains(handle));
        }
    }
}

pub(crate) fn reconcile(
    runner: &dyn CommandRunner,
    strict: bool,
) -> Result<CniReconciliationReport, Box<dyn Error + Send + Sync>> {
    let keyspace_root = Config::Keyspace.get_path().join("cni");
    let allocations_root = keyspace_root.join(ALLOCATIONS_PREFIX.trim_start_matches('/'));
    let ip_pool_root = keyspace_root.join(IP_POOL_PREFIX.trim_start_matches('/'));
    let port_forwards_root = keyspace_root.join(PORT_FORWARDS_PREFIX.trim_start_matches('/'));

    let netns_names = list_network_namespaces(runner)?;
    let veth_interfaces = list_veth_interfaces(runner)?;

    let mut report = CniReconciliationReport::default();

    let allocation_entries = read_keyspace_values(&allocations_root, &mut report.warnings)?;
    let ip_pool_entries = read_keyspace_values(&ip_pool_root, &mut report.warnings)?;
    let port_forward_entries = read_keyspace_values(&port_forwards_root, &mut report.warnings)?;

    let mut allocations: HashMap<String, AllocationRecord> = HashMap::new();
    for (container_id, raw) in allocation_entries {
        let mut record = AllocationRecord::default();
        let mut parts = raw.split_whitespace();
        record.ip = parts
            .next()
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string());
        if record.ip.is_none() {
            report.warnings.push(format!(
                "Allocation record for '{}' is missing an IP address",
                container_id
            ));
        }
        record.host_if = parts
            .next()
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string());
        allocations.insert(container_id, record);
    }

    let mut ip_pool_by_container: HashMap<String, Vec<String>> = HashMap::new();
    for (ip, container_id) in ip_pool_entries {
        if container_id.is_empty() {
            report
                .warnings
                .push(format!("IP pool entry '{}' has an empty container id", ip));
            continue;
        }
        ip_pool_by_container
            .entry(container_id.clone())
            .or_default()
            .push(ip);
    }

    let mut port_forward_containers: HashSet<String> = HashSet::new();
    for (container_id, value) in port_forward_entries {
        if value.is_empty() {
            report.warnings.push(format!(
                "Port-forward record for '{}' is empty; continuing cleanup",
                container_id
            ));
        }
        port_forward_containers.insert(container_id);
    }

    let mut container_ids: HashSet<String> = HashSet::new();
    container_ids.extend(allocations.keys().cloned());
    container_ids.extend(ip_pool_by_container.keys().cloned());
    container_ids.extend(port_forward_containers.iter().cloned());

    let stale_ids: Vec<String> = container_ids
        .into_iter()
        .filter(|id| !netns_names.contains(id))
        .collect();

    let mut nat_cache = NatRuleCache::load(runner);

    for container_id in stale_ids {
        let allocation = allocations.remove(&container_id);
        let had_port_forward_entry = port_forward_containers.remove(&container_id);
        let mut cleanup = CniContainerCleanup {
            container_id: container_id.clone(),
            released_ips: Vec::new(),
            removed_allocation: false,
            host_interface: allocation
                .as_ref()
                .and_then(|record| record.host_if.clone()),
            host_interface_was_present: false,
            host_interface_removed: false,
            had_port_forward_entry,
            port_forward_entry_removed: false,
            removed_nat_rules: Vec::new(),
            warnings: Vec::new(),
            errors: Vec::new(),
        };

        let mut ips = ip_pool_by_container
            .remove(&container_id)
            .unwrap_or_default();
        if let Some(record) = allocation.as_ref() {
            if let Some(ip) = record.ip.as_ref() {
                ips.push(ip.clone());
            }
        }

        let mut seen_ips = HashSet::new();
        let mut deduped_ips = Vec::new();
        for ip in ips {
            if seen_ips.insert(ip.clone()) {
                deduped_ips.push(ip);
            }
        }
        cleanup.released_ips = deduped_ips.clone();

        for ip in &deduped_ips {
            if let Err(err) = CNI_KEYSPACE.delete(&ip_pool_path(ip)) {
                if !is_missing_value_error(err.as_ref()) {
                    cleanup
                        .errors
                        .push(format!("Failed to delete ip-pool entry '{}': {}", ip, err));
                }
            }
        }

        let host_interface_name = cleanup
            .host_interface
            .clone()
            .unwrap_or_else(|| host_interface_name(&container_id));
        cleanup.host_interface = Some(host_interface_name.clone());
        cleanup.host_interface_was_present = veth_interfaces.contains(&host_interface_name);
        if let Err(err) = delete_link_if_exists(runner, &host_interface_name) {
            cleanup.errors.push(format!(
                "Failed to delete veth interface '{}': {}",
                host_interface_name, err
            ));
        } else if cleanup.host_interface_was_present {
            cleanup.host_interface_removed = true;
        }

        if allocation.is_some() {
            match CNI_KEYSPACE.delete(&allocation_path(&container_id)) {
                Ok(_) => {
                    cleanup.removed_allocation = true;
                }
                Err(err) => {
                    if !is_missing_value_error(err.as_ref()) {
                        cleanup
                            .errors
                            .push(format!("Failed to delete allocation record: {}", err));
                    }
                }
            }
        }

        let prefix = format!("nanocloud-{}-", container_id);
        for chain in ["PREROUTING", "OUTPUT"] {
            match nat_cache.rules_with_prefix(chain, &prefix) {
                Ok(rules) => {
                    let handles: Vec<u64> = rules.iter().map(|(handle, _)| *handle).collect();
                    for (handle, comment_opt) in rules {
                        let comment = comment_opt.unwrap_or_else(|| prefix.clone());
                        match delete_rule_by_handle(runner, chain, handle) {
                            Ok(()) => cleanup.removed_nat_rules.push(NftRuleCleanup {
                                chain: chain.to_string(),
                                comment,
                            }),
                            Err(err) => cleanup.errors.push(format!(
                                "Failed to delete nft rule '{}' in {}: {}",
                                comment, chain, err
                            )),
                        }
                    }
                    nat_cache.remove_handles(chain, &handles);
                }
                Err(err) => cleanup
                    .errors
                    .push(format!("Failed to inspect nft {} chain: {}", chain, err)),
            }
        }

        match CNI_KEYSPACE.delete(&port_forward_path(&container_id)) {
            Ok(_) => {
                cleanup.port_forward_entry_removed = true;
            }
            Err(err) => {
                if !is_missing_value_error(err.as_ref()) {
                    cleanup
                        .errors
                        .push(format!("Failed to delete port-forward record: {}", err));
                }
            }
        }

        report.errors.extend(cleanup.errors.clone());
        report.warnings.extend(cleanup.warnings.clone());
        report.stale_containers.push(cleanup);
    }

    if strict {
        let fatal_count = report.errors.len().saturating_add(
            report
                .stale_containers
                .iter()
                .map(|cleanup| cleanup.errors.len())
                .sum::<usize>(),
        );
        if fatal_count > 0 {
            return Err(new_error(format!(
                "Reconciliation encountered {} error(s) in strict mode",
                fatal_count
            )));
        }
    }

    Ok(report)
}
