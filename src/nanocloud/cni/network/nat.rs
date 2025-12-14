use super::*;

pub(crate) fn runtime_mapping_to_rule(
    mapping: &RuntimePortMapping,
    container_ip: Ipv4Addr,
) -> Option<PortForwardRule> {
    if mapping.host_port == 0 || mapping.container_port == 0 {
        return None;
    }

    let protocol = mapping.protocol.as_deref().unwrap_or("tcp").to_lowercase();
    if protocol != "tcp" && protocol != "udp" {
        return None;
    }

    let host_ip = mapping
        .host_ip
        .as_ref()
        .map(|ip| ip.trim())
        .filter(|ip| !ip.is_empty())
        .map(|ip| if ip == "0.0.0.0" { "" } else { ip })
        .map(|ip| ip.to_string())
        .filter(|ip| !ip.is_empty());

    if let Some(ip) = host_ip.as_ref() {
        if ip.parse::<Ipv4Addr>().is_err() {
            return None;
        }
    }

    Some(PortForwardRule {
        host_ip,
        host_port: mapping.host_port,
        container_ip: container_ip.to_string(),
        container_port: mapping.container_port,
        protocol,
    })
}

pub(crate) fn configure_port_forwards(
    runner: &dyn CommandRunner,
    container_id: &str,
    bridge_name: &str,
    rules: Vec<PortForwardRule>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    clear_port_forwards(runner, container_id)?;
    if rules.is_empty() {
        return Ok(());
    }

    ensure_nat_table(runner)?;
    ensure_nat_chain(runner, "PREROUTING", "prerouting", -100)?;
    ensure_nat_chain(runner, "OUTPUT", "output", -100)?;

    let mut applied: Vec<StoredPortForward> = Vec::new();
    for (index, rule) in rules.into_iter().enumerate() {
        match add_port_forward_rule(runner, container_id, bridge_name, index, &rule) {
            Ok(entry) => applied.push(entry),
            Err(err) => {
                for entry in applied.iter().rev() {
                    let _ = remove_port_forward_entry(runner, entry);
                }
                return Err(err);
            }
        }
    }

    let payload = serde_json::to_string(&applied)
        .map_err(|e| with_context(e, "Failed to serialize port forward state"))?;
    CNI_KEYSPACE
        .put(&port_forward_path(container_id), &payload)
        .map_err(|e| with_context(e, "Failed to persist port forward state"))?;

    Ok(())
}

fn add_port_forward_rule(
    runner: &dyn CommandRunner,
    container_id: &str,
    bridge_name: &str,
    index: usize,
    rule: &PortForwardRule,
) -> Result<StoredPortForward, Box<dyn Error + Send + Sync>> {
    let base_comment = format!("nanocloud-{}-{}", container_id, index);
    let prerouting_comment = format!("{}-pr", base_comment);
    let output_comment = format!("{}-out", base_comment);

    apply_nat_rule(
        runner,
        container_id,
        "PREROUTING",
        Some(bridge_name),
        rule,
        &prerouting_comment,
    )?;
    apply_nat_rule(runner, container_id, "OUTPUT", None, rule, &output_comment)?;

    Ok(StoredPortForward {
        rule: rule.clone(),
        prerouting_comment,
        output_comment,
    })
}

fn apply_nat_rule(
    runner: &dyn CommandRunner,
    container_id: &str,
    chain: &str,
    bridge_name: Option<&str>,
    rule: &PortForwardRule,
    comment: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    validate_port_forward_rule(rule, bridge_name, comment)?;

    let mut builder = ArgsBuilder::default();
    builder.extend(&["add", "rule", "ip", "nat", chain]);

    if let Some(host_ip) = &rule.host_ip {
        builder.extend(&["ip", "daddr"]);
        builder.push(host_ip);
    } else {
        builder.extend(&["fib", "daddr", "type", "local"]);
    }

    if let Some(bridge) = bridge_name {
        builder.extend(&["iifname", "!="]);
        builder.push(bridge);
    }

    builder.push(&rule.protocol);
    builder.extend(&["dport"]);
    builder.push(rule.host_port.to_string());

    builder.extend(&["dnat", "to"]);
    builder.push(format!("{}:{}", rule.container_ip, rule.container_port));
    builder.extend(&["comment"]);
    builder.push(comment);

    ensure_success(
        run_status(
            runner,
            "nft",
            &builder.into_vec(),
            format!(
                "Failed to add {} nat rule for container {}",
                chain, container_id
            ),
        )?,
        &format!(
            "Failed to add {} nat rule for container {}",
            chain, container_id
        ),
    )?;

    Ok(())
}

pub(crate) fn clear_port_forwards(
    runner: &dyn CommandRunner,
    container_id: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key = port_forward_path(container_id);
    let stored = CNI_KEYSPACE.get_optional(&key).ok().flatten();
    let mut stored_err: Option<Box<dyn Error + Send + Sync>> = None;

    if let Some(stored) = stored {
        match serde_json::from_str::<Vec<StoredPortForward>>(&stored) {
            Ok(entries) => {
                for entry in &entries {
                    if let Err(err) =
                        validate_port_forward_rule(&entry.rule, None, &entry.prerouting_comment)
                            .and_then(|_| validate_token(&entry.output_comment, "nft comment"))
                    {
                        stored_err
                            .get_or_insert(with_context(err, "Invalid persisted port forward"));
                        continue;
                    }
                    remove_port_forward_entry(runner, entry)?;
                }
            }
            Err(err) => {
                stored_err = Some(with_context(
                    err,
                    "Failed to parse stored port forward state",
                ));
            }
        }
        if let Err(err) = CNI_KEYSPACE.delete(&key) {
            stored_err.get_or_insert(with_context(err, "Failed to delete port forward record"));
        }
    }

    let prefix = format!("nanocloud-{}-", container_id);
    delete_rules_by_comment_prefix(runner, "PREROUTING", &prefix)?;
    delete_rules_by_comment_prefix(runner, "OUTPUT", &prefix)?;

    if let Some(err) = stored_err {
        return Err(err);
    }

    Ok(())
}

fn remove_port_forward_entry(
    runner: &dyn CommandRunner,
    entry: &StoredPortForward,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    delete_rule_by_comment(runner, "PREROUTING", &entry.prerouting_comment)?;
    delete_rule_by_comment(runner, "OUTPUT", &entry.output_comment)?;
    Ok(())
}

fn delete_rule_by_comment(
    runner: &dyn CommandRunner,
    chain: &str,
    comment: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Some(handle) =
        list_nat_chain_rules(runner, chain)?
            .into_iter()
            .find_map(|(handle, rule_comment)| {
                if rule_comment.as_deref() == Some(comment) {
                    Some(handle)
                } else {
                    None
                }
            })
    {
        delete_rule_by_handle(runner, chain, handle)?;
    }

    Ok(())
}

fn delete_rules_by_comment_prefix(
    runner: &dyn CommandRunner,
    chain: &str,
    prefix: &str,
) -> DynResult<()> {
    let handles: Vec<u64> = list_nat_chain_rules(runner, chain)?
        .into_iter()
        .filter_map(|(handle, comment)| {
            comment
                .as_deref()
                .filter(|value| value.starts_with(prefix))
                .map(|_| handle)
        })
        .collect();

    for handle in handles {
        delete_rule_by_handle(runner, chain, handle)?;
    }

    Ok(())
}

pub(crate) fn list_nat_chain_rules(
    runner: &dyn CommandRunner,
    chain: &str,
) -> DynResult<Vec<NatRule>> {
    let output = run_output(
        runner,
        "nft",
        &args(&["-j", "list", "chain", "ip", "nat", chain]),
        format!("Failed to list nft chain {}", chain),
    )?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(new_error(format!(
            "Failed to inspect nft {} chain: {}",
            chain,
            stderr.trim()
        )));
    }

    let data: Value = serde_json::from_slice(&output.stdout)
        .map_err(|e| with_context(e, "Failed to parse nft ruleset JSON"))?;
    let nftables = data
        .get("nftables")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut rules = Vec::new();
    for entry in nftables {
        if let Some(rule_obj) = entry.get("rule") {
            if let Some(handle) = rule_obj.get("handle").and_then(Value::as_u64) {
                let comment = rule_obj
                    .get("comment")
                    .and_then(Value::as_str)
                    .map(|value| value.to_string());
                rules.push((handle, comment));
            }
            continue;
        }

        if let Some(chain_obj) = entry.get("chain") {
            if let Some(rule_array) = chain_obj.get("rules").and_then(Value::as_array) {
                for rule in rule_array {
                    if let Some(handle) = rule.get("handle").and_then(Value::as_u64) {
                        let comment = rule
                            .get("comment")
                            .and_then(Value::as_str)
                            .map(|value| value.to_string());
                        rules.push((handle, comment));
                    }
                }
            }
        }
    }

    Ok(rules)
}

pub(crate) fn delete_rule_by_handle(
    runner: &dyn CommandRunner,
    chain: &str,
    handle: u64,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    ensure_success(
        run_status(
            runner,
            "nft",
            &args(&[
                "delete",
                "rule",
                "ip",
                "nat",
                chain,
                "handle",
                &handle.to_string(),
            ]),
            format!(
                "Failed to execute delete for {} nat rule handle {}",
                chain, handle
            ),
        )?,
        &format!("Failed to delete {} nat rule with handle {}", chain, handle),
    )?;

    Ok(())
}

pub(crate) fn ensure_nat_table(
    runner: &dyn CommandRunner,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let nat_table_exists = run_output(
        runner,
        "nft",
        &args(&["list", "table", "ip", "nat"]),
        "Failed to check nft nat table",
    )?
    .status
    .success();
    if !nat_table_exists {
        ensure_success(
            run_status(
                runner,
                "nft",
                &args(&["add", "table", "ip", "nat"]),
                "Failed to create nft nat table",
            )?,
            "Failed to ensure nft nat table exists",
        )?;
    }
    Ok(())
}

pub(crate) fn ensure_nat_chain(
    runner: &dyn CommandRunner,
    chain: &str,
    hook: &str,
    priority: i32,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let chain_exists = run_output(
        runner,
        "nft",
        &args(&["list", "chain", "ip", "nat", chain]),
        format!("Failed to inspect nft chain {}", chain),
    )?
    .status
    .success();
    if !chain_exists {
        ensure_success(
            run_status(
                runner,
                "nft",
                &args(&[
                    "add",
                    "chain",
                    "ip",
                    "nat",
                    chain,
                    "{",
                    "type",
                    "nat",
                    "hook",
                    hook,
                    "priority",
                    &priority.to_string(),
                    ";",
                    "}",
                ]),
                format!("Failed to create nft chain {}", chain),
            )?,
            &format!("Failed to ensure nft {} chain exists", chain),
        )?;
    }
    Ok(())
}
