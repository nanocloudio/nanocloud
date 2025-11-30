use super::*;
use crate::nanocloud::cni::network::nat::{ensure_nat_chain, ensure_nat_table};

pub(crate) struct BridgeRollback<'a> {
    runner: &'a dyn CommandRunner,
    bridge: &'a str,
    created: bool,
    active: bool,
}

impl<'a> BridgeRollback<'a> {
    pub(crate) fn new(runner: &'a dyn CommandRunner, bridge: &'a str) -> Self {
        Self {
            runner,
            bridge,
            created: false,
            active: true,
        }
    }

    pub(crate) fn mark_created(&mut self) {
        self.created = true;
    }

    pub(crate) fn disable(&mut self) {
        self.active = false;
    }
}

impl<'a> Drop for BridgeRollback<'a> {
    fn drop(&mut self) {
        if !self.active || !self.created {
            return;
        }

        let mut status = "ok".to_string();
        if let Err(err) = run_status(
            self.runner,
            "ip",
            &args(&["link", "delete", self.bridge]),
            "Failed to roll back bridge creation",
        )
        .and_then(|status| ensure_success(status, "Failed to delete bridge")) {
            status = err.to_string();
        }
        log_info(
            "cni",
            "Rolled back bridge interface",
            &[("bridge", self.bridge), ("status", status.as_str())],
        );
    }
}

pub(crate) fn bridge_with_runner(
    runner: &dyn CommandRunner,
    name: &str,
    cidr: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let overall_start = Instant::now();
    log_info(
        "cni",
        "Starting network bridge reconciliation",
        &[("bridge", name), ("cidr", cidr)],
    );

    // Interpret the provided CIDR as the desired bridge address plus prefix
    // and derive the network portion from it so the gateway is assigned correctly.
    let (gateway_str, prefix_str) = cidr
        .split_once('/')
        .ok_or_else(|| new_error(format!("Invalid CIDR format: {cidr}")))?;
    let gateway_str = gateway_str.trim();
    let prefix_str = prefix_str.trim();

    let gateway_ip: Ipv4Addr = gateway_str
        .parse()
        .map_err(|e| with_context(e, format!("Invalid gateway address: {gateway_str}")))?;
    let prefix: u8 = prefix_str
        .parse()
        .map_err(|e| with_context(e, format!("Invalid prefix length: {prefix_str}")))?;

    let mask = (!0u32) << (32 - prefix);
    let network_u32 = u32::from(gateway_ip) & mask;
    let network_ip = Ipv4Addr::from(network_u32);
    let network_cidr = format!("{}/{}", network_ip, prefix);

    let subnet = Subnet::new(&network_cidr, Some(gateway_str))?;
    let mut bridge_rollback = BridgeRollback::new(runner, name);

    // Create a bridge interface if it doesn't already exist
    let ensure_device_start = Instant::now();
    let mut bridge_created = false;
    let bridge_inspect = run_output(
        runner,
        "ip",
        &args(&["link", "show", name]),
        format!("Failed to inspect bridge {name}"),
    )?;
    if !bridge_inspect.status.success() {
        let status = run_status(
            runner,
            "ip",
            &args(&["link", "add", name, "type", "bridge"]),
            format!("Failed to run bridge creation for {name}"),
        )?;
        ensure_success(status, &format!("Failed to create bridge '{name}'"))?;
        bridge_created = true;
        bridge_rollback.mark_created();
    }
    let ensure_device_elapsed = ensure_device_start.elapsed().as_millis().to_string();
    log_info(
        "cni",
        "Bridge device ready",
        &[
            ("bridge", name),
            ("elapsed_ms", ensure_device_elapsed.as_str()),
            ("created", if bridge_created { "true" } else { "false" }),
        ],
    );
    let addr_config_start = Instant::now();
    let assigned_cidr = format!("{}/{}", subnet.gateway, subnet.mask);
    let addr_args = {
        let mut builder = ArgsBuilder::default();
        builder.extend(&["addr", "replace"]);
        builder.push(&assigned_cidr);
        builder.extend(&["dev", name]);
        builder.into_vec()
    };
    let addr_status = run_status(
        runner,
        "ip",
        &addr_args,
        format!("Failed to run address configuration for bridge {name}"),
    )?;
    ensure_success(
        addr_status,
        &format!("Failed to configure address on bridge '{name}'"),
    )?;
    let addr_elapsed = addr_config_start.elapsed().as_millis().to_string();
    log_info(
        "cni",
        "Bridge address configured",
        &[
            ("bridge", name),
            ("cidr", assigned_cidr.as_str()),
            ("elapsed_ms", addr_elapsed.as_str()),
        ],
    );

    let link_up_start = Instant::now();
    let link_up_status = run_status(
        runner,
        "ip",
        &args(&["link", "set", name, "up"]),
        format!("Failed to run link-up for bridge {name}"),
    )?;
    ensure_success(
        link_up_status,
        &format!("Failed to bring bridge '{name}' up"),
    )?;
    let link_elapsed = link_up_start.elapsed().as_millis().to_string();
    log_info(
        "cni",
        "Bridge link is up",
        &[("bridge", name), ("elapsed_ms", link_elapsed.as_str())],
    );

    // Ensure the NAT table exists
    let nat_table_start = Instant::now();
    let nat_table_exists = run_output(
        runner,
        "nft",
        &args(&["list", "table", "ip", "nat"]),
        "Failed to inspect nft nat table",
    )?
    .status
    .success();
    if !nat_table_exists {
        ensure_nat_table(runner)?;
    }
    let nat_table_elapsed = nat_table_start.elapsed().as_millis().to_string();
    log_info(
        "cni",
        "NAT table ready",
        &[
            ("bridge", name),
            ("elapsed_ms", nat_table_elapsed.as_str()),
            ("created", if nat_table_exists { "false" } else { "true" }),
        ],
    );

    // Ensure the postrouting chain exists
    let postrouting_start = Instant::now();
    let postrouting_chain_exists = run_output(
        runner,
        "nft",
        &args(&["list", "chain", "ip", "nat", "POSTROUTING"]),
        "Failed to inspect nft POSTROUTING chain",
    )?
    .status
    .success();
    if !postrouting_chain_exists {
        ensure_nat_chain(runner, "POSTROUTING", "postrouting", 100)?;
    }
    let postrouting_elapsed = postrouting_start.elapsed().as_millis().to_string();
    log_info(
        "cni",
        "POSTROUTING chain ready",
        &[
            ("bridge", name),
            ("elapsed_ms", postrouting_elapsed.as_str()),
            (
                "created",
                if postrouting_chain_exists {
                    "false"
                } else {
                    "true"
                },
            ),
        ],
    );

    // Add masquerade rule if it doesn't already exist
    let ruleset_dump_start = Instant::now();
    let ruleset = run_output(
        runner,
        "nft",
        &args(&["list", "ruleset"]),
        "Failed to list nft ruleset",
    )?;
    let ruleset_str = String::from_utf8_lossy(&ruleset.stdout);
    let ruleset_elapsed = ruleset_dump_start.elapsed().as_millis().to_string();
    let ruleset_bytes = ruleset.stdout.len().to_string();
    log_info(
        "cni",
        "Dumped nft ruleset",
        &[
            ("bridge", name),
            ("elapsed_ms", ruleset_elapsed.as_str()),
            ("bytes", ruleset_bytes.as_str()),
        ],
    );

    let masquerade_step = Instant::now();
    let mut masquerade_created = false;
    if !ruleset_str.contains(&format!(
        "saddr {}/{} oifname != \"{}\" masquerade",
        subnet.network, subnet.mask, name
    )) {
        let status = run_status(
            runner,
            "nft",
            &args(&[
                "add",
                "rule",
                "ip",
                "nat",
                "POSTROUTING",
                "ip",
                "saddr",
                &format!("{}/{}", subnet.network, subnet.mask),
                "oifname",
                "!=",
                name,
                "masquerade",
            ]),
            "Failed to create masquerade rule",
        )?;
        ensure_success(status, "Failed to ensure masquerade rule exists")?;
        masquerade_created = true;
    }
    let masquerade_elapsed = masquerade_step.elapsed().as_millis().to_string();
    log_info(
        "cni",
        "Masquerade rule ensured",
        &[
            ("bridge", name),
            ("elapsed_ms", masquerade_elapsed.as_str()),
            ("created", if masquerade_created { "true" } else { "false" }),
        ],
    );

    let hairpin_step = Instant::now();
    let mut hairpin_created = false;
    let hairpin_rule_snippet = format!("fib saddr type local oifname \"{}\"", name);
    if !ruleset_str.contains(&hairpin_rule_snippet) {
        let mut builder = ArgsBuilder::default();
        builder.extend(&[
            "add",
            "rule",
            "ip",
            "nat",
            "POSTROUTING",
            "fib",
            "saddr",
            "type",
            "local",
            "oifname",
        ]);
        builder.push(name);
        builder.extend(&["counter", "masquerade"]);
        let status = run_status(
            runner,
            "nft",
            &builder.into_vec(),
            "Failed to create hairpin masquerade rule",
        )?;
        ensure_success(status, "Failed to ensure hairpin masquerade rule exists")?;
        hairpin_created = true;
    }
    let hairpin_elapsed = hairpin_step.elapsed().as_millis().to_string();
    log_info(
        "cni",
        "Hairpin rule ensured",
        &[
            ("bridge", name),
            ("elapsed_ms", hairpin_elapsed.as_str()),
            ("created", if hairpin_created { "true" } else { "false" }),
        ],
    );

    let sysctl_start = Instant::now();
    ensure_sysctl_value("net.ipv4.conf.all.route_localnet", "1")?;
    ensure_sysctl_value(&format!("net.ipv4.conf.{}.route_localnet", name), "1")?;
    ensure_sysctl_value("net.ipv4.ip_forward", "1")?;
    let sysctl_elapsed = sysctl_start.elapsed().as_millis().to_string();
    log_info(
        "cni",
        "Sysctl forwarding configured",
        &[("bridge", name), ("elapsed_ms", sysctl_elapsed.as_str())],
    );

    let total_elapsed = overall_start.elapsed().as_millis().to_string();
    log_info(
        "cni",
        "Network bridge reconciliation complete",
        &[
            ("bridge", name),
            ("cidr", cidr),
            ("elapsed_ms", total_elapsed.as_str()),
        ],
    );

    bridge_rollback.disable();
    Ok(())
}
