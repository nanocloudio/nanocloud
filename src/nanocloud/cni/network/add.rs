use super::*;
use crate::nanocloud::cni::network::nat::{clear_port_forwards, configure_port_forwards};

pub(crate) fn add_with_runner<R: Read>(
    runner: &dyn CommandRunner,
    env: &HashMap<String, String>,
    input: R,
) -> Result<CniResult, Box<dyn Error + Send + Sync>> {
    let request = parse_add_env(env)?;
    let config = parse_cni_config(input)?;
    let (bridge_name, subnet) = desired_network(&config)?;

    let (ip, netns_created) = add(
        runner,
        &request.container_id,
        &request.netns,
        &request.ifname,
        &bridge_name,
        subnet.clone(),
    )?;
    let mut apply_guard = AddApplyGuard::new(runner, &request.container_id, &ip, netns_created);
    let port_forward_rules = build_port_forward_rules(&config, ip.addr);
    let result = CniResult {
        cni_version: config.cni_version.clone(),
        interfaces: vec![Interface {
            name: request.ifname.clone(),
            mac: ip.mac.to_string(),
            sandbox: request.netns.clone(),
        }],
        ips: vec![Ip {
            version: "4".to_string(),
            address: format!("{}/{}", ip.addr, ip.subnet.mask),
            gateway: ip.subnet.gateway.to_string(),
            interface: 0,
        }],
        routes: vec![Route {
            dst: "0.0.0.0/0".to_string(),
            gw: ip.subnet.gateway.to_string(),
        }],
    };
    let json = serde_json::to_string(&result)
        .map_err(|e| with_context(e, "Failed to serialize CNI result"))?;
    log_info("cni", "CNI operation result", &[("json", json.as_str())]);

    let allocation_record = format!(
        "{} {} {}",
        ip.addr,
        ip.host_if,
        if netns_created { "1" } else { "0" }
    );
    CNI_KEYSPACE
        .put(&allocation_path(&request.container_id), &allocation_record)
        .map_err(|e| with_context(e, "Failed to record IP allocation"))?;
    apply_guard.mark_allocation_recorded();

    configure_port_forwards(runner, &request.container_id, &bridge_name, port_forward_rules)?;

    apply_guard.disable();
    Ok(result)
}

pub(crate) fn delete_with_runner(
    runner: &dyn CommandRunner,
    env: &HashMap<String, String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let command = env
        .get("CNI_COMMAND")
        .ok_or_else(|| new_error("CNI_COMMAND not set"))?;
    let container_id = env
        .get("CNI_CONTAINERID")
        .ok_or_else(|| new_error("CNI_CONTAINERID not set"))?;

    if command.as_str() != "DEL" {
        return Err(new_error("CNI_COMMAND must be set to DEL"));
    }

    delete(runner, container_id)?;
    clear_port_forwards(runner, container_id)
}

pub(crate) fn add(
    runner: &dyn CommandRunner,
    container_id: &str,
    netns_path: &str,
    ifname: &str,
    bridge_name: &str,
    subnet: Subnet,
) -> Result<(IpAssignment, bool), Box<dyn Error + Send + Sync>> {
    let mut rollback = AddRollback::new(runner, container_id);
    let netns_created = ensure_namespace(runner, container_id, netns_path)?;
    if netns_created {
        rollback.netns_created();
    }
    let _netns_guard = NetnsLink::attach(container_id, netns_path)?;

    // Create veth pair on host
    let veth_host = host_interface_name(container_id);
    let veth_peer = peer_interface_name(&veth_host);
    rollback.host_if(&veth_host);
    delete_link_if_exists(runner, &veth_host)?;
    let status = run_status(
        runner,
        "ip",
        &args(&[
            "link", "add", &veth_host, "type", "veth", "peer", "name", &veth_peer,
        ]),
        "Failed to run veth pair creation command",
    )?;
    ensure_success(status, "Failed to create veth pair")?;

    // Move the container end into the netns
    let status = run_status(
        runner,
        "ip",
        &args(&["link", "set", &veth_peer, "netns", container_id]),
        "Failed to run veth namespace attach command",
    )?;
    ensure_success(status, "Failed to move veth peer into container namespace")?;

    // Attach host to bridge and bring up
    let status = run_status(
        runner,
        "ip",
        &args(&["link", "set", &veth_host, "master", bridge_name]),
        "Failed to run bridge attach command",
    )?;
    ensure_success(status, "Failed to connect veth host interface to bridge")?;
    let status = run_status(
        runner,
        "ip",
        &args(&["link", "set", &veth_host, "up"]),
        "Failed to run veth host interface up command",
    )?;
    ensure_success(status, "Failed to bring veth host interface up")?;

    // Retrieve MAC address and allocate IP address
    let mac_output = run_output(
        runner,
        "ip",
        &args(&[
            "netns",
            "exec",
            container_id,
            "cat",
            &format!("/sys/class/net/{}/address", &veth_peer),
        ]),
        "Failed to execute MAC address read command",
    )?;
    ensure_success(
        mac_output.status,
        "Failed to read MAC address from container namespace",
    )?;
    let mac: String = String::from_utf8(mac_output.stdout)
        .map_err(|e| with_context(e, "Failed to decode MAC address"))?
        .trim()
        .to_owned();
    let ip = CNI_KEYSPACE
        .put_first_fit(
            IP_POOL_PREFIX,
            container_id,
            |s| {
                s.parse::<Ipv4Addr>()
                    .map_err(|e| with_context(e, format!("Failed to parse IP address {}", s)))
            },
            subnet.iter(),
        )
        .and_then(|ip| {
            ip.parse::<Ipv4Addr>()
                .map(|addr| IpAssignment {
                    addr,
                    mac,
                    subnet: subnet.clone(),
                    host_if: veth_host.clone(),
                })
                .map_err(|_| new_error("Invalid IP address allocated"))
        })?;
    rollback.ip(ip.addr);

    // Finish configuration inside container netns
    let status = run_status(
        runner,
        "ip",
        &args(&[
            "netns",
            "exec",
            container_id,
            "ip",
            "link",
            "set",
            &veth_peer,
            "name",
            ifname,
        ]),
        "Failed to run veth rename inside container namespace",
    )?;
    ensure_success(
        status,
        "Failed to rename veth peer inside container namespace",
    )?;
    let status = run_status(
        runner,
        "ip",
        &args(&[
            "netns",
            "exec",
            container_id,
            "ip",
            "link",
            "set",
            "lo",
            "up",
        ]),
        "Failed to run loopback up command inside container namespace",
    )?;
    ensure_success(
        status,
        "Failed to bring loopback interface up inside container namespace",
    )?;
    let status = run_status(
        runner,
        "ip",
        &args(&[
            "netns",
            "exec",
            container_id,
            "ip",
            "link",
            "set",
            ifname,
            "up",
        ]),
        "Failed to run container interface up command",
    )?;
    ensure_success(status, "Failed to bring container interface up")?;
    let status = run_status(
        runner,
        "ip",
        &args(&[
            "netns",
            "exec",
            container_id,
            "ip",
            "addr",
            "add",
            &format!("{}/{}", ip.addr, ip.subnet.mask),
            "dev",
            ifname,
        ]),
        "Failed to run IP assignment inside container namespace",
    )?;
    ensure_success(
        status,
        "Failed to assign IP address inside container namespace",
    )?;
    let status = run_status(
        runner,
        "ip",
        &args(&[
            "netns",
            "exec",
            container_id,
            "ip",
            "route",
            "add",
            "default",
            "via",
            &ip.subnet.gateway.to_string(),
        ]),
        "Failed to run default route configuration inside container namespace",
    )?;
    ensure_success(
        status,
        "Failed to configure default route inside container namespace",
    )?;

    rollback.disable();
    Ok((ip, netns_created))
}

pub(crate) fn delete(
    runner: &dyn CommandRunner,
    container_id: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let allocation_key = allocation_path(container_id);
    let allocation = CNI_KEYSPACE.get(&allocation_key).ok();
    if let Some(allocation) = allocation {
        let mut parts = allocation.split_whitespace();
        let ip_address = parts.next().unwrap_or("").to_string();
        let host_if = parts.next().map(|s| s.to_string());
        let netns_flag = parts.next().map(|flag| flag == "1").unwrap_or(false);
        let link_name = host_if.unwrap_or_else(|| host_interface_name(container_id));
        let _ = delete_link_if_exists(runner, &link_name);
        if !ip_address.is_empty() {
            let _ = CNI_KEYSPACE.delete(&ip_pool_path(&ip_address));
        }
        let _ = CNI_KEYSPACE.delete(&allocation_key);
        if netns_flag {
            let _ = run_status(
                runner,
                "ip",
                &args(&["netns", "delete", container_id]),
                "Failed to delete container netns",
            );
        }
    } else {
        let ip_path = container_root_path(container_id)
            .join("network")
            .join("ip_address");
        if let Ok(mut file) = fs::File::open(&ip_path) {
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).map_err(|e| {
                with_context(
                    e,
                    format!("Failed to read legacy IP allocation {}", ip_path.display()),
                )
            })?;
            let ip_address = String::from_utf8_lossy(&buffer).trim().to_string();
            if !ip_address.is_empty() {
                let _ = CNI_KEYSPACE.delete(&ip_pool_path(&ip_address));
            }
        }
        let _ = delete_link_if_exists(runner, &host_interface_name(container_id));
        let _ = run_status(
            runner,
            "ip",
            &args(&["netns", "delete", container_id]),
            "Failed to delete container netns",
        );
    }

    Ok(())
}

pub(crate) struct AddRollback<'a> {
    runner: &'a dyn CommandRunner,
    container_id: &'a str,
    host_if: Option<String>,
    ip: Option<Ipv4Addr>,
    netns_created: bool,
    active: bool,
}

impl<'a> AddRollback<'a> {
    pub(crate) fn new(runner: &'a dyn CommandRunner, container_id: &'a str) -> Self {
        Self {
            runner,
            container_id,
            host_if: None,
            ip: None,
            netns_created: false,
            active: true,
        }
    }

    fn host_if(&mut self, name: &str) {
        self.host_if = Some(name.to_string());
    }

    fn ip(&mut self, ip: Ipv4Addr) {
        self.ip = Some(ip);
    }

    fn netns_created(&mut self) {
        self.netns_created = true;
    }

    fn disable(&mut self) {
        self.active = false;
    }
}

impl<'a> Drop for AddRollback<'a> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }

        if let Some(ip) = self.ip {
            let ip_str = ip.to_string();
            let (status, detail) =
                match CNI_KEYSPACE.delete(&ip_pool_path(&ip_str)).map(|_| ()) {
                    Ok(_) => ("ok".to_string(), String::new()),
                    Err(err) => ("error".to_string(), err.to_string()),
                };
            log_info(
                "cni",
                "Rolled back IP allocation",
                &[
                    ("container", self.container_id),
                    ("ip", ip_str.as_str()),
                    ("status", status.as_str()),
                    ("detail", detail.as_str()),
                ],
            );
        }

        if let Some(host_if) = self.host_if.as_ref() {
            let mut status = "ok".to_string();
            if let Err(err) = delete_link_if_exists(self.runner, host_if) {
                status = err.to_string();
            }
            log_info(
                "cni",
                "Rolled back host veth",
                &[
                    ("container", self.container_id),
                    ("interface", host_if.as_str()),
                    ("status", status.as_str()),
                ],
            );
        }

        if self.netns_created {
            let mut status = "ok".to_string();
            if let Err(err) = run_status(
                self.runner,
                "ip",
                &args(&["netns", "delete", self.container_id]),
                "Failed to roll back network namespace creation",
            )
            .and_then(|status| ensure_success(status, "Failed to remove container netns"))
            {
                status = err.to_string();
            }
            log_info(
                "cni",
                "Rolled back container netns",
                &[
                    ("container", self.container_id),
                    ("status", status.as_str()),
                ],
            );
        }
    }
}

pub(crate) struct AddApplyGuard<'a> {
    runner: &'a dyn CommandRunner,
    container_id: &'a str,
    host_if: String,
    ip: String,
    netns_created: bool,
    allocation_recorded: bool,
    active: bool,
}

impl<'a> AddApplyGuard<'a> {
    pub(crate) fn new(
        runner: &'a dyn CommandRunner,
        container_id: &'a str,
        assignment: &IpAssignment,
        netns_created: bool,
    ) -> Self {
        Self {
            runner,
            container_id,
            host_if: assignment.host_if.clone(),
            ip: assignment.addr.to_string(),
            netns_created,
            allocation_recorded: false,
            active: true,
        }
    }

    fn mark_allocation_recorded(&mut self) {
        self.allocation_recorded = true;
    }

    fn disable(&mut self) {
        self.active = false;
    }
}

impl<'a> Drop for AddApplyGuard<'a> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }

        let mut pf_status = "ok".to_string();
        if let Err(err) = clear_port_forwards(self.runner, self.container_id) {
            pf_status = err.to_string();
        }
        log_info(
            "cni",
            "Rolled back port forwards",
            &[
                ("container", self.container_id),
                ("status", pf_status.as_str()),
            ],
        );

        if self.allocation_recorded {
            let mut status = "ok".to_string();
            if let Err(err) = CNI_KEYSPACE.delete(&allocation_path(self.container_id)) {
                status = err.to_string();
            }
            log_info(
                "cni",
                "Rolled back allocation record",
                &[
                    ("container", self.container_id),
                    ("status", status.as_str()),
                ],
            );
        }

        let mut ip_status = "ok".to_string();
        if let Err(err) = CNI_KEYSPACE.delete(&ip_pool_path(&self.ip)) {
            ip_status = err.to_string();
        }
        log_info(
            "cni",
            "Rolled back IP pool entry",
            &[
                ("container", self.container_id),
                ("ip", self.ip.as_str()),
                ("status", ip_status.as_str()),
            ],
        );

        let mut veth_status = "ok".to_string();
        if let Err(err) = delete_link_if_exists(self.runner, &self.host_if) {
            veth_status = err.to_string();
        }
        log_info(
            "cni",
            "Rolled back veth interface",
            &[
                ("container", self.container_id),
                ("interface", self.host_if.as_str()),
                ("status", veth_status.as_str()),
            ],
        );

        if self.netns_created {
            let mut status = "ok".to_string();
            if let Err(err) = run_status(
                self.runner,
                "ip",
                &args(&["netns", "delete", self.container_id]),
                "Failed to roll back network namespace creation",
            )
            .and_then(|status| ensure_success(status, "Failed to remove container netns"))
            {
                status = err.to_string();
            }
            log_info(
                "cni",
                "Rolled back container netns",
                &[
                    ("container", self.container_id),
                    ("status", status.as_str()),
                ],
            );
        }
    }
}

fn ensure_namespace(
    runner: &dyn CommandRunner,
    container_id: &str,
    netns_path: &str,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let path = Path::new(netns_path);
    if path.exists() {
        return Ok(false);
    }

    let run_dir = netns_dir();
    if path.starts_with(&run_dir) {
        fs::create_dir_all(&run_dir).map_err(|e| {
            with_context(
                e,
                format!("Failed to create netns run directory {}", run_dir.display()),
            )
        })?;
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| new_error(format!("Invalid network namespace path: {}", netns_path)))?;

        if name != container_id {
            return Err(new_error(format!(
                "Network namespace {} does not exist and name does not match container id",
                netns_path
            )));
        }

        ensure_success(
            run_status(
                runner,
                "ip",
                &args(&["netns", "add", name]),
                format!("Failed to execute netns add for {}", name),
            )?,
            &format!("Failed to create network namespace {}", name),
        )?;

        if path.exists() {
            return Ok(true);
        }
    }

    Err(new_error(format!(
        "Network namespace {} does not exist",
        netns_path
    )))
}

struct NetnsLink {
    path: std::path::PathBuf,
    created: bool,
}

impl NetnsLink {
    fn attach(name: &str, netns_path: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let netns = std::path::Path::new(netns_path);
        if !netns.exists() {
            return Err(new_error(format!(
                "Network namespace {} does not exist",
                netns_path
            )));
        }
        let run_dir = netns_dir();
        fs::create_dir_all(&run_dir).map_err(|e| {
            with_context(
                e,
                format!("Failed to create netns run directory {}", run_dir.display()),
            )
        })?;
        let link_path = run_dir.join(name);
        if netns == link_path {
            return Ok(NetnsLink {
                path: link_path,
                created: false,
            });
        }
        let mut created = false;
        match fs::read_link(&link_path) {
            Ok(existing) if existing == netns => {}
            Ok(_) => {
                fs::remove_file(&link_path).map_err(|e| {
                    with_context(
                        e,
                        format!(
                            "Failed to remove existing netns link {}",
                            link_path.display()
                        ),
                    )
                })?;
                symlink(netns, &link_path).map_err(|e| {
                    with_context(
                        e,
                        format!(
                            "Failed to create netns symlink {} -> {}",
                            link_path.display(),
                            netns.display()
                        ),
                    )
                })?;
                created = true;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                symlink(netns, &link_path).map_err(|e| {
                    with_context(
                        e,
                        format!(
                            "Failed to create netns symlink {} -> {}",
                            link_path.display(),
                            netns.display()
                        ),
                    )
                })?;
                created = true;
            }
            Err(err) => {
                return Err(with_context(
                    err,
                    format!("Failed to inspect netns link {}", link_path.display()),
                ))
            }
        }
        Ok(NetnsLink {
            path: link_path,
            created,
        })
    }
}

impl Drop for NetnsLink {
    fn drop(&mut self) {
        if self.created {
            let _ = fs::remove_file(&self.path);
        }
    }
}
