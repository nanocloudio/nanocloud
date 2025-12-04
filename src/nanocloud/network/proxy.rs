#![allow(dead_code)]

use crate::nanocloud::k8s::endpoints::Endpoints;
use crate::nanocloud::k8s::service::{Service, ServicePort};
use crate::nanocloud::observability::metrics::{self, ProxyOperation};
use crate::nanocloud::util::error::{new_error, with_context};

use log::{debug, info};
use sha1::{Digest, Sha1};
use std::env;
use std::error::Error;
use std::fmt::{self, Write as _};
use std::fs::OpenOptions;
use std::io::Write;
use std::net::IpAddr;
use std::process::{Command, Stdio};

const PRIMARY_CHAIN: &str = "NCLD-SERVICES";

type AnyError = Box<dyn Error + Send + Sync>;

/// Structured errors for proxy programming operations.
#[derive(Debug)]
pub enum ProxyError {
    Validation { target: String, reason: String },
    Command { command: String, source: AnyError },
    Io { context: String, source: AnyError },
}

impl ProxyError {
    fn validation(target: impl Into<String>, reason: impl Into<String>) -> Self {
        ProxyError::Validation {
            target: target.into(),
            reason: reason.into(),
        }
    }

    fn command(command: impl Into<String>, source: AnyError) -> Self {
        ProxyError::Command {
            command: command.into(),
            source,
        }
    }

    fn io(context: impl Into<String>, source: AnyError) -> Self {
        ProxyError::Io {
            context: context.into(),
            source,
        }
    }
}

impl fmt::Display for ProxyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProxyError::Validation { target, reason } => {
                write!(f, "invalid proxy input for {}: {}", target, reason)
            }
            ProxyError::Command { command, source } => {
                write!(f, "proxy command `{}` failed: {}", command, source)
            }
            ProxyError::Io { context, source } => write!(f, "{}: {}", context, source),
        }
    }
}

impl Error for ProxyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ProxyError::Validation { .. } => None,
            ProxyError::Command { source, .. } => Some(source.as_ref()),
            ProxyError::Io { source, .. } => Some(source.as_ref()),
        }
    }
}

/// Convenient alias for proxy operation results.
pub type ProxyResult<T> = Result<T, ProxyError>;

fn validate_ip(address: &str) -> ProxyResult<IpAddr> {
    address.parse::<IpAddr>().map_err(|err| {
        ProxyError::validation(address.to_string(), format!("invalid IP address: {}", err))
    })
}

fn validate_service_port(port: &ServicePort) -> ProxyResult<()> {
    if port.port == 0 {
        return Err(ProxyError::validation(
            port.name.clone().unwrap_or_else(|| "port".to_string()),
            "service port must be greater than zero",
        ));
    }
    if let Some(target_port) = port.target_port {
        if target_port == 0 {
            return Err(ProxyError::validation(
                port.name
                    .clone()
                    .unwrap_or_else(|| "targetPort".to_string()),
                "targetPort must be greater than zero",
            ));
        }
    }
    if let Some(protocol) = port.protocol.as_deref() {
        if !protocol.eq_ignore_ascii_case("tcp") {
            return Err(ProxyError::validation(
                port.name.clone().unwrap_or_else(|| "protocol".to_string()),
                "only TCP services are supported",
            ));
        }
    }
    Ok(())
}

fn validate_service(service: &Service) -> ProxyResult<&str> {
    let namespace = service.metadata.namespace.as_deref().unwrap_or("default");
    let name = service.metadata.name.as_deref().unwrap_or("service");
    let ports = service_ports(service);
    if ports.is_empty() {
        return Err(ProxyError::validation(
            format!("{}/{}", namespace, name),
            "service has no ports to program",
        ));
    }
    for port in ports.iter() {
        validate_service_port(port)?;
    }
    let cluster_ip = cluster_ip(service)?;
    validate_ip(cluster_ip)?;
    Ok(cluster_ip)
}

fn validate_endpoints(endpoints: &Endpoints) -> ProxyResult<usize> {
    let mut valid = 0;
    for subset in &endpoints.subsets {
        for address in &subset.addresses {
            validate_ip(&address.ip)?;
            valid += 1;
        }
    }
    Ok(valid)
}

/// Programs proxy rules for a service and its endpoints.
pub fn program_service(service: &Service, endpoints: &Endpoints) -> ProxyResult<()> {
    let namespace = service.metadata.namespace.as_deref();
    let service_name = service.metadata.name.as_deref().unwrap_or("service");
    metrics::observe_proxy_operation(namespace, service_name, ProxyOperation::Program, || {
        program_service_inner(service, endpoints)
    })
}

fn program_service_inner(service: &Service, endpoints: &Endpoints) -> ProxyResult<()> {
    let cluster_ip = validate_service(service)?;
    let endpoints_count = validate_endpoints(endpoints)?;
    info!(
        "Programming proxy rules for {}/{} with {} endpoint(s)",
        service.metadata.namespace.as_deref().unwrap_or("default"),
        service.metadata.name.as_deref().unwrap_or("service"),
        endpoints_count
    );
    let runner = CommandRunner::new();
    runner.ensure_primary_chain()?;

    let ports = service_ports(service);
    for port in ports.iter() {
        program_port(&runner, service, cluster_ip, port, endpoints)?;
    }
    Ok(())
}

/// Removes proxy rules for a service.
pub fn remove_service(service: &Service) -> ProxyResult<()> {
    let namespace = service.metadata.namespace.as_deref();
    let service_name = service.metadata.name.as_deref().unwrap_or("service");
    metrics::observe_proxy_operation(namespace, service_name, ProxyOperation::Remove, || {
        remove_service_inner(service)
    })
}

fn remove_service_inner(service: &Service) -> ProxyResult<()> {
    let cluster_ip = validate_service(service)?;
    info!(
        "Removing proxy rules for {}/{}",
        service.metadata.namespace.as_deref().unwrap_or("default"),
        service.metadata.name.as_deref().unwrap_or("service"),
    );
    let runner = CommandRunner::new();
    let ports = service_ports(service);
    for port in ports {
        let chain = chain_name(service, port.port);
        runner.remove_service_rule(&chain, cluster_ip, port.port)?;
        runner.delete_chain(&chain)?;
    }
    Ok(())
}

fn program_port(
    runner: &CommandRunner,
    service: &Service,
    cluster_ip: &str,
    port: &ServicePort,
    endpoints: &Endpoints,
) -> ProxyResult<()> {
    let chain = chain_name(service, port.port);
    runner.ensure_chain(&chain)?;
    runner.clear_chain(&chain)?;
    runner.remove_service_rule(&chain, cluster_ip, port.port)?;
    runner.install_service_rule(&chain, cluster_ip, port.port)?;

    let addresses = collect_addresses(endpoints);
    debug!(
        "Programming proxy chain {} for port {} with {} target(s)",
        chain,
        port.port,
        addresses.len()
    );
    if addresses.is_empty() {
        return Ok(());
    }
    let target_port = port.target_port.unwrap_or(port.port);
    add_endpoint_rules(runner, &chain, &addresses, target_port)?;
    Ok(())
}

fn add_endpoint_rules(
    runner: &CommandRunner,
    chain: &str,
    addresses: &[String],
    target_port: u16,
) -> ProxyResult<()> {
    let total = addresses.len();
    for (idx, address) in addresses.iter().enumerate() {
        let mut args: Vec<String> = vec![
            "-w".to_string(),
            "-t".to_string(),
            "nat".to_string(),
            "-A".to_string(),
            chain.to_string(),
        ];
        if idx + 1 != total {
            let remaining = (total - idx) as f64;
            let probability = 1.0f64 / remaining;
            args.push("-m".to_string());
            args.push("statistic".to_string());
            args.push("--mode".to_string());
            args.push("random".to_string());
            args.push("--probability".to_string());
            let mut prob = String::new();
            write!(&mut prob, "{:.6}", probability).ok();
            args.push(prob);
        }
        args.push("-j".to_string());
        args.push("DNAT".to_string());
        args.push("--to-destination".to_string());
        args.push(format!("{}:{}", address, target_port));
        runner.run(args)?;
    }
    Ok(())
}

fn collect_addresses(endpoints: &Endpoints) -> Vec<String> {
    let mut result = Vec::new();
    for subset in &endpoints.subsets {
        for address in &subset.addresses {
            result.push(address.ip.clone());
        }
    }
    result
}

fn cluster_ip(service: &Service) -> ProxyResult<&str> {
    let namespace = service.metadata.namespace.as_deref().unwrap_or("default");
    let name = service.metadata.name.as_deref().unwrap_or("service");
    service
        .status
        .as_ref()
        .and_then(|status| status.cluster_ip.as_deref())
        .ok_or_else(|| {
            ProxyError::validation(
                format!("{}/{}", namespace, name),
                "service missing ClusterIP",
            )
        })
}

fn service_ports(service: &Service) -> Vec<ServicePort> {
    if service.spec.ports.is_empty() {
        Vec::new()
    } else {
        service.spec.ports.clone()
    }
}

/// Deterministically derives the iptables chain name for a service port.
fn chain_name(service: &Service, port: u16) -> String {
    let namespace = service.metadata.namespace.as_deref().unwrap_or("default");
    let name = service.metadata.name.as_deref().unwrap_or("service");
    let mut hasher = Sha1::new();
    hasher.update(namespace.as_bytes());
    hasher.update(b"/");
    hasher.update(name.as_bytes());
    hasher.update(b":");
    hasher.update(port.to_string().as_bytes());
    let digest = hex::encode(hasher.finalize());
    format!("NCLD-{}", &digest[..12]).to_uppercase()
}

struct CommandRunner {
    binary: String,
    record_path: Option<String>,
}

impl CommandRunner {
    fn new() -> Self {
        let binary = env::var("NANOCLOUD_IPTABLES").unwrap_or_else(|_| "iptables".to_string());
        let record_path = env::var("NANOCLOUD_IPTABLES_RECORD").ok();
        CommandRunner {
            binary,
            record_path,
        }
    }

    fn health_check(&self) -> ProxyResult<()> {
        if let Some(record_path) = self.record_path.as_ref() {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(record_path)
                .map_err(|e| ProxyError::io("Failed to open iptables record log", Box::new(e)))?;
            writeln!(file, "{} --version", self.binary)
                .map_err(|e| ProxyError::io("Failed to write iptables record", Box::new(e)))?;
            return Ok(());
        }

        let command_line = format!("{} --version", self.binary);
        let status = Command::new(&self.binary)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|e| {
                ProxyError::command(
                    command_line.clone(),
                    with_context(e, format!("Failed to execute {}", self.binary)),
                )
            })?;
        if status.success() {
            Ok(())
        } else {
            let descriptor = status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "terminated by signal".to_string());
            Err(ProxyError::command(
                command_line,
                new_error(format!("exited with status {}", descriptor)),
            ))
        }
    }

    fn ensure_primary_chain(&self) -> ProxyResult<()> {
        if !self.run(["-w", "-t", "nat", "-N", PRIMARY_CHAIN])? {
            self.run(["-w", "-t", "nat", "-F", PRIMARY_CHAIN])?;
        }
        self.ensure_global_jump("PREROUTING")?;
        self.ensure_global_jump("OUTPUT")?;
        Ok(())
    }

    fn ensure_global_jump(&self, source: &str) -> ProxyResult<()> {
        let check = self.run(["-w", "-t", "nat", "-C", source, "-j", PRIMARY_CHAIN])?;
        if !check {
            self.run(["-w", "-t", "nat", "-A", source, "-j", PRIMARY_CHAIN])?;
        }
        Ok(())
    }

    fn ensure_chain(&self, chain: &str) -> ProxyResult<()> {
        if !self.run(["-w", "-t", "nat", "-N", chain])? {
            self.run(["-w", "-t", "nat", "-F", chain])?;
        }
        Ok(())
    }

    fn clear_chain(&self, chain: &str) -> ProxyResult<()> {
        self.run(["-w", "-t", "nat", "-F", chain])?;
        Ok(())
    }

    fn delete_chain(&self, chain: &str) -> ProxyResult<()> {
        self.run(["-w", "-t", "nat", "-F", chain])?;
        self.run(["-w", "-t", "nat", "-X", chain])?;
        Ok(())
    }

    fn remove_service_rule(&self, chain: &str, cluster_ip: &str, port: u16) -> ProxyResult<()> {
        loop {
            let args = vec![
                "-w".to_string(),
                "-t".to_string(),
                "nat".to_string(),
                "-D".to_string(),
                PRIMARY_CHAIN.to_string(),
                "-d".to_string(),
                format!("{}/32", cluster_ip),
                "-p".to_string(),
                "tcp".to_string(),
                "--dport".to_string(),
                port.to_string(),
                "-j".to_string(),
                chain.to_string(),
            ];
            if !self.run(args)? {
                break;
            }
        }
        Ok(())
    }

    fn install_service_rule(&self, chain: &str, cluster_ip: &str, port: u16) -> ProxyResult<()> {
        let args = vec![
            "-w".to_string(),
            "-t".to_string(),
            "nat".to_string(),
            "-A".to_string(),
            PRIMARY_CHAIN.to_string(),
            "-d".to_string(),
            format!("{}/32", cluster_ip),
            "-p".to_string(),
            "tcp".to_string(),
            "--dport".to_string(),
            port.to_string(),
            "-j".to_string(),
            chain.to_string(),
        ];
        self.run(args)?;
        Ok(())
    }

    fn run<I, S>(&self, args: I) -> ProxyResult<bool>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let args_vec: Vec<String> = args.into_iter().map(|s| s.as_ref().to_string()).collect();
        let command_line = format!("{} {}", self.binary, args_vec.join(" "));
        if let Some(ref record_path) = self.record_path {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(record_path)
                .map_err(|e| ProxyError::io("Failed to open iptables record log", Box::new(e)))?;
            writeln!(file, "{}", command_line)
                .map_err(|e| ProxyError::io("Failed to write iptables record", Box::new(e)))?;
            let is_delete = args_vec.iter().any(|arg| arg == "-D");
            return Ok(!is_delete);
        }

        let output = Command::new(&self.binary)
            .args(&args_vec)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .map_err(|e| {
                ProxyError::command(
                    command_line.clone(),
                    with_context(e, format!("Failed to execute {}", self.binary)),
                )
            })?;
        if output.status.success() {
            return Ok(true);
        }

        let stderr = String::from_utf8_lossy(&output.stderr);
        let exit_code = output.status.code();
        let is_check = args_vec.iter().any(|arg| arg == "-C");
        let is_new_chain = args_vec.iter().any(|arg| arg == "-N");
        let is_flush = args_vec.iter().any(|arg| arg == "-F");
        let is_delete_chain = args_vec.iter().any(|arg| arg == "-X");
        let is_delete_rule = args_vec.iter().any(|arg| arg == "-D");

        if is_check && exit_code == Some(1) {
            return Ok(false);
        }
        if is_new_chain && stderr.contains("Chain already exists") {
            return Ok(false);
        }
        if (is_flush || is_delete_chain || is_delete_rule)
            && stderr.contains("No chain/target/match by that name")
        {
            return Ok(false);
        }
        if is_delete_rule && stderr.contains("Bad rule") {
            return Ok(false);
        }

        Err(ProxyError::command(
            command_line,
            with_context(
                new_error(format!(
                    "{} exited with status {:?}: {}",
                    self.binary,
                    exit_code,
                    stderr.trim()
                )),
                "iptables command failed",
            ),
        ))
    }
}

/// Verifies iptables is reachable or records a dry-run log entry.
pub fn health_check() -> ProxyResult<()> {
    CommandRunner::new().health_check()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::endpoints::{EndpointAddress, EndpointSubset};
    use crate::nanocloud::k8s::pod::ObjectMeta;
    use crate::nanocloud::network::policy::{
        PolicyChain, PolicyDirection, PolicyProgrammer, PolicyRule,
    };
    use crate::nanocloud::test_support::keyspace_lock;
    use serial_test::serial;
    use std::env;
    use std::fs;
    use std::thread;
    use tempfile::tempdir;

    fn make_service() -> Service {
        Service {
            metadata: ObjectMeta {
                name: Some("svc".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: crate::nanocloud::k8s::service::ServiceSpec {
                ports: vec![ServicePort {
                    name: Some("http".to_string()),
                    port: 80,
                    target_port: Some(8080),
                    protocol: Some("TCP".to_string()),
                }],
                ..Default::default()
            },
            status: Some(crate::nanocloud::k8s::service::ServiceStatus {
                cluster_ip: Some("10.203.0.12".to_string()),
            }),
            ..Default::default()
        }
    }

    fn make_endpoints() -> Endpoints {
        Endpoints {
            metadata: ObjectMeta {
                name: Some("svc".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            subsets: vec![EndpointSubset {
                addresses: vec![
                    EndpointAddress {
                        ip: "10.1.0.30".to_string(),
                    },
                    EndpointAddress {
                        ip: "10.1.0.31".to_string(),
                    },
                ],
                ports: Vec::new(),
            }],
            ..Default::default()
        }
    }

    fn restore_env(key: &str, previous: Option<String>) {
        if let Some(value) = previous {
            env::set_var(key, value);
        } else {
            env::remove_var(key);
        }
    }

    #[test]
    #[serial]
    fn proxy_writes_expected_commands() {
        let dir = tempdir().expect("tempdir");
        let log_path = dir.path().join("iptables.log");
        env::set_var("NANOCLOUD_IPTABLES_RECORD", &log_path);
        env::set_var("NANOCLOUD_IPTABLES", "/sbin/iptables");

        let service = make_service();
        let endpoints = make_endpoints();
        program_service(&service, &endpoints).expect("program service");

        let log = std::fs::read_to_string(&log_path).expect("read log");
        assert!(log.contains("-A NCLD-SERVICES"));
        assert!(log.contains("DNAT --to-destination 10.1.0.30:8080"));
        assert!(log.contains("DNAT --to-destination 10.1.0.31:8080"));

        remove_service(&service).expect("remove service");
        let removal_log = std::fs::read_to_string(&log_path).expect("read removal log");
        assert!(removal_log.contains("-D NCLD-SERVICES"));

        env::remove_var("NANOCLOUD_IPTABLES_RECORD");
        env::remove_var("NANOCLOUD_IPTABLES");
    }

    #[test]
    #[serial]
    fn health_check_writes_to_record_log() {
        let dir = tempdir().expect("tempdir");
        let log_path = dir.path().join("iptables.log");
        env::set_var("NANOCLOUD_IPTABLES_RECORD", &log_path);
        env::set_var("NANOCLOUD_IPTABLES", "/sbin/iptables");

        health_check().expect("health check should succeed");

        let log = std::fs::read_to_string(&log_path).expect("read health log");
        assert!(log.contains("--version"));

        env::remove_var("NANOCLOUD_IPTABLES_RECORD");
        env::remove_var("NANOCLOUD_IPTABLES");
    }

    #[test]
    #[serial]
    fn rejects_service_without_cluster_ip() {
        let mut service = make_service();
        service.status = None;
        let endpoints = make_endpoints();

        let result = program_service(&service, &endpoints);
        assert!(
            matches!(result, Err(ProxyError::Validation { .. })),
            "expected validation error for missing ClusterIP"
        );
    }

    #[test]
    #[serial]
    fn proxy_handles_empty_endpoints() {
        let dir = tempdir().expect("tempdir");
        let log_path = dir.path().join("iptables-empty.log");
        env::set_var("NANOCLOUD_IPTABLES_RECORD", &log_path);
        env::set_var("NANOCLOUD_IPTABLES", "/sbin/iptables");

        let service = make_service();
        let endpoints = Endpoints {
            subsets: Vec::new(),
            ..Default::default()
        };

        program_service(&service, &endpoints).expect("program service without endpoints");

        let log = fs::read_to_string(&log_path).expect("read log");
        assert!(
            log.contains("-A NCLD-SERVICES"),
            "expected service chain install: {log}"
        );
        assert!(
            !log.contains("DNAT --to-destination"),
            "no DNAT rules expected when endpoints are empty: {log}"
        );

        remove_service(&service).expect("remove service");
        env::remove_var("NANOCLOUD_IPTABLES_RECORD");
        env::remove_var("NANOCLOUD_IPTABLES");
    }

    #[test]
    #[serial]
    fn proxy_operations_are_thread_safe() {
        let _guard = keyspace_lock().lock();
        let dir = tempdir().expect("tempdir");
        let log_path = dir.path().join("iptables-concurrent.log");
        let previous_record = env::var("NANOCLOUD_IPTABLES_RECORD").ok();
        let previous_binary = env::var("NANOCLOUD_IPTABLES").ok();
        env::set_var("NANOCLOUD_IPTABLES_RECORD", &log_path);
        env::set_var("NANOCLOUD_IPTABLES", "/sbin/iptables");

        let service = make_service();
        let endpoints = make_endpoints();

        let mut handles = Vec::new();
        for _ in 0..3 {
            let svc = service.clone();
            let eps = endpoints.clone();
            handles.push(thread::spawn(move || program_service(&svc, &eps)));
        }

        for handle in handles {
            handle
                .join()
                .expect("join thread")
                .expect("program service");
        }

        let log = fs::read_to_string(&log_path).expect("read concurrent log");
        assert!(
            log.contains("-A NCLD-SERVICES"),
            "expected service programming to be recorded: {log}"
        );

        env::remove_var("NANOCLOUD_IPTABLES_RECORD");
        env::remove_var("NANOCLOUD_IPTABLES");
        restore_env("NANOCLOUD_IPTABLES_RECORD", previous_record);
        restore_env("NANOCLOUD_IPTABLES", previous_binary);
    }

    #[test]
    #[serial]
    fn policy_and_proxy_integration_logs() {
        let _guard = keyspace_lock().lock();
        let dir = tempdir().expect("tempdir");
        let nft_log = dir.path().join("nft-integration.log");
        let ipt_log = dir.path().join("iptables-integration.log");

        let previous_nft_record = env::var("NANOCLOUD_NFT_RECORD").ok();
        let previous_nft = env::var("NANOCLOUD_NFT").ok();
        let previous_ipt_record = env::var("NANOCLOUD_IPTABLES_RECORD").ok();
        let previous_ipt = env::var("NANOCLOUD_IPTABLES").ok();

        env::set_var("NANOCLOUD_NFT_RECORD", &nft_log);
        env::set_var("NANOCLOUD_NFT", "/usr/sbin/nft");
        env::set_var("NANOCLOUD_IPTABLES_RECORD", &ipt_log);
        env::set_var("NANOCLOUD_IPTABLES", "/sbin/iptables");

        let policy_programmer = PolicyProgrammer::shared();
        policy_programmer
            .sync(&[])
            .expect("clear existing policy chains");
        let policy_chain = PolicyChain::new(
            "default",
            "svc-pod",
            "10.203.0.13",
            PolicyDirection::Ingress,
            vec![PolicyRule {
                cidr: Some("10.1.0.0/24".into()),
                protocol: Some("tcp".into()),
                port: Some(8080),
            }],
        );
        policy_programmer
            .sync(std::slice::from_ref(&policy_chain))
            .expect("program policy chain");

        let service = make_service();
        let endpoints = make_endpoints();
        program_service(&service, &endpoints).expect("program proxy rules");

        let policy_log = fs::read_to_string(&nft_log).expect("read policy log");
        assert!(
            policy_log.contains(&policy_chain.name),
            "expected policy chain name in log: {policy_log}"
        );

        let proxy_log = fs::read_to_string(&ipt_log).expect("read proxy log");
        assert!(
            proxy_log.contains(&chain_name(&service, 80)),
            "expected proxy chain name in log: {proxy_log}"
        );

        policy_programmer.sync(&[]).expect("clear policy chains");
        remove_service(&service).expect("remove service");

        restore_env("NANOCLOUD_NFT_RECORD", previous_nft_record);
        restore_env("NANOCLOUD_NFT", previous_nft);
        restore_env("NANOCLOUD_IPTABLES_RECORD", previous_ipt_record);
        restore_env("NANOCLOUD_IPTABLES", previous_ipt);
    }
}
