#![allow(dead_code)]

use crate::nanocloud::k8s::endpoints::Endpoints;
use crate::nanocloud::k8s::service::{Service, ServicePort};
use crate::nanocloud::observability::metrics::{self, ProxyOperation};
use crate::nanocloud::util::error::{new_error, with_context};

use sha1::{Digest, Sha1};
use std::env;
use std::error::Error;
use std::fmt::Write as _;
use std::fs::OpenOptions;
use std::io::Write;
use std::process::{Command, Stdio};

const PRIMARY_CHAIN: &str = "NCLD-SERVICES";

pub fn program_service(
    service: &Service,
    endpoints: &Endpoints,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let namespace = service.metadata.namespace.as_deref();
    let service_name = service.metadata.name.as_deref().unwrap_or("service");
    metrics::observe_proxy_operation(namespace, service_name, ProxyOperation::Program, || {
        program_service_inner(service, endpoints)
    })
}

fn program_service_inner(
    service: &Service,
    endpoints: &Endpoints,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let cluster_ip = cluster_ip(service)?;
    let runner = CommandRunner::new();
    runner.ensure_primary_chain()?;

    let ports = service_ports(service);
    for port in ports.iter() {
        program_port(&runner, service, cluster_ip, port, endpoints)?;
    }
    Ok(())
}

pub fn remove_service(service: &Service) -> Result<(), Box<dyn Error + Send + Sync>> {
    let namespace = service.metadata.namespace.as_deref();
    let service_name = service.metadata.name.as_deref().unwrap_or("service");
    metrics::observe_proxy_operation(namespace, service_name, ProxyOperation::Remove, || {
        remove_service_inner(service)
    })
}

fn remove_service_inner(service: &Service) -> Result<(), Box<dyn Error + Send + Sync>> {
    let cluster_ip = cluster_ip(service)?;
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
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let chain = chain_name(service, port.port);
    runner.ensure_chain(&chain)?;
    runner.clear_chain(&chain)?;
    runner.remove_service_rule(&chain, cluster_ip, port.port)?;
    runner.install_service_rule(&chain, cluster_ip, port.port)?;

    let addresses = collect_addresses(endpoints);
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
) -> Result<(), Box<dyn Error + Send + Sync>> {
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

fn cluster_ip(service: &Service) -> Result<&str, Box<dyn Error + Send + Sync>> {
    service
        .status
        .as_ref()
        .and_then(|status| status.cluster_ip.as_deref())
        .ok_or_else(|| new_error("Service missing ClusterIP"))
}

fn service_ports(service: &Service) -> Vec<ServicePort> {
    if service.spec.ports.is_empty() {
        Vec::new()
    } else {
        service.spec.ports.clone()
    }
}

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

    fn health_check(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(record_path) = self.record_path.as_ref() {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(record_path)
                .map_err(|e| with_context(e, "Failed to open iptables record log"))?;
            writeln!(file, "{} --version", self.binary)
                .map_err(|e| with_context(e, "Failed to write iptables record"))?;
            return Ok(());
        }

        let status = Command::new(&self.binary)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|e| with_context(e, format!("Failed to execute {}", self.binary)))?;
        if status.success() {
            Ok(())
        } else {
            let descriptor = status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "terminated by signal".to_string());
            Err(new_error(format!(
                "{} --version exited with status {}",
                self.binary, descriptor
            )))
        }
    }

    fn ensure_primary_chain(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if !self.run(["-w", "-t", "nat", "-N", PRIMARY_CHAIN])? {
            self.run(["-w", "-t", "nat", "-F", PRIMARY_CHAIN])?;
        }
        self.ensure_global_jump("PREROUTING")?;
        self.ensure_global_jump("OUTPUT")?;
        Ok(())
    }

    fn ensure_global_jump(&self, source: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let check = self.run(["-w", "-t", "nat", "-C", source, "-j", PRIMARY_CHAIN])?;
        if !check {
            self.run(["-w", "-t", "nat", "-A", source, "-j", PRIMARY_CHAIN])?;
        }
        Ok(())
    }

    fn ensure_chain(&self, chain: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        if !self.run(["-w", "-t", "nat", "-N", chain])? {
            self.run(["-w", "-t", "nat", "-F", chain])?;
        }
        Ok(())
    }

    fn clear_chain(&self, chain: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.run(["-w", "-t", "nat", "-F", chain])?;
        Ok(())
    }

    fn delete_chain(&self, chain: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.run(["-w", "-t", "nat", "-F", chain])?;
        self.run(["-w", "-t", "nat", "-X", chain])?;
        Ok(())
    }

    fn remove_service_rule(
        &self,
        chain: &str,
        cluster_ip: &str,
        port: u16,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
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

    fn install_service_rule(
        &self,
        chain: &str,
        cluster_ip: &str,
        port: u16,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
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

    fn run<I, S>(&self, args: I) -> Result<bool, Box<dyn Error + Send + Sync>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let args_vec: Vec<String> = args.into_iter().map(|s| s.as_ref().to_string()).collect();
        if let Some(ref record_path) = self.record_path {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(record_path)
                .map_err(|e| with_context(e, "Failed to open iptables record log"))?;
            writeln!(file, "{} {}", self.binary, args_vec.join(" "))
                .map_err(|e| with_context(e, "Failed to write iptables record"))?;
            let is_delete = args_vec.iter().any(|arg| arg == "-D");
            return Ok(!is_delete);
        }

        let output = Command::new(&self.binary)
            .args(&args_vec)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .map_err(|e| with_context(e, format!("Failed to execute {}", self.binary)))?;
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

        Err(with_context(
            new_error(format!(
                "{} {} exited with status {:?}: {}",
                self.binary,
                args_vec.join(" "),
                exit_code,
                stderr.trim()
            )),
            "iptables command failed",
        ))
    }
}

pub fn health_check() -> Result<(), Box<dyn Error + Send + Sync>> {
    CommandRunner::new().health_check()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::endpoints::{EndpointAddress, EndpointSubset};
    use crate::nanocloud::k8s::pod::ObjectMeta;
    use serial_test::serial;
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
}
