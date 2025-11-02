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

use crate::nanocloud::util::error::with_context;

use hex;
use sha1::{Digest, Sha1};
use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::fmt;
use std::io::Write;
use std::sync::{Mutex, OnceLock};

const BASE_CHAIN: &str = "NCLD-NP";
const IPTABLES_TABLE: &str = "filter";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PolicyDirection {
    Ingress,
    Egress,
}

impl fmt::Display for PolicyDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PolicyDirection::Ingress => write!(f, "ingress"),
            PolicyDirection::Egress => write!(f, "egress"),
        }
    }
}

impl PolicyDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            PolicyDirection::Ingress => "ingress",
            PolicyDirection::Egress => "egress",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PolicyRule {
    pub cidr: Option<String>,
    pub protocol: Option<String>,
    pub port: Option<u16>,
}

impl PolicyRule {
    pub fn any() -> Self {
        Self {
            cidr: None,
            protocol: None,
            port: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PolicyChain {
    pub name: String,
    pub namespace: String,
    pub pod: String,
    pub direction: PolicyDirection,
    pub pod_ip: String,
    pub rules: Vec<PolicyRule>,
}

impl PolicyChain {
    pub fn new(
        namespace: &str,
        pod: &str,
        pod_ip: &str,
        direction: PolicyDirection,
        rules: Vec<PolicyRule>,
    ) -> Self {
        let name = chain_name(namespace, pod, direction);
        Self {
            name,
            namespace: namespace.to_string(),
            pod: pod.to_string(),
            direction,
            pod_ip: pod_ip.to_string(),
            rules,
        }
    }
}

pub fn chain_name(namespace: &str, pod: &str, direction: PolicyDirection) -> String {
    let mut hasher = Sha1::new();
    hasher.update(namespace.as_bytes());
    hasher.update(b"/");
    hasher.update(pod.as_bytes());
    match direction {
        PolicyDirection::Ingress => hasher.update(b"ingress"),
        PolicyDirection::Egress => hasher.update(b"egress"),
    }
    let digest = hex::encode(hasher.finalize());
    let suffix = &digest[..12];
    let prefix = match direction {
        PolicyDirection::Ingress => "NCLD-NPI",
        PolicyDirection::Egress => "NCLD-NPE",
    };
    format!("{}{}", prefix, suffix).to_uppercase()
}

struct CommandRunner {
    binary: String,
    record_path: Option<String>,
}

impl CommandRunner {
    fn new() -> Self {
        let binary = env::var("NANOCLOUD_IPTABLES").unwrap_or_else(|_| "iptables".to_string());
        let record_path = env::var("NANOCLOUD_IPTABLES_RECORD").ok();
        Self {
            binary,
            record_path,
        }
    }

    fn run<I, S>(&self, args: I) -> Result<bool, Box<dyn Error + Send + Sync>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let args_vec: Vec<String> = args
            .into_iter()
            .map(|segment| segment.as_ref().to_string())
            .collect();
        let binary = env::var("NANOCLOUD_IPTABLES").unwrap_or_else(|_| self.binary.clone());
        let record_path = env::var("NANOCLOUD_IPTABLES_RECORD")
            .ok()
            .or_else(|| self.record_path.clone());
        if let Some(record) = record_path.as_ref() {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(record)
                .map_err(|e| with_context(e, "Failed to open iptables record log"))?;
            writeln!(file, "{} {}", binary, args_vec.join(" "))
                .map_err(|e| with_context(e, "Failed to write iptables record"))?;
            let is_delete = args_vec.iter().any(|arg| arg == "-D" || arg == "-X");
            return Ok(!is_delete);
        }

        let status = std::process::Command::new(&binary)
            .args(&args_vec)
            .status()
            .map_err(|e| with_context(e, format!("Failed to execute {}", binary)))?;
        Ok(status.success())
    }

    fn ensure_chain(&self, chain: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        if !self.run(["-w", "-t", IPTABLES_TABLE, "-N", chain])? {
            self.run(["-w", "-t", IPTABLES_TABLE, "-F", chain])?;
        }
        Ok(())
    }

    fn clear_chain(&self, chain: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.run(["-w", "-t", IPTABLES_TABLE, "-F", chain])?;
        Ok(())
    }

    fn delete_chain(&self, chain: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.run(["-w", "-t", IPTABLES_TABLE, "-F", chain])?;
        self.run(["-w", "-t", IPTABLES_TABLE, "-X", chain])?;
        Ok(())
    }

    fn ensure_base_chain(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.ensure_chain(BASE_CHAIN)?;
        if !self.run([
            "-w",
            "-t",
            IPTABLES_TABLE,
            "-C",
            "FORWARD",
            "-j",
            BASE_CHAIN,
        ])? {
            self.run([
                "-w",
                "-t",
                IPTABLES_TABLE,
                "-A",
                "FORWARD",
                "-j",
                BASE_CHAIN,
            ])?;
        }
        Ok(())
    }

    fn append_base_jump(&self, chain: &PolicyChain) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut args = vec![
            "-w".to_string(),
            "-t".to_string(),
            IPTABLES_TABLE.to_string(),
            "-A".to_string(),
            BASE_CHAIN.to_string(),
        ];
        match chain.direction {
            PolicyDirection::Ingress => args.push("-d".to_string()),
            PolicyDirection::Egress => args.push("-s".to_string()),
        }
        args.push(chain.pod_ip.clone());
        args.push("-j".to_string());
        args.push(chain.name.clone());
        self.run(args.iter().map(|s| s.as_str()))?;
        Ok(())
    }

    fn append_allow_rule(
        &self,
        chain: &PolicyChain,
        rule: &PolicyRule,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut args = vec![
            "-w".to_string(),
            "-t".to_string(),
            IPTABLES_TABLE.to_string(),
            "-A".to_string(),
            chain.name.clone(),
        ];

        if let Some(cidr) = rule.cidr.as_deref() {
            match chain.direction {
                PolicyDirection::Ingress => args.push("-s".to_string()),
                PolicyDirection::Egress => args.push("-d".to_string()),
            }
            args.push(cidr.to_string());
        }

        let mut protocol = rule.protocol.clone().map(|p| p.to_lowercase());
        if rule.port.is_some() && protocol.is_none() {
            protocol = Some("tcp".to_string());
        }

        if let Some(proto) = protocol.as_deref() {
            args.push("-p".to_string());
            args.push(proto.to_string());
        }

        if let Some(port) = rule.port {
            if let Some(proto) = protocol.as_deref() {
                args.push("-m".to_string());
                args.push(proto.to_string());
            }
            args.push("--dport".to_string());
            args.push(port.to_string());
        }

        args.push("-j".to_string());
        args.push("RETURN".to_string());
        self.run(args.iter().map(|s| s.as_str()))?;
        Ok(())
    }

    fn append_drop(&self, chain: &PolicyChain) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.run(["-w", "-t", IPTABLES_TABLE, "-A", &chain.name, "-j", "DROP"])?;
        Ok(())
    }

    fn append_base_return(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.run(["-w", "-t", IPTABLES_TABLE, "-A", BASE_CHAIN, "-j", "RETURN"])?;
        Ok(())
    }
}

pub struct PolicyProgrammer {
    runner: CommandRunner,
    installed_chains: Mutex<HashSet<String>>,
}

impl PolicyProgrammer {
    pub fn shared() -> &'static PolicyProgrammer {
        static INSTANCE: OnceLock<PolicyProgrammer> = OnceLock::new();
        INSTANCE.get_or_init(|| PolicyProgrammer {
            runner: CommandRunner::new(),
            installed_chains: Mutex::new(HashSet::new()),
        })
    }

    pub fn sync(&self, chains: &[PolicyChain]) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.runner.ensure_base_chain()?;
        self.runner.clear_chain(BASE_CHAIN)?;

        let desired_names: HashSet<String> =
            chains.iter().map(|chain| chain.name.clone()).collect();

        let mut installed = self.installed_chains.lock().expect("policy lock poisoned");
        for name in installed.difference(&desired_names) {
            self.runner.delete_chain(name)?;
        }

        for chain in chains {
            self.runner.ensure_chain(&chain.name)?;
            self.runner.clear_chain(&chain.name)?;
            for rule in &chain.rules {
                self.runner.append_allow_rule(chain, rule)?;
            }
            self.runner.append_drop(chain)?;
        }

        for chain in chains {
            self.runner.append_base_jump(chain)?;
        }

        self.runner.append_base_return()?;
        *installed = desired_names;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::test_support::keyspace_lock;
    use serial_test::serial;
    use std::collections::HashSet;
    use std::env;
    use std::fs;
    use std::sync::Mutex;
    use tempfile::tempdir;

    fn restore_env(key: &str, previous: Option<String>) {
        if let Some(value) = previous {
            env::set_var(key, value);
        } else {
            env::remove_var(key);
        }
    }

    #[test]
    #[serial]
    fn sync_programs_ingress_rules() {
        let _guard = keyspace_lock().lock();
        let dir = tempdir().expect("tempdir");
        let log_path = dir.path().join("iptables.log");
        let previous_record = env::var("NANOCLOUD_IPTABLES_RECORD").ok();
        let previous_binary = env::var("NANOCLOUD_IPTABLES").ok();
        env::set_var("NANOCLOUD_IPTABLES_RECORD", &log_path);
        env::set_var("NANOCLOUD_IPTABLES", "/sbin/iptables");

        let programmer = PolicyProgrammer {
            runner: CommandRunner::new(),
            installed_chains: Mutex::new(HashSet::new()),
        };

        let chain = PolicyChain::new(
            "default",
            "web-0",
            "10.203.0.10",
            PolicyDirection::Ingress,
            vec![PolicyRule {
                cidr: Some("10.1.0.0/24".to_string()),
                protocol: Some("tcp".to_string()),
                port: Some(80),
            }],
        );

        programmer.sync(&[chain]).expect("sync policy");

        let log = fs::read_to_string(&log_path).expect("read iptables log");
        assert!(
            log.contains("-A NCLD-NP -d 10.203.0.10 -j"),
            "expected base jump in log: {log}"
        );
        assert!(
            log.contains("--dport 80"),
            "expected port match in log: {log}"
        );
        assert!(log.contains("-j DROP"), "expected drop rule in log: {log}");

        programmer.sync(&[]).expect("sync empty policy set");

        let updated = fs::read_to_string(&log_path).expect("read updated iptables log");
        assert!(
            updated.contains("-X"),
            "expected chain deletion in log: {updated}"
        );

        restore_env("NANOCLOUD_IPTABLES_RECORD", previous_record);
        restore_env("NANOCLOUD_IPTABLES", previous_binary);
    }
}
