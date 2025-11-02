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

use crate::nanocloud::controller::watch::{ControllerWatchEvent, ControllerWatchManager};
use crate::nanocloud::k8s::networkpolicy::{
    NetworkPolicy, NetworkPolicyEgressRule, NetworkPolicyIngressRule, NetworkPolicyPeer,
    NetworkPolicyPort, NetworkPolicyPortValue,
};
use crate::nanocloud::k8s::store::{
    list_network_policies, list_pod_manifests, normalize_namespace, StoredNetworkPolicy, StoredPod,
};
use crate::nanocloud::logger::log_error;
use crate::nanocloud::network::policy::{
    chain_name, PolicyChain, PolicyDirection, PolicyProgrammer, PolicyRule,
};
use crate::nanocloud::observability::metrics::{self, ControllerReconcileResult};
use crate::nanocloud::observability::tracing;
use crate::nanocloud::util::error::with_context;
use crate::nanocloud::util::KeyspaceEventType;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use tokio::task::JoinHandle;

const POD_IP_ANNOTATION: &str = "nanocloud.io/pod-ip";
const COMPONENT: &str = "networkpolicy-controller";
const POLICY_PREFIX: &str = "/networkpolicies";
const POD_PREFIX: &str = "/pods";

pub fn spawn() -> JoinHandle<()> {
    tokio::spawn(async move {
        reconcile_with_reason("startup").await;
        let manager = ControllerWatchManager::shared();
        let mut policy_subscription = manager.subscribe(POLICY_PREFIX, None);
        let mut pod_subscription = manager.subscribe(POD_PREFIX, None);

        loop {
            tokio::select! {
                event = policy_subscription.recv() => {
                    if !handle_watch_event("networkpolicy", event).await {
                        break;
                    }
                }
                event = pod_subscription.recv() => {
                    if !handle_watch_event("pod", event).await {
                        break;
                    }
                }
            }
        }
    })
}

async fn handle_watch_event(kind: &'static str, event: Option<ControllerWatchEvent>) -> bool {
    match event {
        Some(evt) => {
            if matches!(
                evt.event_type,
                KeyspaceEventType::Added | KeyspaceEventType::Modified | KeyspaceEventType::Deleted
            ) {
                reconcile_with_reason(kind).await;
            }
            true
        }
        None => false,
    }
}

async fn reconcile_with_reason(trigger: &'static str) {
    let span_name = format!("trigger:{}", trigger);
    match tracing::with_span("controller.networkpolicy", span_name, async move {
        tokio::task::spawn_blocking(reconcile).await
    })
    .await
    {
        Ok(Ok(())) => {
            metrics::record_controller_reconcile(
                "networkpolicy",
                ControllerReconcileResult::Success,
            );
        }
        Ok(Err(err)) => {
            let message = err.to_string();
            log_error(
                COMPONENT,
                "NetworkPolicy reconciliation failed",
                &[("trigger", trigger), ("error", message.as_str())],
            );
            metrics::record_controller_reconcile("networkpolicy", ControllerReconcileResult::Error);
        }
        Err(join_err) => {
            let message = join_err.to_string();
            log_error(
                COMPONENT,
                "NetworkPolicy reconciliation panic",
                &[("trigger", trigger), ("error", message.as_str())],
            );
            metrics::record_controller_reconcile("networkpolicy", ControllerReconcileResult::Error);
        }
    }
}

#[derive(Debug, Clone)]
struct PodInfo {
    namespace: String,
    name: String,
    ip: String,
    labels: HashMap<String, String>,
}

impl PodInfo {
    fn from_stored(pod: &StoredPod) -> Option<Self> {
        let namespace = normalize_namespace(pod.namespace.as_deref());
        let name = pod.name.clone();
        let labels = pod.workload.metadata.labels.clone();
        let ip = pod
            .workload
            .status
            .as_ref()
            .and_then(|status| status.pod_ip.clone())
            .or_else(|| {
                pod.workload
                    .metadata
                    .annotations
                    .get(POD_IP_ANNOTATION)
                    .cloned()
            })?;
        if ip.trim().is_empty() {
            return None;
        }
        Some(Self {
            namespace,
            name,
            ip,
            labels,
        })
    }
}

struct ChainAccumulator {
    namespace: String,
    pod: String,
    pod_ip: String,
    direction: PolicyDirection,
    rules: Vec<PolicyRule>,
}

impl ChainAccumulator {
    fn new(namespace: &str, pod: &PodInfo, direction: PolicyDirection) -> Self {
        Self {
            namespace: namespace.to_string(),
            pod: pod.name.clone(),
            pod_ip: pod.ip.clone(),
            direction,
            rules: Vec::new(),
        }
    }

    fn append_rules(&mut self, mut rules: Vec<PolicyRule>) {
        self.rules.append(&mut rules);
    }
}

pub fn compile_plan(
) -> Result<(Vec<StoredNetworkPolicy>, Vec<PolicyChain>), Box<dyn Error + Send + Sync>> {
    let stored_policies = list_network_policies()
        .map_err(|err| with_context(err, "Failed to list NetworkPolicy objects"))?;
    let stored_pods = list_pod_manifests()
        .map_err(|err| with_context(err, "Failed to list pods for NetworkPolicy reconciliation"))?;

    let pods: Vec<PodInfo> = stored_pods
        .iter()
        .filter_map(PodInfo::from_stored)
        .collect();

    if stored_policies.is_empty() {
        return Ok((stored_policies, Vec::new()));
    }

    let pod_index: Vec<PodInfo> = pods.clone();
    let mut chains = BTreeMap::<String, ChainAccumulator>::new();

    for stored_policy in &stored_policies {
        apply_policy(stored_policy, &pod_index, &mut chains)?;
    }

    let mut rendered = Vec::new();
    for (_name, mut accumulator) in chains {
        let rules = deduplicate_rules(std::mem::take(&mut accumulator.rules));
        rendered.push(PolicyChain::new(
            &accumulator.namespace,
            &accumulator.pod,
            &accumulator.pod_ip,
            accumulator.direction,
            rules,
        ));
    }

    Ok((stored_policies, rendered))
}

pub fn reconcile() -> Result<(), Box<dyn Error + Send + Sync>> {
    let (policies, plan) = compile_plan()?;
    if policies.is_empty() {
        PolicyProgrammer::shared().sync(&[])?;
        return Ok(());
    }

    PolicyProgrammer::shared().sync(&plan)?;
    Ok(())
}

fn apply_policy(
    stored_policy: &StoredNetworkPolicy,
    pods: &[PodInfo],
    chains: &mut BTreeMap<String, ChainAccumulator>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let namespace = normalize_namespace(stored_policy.namespace.as_deref());
    let policy = &stored_policy.policy;
    let target_pods = select_target_pods(policy, pods, &namespace);

    if target_pods.is_empty() {
        return Ok(());
    }

    let directions = resolve_policy_directions(&policy.spec);
    if directions.is_empty() {
        return Ok(());
    }

    for target in target_pods {
        for direction in directions.iter().copied() {
            let rules = match direction {
                PolicyDirection::Ingress => collect_ingress_rules(policy, &namespace, pods),
                PolicyDirection::Egress => collect_egress_rules(policy, &namespace, pods),
            };
            let chain_key = chain_name(&namespace, &target.name, direction);
            let accumulator = chains
                .entry(chain_key.clone())
                .or_insert_with(|| ChainAccumulator::new(&namespace, target, direction));
            accumulator.append_rules(rules);
        }
    }
    Ok(())
}

fn select_target_pods<'a>(
    policy: &NetworkPolicy,
    pods: &'a [PodInfo],
    namespace: &str,
) -> Vec<&'a PodInfo> {
    pods.iter()
        .filter(|pod| pod.namespace == namespace)
        .filter(|pod| matches_selector(&policy.spec.pod_selector, &pod.labels))
        .collect()
}

fn matches_selector(selector: &HashMap<String, String>, labels: &HashMap<String, String>) -> bool {
    if selector.is_empty() {
        return true;
    }
    selector.iter().all(|(key, value)| {
        labels
            .get(key)
            .map(|candidate| candidate == value)
            .unwrap_or(false)
    })
}

fn resolve_policy_directions(
    spec: &crate::nanocloud::k8s::networkpolicy::NetworkPolicySpec,
) -> Vec<PolicyDirection> {
    let mut requested = HashSet::new();
    for entry in &spec.policy_types {
        match entry.to_ascii_lowercase().as_str() {
            "ingress" => {
                requested.insert(PolicyDirection::Ingress);
            }
            "egress" => {
                requested.insert(PolicyDirection::Egress);
            }
            _ => {}
        }
    }

    if requested.is_empty() {
        if spec.egress.is_empty() {
            requested.insert(PolicyDirection::Ingress);
        } else {
            requested.insert(PolicyDirection::Ingress);
            requested.insert(PolicyDirection::Egress);
        }
    }

    let mut ordered = Vec::new();
    if requested.contains(&PolicyDirection::Ingress) {
        ordered.push(PolicyDirection::Ingress);
    }
    if requested.contains(&PolicyDirection::Egress) {
        ordered.push(PolicyDirection::Egress);
    }
    ordered
}

fn collect_ingress_rules(
    policy: &NetworkPolicy,
    namespace: &str,
    pods: &[PodInfo],
) -> Vec<PolicyRule> {
    render_ingress_rules(&policy.spec.ingress, namespace, pods)
}

fn collect_egress_rules(
    policy: &NetworkPolicy,
    namespace: &str,
    pods: &[PodInfo],
) -> Vec<PolicyRule> {
    render_egress_rules(&policy.spec.egress, namespace, pods)
}

fn render_ingress_rules(
    rules: &[NetworkPolicyIngressRule],
    namespace: &str,
    pods: &[PodInfo],
) -> Vec<PolicyRule> {
    render_rule_set(
        rules
            .iter()
            .map(|rule| (rule.from.as_slice(), rule.ports.as_slice())),
        namespace,
        pods,
    )
}

fn render_egress_rules(
    rules: &[NetworkPolicyEgressRule],
    namespace: &str,
    pods: &[PodInfo],
) -> Vec<PolicyRule> {
    render_rule_set(
        rules
            .iter()
            .map(|rule| (rule.to.as_slice(), rule.ports.as_slice())),
        namespace,
        pods,
    )
}

fn render_rule_set<'a, I>(rule_iter: I, namespace: &str, pods: &[PodInfo]) -> Vec<PolicyRule>
where
    I: Iterator<Item = (&'a [NetworkPolicyPeer], &'a [NetworkPolicyPort])>,
{
    let mut rendered = Vec::new();
    for (peers, ports) in rule_iter {
        let sources = expand_peers(peers, namespace, pods);
        if sources.is_empty() {
            continue;
        }
        let port_constraints = expand_ports(ports);
        for source in &sources {
            for constraint in &port_constraints {
                let mut rule = PolicyRule::any();
                if let Some(cidr) = source {
                    rule.cidr = Some(cidr.clone());
                }
                if let Some(protocol) = constraint.protocol.as_ref() {
                    rule.protocol = Some(protocol.clone());
                }
                rule.port = constraint.port;
                rendered.push(rule);
            }
        }
    }

    if rendered.is_empty() {
        Vec::new()
    } else {
        deduplicate_rules(rendered)
    }
}

fn expand_peers(
    peers: &[NetworkPolicyPeer],
    policy_namespace: &str,
    pods: &[PodInfo],
) -> Vec<Option<String>> {
    if peers.is_empty() {
        return vec![None];
    }

    let mut results: HashSet<Option<String>> = HashSet::new();
    for peer in peers {
        if let Some(ip_block) = &peer.ip_block {
            if !ip_block.cidr.is_empty() {
                results.insert(Some(ip_block.cidr.clone()));
            }
            continue;
        }

        let namespace_filter = resolve_namespaces(peer, policy_namespace);
        if matches!(namespace_filter, NamespaceFilter::Names(ref names) if names.is_empty()) {
            continue;
        }

        for pod in pods {
            if !namespace_filter.matches(&pod.namespace) {
                continue;
            }
            if let Some(selector) = &peer.pod_selector {
                if !matches_selector(selector, &pod.labels) {
                    continue;
                }
            }
            results.insert(Some(pod.ip.clone()));
        }
    }

    results.into_iter().collect()
}

enum NamespaceFilter {
    Any,
    Names(Vec<String>),
}

impl NamespaceFilter {
    fn matches(&self, namespace: &str) -> bool {
        match self {
            NamespaceFilter::Any => true,
            NamespaceFilter::Names(names) => names.iter().any(|value| value == namespace),
        }
    }
}

fn resolve_namespaces(peer: &NetworkPolicyPeer, policy_namespace: &str) -> NamespaceFilter {
    match &peer.namespace_selector {
        Some(selector) if selector.is_empty() => NamespaceFilter::Any,
        Some(selector) => {
            if let Some(ns) = selector.get("kubernetes.io/metadata.name") {
                NamespaceFilter::Names(vec![ns.clone()])
            } else {
                NamespaceFilter::Names(Vec::new())
            }
        }
        None => NamespaceFilter::Names(vec![policy_namespace.to_string()]),
    }
}

#[derive(Clone)]
struct PortConstraint {
    protocol: Option<String>,
    port: Option<u16>,
}

fn expand_ports(ports: &[NetworkPolicyPort]) -> Vec<PortConstraint> {
    if ports.is_empty() {
        return vec![PortConstraint {
            protocol: None,
            port: None,
        }];
    }

    let mut results = Vec::new();
    for port in ports {
        let protocol = port.protocol.as_ref().map(|p| p.to_ascii_lowercase());
        match &port.port {
            Some(NetworkPolicyPortValue::Int(value)) => results.push(PortConstraint {
                protocol: protocol.clone(),
                port: Some(*value),
            }),
            Some(NetworkPolicyPortValue::Str(_)) => {
                // Named ports are not enforced directly by the controller today.
                continue;
            }
            None => results.push(PortConstraint {
                protocol: protocol.clone(),
                port: None,
            }),
        }
    }
    if results.is_empty() {
        vec![PortConstraint {
            protocol: None,
            port: None,
        }]
    } else {
        results
    }
}

fn deduplicate_rules(rules: Vec<PolicyRule>) -> Vec<PolicyRule> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for rule in rules {
        let key = (
            rule.cidr.clone(),
            rule.protocol
                .as_ref()
                .map(|proto| proto.to_ascii_lowercase()),
            rule.port,
        );
        if seen.insert(key) {
            deduped.push(rule);
        }
    }
    deduped
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::networkpolicy::{
        NetworkPolicy, NetworkPolicyIngressRule, NetworkPolicyPeer, NetworkPolicyPort,
        NetworkPolicyPortValue,
    };
    use crate::nanocloud::k8s::pod::{ObjectMeta, Pod, PodSpec, PodStatus};
    use crate::nanocloud::k8s::store::{
        list_network_policies as load_policies, list_pod_manifests,
    };
    use crate::nanocloud::network::policy::{chain_name, PolicyDirection, PolicyProgrammer};
    use crate::nanocloud::test_support::{keyspace_lock, test_output_dir};
    use crate::nanocloud::util::Keyspace;
    use serde_json;
    use serial_test::serial;
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::path::Path;
    use std::time::Duration;
    use tokio::time::sleep;

    fn restore_env(key: &str, previous: Option<String>) {
        if let Some(value) = previous {
            env::set_var(key, value);
        } else {
            env::remove_var(key);
        }
    }

    fn write_json(path: &Path, value: &impl serde::Serialize) {
        fs::create_dir_all(path.parent().expect("parent directory")).expect("create parent");
        let payload = serde_json::to_string(value).expect("serialize value");
        fs::write(path, payload).expect("write json file");
    }

    fn make_pod(namespace: &str, name: &str, labels: &[(&str, &str)], ip: &str) -> Pod {
        let labels_map = labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        let mut annotations = HashMap::new();
        annotations.insert(POD_IP_ANNOTATION.to_string(), ip.to_string());
        let metadata = ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: labels_map,
            annotations,
            ..Default::default()
        };
        let spec = PodSpec::default();
        let mut pod = Pod::new(metadata, spec);
        pod.status = Some(PodStatus {
            pod_ip: Some(ip.to_string()),
            ..PodStatus::default()
        });
        pod
    }

    fn store_pod(root: &Path, namespace: &str, name: &str, labels: &[(&str, &str)], ip: &str) {
        let pod = make_pod(namespace, name, labels, ip);
        let path = root
            .join("k8s")
            .join("pods")
            .join(namespace)
            .join(name)
            .join("_value_");
        write_json(&path, &pod);
    }

    fn make_policy() -> NetworkPolicy {
        let metadata = ObjectMeta {
            name: Some("allow-db".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        };
        let mut policy = NetworkPolicy {
            metadata,
            ..Default::default()
        };
        policy
            .spec
            .pod_selector
            .insert("app".to_string(), "web".to_string());
        let mut peer_selector = HashMap::new();
        peer_selector.insert("app".to_string(), "db".to_string());
        let peer = NetworkPolicyPeer {
            pod_selector: Some(peer_selector),
            namespace_selector: None,
            ip_block: None,
        };
        let port = NetworkPolicyPort {
            protocol: Some("TCP".to_string()),
            port: Some(NetworkPolicyPortValue::Int(80)),
        };
        policy.spec.ingress = vec![NetworkPolicyIngressRule {
            from: vec![peer],
            ports: vec![port],
        }];
        policy
    }

    fn store_policy(root: &Path, namespace: &str, policy: &NetworkPolicy) {
        let path = root
            .join("k8s")
            .join("networkpolicies")
            .join(namespace)
            .join(policy.metadata.name.clone().expect("policy name"))
            .join("_value_");
        write_json(&path, policy);
    }

    #[test]
    #[serial]
    fn reconcile_programs_ingress_policy() {
        let _guard = keyspace_lock().lock();
        let dir = test_output_dir("networkpolicy-controller");
        let keyspace_dir = dir.join("keyspace");
        fs::create_dir_all(&keyspace_dir).expect("create keyspace root");

        let previous_keyspace = env::var("NANOCLOUD_KEYSPACE").ok();
        env::set_var("NANOCLOUD_KEYSPACE", &keyspace_dir);
        let lock_path = dir.join("lock").join("nanocloud.lock");
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).expect("create lock dir");
        }
        std::fs::File::create(&lock_path).expect("touch lock file");
        let previous_lock = env::var("NANOCLOUD_LOCK_FILE").ok();
        env::set_var("NANOCLOUD_LOCK_FILE", lock_path.to_string_lossy().as_ref());

        store_pod(
            &keyspace_dir,
            "default",
            "web-0",
            &[("app", "web")],
            "10.1.0.10",
        );
        store_pod(
            &keyspace_dir,
            "default",
            "db-0",
            &[("app", "db")],
            "10.1.0.20",
        );

        let policy = make_policy();
        store_policy(&keyspace_dir, "default", &policy);

        let policies = load_policies().expect("list policies");
        assert_eq!(policies.len(), 1, "expected a stored NetworkPolicy");
        let pods = list_pod_manifests().expect("list pods");
        assert_eq!(pods.len(), 2, "expected stored pods");

        let log_path = dir.join("iptables.log");
        let previous_record = env::var("NANOCLOUD_IPTABLES_RECORD").ok();
        let previous_binary = env::var("NANOCLOUD_IPTABLES").ok();
        env::set_var("NANOCLOUD_IPTABLES_RECORD", &log_path);
        env::set_var("NANOCLOUD_IPTABLES", "/sbin/iptables");
        std::fs::File::create(&log_path).expect("touch iptables log");

        PolicyProgrammer::shared()
            .sync(&[])
            .expect("reset policy state");
        reconcile().expect("reconcile policies");

        let log = fs::read_to_string(&log_path).expect("read iptables log");
        let chain = chain_name("default", "web-0", PolicyDirection::Ingress);
        assert!(
            log.contains(&format!("-A {chain} -s 10.1.0.20")),
            "expected peer rule in {log}"
        );
        assert!(log.contains("--dport 80"), "expected port match in {log}");
        assert!(
            log.contains(&format!("-A {chain} -j DROP")),
            "expected drop rule in {log}"
        );
        assert!(
            log.contains(&format!("-A NCLD-NP -d 10.1.0.10 -j {chain}")),
            "expected base jump in {log}"
        );

        restore_env("NANOCLOUD_KEYSPACE", previous_keyspace);
        restore_env("NANOCLOUD_IPTABLES_RECORD", previous_record);
        restore_env("NANOCLOUD_IPTABLES", previous_binary);
        restore_env("NANOCLOUD_LOCK_FILE", previous_lock);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)] // Guard must remain held while exercising async reconciliation to avoid filesystem collisions.
    #[serial]
    async fn spawn_reconciles_on_events() {
        let _guard = keyspace_lock().lock();
        let dir = test_output_dir("networkpolicy-controller-spawn");
        let keyspace_dir = dir.join("keyspace");
        fs::create_dir_all(&keyspace_dir).expect("create keyspace root");

        let previous_keyspace = env::var("NANOCLOUD_KEYSPACE").ok();
        env::set_var("NANOCLOUD_KEYSPACE", &keyspace_dir);
        let lock_path = dir.join("lock").join("nanocloud.lock");
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).expect("create lock dir");
        }
        std::fs::File::create(&lock_path).expect("touch lock file");
        let previous_lock = env::var("NANOCLOUD_LOCK_FILE").ok();
        env::set_var("NANOCLOUD_LOCK_FILE", lock_path.to_string_lossy().as_ref());

        let log_path = dir.join("iptables.log");
        let previous_record = env::var("NANOCLOUD_IPTABLES_RECORD").ok();
        let previous_binary = env::var("NANOCLOUD_IPTABLES").ok();
        env::set_var("NANOCLOUD_IPTABLES_RECORD", &log_path);
        env::set_var("NANOCLOUD_IPTABLES", "/sbin/iptables");
        std::fs::File::create(&log_path).expect("touch iptables log");

        let handle = super::spawn();

        sleep(Duration::from_millis(50)).await;

        let keyspace = Keyspace::new("k8s");
        let web_pod = make_pod("default", "web-0", &[("app", "web")], "10.1.0.10");
        keyspace
            .put(
                "/pods/default/web-0",
                &serde_json::to_string(&web_pod).unwrap(),
            )
            .expect("store web pod");
        let db_pod = make_pod("default", "db-0", &[("app", "db")], "10.1.0.20");
        keyspace
            .put(
                "/pods/default/db-0",
                &serde_json::to_string(&db_pod).unwrap(),
            )
            .expect("store db pod");

        sleep(Duration::from_millis(50)).await;

        let policy = make_policy();
        keyspace
            .put(
                "/networkpolicies/default/allow-db",
                &serde_json::to_string(&policy).unwrap(),
            )
            .expect("store policy");

        sleep(Duration::from_millis(250)).await;

        let log = wait_for_log_contains(&log_path, "10.1.0.20").await;
        let chain = chain_name("default", "web-0", PolicyDirection::Ingress);
        assert!(
            log.contains(&format!("-A {chain} -s 10.1.0.20")),
            "expected peer rule in {log}"
        );
        assert!(log.contains("--dport 80"), "expected port match in {log}");

        let initial_len = log.len();

        keyspace
            .delete("/pods/default/web-0")
            .expect("delete web pod");

        let updated_log = wait_for_log_contains(&log_path, "-X").await;
        let delta = &updated_log[initial_len..];
        assert!(
            delta.contains("-X"),
            "expected chain deletion in delta log: {delta}"
        );

        let second_len = updated_log.len();

        let replacement = make_pod("default", "web-0", &[("app", "web")], "10.1.0.30");
        keyspace
            .put(
                "/pods/default/web-0",
                &serde_json::to_string(&replacement).unwrap(),
            )
            .expect("restore web pod");

        let reprogrammed = wait_for_log_contains(&log_path, "10.1.0.30").await;
        let re_delta = &reprogrammed[second_len..];
        assert!(
            re_delta.contains("10.1.0.30"),
            "expected updated IP in delta log: {re_delta}"
        );

        handle.abort();

        restore_env("NANOCLOUD_KEYSPACE", previous_keyspace);
        restore_env("NANOCLOUD_IPTABLES_RECORD", previous_record);
        restore_env("NANOCLOUD_IPTABLES", previous_binary);
        restore_env("NANOCLOUD_LOCK_FILE", previous_lock);
    }

    async fn wait_for_log_contains(path: &Path, needle: &str) -> String {
        for _ in 0..100 {
            // Force a reconciliation attempt to minimize reliance on background timing.
            let _ = super::reconcile();
            if path.exists() {
                if let Ok(contents) = fs::read_to_string(path) {
                    if contents.contains(needle) {
                        return contents;
                    }
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
        let contents = if path.exists() {
            fs::read_to_string(path).unwrap_or_default()
        } else {
            String::new()
        };
        panic!("expected '{}' in {}", needle, contents);
    }
}
