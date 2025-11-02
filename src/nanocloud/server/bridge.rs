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

use std::error::Error;
use std::io::ErrorKind;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::fs;
use tokio::process::Command;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::nanocloud::logger::{log_error, log_info, log_warn};
use crate::nanocloud::util::error::{new_error, with_context};

const BRIDGE_READY_TIMEOUT: Duration = Duration::from_secs(60);
const BRIDGE_READY_INTERVAL: Duration = Duration::from_secs(1);

static BRIDGE_TRACKER: OnceLock<Arc<RwLock<BridgeReadinessState>>> = OnceLock::new();

pub async fn readiness_snapshot() -> Option<BridgeReadinessSnapshot> {
    let tracker = BRIDGE_TRACKER.get()?;
    let state = tracker.read().await;
    Some(state.snapshot())
}

pub async fn wait_for_bridge_ready(
    bridge_name: &'static str,
    expected_cidr: &'static str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (expected_ip, expected_prefix) = parse_cidr(expected_cidr)?;
    let tracker = BRIDGE_TRACKER.get_or_init(|| Arc::new(RwLock::new(BridgeReadinessState::new())));

    {
        let mut state = tracker.write().await;
        state.reset(bridge_name, expected_cidr);
    }

    log_info(
        "server",
        "Awaiting bridge carrier",
        &[("bridge", bridge_name), ("cidr", expected_cidr)],
    );

    let start = Instant::now();

    loop {
        {
            let mut state = tracker.write().await;
            state.prepare_attempt();
        }

        match inspect_bridge(bridge_name, &expected_ip, expected_prefix).await {
            Ok(observation) => {
                let ready = observation.has_expected_cidr
                    && (observation.carrier.unwrap_or(false) || observation.admin_up);
                {
                    let mut state = tracker.write().await;
                    state.record_observation(observation.clone());
                    state.ready = ready;
                    state.last_attempt_completed = Some(Instant::now());
                }

                if ready {
                    let attempts = {
                        let state = tracker.read().await;
                        state.attempts
                    };
                    let message = if attempts == 1 {
                        "Bridge carrier confirmed after 1 attempt".to_string()
                    } else {
                        format!("Bridge carrier confirmed after {attempts} attempts")
                    };
                    log_info(
                        "server",
                        message.as_str(),
                        &[("bridge", bridge_name), ("cidr", expected_cidr)],
                    );
                    return Ok(());
                }
            }
            Err(err) => {
                let error_text = err.to_string();
                {
                    let mut state = tracker.write().await;
                    state.last_error = Some(error_text.clone());
                    state.last_attempt_completed = Some(Instant::now());
                    state.ready = false;
                }
                log_warn(
                    "server",
                    "Bridge readiness probe failed",
                    &[
                        ("bridge", bridge_name),
                        ("cidr", expected_cidr),
                        ("error", error_text.as_str()),
                    ],
                );
            }
        }

        if start.elapsed() >= BRIDGE_READY_TIMEOUT {
            log_error(
                "server",
                "Bridge readiness timeout",
                &[("bridge", bridge_name), ("cidr", expected_cidr)],
            );

            let mut state = tracker.write().await;
            state.ready = false;
            state
                .last_error
                .get_or_insert_with(|| "bridge readiness timed out".to_string());
            state.last_attempt_completed = Some(Instant::now());
            return Err(new_error(format!(
                "Timed out waiting for {bridge_name} to report carrier UP with {expected_cidr}"
            )));
        }

        sleep(BRIDGE_READY_INTERVAL).await;
    }
}

fn parse_cidr(cidr: &str) -> Result<(String, u8), Box<dyn Error + Send + Sync>> {
    let (ip, prefix) = cidr
        .split_once('/')
        .ok_or_else(|| new_error(format!("Invalid CIDR format: {cidr}")))?;
    let prefix = prefix
        .parse::<u8>()
        .map_err(|e| with_context(e, format!("Invalid prefix in CIDR {cidr}")))?;
    Ok((ip.trim().to_string(), prefix))
}

async fn inspect_bridge(
    bridge_name: &str,
    expected_ip: &str,
    expected_prefix: u8,
) -> Result<BridgeObservation, Box<dyn Error + Send + Sync>> {
    let output = Command::new("ip")
        .args(["-j", "address", "show", "dev", bridge_name])
        .output()
        .await
        .map_err(|e| with_context(e, format!("Failed running ip addr for {bridge_name}")))?;

    if !output.status.success() {
        return Err(new_error(format!(
            "ip addr show failed for {} (exit code {:?})",
            bridge_name,
            output.status.code()
        )));
    }

    let entries: Vec<IpAddressEntry> = serde_json::from_slice(&output.stdout).map_err(|e| {
        with_context(
            e,
            format!("Failed to parse ip addr output for {}", bridge_name),
        )
    })?;
    let entry = entries.into_iter().next().ok_or_else(|| {
        new_error(format!(
            "ip addr output missing interface entry for {}",
            bridge_name
        ))
    })?;

    let mut addresses = Vec::new();
    let mut has_expected_cidr = false;
    for info in entry.addr_info {
        if info.family.as_deref() != Some("inet") {
            continue;
        }
        if let Some(local) = info.local {
            if let Some(prefixlen) = info.prefixlen {
                let cidr = format!("{local}/{prefixlen}");
                if local == expected_ip && prefixlen == expected_prefix {
                    has_expected_cidr = true;
                }
                addresses.push(cidr);
            }
        }
    }

    let carrier = read_carrier_flag(bridge_name).await?;
    let operstate = entry.operstate.map(|s| s.to_uppercase());

    let admin_up = entry
        .flags
        .as_ref()
        .map(|flags| flags.iter().any(|flag| flag == "UP"))
        .unwrap_or(false);

    Ok(BridgeObservation {
        carrier,
        operstate,
        addresses,
        has_expected_cidr,
        admin_up,
    })
}

async fn read_carrier_flag(
    bridge_name: &str,
) -> Result<Option<bool>, Box<dyn Error + Send + Sync>> {
    let path = format!("/sys/class/net/{}/carrier", bridge_name);
    match fs::read_to_string(&path).await {
        Ok(contents) => match contents.trim() {
            "1" => Ok(Some(true)),
            "0" => Ok(Some(false)),
            _ => Ok(None),
        },
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(with_context(
            err,
            format!("Failed to read carrier status for {}", bridge_name),
        )),
    }
}

#[derive(Deserialize)]
struct IpAddressEntry {
    #[serde(default)]
    operstate: Option<String>,
    #[serde(default)]
    flags: Option<Vec<String>>,
    #[serde(default)]
    addr_info: Vec<IpAddressInfo>,
}

#[derive(Deserialize)]
struct IpAddressInfo {
    #[serde(default)]
    family: Option<String>,
    #[serde(default)]
    local: Option<String>,
    #[serde(default)]
    prefixlen: Option<u8>,
}

#[derive(Clone, Debug)]
pub struct BridgeReadinessSnapshot {
    pub bridge_name: &'static str,
    pub expected_cidr: &'static str,
    pub ready: bool,
    pub attempts: u32,
    pub started_at: Option<Instant>,
    pub last_attempt_completed: Option<Instant>,
    pub last_error: Option<String>,
    pub last_observation: Option<BridgeObservation>,
}

#[derive(Clone, Debug)]
pub struct BridgeObservation {
    pub carrier: Option<bool>,
    pub operstate: Option<String>,
    pub addresses: Vec<String>,
    pub has_expected_cidr: bool,
    pub admin_up: bool,
}

#[derive(Debug)]
struct BridgeReadinessState {
    bridge_name: &'static str,
    expected_cidr: &'static str,
    attempts: u32,
    started_at: Option<Instant>,
    last_attempt_completed: Option<Instant>,
    last_observation: Option<BridgeObservation>,
    last_error: Option<String>,
    ready: bool,
}

impl BridgeReadinessState {
    fn new() -> Self {
        BridgeReadinessState {
            bridge_name: "",
            expected_cidr: "",
            attempts: 0,
            started_at: None,
            last_attempt_completed: None,
            last_observation: None,
            last_error: None,
            ready: false,
        }
    }

    fn reset(&mut self, bridge_name: &'static str, expected_cidr: &'static str) {
        self.bridge_name = bridge_name;
        self.expected_cidr = expected_cidr;
        self.attempts = 0;
        self.started_at = Some(Instant::now());
        self.last_attempt_completed = None;
        self.last_observation = None;
        self.last_error = None;
        self.ready = false;
    }

    fn prepare_attempt(&mut self) {
        self.attempts = self.attempts.saturating_add(1);
        self.last_error = None;
    }

    fn record_observation(&mut self, observation: BridgeObservation) {
        self.last_observation = Some(observation);
    }

    fn snapshot(&self) -> BridgeReadinessSnapshot {
        BridgeReadinessSnapshot {
            bridge_name: self.bridge_name,
            expected_cidr: self.expected_cidr,
            ready: self.ready,
            attempts: self.attempts,
            started_at: self.started_at,
            last_attempt_completed: self.last_attempt_completed,
            last_error: self.last_error.clone(),
            last_observation: self.last_observation.clone(),
        }
    }
}
