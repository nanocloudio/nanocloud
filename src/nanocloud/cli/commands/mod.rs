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

mod bundles;
mod ca;
mod config;
mod devices;
mod diagnostics;
mod events;
mod exec;
mod install;
mod lifecycle;
mod logs;
mod policy;
mod restore;
mod status;
pub(crate) mod token;
mod volume;

use std::error::Error;
use std::fs;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::process;
use std::str::FromStr;

use crate::nanocloud::api::client::NanocloudClient;
use crate::nanocloud::cli::Setup;
use crate::nanocloud::dns::DnsConfig;
use crate::nanocloud::logger;
use crate::nanocloud::observability::tracing;
use crate::nanocloud::server::{self, ServerConfig};

use super::args::Commands;

pub async fn run(command: &Commands) -> Result<(), Box<dyn Error + Send + Sync>> {
    match command {
        Commands::Setup(args) => {
            Setup::run(args.repair);
            Ok(())
        }
        Commands::Server(args) => {
            logger::set_log_format(args.log_format.into());
            tracing::init();
            let addr: SocketAddr =
                args.listen
                    .parse()
                    .map_err(|e| -> Box<dyn Error + Send + Sync> {
                        Box::new(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("Invalid listen address '{}': {}", args.listen, e),
                        ))
                    })?;
            let dns_listen: IpAddr =
                args.dns_listen
                    .parse()
                    .map_err(|e| -> Box<dyn Error + Send + Sync> {
                        Box::new(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("Invalid DNS listen address '{}': {}", args.dns_listen, e),
                        ))
                    })?;
            let dns_upstream = if args.dns_upstream.is_empty() {
                default_upstream_from_host()
            } else {
                parse_upstream(&args.dns_upstream)?
            };
            let dns_config = DnsConfig::new(
                args.dns_cluster_domain.clone(),
                dns_listen,
                args.dns_port,
                args.dns_default_ttl,
                dns_upstream,
                args.dns_max_udp_payload,
            )
            .map_err(|e| -> Box<dyn Error + Send + Sync> {
                Box::new(io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))
            })?;
            server::serve(ServerConfig {
                http_listen: addr,
                dns: dns_config,
            })
            .await?;
            Ok(())
        }
        Commands::Config(args) => config::handle_config(args).await,
        Commands::Token(args) => token::handle_token(args),
        Commands::Ca(args) => {
            let client = NanocloudClient::new()?;
            ca::handle_ca(&client, args).await
        }
        Commands::Install(args) => {
            let client = NanocloudClient::new()?;
            install::handle_install(&client, args).await
        }
        Commands::Uninstall(args) => {
            let client = NanocloudClient::new()?;
            install::handle_uninstall(&client, args).await
        }
        Commands::Start(args) => {
            let client = NanocloudClient::new()?;
            lifecycle::handle_simple_action(&client, args, lifecycle::ServiceAction::Start).await
        }
        Commands::Stop(args) => {
            let client = NanocloudClient::new()?;
            lifecycle::handle_simple_action(&client, args, lifecycle::ServiceAction::Stop).await
        }
        Commands::Restart(args) => {
            let client = NanocloudClient::new()?;
            lifecycle::handle_simple_action(&client, args, lifecycle::ServiceAction::Restart).await
        }
        Commands::Logs(args) => {
            let client = NanocloudClient::new()?;
            logs::handle_logs(&client, args).await
        }
        Commands::Restore(args) => restore::handle_restore(args).await,
        Commands::Exec(args) => {
            let client = NanocloudClient::new()?;
            let exit_code = exec::handle_exec(&client, args).await?;
            if exit_code != 0 {
                process::exit(exit_code);
            }
            Ok(())
        }
        Commands::Status(args) => {
            let client = NanocloudClient::new()?;
            status::handle_status(&client, args).await
        }
        Commands::Diagnostics(args) => {
            let exit_code = diagnostics::handle_diagnostics(args).await?;
            if exit_code != 0 {
                process::exit(exit_code);
            }
            Ok(())
        }
        Commands::Policy(args) => {
            let client = NanocloudClient::new()?;
            policy::handle_policy(&client, args).await
        }
        Commands::Events(args) => {
            let client = NanocloudClient::new()?;
            events::handle_events(&client, args).await
        }
        Commands::Device(args) => {
            let client = NanocloudClient::new()?;
            devices::handle_devices(&client, args).await
        }
        Commands::Volume(args) => volume::handle_volume(args).await,
        Commands::Bundle(args) => {
            let client = NanocloudClient::new()?;
            bundles::handle_bundle(&client, args).await
        }
    }
}

fn parse_upstream(values: &[String]) -> Result<Vec<SocketAddr>, Box<dyn Error + Send + Sync>> {
    let mut parsed = Vec::new();
    for entry in values {
        let addr: SocketAddr = entry.parse().map_err(|e| -> Box<dyn Error + Send + Sync> {
            Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Invalid upstream server '{}': {}", entry, e),
            ))
        })?;
        parsed.push(addr);
    }
    Ok(parsed)
}

fn default_upstream_from_host() -> Vec<SocketAddr> {
    let resolv_conf = fs::read_to_string("/etc/resolv.conf").unwrap_or_default();
    resolv_conf
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.starts_with('#') || !line.starts_with("nameserver") {
                return None;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 2 {
                return None;
            }
            IpAddr::from_str(parts[1]).ok()
        })
        .map(|ip| SocketAddr::new(ip, 53))
        .collect()
}
