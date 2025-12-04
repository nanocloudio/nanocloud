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
mod log_stream;
mod logs;
mod policy;
mod restore;
mod status;
pub(crate) mod token;
mod volume;
mod watch_parser;

use std::error::Error;
use std::fs;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::str::FromStr;

use crate::nanocloud::api::client::NanocloudClient;
use crate::nanocloud::cli::{Setup, Terminal};
use crate::nanocloud::dns::DnsConfig;
use crate::nanocloud::logger;
use crate::nanocloud::observability::{self, TelemetryConfig};
use crate::nanocloud::server::{self, ServerConfig};

use super::args::Commands;
pub(crate) use log_stream::{stream_logs_to_terminal, LogStreamConfig};
pub(crate) use watch_parser::{parse_json_lines, WatchParseError};

pub struct CommandContext {
    pub server: Option<ServerBootstrap>,
    pub telemetry: Option<observability::TelemetryHandle>,
}

pub struct ServerBootstrap {
    pub config: ServerConfig,
}

pub fn bootstrap(command: &Commands) -> Result<CommandContext, Box<dyn Error + Send + Sync>> {
    match command {
        Commands::Server(args) => {
            logger::set_log_format(args.log_format.into());
            let telemetry_config = TelemetryConfig::from_env();
            let telemetry = observability::init(&telemetry_config).map_err(
                |err| -> Box<dyn Error + Send + Sync> {
                    Box::new(io::Error::other(format!(
                        "telemetry initialization failed: {}",
                        err
                    )))
                },
            )?;

            let addr: SocketAddr =
                args.listen
                    .parse()
                    .map_err(|e| -> Box<dyn Error + Send + Sync> {
                        Box::new(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("Invalid listen address '{}': {}", args.listen, e),
                        ))
                    })?;
            let dns_listen: SocketAddr =
                args.dns_listen
                    .parse()
                    .map_err(|e| -> Box<dyn Error + Send + Sync> {
                        Box::new(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                            "Invalid DNS listen address '{}'. Use HOST:PORT (e.g. 0.0.0.0:53): {}",
                            args.dns_listen, e
                        ),
                        ))
                    })?;
            let dns_upstream = if args.dns_upstream.is_empty() {
                match default_upstream_from_host() {
                    Ok(upstreams) => upstreams,
                    Err(err) => {
                        Terminal::stderr(format_args!(
                            "Warning: failed to read DNS upstreams from host: {}. Continuing without upstreams.",
                            err
                        ));
                        Vec::new()
                    }
                }
            } else {
                parse_upstream(&args.dns_upstream)?
            };
            let defaults = DnsConfig::default();
            let dns_config = DnsConfig::new(
                args.dns_cluster_domain.clone(),
                dns_listen.ip(),
                dns_listen.port(),
                defaults.default_ttl_seconds,
                dns_upstream,
                defaults.max_udp_payload_size,
            )
            .map(|mut cfg| {
                cfg.handler_concurrency = args.dns_handler_concurrency;
                cfg
            })
            .and_then(|cfg| cfg.validate().map(|_| cfg))
            .map_err(|e| -> Box<dyn Error + Send + Sync> {
                Box::new(io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))
            })?;
            Ok(CommandContext {
                server: Some(ServerBootstrap {
                    config: ServerConfig {
                        http_listen: addr,
                        dns: dns_config,
                    },
                }),
                telemetry: Some(telemetry),
            })
        }
        _ => {
            let telemetry = observability::init(&TelemetryConfig::noop()).ok();
            Ok(CommandContext {
                server: None,
                telemetry,
            })
        }
    }
}

fn unit_to_exit(
    result: Result<(), Box<dyn Error + Send + Sync>>,
) -> Result<i32, Box<dyn Error + Send + Sync>> {
    result.map(|_| 0)
}

pub async fn run(
    command: &Commands,
    context: CommandContext,
) -> Result<i32, Box<dyn Error + Send + Sync>> {
    match command {
        Commands::Setup(args) => unit_to_exit(
            Setup::run(args.repair).map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>),
        ),
        Commands::Server(_) => {
            let CommandContext { server, telemetry } = context;
            let server_ctx = server.ok_or_else(|| -> Box<dyn Error + Send + Sync> {
                Box::new(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "server configuration was not prepared",
                ))
            })?;
            let result = server::serve(server_ctx.config).await;
            if let Some(handle) = telemetry.as_ref() {
                handle.shutdown();
            }
            unit_to_exit(result)
        }
        Commands::Config(args) => unit_to_exit(config::handle_config(args).await),
        Commands::Token(args) => unit_to_exit(token::handle_token(args)),
        Commands::Ca(args) => {
            let client = NanocloudClient::new()?;
            unit_to_exit(ca::handle_ca(&client, args).await)
        }
        Commands::Install(args) => {
            let client = NanocloudClient::new()?;
            unit_to_exit(install::handle_install(&client, args).await)
        }
        Commands::Uninstall(args) => {
            let client = NanocloudClient::new()?;
            unit_to_exit(install::handle_uninstall(&client, args).await)
        }
        Commands::Start(args) => {
            let client = NanocloudClient::new()?;
            unit_to_exit(
                lifecycle::handle_simple_action(&client, args, lifecycle::ServiceAction::Start)
                    .await,
            )
        }
        Commands::Stop(args) => {
            let client = NanocloudClient::new()?;
            unit_to_exit(
                lifecycle::handle_simple_action(&client, args, lifecycle::ServiceAction::Stop)
                    .await,
            )
        }
        Commands::Restart(args) => {
            let client = NanocloudClient::new()?;
            unit_to_exit(
                lifecycle::handle_simple_action(&client, args, lifecycle::ServiceAction::Restart)
                    .await,
            )
        }
        Commands::Logs(args) => {
            let client = NanocloudClient::new()?;
            unit_to_exit(logs::handle_logs(&client, args).await)
        }
        Commands::Restore(args) => unit_to_exit(restore::handle_restore(args).await),
        Commands::Exec(args) => {
            let client = NanocloudClient::new()?;
            exec::handle_exec(&client, args).await
        }
        Commands::Status(args) => {
            let client = NanocloudClient::new()?;
            unit_to_exit(status::handle_status(&client, args).await)
        }
        Commands::Diagnostics => diagnostics::handle_diagnostics().await,
        Commands::Policy(args) => {
            let client = NanocloudClient::new()?;
            unit_to_exit(policy::handle_policy(&client, args).await)
        }
        Commands::Events(args) => {
            let client = NanocloudClient::new()?;
            unit_to_exit(events::handle_events(&client, args).await)
        }
        Commands::Device(args) => {
            let client = NanocloudClient::new()?;
            unit_to_exit(devices::handle_devices(&client, args).await)
        }
        Commands::Volume(args) => unit_to_exit(volume::handle_volume(args).await),
        Commands::Bundle(args) => {
            let client = NanocloudClient::new()?;
            unit_to_exit(bundles::handle_bundle(&client, args).await)
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

fn default_upstream_from_host() -> Result<Vec<SocketAddr>, Box<dyn Error + Send + Sync>> {
    default_upstream_from_path(Path::new("/etc/resolv.conf"))
}

fn default_upstream_from_path(
    path: &Path,
) -> Result<Vec<SocketAddr>, Box<dyn Error + Send + Sync>> {
    let resolv_conf = fs::read_to_string(path).map_err(|err| {
        Box::new(io::Error::other(format!(
            "Unable to read {}: {err}",
            path.display()
        ))) as Box<dyn Error + Send + Sync>
    })?;

    let mut parsed = Vec::new();
    for (idx, line) in resolv_conf.lines().enumerate() {
        let line = line.trim();
        if line.starts_with('#') || !line.starts_with("nameserver") {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 2 {
            Terminal::stderr(format_args!(
                "Warning: ignoring malformed nameserver entry on line {}",
                idx + 1
            ));
            continue;
        }
        match IpAddr::from_str(parts[1]) {
            Ok(ip) => parsed.push(SocketAddr::new(ip, 53)),
            Err(err) => Terminal::stderr(format_args!(
                "Warning: invalid nameserver '{}' on line {}: {}",
                parts[1],
                idx + 1,
                err
            )),
        }
    }

    if parsed.is_empty() {
        return Err(Box::new(io::Error::new(
            io::ErrorKind::NotFound,
            "no valid nameserver entries found in /etc/resolv.conf",
        )));
    }

    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn unit_to_exit_maps_success_and_errors() {
        let ok_result = unit_to_exit(Ok(())).expect("success should map to zero");
        assert_eq!(ok_result, 0);

        let err = unit_to_exit(Err(Box::new(io::Error::other("boom"))))
            .expect_err("error should propagate");
        assert!(format!("{err}").contains("boom"));
    }

    #[test]
    fn parse_upstream_rejects_invalid_entries() {
        let err =
            parse_upstream(&["not-an-addr".to_string()]).expect_err("invalid address should fail");
        assert!(err.to_string().contains("Invalid upstream server"));
    }

    #[test]
    fn default_upstream_from_path_errors_on_missing_or_invalid() {
        let tmp = TempDir::new().expect("tmpdir");
        let bad_path = tmp.path().join("resolv.conf");
        // Write a malformed entry
        let mut file = File::create(&bad_path).expect("create");
        writeln!(file, "nameserver not-an-ip").expect("write");
        drop(file);

        let err = default_upstream_from_path(&bad_path).expect_err("invalid entry");
        assert!(err
            .to_string()
            .contains("no valid nameserver entries found"));

        let missing_path = tmp.path().join("missing");
        let missing_err =
            default_upstream_from_path(&missing_path).expect_err("missing resolv.conf");
        assert!(missing_err.to_string().contains("Unable to read"));
    }

    #[test]
    fn default_upstream_from_path_parses_valid_entries() {
        let tmp = TempDir::new().expect("tmpdir");
        let path = tmp.path().join("resolv.conf");
        let mut file = File::create(&path).expect("create");
        writeln!(file, "nameserver 8.8.8.8").expect("write primary");
        writeln!(file, "nameserver 1.1.1.1").expect("write secondary");
        drop(file);

        let servers = default_upstream_from_path(&path).expect("parse upstreams");
        assert_eq!(servers.len(), 2);
        assert!(servers
            .iter()
            .any(|addr| addr.ip().to_string() == "8.8.8.8"));
        assert!(servers
            .iter()
            .any(|addr| addr.ip().to_string() == "1.1.1.1"));
    }
}
