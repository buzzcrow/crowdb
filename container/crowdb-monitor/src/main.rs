// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use crowdb_monitor::{probe_liveness, show_client_credentials, DeploymentProfile, StatusStore};

#[derive(Debug, Parser)]
#[command(name = "crowdb-monitor")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the node discovery and read-only management endpoint.
    Node {
        #[arg(long)]
        seed: Vec<String>,
        #[arg(long)]
        data_root: PathBuf,
        #[arg(long, default_value = "0.0.0.0:9095")]
        bind: std::net::SocketAddr,
        #[arg(long, required = true)]
        interface: Vec<String>,
        #[arg(long, required = true)]
        advertise: Vec<std::net::IpAddr>,
        #[arg(long)]
        physical_host_id: String,
    },
    Run {
        #[arg(long, default_value = "/opt/crowdb/etc/profile.toml")]
        profile: PathBuf,
        #[arg(long, default_value = "single", value_parser = ["single", "manual"])]
        mode: String,
        #[arg(long, default_value = "eth0")]
        interface: Vec<String>,
        #[arg(long)]
        physical_host_id: Option<String>,
    },
    Control {
        #[arg(long, default_value = "/opt/crowdb/run/node-control.sock")]
        socket: PathBuf,
        #[arg(long)]
        json: String,
    },
    Validate {
        profile: PathBuf,
    },
    Liveness {
        #[arg(long, default_value = "/opt/crowdb/run")]
        run_root: PathBuf,
    },
    Readiness {
        #[arg(long, default_value = "/opt/crowdb/run")]
        run_root: PathBuf,
    },
    Credentials {
        #[command(subcommand)]
        command: CredentialsCommand,
    },
}

#[derive(Debug, Subcommand)]
enum CredentialsCommand {
    Show {
        #[arg(long, value_enum)]
        format: CredentialFormat,
        #[arg(long, default_value = "/opt/crowdb/data")]
        data_root: PathBuf,
    },
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum CredentialFormat {
    Env,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Node {
            seed,
            data_root,
            bind,
            interface,
            advertise,
            physical_host_id,
        } => {
            let config = crowdb_monitor::DiscoveryConfig {
                seeds: seed,
                interfaces: interface,
                addresses: advertise,
                monitor_port: bind.port(),
                cluster_id: None,
            };
            let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            crowdb_monitor::serve_node_management(&data_root, bind, &config, physical_host_id, async move {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if let Err(error) = result { eprintln!("node termination signal failed: {error}"); }
                    }
                    _ = terminate.recv() => {}
                }
            })
            .await?;
        }
        Command::Run {
            profile,
            mode,
            interface,
            physical_host_id,
        } => {
            let addresses = if_addrs::get_if_addrs()?
                .into_iter()
                .filter(|address| interface.contains(&address.name))
                .map(|address| address.ip())
                .collect();
            let config = crowdb_monitor::NodeRuntimeConfig {
                profile,
                bind: "0.0.0.0:9095".parse()?,
                discovery: crowdb_monitor::DiscoveryConfig {
                    interfaces: interface,
                    addresses,
                    monitor_port: 9095,
                    cluster_id: None,
                    seeds: std::env::var("CROWDB_DISCOVERY_SEEDS")
                        .unwrap_or_default()
                        .split(',')
                        .filter(|seed| !seed.is_empty())
                        .map(str::to_owned)
                        .collect(),
                },
                physical_host_id: match physical_host_id {
                    Some(id) => id,
                    None if mode == "single" => "unspecified-single-node-host".into(),
                    None => return Err("manual mode requires physical-host-id".into()),
                },
            };
            let result = if mode == "single" {
                crowdb_monitor::run_single_node(config).await
            } else {
                crowdb_monitor::run_node(config).await
            };
            result.map_err(|error| error.to_string())?;
        }
        Command::Control { socket, json } => {
            if json.len() > 65536 {
                return Err("control request exceeds limit".into());
            }
            let request = serde_json::from_str(&json)?;
            let reply = crowdb_monitor::control_node(&socket, &request)
                .await
                .map_err(|error| error.to_string())?;
            println!("{}", serde_json::to_string(&reply)?);
        }
        Command::Validate { profile } => {
            let profile = DeploymentProfile::load(profile)?;
            println!("{}", profile.name);
        }
        Command::Liveness { run_root } => {
            probe_liveness(&run_root).await?;
        }
        Command::Readiness { run_root } => {
            StatusStore::open(&run_root)?.readiness(Duration::from_secs(10))?;
        }
        Command::Credentials {
            command:
                CredentialsCommand::Show {
                    format: CredentialFormat::Env,
                    data_root,
                },
        } => {
            print!("{}", show_client_credentials(&data_root)?);
        }
    }
    Ok(())
}
