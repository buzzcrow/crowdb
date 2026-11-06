// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod node_args;

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
        #[arg(long, default_value_t = std::net::SocketAddr::from(([0, 0, 0, 0], crowdb_protocol::MONITOR_HTTP_BASE)))]
        bind: std::net::SocketAddr,
        #[arg(long, required = true)]
        interface: Vec<String>,
        #[arg(long, required = true)]
        advertise: Vec<std::net::IpAddr>,
        #[arg(long)]
        physical_host_id: String,
    },
    Run {
        #[command(flatten)]
        args: node_args::NodeRunArgs,
    },
    /// Seal startup policy before the container creates its SSH identity.
    AcceptPolicy {
        #[command(flatten)]
        args: node_args::NodeRunArgs,
    },
    Control {
        #[arg(long, default_value = "/opt/crowdb/run/node-control.sock")]
        socket: PathBuf,
        #[arg(long, default_value = "/opt/crowdb/data")]
        data_root: PathBuf,
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

fn main() -> std::process::ExitCode {
    match execute(Cli::parse()) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn execute(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    // Only long-running services need a worker pool. Socket control, probes and
    // other short commands must not inherit service thread-pool sizing.
    let mut builder = if matches!(&cli.command, Command::Node { .. } | Command::Run { .. }) {
        tokio::runtime::Builder::new_multi_thread()
    } else {
        tokio::runtime::Builder::new_current_thread()
    };
    builder.enable_all().build()?.block_on(run(cli))
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
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
        Command::Run { args } => node_args::run(args).await?,
        Command::AcceptPolicy { args } => println!("{}", node_args::accept(&args)?),
        Command::Control {
            socket,
            data_root,
            json,
        } => {
            if json.len() > 65536 {
                return Err("control request exceeds limit".into());
            }
            let request = serde_json::from_str(&json)?;
            let reply = if matches!(
                request,
                crowdb_protocol::mgmt::node::NodeControl::RecoveryIdentity
            ) {
                serde_json::to_value(
                    crowdb_monitor::read_recovery_identity(&data_root)
                        .await
                        .map_err(|error| error.to_string())?,
                )?
            } else {
                crowdb_monitor::control_node(&socket, &request)
                    .await
                    .map_err(|error| error.to_string())?
            };
            println!("{}", serde_json::to_string(&reply)?);
        }
        Command::Validate { profile } => {
            validate_profile(profile)?;
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
            let credential_root = if crowdb_monitor::read_node_policy(&data_root)?.is_some() {
                data_root.join("console")
            } else {
                data_root
            };
            print!("{}", show_client_credentials(&credential_root)?);
        }
    }
    Ok(())
}

fn validate_profile(path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let profile = DeploymentProfile::load(path)?;
    profile.validate_node_catalog()?;
    println!("{}", profile.name);
    Ok(())
}
