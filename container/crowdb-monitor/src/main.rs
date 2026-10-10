// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use crowdb_monitor::{probe_liveness, run_preview, show_client_credentials, DeploymentProfile, StatusStore};

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
        data_root: PathBuf,
        #[arg(long, default_value = "0.0.0.0:9093")]
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
            data_root,
            bind,
            interface,
            advertise,
            physical_host_id,
        } => {
            let config = crowdb_monitor::DiscoveryConfig {
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
        Command::Run { profile } => run_preview(&profile).await?,
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
