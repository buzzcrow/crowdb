// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use crowdb_monitor::{show_client_credentials, DeploymentProfile, StatusStore};

#[derive(Debug, Parser)]
#[command(name = "crowdb-monitor")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Validate { profile } => {
            let profile = DeploymentProfile::load(profile)?;
            println!("{}", profile.name);
        }
        Command::Liveness { run_root } => {
            StatusStore::open(&run_root)?.read(Duration::from_secs(10))?;
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
