// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use crowdb_monitor::{show_client_credentials, DeploymentProfile};

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
