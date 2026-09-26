// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use crowdb_monitor::DeploymentProfile;

#[derive(Debug, Parser)]
#[command(name = "crowdb-monitor")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Validate { profile: PathBuf },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Validate { profile } => {
            let profile = DeploymentProfile::load(profile)?;
            println!("{}", profile.name);
        }
    }
    Ok(())
}
