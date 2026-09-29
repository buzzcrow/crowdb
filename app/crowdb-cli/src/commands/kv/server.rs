// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `kv server` process controls backed by a launch-only registry.

use std::process::ExitCode;

use clap::Subcommand;

use crate::Cli;

mod registry;

#[derive(Subcommand, Debug)]
pub enum KvServerVerb {
    Deploy {
        #[arg(short = 'n', long)]
        node: String,
    },
    Start {
        #[arg(short = 'n', long)]
        node: String,
    },
    Restart {
        #[arg(short = 'n', long)]
        node: String,
    },
    Stop {
        #[arg(short = 'n', long)]
        node: String,
    },
    Delete {
        #[arg(short = 'n', long)]
        node: String,
    },
    List,
}

pub async fn run_kv_server_verb(cli: &Cli, verb: KvServerVerb) -> ExitCode {
    let Some(path) = &cli.registry else {
        eprintln!("error: kv server controls require --registry");
        return ExitCode::from(2);
    };
    registry::run(cli, path, verb).await
}
