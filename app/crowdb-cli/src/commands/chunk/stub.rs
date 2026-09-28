// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `chunk chunkdb/diskio` + data-plane stub command handlers.

use std::process::ExitCode;

use clap::Subcommand;

use crate::commands::launch::{self, LaunchVerb};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum ChunkStubVerb {
    #[command(subcommand)]
    Chunkdb(ServiceVerb),
    #[command(subcommand)]
    Diskio(ServiceVerb),
    Allocate,
    Free,
    Write,
    Read,
    Gc,
}

#[derive(Subcommand, Debug)]
pub enum ServiceVerb {
    Deploy {
        #[arg(short = 'n', long)]
        node: u64,
    },
    Start {
        #[arg(short = 'n', long)]
        node: u64,
    },
    Restart {
        #[arg(short = 'n', long)]
        node: u64,
    },
    Stop {
        #[arg(short = 'n', long)]
        node: u64,
    },
    List,
}

pub async fn run_chunk_stub_verb(cli: &Cli, verb: ChunkStubVerb) -> ExitCode {
    match verb {
        ChunkStubVerb::Chunkdb(verb) => run_service(cli, "chunkdb", verb).await,
        ChunkStubVerb::Diskio(verb) => run_service(cli, "diskio", verb).await,
        other => {
            eprintln!("chunk {other:?} — not yet implemented (Phase 3)");
            ExitCode::from(1)
        }
    }
}

async fn run_service(cli: &Cli, service: &str, verb: ServiceVerb) -> ExitCode {
    if matches!(verb, ServiceVerb::List) {
        return run_list(cli, service).await;
    }
    let service = service.into();
    let verb = match verb {
        ServiceVerb::Deploy { node } | ServiceVerb::Start { node } => LaunchVerb::Start { node, service },
        ServiceVerb::Restart { node } => LaunchVerb::Restart { node, service },
        ServiceVerb::Stop { node } => LaunchVerb::Stop { node, service },
        ServiceVerb::List => unreachable!(),
    };
    launch::run(cli, verb).await
}

async fn run_list(cli: &Cli, service: &str) -> ExitCode {
    let ctx = match crate::commands::op_context(cli) {
        Ok(ctx) => ctx,
        Err(code) => return code,
    };
    match ctx.sysmd().read_service_instances(service).await {
        Ok(instances) => {
            println!("living {service} instances ({}):", instances.len());
            for (id, instance) in instances {
                println!("  instance {id}: {}", instance.rpc_endpoint);
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("error: read {service} registration: {error}");
            ExitCode::from(2)
        }
    }
}
