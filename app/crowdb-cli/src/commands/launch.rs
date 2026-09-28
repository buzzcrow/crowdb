// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Process controls using launch policy, independent of cluster topology.

use std::process::ExitCode;

use clap::Subcommand;
use crowdb_console_shared::config::web::LaunchRegistry;
use crowdb_console_shared::error::{Error, Result};
use crowdb_console_shared::launch::LaunchRuntime;

use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum LaunchVerb {
    /// Show configured processes and their current runtime identities.
    List,
    /// Start a configured process, preserving an already running instance.
    Start {
        #[arg(long)]
        node: u64,
        #[arg(long)]
        service: String,
    },
    Restart {
        #[arg(long)]
        node: u64,
        #[arg(long)]
        service: String,
    },
    Stop {
        #[arg(long)]
        node: u64,
        #[arg(long)]
        service: String,
    },
}

pub(crate) async fn run(cli: &Cli, verb: LaunchVerb) -> ExitCode {
    match execute(cli, verb).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(2)
        }
    }
}

async fn execute(cli: &Cli, verb: LaunchVerb) -> Result<()> {
    let path = cli
        .registry
        .as_ref()
        .ok_or_else(|| Error::Config("process controls require --registry".into()))?;
    let registry = LaunchRegistry::load(path)?;
    let runtime = LaunchRuntime::for_registry(path)?;
    if matches!(verb, LaunchVerb::List) {
        println!("{:<12}  {:<16}  {:<26}  PID", "NODE", "SERVICE", "HOST");
        for launch in registry.launches {
            let identity = runtime.status(&launch).await?;
            println!(
                "{:<12}  {:<16}  {:<26}  {}",
                launch.node_id,
                launch.service_id,
                launch.host,
                identity.map_or_else(|| "stopped".into(), |identity| identity.pid.to_string())
            );
        }
        return Ok(());
    }
    let (node, service) = match &verb {
        LaunchVerb::Start { node, service }
        | LaunchVerb::Restart { node, service }
        | LaunchVerb::Stop { node, service } => (*node, service.as_str()),
        LaunchVerb::List => unreachable!(),
    };
    let launch = registry
        .launches
        .iter()
        .find(|launch| launch.node_id == node && launch.service_id == service)
        .ok_or_else(|| Error::NotFound {
            kind: "configured launch".into(),
            id: format!("{node}/{service}"),
        })?;
    match &verb {
        LaunchVerb::Start { .. } => {
            let identity = runtime.start(launch).await?;
            println!("started {service} on node {node} (pid {})", identity.pid);
        }
        LaunchVerb::Restart { .. } => {
            let identity = runtime.restart(launch).await?;
            println!("restarted {service} on node {node} (pid {})", identity.pid);
        }
        LaunchVerb::Stop { .. } => {
            runtime.stop(launch).await?;
            println!("stopped {service} on node {node}");
        }
        LaunchVerb::List => unreachable!(),
    }
    Ok(())
}
