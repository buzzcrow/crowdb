// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::Path;
use std::process::ExitCode;

use crowdb_console_shared::config::web::LaunchRegistry;
use crowdb_console_shared::error::{Error, Result};
use crowdb_console_shared::launch::LaunchRuntime;

use super::KvServerVerb;
use crate::Cli;

pub(super) async fn run(cli: &Cli, path: &Path, verb: KvServerVerb) -> ExitCode {
    match execute(cli, path, verb).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(2)
        }
    }
}

async fn execute(cli: &Cli, path: &Path, verb: KvServerVerb) -> Result<()> {
    let mut registry = LaunchRegistry::load(path)?;
    let runtime = LaunchRuntime::for_registry(path)?;
    if matches!(verb, KvServerVerb::List) {
        println!("{:<12}  {:<26}  PID", "NODE", "HOST");
        for launch in registry
            .launches
            .iter()
            .filter(|launch| launch.service_id == "kv")
        {
            let identity = runtime.status(launch).await?;
            println!(
                "{:<12}  {:<26}  {}",
                launch.node_id,
                launch.host,
                identity.map_or_else(|| "stopped".into(), |identity| identity.pid.to_string())
            );
        }
        return Ok(());
    }
    let node = match &verb {
        KvServerVerb::Deploy { node, .. }
        | KvServerVerb::Start { node }
        | KvServerVerb::Restart { node }
        | KvServerVerb::Stop { node }
        | KvServerVerb::Delete { node } => node,
        KvServerVerb::List => unreachable!(),
    }
    .parse::<u64>()
    .map_err(|error| Error::Validation {
        field: "node".into(),
        message: error.to_string(),
    })?;
    let launch = registry
        .launches
        .iter()
        .find(|launch| launch.node_id == node && launch.service_id == "kv")
        .cloned()
        .ok_or_else(|| Error::NotFound {
            kind: "configured kv launch".into(),
            id: node.to_string(),
        })?;
    match verb {
        KvServerVerb::Deploy {
            rest_port,
            rpc_port,
            binary,
            ..
        } => {
            if rest_port.is_some() || rpc_port.is_some() || binary.is_some() {
                return Err(Error::Validation {
                    field: "registry".into(),
                    message: "binary and listener arguments must come from the launch registry".into(),
                });
            }
            let identity = runtime.start(&launch).await?;
            println!("started kv on node {node} (pid {})", identity.pid);
        }
        KvServerVerb::Start { .. } => {
            let identity = runtime.start(&launch).await?;
            println!("started kv on node {node} (pid {})", identity.pid);
        }
        KvServerVerb::Restart { .. } => {
            let identity = runtime.restart(&launch).await?;
            println!("restarted kv on node {node} (pid {})", identity.pid);
        }
        KvServerVerb::Stop { .. } => {
            runtime.stop(&launch).await?;
            println!("stopped kv on node {node}");
        }
        KvServerVerb::Delete { .. } => {
            let ctx = crate::commands::op_context(cli)
                .map_err(|_| Error::Config("cannot initialize authority client".into()))?;
            if ctx
                .sysmd()
                .list_all_replicas()
                .await?
                .iter()
                .any(|replica| replica.node_id == node)
            {
                return Err(Error::Conflict {
                    kind: "node with replica membership".into(),
                    id: node.to_string(),
                });
            }
            runtime.stop(&launch).await?;
            registry
                .launches
                .retain(|record| record.node_id != node || record.service_id != "kv");
            registry.save(path)?;
            println!("removed kv launch for node {node}");
        }
        KvServerVerb::List => unreachable!(),
    }
    Ok(())
}
