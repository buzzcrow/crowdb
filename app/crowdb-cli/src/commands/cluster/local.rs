// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Runtime effects use host ownership receipts; readiness uses application evidence.

mod docker;
mod launch;
mod state;

use crate::Cli;
use clap::Args;
use crowdb_console_shared::{
    deployment::target::{ClusterTargetBinding, RuntimeOwnership, RuntimeSelection},
    error::Result,
};
use crowdb_protocol::mgmt::cluster::ClusterPhase;
use docker::{owned, strings, Docker};
use state::{invalid, Launch};
use std::process::ExitCode;

#[derive(Args, Debug)]
pub struct StartArgs {
    /// Initial voter count, used only when accepting a new launch.
    #[arg(long, value_parser = clap::value_parser!(u8).range(1..=3))]
    nodes: Option<u8>,
    /// Already loaded CROWDB node image; start never builds or pulls.
    #[arg(long)]
    image: Option<String>,
    /// First localhost console port; each node reserves four ports, ten apart.
    #[arg(long)]
    port_base: Option<u16>,
    #[arg(long)]
    cpus: Option<f64>,
    #[arg(long)]
    memory_mib: Option<u64>,
    /// Application readiness wait in seconds; zero observes once.
    #[arg(long, default_value_t = 180)]
    wait: u64,
    #[arg(long)]
    json: bool,
}

fn selection(cli: &Cli) -> Result<&str> {
    if !cli.management_endpoints.is_empty() {
        return Err(invalid(
            "runtime start/stop requires a local cluster name, not management endpoints",
        ));
    }
    let name = cli.cluster.as_deref().unwrap_or("local");
    if !state::name_valid(name) {
        return Err(invalid("invalid cluster name"));
    }
    Ok(name)
}

fn binding_path(name: &str) -> std::path::PathBuf {
    crowdb_protocol::port::namespace::runtime_root()
        .join("persistent/clusters")
        .join(name)
        .join("target.json")
}

fn binding(record: &Launch) -> Result<Option<ClusterTargetBinding>> {
    let Some(binding) = load_target(&record.cluster)? else {
        return Ok(None);
    };
    let expected = ownership(record);
    if binding.version != 1
        || binding.name != record.cluster
        || binding.runtime.as_ref() != Some(&expected)
        || binding.management_endpoints != record.origins()
        || uuid::Uuid::parse_str(&binding.cluster_id)
            .map_or(true, |id| id.is_nil() || id.to_string() != binding.cluster_id)
    {
        return Err(invalid(
            "cluster target binding differs from accepted runtime ownership",
        ));
    }
    Ok(Some(binding))
}

pub(super) fn load_target(name: &str) -> Result<Option<ClusterTargetBinding>> {
    if !state::name_valid(name) {
        return Err(invalid("invalid cluster name"));
    }
    let Some(value) = state::read_json(&binding_path(name))? else {
        return Ok(None);
    };
    let binding: ClusterTargetBinding =
        serde_json::from_value(value).map_err(|_| invalid("invalid selected cluster target binding"))?;
    if binding.version != 1
        || binding.name != name
        || uuid::Uuid::parse_str(&binding.cluster_id)
            .map_or(true, |id| id.is_nil() || id.to_string() != binding.cluster_id)
    {
        return Err(invalid("selected cluster target binding has an invalid identity"));
    }
    crowdb_console_shared::deployment::target::management_origins(&binding.management_endpoints)
        .map_err(|error| invalid(&error.to_string()))?;
    Ok(Some(binding))
}

pub(super) fn pending_origins(name: &str) -> Result<Vec<String>> {
    Launch::load(name, &state::directory())?
        .map(|record| record.origins())
        .ok_or_else(|| {
            invalid("selected cluster has no readable target binding or runtime ownership receipt")
        })
}

fn ownership(record: &Launch) -> RuntimeOwnership {
    RuntimeOwnership {
        selection: RuntimeSelection {
            runtime: "docker".into(),
            endpoint: record.runtime.endpoint.clone(),
            context: record.runtime.context.clone(),
        },
        owner: record.owner.clone(),
    }
}

pub(super) async fn start(cli: &Cli, args: &StartArgs) -> ExitCode {
    match start_inner(cli, args).await {
        Ok(ready) => {
            if ready {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            }
        }
        Err(error) => failure(error),
    }
}

async fn start_inner(cli: &Cli, args: &StartArgs) -> Result<bool> {
    let name = selection(cli)?;
    if args.nodes.is_some_and(|nodes| !matches!(nodes, 1 | 3)) {
        return Err(invalid("initial voter count must be one or three"));
    }
    // Existing target ownership must be checked before any runtime mutation.
    if Launch::load(name, &state::directory())?.is_none() && state::read_json(&binding_path(name))?.is_some()
    {
        return Err(invalid(
            "selected application binding has no local runtime ownership receipt",
        ));
    }
    let record = launch::accepted(&Docker, name, args).await?;
    let previous = binding(&record)?;
    launch::run(&Docker, &record).await?;
    let report = super::status::observe_origins(&record.origins(), previous.as_ref(), true, args.wait, 10)
        .await
        .map_err(|error| invalid(&error))?;
    if report.observation.cluster != record.cluster || report.observation.initial_voters != record.nodes {
        return Err(invalid("runtime endpoints report a different deployment policy"));
    }
    if let Some(cluster_id) = &report.observation.cluster_id {
        let target = ClusterTargetBinding {
            version: 1,
            name: record.cluster.clone(),
            cluster_id: cluster_id.clone(),
            management_endpoints: record.origins(),
            runtime: Some(ownership(&record)),
        };
        state::seal_json(
            &binding_path(name),
            &serde_json::to_value(target).map_err(|_| invalid("invalid application target"))?,
        )?;
    }
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|_| invalid("invalid status output"))?
        );
    } else {
        println!(
            "cluster {}: {:?} (UUID {})",
            record.cluster,
            report.phase,
            report.observation.cluster_id.as_deref().unwrap_or("unbound")
        );
        for fact in &report.observation.dependencies {
            println!("  {}: {:?}: {}", fact.id, fact.state, fact.detail);
        }
        for reason in &report.reasons {
            println!("  {}: {}: {}", reason.dependency, reason.code, reason.detail);
        }
    }
    Ok(report.phase == ClusterPhase::Ready)
}

pub(super) async fn stop(cli: &Cli) -> ExitCode {
    match stop_inner(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => failure(error),
    }
}

async fn stop_inner(cli: &Cli) -> Result<()> {
    let name = selection(cli)?;
    let record = Launch::load(name, &state::directory())?
        .ok_or_else(|| invalid("selected cluster has no local runtime ownership receipt"))?;
    binding(&record)?;
    Docker.check(&record).await?;
    for item in Docker.objects("container").await? {
        if owned(&item, &record, "container") && item["State"]["Running"] == true {
            let identity = item["Id"]
                .as_str()
                .ok_or_else(|| invalid("container ID absent"))?;
            let current = Docker.inspect("container", identity).await?;
            docker::require_owned(&current, &record, "container")?;
            Docker.check(&record).await?;
            Docker.call(&strings(&["stop", identity])).await?;
        }
    }
    println!("cluster {name}: stopped; retained data and ownership");
    Ok(())
}

fn failure(error: impl std::fmt::Display) -> ExitCode {
    eprintln!("error: cluster lifecycle: {error}");
    ExitCode::from(2)
}
