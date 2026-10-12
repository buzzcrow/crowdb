// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Host CLI lifecycle for an owned three-node release-container cluster.

mod bootstrap;
mod maintenance;
mod runtime;
mod state;

use clap::Args;
use crowdb_console_shared::error::{Error, Result};
use serde_json::Value;
use state::{State, FILE, INITIALIZING, ROOT_LABEL};
use std::path::PathBuf;

#[derive(Args, Debug)]
#[group(required = true, multiple = true)]
pub(crate) struct Target {
    #[arg(long, alias = "data-dir")]
    pub root: Option<PathBuf>,
    #[arg(long, alias = "name")]
    pub container: Option<String>,
}

pub(crate) struct Status {
    pub root: PathBuf,
    pub container: String,
    pub image: String,
    pub running: usize,
    pub total: usize,
    pub endpoint: String,
    pub web: String,
}

pub(crate) async fn resolve(target: &Target, create: bool) -> Result<PathBuf> {
    if let Some(name) = &target.container {
        state::validate_member_name(name)?;
    }
    let mut observed = None;
    if let Some(name) = &target.container {
        if let Some(value) = runtime::inspect("container", name).await? {
            let path = value["Config"]["Labels"][ROOT_LABEL]
                .as_str()
                .ok_or_else(|| Error::Config("container has no cluster ownership label".into()))?;
            let root = std::fs::canonicalize(path)?;
            let state = State::load(&root)?;
            let index = (0..3)
                .find(|index| state.node_name(*index) == *name)
                .ok_or_else(|| Error::Config("container is not a recorded cluster member".into()))?;
            runtime::verify(&state, index, &value)?;
            observed = Some(root);
        }
    }
    let selected = target
        .root
        .clone()
        .or_else(|| observed.clone())
        .or_else(|| {
            target.container.as_ref().map(|name| {
                crowdb_protocol::port::namespace::runtime_root()
                    .join("persistent/container-clusters")
                    .join(name)
            })
        })
        .ok_or_else(|| Error::Config("select a cluster root or member container".into()))?;
    if create {
        std::fs::create_dir_all(&selected)?;
    }
    let root = std::fs::canonicalize(selected)?;
    if observed.is_some_and(|observed| observed != root) {
        return Err(Error::Config(
            "root and container select different clusters".into(),
        ));
    }
    Ok(root)
}

pub(crate) async fn start(
    target: &Target,
    image: Option<&str>,
    node_cpus: Option<f64>,
    memory_mib: Option<u64>,
    diskdb_heartbeat_interval_secs: Option<u32>,
) -> Result<Status> {
    let root = resolve(target, true).await?;
    let mut state = if root.join(FILE).is_file() {
        State::load(&root)?
    } else {
        if std::fs::read_dir(&root)?.next().transpose()?.is_some() {
            return Err(Error::Config(
                "refusing a non-empty unrecognized cluster root".into(),
            ));
        }
        let name = target
            .container
            .clone()
            .unwrap_or_else(|| format!("crowdb-cluster-{}", state::random_id()));
        let image = image.unwrap_or("crowdb-node:dev");
        let value = runtime::inspect("image", image)
            .await?
            .ok_or_else(|| Error::Config("formal release image is absent".into()))?;
        if value["Config"]["Labels"]["org.crowdb.runtime.sha256"]
            .as_str()
            .is_none()
            || value["Os"] != "linux"
        {
            return Err(Error::Config(
                "cluster needs the formal Linux CROWDB release image".into(),
            ));
        }
        let state = State {
            version: 1,
            id: state::random_id(),
            root: root.clone(),
            image: value["Id"]
                .as_str()
                .ok_or_else(|| Error::Config("image identity is absent".into()))?
                .into(),
            network: format!("{name}-network"),
            name,
            phase: "allocated".into(),
            node_cpus: node_cpus.unwrap_or(2.0),
            memory_mib: memory_mib.unwrap_or(2048),
            diskdb_heartbeat_interval_secs: diskdb_heartbeat_interval_secs
                .unwrap_or(state::DEFAULT_DISKDB_HEARTBEAT_INTERVAL_SECS),
            addresses: Vec::new(),
        };
        state.save()?;
        state
    };
    if let Some(image) = image {
        let current = runtime::inspect("image", image)
            .await?
            .ok_or_else(|| Error::Config("requested release image is absent".into()))?;
        if current["Id"] != state.image {
            return Err(Error::Config("existing cluster keeps its recorded release image; explicit upgrades require a separate operation".into()));
        }
    }
    validate_requested_settings(&state, node_cpus, memory_mib, diskdb_heartbeat_interval_secs)?;
    if target
        .container
        .as_ref()
        .is_some_and(|name| !(0..3).any(|index| state.node_name(index) == *name))
    {
        return Err(Error::Config(
            "container name does not select the recorded cluster".into(),
        ));
    }
    verify_members(&state).await?;
    runtime::ensure_network(&mut state).await?;
    runtime::ensure_nodes(&state).await?;
    state.phase = "nodes".into();
    state.save()?;
    bootstrap::run(&mut state).await?;
    state.phase = "ready".into();
    state.save()?;
    state.client_record()?;
    if root.join(INITIALIZING).exists() {
        std::fs::remove_file(root.join(INITIALIZING))?;
    }
    status(target).await
}

fn validate_requested_settings(
    state: &State,
    node_cpus: Option<f64>,
    memory_mib: Option<u64>,
    diskdb_heartbeat_interval_secs: Option<u32>,
) -> Result<()> {
    if node_cpus.is_some_and(|cpus| cpus.to_bits() != state.node_cpus.to_bits())
        || memory_mib.is_some_and(|memory| memory != state.memory_mib)
    {
        return Err(Error::Config(
            "existing cluster keeps its recorded resource limits".into(),
        ));
    }
    if diskdb_heartbeat_interval_secs.is_some_and(|interval| interval != state.diskdb_heartbeat_interval_secs)
    {
        return Err(Error::Config(
            "existing cluster keeps its recorded Diskdb heartbeat configuration".into(),
        ));
    }
    Ok(())
}

async fn verify_members(state: &State) -> Result<Vec<Option<Value>>> {
    let mut values = Vec::new();
    if let Some(network) = runtime::inspect("network", &state.network).await? {
        runtime::verify_network(state, &network)?;
    }
    for index in 0..3 {
        let value = runtime::inspect("container", &state.node_name(index)).await?;
        if let Some(value) = &value {
            runtime::verify(state, index, value)?;
        }
        values.push(value);
    }
    Ok(values)
}

pub(crate) async fn status(target: &Target) -> Result<Status> {
    let root = resolve(target, false).await?;
    let state = State::load(&root)?;
    let values = verify_members(&state).await?;
    let web = state.web().unwrap_or_default();
    Ok(Status {
        root,
        container: state.name,
        image: state.image,
        running: values
            .iter()
            .flatten()
            .filter(|v| v["State"]["Running"] == true)
            .count(),
        total: 3,
        endpoint: format!("{web}/api/access/s3"),
        web,
    })
}

pub(crate) async fn stop(target: &Target) -> Result<Status> {
    let root = resolve(target, false).await?;
    let mut state = State::load(&root)?;
    let values = verify_members(&state).await?;
    for index in (0..3).rev() {
        if values[index]
            .as_ref()
            .is_some_and(|value| value["State"]["Running"] == true)
        {
            runtime::docker(&[
                "stop".into(),
                "--time".into(),
                "10".into(),
                state.node_name(index),
            ])
            .await?;
        }
    }
    state.phase = "stopped".into();
    state.save()?;
    status(target).await
}

pub(crate) async fn restart(target: &Target) -> Result<Status> {
    stop(target).await?;
    start(target, None, None, None, None).await
}

pub(crate) async fn delete(target: &Target) -> Result<Status> {
    let previous = stop(target).await?;
    let state = State::load(&previous.root)?;
    let values = verify_members(&state).await?;
    for (index, value) in values.iter().enumerate() {
        if value.is_some() {
            runtime::docker(&["rm".into(), state.node_name(index)]).await?;
        }
    }
    if let Some(network) = runtime::inspect("network", &state.network).await? {
        runtime::verify_network(&state, &network)?;
        runtime::docker(&["network".into(), "rm".into(), state.network.clone()]).await?;
    }
    let nodes = state.root.join("nodes");
    if nodes.exists() {
        maintenance::run(
            &state,
            "delete-data",
            &nodes,
            "/bin/rm",
            &[
                "-rf",
                "/cluster/data/node0",
                "/cluster/data/node1",
                "/cluster/data/node2",
            ],
        )
        .await?;
        std::fs::remove_dir(nodes)?;
    }
    state::remove_temporary(&state)?;
    for file in [FILE, INITIALIZING, "s3-mini-cluster.json", "s3-local-state.toml"] {
        let path = state.root.join(file);
        if path.exists() {
            std::fs::remove_file(path)?;
        }
    }
    std::fs::remove_dir(&state.root)?;
    Ok(previous)
}
