// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One production node per host, with runtime ownership separate from application identity.

mod args;
mod helper;
use super::{
    binding_path, load_target,
    state::{self, invalid, Launch},
    StartArgs,
};
pub(super) use args::HostArgs;
use args::{Network, Runtime};
use crowdb_console_shared::{
    deployment::target::{ClusterTargetBinding, RuntimeOwnership, RuntimeSelection},
    error::Result,
};
use crowdb_protocol::mgmt::cluster::ClusterPhase;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::PathBuf;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeIdentity {
    selection: RuntimeSelection,
    daemon: String,
    snapshotter: String,
    binary: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HostLaunch {
    version: u8,
    cluster: String,
    owner: String,
    name: String,
    runtime: RuntimeIdentity,
    runtime_image: String,
    image_id: String,
    options: Value,
    disks: Value,
    file_hashes: Value,
    pub(super) origins: Vec<String>,
}

fn path(name: &str) -> Result<PathBuf> {
    state::path(
        name,
        &crowdb_protocol::port::namespace::runtime_root().join("persistent/oci-hosts"),
    )
}

pub(super) fn load(name: &str) -> Result<Option<HostLaunch>> {
    state::read_json(&path(name)?)?
        .map(|value| {
            let record: HostLaunch =
                serde_json::from_value(value).map_err(|_| invalid("invalid host launch receipt"))?;
            record.validate(name)?;
            Ok(record)
        })
        .transpose()
}

impl HostLaunch {
    fn validate(&self, name: &str) -> Result<()> {
        if self.version != 1
            || self.cluster != name
            || self.name != format!("crowdb-{name}-host")
            || uuid::Uuid::parse_str(&self.owner)
                .map_or(true, |id| id.is_nil() || id.to_string() != self.owner)
            || !matches!(self.runtime.selection.runtime.as_str(), "docker" | "containerd")
            || self.runtime.selection.endpoint.is_empty()
            || self.runtime.daemon.is_empty()
            || self.options["network"] != "host"
            || !matches!(self.options["nodes"].as_u64(), Some(1 | 3))
            || self.origins.len() != 1
        {
            return Err(invalid("invalid host runtime identity or deployment policy"));
        }
        crowdb_console_shared::deployment::target::management_origins(&self.origins)
            .map_err(|error| invalid(&error.to_string()))?;
        Ok(())
    }
    fn ownership(&self) -> RuntimeOwnership {
        RuntimeOwnership {
            selection: self.runtime.selection.clone(),
            owner: self.owner.clone(),
        }
    }
    fn check_binding(&self) -> Result<Option<ClusterTargetBinding>> {
        let target = load_target(&self.cluster)?;
        if target.as_ref().is_some_and(|binding| {
            binding.runtime.as_ref() != Some(&self.ownership())
                || binding.management_endpoints != self.origins
        }) {
            return Err(invalid("cluster target differs from accepted host ownership"));
        }
        Ok(target)
    }
    fn value(&self) -> Result<Value> {
        serde_json::to_value(self).map_err(|_| invalid("invalid host launch receipt"))
    }
}

pub(super) fn selected(name: &str, args: &StartArgs) -> Result<bool> {
    let previous = load(name)?;
    if previous.is_some() || args.host.network == Some(Network::Host) {
        if Launch::load(name, &state::directory())?.is_some() {
            return Err(invalid("cluster already has a Docker bridge ownership receipt"));
        }
        if args.port_base.is_some() {
            return Err(invalid(
                "host networking uses port 9090; --port-base is for a local bridge",
            ));
        }
        return Ok(true);
    }
    if args.host.runtime == Some(Runtime::Containerd) || args.host.has_host_inputs() {
        return Err(invalid(
            "host/containerd inputs require --network host; local bridge uses Docker",
        ));
    }
    Ok(false)
}

pub(super) async fn start(args: &StartArgs, name: &str) -> Result<bool> {
    let previous = load(name)?;
    if previous.is_none() && state::read_json(&binding_path(name))?.is_some() {
        return Err(invalid(
            "selected application binding has no host runtime ownership receipt",
        ));
    }
    if let Some(record) = &previous {
        record.check_binding()?;
    }
    let mut options = serde_json::to_value(&args.host).map_err(|_| invalid("invalid host options"))?;
    for (key, value) in [
        ("nodes", json!(args.nodes)),
        ("image", json!(args.image)),
        ("cpus", json!(args.cpus)),
        ("memory_mib", json!(args.memory_mib)),
    ] {
        options[key] = value;
    }
    let value=helper::invoke(json!({"operation":"prepare","cluster":name,"options":options,"record":previous.map(|record|record.value()).transpose()?})).await?;
    let record: HostLaunch =
        serde_json::from_value(value.clone()).map_err(|_| invalid("invalid accepted host receipt"))?;
    record.validate(name)?;
    state::seal_json(&path(name)?, &value)?;
    let target = record.check_binding()?;
    helper::invoke(json!({"operation":"run","cluster":name,"options":{},"record":value})).await?;
    let report = super::super::status::observe_origins(&record.origins, target.as_ref(), true, args.wait, 10)
        .await
        .map_err(|error| invalid(&error))?;
    if report.observation.cluster != name
        || Some(u64::from(report.observation.initial_voters)) != record.options["nodes"].as_u64()
    {
        return Err(invalid("host endpoints report a different deployment policy"));
    }
    if let Some(cluster_id) = &report.observation.cluster_id {
        let binding = ClusterTargetBinding {
            version: 1,
            name: name.into(),
            cluster_id: cluster_id.clone(),
            management_endpoints: record.origins.clone(),
            runtime: Some(record.ownership()),
        };
        state::seal_json(
            &binding_path(name),
            &serde_json::to_value(binding).map_err(|_| invalid("invalid host target"))?,
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
            name,
            report.phase,
            report.observation.cluster_id.as_deref().unwrap_or("unbound")
        );
    }
    Ok(report.phase == ClusterPhase::Ready)
}

pub(super) async fn stop(name: &str, record: HostLaunch) -> Result<()> {
    if Launch::load(name, &state::directory())?.is_some() {
        return Err(invalid("conflicting host and bridge ownership receipts"));
    }
    record.check_binding()?;
    helper::invoke(json!({"operation":"stop","cluster":name,"record":record.value()?})).await?;
    println!("cluster {name}: stopped; retained data and ownership");
    Ok(())
}
