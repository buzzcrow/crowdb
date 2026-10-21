// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Resolve startup sources once before handing accepted inputs to the node owner.

use std::{fs, path::PathBuf};

use clap::{Args, ValueEnum};
use crowdb_monitor::{DeploymentProfile, DiscoveryConfig, NodeRuntimeConfig};
use crowdb_protocol::mgmt::node::deployment::{
    DeploymentMode, NodeDeploymentOverrides, NodeDeploymentPolicy,
};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    Local,
    Production,
}

#[derive(Debug, Args)]
pub(crate) struct NodeRunArgs {
    #[arg(long, default_value = "/opt/crowdb/etc/profile.toml")]
    profile: PathBuf,
    #[arg(long, value_parser = ["single", "manual"])]
    mode: Option<String>,
    #[arg(long, default_value = "eth0")]
    interface: Vec<String>,
    #[arg(long, env = "CROWDB_PHYSICAL_HOST_ID")]
    physical_host_id: Option<String>,
    /// Optional TOML startup inputs; explicit arguments/environment take precedence.
    #[arg(long, env = "CROWDB_NODE_POLICY_FILE")]
    deployment_config: Option<PathBuf>,
    #[arg(long, value_enum, env = "CROWDB_DEPLOYMENT_MODE")]
    deployment_mode: Option<Mode>,
    #[arg(long, env = "CROWDB_CLUSTER")]
    cluster: Option<String>,
    /// Initial voting count; accepted membership is never automatically resized.
    #[arg(long, env = "CROWDB_INITIAL_VOTERS")]
    initial_voters: Option<u8>,
}

pub(crate) async fn run(args: NodeRunArgs) -> Result<(), Box<dyn std::error::Error>> {
    let file = read_inputs(args.deployment_config.as_deref())?;
    let explicit = NodeDeploymentOverrides {
        mode: args.deployment_mode.map(|mode| match mode {
            Mode::Local => DeploymentMode::Local,
            Mode::Production => DeploymentMode::Production,
        }),
        cluster: args.cluster,
        initial_voters: args.initial_voters,
        ..Default::default()
    };
    let profile = DeploymentProfile::load(&args.profile)?;
    let legacy_single = args.mode.as_deref() == Some("single");
    let accepted = if legacy_single {
        None
    } else {
        crowdb_monitor::read_node_policy(&profile.paths.data_root)?
    };
    let selected = explicit.resolve(&file, accepted.as_ref())?;
    let physical_host_id = physical_host(args.physical_host_id, &selected)?;
    let addresses = if_addrs::get_if_addrs()?
        .into_iter()
        .filter(|address| args.interface.contains(&address.name))
        .map(|address| address.ip())
        .collect();
    let config = NodeRuntimeConfig {
        profile: args.profile,
        bind: "0.0.0.0:9095".parse()?,
        discovery: DiscoveryConfig {
            interfaces: args.interface,
            addresses,
            monitor_port: 9095,
            cluster_id: None,
            seeds: std::env::var("CROWDB_DISCOVERY_SEEDS")
                .unwrap_or_default()
                .split(',')
                .filter(|seed| !seed.is_empty())
                .map(str::to_owned)
                .collect(),
        },
        physical_host_id,
    };
    let result = if legacy_single {
        if explicit != NodeDeploymentOverrides::default() || args.deployment_config.is_some() {
            return Err("single-profile mode cannot accept normal deployment inputs".into());
        }
        crowdb_monitor::run_single_node(config).await
    } else {
        crowdb_monitor::run_node_with_inputs(config, &file, &explicit).await
    };
    result.map_err(|error| error.to_string().into())
}

fn physical_host(value: Option<String>, policy: &NodeDeploymentPolicy) -> Result<String, &'static str> {
    match value {
        Some(value) if !value.is_empty() && value.len() <= 256 => Ok(value),
        Some(_) => Err("physical host identity must contain 1 to 256 bytes"),
        None if policy.mode == DeploymentMode::Local => Ok("unverified-local-host".into()),
        None => Err("production deployment requires physical-host-id"),
    }
}

fn read_inputs(
    path: Option<&std::path::Path>,
) -> Result<NodeDeploymentOverrides, Box<dyn std::error::Error>> {
    let Some(path) = path else {
        return Ok(NodeDeploymentOverrides::default());
    };
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() > 32768 {
        return Err("node deployment configuration must be a bounded regular TOML file".into());
    }
    Ok(toml::from_str(&fs::read_to_string(path)?)?)
}
