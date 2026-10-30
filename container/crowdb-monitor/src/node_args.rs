// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Resolve startup sources once before handing accepted inputs to the node owner.

use std::{fs, io::Read, path::PathBuf};

use clap::{Args, ValueEnum};
use crowdb_monitor::{DeploymentProfile, DiscoveryConfig, NodeRuntimeConfig};
use crowdb_protocol::mgmt::node::deployment::{
    DeploymentMode, NodeDeploymentOverrides, NodeDeploymentPolicy, PublicAccessUris,
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
    #[arg(long, env = "CROWDB_S3_PUBLIC_URI")]
    s3_public_uri: Option<String>,
    #[arg(long, env = "CROWDB_ICEBERG_PUBLIC_URI")]
    iceberg_public_uri: Option<String>,
    #[arg(long, env = "CROWDB_DATASET_PUBLIC_URI")]
    dataset_public_uri: Option<String>,
}

pub(crate) async fn run(args: NodeRunArgs) -> Result<(), Box<dyn std::error::Error>> {
    let inputs = resolve(&args)?;
    let offset = port_offset()?;
    let monitor_port = shifted_port(9095, offset)?;
    let addresses = if_addrs::get_if_addrs()?
        .into_iter()
        .filter(|address| args.interface.contains(&address.name))
        .map(|address| address.ip())
        .collect();
    let config = NodeRuntimeConfig {
        profile: args.profile,
        bind: format!("0.0.0.0:{monitor_port}").parse()?,
        discovery: DiscoveryConfig {
            interfaces: args.interface,
            addresses,
            monitor_port,
            cluster_id: None,
            seeds: discovery_seeds(),
        },
        physical_host_id: inputs.physical_host_id,
    };
    let result = crowdb_monitor::run_node_with_inputs(config, &inputs.file, &inputs.explicit).await;
    result.map_err(|error| error.to_string().into())
}

fn port_offset() -> Result<u16, Box<dyn std::error::Error>> {
    let value = std::env::var("CROWDB_PORT_OFFSET").unwrap_or_default();
    if value.is_empty() {
        return Ok(0);
    }
    value
        .parse::<u16>()
        .map_err(|_| "CROWDB_PORT_OFFSET must be a non-negative integer fitting a port".into())
}

fn shifted_port(base: u16, offset: u16) -> Result<u16, Box<dyn std::error::Error>> {
    base.checked_add(offset)
        .ok_or_else(|| "CROWDB_PORT_OFFSET shifts a listener beyond port 65535".into())
}

pub(crate) fn accept(args: &NodeRunArgs) -> Result<&'static str, Box<dyn std::error::Error>> {
    let inputs = resolve(args)?;
    let policy = crowdb_monitor::accept_node_policy_with_port_offset(
        &inputs.data_root,
        &inputs.file,
        &inputs.explicit,
        port_offset()?,
    )?;
    Ok(match policy.mode {
        DeploymentMode::Local => "local",
        DeploymentMode::Production => "production",
    })
}

struct ResolvedInputs {
    data_root: PathBuf,
    file: NodeDeploymentOverrides,
    explicit: NodeDeploymentOverrides,
    physical_host_id: String,
}

fn resolve(args: &NodeRunArgs) -> Result<ResolvedInputs, Box<dyn std::error::Error>> {
    DiscoveryConfig::validate_seed_origins(&discovery_seeds())?;
    let file = read_inputs(args.deployment_config.as_deref())?;
    let explicit = NodeDeploymentOverrides {
        mode: args.deployment_mode.map(|mode| match mode {
            Mode::Local => DeploymentMode::Local,
            Mode::Production => DeploymentMode::Production,
        }),
        cluster: args.cluster.clone(),
        initial_voters: args.initial_voters,
        public_access: PublicAccessUris {
            s3: args.s3_public_uri.clone(),
            iceberg: args.iceberg_public_uri.clone(),
            dataset: args.dataset_public_uri.clone(),
        },
        ..Default::default()
    };
    let profile = DeploymentProfile::load(&args.profile)?;
    profile.validate_node_catalog()?;
    let accepted = crowdb_monitor::read_node_policy(&profile.paths.data_root)?;
    let selected = explicit.resolve(&file, accepted.as_ref())?;
    let physical_host_id = physical_host(args.physical_host_id.clone(), &selected)?;
    Ok(ResolvedInputs {
        data_root: profile.paths.data_root,
        file,
        explicit,
        physical_host_id,
    })
}

fn discovery_seeds() -> Vec<String> {
    std::env::var("CROWDB_DISCOVERY_SEEDS")
        .unwrap_or_default()
        .split(',')
        .filter(|seed| !seed.is_empty())
        .map(str::to_owned)
        .collect()
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
    let mut content = String::new();
    fs::File::open(path)?.take(32769).read_to_string(&mut content)?;
    if content.len() > 32768 {
        return Err("node deployment configuration exceeds its size bound".into());
    }
    Ok(toml::from_str(&content)?)
}

#[cfg(test)]
mod tests {
    use super::shifted_port;

    #[test]
    fn shifted_ports_are_bounded() {
        assert_eq!(shifted_port(9095, 100).unwrap(), 9195);
        assert!(shifted_port(u16::MAX, 1).is_err());
    }
}
