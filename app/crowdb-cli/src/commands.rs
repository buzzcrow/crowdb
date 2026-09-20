// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Command module dispatch for the four-domain CLI (R126).
//!
//! Each subcommand module defines a `*Verb` enum (clap) and a `run_*`
//! async function that builds an [`OpContext`] and delegates to the
//! corresponding `ops::*` function.

pub(crate) mod bench;
pub(crate) mod chunk;
pub(crate) mod cluster;
pub(crate) mod kv;
pub(crate) mod port_alloc;
pub(crate) mod s3;

pub(crate) use bench::{run_bench_verb, BenchVerb};
pub(crate) use chunk::{run_chunk_diskdb_verb, run_chunk_stub_verb, ChunkDiskdbVerb, ChunkStubVerb};
pub(crate) use cluster::{run_cluster_verb, ClusterVerb};
pub(crate) use kv::{
    run_group_verb, run_kv_data_verb, run_kv_server_verb, run_replica_verb, run_store_verb, GroupVerb,
    KvDataVerb, KvServerVerb, ReplicaVerb, StoreVerb,
};
pub(crate) use port_alloc::{run as run_port_alloc, PortAllocArgs};
pub(crate) use s3::{run_s3_verb, S3Verb};

use std::process::ExitCode;

use crowdb_console_shared::ops::OpContext;
use crowdb_console_shared::ConsoleConfigEngine;

use crate::Cli;

/// Build an [`OpContext`] from the CLI global flags. The system endpoint
/// is `http://{system_ip}:{system_port}` and the
/// CLI state is loaded from the fixed runtime location.
///
/// When the config has server entries, their mgmt URLs are added as
/// additional seeds so the client can discover the group-0 leader even
/// if `--system-port` doesn't point at a running server (e.g. after
/// `local-deploy` which allocates dynamic ports).
pub(crate) fn op_context(cli: &Cli) -> Result<OpContext, ExitCode> {
    let config = load_config(cli)?;
    let mgmt_url = format!("http://{}:{}", cli.system_ip, cli.system_port);
    let group0_endpoint = format!("{}:{}", cli.system_ip, cli.system_port);

    // Collect mgmt seeds: the explicit --system-port endpoint plus all
    // server URLs from the config (so local-deploy'd servers are found).
    let mut seeds = vec![mgmt_url];
    for server in &config.servers {
        if !seeds.contains(&server.url) {
            seeds.push(server.url.clone());
        }
    }

    // Use the first config server's RPC URL as the group0 endpoint hint
    // if available (more accurate than the default port). Strip the
    // `http://` prefix since the crowdb-rpc endpoint format is `ip:port`.
    let effective_g0 =
        config
            .servers
            .first()
            .and_then(|s| s.rpc_url.as_ref())
            .map_or(group0_endpoint, |url| {
                url.strip_prefix("http://")
                    .or_else(|| url.strip_prefix("https://"))
                    .unwrap_or(url)
                    .to_string()
            });

    Ok(OpContext::new(effective_g0, seeds, config))
}

/// Load the CLI's internal persisted state from its fixed runtime location.
pub(crate) fn load_config(_cli: &Cli) -> Result<crowdb_console_shared::ConsoleConfig, ExitCode> {
    let path = config_path();
    if !path.exists() {
        return Ok(crowdb_console_shared::ConsoleConfig::default());
    }
    let engine = crowdb_console_shared::TomlFileEngine::new(path);
    engine.load().map_err(|e| {
        eprintln!("error: load config: {e}");
        ExitCode::from(2)
    })
}

/// Resolve the private CLI state file. The environment override is reserved
/// for isolated test and benchmark harnesses and is intentionally not a CLI
/// option.
fn config_path() -> std::path::PathBuf {
    std::env::var_os("CROWDB_CLI_STATE").map_or_else(
        || {
            crowdb_protocol::port::namespace::runtime_root()
                .join("persistent")
                .join("console")
                .join("crowdb-kv.db.toml")
        },
        std::path::PathBuf::from,
    )
}

/// Persist the config from an [`OpContext`] back to the config file.
pub(crate) fn commit_config(_cli: &Cli, ctx: &OpContext) -> Result<(), ExitCode> {
    let path = config_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            eprintln!("error: create config dir {}: {e}", parent.display());
            ExitCode::from(2)
        })?;
    }
    let engine = crowdb_console_shared::TomlFileEngine::new(path.clone());
    let cfg = ctx.config().clone();
    engine.save(&cfg).map_err(|e| {
        eprintln!("error: save config {}: {e}", path.display());
        ExitCode::from(2)
    })
}
