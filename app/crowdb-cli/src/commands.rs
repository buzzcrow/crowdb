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
pub(crate) mod launch;
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

use crate::Cli;

/// Build a Group 0 context for hardware operations without reading the old
/// local console state. The CLI endpoint is a discovery seed, while the
/// optional launch registry is validated only as local process policy.
pub(crate) async fn authority_context(cli: &Cli) -> Result<OpContext, ExitCode> {
    if let Some(path) = &cli.registry {
        crowdb_console_shared::config::web::LaunchRegistry::load(path).map_err(|error| {
            eprintln!("error: load launch registry: {error}");
            ExitCode::from(2)
        })?;
    }
    let (management, rpc) = group0_connection(cli).await?;
    Ok(OpContext::new(
        rpc,
        management,
        crowdb_console_shared::ConsoleConfig::default(),
    ))
}

pub(crate) async fn group0_connection(cli: &Cli) -> Result<(Vec<String>, String), ExitCode> {
    if let Some(target) = &cli.target {
        target.require_native_transport().await.map_err(|error| {
            eprintln!("error: cluster native transport: {error}");
            ExitCode::from(2)
        })?;
        let native = crowdb_kv_client::CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(
            target.identity.group0_management_endpoints.clone(),
        ));
        native.seed_leader(0, 0, target.identity.group0_rpc_endpoints[0].clone());
        target.verify_native_authority(&native).await.map_err(|error| {
            eprintln!("error: cluster native identity: {error}");
            ExitCode::from(2)
        })?;
        return Ok((
            target.identity.group0_management_endpoints.clone(),
            target.identity.group0_rpc_endpoints[0].clone(),
        ));
    }
    let mgmt_url = format!("http://{}:{}", cli.system_ip, cli.system_port);
    let group0_endpoint = match crowdb_console_shared::clients::http::ServerClient::new(&mgmt_url) {
        Ok(client) => client
            .topology()
            .await
            .ok()
            .and_then(|stores| stores.into_iter().find(|store| store.store_id == 0))
            .and_then(|store| store.listen_addr),
        Err(_) => None,
    }
    .unwrap_or_else(|| format!("{}:{}", cli.system_ip, cli.system_port));
    Ok((vec![mgmt_url], group0_endpoint))
}
