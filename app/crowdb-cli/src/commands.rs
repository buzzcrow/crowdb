// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Command module dispatch for cluster operations and attached workloads.
//!
//! Each subcommand module defines a `*Verb` enum (clap) and a `run_*`
//! async function that builds an [`OpContext`] and delegates to the
//! corresponding `ops::*` function.

pub(crate) mod bench;
pub(crate) mod cluster;
pub(crate) mod kv;
pub(crate) mod port_alloc;
pub(crate) mod s3;

pub(crate) use bench::{run_bench_verb, BenchVerb};
pub(crate) use cluster::{run_cluster_verb, ClusterVerb};
pub(crate) use kv::{
    run_group_verb, run_kv_data_verb, run_replica_verb, run_store_verb, GroupVerb, KvDataVerb, ReplicaVerb,
    StoreVerb,
};
pub(crate) use port_alloc::{run as run_port_alloc, PortAllocArgs};
pub(crate) use s3::{run_s3_verb, S3Verb};

use std::process::ExitCode;

use crowdb_console_shared::ops::OpContext;

use crate::Cli;

/// Build a Group 0 context from the verified application target.
pub(crate) async fn authority_context(cli: &Cli) -> Result<OpContext, ExitCode> {
    let (management, rpc) = group0_connection(cli).await?;
    Ok(OpContext::new(
        rpc,
        management,
        crowdb_console_shared::ConsoleConfig::default(),
    ))
}

pub(crate) async fn group0_connection(cli: &Cli) -> Result<(Vec<String>, String), ExitCode> {
    let target = cli.target.as_ref().ok_or_else(|| {
        eprintln!("error: select --cluster or --management-endpoint before a cluster operation or workload");
        ExitCode::from(2)
    })?;
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
    Ok((
        target.identity.group0_management_endpoints.clone(),
        target.identity.group0_rpc_endpoints[0].clone(),
    ))
}
