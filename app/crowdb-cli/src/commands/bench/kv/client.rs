// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared helper for building a [`CrowdbKvClient`] from the CLI endpoint
//! with a bench-specific [`ReadEndpointPolicy`]. The standard `op_context`
//! helper always uses the default `Leader` policy; bench read/scan
//! commands need `AnyReplica` for distributed `MinSlot` reads.

use std::process::ExitCode;

use crowdb_kv_client::{ClientConfig, CrowdbKvClient, ReadEndpointPolicy};

use crate::Cli;

/// Client-side tunables for bench KV commands. Mirrors the server-side
/// `KvDeployTunables` — the client transport must match the server's
/// `event_write` and `rpc_workers` for optimal performance.
#[derive(Debug, Clone)]
pub(crate) struct KvClientTunables {
    pub event_write: bool,
    pub rpc_workers: u32,
    pub enable_nagle: bool,
    pub quickack: bool,
    pub send_queue_capacity: u32,
    pub pool_size: usize,
}

impl Default for KvClientTunables {
    fn default() -> Self {
        Self {
            event_write: false,
            rpc_workers: 2,
            enable_nagle: false,
            quickack: false,
            send_queue_capacity: 4096,
            pool_size: 1,
        }
    }
}

/// Build a `CrowdbKvClient` from `--system-*` with the given
/// `read_endpoint_policy`.
///
/// # Errors
/// Returns `ExitCode::from(2)` if the management endpoint is unavailable.
pub(crate) async fn build_kv_client(
    cli: &Cli,
    read_endpoint_policy: ReadEndpointPolicy,
    tunables: &KvClientTunables,
) -> Result<CrowdbKvClient, ExitCode> {
    let seed = format!("http://{}:{}", cli.system_ip, cli.system_port);
    let server = crowdb_console_shared::clients::http::ServerClient::new(&seed).map_err(|error| {
        eprintln!("bench kv client: {error}");
        ExitCode::from(2)
    })?;
    let effective_g0 = server
        .topology()
        .await
        .ok()
        .and_then(|stores| stores.into_iter().find(|store| store.store_id == 0))
        .and_then(|store| store.listen_addr)
        .ok_or_else(|| {
            eprintln!("bench kv client: Group 0 endpoint unavailable at {seed}");
            ExitCode::from(2)
        })?;
    let seeds = vec![seed];

    let mut client_config = ClientConfig::new(seeds);
    client_config.read_endpoint_policy = read_endpoint_policy;
    client_config.event_write = tunables.event_write;
    client_config.rpc_workers = tunables.rpc_workers;
    client_config.enable_nagle = tunables.enable_nagle;
    client_config.quickack = tunables.quickack;
    client_config.send_queue_capacity = tunables.send_queue_capacity;
    client_config.pool_size_per_endpoint = tunables.pool_size;

    eprintln!(
        "bench kv client: read_endpoint_policy={:?} event_write={} rpc_workers={} nagle={} quickack={} send_queue={}",
        client_config.read_endpoint_policy,
        client_config.event_write,
        client_config.rpc_workers,
        client_config.enable_nagle,
        client_config.quickack,
        client_config.send_queue_capacity,
    );

    let client = CrowdbKvClient::new(client_config);
    client.seed_leader(0, 0, effective_g0);
    Ok(client)
}
