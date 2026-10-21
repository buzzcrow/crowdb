// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::ChunkdbDeployRequest;
use crate::config::NodeEntry;

/// Render the node-specific `ChunkDB` configuration after deployment input validation.
#[must_use]
pub fn chunkdb_config(req: &ChunkdbDeployRequest, node: &NodeEntry) -> String {
    let seeds = req
        .kv_server_mgmt_seeds
        .iter()
        .map(|seed| format!("{seed:?}"))
        .collect::<Vec<_>>()
        .join(", ");
    let deployment_mode = if req.local_deployment {
        "local"
    } else if req.allow_unsafe_ec {
        "test_unsafe_placement"
    } else {
        "production"
    };
    let placement_mode = if req.local_deployment || req.allow_unsafe_ec {
        "unsafe_colocated"
    } else {
        "protected"
    };
    let deployment_budget = if req.local_deployment {
        "max_node_failures = 0\n"
    } else {
        ""
    };
    let mut configuration = format!(
        "service_ownership = \"{}\"\n\n[deployment]\nmode = \"{}\"\n{}\n[server]\nrpc_workers = {}\nhttp_listen_addr = \"{}:{}\"\nrpc_listen_addr = \"{}:{}\"\ninstance_id = \"{}\"\nkv_server_mgmt_seeds = [{}]\nkeepalive_interval_secs = 1\nkv_pool_size = {}\nkv_rpc_workers = {}\ndiskdb_pool_size = {}\ndiskdb_rpc_workers = {}\n\n[topology]\nrefresh_interval_secs = 1\n\n[range_guard]\nallow_all_when_empty = false\n\n[lifecycle]\ncache_capacity = 10000\nsweep_chunk_lock_interval_secs = 60\nlock_hold_warn_threshold_ms = 1000\n\n[placement]\nmode = \"{}\"\nallow_unsafe_ec = {}\nallow_degraded_failure_domains = {}\n",
        if req.dynamic_ownership { "dynamic" } else { "fixed" },
        deployment_mode,
        deployment_budget,
        req.rpc_workers.unwrap_or(2),
        node.host,
        req.http_port,
        node.host,
        req.rpc_port,
        req.instance_id,
        seeds,
        req.kv_connections.unwrap_or(1),
        req.kv_client_rpc_workers.unwrap_or(2),
        req.diskdb_connections.unwrap_or(1),
        req.diskdb_client_rpc_workers.unwrap_or(2),
        placement_mode,
        req.allow_unsafe_ec && !req.local_deployment,
        req.allow_unsafe_ec || req.local_deployment,
    );
    if req.local_deployment {
        configuration.push_str("\n[conversion]\nenabled = false\n");
    }
    configuration
}
