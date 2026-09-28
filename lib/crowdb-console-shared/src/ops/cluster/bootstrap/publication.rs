// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::ops::OpContext;
use crowdb_protocol::common::{HwStatus, NodeValue, RackValue, ReplicaValue};

/// Write the hardware hierarchy + KV-cluster topology from the local
/// config into group-0 sysdata. Best-effort: individual write failures
/// are logged and skipped.
pub(super) async fn write_topology_to_sysdata(
    ctx: &OpContext,
    store_nodes: &[u64],
    succeeded: &[(u64, u64)],
) {
    let cfg_snapshot = ctx.config().clone();
    let sysmd = ctx.sysmd();

    // All sysmd keys (racks, nodes, stores, groups, replicas) are
    // independent — write them concurrently to avoid sequential RTTs.
    let mut writes: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // Hardware hierarchy.
    for rack in &cfg_snapshot.racks {
        let sysmd = sysmd.clone();
        let rack_id = rack.id;
        let value = RackValue {
            status: HwStatus::Up as i32,
            node_ids: Vec::new(),
        };
        writes.push(tokio::spawn(async move {
            let _ = sysmd.add_rack(rack_id, &value).await;
        }));
    }
    for node in &cfg_snapshot.nodes {
        let sysmd = sysmd.clone();
        let rack_id = node.rack_id;
        let node_id = node.id;
        let value = NodeValue {
            status: HwStatus::Up as i32,
            last_used_dg_id: 0,
            disk_group_ids: Vec::new(),
            status_changed_at_ms: 0,
            temp_failure_since_ms: None,
        };
        writes.push(tokio::spawn(async move {
            let _ = sysmd.add_node(rack_id, node_id, &value).await;
        }));
    }

    // KV-cluster topology.
    {
        let sysmd = sysmd.clone();
        let node_ids = store_nodes.to_vec();
        writes.push(tokio::spawn(async move {
            let _ = sysmd.add_store(0, &node_ids).await;
        }));
    }
    {
        let sysmd = sysmd.clone();
        writes.push(tokio::spawn(async move {
            let _ = sysmd.add_group(0, 0).await;
        }));
    }
    for (nid, rid) in succeeded {
        let sysmd = sysmd.clone();
        let endpoint = cfg_snapshot
            .server_for_node(*nid)
            .and_then(|s| s.rpc_url.clone())
            .unwrap_or_default();
        let value = ReplicaValue {
            store_id: 0,
            group_id: 0,
            replica_id: *rid,
            node_id: *nid,
            role: String::new(),
            voting: true,
            endpoint,
        };
        writes.push(tokio::spawn(async move {
            let _ = sysmd.add_replica(&value).await;
        }));
    }

    for h in writes {
        let _ = h.await;
    }
}
