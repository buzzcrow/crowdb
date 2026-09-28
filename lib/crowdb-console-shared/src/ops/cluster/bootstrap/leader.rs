// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{server_client, wait_for_leader};
use crate::clients::http::ServerClient;
use crate::ops::OpContext;

async fn rpc_endpoint_for_store(ctx: &OpContext, node_id: u64, store_id: u64) -> Option<String> {
    let client = server_client(ctx, node_id).ok()?;
    let stores = client.topology().await.ok()?;
    for s in &stores {
        if s.store_id == store_id {
            if let Some(addr) = &s.listen_addr {
                let stripped = addr
                    .strip_prefix("http://")
                    .or_else(|| addr.strip_prefix("https://"))
                    .unwrap_or(addr);
                let remapped = stripped
                    .strip_prefix("0.0.0.0:")
                    .map_or_else(|| stripped.to_string(), |port| format!("127.0.0.1:{port}"));
                return Some(remapped);
            }
        }
    }
    None
}

/// Seed the KV client with the group-0 leader endpoint after init.
/// For single-node, use the node's own RPC endpoint. For multi-node,
/// wait for election to complete and read the leader from topology.
pub(super) async fn seed_leader_after_init(
    ctx: &OpContext,
    single_node: bool,
    succeeded: &[(u64, u64)],
    mgmt_seeds: &[String],
) {
    if single_node {
        for (nid, _) in succeeded {
            if let Some(ep) = rpc_endpoint_for_store(ctx, *nid, 0).await {
                ctx.kv().seed_leader(0, 0, ep);
                break;
            }
        }
        return;
    }
    if let Some(leader_url) = wait_for_leader(mgmt_seeds, 0, 0, std::time::Duration::from_secs(10)).await {
        if let Ok(sc) = ServerClient::new(&leader_url) {
            if let Ok(topo) = sc.topology().await {
                for store in &topo {
                    if store.store_id == 0 {
                        for group in &store.groups {
                            if group.group_id == 0 && group.leader_id > 0 {
                                // The leader is either the local replica
                                // (leader_id == local_replica_id) or a
                                // remote replica. When local, the endpoint
                                // is the store's own listen_addr (not in
                                // the remotes list). When remote, find the
                                // matching remote's endpoint.
                                let leader_ep = if group.leader_id == group.local_replica_id {
                                    store.listen_addr.clone()
                                } else {
                                    group
                                        .remotes
                                        .iter()
                                        .find(|r| r.id == group.leader_id)
                                        .map(|r| r.endpoint.clone())
                                };
                                if let Some(ep) = leader_ep {
                                    let stripped = ep
                                        .strip_prefix("http://")
                                        .or_else(|| ep.strip_prefix("https://"))
                                        .unwrap_or(&ep);
                                    let remapped = stripped.strip_prefix("0.0.0.0:").map_or_else(
                                        || stripped.to_string(),
                                        |port| format!("127.0.0.1:{port}"),
                                    );
                                    ctx.kv().seed_leader(0, 0, remapped);
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}
