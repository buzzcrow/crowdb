// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Complete peer wiring before publishing a newly created group.

use crowdb_protocol::mgmt::RemoteReplicaInfo;

use crate::clients::http::ServerClient;
use crate::error::{Error, Result};
use crate::ops::OpContext;

use super::server_client;

pub(super) async fn resolve(
    ctx: &OpContext,
    store_id: u64,
    members: &[(u64, u64)],
) -> Result<Vec<(ServerClient, String)>> {
    futures::future::join_all(members.iter().map(|(node_id, _)| async move {
        let client = server_client(ctx, *node_id).await?;
        let stores = client.topology().await?;
        let endpoint = stores
            .iter()
            .find(|store| store.store_id == store_id)
            .and_then(|store| store.listen_addr.as_deref())
            .ok_or_else(|| Error::NodeUnreachable {
                node_id: node_id.to_string(),
                reason: format!("store {store_id} has no peer RPC endpoint"),
            })?;
        let endpoint = client.resolve_rpc_endpoint(endpoint)?;
        Ok((client, endpoint))
    }))
    .await
    .into_iter()
    .collect()
}

pub(super) async fn wire(
    ctx: &OpContext,
    store_id: u64,
    group_id: u64,
    members: &[(u64, u64)],
) -> Result<()> {
    if members.len() < 2 {
        return Ok(());
    }
    let resolved = resolve(ctx, store_id, members).await?;
    let results = futures::future::join_all(resolved.iter().enumerate().map(|(index, (client, _))| {
        let remotes: Vec<_> = members
            .iter()
            .zip(&resolved)
            .enumerate()
            .filter(|(peer, _)| *peer != index)
            .map(|(_, ((_, replica_id), (_, endpoint)))| RemoteReplicaInfo {
                replica_id: *replica_id,
                endpoint: endpoint.clone(),
                voting: true,
            })
            .collect();
        async move { client.add_remote_replicas(store_id, group_id, &remotes).await }
    }))
    .await;
    results.into_iter().collect()
}

pub(super) async fn rollback(
    ctx: &OpContext,
    store_id: u64,
    group_id: u64,
    created: &[(u64, u64)],
    original: Error,
) -> Error {
    let results = futures::future::join_all(created.iter().map(|(node_id, _)| async move {
        server_client(ctx, *node_id)
            .await?
            .remove_group(store_id, group_id)
            .await
    }))
    .await;
    if let Some(error) = results.into_iter().find_map(std::result::Result::err) {
        Error::UpstreamRpc {
            node_id: format!("group {store_id}/{group_id}"),
            status: format!("{original}; rollback incomplete: {error}"),
        }
    } else {
        original
    }
}
