// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Snapshot/WAL catch-up before changing the existing voting set.

use crate::{
    clients::http::ServerClient,
    error::{Error, Result},
};
use crowdb_protocol::mgmt::{GroupStatus, RemoteReplicaInfo};
use std::time::Duration;

pub(super) async fn bootstrap(
    ctx: &crate::ops::OpContext,
) -> Result<Option<crowdb_protocol::mgmt::SystemBootstrapIdentity>> {
    match ctx
        .kv()
        .get(
            0,
            0,
            crate::deployment::CLUSTER_KEY,
            crowdb_kv_client::ReadMode::Linearizable,
            None,
        )
        .await?
    {
        crowdb_kv_client::GetOutcome::Found { value, .. } => {
            let operation: crate::deployment::PreparedBootstrap =
                serde_json::from_slice(&value).map_err(|error| Error::Config(error.to_string()))?;
            Ok(Some(operation.identity))
        }
        crowdb_kv_client::GetOutcome::NotFound => Ok(None),
    }
}

async fn group(client: &ServerClient, sid: u64, gid: u64) -> Result<GroupStatus> {
    client
        .topology()
        .await?
        .into_iter()
        .find(|store| store.store_id == sid)
        .and_then(|store| store.groups.into_iter().find(|group| group.group_id == gid))
        .ok_or_else(|| Error::Config(format!("group {sid}/{gid} absent on {}", client.base_url())))
}

pub(super) async fn source(peers: &[(ServerClient, String)], sid: u64, gid: u64) -> Result<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        for (client, endpoint) in peers {
            let observed = group(client, sid, gid).await?;
            if observed.leader_id != 0 && observed.leader_id == observed.local_replica_id {
                return Ok(endpoint.clone());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Config("snapshot source has no confirmed leader".into()));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub(super) async fn catch_up(
    target: &ServerClient,
    peers: &[(ServerClient, String)],
    sid: u64,
    gid: u64,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let endpoint = source(peers, sid, gid).await?;
        let leader = peers
            .iter()
            .find(|(_, address)| *address == endpoint)
            .ok_or_else(|| Error::Config("snapshot source changed".into()))?;
        let required = group(&leader.0, sid, gid)
            .await?
            .read_state
            .ok_or_else(|| Error::Config("source applied frontier unavailable".into()))?
            .contiguous_applied;
        let applied = group(target, sid, gid)
            .await?
            .read_state
            .ok_or_else(|| Error::Config("joining replica applied frontier unavailable".into()))?
            .contiguous_applied;
        if applied >= required {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Config(
                "joining replica WAL catch-up exceeded 10 seconds".into(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

pub(super) async fn promote(
    peers: &[(ServerClient, String)],
    sid: u64,
    gid: u64,
    remote: &RemoteReplicaInfo,
) -> Result<()> {
    let voting = RemoteReplicaInfo {
        voting: true,
        ..remote.clone()
    };
    for (peer, _) in peers {
        peer.add_remote_replicas(sid, gid, std::slice::from_ref(&voting))
            .await?;
    }
    Ok(())
}
