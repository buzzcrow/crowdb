// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Publish logical membership without overwriting a concurrent winner.

use crowdb_kv_client::{BatchOp, Error as KvError, GetOutcome, ReadMode};
use crowdb_protocol::common::{GroupValue, ReplicaValue, StoreValue};
use crowdb_protocol::key::{KvGroupKey, KvReplicaKey, KvStoreKey, TextKey};
use crowdb_protocol::kv_membership::GroupMember;

use crate::error::{Error, Result};
use crate::ops::OpContext;

pub(super) async fn store(ctx: &OpContext, store_id: u64, node_ids: &[u64]) -> Result<()> {
    create(
        ctx,
        KvStoreKey { store_id }.to_path(),
        &StoreValue {
            store_id,
            node_ids: node_ids.to_vec(),
        },
    )
    .await
}

pub(super) async fn group(
    ctx: &OpContext,
    store_id: u64,
    group_id: u64,
    members: &[(u64, u64)],
) -> Result<()> {
    let authority = ctx.membership();
    let mut complete_members = Vec::with_capacity(members.len());
    for (node_id, replica_id) in members {
        let endpoint = ctx
            .sysmd()
            .read_all_kv_server_instances()
            .await?
            .into_iter()
            .find_map(|(_, instance)| {
                (instance
                    .extra
                    .as_ref()
                    .and_then(|extra| extra.kv_server.as_ref())
                    .and_then(|server| server.node_id)
                    == Some(*node_id))
                .then_some(instance.rpc_endpoint)
            });
        let endpoint = match endpoint {
            Some(endpoint) => endpoint,
            None => super::rpc_endpoint_for_store(ctx, *node_id, store_id)
                .await
                .ok_or_else(|| Error::Validation {
                    field: "members.endpoint".into(),
                    message: format!("node {node_id} has no KV endpoint for store {store_id}"),
                })?,
        };
        complete_members.push(GroupMember {
            replica_id: *replica_id,
            node_id: *node_id,
            endpoint,
            voting: true,
        });
    }
    // Publish Installing before the materialized topology projection. The
    // projection remains useful to existing readers, but the complete record
    // is the authority and is only marked Ready after all records commit.
    let installing = authority
        .create(store_id, group_id, complete_members)
        .await
        .map_err(Error::from)?;
    let key = KvGroupKey { store_id, group_id }.to_path();
    let value = GroupValue { store_id, group_id };
    let mut replicas: Vec<_> = members
        .iter()
        .map(|(node_id, replica_id)| replica_value(store_id, group_id, *replica_id, *node_id))
        .collect();
    replicas.sort_unstable_by_key(|replica| replica.replica_id);
    let mut batch = vec![put(&key, &value)?];
    for replica in &replicas {
        let path = KvReplicaKey {
            store_id,
            group_id,
            replica_id: replica.replica_id,
        }
        .to_path();
        batch.push(put(&path, replica)?);
    }
    match ctx.kv().batch_write_cas(0, 0, &batch, key.as_bytes(), 0).await {
        Ok(_) => {
            authority.complete(&installing).await.map_err(Error::from)?;
            Ok(())
        }
        Err(error @ (KvError::CasFailed { .. } | KvError::OutcomeUnknown)) => {
            let actual = ctx.sysmd().get_group(store_id, group_id).await?;
            let mut actual_replicas = ctx.sysmd().list_replicas_in_group(store_id, group_id).await?;
            actual_replicas.sort_unstable_by_key(|replica| replica.replica_id);
            if actual == Some(value) && actual_replicas == replicas {
                authority.complete(&installing).await.map_err(Error::from)?;
                Ok(())
            } else if actual.is_some() {
                Err(Error::Conflict {
                    kind: "logical membership".into(),
                    id: key,
                })
            } else {
                Err(error.into())
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn put<T: serde::Serialize>(key: &str, value: &T) -> Result<BatchOp> {
    let payload = serde_json::to_vec(value).map_err(|error| Error::Config(error.to_string()))?;
    Ok(BatchOp::Put {
        key: key.as_bytes().to_vec().into(),
        value: payload.into(),
    })
}

pub(super) fn replica_value(store_id: u64, group_id: u64, replica_id: u64, node_id: u64) -> ReplicaValue {
    ReplicaValue {
        store_id,
        group_id,
        replica_id,
        node_id,
        role: String::new(),
        voting: true,
        endpoint: String::new(),
    }
}

pub(super) async fn replica(ctx: &OpContext, value: &ReplicaValue) -> Result<()> {
    let store_key = KvStoreKey {
        store_id: value.store_id,
    }
    .to_path();
    let replica_key = KvReplicaKey {
        store_id: value.store_id,
        group_id: value.group_id,
        replica_id: value.replica_id,
    }
    .to_path();
    for _ in 0..32 {
        let GetOutcome::Found {
            value: bytes,
            revision,
        } = ctx
            .kv()
            .get(0, 0, store_key.as_bytes(), ReadMode::Linearizable, None)
            .await?
        else {
            return Err(Error::NotFound {
                kind: "store".into(),
                id: value.store_id.to_string(),
            });
        };
        let mut store: StoreValue =
            serde_json::from_slice(&bytes).map_err(|error| Error::Config(error.to_string()))?;
        if let Some(actual) = ctx
            .sysmd()
            .get_replica(value.store_id, value.group_id, value.replica_id)
            .await?
        {
            if actual != *value {
                return Err(Error::Conflict {
                    kind: "replica".into(),
                    id: replica_key,
                });
            }
            if store.node_ids.contains(&value.node_id) {
                return Ok(());
            }
        }
        if !store.node_ids.contains(&value.node_id) {
            store.node_ids.push(value.node_id);
            store.node_ids.sort_unstable();
        }
        let batch = [put(&store_key, &store)?, put(&replica_key, value)?];
        match ctx
            .kv()
            .batch_write_cas(0, 0, &batch, store_key.as_bytes(), revision)
            .await
        {
            Ok(_) => return Ok(()),
            Err(KvError::CasFailed { .. } | KvError::CasBusy | KvError::OutcomeUnknown) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(KvError::CasBusy.into())
}

async fn create<T: serde::Serialize>(ctx: &OpContext, key: String, value: &T) -> Result<()> {
    let payload = serde_json::to_vec(value).map_err(|error| Error::Config(error.to_string()))?;
    match ctx.kv().put_cas(0, 0, key.as_bytes(), &payload, 0).await {
        Ok(_) => Ok(()),
        Err(error @ (KvError::CasFailed { .. } | KvError::OutcomeUnknown)) => {
            // A CAS race or a lost response does not authorize an overwrite
            // or rollback. Only a linearizable matching read proves success.
            match ctx
                .kv()
                .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
                .await?
            {
                GetOutcome::Found { value, .. } if value.as_ref() == payload.as_slice() => Ok(()),
                GetOutcome::Found { .. } => Err(Error::Conflict {
                    kind: "logical membership".into(),
                    id: key,
                }),
                GetOutcome::NotFound => Err(error.into()),
            }
        }
        Err(error) => Err(error.into()),
    }
}

pub(super) async fn remove(ctx: &OpContext, key: impl TextKey) -> Result<()> {
    let path = key.to_path();
    match ctx.kv().delete(0, 0, path.as_bytes(), None).await {
        Ok(_) => Ok(()),
        Err(error) => {
            // Deletion may have committed even when its response was lost.
            // Absence must be confirmed by authority, never by a local cache.
            match ctx
                .kv()
                .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
                .await?
            {
                GetOutcome::NotFound => Ok(()),
                GetOutcome::Found { .. } => Err(error.into()),
            }
        }
    }
}
