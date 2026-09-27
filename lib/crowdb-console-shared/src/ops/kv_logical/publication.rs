// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Publish logical membership without overwriting a concurrent winner.

use crowdb_kv_client::{Error as KvError, GetOutcome, ReadMode};
use crowdb_protocol::common::{GroupValue, ReplicaValue, StoreValue};
use crowdb_protocol::key::{KvGroupKey, KvReplicaKey, KvStoreKey, TextKey};

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

pub(super) async fn group(ctx: &OpContext, store_id: u64, group_id: u64) -> Result<()> {
    create(
        ctx,
        KvGroupKey { store_id, group_id }.to_path(),
        &GroupValue { store_id, group_id },
    )
    .await
}

pub(super) async fn replica(ctx: &OpContext, value: &ReplicaValue) -> Result<()> {
    create(
        ctx,
        KvReplicaKey {
            store_id: value.store_id,
            group_id: value.group_id,
            replica_id: value.replica_id,
        }
        .to_path(),
        value,
    )
    .await
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
