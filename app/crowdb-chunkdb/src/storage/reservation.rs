// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable strip-reservation values co-located with their owning chunk.

use bytes::Bytes;
use crowdb_kv_client::{BatchOp, GetOutcome, ReadMode, ScanOutcome};
use crowdb_protocol::chunkdb::rpc::{Chunk, StripReservationGroup};
use crowdb_protocol::common::ChunkId;
use tracing::warn;

use crate::routing::{route, MigrationState, Route};

use super::{chunk_key, encode_chunk, ChunkStore, Result, StoreError};

impl ChunkStore {
    pub async fn list_reservation_groups(&self, chunk_id: &ChunkId) -> Result<Vec<StripReservationGroup>> {
        let reservation_route = route(&self.bindings, chunk_id)?;
        let prefix = reservation_prefix(chunk_id);
        let outcome: ScanOutcome = self
            .kv
            .scan(
                reservation_route.kv_store_id,
                reservation_route.kv_group_id,
                &prefix,
                &[],
                &[],
                u32::MAX,
                ReadMode::Linearizable,
                None,
                false,
                None,
            )
            .await
            .map_err(|error| StoreError::Kv(error.to_string()))?;
        outcome
            .items
            .into_iter()
            .map(|(_, value)| decode_group(&value))
            .collect()
    }

    pub async fn delete_reservation_group(&self, chunk_id: &ChunkId, group_id: &ChunkId) -> Result<()> {
        self.write_reservation_ops(
            chunk_id,
            &[BatchOp::Delete {
                key: Bytes::from(reservation_key(chunk_id, group_id)),
            }],
        )
        .await
    }

    pub async fn get_reservation_group(
        &self,
        chunk_id: &ChunkId,
        group_id: &ChunkId,
    ) -> Result<Option<StripReservationGroup>> {
        let reservation_route = route(&self.bindings, chunk_id)?;
        let key = reservation_key(chunk_id, group_id);
        if let Some(value) = self.read_reservation_raw(&reservation_route, &key).await? {
            return decode_group(&value).map(Some);
        }
        if matches!(
            reservation_route.migration_state,
            MigrationState::Copying | MigrationState::Cutover
        ) {
            if let (Some(store), Some(group)) = (
                reservation_route.old_kv_store_id,
                reservation_route.old_kv_group_id,
            ) {
                let old_route = Route {
                    kv_store_id: store,
                    kv_group_id: group,
                    migration_state: MigrationState::NotMigrating,
                    old_kv_store_id: None,
                    old_kv_group_id: None,
                };
                if let Some(value) = self.read_reservation_raw(&old_route, &key).await? {
                    return decode_group(&value).map(Some);
                }
            }
        }
        Ok(None)
    }

    pub async fn put_chunk_and_reservation(
        &self,
        chunk: &Chunk,
        group: &StripReservationGroup,
    ) -> Result<()> {
        let chunk_id = chunk
            .id
            .ok_or_else(|| StoreError::Serde("chunk has no id".into()))?;
        let group_id = group
            .group_id
            .ok_or_else(|| StoreError::Serde("reservation group has no id".into()))?;
        self.write_reservation_ops(
            &chunk_id,
            &[
                BatchOp::Put {
                    key: Bytes::from(chunk_key(&chunk_id)),
                    value: Bytes::from(encode_chunk(chunk)),
                },
                BatchOp::Put {
                    key: Bytes::from(reservation_key(&chunk_id, &group_id)),
                    value: Bytes::from(encode_group(group)?),
                },
            ],
        )
        .await
    }

    pub async fn put_reservation_group(&self, group: &StripReservationGroup) -> Result<()> {
        let chunk_id = group
            .chunk_id
            .ok_or_else(|| StoreError::Serde("reservation group has no chunk id".into()))?;
        let group_id = group
            .group_id
            .ok_or_else(|| StoreError::Serde("reservation group has no id".into()))?;
        self.write_reservation_ops(
            &chunk_id,
            &[BatchOp::Put {
                key: Bytes::from(reservation_key(&chunk_id, &group_id)),
                value: Bytes::from(encode_group(group)?),
            }],
        )
        .await
    }

    pub async fn put_chunk_and_finish_reservation(
        &self,
        chunk: &Chunk,
        group: Option<&StripReservationGroup>,
        group_id: &ChunkId,
    ) -> Result<()> {
        let chunk_id = chunk
            .id
            .ok_or_else(|| StoreError::Serde("chunk has no id".into()))?;
        let mut ops = vec![BatchOp::Put {
            key: Bytes::from(chunk_key(&chunk_id)),
            value: Bytes::from(encode_chunk(chunk)),
        }];
        if let Some(group) = group {
            ops.push(BatchOp::Put {
                key: Bytes::from(reservation_key(&chunk_id, group_id)),
                value: Bytes::from(encode_group(group)?),
            });
        } else {
            ops.push(BatchOp::Delete {
                key: Bytes::from(reservation_key(&chunk_id, group_id)),
            });
        }
        self.write_reservation_ops(&chunk_id, &ops).await
    }

    async fn write_reservation_ops(&self, chunk_id: &ChunkId, ops: &[BatchOp]) -> Result<()> {
        let reservation_route = route(&self.bindings, chunk_id)?;
        self.kv
            .batch_write(reservation_route.kv_store_id, reservation_route.kv_group_id, ops)
            .await
            .map_err(|error| StoreError::Kv(error.to_string()))?;
        if matches!(
            reservation_route.migration_state,
            MigrationState::Copying | MigrationState::Cutover
        ) {
            if let (Some(store), Some(group)) = (
                reservation_route.old_kv_store_id,
                reservation_route.old_kv_group_id,
            ) {
                if let Err(error) = self.kv.batch_write(store, group, ops).await {
                    warn!(%error, "reservation dual-write to old group failed");
                }
            }
        }
        Ok(())
    }

    async fn read_reservation_raw(&self, reservation_route: &Route, key: &[u8]) -> Result<Option<Bytes>> {
        match self
            .kv
            .get(
                reservation_route.kv_store_id,
                reservation_route.kv_group_id,
                key,
                ReadMode::Linearizable,
                None,
            )
            .await
            .map_err(|error| StoreError::Kv(error.to_string()))?
        {
            GetOutcome::Found { value, .. } => Ok(Some(value)),
            GetOutcome::NotFound => Ok(None),
        }
    }
}

fn reservation_key(chunk_id: &ChunkId, group_id: &ChunkId) -> Vec<u8> {
    let mut key = Vec::with_capacity(45);
    key.extend_from_slice(b"/reservation/");
    key.extend_from_slice(&chunk_id.high.to_be_bytes());
    key.extend_from_slice(&chunk_id.low.to_be_bytes());
    key.extend_from_slice(&group_id.high.to_be_bytes());
    key.extend_from_slice(&group_id.low.to_be_bytes());
    key
}

fn reservation_prefix(chunk_id: &ChunkId) -> Vec<u8> {
    let mut key = Vec::with_capacity(29);
    key.extend_from_slice(b"/reservation/");
    key.extend_from_slice(&chunk_id.high.to_be_bytes());
    key.extend_from_slice(&chunk_id.low.to_be_bytes());
    key
}

fn encode_group(group: &StripReservationGroup) -> Result<Vec<u8>> {
    bincode::serialize(group).map_err(|error| StoreError::Serde(error.to_string()))
}

fn decode_group(bytes: &[u8]) -> Result<StripReservationGroup> {
    bincode::deserialize(bytes).map_err(|error| StoreError::Serde(error.to_string()))
}
