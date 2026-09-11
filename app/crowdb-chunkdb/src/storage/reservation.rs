// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable strip-reservation values co-located with their owning chunk.

use std::collections::HashMap;

use bytes::Bytes;
use crowdb_kv_client::{BatchOp, GetOutcome, ReadMode, ScanOutcome};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkStrip, StripReservationGroup};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::Segment;
use serde::Deserialize;
use tracing::warn;

use crate::routing::{route, MigrationState, Route};

use super::{chunk_key, encode_chunk, ChunkStore, Result, StoreError};

impl ChunkStore {
    pub async fn scan_reservation_groups(&self, max_keys: u32) -> Result<Vec<StripReservationGroup>> {
        self.scan_reservation_groups_after(max_keys, None).await
    }

    pub async fn scan_reservation_groups_after(
        &self,
        max_keys: u32,
        start_after: Option<(&ChunkId, &ChunkId)>,
    ) -> Result<Vec<StripReservationGroup>> {
        let table = self.bindings.snapshot();
        if table.is_empty() {
            return Err(crate::routing::RouteError::NoBinding.into());
        }
        let prefix = b"/reservation/";
        let start_after = start_after.map_or_else(Vec::new, |(chunk_id, group_id)| {
            reservation_key(chunk_id, group_id)
        });
        let mut groups = HashMap::new();
        for binding in table.bindings() {
            let outcome: ScanOutcome = self
                .kv
                .scan(
                    binding.kv_store_id,
                    binding.kv_group_id,
                    prefix,
                    &start_after,
                    &[],
                    max_keys,
                    ReadMode::Linearizable,
                    None,
                    false,
                    None,
                )
                .await
                .map_err(|error| StoreError::Kv(error.to_string()))?;
            for (_, value) in outcome.items {
                let group = decode_group(&value)?;
                if let (Some(chunk_id), Some(group_id)) = (group.chunk_id, group.group_id) {
                    groups.insert((chunk_id, group_id), group);
                }
            }
        }
        let mut groups = groups.into_values().collect::<Vec<_>>();
        groups.sort_unstable_by_key(|group| {
            let chunk = group.chunk_id.unwrap_or_default();
            let id = group.group_id.unwrap_or_default();
            (chunk.high, chunk.low, id.high, id.low)
        });
        groups.truncate(usize::try_from(max_keys).unwrap_or(usize::MAX));
        Ok(groups)
    }

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
            None,
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
            Some(chunk),
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
            None,
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
        self.write_reservation_ops(&chunk_id, &ops, Some(chunk)).await
    }

    async fn write_reservation_ops(
        &self,
        chunk_id: &ChunkId,
        ops: &[BatchOp],
        chunk: Option<&Chunk>,
    ) -> Result<()> {
        let reservation_route = route(&self.bindings, chunk_id)?;
        self.write_reservation_ops_at(
            reservation_route.kv_store_id,
            reservation_route.kv_group_id,
            chunk_id,
            ops,
            chunk,
        )
        .await?;
        if matches!(
            reservation_route.migration_state,
            MigrationState::Copying | MigrationState::Cutover
        ) {
            if let (Some(store), Some(group)) = (
                reservation_route.old_kv_store_id,
                reservation_route.old_kv_group_id,
            ) {
                if let Err(error) = self
                    .write_reservation_ops_at(store, group, chunk_id, ops, chunk)
                    .await
                {
                    warn!(%error, "reservation dual-write to old group failed");
                }
            }
        }
        Ok(())
    }

    async fn write_reservation_ops_at(
        &self,
        store_id: u64,
        group_id: u64,
        chunk_id: &ChunkId,
        ops: &[BatchOp],
        chunk: Option<&Chunk>,
    ) -> Result<()> {
        let Some(chunk) = chunk else {
            return self
                .kv
                .batch_write(store_id, group_id, ops)
                .await
                .map(|_| ())
                .map_err(|error| StoreError::Kv(error.to_string()));
        };
        let key = chunk_key(chunk_id);
        let expected_revision = match self
            .kv
            .get(store_id, group_id, &key, ReadMode::Linearizable, None)
            .await
            .map_err(|error| StoreError::Kv(error.to_string()))?
        {
            GetOutcome::NotFound => 0,
            GetOutcome::Found { value, revision } => {
                let current = super::decode_chunk(&value)?;
                if current == *chunk {
                    return Ok(());
                }
                if chunk.modify_ts != current.modify_ts.saturating_add(1) {
                    return Err(StoreError::Conflict);
                }
                revision
            }
        };
        match self
            .kv
            .batch_write_cas(store_id, group_id, ops, &key, expected_revision)
            .await
        {
            Ok(_) => Ok(()),
            Err(
                crowdb_kv_client::Error::CasFailed { .. }
                | crowdb_kv_client::Error::CasBusy
                | crowdb_kv_client::Error::OutcomeUnknown,
            ) => match self
                .kv
                .get(store_id, group_id, &key, ReadMode::Linearizable, None)
                .await
            {
                Ok(GetOutcome::Found { value, .. }) if super::decode_chunk(&value)? == *chunk => Ok(()),
                Ok(_) => Err(StoreError::Conflict),
                Err(error) => Err(StoreError::Kv(error.to_string())),
            },
            Err(error) => Err(StoreError::Kv(error.to_string())),
        }
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
    let mut group: StripReservationGroup = match bincode::deserialize(bytes) {
        Ok(group) => group,
        Err(current_error) => {
            let legacy: LegacyStripReservationGroup =
                bincode::deserialize(bytes).map_err(|legacy_error| {
                    StoreError::Serde(format!(
                        "reservation decode failed: current={current_error}; legacy={legacy_error}"
                    ))
                })?;
            legacy.into()
        }
    };
    if group.planned_cursors.is_empty() {
        group.planned_cursors.resize(group.strips.len(), 0);
    }
    if group.planned_closed_sequences.is_empty() {
        group
            .planned_closed_sequences
            .resize(group.strips.len(), u32::MAX);
    }
    Ok(group)
}

#[derive(Deserialize)]
struct LegacyStripReservationGroup {
    group_id: Option<ChunkId>,
    chunk_id: Option<ChunkId>,
    writer_epoch: u64,
    lease_generation: u64,
    lease_deadline_ms: u64,
    placement_epoch: u64,
    strips: Vec<ChunkStrip>,
    states: Vec<i32>,
    parity_segments: Vec<Segment>,
    preferred_survivors: Vec<u32>,
    data_num: u32,
    code_num: u32,
}

impl From<LegacyStripReservationGroup> for StripReservationGroup {
    fn from(legacy: LegacyStripReservationGroup) -> Self {
        let strip_count = legacy.strips.len();
        Self {
            group_id: legacy.group_id,
            chunk_id: legacy.chunk_id,
            writer_epoch: legacy.writer_epoch,
            lease_generation: legacy.lease_generation,
            lease_deadline_ms: legacy.lease_deadline_ms,
            placement_epoch: legacy.placement_epoch,
            strips: legacy.strips,
            states: legacy.states,
            parity_segments: legacy.parity_segments,
            preferred_survivors: legacy.preferred_survivors,
            data_num: legacy.data_num,
            code_num: legacy.code_num,
            planned_cursors: vec![0; strip_count],
            planned_closed_sequences: vec![u32::MAX; strip_count],
        }
    }
}
