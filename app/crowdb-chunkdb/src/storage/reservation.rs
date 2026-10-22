// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable strip-reservation values co-located with their owning chunk.

use bytes::Bytes;
use crowdb_kv_client::{BatchOp, GetOutcome, ReadMode, ScanOutcome};
use crowdb_protocol::chunkdb::rpc::{Chunk, StripReservationGroup};
use crowdb_protocol::common::ChunkId;

use crate::routing::Route;

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
        let start_after = start_after.map_or_else(Vec::new, |(chunk_id, group_id)| {
            reservation_key(chunk_id, group_id)
        });
        self.scan_owned_records(b"/reservation/", &start_after, max_keys)
            .await?
            .into_iter()
            .map(|(_, value)| decode_group(&value))
            .collect()
    }

    pub async fn list_reservation_groups(&self, chunk_id: &ChunkId) -> Result<Vec<StripReservationGroup>> {
        let reservation_route = self.route(chunk_id)?;
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
        let reservation_route = self.route(chunk_id)?;
        let key = reservation_key(chunk_id, group_id);
        if let Some(value) = self.read_reservation_raw(&reservation_route, &key).await? {
            return decode_group(&value).map(Some);
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
                    value: Bytes::from(encode_chunk(chunk)?),
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
            value: Bytes::from(encode_chunk(chunk)?),
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
        let reservation_route = self.route(chunk_id)?;
        self.write_reservation_ops_at(
            reservation_route.kv_store_id,
            reservation_route.kv_group_id,
            chunk_id,
            ops,
            chunk,
        )
        .await?;
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
        let authority = self.capture_authority(chunk_id)?;
        let Some(chunk) = chunk else {
            self.check_submission(chunk_id, authority.as_ref())?;
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
        self.check_submission(chunk_id, authority.as_ref())?;
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
    use bincode::Options;
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(bytes.len() as u64)
        .reject_trailing_bytes()
        .deserialize(bytes)
        .map_err(|error| StoreError::Serde(error.to_string()))
}
