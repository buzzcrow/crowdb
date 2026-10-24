// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Exact dead byte ranges, co-located with and fenced by the owning chunk.

mod record;
pub use record::{ChunkRangeRetirement, RetiredByteRange};

use super::{chunk_key, decode_chunk, encode_chunk, ChunkStore, Result, StoreError};
use bytes::Bytes;
use crowdb_kv_client::{BatchOp, GetOutcome, ReadMode};
use crowdb_protocol::chunk_domain::ChunkDomain;
use crowdb_protocol::chunkdb::rpc::{ChunkState, DeleteChunkRangeRequest, DeleteChunkRangeResponse};
use crowdb_protocol::common::ChunkId;

impl ChunkStore {
    /// Qualifies and records a logically dead range without freeing physical blocks.
    /// The caller must establish absence of live/future references before calling.
    /// Conflicts and unknown results are returned without confirmation reads.
    pub async fn retire_chunk_range(
        &self,
        request: &DeleteChunkRangeRequest,
    ) -> Result<DeleteChunkRangeResponse> {
        let id = request
            .chunk_id
            .ok_or_else(|| invalid("missing chunk identity"))?;
        let end = u64::from(request.chunk_offset) + u64::from(request.chunk_size);
        if request.chunk_size == 0 {
            return Err(invalid("empty range"));
        }
        let route = self.route(&id)?;
        let authority = self.capture_authority(&id)?;
        let key = chunk_key(&id);
        let GetOutcome::Found { value, revision } = self
            .kv
            .get(
                route.kv_store_id,
                route.kv_group_id,
                &key,
                ReadMode::Linearizable,
                None,
            )
            .await
            .map_err(|error| StoreError::Kv(error.to_string()))?
        else {
            return Err(StoreError::ChunkNotFound);
        };
        let chunk = decode_chunk(&value)?;
        if chunk.id != Some(id)
            || ChunkDomain::for_chunk(&id) != Some(ChunkDomain::UserData)
            || chunk.chunk_type != i32::from(id.high.to_be_bytes()[0])
            || chunk.modify_ts == 0
        {
            return Err(invalid("range must belong to a user-data chunk"));
        }
        match ChunkState::try_from(chunk.state) {
            Ok(ChunkState::Deleted) => {
                return Ok(DeleteChunkRangeResponse {
                    reclaimed: chunk.strips.is_empty() && chunk.cleanup_intents.is_empty(),
                })
            }
            Ok(ChunkState::Sealed) => {}
            _ => return Err(StoreError::Conflict),
        }
        if chunk.writer_epoch == 0 {
            if request.chunk_offset != 0 || end.div_ceil(1024) != u64::from(chunk.sealed_length) {
                return Err(invalid("range must own the complete sealed dedicated chunk"));
            }
        } else if end > chunk.acknowledged_cursor {
            return Err(StoreError::Conflict);
        }
        let mut record = self
            .get_chunk_range_retirement(&id)
            .await?
            .unwrap_or_else(|| ChunkRangeRetirement::new(&chunk));
        record.validate(&chunk)?;
        let range = RetiredByteRange {
            start: u64::from(request.chunk_offset),
            end,
        };
        if record.contains(range) {
            // Repeating a proven retirement is safe after later layout changes.
            return Ok(DeleteChunkRangeResponse { reclaimed: false });
        }
        if request.expected_modify_ts != 0 && chunk.modify_ts != request.expected_modify_ts {
            return Err(StoreError::Conflict);
        }
        record.insert(range)?;
        record.revision = chunk.modify_ts;
        // Re-put the unchanged chunk as the atomic batch precondition. Retirement
        // does not change physical layout or invalidate the foreground layout cache.
        let ops = [
            BatchOp::Put {
                key: Bytes::from(key.clone()),
                value: Bytes::from(encode_chunk(&chunk)),
            },
            BatchOp::Put {
                key: Bytes::from(resource_key(&id)),
                value: Bytes::from(record.encode()?),
            },
        ];
        self.check_submission(&id, authority.as_ref())?;
        match self
            .kv
            .batch_write_cas(route.kv_store_id, route.kv_group_id, &ops, &key, revision)
            .await
        {
            Ok(_) => Ok(DeleteChunkRangeResponse { reclaimed: false }),
            Err(crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::CasBusy) => {
                Err(StoreError::Conflict)
            }
            Err(error) => Err(StoreError::Kv(error.to_string())),
        }
    }

    /// Loads physical retirement metadata; ordinary chunk reads do not fetch it.
    pub async fn get_chunk_range_retirement(&self, id: &ChunkId) -> Result<Option<ChunkRangeRetirement>> {
        let route = self.route(id)?;
        match self
            .kv
            .get(
                route.kv_store_id,
                route.kv_group_id,
                &resource_key(id),
                ReadMode::Linearizable,
                None,
            )
            .await
            .map_err(|error| StoreError::Kv(error.to_string()))?
        {
            GetOutcome::NotFound => Ok(None),
            GetOutcome::Found { value, .. } => ChunkRangeRetirement::decode(&value).map(Some),
        }
    }
}

fn resource_key(id: &ChunkId) -> Vec<u8> {
    let mut key = b"/chunk-ranges/".to_vec();
    key.extend_from_slice(&id.high.to_be_bytes());
    key.extend_from_slice(&id.low.to_be_bytes());
    key
}
fn invalid(message: &str) -> StoreError {
    StoreError::InvalidRange(message.into())
}
