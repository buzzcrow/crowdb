// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reconcile checkpoint publication across independent layout preparation.

use crowdb_chunk_client::IoError;
use crowdb_protocol::chunkdb::rpc::{AdvanceChunkWriteRequest, Chunk, ChunkState, QueryChunkRequest};

use super::{update_state, ChunkStateView, Ordering, ProductionStreamChunkStore};
use crate::{CursorAdvance, Result, StreamError, StreamName};

impl ProductionStreamChunkStore {
    pub(super) async fn advance_active_cursor(
        &self,
        stream_name: StreamName,
        mut request: AdvanceChunkWriteRequest,
        expected_cursor: u64,
        checksum: u32,
    ) -> Result<CursorAdvance> {
        let chunk_id = request.chunk_id.expect("cursor request has a chunk");
        let state = self.state(chunk_id).await?;
        validate_owner(&state.chunk.load(), stream_name, &request)?;
        if state.persisted_cursor.load(Ordering::Acquire) != expected_cursor {
            return Err(StreamError::StaleWriter);
        }
        for attempt in 0..2 {
            request.expected_modify_ts = state.modify_ts.load(Ordering::Acquire);
            let advance = self.allocator.advance_chunk_write(request.clone()).await;
            if let Ok(response) = advance {
                let chunk = response
                    .chunk
                    .ok_or_else(|| StreamError::Corruption("cursor advance returned no chunk".into()))?;
                validate_owner(&chunk, stream_name, &request)?;
                if chunk.acknowledged_cursor != request.acknowledged_cursor {
                    return Err(StreamError::Corruption(
                        "cursor advance returned inconsistent metadata".into(),
                    ));
                }
                return Ok(committed(&state, chunk, checksum));
            }
            let definite_conflict = matches!(advance, Err(IoError::MetadataConflict(_)));
            if let Err(error) = &advance {
                if !definite_conflict {
                    tracing::warn!(?chunk_id, %error, "stream cursor advance failed; querying durable state");
                }
            }
            let response = self
                .allocator
                .query_chunk(QueryChunkRequest {
                    chunk_id: Some(chunk_id),
                })
                .await;
            let Ok(Some(chunk)) = response.map(|response| response.chunk) else {
                return Ok(CursorAdvance::Ambiguous);
            };
            validate_owner(&chunk, stream_name, &request)?;
            update_state(&state, chunk.clone());
            if chunk.acknowledged_cursor == request.acknowledged_cursor {
                return Ok(committed(&state, chunk, checksum));
            }
            if attempt == 0
                && definite_conflict
                && chunk.state == ChunkState::Active as i32
                && chunk.acknowledged_cursor == expected_cursor
                && chunk.modify_ts > request.expected_modify_ts
            {
                // One preparation may have published a new layout revision.
                // Only retry a definite rejection, never ambiguous data IO.
                continue;
            }
            return Ok(CursorAdvance::Ambiguous);
        }
        unreachable!("one definite-conflict retry returns a cursor outcome")
    }
}

fn validate_owner(chunk: &Chunk, name: StreamName, request: &AdvanceChunkWriteRequest) -> Result<()> {
    if chunk.id != request.chunk_id
        || chunk.writer_epoch != request.writer_epoch
        || StreamName::from_chunk_owner_key(&chunk.owner_key) != Some(name)
    {
        return Err(StreamError::StaleWriter);
    }
    Ok(())
}

fn committed(state: &ChunkStateView, chunk: Chunk, checksum: u32) -> CursorAdvance {
    update_state(state, chunk);
    state.last_checksum.store(checksum, Ordering::Release);
    state.has_checksum.store(true, Ordering::Release);
    CursorAdvance::Committed
}
