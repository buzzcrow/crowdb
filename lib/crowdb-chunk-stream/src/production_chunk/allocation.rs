// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fresh attributed Stream/Wal allocation before attachment publication.

use crowdb_chunk_client::STREAM_STRIP_BYTES;
use crowdb_protocol::chunk_stream::StreamPurpose;
use crowdb_protocol::chunkdb::rpc::{AllocateChunkRequest, Chunk, ChunkState, StripType};

use super::{
    io_error, progress, ActiveChunkDescriptor, Ordering, ProductionStreamChunkStore, Result, StreamError,
    StreamName,
};

impl ProductionStreamChunkStore {
    pub(super) async fn allocate_active_chunk(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
        purpose: StreamPurpose,
    ) -> Result<ActiveChunkDescriptor> {
        if writer_epoch == 0 {
            return Err(StreamError::InvalidRequest(
                "stream allocation requires a writer epoch".into(),
            ));
        }
        let response = self
            .allocator
            .allocate_chunk(AllocateChunkRequest {
                chunk_id: None,
                write_granularity: u32::try_from(STREAM_STRIP_BYTES / 1024).expect("bounded strip"),
                strip_count: 1,
                strip_type: StripType::Mirror as i32,
                data_num: 0,
                code_num: 0,
                copy_count: self.mirror_copies,
                chunk_type: purpose.chunk_type() as i32,
                writer_epoch,
                writer_lease_ms: self.writer_lease_ms,
                owner_key: stream_name.chunk_owner_key(),
            })
            .await
            .map_err(io_error)?;
        let chunk = response
            .chunk
            .ok_or_else(|| StreamError::Corruption("stream allocation returned no chunk".into()))?;
        validate_allocation(&chunk, stream_name, writer_epoch, purpose)?;
        progress::ensure_mirror_copies(&chunk.strips[0], self.mirror_copies)?;
        let chunk_id = chunk.id.expect("validated allocation identity");
        if self.chunks.get(&(chunk_id.high, chunk_id.low)).is_some() {
            return Err(StreamError::Corruption(
                "allocator reused a known stream chunk identity".into(),
            ));
        }
        self.install(chunk)?
            .fresh_grouped_allocation
            .store(true, Ordering::Release);
        Ok(ActiveChunkDescriptor {
            chunk_id,
            physical_start: 0,
            logical_start: 0,
            acknowledged_cursor: 0,
            capacity: STREAM_STRIP_BYTES,
        })
    }
}

fn validate_allocation(chunk: &Chunk, name: StreamName, epoch: u64, purpose: StreamPurpose) -> Result<()> {
    if !chunk.id.is_some_and(|id| purpose.matches(id))
        || chunk.chunk_type != purpose.chunk_type() as i32
        || chunk.writer_epoch != epoch
        || StreamName::from_chunk_owner_key(&chunk.owner_key) != Some(name)
        || chunk.state != ChunkState::Active as i32
        || chunk.modify_ts == 0
        || chunk.acknowledged_cursor != 0
        || chunk.strips.len() != 1
        || u64::from(chunk.capacity) * 1024 != STREAM_STRIP_BYTES
    {
        return Err(StreamError::Corruption(
            "stream allocation has invalid identity, ownership, progress or capacity".into(),
        ));
    }
    let strip = &chunk.strips[0];
    if strip.chunk_offset != 0 || u64::from(strip.capacity) * 1024 != STREAM_STRIP_BYTES || strip.unit_kb == 0
    {
        return Err(StreamError::Corruption(
            "stream allocation has invalid initial strip geometry".into(),
        ));
    }
    Ok(())
}
