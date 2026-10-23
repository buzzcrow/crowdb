// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fresh attributed Stream/Wal allocation before attachment publication.

use crate::perf::{perf, PhaseTimer};
use bytes::Bytes;
use crowdb_chunk_client::STREAM_STRIP_BYTES;
use crowdb_protocol::chunk_stream::StreamPurpose;
use crowdb_protocol::chunkdb::rpc::{AllocateChunkRequest, Chunk, ChunkState, ChunkStrip, Strip, StripType};
use futures::{stream::FuturesUnordered, StreamExt};

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
        let state = self.install(chunk.clone())?;
        self.initialize_strip(&chunk, &chunk.strips[0]).await?;
        state
            .initialized_capacity
            .store(STREAM_STRIP_BYTES, Ordering::Release);
        state.fresh_grouped_allocation.store(true, Ordering::Release);
        Ok(ActiveChunkDescriptor {
            chunk_id,
            physical_start: 0,
            logical_start: 0,
            acknowledged_cursor: 0,
            capacity: STREAM_STRIP_BYTES,
        })
    }

    pub(super) async fn initialize_strips(&self, chunk: &Chunk, strips: &[ChunkStrip]) -> Result<()> {
        // Await every submitted synchronization even if another strip failed.
        let mut pending = FuturesUnordered::new();
        let mut strips = strips.iter();
        for strip in strips.by_ref().take(8) {
            pending.push(self.initialize_strip(chunk, strip));
        }
        let mut error = None;
        while let Some(result) = pending.next().await {
            if let Err(failure) = result {
                error.get_or_insert(failure);
            }
            if let Some(strip) = strips.next() {
                pending.push(self.initialize_strip(chunk, strip));
            }
        }
        error.map_or(Ok(()), Err)
    }

    pub(super) async fn initialize_strip(&self, chunk: &Chunk, strip: &ChunkStrip) -> Result<()> {
        let _timer = PhaseTimer::start(&perf().initialize);
        progress::ensure_mirror_copies(strip, self.mirror_copies)?;
        if u64::from(strip.capacity) * 1024 != STREAM_STRIP_BYTES {
            return Err(StreamError::Corruption(
                "stream strip has invalid initialization capacity".into(),
            ));
        }
        let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
            return Err(StreamError::Corruption(
                "fresh stream allocation is not mirrored".into(),
            ));
        };
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        for (index, segment) in mirror.segments.iter().enumerate() {
            if segment.disk_id.is_none()
                || segment.owner_chunk != chunk.id
                || segment.allocation_ts == 0
                || u64::from(segment.unit_count) * unit_bytes < STREAM_STRIP_BYTES
                || segment
                    .unit_offset
                    .checked_add(u64::from(segment.unit_count))
                    .and_then(|end| end.checked_mul(unit_bytes))
                    .is_none()
                || mirror.segments[..index]
                    .iter()
                    .any(|previous| previous.disk_id == segment.disk_id)
            {
                return Err(StreamError::Corruption(
                    "fresh stream segment identity or geometry is invalid".into(),
                ));
            }
        }
        let strip_bytes = usize::try_from(STREAM_STRIP_BYTES).expect("bounded stream strip");
        let zeroes = Bytes::from(vec![0; strip_bytes]);
        let writes = mirror.segments.iter().map(|segment| {
            self.disk_writer
                .initialize_synchronized_at_byte_offset(segment, unit_bytes, 0, zeroes.clone())
        });
        let results = futures::future::join_all(writes).await;
        for result in results {
            result.map_err(io_error)?;
        }
        Ok(())
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
