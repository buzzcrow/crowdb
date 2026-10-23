// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Grow physical allocation only after initializing every newly attached strip.

use super::{
    chunk_capacity, io_error, update_state, write_cursor, ActiveChunkDescriptor, ChunkId, ChunkStateView,
    Ordering, ProductionStreamChunkStore, Result, StreamError, StreamName,
};
use crate::perf::{perf, PhaseTimer};
use crowdb_chunk_client::{IoError, STREAM_STRIP_BYTES};
use crowdb_protocol::chunkdb::rpc::{
    AppendChunkRequest, AppendChunkResponse, Chunk, QueryChunkRequest, Strip, StripType,
};

const MIN_GROWTH_WINDOW_BYTES: u64 = 2 * STREAM_STRIP_BYTES;
const MAX_GROWTH_WINDOW_BYTES: u64 = 8 * STREAM_STRIP_BYTES;

impl ProductionStreamChunkStore {
    pub(super) async fn grow_active_chunk(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
        chunk_id: ChunkId,
        required_capacity: u64,
    ) -> Result<Option<ActiveChunkDescriptor>> {
        if required_capacity > self.chunk_capacity_bytes {
            return Ok(None);
        }
        let state = self.state(chunk_id).await?;
        for attempt in 0..2 {
            let chunk = state.chunk.load_full();
            if chunk.writer_epoch != writer_epoch || state.sealed.load(Ordering::Acquire) {
                return Err(StreamError::StaleWriter);
            }
            let capacity = chunk_capacity(&chunk)?;
            if StreamName::from_chunk_owner_key(&chunk.owner_key) != Some(stream_name)
                || state.initialized_capacity.load(Ordering::Acquire) != capacity
            {
                return Err(StreamError::InvalidRequest(
                    "stream growth requires initialized owned capacity".into(),
                ));
            }
            if capacity >= required_capacity {
                return Ok(Some(ActiveChunkDescriptor {
                    chunk_id,
                    physical_start: write_cursor(&state),
                    logical_start: 0,
                    acknowledged_cursor: write_cursor(&state),
                    capacity,
                }));
            }
            // Small appends must not wait for a large initialization burst.
            // Large appends amortize allocation over several following writes.
            let window = required_capacity
                .saturating_sub(write_cursor(&state))
                .saturating_mul(4)
                .clamp(MIN_GROWTH_WINDOW_BYTES, MAX_GROWTH_WINDOW_BYTES)
                .next_power_of_two();
            let target = required_capacity
                .div_ceil(window)
                .saturating_mul(window)
                .min(self.chunk_capacity_bytes);
            let growth_timer = PhaseTimer::start(&perf().growth);
            let response = self.request_growth(&chunk, target, capacity).await?;
            drop(growth_timer);
            if let Some(current) = response.chunk {
                update_state(&state, current);
                if attempt == 0 {
                    continue;
                }
                return Err(StreamError::WriteStalled);
            }
            return self.finish_growth(&state, &chunk, response).await.map(Some);
        }
        unreachable!("two append attempts either return or fail")
    }

    async fn finish_growth(
        &self,
        state: &ChunkStateView,
        chunk: &Chunk,
        response: AppendChunkResponse,
    ) -> Result<ActiveChunkDescriptor> {
        if response.strips.is_empty() {
            return Err(StreamError::Corruption(
                "stream chunk growth returned no strips".into(),
            ));
        }
        let added = response.strips;
        // Allocation may merge a concurrent cursor commit. Fetch the full
        // record instead of stamping its new revision onto our old cursor,
        // which could hide a delayed cursor response.
        let updated = self
            .allocator
            .query_chunk(QueryChunkRequest { chunk_id: chunk.id })
            .await
            .map_err(preparation_error)?
            .chunk
            .ok_or_else(|| StreamError::Corruption("grown chunk metadata is missing".into()))?;
        if updated.id != chunk.id
            || updated.writer_epoch != chunk.writer_epoch
            || updated.owner_key != chunk.owner_key
        {
            return Err(StreamError::StaleWriter);
        }
        if updated.modify_ts < response.modify_ts
            || updated.strips.get(chunk.strips.len()..) != Some(added.as_slice())
        {
            return Err(StreamError::Corruption(
                "grown layout changed before initialization".into(),
            ));
        }
        let capacity = chunk_capacity(&updated)?;
        update_state(state, updated.clone());
        self.initialize_strips(&updated, &added).await.map_err(|error| {
            StreamError::InvalidRequest(format!("stream growth initialization is uncertain: {error}"))
        })?;
        state.initialized_capacity.store(capacity, Ordering::Release);
        Ok(ActiveChunkDescriptor {
            chunk_id: chunk.id.expect("owned chunk has an identity"),
            physical_start: write_cursor(state),
            logical_start: 0,
            acknowledged_cursor: write_cursor(state),
            capacity,
        })
    }

    async fn request_growth(
        &self,
        chunk: &Chunk,
        required_capacity: u64,
        capacity: u64,
    ) -> Result<AppendChunkResponse> {
        let first = chunk
            .strips
            .first()
            .ok_or_else(|| StreamError::Corruption("stream chunk has no mirror strip".into()))?;
        let Some(Strip::MirrorStrip(mirror)) = &first.strip else {
            return Err(StreamError::Corruption(
                "stream chunk strip is not mirrored".into(),
            ));
        };
        let unit_count = mirror
            .segments
            .first()
            .map(|segment| segment.unit_count)
            .filter(|count| *count > 0)
            .ok_or_else(|| StreamError::Corruption("stream mirror strip has no segment geometry".into()))?;
        let strip_bytes = u64::from(first.capacity) * 1024;
        let strip_count = u32::try_from((required_capacity - capacity).div_ceil(strip_bytes))
            .map_err(|_| StreamError::InvalidRequest("stream growth needs too many strips".into()))?;
        self.allocator
            .append_chunk(AppendChunkRequest {
                chunk_id: chunk.id,
                modify_ts: chunk.modify_ts,
                strip_size: unit_count,
                strip_count,
                strip_type: StripType::Mirror as i32,
                data_num: 0,
                code_num: 0,
                copy_count: self.mirror_copies,
            })
            .await
            .map_err(preparation_error)
    }
}

fn preparation_error(error: IoError) -> StreamError {
    match error {
        IoError::AllocationFailed(message) => StreamError::ReadUnavailable(message),
        other => io_error(other),
    }
}
