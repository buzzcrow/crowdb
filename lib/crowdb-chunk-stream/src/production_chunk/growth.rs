// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Grow physical allocation only after initializing every newly attached strip.

use super::{
    chunk_capacity, io_error, update_state, write_cursor, ActiveChunkDescriptor, ChunkId, Ordering,
    ProductionStreamChunkStore, Result, StreamError, StreamName,
};
use crate::perf::{perf, PhaseTimer};
use crowdb_protocol::chunkdb::rpc::{AppendChunkRequest, AppendChunkResponse, Chunk, Strip, StripType};

const GROWTH_WINDOW_BYTES: u64 = 8 * 1024 * 1024;

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
            let target = required_capacity
                .div_ceil(GROWTH_WINDOW_BYTES)
                .saturating_mul(GROWTH_WINDOW_BYTES)
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
            if response.strips.is_empty() {
                return Err(StreamError::Corruption(
                    "stream chunk growth returned no strips".into(),
                ));
            }
            let mut updated = (*chunk).clone();
            let added = response.strips;
            updated.strips.extend(added.iter().cloned());
            updated.modify_ts = response.modify_ts;
            updated.capacity = updated.strips.iter().map(|strip| strip.capacity).sum();
            let capacity = chunk_capacity(&updated)?;
            update_state(&state, updated.clone());
            self.initialize_strips(&updated, &added).await.map_err(|error| {
                StreamError::InvalidRequest(format!("stream growth initialization is uncertain: {error}"))
            })?;
            state.initialized_capacity.store(capacity, Ordering::Release);
            return Ok(Some(ActiveChunkDescriptor {
                chunk_id,
                physical_start: write_cursor(&state),
                logical_start: 0,
                acknowledged_cursor: write_cursor(&state),
                capacity,
            }));
        }
        unreachable!("two append attempts either return or fail")
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
            .map_err(io_error)
    }
}
