// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Exact byte sealing and retirement of unused strips.

use super::{
    close_acknowledged_strips, extract_segments, reservation, seal_written_strips, unix_time_ms, CacheHint,
    Chunk, ChunkId, ChunkState, LifecycleError, LifecycleHandler, LockPolicy, ProtoChunkState, Segment,
    StripCleanupIntent, StripReservationState,
};
use tracing::info;

impl LifecycleHandler {
    /// Seal a chunk — no more appends allowed.
    pub async fn seal_chunk(&self, chunk_id: &ChunkId, seal_length: u32) -> Result<Chunk, LifecycleError> {
        self.seal_chunk_bytes(chunk_id, u64::from(seal_length) * 1024)
            .await
    }

    pub async fn seal_chunk_bytes(
        &self,
        chunk_id: &ChunkId,
        seal_bytes: u64,
    ) -> Result<Chunk, LifecycleError> {
        let seal_length = u32::try_from(seal_bytes.div_ceil(1024))
            .map_err(|_| LifecycleError::InvalidRequest("seal size exceeds capacity field".into()))?;
        self.check_range(chunk_id)?;

        let mut guard = if let Some(locks) = &self.locks {
            Some(
                locks
                    .acquire(chunk_id, &self.store, &LockPolicy::default(), CacheHint::Cache)
                    .await?,
            )
        } else {
            None
        };

        let mut chunk = match &guard {
            Some(g) => g
                .chunk()
                .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
                .clone(),
            None => self.store.get_chunk(chunk_id).await?,
        };
        let current_state = ChunkState::from_proto(chunk.state);
        current_state.check_can_seal()?;
        if seal_length > chunk.capacity {
            return Err(LifecycleError::InvalidRequest(format!(
                "seal_length {seal_length} exceeds chunk capacity {}",
                chunk.capacity
            )));
        }

        let reservation_groups = self.store.list_reservation_groups(chunk_id).await?;
        if reservation_groups
            .iter()
            .any(|group| group.states.contains(&(StripReservationState::Consumed as i32)))
        {
            return Err(LifecycleError::StateConflict);
        }
        let mut reserved_strips = Vec::new();
        let mut reserved_parity = Vec::new();
        let mut reserved_parity_groups = Vec::new();
        for mut group in reservation_groups {
            reservation::validate_group_shape(&group)?;
            reserved_parity.extend_from_slice(&group.parity_segments);
            for (index, state) in group.states.iter_mut().enumerate() {
                if *state == StripReservationState::Reserved as i32 {
                    *state = StripReservationState::Cancelled as i32;
                    reserved_strips.push(group.strips[index].clone());
                } else if *state == StripReservationState::Cancelled as i32 {
                    reserved_strips.push(group.strips[index].clone());
                }
            }
            self.store.put_reservation_group(&group).await?;
            reserved_parity_groups.push(group);
        }

        let now_ms = unix_time_ms();
        let (unused_segments, cleanup_operation) = trim_sealed_strips(&mut chunk, chunk_id, seal_length);

        chunk.state = ProtoChunkState::Sealed as i32;
        chunk.modify_ts = chunk.modify_ts.saturating_add(1);
        chunk.sealed_length = seal_length;
        chunk.acknowledged_cursor = seal_bytes;
        chunk.sealed_ts_ms = now_ms;
        chunk.capacity = chunk.strips.iter().map(|strip| strip.capacity).sum();
        if let Some(operation_id) = cleanup_operation {
            chunk.cleanup_intents.push(StripCleanupIntent {
                operation_id: Some(operation_id),
                retired_segments: unused_segments.clone(),
                not_before_ms: now_ms,
            });
        }
        self.rollback_reserved_resources(&reserved_strips, reserved_parity, &reserved_parity_groups)
            .await?;
        for group in self.store.list_reservation_groups(chunk_id).await? {
            if let Some(group_id) = group.group_id {
                self.store.delete_reservation_group(chunk_id, &group_id).await?;
            }
        }
        seal_written_strips(&mut chunk, seal_length, now_ms);
        close_acknowledged_strips(&mut chunk, now_ms);

        self.store.put_chunk(&chunk).await?;

        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        }
        if let Some(operation_id) = cleanup_operation {
            self.allocator
                .pool()
                .free_blocks(unused_segments)
                .await
                .map_err(LifecycleError::Cleanup)?;
            chunk
                .cleanup_intents
                .retain(|intent| intent.operation_id != Some(operation_id));
            chunk.modify_ts = chunk.modify_ts.saturating_add(1);
            self.store.put_chunk(&chunk).await?;
            if let Some(ref mut g) = guard {
                g.refresh(chunk.clone());
            }
        }
        info!(chunk_id = ?chunk_id, seal_length, "chunk sealed");
        Ok(chunk)
    }
}

fn trim_sealed_strips(
    chunk: &mut Chunk,
    chunk_id: &ChunkId,
    seal_length: u32,
) -> (Vec<Segment>, Option<ChunkId>) {
    let first_unused = chunk
        .strips
        .iter()
        .position(|strip| strip.chunk_offset >= seal_length)
        .unwrap_or(chunk.strips.len());
    let unused_strips = chunk.strips.split_off(first_unused);
    let unused_segments: Vec<_> = unused_strips.iter().flat_map(extract_segments).collect();
    let operation = (!unused_segments.is_empty()).then_some(ChunkId {
        high: chunk_id.high ^ chunk.modify_ts,
        low: chunk_id.low ^ u64::from(seal_length),
    });
    (unused_segments, operation)
}
