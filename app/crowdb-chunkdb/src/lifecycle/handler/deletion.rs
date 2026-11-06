// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Physical deletion and fixed allocation identity fencing.
use super::{
    extract_segments, info, reservation, unix_time_ms, CacheHint, Chunk, ChunkGuard, ChunkId, ChunkState,
    LifecycleError, LifecycleHandler, LockPolicy, ProtoChunkState, StoreError, StripReservationState,
};

impl LifecycleHandler {
    /// Delete a chunk — marks deleted and frees segments.
    /// Repeated deletion returns the existing tombstone so retries are idempotent.
    pub async fn delete_chunk(&self, chunk_id: &ChunkId) -> Result<Chunk, LifecycleError> {
        self.delete_chunk_fenced(chunk_id, false).await
    }

    /// Fence a fixed identity even when its allocation request has not arrived.
    pub async fn delete_chunk_fenced(
        &self,
        chunk_id: &ChunkId,
        fence_missing: bool,
    ) -> Result<Chunk, LifecycleError> {
        self.check_range(chunk_id)?;
        if fence_missing && !matches!(chunk_id.high >> 56, 2 | 3) {
            return Err(LifecycleError::InvalidRequest(
                "allocation fencing requires a tree chunk identity".into(),
            ));
        }
        let mut guard = if let Some(locks) = &self.locks {
            Some(if fence_missing {
                locks
                    .acquire_for_create(chunk_id, &LockPolicy::default(), CacheHint::Cache)
                    .await?
            } else {
                locks
                    .acquire(chunk_id, &self.store, &LockPolicy::default(), CacheHint::Cache)
                    .await?
            })
        } else {
            None
        };

        let chunk = if fence_missing {
            match self.store.get_chunk(chunk_id).await {
                Ok(chunk) => chunk,
                Err(StoreError::ChunkNotFound) => {
                    let chunk = Chunk {
                        id: Some(*chunk_id),
                        modify_ts: 1,
                        state: ProtoChunkState::Deleted as i32,
                        create_ts_ms: unix_time_ms(),
                        chunk_type: if chunk_id.high >> 56 == 2 { 2 } else { 3 },
                        ..Default::default()
                    };
                    self.store.put_chunk(&chunk).await?;
                    if let Some(ref mut guard) = guard {
                        guard.refresh(chunk.clone());
                    }
                    return Ok(chunk);
                }
                Err(error) => return Err(error.into()),
            }
        } else {
            match &guard {
                Some(guard) => guard
                    .chunk()
                    .unwrap_or_else(|| unreachable!("acquire guarantees a chunk"))
                    .clone(),
                None => self.store.get_chunk(chunk_id).await?,
            }
        };
        self.finish_deletion(chunk_id, chunk, guard).await
    }

    async fn finish_deletion(
        &self,
        chunk_id: &ChunkId,
        mut chunk: Chunk,
        mut guard: Option<ChunkGuard>,
    ) -> Result<Chunk, LifecycleError> {
        let current_state = ChunkState::from_proto(chunk.state);

        // A Deleted record that still owns strips is a durable cleanup intent.
        if current_state == ChunkState::Deleted {
            if chunk.strips.is_empty() {
                return Ok(chunk);
            }
            let segments: Vec<_> = chunk.strips.iter().flat_map(extract_segments).collect();
            self.allocator
                .pool()
                .free_blocks(segments)
                .await
                .map_err(LifecycleError::Cleanup)?;
            chunk.strips.clear();
            chunk.capacity = 0;
            chunk.modify_ts = chunk.modify_ts.saturating_add(1);
            self.store.put_chunk(&chunk).await?;
            if let Some(ref mut g) = guard {
                g.refresh(chunk.clone());
            }
            return Ok(chunk);
        }

        current_state.check_can_delete()?;

        self.retire_reservation_groups(chunk_id).await?;

        // Publish Deleted before making any referenced block reusable.
        let all_segments: Vec<_> = chunk.strips.iter().flat_map(extract_segments).collect();
        chunk.state = ProtoChunkState::Deleted as i32;
        chunk.modify_ts = chunk.modify_ts.saturating_add(1);
        self.store.put_chunk(&chunk).await?;

        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        }
        if !all_segments.is_empty() {
            self.allocator
                .pool()
                .free_blocks(all_segments)
                .await
                .map_err(LifecycleError::Cleanup)?;
        }
        chunk.strips.clear();
        chunk.capacity = 0;
        chunk.modify_ts = chunk.modify_ts.saturating_add(1);
        self.store.put_chunk(&chunk).await?;
        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        }
        info!(chunk_id = ?chunk_id, "chunk deleted");
        Ok(chunk)
    }
    async fn retire_reservation_groups(&self, chunk_id: &ChunkId) -> Result<(), LifecycleError> {
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
        self.rollback_reserved_resources(&reserved_strips, reserved_parity, &reserved_parity_groups)
            .await?;
        for group in self.store.list_reservation_groups(chunk_id).await? {
            if let Some(group_id) = group.group_id {
                self.store.delete_reservation_group(chunk_id, &group_id).await?;
            }
        }

        Ok(())
    }
}
