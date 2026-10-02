// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    assign_strip_offsets, reservation_usage, validate_chunk_fence, validate_chunk_identity,
    validate_existing_group, validate_reserve_spec, writer_lease_deadline, ChunkId, ChunkState, ChunkStrip,
    LifecycleError, LifecycleHandler, PendingReservation, ReservationFence, ReservationMutation,
    ReservationPermit, ReserveGroupSpec, Segment, StripAllocType, StripBatchSpec, StripReservationGroup,
    StripReservationState,
};

impl LifecycleHandler {
    async fn allocate_reservation_resources(
        &self,
        chunk_id: &ChunkId,
        start_sequence: u32,
        spec: ReserveGroupSpec,
    ) -> Result<(Vec<ChunkStrip>, Vec<Segment>, Vec<u32>, ReservationPermit), LifecycleError> {
        let snap = self.topology.snapshot();
        let usage = reservation_usage(spec, snap.unit_size_bytes());
        let permit = self
            .reservation_admission
            .try_acquire(usage.0, usage.1)
            .ok_or(LifecycleError::ReservationLimit)?;
        if spec.conversion_data_num != 0 || spec.conversion_code_num != 0 {
            if spec.strip_count != spec.conversion_data_num || spec.conversion_code_num == 0 {
                return Err(LifecycleError::InvalidRequest(
                    "conversion reservation geometry must match its data width".into(),
                ));
            }
            let allocation = self
                .allocator
                .allocate_conversion_group(
                    &snap,
                    chunk_id,
                    spec.strip_size,
                    start_sequence,
                    spec.conversion_data_num as usize,
                    spec.conversion_code_num as usize,
                    spec.copy_count as usize,
                    &self.placement_constraints(),
                )
                .await?;
            return Ok((
                allocation.mirrors,
                allocation.parity_segments,
                allocation.preferred_survivors,
                permit,
            ));
        }
        let strips = self
            .allocator
            .allocate_strips(
                &snap,
                chunk_id,
                StripBatchSpec {
                    strip_type: StripAllocType::Mirror {
                        copy_count: spec.copy_count as usize,
                    },
                    unit_count: spec.strip_size,
                    start_sequence,
                    strip_count: spec.strip_count,
                },
                &self.placement_constraints(),
            )
            .await?;
        Ok((strips, Vec::new(), Vec::new(), permit))
    }

    pub async fn reserve_strip_group(
        &self,
        chunk_id: &ChunkId,
        group_id: &ChunkId,
        fence: ReservationFence,
        spec: ReserveGroupSpec,
    ) -> Result<ReservationMutation, LifecycleError> {
        let capacity_kb = spec
            .strip_size
            .saturating_mul(self.topology.snapshot().unit_size_bytes() / 1024);
        self.validate_strip_layout(
            super::super::ProtoStripType::Mirror,
            spec.conversion_data_num,
            spec.conversion_code_num,
            spec.copy_count,
            capacity_kb,
        )?;
        self.check_range(chunk_id)?;
        validate_reserve_spec(fence, spec)?;
        let guard = self.acquire_reservation_guard(chunk_id).await?;
        let chunk = guard
            .chunk()
            .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
            .clone();
        ChunkState::from_proto(chunk.state).check_can_append()?;
        validate_chunk_identity(&chunk, fence.writer_epoch)?;
        if let Some(existing) = self.store.get_reservation_group(chunk_id, group_id).await? {
            validate_existing_group(&existing, chunk_id, group_id, fence, spec)?;
            return Ok(ReservationMutation {
                chunk,
                group: Some(existing),
            });
        }
        if spec.reservation_offset_kb.is_none() {
            validate_chunk_fence(&chunk, fence)?;
        } else if fence.expected_modify_ts > chunk.modify_ts {
            return Err(LifecycleError::StateConflict);
        }
        let offset = spec.reservation_offset_kb.unwrap_or(chunk.capacity);
        if offset < chunk.capacity {
            return Err(LifecycleError::InvalidRequest(
                "reservation append offset precedes readable capacity".into(),
            ));
        }
        let start_sequence = chunk.next_strip_sequence;
        let next_sequence = start_sequence
            .checked_add(spec.strip_count)
            .ok_or_else(|| LifecycleError::InvalidRequest("chunk strip sequence space exhausted".into()))?;
        drop(guard);
        let resources = self
            .allocate_reservation_resources(chunk_id, start_sequence, spec)
            .await?;
        self.publish_new_group(
            chunk_id,
            group_id,
            fence,
            spec,
            (start_sequence, next_sequence, offset),
            resources,
        )
        .await
    }

    async fn publish_new_group(
        &self,
        chunk_id: &ChunkId,
        group_id: &ChunkId,
        fence: ReservationFence,
        spec: ReserveGroupSpec,
        placement: (u32, u32, u32),
        resources: (Vec<ChunkStrip>, Vec<Segment>, Vec<u32>, ReservationPermit),
    ) -> Result<ReservationMutation, LifecycleError> {
        let (start_sequence, next_sequence, offset) = placement;
        let (mut strips, parity_segments, preferred_survivors, permit) = resources;
        let publication = async {
            let guard = self.acquire_reservation_guard(chunk_id).await?;
            let current = guard.chunk().expect("acquire guarantees a chunk");
            ChunkState::from_proto(current.state).check_can_append()?;
            validate_chunk_identity(current, fence.writer_epoch)?;
            if current.next_strip_sequence != start_sequence || current.capacity > offset {
                return Err(LifecycleError::StateConflict);
            }
            Ok(guard)
        }
        .await;
        let mut guard = match publication {
            Ok(guard) => guard,
            Err(error) => {
                if let Err(cleanup) = self
                    .allocator
                    .rollback_conversion_group(&strips, &parity_segments)
                    .await
                {
                    permit.retain();
                    return Err(cleanup.into());
                }
                return Err(error);
            }
        };
        let mut chunk = guard.chunk().expect("publication acquired a chunk").clone();
        assign_strip_offsets(&mut strips, offset);
        let now_ms = super::super::unix_time_ms();
        let group = StripReservationGroup {
            group_id: Some(*group_id),
            chunk_id: Some(*chunk_id),
            writer_epoch: fence.writer_epoch,
            lease_generation: fence.lease_generation,
            lease_deadline_ms: writer_lease_deadline(now_ms, fence.writer_epoch, fence.lease_ms),
            placement_epoch: now_ms,
            states: vec![StripReservationState::Reserved as i32; strips.len()],
            strips,
            parity_segments,
            preferred_survivors,
            data_num: spec.conversion_data_num,
            code_num: spec.conversion_code_num,
            planned_cursors: vec![0; spec.strip_count as usize],
            planned_closed_sequences: vec![u32::MAX; spec.strip_count as usize],
        };
        chunk.next_strip_sequence = next_sequence;
        chunk.modify_ts = chunk.modify_ts.saturating_add(1);
        chunk.writer_lease_deadline_ms = group.lease_deadline_ms;
        let mutation = self
            .persist_new_reservation(
                chunk_id,
                group_id,
                fence,
                spec,
                PendingReservation { chunk, group, permit },
            )
            .await?;
        guard.refresh(mutation.chunk.clone());
        Ok(mutation)
    }
}
