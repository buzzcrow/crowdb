// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk allocation and owner-aware initial placement.

use super::{
    assign_strip_offsets, info, unix_time_ms, writer_lease_deadline, AllocationMetricGuard, CacheHint, Chunk,
    ChunkGuard, ChunkId, ChunkStrip, ChunkType, LifecycleError, LifecycleHandler, LockPolicy,
    ProtoChunkState, ProtoStripType, StoreError, StripAllocType,
};
use crate::allocator::StripBatchSpec;
use crowdb_protocol::{chunk_stream::chunk_owner_key_matches_type, generate_chunk_id};

impl LifecycleHandler {
    /// Allocate a new chunk.
    #[allow(clippy::too_many_arguments)]
    pub async fn allocate_chunk(
        &self,
        chunk_id: Option<ChunkId>,
        write_granularity_kb: u32,
        strip_count: u32,
        strip_type: ProtoStripType,
        data_num: u32,
        code_num: u32,
        copy_count: u32,
        chunk_type: ChunkType,
        writer_epoch: u64,
        writer_lease_ms: u64,
    ) -> Result<Chunk, LifecycleError> {
        self.allocate_chunk_owned(
            chunk_id,
            write_granularity_kb,
            strip_count,
            strip_type,
            data_num,
            code_num,
            copy_count,
            chunk_type,
            writer_epoch,
            writer_lease_ms,
            Vec::new(),
        )
        .await
    }

    /// Allocate a new chunk with a stable logical owner identity.
    #[allow(clippy::too_many_arguments)]
    pub async fn allocate_chunk_owned(
        &self,
        chunk_id: Option<ChunkId>,
        write_granularity_kb: u32,
        strip_count: u32,
        strip_type: ProtoStripType,
        data_num: u32,
        code_num: u32,
        copy_count: u32,
        chunk_type: ChunkType,
        writer_epoch: u64,
        writer_lease_ms: u64,
        owner_key: Vec<u8>,
    ) -> Result<Chunk, LifecycleError> {
        self.validate_strip_layout(strip_type, data_num, code_num, copy_count, write_granularity_kb)?;
        if !chunk_owner_key_matches_type(chunk_type, &owner_key) {
            return Err(LifecycleError::InvalidRequest(
                "chunk owner key does not match chunk type".into(),
            ));
        }
        let id = match chunk_id {
            Some(id) => id,
            None => self.generate_owned_chunk_id(chunk_type)?,
        };
        if (id.high >> 56) != u64::from(chunk_type as u8) {
            return Err(LifecycleError::InvalidRequest(
                "chunk id prefix does not match chunk type".into(),
            ));
        }
        self.check_range(&id)?;
        let mut allocation_guard = AllocationMetricGuard::new(self.metrics.clone());

        let mut guard = self.acquire_allocation_guard(&id, chunk_id.is_some()).await?;
        let strips = self
            .allocate_initial_strips(
                &id,
                write_granularity_kb,
                strip_count,
                (strip_type, data_num, code_num, copy_count),
            )
            .await?;
        let now_ms = unix_time_ms();
        if writer_epoch != 0 && writer_lease_ms == 0 {
            self.allocator.rollback_strips(&strips).await?;
            return Err(LifecycleError::InvalidRequest(
                "writer_lease_ms must be nonzero for a shared writer".into(),
            ));
        }

        let chunk = Chunk {
            id: Some(id),
            modify_ts: 1,
            state: ProtoChunkState::Active as i32,
            create_ts_ms: now_ms,
            sealed_ts_ms: 0,
            capacity: strips.iter().map(|s| s.capacity).sum(),
            sealed_length: 0,
            strips,
            chunk_type: chunk_type as i32,
            writer_epoch,
            acknowledged_cursor: 0,
            closed_strip_sequence: None,
            writer_lease_deadline_ms: writer_lease_deadline(now_ms, writer_epoch, writer_lease_ms),
            next_strip_sequence: strip_count,
            cleanup_intents: Vec::new(),
            last_strip_replacement: None,
            owner_key,
        };
        self.create_chunk_with_liveness_task(&chunk).await?;
        self.liveness_renewed_at.insert(id, now_ms);
        self.admit_placement_repairs(&chunk);
        self.commit_strip_segments_background(chunk.strips.clone());

        // Update cache.
        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        } else if chunk_id.is_none() {
            // Auto-generated ID: populate cache directly (no guard).
            if let Some(locks) = &self.locks {
                locks.populate_cache(&id, chunk.clone());
            }
        }
        info!(chunk_id = ?id, strips = strip_count, "chunk allocated");
        allocation_guard.mark_success();
        Ok(chunk)
    }

    fn generate_owned_chunk_id(&self, chunk_type: ChunkType) -> Result<ChunkId, LifecycleError> {
        let Some(range_guard) = &self.range_guard else {
            return Ok(generate_chunk_id(chunk_type as u8).to_proto());
        };
        if !range_guard.is_ready() {
            let candidate = generate_chunk_id(chunk_type as u8).to_proto();
            return self.check_range(&candidate).map(|()| candidate);
        }
        let attempts = 32 * u64::from(crowdb_protocol::chunk_slot::CHUNK_SLOT_COUNT)
            / range_guard.owned_bucket_count().max(1);
        for _ in 0..attempts {
            let candidate = generate_chunk_id(chunk_type as u8).to_proto();
            if range_guard.check(&candidate).is_ok() {
                return Ok(candidate);
            }
        }
        Err(LifecycleError::InvalidRequest(
            "failed to generate a chunk id in the owned range".into(),
        ))
    }

    async fn acquire_allocation_guard(
        &self,
        id: &ChunkId,
        caller_supplied: bool,
    ) -> Result<Option<ChunkGuard>, LifecycleError> {
        let guard = if caller_supplied {
            if let Some(locks) = &self.locks {
                let g = locks
                    .acquire_for_create(id, &LockPolicy::default(), CacheHint::Cache)
                    .await?;
                // Existence check inside the lock.
                match self.store.get_chunk(id).await {
                    Ok(_) => return Err(LifecycleError::ChunkAlreadyExists),
                    Err(StoreError::ChunkNotFound) => {}
                    Err(e) => return Err(LifecycleError::Storage(e)),
                }
                Some(g)
            } else {
                None
            }
        } else {
            None
        };

        Ok(guard)
    }

    async fn allocate_initial_strips(
        &self,
        id: &ChunkId,
        write_granularity_kb: u32,
        strip_count: u32,
        layout: (ProtoStripType, u32, u32, u32),
    ) -> Result<Vec<ChunkStrip>, LifecycleError> {
        let (strip_type, data_num, code_num, copy_count) = layout;
        let snap = self.topology.snapshot();

        let mirror_copies = if copy_count == 0 { 2 } else { copy_count as usize };
        let strip_alloc_type =
            self.protected_degraded_layout(strip_type, &snap)
                .unwrap_or(match strip_type {
                    ProtoStripType::Mirror => StripAllocType::Mirror {
                        copy_count: mirror_copies,
                    },
                    ProtoStripType::Ec => StripAllocType::Ec {
                        data_num: data_num as usize,
                        code_num: code_num as usize,
                    },
                });

        let constraints = self.allocation_constraints(strip_type, &snap);
        // Convert write_granularity (KB) to unit_count using the unit
        // size from the topology snapshot. Fall back to treating KB as
        // units if unit_size_bytes is unavailable (0).
        let unit_size_kb = snap.unit_size_bytes() / 1024;
        let unit_count = write_granularity_kb
            .checked_div(unit_size_kb)
            .unwrap_or(write_granularity_kb)
            .max(1);

        let mut strips = self
            .allocator
            .allocate_strips(
                &snap,
                id,
                StripBatchSpec {
                    strip_type: strip_alloc_type,
                    unit_count,
                    start_sequence: 0,
                    strip_count,
                },
                &constraints,
            )
            .await?;
        assign_strip_offsets(&mut strips, 0);

        Ok(strips)
    }
}
