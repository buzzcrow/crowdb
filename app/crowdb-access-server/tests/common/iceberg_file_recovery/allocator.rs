// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use arc_swap::ArcSwap;
use async_trait::async_trait;
use crowdb_chunk_client::{ChunkAllocator, IoError, Result};
use crowdb_protocol::chunkdb::rpc::*;
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

#[derive(Clone, Default)]
pub struct TestState {
    chunks: HashMap<(u64, u64), Chunk>,
    reservations: HashMap<(u64, u64), StripReservationGroup>,
}

#[derive(Default)]
pub struct TestAllocator {
    next_chunk: AtomicU64,
    state: ArcSwap<TestState>,
}

impl TestAllocator {
    fn update<T>(&self, action: impl Fn(&mut TestState) -> Result<T>) -> Result<T> {
        loop {
            let current = self.state.load_full();
            let mut next = (*current).clone();
            let result = action(&mut next)?;
            let previous = self.state.compare_and_swap(&current, Arc::new(next));
            if Arc::ptr_eq(&previous, &current) {
                return Ok(result);
            }
        }
    }
}

#[async_trait]
impl ChunkAllocator for TestAllocator {
    async fn allocate_chunk(&self, req: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        let low = self.next_chunk.fetch_add(1, Ordering::Relaxed) + 1;
        let chunk_id = req.chunk_id.unwrap_or(ChunkId {
            high: u64::try_from(req.chunk_type).unwrap() << 56,
            low,
        });
        let strips: Vec<_> = (0..req.strip_count.max(1))
            .map(|sequence| make_strip(chunk_id, sequence, req.copy_count.max(1)))
            .collect();
        let chunk = Chunk {
            id: Some(chunk_id),
            modify_ts: 1,
            state: ChunkState::Active as i32,
            create_ts_ms: 0,
            sealed_ts_ms: 0,
            capacity: strips.iter().map(|strip| strip.capacity).sum(),
            sealed_length: 0,
            strips,
            chunk_type: req.chunk_type,
            writer_epoch: req.writer_epoch,
            acknowledged_cursor: 0,
            closed_strip_sequence: None,
            writer_lease_deadline_ms: req.writer_lease_ms,
            next_strip_sequence: req.strip_count.max(1),
            cleanup_intents: vec![],
            last_strip_replacement: None,
            owner_key: req.owner_key.clone(),
        };
        self.update(|state| {
            state.chunks.insert((chunk_id.high, chunk_id.low), chunk.clone());
            Ok(AllocateChunkResponse {
                chunk: Some(chunk.clone()),
            })
        })
    }

    async fn append_chunk(&self, req: AppendChunkRequest) -> Result<AppendChunkResponse> {
        let id = req.chunk_id.unwrap();
        self.update(|state| {
            let chunk = state.chunks.get_mut(&(id.high, id.low)).unwrap();
            if chunk.modify_ts != req.modify_ts {
                return Ok(AppendChunkResponse {
                    modify_ts: chunk.modify_ts,
                    strips: Vec::new(),
                    chunk: Some(chunk.clone()),
                });
            }
            let sequence = u32::try_from(chunk.strips.len()).unwrap();
            let strips: Vec<_> = (0..req.strip_count.max(1))
                .map(|offset| make_strip(id, sequence + offset, req.copy_count.max(1)))
                .collect();
            chunk.capacity += strips.iter().map(|strip| strip.capacity).sum::<u32>();
            chunk.modify_ts += 1;
            chunk.strips.extend(strips.iter().cloned());
            chunk.next_strip_sequence = sequence + req.strip_count.max(1);
            Ok(AppendChunkResponse {
                modify_ts: chunk.modify_ts,
                strips,
                chunk: None,
            })
        })
    }

    async fn reserve_strip_group(&self, req: ReserveStripGroupRequest) -> Result<ReserveStripGroupResponse> {
        let chunk_id = req.chunk_id.unwrap();
        let group_id = req.group_id.unwrap();
        self.update(|state| {
            if let Some(group) = state.reservations.get(&(group_id.high, group_id.low)) {
                return Ok(ReserveStripGroupResponse {
                    chunk: state.chunks.get(&(chunk_id.high, chunk_id.low)).cloned(),
                    group: Some(group.clone()),
                });
            }
            let chunk = state.chunks.get_mut(&(chunk_id.high, chunk_id.low)).unwrap();
            if req.reservation_offset_kb.is_none() && chunk.modify_ts != req.expected_modify_ts {
                return Err(IoError::MetadataConflict("stale reservation revision".into()));
            }
            let start_sequence = chunk.next_strip_sequence;
            let mut strips: Vec<_> = (0..req.strip_count)
                .map(|offset| make_strip(chunk_id, start_sequence + offset, req.copy_count.max(1)))
                .collect();
            let mut chunk_offset = req.reservation_offset_kb.unwrap_or(chunk.capacity);
            for strip in &mut strips {
                strip.chunk_offset = chunk_offset;
                chunk_offset += strip.capacity;
            }
            chunk.next_strip_sequence += req.strip_count;
            chunk.modify_ts += 1;
            let chunk = chunk.clone();
            let group = StripReservationGroup {
                group_id: Some(group_id),
                chunk_id: Some(chunk_id),
                writer_epoch: req.writer_epoch,
                lease_generation: req.lease_generation,
                lease_deadline_ms: req.lease_ms,
                placement_epoch: 1,
                states: vec![StripReservationState::Reserved as i32; strips.len()],
                strips,
                parity_segments: vec![],
                preferred_survivors: vec![],
                data_num: 0,
                code_num: 0,
                planned_cursors: vec![0; req.strip_count as usize],
                planned_closed_sequences: vec![u32::MAX; req.strip_count as usize],
            };
            state
                .reservations
                .insert((group_id.high, group_id.low), group.clone());
            Ok(ReserveStripGroupResponse {
                chunk: Some(chunk),
                group: Some(group),
            })
        })
    }

    async fn mutate_strip_reservation(
        &self,
        req: MutateStripReservationRequest,
    ) -> Result<MutateStripReservationResponse> {
        let chunk_id = req.chunk_id.unwrap();
        let group_id = req.group_id.unwrap();
        self.update(|state| {
            let mut group = state
                .reservations
                .remove(&(group_id.high, group_id.low))
                .ok_or_else(|| IoError::MetadataConflict("reservation missing".into()))?;
            let index = group
                .strips
                .iter()
                .position(|strip| strip.strip_sequence == req.strip_sequence)
                .ok_or_else(|| IoError::MetadataConflict("reservation strip missing".into()))?;
            let action = StripReservationAction::try_from(req.action).unwrap();
            let current = StripReservationState::try_from(group.states[index]).unwrap();
            let chunk = state.chunks.get_mut(&(chunk_id.high, chunk_id.low)).unwrap();
            match action {
                StripReservationAction::Consume if current == StripReservationState::Reserved => {
                    group.states[index] = StripReservationState::Consumed as i32;
                }
                StripReservationAction::Confirm if current == StripReservationState::Consumed => {
                    chunk.strips.push(group.strips[index].clone());
                    chunk.capacity += group.strips[index].capacity;
                    chunk.modify_ts += 1;
                    chunk.acknowledged_cursor = req.acknowledged_cursor;
                    chunk.closed_strip_sequence = req.closed_strip_sequence;
                    group.states[index] = StripReservationState::Confirmed as i32;
                }
                StripReservationAction::Cancel if current == StripReservationState::Reserved => {
                    group.states[index] = StripReservationState::Cancelled as i32;
                }
                StripReservationAction::Consume if current == StripReservationState::Consumed => {}
                StripReservationAction::Confirm if current == StripReservationState::Confirmed => {}
                StripReservationAction::Cancel if current == StripReservationState::Cancelled => {}
                _ => return Err(IoError::MetadataConflict("invalid reservation transition".into())),
            }
            let chunk = chunk.clone();
            state
                .reservations
                .insert((group_id.high, group_id.low), group.clone());
            Ok(MutateStripReservationResponse {
                chunk: Some(chunk),
                group: Some(group),
            })
        })
    }

    async fn advance_chunk_write(&self, req: AdvanceChunkWriteRequest) -> Result<AdvanceChunkWriteResponse> {
        let id = req.chunk_id.unwrap();
        self.update(|state| {
            let chunk = state.chunks.get_mut(&(id.high, id.low)).unwrap();
            if chunk.writer_epoch != req.writer_epoch || chunk.modify_ts != req.expected_modify_ts {
                return Err(IoError::AllocationFailed("stale writer".into()));
            }
            chunk.modify_ts += 1;
            chunk.acknowledged_cursor = req.acknowledged_cursor;
            chunk.closed_strip_sequence = req.closed_strip_sequence;
            Ok(AdvanceChunkWriteResponse {
                chunk: Some(chunk.clone()),
            })
        })
    }

    async fn seal_chunk(&self, req: SealChunkRequest) -> Result<SealChunkResponse> {
        let id = req.chunk_id.unwrap();
        self.update(|state| {
            let chunk = state.chunks.get_mut(&(id.high, id.low)).unwrap();
            chunk.state = ChunkState::Sealed as i32;
            chunk.sealed_length = u32::try_from(req.seal_bytes.div_ceil(1024)).unwrap();
            chunk.acknowledged_cursor = req.seal_bytes;
            Ok(SealChunkResponse {
                chunk: Some(chunk.clone()),
            })
        })
    }

    async fn delete_chunk(&self, req: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        let id = req.chunk_id.unwrap();
        self.update(|state| {
            let chunk = state.chunks.get_mut(&(id.high, id.low)).unwrap();
            chunk.state = ChunkState::Deleted as i32;
            Ok(DeleteChunkResponse {
                chunk: Some(chunk.clone()),
            })
        })
    }

    async fn query_chunk(&self, req: QueryChunkRequest) -> Result<QueryChunkResponse> {
        let id = req.chunk_id.unwrap();
        let state = self.state.load();
        Ok(QueryChunkResponse {
            chunk: state.chunks.get(&(id.high, id.low)).cloned(),
            layout_validity_ms: 1_000,
        })
    }
}

fn make_strip(chunk_id: ChunkId, sequence: u32, copies: u32) -> ChunkStrip {
    let segments = (0..copies)
        .map(|copy| Segment {
            disk_id: Some(DiskId {
                high: u64::from(copy) + 1,
                low: 0,
            }),
            owner_chunk: Some(chunk_id),
            unit_offset: chunk_id.low * 4096 + u64::from(sequence) * 256,
            zone_index: 0,
            unit_count: 256,
            allocation_ts: 0,
        })
        .collect();
    ChunkStrip {
        chunk_offset: sequence * 1024,
        strip_sequence: sequence,
        unit_kb: 4,
        capacity: 1024,
        create_ts_ms: 0,
        sealed_ts_ms: 0,
        sealed_length: 0,
        strip_type: StripType::Mirror as i32,
        strip: Some(Strip::MirrorStrip(MirrorStrip { segments })),
        usage_bitmap: Vec::new(),
        unavailable_segments: Vec::new(),
        placement_priority: 0,
        placement_assessment: None,
        placement_repair_required: false,
    }
}
