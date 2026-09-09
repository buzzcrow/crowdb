// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fenced strip reservation lifecycle under the owning chunk guard.

use std::collections::{HashMap, HashSet};

use crowdb_protocol::chunkdb::rpc::{
    Chunk, ChunkStrip, EcState, EcStrip, Strip, StripCleanupIntent, StripReservationAction,
    StripReservationGroup, StripReservationState, StripType,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::Segment;
use tracing::warn;

use crate::allocator::{StripAllocType, StripBatchSpec};

use super::{
    assign_strip_offsets, writer_lease_deadline, CacheHint, ChunkState, LifecycleError, LifecycleHandler,
    LockPolicy,
};

#[derive(Debug, Clone)]
pub struct ReservationMutation {
    pub chunk: Chunk,
    pub group: Option<StripReservationGroup>,
}

#[derive(Debug, Clone, Copy)]
pub struct ReserveGroupSpec {
    pub strip_size: u32,
    pub strip_count: u32,
    pub copy_count: u32,
    pub conversion_data_num: u32,
    pub conversion_code_num: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct ReservationFence {
    pub expected_modify_ts: u64,
    pub writer_epoch: u64,
    pub lease_generation: u64,
    pub lease_ms: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct ReservationUpdate {
    pub strip_sequence: u32,
    pub action: StripReservationAction,
    pub acknowledged_cursor: u64,
    pub closed_strip_sequence: Option<u32>,
}

impl LifecycleHandler {
    pub(super) async fn reclaim_unconsumed_reservations(
        &self,
        chunk_id: &ChunkId,
    ) -> Result<u64, LifecycleError> {
        let groups = self.store.list_reservation_groups(chunk_id).await?;
        let mut reclaimed = 0_u64;
        for mut group in groups {
            validate_group_shape(&group)?;
            let mut rollback = Vec::new();
            for (index, state) in group.states.iter_mut().enumerate() {
                if *state == StripReservationState::Reserved as i32 {
                    *state = StripReservationState::Cancelled as i32;
                    rollback.push(group.strips[index].clone());
                } else if *state == StripReservationState::Cancelled as i32 {
                    rollback.push(group.strips[index].clone());
                }
            }
            if !rollback.is_empty() {
                self.store.put_reservation_group(&group).await?;
                self.allocator.rollback_strips(&rollback).await?;
                reclaimed = reclaimed.saturating_add(rollback.len() as u64);
            }
            let has_consumed = group.states.contains(&(StripReservationState::Consumed as i32));
            if !has_consumed
                && group.states.iter().all(|state| {
                    *state == StripReservationState::Confirmed as i32
                        || *state == StripReservationState::Cancelled as i32
                })
            {
                if !group.parity_segments.is_empty() {
                    self.allocator
                        .pool()
                        .free_blocks(group.parity_segments.clone())
                        .await
                        .map_err(LifecycleError::Cleanup)?;
                }
                if let Some(group_id) = group.group_id {
                    self.store.delete_reservation_group(chunk_id, &group_id).await?;
                }
            }
        }
        Ok(reclaimed)
    }

    pub async fn reserve_strip_group(
        &self,
        chunk_id: &ChunkId,
        group_id: &ChunkId,
        fence: ReservationFence,
        spec: ReserveGroupSpec,
    ) -> Result<ReservationMutation, LifecycleError> {
        self.check_range(chunk_id)?;
        validate_reserve_spec(fence, spec)?;
        let mut guard = self.acquire_reservation_guard(chunk_id).await?;
        let mut chunk = guard
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
        validate_chunk_fence(&chunk, fence)?;
        let snap = self.topology.snapshot();
        let unit_count = spec.strip_size;
        let start_sequence = chunk.next_strip_sequence;
        let (mut strips, parity_segments, preferred_survivors) =
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
                        unit_count,
                        start_sequence,
                        spec.conversion_data_num as usize,
                        spec.conversion_code_num as usize,
                        spec.copy_count as usize,
                        &self.placement_constraints(),
                    )
                    .await?;
                (
                    allocation.mirrors,
                    allocation.parity_segments,
                    allocation.preferred_survivors,
                )
            } else {
                (
                    self.allocator
                        .allocate_strips(
                            &snap,
                            chunk_id,
                            StripBatchSpec {
                                strip_type: StripAllocType::Mirror {
                                    copy_count: spec.copy_count as usize,
                                },
                                unit_count,
                                start_sequence,
                                strip_count: spec.strip_count,
                            },
                            &self.placement_constraints(),
                        )
                        .await?,
                    Vec::new(),
                    Vec::new(),
                )
            };
        assign_strip_offsets(&mut strips, chunk.capacity);
        let now_ms = super::unix_time_ms();
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
        };
        chunk.next_strip_sequence = start_sequence
            .checked_add(spec.strip_count)
            .ok_or_else(|| LifecycleError::InvalidRequest("chunk strip sequence space exhausted".into()))?;
        chunk.modify_ts = chunk.modify_ts.saturating_add(1);
        chunk.writer_lease_deadline_ms = group.lease_deadline_ms;
        let mutation = self
            .persist_new_reservation(chunk_id, group_id, fence, spec, chunk, group)
            .await?;
        guard.refresh(mutation.chunk.clone());
        Ok(mutation)
    }

    #[allow(clippy::too_many_lines)]
    pub async fn mutate_strip_reservation(
        &self,
        chunk_id: &ChunkId,
        group_id: &ChunkId,
        fence: ReservationFence,
        update: ReservationUpdate,
    ) -> Result<ReservationMutation, LifecycleError> {
        self.check_range(chunk_id)?;
        let mut guard = self.acquire_reservation_guard(chunk_id).await?;
        let mut chunk = guard
            .chunk()
            .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
            .clone();
        ChunkState::from_proto(chunk.state).check_can_append()?;
        validate_chunk_identity(&chunk, fence.writer_epoch)?;
        if update.action == StripReservationAction::Publish && chunk.last_strip_replacement == Some(*group_id)
        {
            return Ok(ReservationMutation { chunk, group: None });
        }
        let mut group = self
            .store
            .get_reservation_group(chunk_id, group_id)
            .await?
            .ok_or(LifecycleError::StateConflict)?;
        validate_group_fence(&group, fence)?;
        validate_group_shape(&group)?;
        let index = group
            .strips
            .iter()
            .position(|strip| strip.strip_sequence == update.strip_sequence)
            .ok_or(LifecycleError::StripIndexOutOfRange {
                index: update.strip_sequence,
                len: group.strips.len(),
            })?;
        let state = StripReservationState::try_from(group.states[index])
            .map_err(|()| LifecycleError::InvalidRequest("invalid reservation state".into()))?;
        if super::unix_time_ms() > group.lease_deadline_ms
            && state == StripReservationState::Reserved
            && update.action != StripReservationAction::Cancel
        {
            return Err(LifecycleError::StateConflict);
        }
        match update.action {
            StripReservationAction::Consume => {
                if state == StripReservationState::Reserved {
                    group.states[index] = StripReservationState::Consumed as i32;
                    self.store.put_reservation_group(&group).await?;
                } else if state != StripReservationState::Consumed {
                    return Err(LifecycleError::StateConflict);
                }
            }
            StripReservationAction::Confirm => {
                if state == StripReservationState::Consumed {
                    if chunk.modify_ts != fence.expected_modify_ts {
                        return Err(LifecycleError::StateConflict);
                    }
                    Self::confirm_reserved_strip(&mut chunk, &mut group, index, update)?;
                    self.store
                        .put_chunk_and_finish_reservation(&chunk, Some(&group), group_id)
                        .await?;
                    self.commit_strip_segments_background(vec![group.strips[index].clone()]);
                    guard.refresh(chunk.clone());
                    return Ok(ReservationMutation {
                        chunk,
                        group: Some(group),
                    });
                }
                if state != StripReservationState::Confirmed
                    || !chunk
                        .strips
                        .iter()
                        .any(|strip| strip.strip_sequence == update.strip_sequence)
                {
                    return Err(LifecycleError::StateConflict);
                }
            }
            StripReservationAction::Cancel => {
                if state == StripReservationState::Reserved {
                    group.states[index] = StripReservationState::Cancelled as i32;
                    self.store.put_reservation_group(&group).await?;
                    self.allocator
                        .rollback_strips(&[group.strips[index].clone()])
                        .await?;
                    return Ok(ReservationMutation {
                        chunk,
                        group: Some(group),
                    });
                }
                if state == StripReservationState::Cancelled {
                    // The persisted terminal state is also the cleanup intent.
                    // Repeating the generation-fenced free closes a crash
                    // between intent persistence and block reclamation.
                    self.allocator
                        .rollback_strips(&[group.strips[index].clone()])
                        .await?;
                } else {
                    return Err(LifecycleError::StateConflict);
                }
            }
            StripReservationAction::Renew => {
                if state != StripReservationState::Reserved && state != StripReservationState::Consumed {
                    return Err(LifecycleError::StateConflict);
                }
                group.lease_generation = group.lease_generation.saturating_add(1);
                group.lease_deadline_ms =
                    writer_lease_deadline(super::unix_time_ms(), fence.writer_epoch, fence.lease_ms);
                self.store.put_reservation_group(&group).await?;
            }
            StripReservationAction::Publish => {
                if chunk.modify_ts != fence.expected_modify_ts {
                    return Err(LifecycleError::StateConflict);
                }
                let published = self.publish_conversion_group(&mut chunk, &group).await?;
                self.store
                    .put_chunk_and_finish_reservation(&published, None, group_id)
                    .await?;
                guard.refresh(published.clone());
                return Ok(ReservationMutation {
                    chunk: published,
                    group: None,
                });
            }
        }
        Ok(ReservationMutation {
            chunk,
            group: Some(group),
        })
    }

    async fn publish_conversion_group(
        &self,
        chunk: &mut Chunk,
        group: &StripReservationGroup,
    ) -> Result<Chunk, LifecycleError> {
        if group.data_num == 0
            || group.code_num == 0
            || group.strips.len() != group.data_num as usize
            || group.parity_segments.len() != group.code_num as usize
            || !group
                .states
                .iter()
                .all(|state| *state == StripReservationState::Confirmed as i32)
        {
            return Err(LifecycleError::StateConflict);
        }
        let first_sequence = group.strips[0].strip_sequence;
        let start = chunk
            .strips
            .iter()
            .position(|strip| strip.strip_sequence == first_sequence)
            .ok_or(LifecycleError::StateConflict)?;
        let end = start.saturating_add(group.strips.len());
        if chunk.strips.get(start..end).map_or(true, |current| {
            current
                .iter()
                .zip(&group.strips)
                .any(|(current, reserved)| !same_reserved_layout(current, reserved))
        }) {
            return Err(LifecycleError::StateConflict);
        }
        let range_end = u64::from(
            group
                .strips
                .last()
                .map_or(0, |strip| strip.chunk_offset.saturating_add(strip.capacity)),
        ) * 1024;
        if chunk.acknowledged_cursor < range_end
            || chunk.closed_strip_sequence < group.strips.last().map(|strip| strip.strip_sequence)
        {
            return Err(LifecycleError::StateConflict);
        }
        let selected = select_conversion_survivors(
            &self.topology.snapshot(),
            self.allocator.pool(),
            group,
            self.allow_unsafe_ec,
        )?;
        let mut segments = selected.clone();
        segments.extend_from_slice(&group.parity_segments);
        self.allocator
            .pool()
            .commit_blocks(group.parity_segments.clone())
            .await
            .map_err(LifecycleError::Commit)?;
        let first = &group.strips[0];
        let replacement = ChunkStrip {
            chunk_offset: first.chunk_offset,
            strip_sequence: first.strip_sequence,
            unit_kb: first.unit_kb,
            capacity: group.strips.iter().map(|strip| strip.capacity).sum(),
            create_ts_ms: super::unix_time_ms(),
            sealed_ts_ms: 0,
            sealed_length: 0,
            strip_type: StripType::Ec as i32,
            strip: Some(Strip::EcStrip(EcStrip {
                data_num: group.data_num,
                code_num: group.code_num,
                ec_state: EcState::Parity as i32,
                segments,
            })),
            usage_bitmap: Vec::new(),
            unavailable_segments: Vec::new(),
        };
        let selected_set: HashSet<_> = selected.into_iter().collect();
        let retired_segments = group
            .strips
            .iter()
            .flat_map(mirror_segments)
            .filter(|segment| !selected_set.contains(segment))
            .collect::<Vec<_>>();
        chunk.strips.splice(start..end, [replacement]);
        chunk.capacity = chunk.strips.iter().map(|strip| strip.capacity).sum();
        chunk.modify_ts = chunk.modify_ts.saturating_add(1);
        chunk.last_strip_replacement = group.group_id;
        if !retired_segments.is_empty() {
            chunk.cleanup_intents.push(StripCleanupIntent {
                operation_id: group.group_id,
                retired_segments,
                not_before_ms: super::unix_time_ms().saturating_add(self.layout_validity_ms),
            });
        }
        Ok(chunk.clone())
    }

    async fn acquire_reservation_guard(
        &self,
        chunk_id: &ChunkId,
    ) -> Result<super::ChunkGuard, LifecycleError> {
        let locks = self.locks.as_ref().ok_or_else(|| {
            LifecycleError::InvalidRequest("strip reservations require lifecycle locking".into())
        })?;
        locks
            .acquire(chunk_id, &self.store, &LockPolicy::default(), CacheHint::Cache)
            .await
    }

    async fn persist_new_reservation(
        &self,
        chunk_id: &ChunkId,
        group_id: &ChunkId,
        fence: ReservationFence,
        spec: ReserveGroupSpec,
        chunk: Chunk,
        group: StripReservationGroup,
    ) -> Result<ReservationMutation, LifecycleError> {
        if let Err(error) = self.store.put_chunk_and_reservation(&chunk, &group).await {
            // A failed response can be ambiguous after KV commit. Never make
            // those extents reusable until a linearizable read proves the
            // reservation record is absent.
            match self.store.get_reservation_group(chunk_id, group_id).await {
                Ok(Some(existing)) => {
                    validate_existing_group(&existing, chunk_id, group_id, fence, spec)?;
                    return Ok(ReservationMutation {
                        chunk: self.store.get_chunk(chunk_id).await?,
                        group: Some(existing),
                    });
                }
                Ok(None) => {
                    self.allocator
                        .rollback_conversion_group(&group.strips, &group.parity_segments)
                        .await?;
                }
                Err(read_error) => {
                    warn!(%read_error, "reservation commit outcome remains ambiguous; retaining tentative blocks");
                }
            }
            return Err(error.into());
        }
        Ok(ReservationMutation {
            chunk,
            group: Some(group),
        })
    }

    fn confirm_reserved_strip(
        chunk: &mut Chunk,
        group: &mut StripReservationGroup,
        index: usize,
        update: ReservationUpdate,
    ) -> Result<(), LifecycleError> {
        let strip = &group.strips[index];
        if strip.chunk_offset != chunk.capacity {
            return Err(LifecycleError::StateConflict);
        }
        let new_capacity = chunk.capacity.saturating_add(strip.capacity);
        let capacity_bytes = u64::from(new_capacity).saturating_mul(1024);
        if update.acknowledged_cursor <= chunk.acknowledged_cursor
            || update.acknowledged_cursor > capacity_bytes
        {
            return Err(LifecycleError::InvalidRequest(
                "reservation confirmation cursor is outside the new attached capacity".into(),
            ));
        }
        chunk.strips.push(strip.clone());
        chunk.capacity = new_capacity;
        chunk.acknowledged_cursor = update.acknowledged_cursor;
        chunk.closed_strip_sequence = update.closed_strip_sequence;
        chunk.modify_ts = chunk.modify_ts.saturating_add(1);
        group.states[index] = StripReservationState::Confirmed as i32;
        Ok(())
    }
}

fn validate_reserve_spec(fence: ReservationFence, spec: ReserveGroupSpec) -> Result<(), LifecycleError> {
    if fence.writer_epoch == 0
        || fence.lease_generation == 0
        || fence.lease_ms == 0
        || spec.strip_size == 0
        || spec.strip_count == 0
        || spec.copy_count == 0
    {
        return Err(LifecycleError::InvalidRequest(
            "reservation fence and geometry must be nonzero".into(),
        ));
    }
    Ok(())
}

fn validate_chunk_fence(chunk: &Chunk, fence: ReservationFence) -> Result<(), LifecycleError> {
    validate_chunk_identity(chunk, fence.writer_epoch)?;
    if chunk.modify_ts != fence.expected_modify_ts {
        return Err(LifecycleError::StateConflict);
    }
    Ok(())
}

fn validate_chunk_identity(chunk: &Chunk, writer_epoch: u64) -> Result<(), LifecycleError> {
    if chunk.writer_epoch != writer_epoch {
        return Err(LifecycleError::StateConflict);
    }
    Ok(())
}

fn validate_group_fence(
    group: &StripReservationGroup,
    fence: ReservationFence,
) -> Result<(), LifecycleError> {
    if group.writer_epoch != fence.writer_epoch || group.lease_generation != fence.lease_generation {
        return Err(LifecycleError::StateConflict);
    }
    Ok(())
}

pub(super) fn validate_group_shape(group: &StripReservationGroup) -> Result<(), LifecycleError> {
    if group.states.len() != group.strips.len() {
        return Err(LifecycleError::InvalidRequest(
            "reservation state and strip counts differ".into(),
        ));
    }
    Ok(())
}

fn validate_existing_group(
    group: &StripReservationGroup,
    chunk_id: &ChunkId,
    group_id: &ChunkId,
    fence: ReservationFence,
    spec: ReserveGroupSpec,
) -> Result<(), LifecycleError> {
    if group.chunk_id != Some(*chunk_id)
        || group.group_id != Some(*group_id)
        || group.writer_epoch != fence.writer_epoch
        || group.lease_generation != fence.lease_generation
        || group.strips.len() != spec.strip_count as usize
        || group.states.len() != group.strips.len()
        || group.data_num != spec.conversion_data_num
        || group.code_num != spec.conversion_code_num
        || group.strips.iter().any(|strip| {
            strip.capacity != spec.strip_size.saturating_mul(strip.unit_kb)
                || mirror_segments(strip).len() != spec.copy_count as usize
        })
    {
        return Err(LifecycleError::StateConflict);
    }
    Ok(())
}

fn mirror_segments(strip: &ChunkStrip) -> Vec<Segment> {
    match strip.strip.as_ref() {
        Some(Strip::MirrorStrip(mirror)) => mirror.segments.clone(),
        _ => Vec::new(),
    }
}

fn same_reserved_layout(current: &ChunkStrip, reserved: &ChunkStrip) -> bool {
    current.chunk_offset == reserved.chunk_offset
        && current.strip_sequence == reserved.strip_sequence
        && current.unit_kb == reserved.unit_kb
        && current.capacity == reserved.capacity
        && current.strip_type == reserved.strip_type
        && current.strip == reserved.strip
        && current.unavailable_segments == reserved.unavailable_segments
}

type SurvivorScore = (u32, u32, usize, usize);
type SurvivorSelection = (SurvivorScore, Vec<Segment>);

fn select_conversion_survivors(
    snapshot: &crate::topology::TopologySnapshot,
    pool: &crate::allocator::DiskdbClientPool,
    group: &StripReservationGroup,
    allow_unsafe: bool,
) -> Result<Vec<Segment>, LifecycleError> {
    let healthy_groups = snapshot
        .healthy_disk_groups()
        .into_iter()
        .map(|entry| entry.dg_id)
        .collect::<HashSet<_>>();
    let locate = |segment: &Segment| {
        segment
            .disk_id
            .and_then(|disk| pool.dg_for_disk(&disk))
            .filter(|dg| healthy_groups.contains(dg))
            .and_then(|dg| snapshot.disk_group(dg))
            .map(|entry| (entry.node_id, entry.rack_id))
    };
    let mut node_load = HashMap::new();
    let mut racks = HashSet::new();
    for parity in &group.parity_segments {
        let Some((node, rack)) = locate(parity) else {
            return Err(LifecycleError::StateConflict);
        };
        *node_load.entry(node).or_insert(0_u32) += 1;
        racks.insert(rack);
    }
    let candidates = group
        .strips
        .iter()
        .map(|strip| {
            mirror_segments(strip)
                .into_iter()
                .filter_map(|segment| locate(&segment).map(|location| (segment, location)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    if candidates.iter().any(Vec::is_empty) {
        return Err(LifecycleError::StateConflict);
    }
    let mut best: Option<SurvivorSelection> = None;
    select_survivor_dfs(
        &candidates,
        0,
        if allow_unsafe {
            group.data_num.saturating_add(group.code_num)
        } else {
            group.code_num
        },
        &mut node_load,
        &mut racks,
        &mut Vec::with_capacity(candidates.len()),
        &mut best,
    );
    best.map(|(_, segments)| segments)
        .ok_or(LifecycleError::StateConflict)
}

#[allow(clippy::too_many_arguments)]
fn select_survivor_dfs(
    candidates: &[Vec<(Segment, (u64, u64))>],
    index: usize,
    max_per_node: u32,
    node_load: &mut HashMap<u64, u32>,
    racks: &mut HashSet<u64>,
    selected: &mut Vec<Segment>,
    best: &mut Option<SurvivorSelection>,
) {
    if index == candidates.len() {
        let max_load = node_load.values().copied().max().unwrap_or(0);
        if max_load > max_per_node {
            return;
        }
        let concentration = node_load.values().map(|load| load.saturating_mul(*load)).sum();
        let score = (
            max_load,
            concentration,
            usize::MAX - racks.len(),
            usize::MAX - node_load.len(),
        );
        if best.as_ref().map_or(true, |(current, _)| score < *current) {
            *best = Some((score, selected.clone()));
        }
        return;
    }
    for (segment, (node, rack)) in &candidates[index] {
        let previous = node_load.get(node).copied().unwrap_or(0);
        if previous >= max_per_node {
            continue;
        }
        node_load.insert(*node, previous + 1);
        let inserted_rack = racks.insert(*rack);
        selected.push(*segment);
        select_survivor_dfs(
            candidates,
            index + 1,
            max_per_node,
            node_load,
            racks,
            selected,
            best,
        );
        selected.pop();
        if previous == 0 {
            node_load.remove(node);
        } else {
            node_load.insert(*node, previous);
        }
        if inserted_rack {
            racks.remove(rack);
        }
    }
}
