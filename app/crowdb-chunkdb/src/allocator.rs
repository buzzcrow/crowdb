// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk allocator — orchestrates strip layout → selector → parallel
//! diskdb `AllocateBlocks` with rollback on partial failure.
//!
//! Design §8: parallel allocation via `futures::join_all`; rollback
//! frees successfully-allocated segments on any failure.

pub mod pool;

use std::collections::HashMap;
use std::sync::Arc;

use futures::future::join_all;
use tracing::{info, warn};

use crowdb_protocol::chunkdb::rpc::StripType as ProtoStripType;
use crowdb_protocol::chunkdb::rpc::{ChunkStrip, EcStrip, MirrorStrip};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;

use crate::metrics::ChunkdbMetrics;
use crate::selector::{EcPlacement, MirrorPlacement, PlacementConstraints, PlacementPlan};
use crate::topology::TopologySnapshot;

pub use pool::DiskdbClientPool;

const MAX_ALLOC_RETRIES: usize = 3;

/// Allocator error.
#[derive(Debug, thiserror::Error)]
pub enum AllocError {
    #[error("placement failed: {0}")]
    Placement(#[from] crate::selector::PlacementError),
    #[error("diskdb allocate failed for disk_group {dg_id}: {error}")]
    AllocateFailed { dg_id: u64, error: String },
    #[error("partial allocation: requested {requested}, got {got}")]
    PartialAllocation { requested: u32, got: u32 },
    #[error("invalid diskdb allocation response for disk_group {dg_id}: {reason}")]
    InvalidResponse { dg_id: u64, reason: String },
    #[error("rollback failed: {0}")]
    Rollback(String),
}

/// Strip type for allocation.
#[derive(Debug, Clone, Copy)]
pub enum StripAllocType {
    Mirror { copy_count: usize },
    Ec { data_num: usize, code_num: usize },
}

/// Geometry and ordered identity range for one atomic strip batch.
#[derive(Debug, Clone, Copy)]
pub struct StripBatchSpec {
    pub strip_type: StripAllocType,
    pub unit_count: u32,
    pub start_sequence: u32,
    pub strip_count: u32,
}

pub struct ConversionGroupAllocation {
    pub mirrors: Vec<ChunkStrip>,
    pub parity_segments: Vec<Segment>,
    pub preferred_survivors: Vec<u32>,
}

/// Chunk allocator — orchestrates placement + parallel diskdb calls.
pub struct ChunkAllocator {
    pool: Arc<DiskdbClientPool>,
    metrics: Option<Arc<ChunkdbMetrics>>,
}

impl ChunkAllocator {
    #[allow(clippy::too_many_arguments)]
    pub async fn allocate_conversion_group(
        &self,
        snap: &TopologySnapshot,
        owner_chunk: &ChunkId,
        unit_count: u32,
        start_sequence: u32,
        data_num: usize,
        code_num: usize,
        copy_count: usize,
        constraints: &PlacementConstraints,
    ) -> Result<ConversionGroupAllocation, AllocError> {
        if copy_count < 1 {
            return Err(crate::selector::PlacementError::InvalidShape(
                "conversion mirror copy count must be nonzero".into(),
            )
            .into());
        }
        self.pool.update_disk_id_lookup(&snap.disk_groups());
        let ec_plan = EcPlacement::select(snap, data_num, code_num, constraints)?;
        let mut ec_blocks_by_group = HashMap::<u64, usize>::new();
        for entry in &ec_plan.entries {
            *ec_blocks_by_group.entry(entry.disk_group_id).or_default() += entry.block_count as usize;
        }
        if ec_blocks_by_group.into_iter().any(|(dg_id, block_count)| {
            snap.disk_group(dg_id)
                .map_or(true, |entry| entry.value.disk_ids.len() < block_count)
        }) {
            return Err(crate::selector::PlacementError::InsufficientCapacity.into());
        }
        let mut entries = ec_plan.entries.clone();
        if copy_count > 1 {
            for survivor in ec_plan.entries.iter().take(data_num) {
                let mut mirror_constraints = constraints.clone();
                mirror_constraints.exclude_nodes.push(survivor.node_id);
                let extras = MirrorPlacement::select(snap, copy_count - 1, &mirror_constraints)?;
                entries.extend(extras.entries);
            }
        }
        let plan = PlacementPlan {
            entries,
            safe_mode: ec_plan.safe_mode,
        };
        let mut blocks_by_group = HashMap::<u64, usize>::new();
        for entry in &plan.entries {
            *blocks_by_group.entry(entry.disk_group_id).or_default() += 1;
        }
        let requires_reuse = blocks_by_group.into_iter().any(|(dg_id, block_count)| {
            snap.disk_group(dg_id)
                .map_or(true, |entry| entry.value.disk_ids.len() < block_count)
        });
        let segments = if requires_reuse {
            self.allocate_blocks_in_plan_order_reusing_disks(owner_chunk, &plan, unit_count)
                .await?
        } else {
            self.allocate_blocks_in_plan_order(owner_chunk, &plan, unit_count)
                .await?
        };
        let mut mirrors = Vec::with_capacity(data_num);
        let extras_start = data_num + code_num;
        for index in 0..data_num {
            let mut mirror_segments = Vec::with_capacity(copy_count);
            mirror_segments.push(segments[index]);
            let start = extras_start + index * (copy_count - 1);
            mirror_segments.extend_from_slice(&segments[start..start + copy_count - 1]);
            mirrors.push(assemble_strip(
                &mirror_segments,
                StripAllocType::Mirror { copy_count },
                start_sequence.saturating_add(u32::try_from(index).unwrap_or(u32::MAX)),
                unit_count,
                (snap.unit_size_bytes() / 1024).max(1),
            ));
        }
        Ok(ConversionGroupAllocation {
            mirrors,
            parity_segments: segments[data_num..data_num + code_num].to_vec(),
            preferred_survivors: vec![0; data_num],
        })
    }

    #[must_use]
    pub fn new(pool: Arc<DiskdbClientPool>) -> Self {
        Self { pool, metrics: None }
    }

    /// Attach allocation workflow metrics.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<ChunkdbMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Get a reference to the diskdb client pool.
    pub fn pool(&self) -> &DiskdbClientPool {
        &self.pool
    }

    /// Allocate a single strip.
    ///
    /// # Errors
    /// Returns `AllocError` on placement failure, diskdb RPC failure,
    /// or partial allocation (triggers rollback).
    pub async fn allocate_strip(
        &self,
        snap: &TopologySnapshot,
        owner_chunk: &ChunkId,
        strip_type: StripAllocType,
        unit_count: u32,
        strip_sequence: u32,
        constraints: &PlacementConstraints,
    ) -> Result<ChunkStrip, AllocError> {
        self.pool.update_disk_id_lookup(&snap.disk_groups());
        let plan = match strip_type {
            StripAllocType::Mirror { copy_count } => MirrorPlacement::select(snap, copy_count, constraints)?,
            StripAllocType::Ec { data_num, code_num } => {
                EcPlacement::select(snap, data_num, code_num, constraints)?
            }
        };
        if let Some(metrics) = &self.metrics {
            metrics
                .allocate_diskdb_calls
                .inc_by(u64::try_from(plan.entries.len()).unwrap_or(u64::MAX));
        }

        let segments = self
            .allocate_blocks_parallel(owner_chunk, &plan, unit_count)
            .await?;
        if let Some(metrics) = &self.metrics {
            metrics
                .allocate_blocks
                .inc_by(u64::try_from(segments.len()).unwrap_or(u64::MAX));
            metrics.allocate_strips.inc();
        }

        let strip = assemble_strip(
            &segments,
            strip_type,
            strip_sequence,
            unit_count,
            (snap.unit_size_bytes() / 1024).max(1),
        );
        Ok(strip)
    }

    /// Allocate independent strips concurrently from one topology snapshot.
    /// Results retain strip-sequence order. Any member failure rolls back every
    /// successful member before returning the first allocation error.
    pub async fn allocate_strips(
        &self,
        snap: &TopologySnapshot,
        owner_chunk: &ChunkId,
        spec: StripBatchSpec,
        constraints: &PlacementConstraints,
    ) -> Result<Vec<ChunkStrip>, AllocError> {
        let allocations = (0..spec.strip_count).map(|index| {
            self.allocate_strip(
                snap,
                owner_chunk,
                spec.strip_type,
                spec.unit_count,
                spec.start_sequence.saturating_add(index),
                constraints,
            )
        });
        let results = join_all(allocations).await;
        let mut strips = Vec::with_capacity(spec.strip_count as usize);
        let mut first_error = None;
        for result in results {
            match result {
                Ok(strip) => strips.push(strip),
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
        }
        if let Some(error) = first_error {
            self.rollback_strips(&strips).await?;
            return Err(error);
        }
        Ok(strips)
    }

    pub async fn allocate_replacement_segment(
        &self,
        snap: &TopologySnapshot,
        owner_chunk: &ChunkId,
        unit_count: u32,
        constraints: &PlacementConstraints,
        exclude_disk_ids: Vec<DiskId>,
    ) -> Result<Segment, AllocError> {
        self.pool.update_disk_id_lookup(&snap.disk_groups());
        let mut constraints = constraints.clone();
        constraints.exclude_disk_groups.extend(
            snap.disk_groups()
                .into_iter()
                .filter(|disk_group| {
                    disk_group.value.disk_ids.is_empty()
                        || disk_group
                            .value
                            .disk_ids
                            .iter()
                            .all(|disk_id| exclude_disk_ids.contains(disk_id))
                })
                .map(|disk_group| disk_group.dg_id),
        );
        let plan = MirrorPlacement::select(snap, 1, &constraints)?;
        let entry = plan
            .entries
            .first()
            .ok_or(crate::selector::PlacementError::NoHealthyDiskGroups)?;
        let response = self
            .pool
            .allocate_blocks_excluding(entry.disk_group_id, 1, unit_count, owner_chunk, exclude_disk_ids)
            .await
            .map_err(|error| AllocError::AllocateFailed {
                dg_id: entry.disk_group_id,
                error: error.to_string(),
            })?;
        if response.segments.len() != 1 {
            self.rollback_or_error(
                &response.segments,
                AllocError::PartialAllocation {
                    requested: 1,
                    got: u32::try_from(response.segments.len()).unwrap_or(u32::MAX),
                },
            )
            .await?;
        }
        let segment = response.segments[0];
        if let Some(reason) = self.validate_segment(&segment, entry.disk_group_id, owner_chunk, unit_count) {
            self.rollback_or_error(
                &response.segments,
                AllocError::InvalidResponse {
                    dg_id: entry.disk_group_id,
                    reason,
                },
            )
            .await?;
        }
        Ok(segment)
    }

    /// Allocate blocks in parallel across all placement entries.
    ///
    /// Per-instance verification: each diskdb response is checked for
    /// the requested segment count. Partial responses trigger a retry
    /// for just the missing blocks (up to `MAX_ALLOC_RETRIES`). On
    /// final failure, all successfully-allocated segments are freed.
    async fn allocate_blocks_parallel(
        &self,
        owner_chunk: &ChunkId,
        plan: &PlacementPlan,
        unit_count: u32,
    ) -> Result<Vec<Segment>, AllocError> {
        let mut all_segments: Vec<Segment> = Vec::new();
        // One request per DiskDB/data group. The selector still chooses each
        // EC position independently; aggregation only combines the counts for
        // positions placed on the same node.
        let mut pending = grouped_requests(plan);

        for attempt in 0..=MAX_ALLOC_RETRIES {
            if pending.is_empty() {
                break;
            }

            let mut futures = Vec::new();
            for (dg_id, count) in &pending {
                let pool = Arc::clone(&self.pool);
                let owner = *owner_chunk;
                let dg = *dg_id;
                let cnt = *count;
                futures.push(async move {
                    pool.allocate_blocks(dg, cnt, unit_count, &owner)
                        .await
                        .map_err(|e| AllocError::AllocateFailed {
                            dg_id: dg,
                            error: e.to_string(),
                        })
                });
            }

            let results = join_all(futures).await;

            // Check for hard failures and per-instance count mismatches.
            let mut errors = Vec::new();
            let mut next_pending: Vec<(u64, u32)> = Vec::new();
            for (result, (dg_id, requested)) in results.into_iter().zip(&pending) {
                match result {
                    Ok(resp) => {
                        let got = u32::try_from(resp.segments.len()).unwrap_or(u32::MAX);
                        if got > *requested {
                            all_segments.extend(resp.segments);
                            let error = AllocError::InvalidResponse {
                                dg_id: *dg_id,
                                reason: format!("requested {requested} segments, got {got}"),
                            };
                            self.rollback_or_error(&all_segments, error).await?;
                            unreachable!("rollback_or_error always returns Err")
                        }
                        if let Some(reason) = resp.segments.iter().find_map(|segment| {
                            self.validate_segment(segment, *dg_id, owner_chunk, unit_count)
                        }) {
                            all_segments.extend(resp.segments);
                            let error = AllocError::InvalidResponse {
                                dg_id: *dg_id,
                                reason,
                            };
                            self.rollback_or_error(&all_segments, error).await?;
                            unreachable!("rollback_or_error always returns Err")
                        }
                        if got < *requested {
                            warn!(
                                disk_group_id = *dg_id,
                                requested, got, attempt, "partial response from diskdb, will retry missing"
                            );
                            all_segments.extend(resp.segments);
                            next_pending.push((*dg_id, *requested - got));
                        } else {
                            all_segments.extend(resp.segments);
                        }
                    }
                    Err(e) => {
                        errors.push(e);
                    }
                }
            }

            if !errors.is_empty() {
                // Hard failure — free everything allocated so far.
                let error = errors.into_iter().next().expect("at least one error");
                return self.rollback_or_error(&all_segments, error).await;
            }

            pending = next_pending;
            if !pending.is_empty() && attempt < MAX_ALLOC_RETRIES {
                self.record_diskdb_retry();
                warn!(
                    pending_count = pending.len(),
                    attempt = attempt + 1,
                    "retrying partial allocation"
                );
            }
        }

        if !pending.is_empty() {
            let expected: u32 = plan.entries.iter().map(|e| e.block_count).sum();
            let got = u32::try_from(all_segments.len()).unwrap_or(u32::MAX);
            warn!(expected, got, "allocation retries exhausted, freeing all");
            return self
                .rollback_or_error(
                    &all_segments,
                    AllocError::PartialAllocation {
                        requested: expected,
                        got,
                    },
                )
                .await;
        }

        info!(segment_count = all_segments.len(), "strip allocated");
        Ok(all_segments)
    }

    async fn allocate_blocks_in_plan_order(
        &self,
        owner_chunk: &ChunkId,
        plan: &PlacementPlan,
        unit_count: u32,
    ) -> Result<Vec<Segment>, AllocError> {
        let mut grouped = HashMap::<u64, Vec<usize>>::new();
        for (index, entry) in plan.entries.iter().enumerate() {
            grouped.entry(entry.disk_group_id).or_default().push(index);
        }
        let requests = grouped.iter().map(|(dg_id, indexes)| {
            let pool = Arc::clone(&self.pool);
            let owner = *owner_chunk;
            let dg_id = *dg_id;
            let count = u32::try_from(indexes.len()).unwrap_or(u32::MAX);
            async move {
                pool.allocate_blocks(dg_id, count, unit_count, &owner)
                    .await
                    .map_err(|error| AllocError::AllocateFailed {
                        dg_id,
                        error: error.to_string(),
                    })
            }
        });
        let results = join_all(requests).await;
        let mut ordered = vec![None; plan.entries.len()];
        let mut allocated = Vec::with_capacity(plan.entries.len());
        let mut error = None;
        for (result, (dg_id, indexes)) in results.into_iter().zip(&grouped) {
            match result {
                Ok(response) => {
                    let got = response.segments.len();
                    allocated.extend_from_slice(&response.segments);
                    if got != indexes.len() {
                        error = Some(AllocError::PartialAllocation {
                            requested: u32::try_from(indexes.len()).unwrap_or(u32::MAX),
                            got: u32::try_from(got).unwrap_or(u32::MAX),
                        });
                        continue;
                    }
                    for (index, segment) in indexes.iter().copied().zip(response.segments) {
                        if let Some(reason) = self.validate_segment(&segment, *dg_id, owner_chunk, unit_count)
                        {
                            error = Some(AllocError::InvalidResponse {
                                dg_id: *dg_id,
                                reason,
                            });
                        }
                        ordered[index] = Some(segment);
                    }
                }
                Err(current) if error.is_none() => error = Some(current),
                Err(_) => {}
            }
        }
        self.finish_ordered_allocation(plan, ordered, allocated, error)
            .await
    }

    async fn allocate_blocks_in_plan_order_reusing_disks(
        &self,
        owner_chunk: &ChunkId,
        plan: &PlacementPlan,
        unit_count: u32,
    ) -> Result<Vec<Segment>, AllocError> {
        let mut grouped = HashMap::<u64, Vec<usize>>::new();
        for (index, entry) in plan.entries.iter().enumerate() {
            grouped.entry(entry.disk_group_id).or_default().push(index);
        }
        let requests = grouped.iter().map(|(dg_id, indexes)| {
            let pool = Arc::clone(&self.pool);
            let owner = *owner_chunk;
            let dg_id = *dg_id;
            let count = u32::try_from(indexes.len()).unwrap_or(u32::MAX);
            async move {
                pool.allocate_blocks_reusing_disks(dg_id, count, unit_count, &owner)
                    .await
                    .map_err(|error| AllocError::AllocateFailed {
                        dg_id,
                        error: error.to_string(),
                    })
            }
        });
        let results = join_all(requests).await;
        let mut ordered = vec![None; plan.entries.len()];
        let mut allocated = Vec::with_capacity(plan.entries.len());
        let mut error = None;
        for (result, (dg_id, indexes)) in results.into_iter().zip(&grouped) {
            match result {
                Ok(response) => {
                    let got = response.segments.len();
                    allocated.extend_from_slice(&response.segments);
                    if got != indexes.len() {
                        error = Some(AllocError::PartialAllocation {
                            requested: u32::try_from(indexes.len()).unwrap_or(u32::MAX),
                            got: u32::try_from(got).unwrap_or(u32::MAX),
                        });
                        continue;
                    }
                    for (index, segment) in indexes.iter().copied().zip(response.segments) {
                        if let Some(reason) = self.validate_segment(&segment, *dg_id, owner_chunk, unit_count)
                        {
                            error = Some(AllocError::InvalidResponse {
                                dg_id: *dg_id,
                                reason,
                            });
                        }
                        ordered[index] = Some(segment);
                    }
                }
                Err(current) if error.is_none() => error = Some(current),
                Err(_) => {}
            }
        }
        self.finish_ordered_allocation(plan, ordered, allocated, error)
            .await
    }

    async fn finish_ordered_allocation(
        &self,
        plan: &PlacementPlan,
        ordered: Vec<Option<Segment>>,
        allocated: Vec<Segment>,
        error: Option<AllocError>,
    ) -> Result<Vec<Segment>, AllocError> {
        if let Some(error) = error {
            return self.rollback_or_error(&allocated, error).await;
        }
        if let Some(ordered) = ordered.into_iter().collect::<Option<Vec<_>>>() {
            return Ok(ordered);
        }
        self.rollback_or_error(
            &allocated,
            AllocError::PartialAllocation {
                requested: u32::try_from(plan.entries.len()).unwrap_or(u32::MAX),
                got: u32::try_from(allocated.len()).unwrap_or(u32::MAX),
            },
        )
        .await?;
        unreachable!("rollback_or_error always returns Err")
    }

    pub async fn rollback_conversion_group(
        &self,
        mirrors: &[ChunkStrip],
        parity_segments: &[Segment],
    ) -> Result<(), AllocError> {
        let mut segments: Vec<_> = mirrors.iter().flat_map(extract_segments).collect();
        segments.extend_from_slice(parity_segments);
        self.free_all(&segments).await
    }

    fn record_diskdb_retry(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.allocate_diskdb_retries.inc();
        }
    }

    /// Free all allocated segments (rollback). Logs failures for the
    /// orphan scanner but does not propagate the free error.
    pub async fn rollback_strips(&self, strips: &[ChunkStrip]) -> Result<(), AllocError> {
        let segments: Vec<_> = strips.iter().flat_map(extract_segments).collect();
        let result = self.free_all(&segments).await;
        if let Some(metrics) = &self.metrics {
            metrics
                .allocate_rollback_blocks
                .inc_by(u64::try_from(segments.len()).unwrap_or(u64::MAX));
        }
        result
    }

    fn validate_segment(
        &self,
        segment: &Segment,
        dg_id: u64,
        owner_chunk: &ChunkId,
        unit_count: u32,
    ) -> Option<String> {
        if segment.owner_chunk.as_ref() != Some(owner_chunk) {
            return Some("owner_chunk does not match request".to_string());
        }
        if segment.unit_count != unit_count {
            return Some(format!(
                "requested unit_count {unit_count}, got {}",
                segment.unit_count
            ));
        }
        let Some(disk_id) = segment.disk_id.as_ref() else {
            return Some("segment has no disk_id".to_string());
        };
        if self.pool.dg_for_disk(disk_id) != Some(dg_id) {
            return Some("segment disk does not belong to requested disk_group".to_string());
        }
        None
    }

    async fn rollback_or_error<T>(&self, segments: &[Segment], cause: AllocError) -> Result<T, AllocError> {
        match self.free_all(segments).await {
            Ok(()) => Err(cause),
            Err(rollback) => Err(AllocError::Rollback(format!("{cause}; {rollback}"))),
        }
    }

    async fn free_all(&self, segments: &[Segment]) -> Result<(), AllocError> {
        if segments.is_empty() {
            return Ok(());
        }
        self.pool
            .free_blocks(segments.to_vec())
            .await
            .map_err(AllocError::Rollback)
    }
}

fn grouped_requests(plan: &PlacementPlan) -> Vec<(u64, u32)> {
    let mut grouped = HashMap::<u64, u32>::new();
    for entry in &plan.entries {
        grouped
            .entry(entry.disk_group_id)
            .and_modify(|count| *count = count.saturating_add(entry.block_count))
            .or_insert(entry.block_count);
    }
    grouped.into_iter().collect()
}

fn extract_segments(strip: &ChunkStrip) -> Vec<Segment> {
    use crowdb_protocol::chunkdb::rpc::Strip;
    match &strip.strip {
        Some(Strip::MirrorStrip(mirror)) => mirror.segments.clone(),
        Some(Strip::EcStrip(ec)) => ec.segments.clone(),
        None => Vec::new(),
    }
}

/// Assemble a `ChunkStrip` from allocated segments.
fn assemble_strip(
    segments: &[Segment],
    strip_type: StripAllocType,
    strip_sequence: u32,
    unit_count: u32,
    unit_kb: u32,
) -> ChunkStrip {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));

    match strip_type {
        StripAllocType::Mirror { .. } => ChunkStrip {
            chunk_offset: 0,
            strip_sequence,
            unit_kb,
            capacity: unit_count.saturating_mul(unit_kb),
            create_ts_ms: now_ms,
            sealed_ts_ms: 0,
            sealed_length: 0,
            strip_type: ProtoStripType::Mirror as i32,
            strip: Some(crowdb_protocol::chunkdb::rpc::Strip::MirrorStrip(MirrorStrip {
                segments: segments.to_vec(),
            })),
            usage_bitmap: Vec::new(),
            unavailable_segments: Vec::new(),
        },
        StripAllocType::Ec { data_num, code_num } => ChunkStrip {
            chunk_offset: 0,
            strip_sequence,
            unit_kb,
            capacity: unit_count
                .saturating_mul(unit_kb)
                .saturating_mul(u32::try_from(data_num).unwrap_or(u32::MAX)),
            create_ts_ms: now_ms,
            sealed_ts_ms: 0,
            sealed_length: 0,
            strip_type: ProtoStripType::Ec as i32,
            strip: Some(crowdb_protocol::chunkdb::rpc::Strip::EcStrip(EcStrip {
                data_num: u32::try_from(data_num).unwrap_or(u32::MAX),
                code_num: u32::try_from(code_num).unwrap_or(u32::MAX),
                ec_state: crowdb_protocol::chunkdb::rpc::EcState::NoParity as i32,
                segments: segments.to_vec(),
            })),
            usage_bitmap: Vec::new(),
            unavailable_segments: Vec::new(),
        },
    }
}
