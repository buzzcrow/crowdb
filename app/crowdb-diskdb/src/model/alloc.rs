// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Two-phase allocate/free orchestration — coordinates the in-memory
//! `DdbDiskGroup`/`DdbDisk`/`DdbZone` model with durable writes via
//! `DdbKvClient`.
//!
//! Phase 1 (sync): bitmap CAS on the in-memory zone.
//! Phase 2 (async): persist the durable record via `DdbKvClient`.
//!
//! On Phase 2 failure, rolls back the bitmap bits (Phase 1 undo).

use std::sync::Arc;

use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::{
    BlockState, BusyBlockValue, CommitState, FreeBlockValue, FreeFailure, FreeFailureReason, Segment,
};

use crate::ddb_kv_client::{Bind, DdbKvClient};
use crate::model::disk_group::{AllocClaim, AllocError, DdbDiskGroup, TentativeBlock};
use crate::recovery::compaction::compact_zone;
/// Errors from the free path.
#[derive(Debug)]
pub enum FreeError {
    /// KV client error during lookup or persist.
    Kv(crowdb_kv_client::Error),
    /// Block is not busy (no `BusyBlockKey` exists) — double-free or
    /// never allocated.
    NotBusy {
        disk_id: DiskId,
        zone_index: u32,
        unit_offset: u64,
    },
    IncarnationMismatch,
    Conflict,
    OutcomeUnknown,
}

impl std::fmt::Display for FreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kv(e) => write!(f, "kv error: {e}"),
            Self::NotBusy {
                disk_id,
                zone_index,
                unit_offset,
            } => write!(
                f,
                "block not busy: disk {disk_id:?} zone {zone_index} offset {unit_offset}"
            ),
            Self::IncarnationMismatch => write!(f, "block incarnation does not match"),
            Self::Conflict => write!(f, "busy block changed concurrently"),
            Self::OutcomeUnknown => write!(f, "free outcome is unknown"),
        }
    }
}

impl std::error::Error for FreeError {}

impl From<crowdb_kv_client::Error> for FreeError {
    fn from(e: crowdb_kv_client::Error) -> Self {
        Self::Kv(e)
    }
}

/// Synchronous compaction fallback: when `dg.allocate_block` returns
/// `NoSpace`, compact non-active zones on all disks in the disk-group
/// (up to `zone_rotate_count` per disk), then retry. This reclaims
/// freed space that hasn't been compacted yet (persist-only free
/// leaves bits set). See §5 Preparatory thread — Fallback.
async fn compact_fallback(
    dg: &Arc<DdbDiskGroup>,
    kv: &DdbKvClient,
    zone_rotate_count: u32,
    metrics: &crate::metrics::DiskdbMetrics,
) {
    let bind = dg.bind();
    let disks = dg.disks.read().unwrap().clone();
    for disk in disks {
        // Collect active zone indices to skip (I4).
        let active_zone_indices: std::collections::HashSet<u32> = {
            let active = disk.active_zone_context.load();
            active.iter().map(|z| z.zone_index).collect()
        };
        let zones = disk.zones.load_full();
        let mut compacted = 0u32;
        for zone in zones.iter() {
            if compacted >= zone_rotate_count {
                break;
            }
            // Skip active zones — no concurrent allocate (I4).
            if active_zone_indices.contains(&zone.zone_index) {
                continue;
            }
            // Skip zones that are already ready.
            if zone.compacted_ready.load(std::sync::atomic::Ordering::Acquire) {
                continue;
            }
            if let Err(e) = compact_zone(kv, bind, disk.disk_id, zone, zone.zone_index, metrics).await {
                tracing::warn!(
                    disk_id = ?disk.disk_id,
                    zone_index = zone.zone_index,
                    error = %e,
                    "synchronous compaction fallback failed"
                );
            } else {
                compacted += 1;
            }
        }
    }
}

/// Two-phase allocate a single block.
///
/// Phase 1 (sync): bitmap CAS via `dg.allocate_block`.
/// Phase 2 (async): persist `BusyBlockValue` via `DdbKvClient`.
///
/// On Phase 2 failure, rolls back the bitmap bits (Phase 1 undo) and
/// returns the error. See §4.5.
///
/// Synchronous compaction fallback: if Phase 1 returns `NoSpace`,
/// compacts non-active zones on all disks (reclaiming freed space),
/// then retries Phase 1 once. See §5 Preparatory thread — Fallback.
///
/// # Errors
/// Returns `AllocError::NoSpace` if no disk/zone can satisfy the
/// request (even after compaction fallback), or a KV client error if
/// the persist fails.
#[allow(clippy::too_many_arguments)]
pub async fn allocate_block(
    dg: &Arc<DdbDiskGroup>,
    unit_count: u32,
    owner_chunk: &ChunkId,
    unit_size: u32,
    kv: &DdbKvClient,
    cas_retry_limit: u32,
    zone_rotate_count: u32,
    metrics: &crate::metrics::DiskdbMetrics,
) -> std::result::Result<Segment, AllocError> {
    // Phase 1: bitmap CAS.
    let (disk, zone, range) = match dg.allocate_block(unit_count, &[], cas_retry_limit, zone_rotate_count) {
        Ok(claim) => claim,
        Err(AllocError::NoSpace) => {
            // Synchronous compaction fallback: compact non-active
            // zones to reclaim freed space, then retry once.
            tracing::info!("allocate NoSpace — running synchronous compaction fallback");
            compact_fallback(dg, kv, zone_rotate_count, metrics).await;
            dg.allocate_block(unit_count, &[], cas_retry_limit, zone_rotate_count)?
        }
        Err(error @ AllocError::Persistence) => return Err(error),
    };

    // Record per-disk event counter after Phase 1 CAS succeeds.
    if let Some(m) = &disk.metrics {
        m.record_allocate(range.unit_count, unit_size);
    }

    // Phase 2: persist BusyBlockValue.
    let value = BusyBlockValue {
        unit_count: range.unit_count,
        owner_chunk: Some(*owner_chunk),
        unit_size,
        state: BlockState::Ok as i32,
        commit_state: CommitState::Tentative as i32,
        allocation_ts: dg.next_allocation_ts(),
    };
    let bind = dg.bind();
    if let Err(e) = kv
        .persist_busy(bind, &disk.disk_id, zone.zone_index, range.unit_offset, &value)
        .await
    {
        // Rollback Phase 1.
        let _ = zone.rollback_allocate(range.unit_offset, range.unit_count);
        tracing::warn!("allocate persist failed, rolled back bitmap: {e}");
        return Err(AllocError::Persistence);
    }

    Ok(Segment {
        disk_id: Some(disk.disk_id),
        zone_index: zone.zone_index,
        unit_offset: range.unit_offset,
        unit_count: range.unit_count,
        owner_chunk: Some(*owner_chunk),
        allocation_ts: value.allocation_ts,
    })
}

/// Two-phase allocate multiple blocks (one `batch_write` per data
/// group). See §4.5.
///
/// Synchronous compaction fallback: if Phase 1 cannot place all
/// `count` blocks, compacts non-active zones on all disks (reclaiming
/// freed space), then retries Phase 1 once.
///
/// # Errors
/// Returns `AllocError::NoSpace` if not all `count` blocks can be
/// placed (even after compaction fallback), or a KV client error if
/// the batch persist fails.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn allocate_blocks(
    dg: &Arc<DdbDiskGroup>,
    unit_count: u32,
    count: u32,
    exclude_disks: &[DiskId],
    allow_disk_reuse: bool,
    owner_chunk: &ChunkId,
    unit_size: u32,
    kv: &DdbKvClient,
    cas_retry_limit: u32,
    zone_rotate_count: u32,
    metrics: &crate::metrics::DiskdbMetrics,
) -> std::result::Result<Vec<Segment>, AllocError> {
    // Phase 1: bitmap CAS for all blocks.
    let allocate = || {
        if allow_disk_reuse {
            dg.allocate_blocks_reusing_disks(
                unit_count,
                count,
                exclude_disks,
                cas_retry_limit,
                zone_rotate_count,
            )
        } else {
            dg.allocate_blocks(
                unit_count,
                count,
                exclude_disks,
                cas_retry_limit,
                zone_rotate_count,
            )
        }
    };
    let claims: Vec<AllocClaim> = match allocate() {
        Ok(claims) if claims.len() == count as usize => claims,
        Ok(claims) => {
            metrics.allocate_partial_batches.inc();
            rollback_claims(&claims, metrics);
            return Err(AllocError::NoSpace);
        }
        Err(AllocError::NoSpace) => {
            // No space at all — try compaction fallback then retry.
            tracing::info!("allocate_blocks NoSpace — running synchronous compaction fallback");
            compact_fallback(dg, kv, zone_rotate_count, metrics).await;
            allocate()?
        }
        Err(error @ AllocError::Persistence) => return Err(error),
    };

    // Record per-disk event counters after Phase 1 CAS succeeds.
    for (disk, _zone, range) in &claims {
        if let Some(m) = &disk.metrics {
            m.record_allocate(range.unit_count, unit_size);
        }
    }
    metrics.allocate_claimed_units.inc_by(
        claims
            .iter()
            .map(|(_, _, range)| u64::from(range.unit_count))
            .sum(),
    );

    // Phase 2: persist all in one batch_write.
    let records: Vec<(DiskId, u32, u64, BusyBlockValue)> = claims
        .iter()
        .map(|(disk, zone, range)| {
            (
                disk.disk_id,
                zone.zone_index,
                range.unit_offset,
                BusyBlockValue {
                    unit_count: range.unit_count,
                    owner_chunk: Some(*owner_chunk),
                    unit_size,
                    state: BlockState::Ok as i32,
                    commit_state: CommitState::Tentative as i32,
                    allocation_ts: dg.next_allocation_ts(),
                },
            )
        })
        .collect();
    let bind = dg.bind();
    if let Err(e) = kv.persist_busy_batch(bind, &records).await {
        // Rollback ALL Phase 1 claims.
        rollback_claims(&claims, metrics);
        tracing::warn!("allocate_blocks persist failed, rolled back {count} claims: {e}");
        metrics.allocate_errors_total.inc();
        metrics.allocate_kv_errors.inc();
        return Err(AllocError::Persistence);
    }

    for (disk_id, zone_index, unit_offset, value) in &records {
        dg.cache_tentative(TentativeBlock {
            disk_id: *disk_id,
            zone_index: *zone_index,
            unit_offset: *unit_offset,
            value: value.clone(),
        });
    }

    let segments: Vec<Segment> = claims
        .iter()
        .zip(&records)
        .map(|((disk, zone, range), (_, _, _, value))| Segment {
            disk_id: Some(disk.disk_id),
            zone_index: zone.zone_index,
            unit_offset: range.unit_offset,
            unit_count: range.unit_count,
            owner_chunk: Some(*owner_chunk),
            allocation_ts: value.allocation_ts,
        })
        .collect();
    Ok(segments)
}

fn rollback_claims(claims: &[AllocClaim], metrics: &crate::metrics::DiskdbMetrics) {
    let mut units = 0_u64;
    for (_, zone, range) in claims {
        let _ = zone.rollback_allocate(range.unit_offset, range.unit_count);
        units = units.saturating_add(u64::from(range.unit_count));
    }
    metrics.allocate_rollback_units.inc_by(units);
}

// ── Immediate free ──────────────────────────────────────────────

/// Free a single block. v1: synchronous (no batch, no timer).
///
/// The free path is **persist-only** with respect to the bitmap. The current
/// busy record and revision are read from the complete KV engine.
/// A matching incarnation is CAS-deleted while its `FreeBlockValue` is written
/// atomically. The in-memory bitmap is not touched,
/// `used_count` is not decremented (I1). Compaction is the sole
/// bit-clearer for freed blocks (I3); `rollback_allocate` is the
/// allocate-only bitmap clear and is never used here.
///
/// A retry after a lost response succeeds when the matching free fact already
/// exists. A missing or mismatched busy incarnation is rejected without a
/// mutation.
/// Post-persist: increment `uncompacted_free_record_count` on the zone
/// so compaction knows there is work to do.
///
/// If the persist fails, the block is still busy on both disk and
/// memory — the caller can retry safely. If the persist succeeds but
/// the in-memory zone lookup fails (rare: disk removed concurrently),
/// the free record is durable but the backlog counter is not bumped;
/// the periodic compaction cadence still reclaims the block.
///
/// # Errors
/// Returns `FreeError::Kv` if the persist fails; retrying is idempotent.
pub async fn free_block(
    dg: &Arc<DdbDiskGroup>,
    segment: &Segment,
    kv: &DdbKvClient,
) -> std::result::Result<(), FreeError> {
    let disk_id = segment.disk_id.ok_or_else(|| {
        FreeError::Kv(crowdb_kv_client::Error::SysdataDecode {
            key: "segment.disk_id".to_string(),
            reason: "missing disk_id in Segment".to_string(),
        })
    })?;
    let bind: Bind = dg.bind();

    let value = FreeBlockValue {
        unit_count: segment.unit_count,
        previous_owner: segment.owner_chunk,
        pre_allocation_ts: segment.allocation_ts,
        free_ts: crate::model::disk_group::now_nanos(),
    };
    let Some((busy, revision)) = kv
        .get_busy(bind, &disk_id, segment.zone_index, segment.unit_offset)
        .await?
    else {
        let existing = kv
            .get_free(
                bind,
                &disk_id,
                segment.zone_index,
                segment.unit_offset,
                segment.allocation_ts,
            )
            .await?;
        return if existing.as_ref().is_some_and(|free| {
            free.unit_count == segment.unit_count
                && free.previous_owner == segment.owner_chunk
                && free.pre_allocation_ts == segment.allocation_ts
        }) {
            Ok(())
        } else {
            Err(FreeError::NotBusy {
                disk_id,
                zone_index: segment.zone_index,
                unit_offset: segment.unit_offset,
            })
        };
    };
    if busy.allocation_ts != segment.allocation_ts
        || busy.unit_count != segment.unit_count
        || busy.owner_chunk != segment.owner_chunk
    {
        return Err(FreeError::IncarnationMismatch);
    }
    kv.free_busy_cas(
        bind,
        &disk_id,
        segment.zone_index,
        segment.unit_offset,
        revision,
        &value,
    )
    .await
    .map_err(|error| match error {
        crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::CasBusy => FreeError::Conflict,
        crowdb_kv_client::Error::OutcomeUnknown => FreeError::OutcomeUnknown,
        other => FreeError::Kv(other),
    })?;
    if dg.remove_matching_tentative(
        segment.allocation_ts,
        disk_id,
        segment.zone_index,
        segment.unit_offset,
    ) {
        // The durable free supersedes the short-lived tentative cache entry.
        // A committed entry is normally already absent.
    }
    // Persist succeeded — the block is free on disk. The in-memory
    // bitmap is untouched (persist-only, I1); compaction reconciles.

    // Post-persist: bump the zone's uncompacted-free backlog. The
    // bitmap is NOT mutated — free is persist-only.
    if !dg.free_block(
        &disk_id,
        segment.zone_index,
        segment.unit_offset,
        segment.unit_count,
    ) {
        // Persist succeeded but the in-memory zone was not found
        // (rare: disk removed concurrently). The free record is
        // durable; the periodic compaction cadence still reclaims the
        // block. The caller's intent was achieved.
        tracing::warn!(
            "free persist succeeded but in-memory zone not found for disk {disk_id:?} zone {} offset {} — backlog counter not bumped",
            segment.zone_index,
            segment.unit_offset
        );
    }

    // Record per-disk event counter after durable free.
    let unit_size = dg.disk_unit_size(disk_id).unwrap_or(0);
    if let Some(m) = dg.disk_metrics(disk_id) {
        m.record_free(segment.unit_count, unit_size);
    }

    Ok(())
}

/// Free multiple blocks (one `batch_write` per data group).
///
/// Persist-only free (same contract as `free_block`): each distinct segment
/// validates and conditionally replaces its busy incarnation. Successful
/// segments are counted and rejected segments are returned individually. The
/// in-memory bitmaps are not touched — bits stay set, `used_count` is not
/// decremented (I1); compaction is the sole bit-clearer (I3).
///
/// If the persist fails, no in-memory state changed — all blocks are
/// still busy and the caller can retry safely.
///
/// # Errors
/// Returns `FreeError::Kv` if the persist fails; retrying is idempotent.
pub async fn free_blocks(
    dg: &Arc<DdbDiskGroup>,
    segments: &[Segment],
    kv: &DdbKvClient,
) -> std::result::Result<FreeBatchResult, FreeError> {
    let mut result = FreeBatchResult::default();
    let mut seen = std::collections::HashSet::with_capacity(segments.len());
    for segment in segments {
        let identity = (
            segment.disk_id,
            segment.zone_index,
            segment.unit_offset,
            segment.allocation_ts,
        );
        if !seen.insert(identity) {
            continue;
        }
        match free_block(dg, segment, kv).await {
            Ok(()) => result.freed_count = result.freed_count.saturating_add(1),
            Err(error) => result.failures.push(FreeFailure {
                segment: *segment,
                reason: match error {
                    FreeError::NotBusy { .. } => FreeFailureReason::NotBusy,
                    FreeError::IncarnationMismatch => FreeFailureReason::IncarnationMismatch,
                    FreeError::Conflict => FreeFailureReason::Conflict,
                    FreeError::OutcomeUnknown => FreeFailureReason::OutcomeUnknown,
                    FreeError::Kv(_) => FreeFailureReason::Unavailable,
                },
            }),
        }
    }
    Ok(result)
}

#[derive(Debug, Default)]
pub struct FreeBatchResult {
    pub freed_count: u32,
    pub failures: Vec<FreeFailure>,
}

/// Commit blocks — mark previously-allocated blocks as permanent.
///
/// For each segment, consults the tentative cache for metrics only and reads
/// the authoritative busy record plus revision from KV. A matching tentative
/// record is changed to committed with CAS, so a concurrent free or reuse
/// cannot be overwritten by a stale commit.
///
/// # Errors
/// Returns `FreeError::NotBusy` if a segment has no busy-block record.
/// Returns `FreeError::Kv` if the read or persist fails.
pub async fn commit_blocks(
    dg: &Arc<DdbDiskGroup>,
    segments: &[Segment],
    kv: &DdbKvClient,
    metrics: &crate::metrics::DiskdbMetrics,
) -> std::result::Result<u32, FreeError> {
    let bind: Bind = dg.bind();

    let mut committed_count = 0_u32;
    let mut seen = std::collections::HashSet::with_capacity(segments.len());
    for seg in segments {
        let disk_id = seg.disk_id.ok_or_else(|| {
            FreeError::Kv(crowdb_kv_client::Error::SysdataDecode {
                key: "segment.disk_id".to_string(),
                reason: "missing disk_id in Segment".to_string(),
            })
        })?;
        if !seen.insert((disk_id, seg.zone_index, seg.unit_offset, seg.allocation_ts)) {
            continue;
        }
        let cached = dg.tentative(seg.allocation_ts).filter(|entry| {
            entry.disk_id == disk_id
                && entry.zone_index == seg.zone_index
                && entry.unit_offset == seg.unit_offset
                && entry.value.owner_chunk == seg.owner_chunk
                && entry.value.unit_count == seg.unit_count
        });
        if cached.is_some() {
            metrics.tentative_cache_hits.inc();
        } else {
            metrics.tentative_cache_misses.inc();
        }
        let Some((mut busy, revision)) = kv
            .get_busy(bind, &disk_id, seg.zone_index, seg.unit_offset)
            .await?
        else {
            return Err(FreeError::NotBusy {
                disk_id,
                zone_index: seg.zone_index,
                unit_offset: seg.unit_offset,
            });
        };
        if busy.allocation_ts != seg.allocation_ts
            || busy.unit_count != seg.unit_count
            || busy.owner_chunk != seg.owner_chunk
        {
            return Err(FreeError::IncarnationMismatch);
        }
        if busy.commit_state != CommitState::Committed as i32 {
            busy.commit_state = CommitState::Committed as i32;
            kv.persist_busy_cas(bind, &disk_id, seg.zone_index, seg.unit_offset, &busy, revision)
                .await
                .map_err(|error| match error {
                    crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::CasBusy => {
                        FreeError::Conflict
                    }
                    crowdb_kv_client::Error::OutcomeUnknown => FreeError::OutcomeUnknown,
                    other => FreeError::Kv(other),
                })?;
        }
        if cached.is_some() {
            let _ = dg.remove_tentative(seg.allocation_ts);
        }
        committed_count = committed_count.saturating_add(1);
    }
    Ok(committed_count)
}
