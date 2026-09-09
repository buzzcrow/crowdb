// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk lifecycle management — state machine + crowdb-rpc handlers.
//!
//! Design §9: `Init → Active → Sealed → Deleted` state machine.
//! Transitions are validated; invalid transitions return
//! `InvalidStateTransition`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use quick_cache::sync::Cache;
use tokio::sync::{Mutex, OwnedMutexGuard};
use tracing::{info, warn};

use crowdb_common::metrics::LatencyHistogram;
use crowdb_protocol::chunkdb::rpc::{
    Chunk, ChunkState as ProtoChunkState, ChunkStrip, ChunkType, Strip, StripCleanupIntent,
    StripType as ProtoStripType,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::common::DiskId;
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_protocol::generate_chunk_id;

use crate::allocator::{AllocError, ChunkAllocator, StripAllocType, StripBatchSpec};
use crate::metrics::ChunkdbMetrics;
use crate::metrics::LifecycleMetrics;
use crate::range_guard::RangeGuard;
use crate::routing::hash_to_bucket;
use crate::selector::PlacementConstraints;
use crate::storage::{ChunkStore, StoreError};
use crate::topology::TopologyCache;

use super::state::{ChunkState, StateTransitionError};

/// Default lock wait time for `LockPolicy::default()`.
const DEFAULT_LOCK_WAIT: Duration = Duration::from_secs(10);
const DEFAULT_LAYOUT_VALIDITY_MS: u64 = 30_000;

/// Lifecycle error — maps to crowdb-rpc status codes in the service layer.
#[derive(Debug, thiserror::Error)]
pub enum LifecycleError {
    #[error("invalid state transition: {0}")]
    InvalidStateTransition(#[from] StateTransitionError),
    #[error("chunk not found")]
    ChunkNotFound,
    #[error("chunk already exists")]
    ChunkAlreadyExists,
    #[error("state conflict — concurrent modification")]
    StateConflict,
    #[error("allocation failed: {0}")]
    Allocation(#[from] AllocError),
    #[error("storage error: {0}")]
    Storage(#[from] StoreError),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("chunk bucket {bucket} not in owned ranges")]
    NotMyRange { bucket: u16 },
    #[error("chunk lock busy — retry later")]
    LockBusy,
    #[error("chunk lock acquire timed out")]
    LockTimeout,
    #[error("strip index {index} out of range (chunk has {len} strips)")]
    StripIndexOutOfRange { index: u32, len: usize },
    #[error("diskdb commit failed: {0}")]
    Commit(String),
    #[error("diskdb cleanup failed after metadata publication: {0}")]
    Cleanup(String),
}

#[derive(Debug)]
pub struct AppendChunkOutcome {
    pub modify_ts: u64,
    pub strips: Vec<ChunkStrip>,
    pub chunk: Option<Chunk>,
}

/// Lock policy — how to handle contention on a per-chunk mutex.
#[derive(Debug, Clone)]
pub enum LockPolicy {
    /// Fail fast with `LockBusy` on contention.
    TryLock,
    /// Park the task up to `duration`, then `LockTimeout`.
    Wait(Duration),
}

impl Default for LockPolicy {
    fn default() -> Self {
        Self::Wait(DEFAULT_LOCK_WAIT)
    }
}

/// Cache hint — whether to populate the payload cache.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CacheHint {
    /// Populate cache on miss, write to cache on refresh (default).
    #[default]
    Cache,
    /// Skip cache population — always fetch from store.
    NoCache,
}

#[path = "lock_map.rs"]
mod lock_map;

pub use lock_map::{ChunkGuard, ChunkLockMap};

/// Lifecycle handler — orchestrates allocate/append/seal/delete/query/list.
pub struct LifecycleHandler {
    store: Arc<ChunkStore>,
    allocator: Arc<ChunkAllocator>,
    topology: TopologyCache,
    /// Range guard — enforces chunkdb instance sharding. `None` for
    /// v1 single-instance mode (no binding table); `Some` for R99
    /// sharded mode.
    range_guard: Option<Arc<RangeGuard>>,
    /// Per-chunk lock map + payload cache. `None` when R100 is not
    /// configured (no lifecycle section in config).
    locks: Option<Arc<ChunkLockMap>>,
    allow_unsafe_ec: bool,
    metrics: Option<Arc<ChunkdbMetrics>>,
    layout_validity_ms: u64,
}

struct AllocationMetricGuard {
    metrics: Option<Arc<ChunkdbMetrics>>,
    success: bool,
}

impl AllocationMetricGuard {
    fn new(metrics: Option<Arc<ChunkdbMetrics>>) -> Self {
        if let Some(metrics) = &metrics {
            metrics.allocate_inflight.inc();
        }
        Self {
            metrics,
            success: false,
        }
    }

    fn mark_success(&mut self) {
        self.success = true;
    }
}

impl Drop for AllocationMetricGuard {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.allocate_inflight.dec();
            if !self.success {
                metrics.allocate_errors.inc();
            }
        }
    }
}

impl LifecycleHandler {
    #[must_use]
    pub fn layout_validity_ms(&self) -> u64 {
        self.layout_validity_ms
    }
    #[must_use]
    pub fn new(store: Arc<ChunkStore>, allocator: Arc<ChunkAllocator>, topology: TopologyCache) -> Self {
        Self {
            store,
            allocator,
            topology,
            range_guard: None,
            locks: None,
            allow_unsafe_ec: false,
            metrics: None,
            layout_validity_ms: DEFAULT_LAYOUT_VALIDITY_MS,
        }
    }

    #[must_use]
    pub fn with_layout_validity(mut self, duration: Duration) -> Self {
        self.layout_validity_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        self
    }

    /// Attach a range guard for R99 sharded mode.
    #[must_use]
    pub fn with_range_guard(mut self, guard: Arc<RangeGuard>) -> Self {
        self.range_guard = Some(guard);
        self
    }

    /// Attach a per-chunk lock map (R100).
    #[must_use]
    pub fn with_locks(mut self, locks: Arc<ChunkLockMap>) -> Self {
        self.locks = Some(locks);
        self
    }

    /// Permit explicitly configured unsafe EC placement fallback.
    #[must_use]
    pub fn with_allow_unsafe_ec(mut self, allow: bool) -> Self {
        self.allow_unsafe_ec = allow;
        self
    }

    /// Attach allocation workflow metrics.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<ChunkdbMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Get a reference to the lock map (if attached).
    #[must_use]
    pub fn locks(&self) -> Option<&Arc<ChunkLockMap>> {
        self.locks.as_ref()
    }

    /// Check the range guard (if present) before processing a
    /// mutating RPC. Read-only RPCs (query, list) bypass the guard.
    fn check_range(&self, chunk_id: &ChunkId) -> Result<(), LifecycleError> {
        if let Some(guard) = &self.range_guard {
            guard
                .check(chunk_id)
                .map_err(|e| LifecycleError::NotMyRange { bucket: e.bucket })?;
        }
        Ok(())
    }

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
        let id = chunk_id.unwrap_or_else(|| {
            let parts = generate_chunk_id(chunk_type as u8);
            parts.to_proto()
        });
        self.check_range(&id)?;
        let mut allocation_guard = AllocationMetricGuard::new(self.metrics.clone());

        // Caller-supplied ID: acquire lock + existence check.
        // Auto-generated ID: skip lock (UUID collision negligible).
        let mut guard = if chunk_id.is_some() {
            if let Some(locks) = &self.locks {
                let g = locks
                    .acquire_for_create(&id, &LockPolicy::default(), CacheHint::Cache)
                    .await?;
                // Existence check inside the lock.
                match self.store.get_chunk(&id).await {
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

        let snap = self.topology.snapshot();

        let mirror_copies = if copy_count == 0 { 3 } else { copy_count as usize };
        let strip_alloc_type = match strip_type {
            ProtoStripType::Mirror => StripAllocType::Mirror {
                copy_count: mirror_copies,
            },
            ProtoStripType::Ec => StripAllocType::Ec {
                data_num: data_num as usize,
                code_num: code_num as usize,
            },
        };

        let constraints = self.placement_constraints();
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
                &id,
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

        let record_started = std::time::Instant::now();
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
        };
        if let Some(metrics) = &self.metrics {
            observe_elapsed(&metrics.allocate_record_build, record_started);
        }

        self.persist_active_chunk(&chunk).await?;
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

    /// Advance the durable cursor of an exclusively owned shared chunk.
    pub async fn advance_chunk_write(
        &self,
        chunk_id: &ChunkId,
        writer_epoch: u64,
        expected_modify_ts: u64,
        acknowledged_cursor: u64,
        closed_strip_sequence: Option<u32>,
        writer_lease_ms: u64,
    ) -> Result<Chunk, LifecycleError> {
        self.check_range(chunk_id)?;
        if writer_epoch == 0 || writer_lease_ms == 0 {
            return Err(LifecycleError::InvalidRequest(
                "writer epoch and lease must be nonzero".into(),
            ));
        }
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
            Some(guard) => guard
                .chunk()
                .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
                .clone(),
            None => self.store.get_chunk(chunk_id).await?,
        };
        ChunkState::from_proto(chunk.state).check_can_append()?;
        if chunk.writer_epoch != writer_epoch || chunk.modify_ts != expected_modify_ts {
            return Err(LifecycleError::StateConflict);
        }
        let capacity_bytes = u64::from(chunk.capacity).saturating_mul(1024);
        if acknowledged_cursor <= chunk.acknowledged_cursor || acknowledged_cursor > capacity_bytes {
            return Err(LifecycleError::InvalidRequest(format!(
                "acknowledged cursor {acknowledged_cursor} must advance beyond {} within capacity {capacity_bytes}",
                chunk.acknowledged_cursor
            )));
        }
        if let Some(sequence) = closed_strip_sequence {
            if chunk
                .closed_strip_sequence
                .is_some_and(|current| sequence < current)
            {
                return Err(LifecycleError::InvalidRequest(
                    "closed strip sequence cannot move backward".into(),
                ));
            }
            let strip = chunk
                .strips
                .iter()
                .find(|strip| strip.strip_sequence == sequence)
                .ok_or(LifecycleError::StripIndexOutOfRange {
                    index: sequence,
                    len: chunk.strips.len(),
                })?;
            let strip_end = u64::from(strip.chunk_offset.saturating_add(strip.capacity)) * 1024;
            if strip_end > acknowledged_cursor {
                return Err(LifecycleError::InvalidRequest(
                    "closed strip extends beyond acknowledged cursor".into(),
                ));
            }
        }
        let now_ms = unix_time_ms();
        if let Some(sequence) = closed_strip_sequence {
            for strip in &mut chunk.strips {
                if strip.strip_sequence <= sequence && strip.sealed_ts_ms == 0 {
                    strip.sealed_ts_ms = now_ms;
                    strip.sealed_length = strip.capacity;
                }
            }
            chunk.closed_strip_sequence = Some(sequence);
        }
        chunk.acknowledged_cursor = acknowledged_cursor;
        chunk.writer_lease_deadline_ms = now_ms.saturating_add(writer_lease_ms);
        chunk.modify_ts = chunk.modify_ts.saturating_add(1);
        self.store.put_chunk(&chunk).await?;
        if let Some(ref mut guard) = guard {
            guard.refresh(chunk.clone());
        }
        Ok(chunk)
    }

    async fn persist_active_chunk(&self, chunk: &Chunk) -> Result<(), LifecycleError> {
        let persist_started = std::time::Instant::now();
        for attempt in 0..100_u32 {
            match self.store.put_chunk(chunk).await {
                Ok(()) => break,
                Err(error) if attempt < 99 => {
                    let backoff_ms = 1_u64.checked_shl(attempt).unwrap_or(u64::MAX).min(50);
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    drop(error);
                }
                Err(error) => {
                    self.allocator.rollback_strips(&chunk.strips).await?;
                    return Err(error.into());
                }
            }
        }
        if let Some(metrics) = &self.metrics {
            observe_elapsed(&metrics.allocate_kv_persist, persist_started);
        }
        Ok(())
    }

    fn commit_strip_segments_background(&self, strips: Vec<ChunkStrip>) {
        let allocator = Arc::clone(&self.allocator);
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            let segments: Vec<_> = strips.iter().flat_map(extract_segments).collect();
            let block_count = u64::try_from(segments.len()).unwrap_or(u64::MAX);
            let result = allocator.pool().commit_blocks(segments).await;
            if let Some(metrics) = metrics {
                observe_elapsed(&metrics.allocate_commit, started);
                if result.is_ok() {
                    metrics.allocate_commit_blocks.inc_by(block_count);
                } else {
                    metrics.allocate_commit_errors.inc();
                }
            }
            if let Err(error) = result {
                warn!(%error, "background block commit failed");
            }
        });
    }

    /// Append strips to an active chunk.
    #[allow(clippy::too_many_arguments)]
    pub async fn append_chunk(
        &self,
        chunk_id: &ChunkId,
        observed_modify_ts: u64,
        strip_count: u32,
        strip_type: ProtoStripType,
        data_num: u32,
        code_num: u32,
        copy_count: u32,
        unit_count: u32,
    ) -> Result<AppendChunkOutcome, LifecycleError> {
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
        current_state.check_can_append()?;
        if observed_modify_ts != chunk.modify_ts {
            return Ok(AppendChunkOutcome {
                modify_ts: chunk.modify_ts,
                strips: Vec::new(),
                chunk: Some(chunk),
            });
        }

        let snap = self.topology.snapshot();
        let mirror_copies = if copy_count == 0 { 3 } else { copy_count as usize };
        let strip_alloc_type = match strip_type {
            ProtoStripType::Mirror => StripAllocType::Mirror {
                copy_count: mirror_copies,
            },
            ProtoStripType::Ec => StripAllocType::Ec {
                data_num: data_num as usize,
                code_num: code_num as usize,
            },
        };

        let constraints = self.placement_constraints();
        let start_seq = if chunk.next_strip_sequence == 0 {
            chunk
                .strips
                .iter()
                .map(|strip| strip.strip_sequence)
                .max()
                .map_or(0, |sequence| sequence.saturating_add(1))
        } else {
            chunk.next_strip_sequence
        };
        let next_strip_sequence = start_seq
            .checked_add(strip_count)
            .ok_or_else(|| LifecycleError::InvalidRequest("chunk strip sequence space exhausted".into()))?;

        let mut appended = self
            .allocator
            .allocate_strips(
                &snap,
                chunk_id,
                StripBatchSpec {
                    strip_type: strip_alloc_type,
                    unit_count,
                    start_sequence: start_seq,
                    strip_count,
                },
                &constraints,
            )
            .await?;
        assign_strip_offsets(&mut appended, chunk.capacity);

        if let Err(error) = self.commit_strip_segments(&appended).await {
            self.allocator.rollback_strips(&appended).await?;
            return Err(error);
        }
        chunk.strips.extend(appended.iter().cloned());
        chunk.next_strip_sequence = next_strip_sequence;
        chunk.capacity = chunk.strips.iter().map(|s| s.capacity).sum();
        chunk.modify_ts = chunk.modify_ts.saturating_add(1);
        if let Err(error) = self.store.put_chunk(&chunk).await {
            self.allocator.rollback_strips(&appended).await?;
            return Err(error.into());
        }

        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        }
        info!(chunk_id = ?chunk_id, added_strips = strip_count, "chunk appended");
        Ok(AppendChunkOutcome {
            modify_ts: chunk.modify_ts,
            strips: appended,
            chunk: None,
        })
    }

    /// Seal a chunk — no more appends allowed.
    pub async fn seal_chunk(&self, chunk_id: &ChunkId, seal_length: u32) -> Result<Chunk, LifecycleError> {
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

        let now_ms = unix_time_ms();
        let first_unused = chunk
            .strips
            .iter()
            .position(|strip| strip.chunk_offset >= seal_length)
            .unwrap_or(chunk.strips.len());
        let unused_strips = chunk.strips.split_off(first_unused);
        let unused_segments: Vec<_> = unused_strips.iter().flat_map(extract_segments).collect();
        let cleanup_operation = (!unused_segments.is_empty()).then_some(ChunkId {
            high: chunk_id.high ^ chunk.modify_ts,
            low: chunk_id.low ^ u64::from(seal_length),
        });

        chunk.state = ProtoChunkState::Sealed as i32;
        chunk.modify_ts = chunk.modify_ts.saturating_add(1);
        chunk.sealed_length = seal_length;
        chunk.sealed_ts_ms = now_ms;
        chunk.capacity = chunk.strips.iter().map(|strip| strip.capacity).sum();
        if let Some(operation_id) = cleanup_operation {
            chunk.cleanup_intents.push(StripCleanupIntent {
                operation_id: Some(operation_id),
                retired_segments: unused_segments.clone(),
                not_before_ms: now_ms,
            });
        }
        seal_written_ec_strips(&mut chunk, seal_length, now_ms);
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
            self.store.put_chunk(&chunk).await?;
            if let Some(ref mut g) = guard {
                g.refresh(chunk.clone());
            }
        }
        info!(chunk_id = ?chunk_id, seal_length, "chunk sealed");
        Ok(chunk)
    }

    /// Delete a chunk — marks deleted and frees segments.
    /// Repeated deletion returns the existing tombstone so retries are idempotent.
    pub async fn delete_chunk(&self, chunk_id: &ChunkId) -> Result<Chunk, LifecycleError> {
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
            self.store.put_chunk(&chunk).await?;
            if let Some(ref mut g) = guard {
                g.refresh(chunk.clone());
            }
            return Ok(chunk);
        }

        current_state.check_can_delete()?;

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
        self.store.put_chunk(&chunk).await?;
        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        }
        info!(chunk_id = ?chunk_id, "chunk deleted");
        Ok(chunk)
    }

    /// Delete a range within a chunk (partial delete). Frees the
    /// segments of strips whose `[chunk_offset, chunk_offset +
    /// capacity)` range overlaps with `[offset, offset + size)`, then
    /// removes those strips from the chunk record. The chunk must be
    /// Active.
    pub async fn delete_chunk_range(
        &self,
        chunk_id: &ChunkId,
        offset: u32,
        size: u32,
    ) -> Result<(), LifecycleError> {
        self.check_range(chunk_id)?;
        if size == 0 {
            return Err(LifecycleError::InvalidRequest(
                "delete range size must be non-zero".into(),
            ));
        }

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
        current_state.check_can_append()?;

        let end = offset
            .checked_add(size)
            .ok_or_else(|| LifecycleError::InvalidRequest("delete range overflows u32".into()))?;
        // Find strips that overlap with [offset, end).
        let (to_remove, to_keep): (Vec<_>, Vec<_>) = chunk.strips.into_iter().partition(|s| {
            let s_start = s.chunk_offset;
            let s_end = s_start.saturating_add(s.capacity);
            s_start < end && offset < s_end
        });

        // Remove references durably before making their blocks reusable.
        let all_segments: Vec<_> = to_remove.iter().flat_map(extract_segments).collect();
        let removed_count = to_remove.len();
        chunk.strips = to_keep;
        chunk.capacity = chunk.strips.iter().map(|s| s.capacity).sum();
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
        info!(chunk_id = ?chunk_id, offset, size, removed_strips = removed_count, "chunk range deleted");
        Ok(())
    }

    /// Compatibility wrapper for a fenced one-strip range replacement.
    pub async fn update_chunk_strip(
        &self,
        chunk_id: &ChunkId,
        strip_index: u32,
        new_strip: ChunkStrip,
    ) -> Result<Chunk, LifecycleError> {
        let chunk = self.query_chunk(chunk_id).await?;
        let idx = usize::try_from(strip_index).unwrap_or(usize::MAX);
        if idx >= chunk.strips.len() {
            return Err(LifecycleError::StripIndexOutOfRange {
                index: strip_index,
                len: chunk.strips.len(),
            });
        }
        let operation_id = ChunkId {
            high: chunk_id.high ^ chunk.modify_ts,
            low: chunk_id.low ^ u64::from(strip_index),
        };
        self.replace_chunk_strip_range(
            chunk_id,
            chunk.modify_ts,
            strip_index,
            std::slice::from_ref(&chunk.strips[idx]),
            std::slice::from_ref(&new_strip),
            operation_id,
        )
        .await
    }

    /// Atomically replace a capacity-compatible strip range under a revision
    /// fence. Newly introduced segments are committed before publication;
    /// removed segments stay allocated until the reader-layout grace expires.
    #[allow(clippy::too_many_arguments)]
    pub async fn replace_chunk_strip_range(
        &self,
        chunk_id: &ChunkId,
        expected_modify_ts: u64,
        start_index: u32,
        old_strips: &[ChunkStrip],
        replacement_strips: &[ChunkStrip],
        operation_id: ChunkId,
    ) -> Result<Chunk, LifecycleError> {
        self.check_range(chunk_id)?;
        if old_strips.is_empty() || replacement_strips.is_empty() {
            return Err(LifecycleError::InvalidRequest(
                "strip replacement ranges must be non-empty".into(),
            ));
        }
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
            Some(current) => current
                .chunk()
                .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
                .clone(),
            None => self.store.get_chunk(chunk_id).await?,
        };
        let state = ChunkState::from_proto(chunk.state);
        if state != ChunkState::Active && state != ChunkState::Sealed {
            return Err(LifecycleError::InvalidStateTransition(StateTransitionError::new(
                state,
                "Active|Sealed",
            )));
        }
        let start = usize::try_from(start_index).unwrap_or(usize::MAX);
        let end = start.saturating_add(old_strips.len());
        if chunk.modify_ts == expected_modify_ts.saturating_add(1)
            && chunk.last_strip_replacement == Some(operation_id)
            && chunk
                .strips
                .get(start..start.saturating_add(replacement_strips.len()))
                == Some(replacement_strips)
        {
            return Ok(chunk);
        }
        if end > chunk.strips.len() {
            return Err(LifecycleError::StripIndexOutOfRange {
                index: start_index,
                len: chunk.strips.len(),
            });
        }
        if chunk.modify_ts != expected_modify_ts || chunk.strips[start..end] != *old_strips {
            return Err(LifecycleError::StateConflict);
        }
        validate_replacement_geometry(old_strips, replacement_strips, chunk_id)?;
        let next_strip_sequence = validate_replacement_sequences(&chunk, start..end, replacement_strips)?;

        let old_segments: HashSet<_> = old_strips.iter().flat_map(extract_segments).collect();
        let new_segments: HashSet<_> = replacement_strips.iter().flat_map(extract_segments).collect();
        let new_only: Vec<_> = new_segments.difference(&old_segments).copied().collect();
        let old_only: Vec<_> = old_segments.difference(&new_segments).copied().collect();
        self.allocator
            .pool()
            .commit_blocks(new_only.clone())
            .await
            .map_err(LifecycleError::Commit)?;

        chunk
            .strips
            .splice(start..end, replacement_strips.iter().cloned());
        chunk.capacity = chunk.strips.iter().map(|strip| strip.capacity).sum();
        chunk.next_strip_sequence = next_strip_sequence;
        chunk.modify_ts = chunk.modify_ts.saturating_add(1);
        chunk.last_strip_replacement = Some(operation_id);
        if !old_only.is_empty() {
            chunk.cleanup_intents.push(StripCleanupIntent {
                operation_id: Some(operation_id),
                retired_segments: old_only,
                not_before_ms: unix_time_ms().saturating_add(self.layout_validity_ms),
            });
        }
        // A failed response is ambiguous: the chunk update may already be
        // durable. Never free committed replacement blocks here because a
        // retry (or recovery task) can safely replay the same operation,
        // whereas rollback could free blocks referenced by the new layout.
        self.store.put_chunk(&chunk).await?;
        if let Some(current) = &mut guard {
            current.refresh(chunk.clone());
        }
        info!(chunk_id = ?chunk_id, start_index, "chunk strip range replaced");
        Ok(chunk)
    }

    /// Allocate one tentative segment for an in-place mirror repair.
    pub async fn allocate_replacement_segment(
        &self,
        chunk_id: &ChunkId,
        old_segment: &Segment,
        surviving_segments: &[Segment],
        exclude_disk_ids: &[DiskId],
    ) -> Result<Segment, LifecycleError> {
        self.allocate_repair_segment(chunk_id, old_segment, surviving_segments, exclude_disk_ids, false)
            .await
    }

    /// Allocate one tentative repair segment, optionally relaxing node
    /// anti-affinity for deliberately undersized test clusters.
    pub async fn allocate_repair_segment(
        &self,
        chunk_id: &ChunkId,
        old_segment: &Segment,
        surviving_segments: &[Segment],
        exclude_disk_ids: &[DiskId],
        allow_unsafe_placement: bool,
    ) -> Result<Segment, LifecycleError> {
        self.check_range(chunk_id)?;
        if old_segment.owner_chunk.as_ref() != Some(chunk_id) || old_segment.unit_count == 0 {
            return Err(LifecycleError::InvalidRequest(
                "replacement geometry must belong to the chunk".into(),
            ));
        }
        let snap = self.topology.snapshot();
        let disk_groups = snap.disk_groups();
        let mut constraints = self.placement_constraints();
        if !allow_unsafe_placement {
            let node_count = disk_groups
                .iter()
                .map(|disk_group| disk_group.node_id)
                .collect::<HashSet<_>>()
                .len();
            let node_limit = self
                .replacement_node_limit(chunk_id, old_segment, node_count)
                .await?;
            let mut survivors_by_node = HashMap::new();
            for segment in surviving_segments {
                if let Some(disk_group) = disk_groups.iter().find(|disk_group| {
                    segment
                        .disk_id
                        .is_some_and(|disk| disk_group.value.disk_ids.contains(&disk))
                }) {
                    *survivors_by_node.entry(disk_group.node_id).or_insert(0_usize) += 1;
                }
            }
            constraints.exclude_nodes.extend(
                survivors_by_node
                    .into_iter()
                    .filter_map(|(node_id, count)| (count >= node_limit).then_some(node_id)),
            );
        }
        let mut excluded = exclude_disk_ids.to_vec();
        for segment in std::iter::once(old_segment).chain(surviving_segments) {
            if let Some(disk_id) = segment.disk_id {
                if !excluded.contains(&disk_id) {
                    excluded.push(disk_id);
                }
            }
        }
        self.allocator
            .allocate_replacement_segment(&snap, chunk_id, old_segment.unit_count, &constraints, excluded)
            .await
            .map_err(LifecycleError::Allocation)
    }

    async fn replacement_node_limit(
        &self,
        chunk_id: &ChunkId,
        old_segment: &Segment,
        node_count: usize,
    ) -> Result<usize, LifecycleError> {
        let chunk = self.store.get_chunk(chunk_id).await?;
        let strip = chunk
            .strips
            .iter()
            .find(|strip| extract_segments(strip).contains(old_segment))
            .ok_or_else(|| LifecycleError::InvalidRequest("old segment is not in the chunk".into()))?;
        match strip.strip.as_ref() {
            Some(Strip::MirrorStrip(_)) => Ok(1),
            Some(Strip::EcStrip(ec)) => {
                let total = ec.data_num as usize + ec.code_num as usize;
                let safe_limit = ec.code_num as usize;
                let nodes = node_count.max(1);
                if nodes.saturating_mul(safe_limit) >= total || !self.allow_unsafe_ec {
                    Ok(safe_limit.max(1))
                } else {
                    Ok(total.div_ceil(nodes).max(1))
                }
            }
            None => Err(LifecycleError::InvalidRequest(
                "replacement strip has no body".into(),
            )),
        }
    }

    /// Allocate a tentative EC strip with the exact logical geometry of a
    /// closed mirror range. Publication remains the caller's responsibility.
    pub async fn allocate_conversion_strip(
        &self,
        chunk_id: &ChunkId,
        old_strips: &[ChunkStrip],
        data_num: u32,
        code_num: u32,
    ) -> Result<ChunkStrip, LifecycleError> {
        self.check_range(chunk_id)?;
        let first = old_strips
            .first()
            .ok_or_else(|| LifecycleError::InvalidRequest("conversion range is empty".into()))?;
        if data_num == 0
            || code_num == 0
            || old_strips.len() != usize::try_from(data_num).unwrap_or(usize::MAX)
            || first.unit_kb == 0
        {
            return Err(LifecycleError::InvalidRequest(
                "conversion range does not match the EC scheme".into(),
            ));
        }
        let unit_count = conversion_unit_count(old_strips, chunk_id)?;
        let snap = self.topology.snapshot();
        if snap.unit_size_bytes() / 1024 != first.unit_kb {
            return Err(LifecycleError::InvalidRequest(
                "conversion strip unit size differs from current topology".into(),
            ));
        }
        let mut replacement = self
            .allocator
            .allocate_strip(
                &snap,
                chunk_id,
                StripAllocType::Ec {
                    data_num: usize::try_from(data_num).unwrap_or(usize::MAX),
                    code_num: usize::try_from(code_num).unwrap_or(usize::MAX),
                },
                unit_count,
                first.strip_sequence,
                &self.placement_constraints(),
            )
            .await?;
        replacement.chunk_offset = first.chunk_offset;
        replacement.capacity = old_strips.iter().try_fold(0u32, |total, strip| {
            total
                .checked_add(strip.capacity)
                .ok_or_else(|| LifecycleError::InvalidRequest("conversion capacity overflows u32".into()))
        })?;
        replacement.sealed_length = old_strips.iter().try_fold(0u32, |total, strip| {
            total.checked_add(strip.sealed_length).ok_or_else(|| {
                LifecycleError::InvalidRequest("conversion sealed length overflows u32".into())
            })
        })?;
        replacement.sealed_ts_ms = old_strips
            .iter()
            .map(|strip| strip.sealed_ts_ms)
            .max()
            .unwrap_or(0);
        Ok(replacement)
    }

    /// Release every segment in an unpublished conversion strip. The
    /// authoritative chunk is checked under its existing lifecycle lock first.
    pub async fn discard_conversion_strip(
        &self,
        chunk_id: &ChunkId,
        strip: &ChunkStrip,
    ) -> Result<(), LifecycleError> {
        self.check_range(chunk_id)?;
        let segments = extract_segments(strip);
        if segments.is_empty()
            || segments
                .iter()
                .any(|segment| segment.owner_chunk.as_ref() != Some(chunk_id))
        {
            return Err(LifecycleError::InvalidRequest(
                "conversion replacement is empty or belongs to another chunk".into(),
            ));
        }
        let _guard = if let Some(locks) = &self.locks {
            Some(
                locks
                    .acquire(chunk_id, &self.store, &LockPolicy::default(), CacheHint::NoCache)
                    .await?,
            )
        } else {
            None
        };
        let chunk = self.store.get_chunk(chunk_id).await?;
        let referenced: HashSet<_> = chunk.strips.iter().flat_map(extract_segments).collect();
        if segments.iter().any(|segment| referenced.contains(segment)) {
            return Err(LifecycleError::StateConflict);
        }
        self.allocator
            .pool()
            .free_blocks(segments)
            .await
            .map_err(LifecycleError::Cleanup)
    }

    /// Release a tentative replacement that was never published.
    pub async fn discard_replacement_segment(
        &self,
        chunk_id: &ChunkId,
        segment: Segment,
    ) -> Result<(), LifecycleError> {
        self.check_range(chunk_id)?;
        if segment.owner_chunk.as_ref() != Some(chunk_id) {
            return Err(LifecycleError::InvalidRequest(
                "discarded replacement does not belong to chunk".into(),
            ));
        }
        let _guard = if let Some(locks) = &self.locks {
            Some(
                locks
                    .acquire(chunk_id, &self.store, &LockPolicy::default(), CacheHint::NoCache)
                    .await?,
            )
        } else {
            None
        };
        let chunk = self.store.get_chunk(chunk_id).await?;
        if chunk
            .strips
            .iter()
            .flat_map(extract_segments)
            .any(|current| current == segment)
        {
            return Err(LifecycleError::StateConflict);
        }
        self.allocator
            .pool()
            .free_blocks(vec![segment])
            .await
            .map_err(LifecycleError::Cleanup)
    }

    /// Query a chunk by ID.
    pub async fn query_chunk(&self, chunk_id: &ChunkId) -> Result<Chunk, LifecycleError> {
        self.store.get_chunk(chunk_id).await.map_err(|e| match e {
            StoreError::ChunkNotFound => LifecycleError::ChunkNotFound,
            other => LifecycleError::Storage(other),
        })
    }

    /// List chunks with pagination.
    pub async fn list_chunks(
        &self,
        start_after: Option<&ChunkId>,
        max_keys: u32,
    ) -> Result<Vec<Chunk>, LifecycleError> {
        if max_keys == 0 {
            return Ok(Vec::new());
        }
        self.store
            .list_chunks(start_after, max_keys)
            .await
            .map_err(LifecycleError::Storage)
    }

    /// Finish durable `Init` allocation intents left by an interrupted commit.
    pub async fn reconcile_pending_chunks(&self) -> Result<u64, LifecycleError> {
        let mut start_after = None;
        let mut reconciled = 0u64;
        loop {
            let chunks = self.list_chunks(start_after.as_ref(), 1_000).await?;
            if chunks.is_empty() {
                break;
            }
            for mut chunk in chunks.iter().cloned() {
                if !chunk.cleanup_intents.is_empty() {
                    let intent_count = chunk.cleanup_intents.len();
                    let current_segments: HashSet<_> =
                        chunk.strips.iter().flat_map(extract_segments).collect();
                    let now_ms = unix_time_ms();
                    let mut pending = Vec::new();
                    for intent in std::mem::take(&mut chunk.cleanup_intents) {
                        if intent.not_before_ms > now_ms {
                            pending.push(intent);
                            continue;
                        }
                        let retired: Vec<_> = intent
                            .retired_segments
                            .iter()
                            .filter(|segment| !current_segments.contains(segment))
                            .copied()
                            .collect();
                        self.allocator
                            .pool()
                            .free_blocks(retired)
                            .await
                            .map_err(LifecycleError::Cleanup)?;
                    }
                    chunk.cleanup_intents = pending;
                    if chunk.cleanup_intents.len() != intent_count {
                        self.store.put_chunk(&chunk).await?;
                        if let (Some(locks), Some(chunk_id)) = (&self.locks, chunk.id) {
                            locks.populate_cache(&chunk_id, chunk.clone());
                        }
                        reconciled = reconciled.saturating_add(1);
                    }
                }
                match ChunkState::from_proto(chunk.state) {
                    ChunkState::Init => {
                        self.commit_strip_segments(&chunk.strips).await?;
                        chunk.state = ProtoChunkState::Active as i32;
                    }
                    ChunkState::Deleted if !chunk.strips.is_empty() => {
                        let segments = chunk.strips.iter().flat_map(extract_segments).collect();
                        self.allocator
                            .pool()
                            .free_blocks(segments)
                            .await
                            .map_err(LifecycleError::Cleanup)?;
                        chunk.strips.clear();
                        chunk.capacity = 0;
                    }
                    _ => continue,
                }
                self.store.put_chunk(&chunk).await?;
                if let (Some(locks), Some(chunk_id)) = (&self.locks, chunk.id) {
                    locks.populate_cache(&chunk_id, chunk);
                }
                reconciled += 1;
            }
            start_after = chunks.last().and_then(|chunk| chunk.id);
            if chunks.len() < 1_000 {
                break;
            }
        }
        Ok(reconciled)
    }

    /// Seal Active shared chunks whose persisted writer lease expired.
    pub async fn seal_expired_writer_chunks(&self) -> Result<u64, LifecycleError> {
        let now_ms = unix_time_ms();
        let mut start_after = None;
        let mut sealed = 0_u64;
        loop {
            let chunks = self.list_chunks(start_after.as_ref(), 1_000).await?;
            if chunks.is_empty() {
                break;
            }
            for candidate in &chunks {
                if ChunkState::from_proto(candidate.state) != ChunkState::Active
                    || candidate.writer_epoch == 0
                    || candidate.writer_lease_deadline_ms > now_ms
                {
                    continue;
                }
                let Some(chunk_id) = candidate.id else {
                    continue;
                };
                let mut guard = if let Some(locks) = &self.locks {
                    Some(
                        locks
                            .acquire(&chunk_id, &self.store, &LockPolicy::default(), CacheHint::Cache)
                            .await?,
                    )
                } else {
                    None
                };
                let mut chunk = match &guard {
                    Some(guard) => guard
                        .chunk()
                        .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
                        .clone(),
                    None => self.store.get_chunk(&chunk_id).await?,
                };
                if ChunkState::from_proto(chunk.state) != ChunkState::Active
                    || chunk.writer_epoch == 0
                    || chunk.writer_lease_deadline_ms > now_ms
                {
                    continue;
                }
                chunk.state = ProtoChunkState::Sealed as i32;
                chunk.modify_ts = chunk.modify_ts.saturating_add(1);
                chunk.sealed_ts_ms = now_ms;
                chunk.sealed_length =
                    u32::try_from(chunk.acknowledged_cursor.div_ceil(1024)).unwrap_or(u32::MAX);
                close_acknowledged_strips(&mut chunk, now_ms);
                self.store.put_chunk(&chunk).await?;
                if let Some(ref mut guard) = guard {
                    guard.refresh(chunk.clone());
                }
                sealed = sealed.saturating_add(1);
                info!(chunk_id = ?chunk_id, cursor = chunk.acknowledged_cursor, "expired shared writer chunk sealed");
            }
            start_after = chunks.last().and_then(|chunk| chunk.id);
            if chunks.len() < 1_000 {
                break;
            }
        }
        Ok(sealed)
    }

    /// Commit all segments in the given strips to diskdb (two-phase
    /// commit: mark tentative blocks as permanent after chunk persist).
    async fn commit_strip_segments(&self, strips: &[ChunkStrip]) -> Result<(), LifecycleError> {
        let all_segments: Vec<_> = strips.iter().flat_map(extract_segments).collect();
        if all_segments.is_empty() {
            return Ok(());
        }
        self.allocator
            .pool()
            .commit_blocks(all_segments)
            .await
            .map_err(LifecycleError::Commit)
    }

    fn placement_constraints(&self) -> PlacementConstraints {
        let constraints = PlacementConstraints::new();
        if self.allow_unsafe_ec {
            constraints.allow_unsafe_ec()
        } else {
            constraints
        }
    }
}

/// Extract all segments from a strip (mirror or EC).
fn extract_segments(strip: &ChunkStrip) -> Vec<crowdb_protocol::diskdb::rpc::Segment> {
    use crowdb_protocol::chunkdb::rpc::Strip;
    match &strip.strip {
        Some(Strip::MirrorStrip(m)) => m.segments.clone(),
        Some(Strip::EcStrip(ec)) => ec.segments.clone(),
        None => Vec::new(),
    }
}

fn conversion_unit_count(old: &[ChunkStrip], chunk_id: &ChunkId) -> Result<u32, LifecycleError> {
    let first = &old[0];
    let unit_count = first
        .strip
        .as_ref()
        .and_then(|strip| match strip {
            Strip::MirrorStrip(mirror) => mirror.segments.first(),
            Strip::EcStrip(_) => None,
        })
        .map_or(0, |segment| segment.unit_count);
    let mut expected_offset = first.chunk_offset;
    let valid = unit_count > 0
        && old.iter().all(|strip| {
            let matches = strip.unit_kb == first.unit_kb
                && strip.capacity == first.capacity
                && strip.chunk_offset == expected_offset
                && strip.capacity == unit_count.saturating_mul(strip.unit_kb)
                && matches!(&strip.strip, Some(Strip::MirrorStrip(mirror)) if !mirror.segments.is_empty()
                    && mirror.segments.iter().all(|segment| segment.owner_chunk.as_ref() == Some(chunk_id)
                        && segment.unit_count == unit_count));
            expected_offset = expected_offset.saturating_add(strip.capacity);
            matches
        });
    if !valid {
        return Err(LifecycleError::InvalidRequest(
            "conversion requires contiguous equal-capacity mirror strips".into(),
        ));
    }
    Ok(unit_count)
}

fn validate_replacement_geometry(
    old: &[ChunkStrip],
    replacement: &[ChunkStrip],
    chunk_id: &ChunkId,
) -> Result<(), LifecycleError> {
    let old_capacity: u32 = old.iter().map(|strip| strip.capacity).sum();
    let new_capacity: u32 = replacement.iter().map(|strip| strip.capacity).sum();
    if old_capacity != new_capacity
        || old[0].chunk_offset != replacement[0].chunk_offset
        || old[0].strip_sequence != replacement[0].strip_sequence
    {
        return Err(LifecycleError::InvalidRequest(
            "replacement must preserve first offset, first sequence, and total capacity".into(),
        ));
    }
    if replacement
        .iter()
        .flat_map(extract_segments)
        .any(|segment| segment.owner_chunk.as_ref() != Some(chunk_id) || segment.unit_count == 0)
    {
        return Err(LifecycleError::InvalidRequest(
            "replacement segments must be non-empty and owned by the chunk".into(),
        ));
    }
    let mut expected_offset = replacement[0].chunk_offset;
    for strip in replacement {
        if strip.capacity == 0 || strip.unit_kb == 0 || strip.chunk_offset != expected_offset {
            return Err(LifecycleError::InvalidRequest(
                "replacement strips must be non-empty and contiguous".into(),
            ));
        }
        expected_offset = expected_offset
            .checked_add(strip.capacity)
            .ok_or_else(|| LifecycleError::InvalidRequest("replacement strip range overflows u32".into()))?;
        let segments = extract_segments(strip);
        let shape_is_valid = match &strip.strip {
            Some(Strip::MirrorStrip(_)) => {
                !segments.is_empty()
                    && segments
                        .iter()
                        .all(|segment| segment.unit_count.saturating_mul(strip.unit_kb) == strip.capacity)
            }
            Some(Strip::EcStrip(ec)) => {
                ec.data_num > 0
                    && ec.code_num > 0
                    && segments.len()
                        == usize::try_from(ec.data_num.saturating_add(ec.code_num)).unwrap_or(usize::MAX)
                    && segments.iter().all(|segment| {
                        segment
                            .unit_count
                            .saturating_mul(strip.unit_kb)
                            .saturating_mul(ec.data_num)
                            == strip.capacity
                    })
            }
            None => false,
        };
        if !shape_is_valid {
            return Err(LifecycleError::InvalidRequest(
                "replacement strip has invalid mirror or EC geometry".into(),
            ));
        }
        let segment_set: HashSet<_> = segments.into_iter().collect();
        if strip
            .unavailable_segments
            .iter()
            .any(|segment| !segment_set.contains(segment))
        {
            return Err(LifecycleError::InvalidRequest(
                "unavailable replicas must belong to the replacement strip".into(),
            ));
        }
    }
    Ok(())
}

fn validate_replacement_sequences(
    chunk: &Chunk,
    replaced: std::ops::Range<usize>,
    replacement: &[ChunkStrip],
) -> Result<u32, LifecycleError> {
    let retained: HashSet<_> = chunk
        .strips
        .iter()
        .enumerate()
        .filter(|(index, _)| !replaced.contains(index))
        .map(|(_, strip)| strip.strip_sequence)
        .collect();
    let mut seen = HashSet::with_capacity(replacement.len());
    for (index, strip) in replacement.iter().enumerate() {
        let sequence_is_invalid = index > 0
            && (strip.strip_sequence < chunk.next_strip_sequence
                || strip.strip_sequence <= replacement[index - 1].strip_sequence);
        if !seen.insert(strip.strip_sequence)
            || retained.contains(&strip.strip_sequence)
            || sequence_is_invalid
        {
            return Err(LifecycleError::InvalidRequest(
                "replacement strip sequences must be unique and newly allocated after the first".into(),
            ));
        }
    }
    replacement
        .iter()
        .skip(1)
        .try_fold(chunk.next_strip_sequence, |next, strip| {
            strip
                .strip_sequence
                .checked_add(1)
                .map(|candidate| next.max(candidate))
                .ok_or_else(|| {
                    LifecycleError::InvalidRequest("replacement strip sequence space exhausted".into())
                })
        })
}

fn observe_elapsed(metric: &LatencyHistogram, started: std::time::Instant) {
    metric.observe(started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
}

fn assign_strip_offsets(strips: &mut [ChunkStrip], mut chunk_offset: u32) {
    for strip in strips {
        strip.chunk_offset = chunk_offset;
        chunk_offset = chunk_offset.saturating_add(strip.capacity);
    }
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn writer_lease_deadline(now_ms: u64, writer_epoch: u64, writer_lease_ms: u64) -> u64 {
    if writer_epoch == 0 {
        0
    } else {
        now_ms.saturating_add(writer_lease_ms)
    }
}

fn close_acknowledged_strips(chunk: &mut Chunk, now_ms: u64) {
    if chunk.writer_epoch == 0 || chunk.acknowledged_cursor == 0 {
        return;
    }
    let cursor = chunk.acknowledged_cursor;
    let mut last_closed = chunk.closed_strip_sequence;
    for strip in &mut chunk.strips {
        let start = u64::from(strip.chunk_offset) * 1024;
        if cursor <= start {
            break;
        }
        let end = u64::from(strip.chunk_offset.saturating_add(strip.capacity)) * 1024;
        let length = cursor.min(end).saturating_sub(start);
        strip.sealed_length = u32::try_from(length.div_ceil(1024)).unwrap_or(u32::MAX);
        strip.sealed_ts_ms = now_ms;
        last_closed = Some(strip.strip_sequence);
        if cursor < end {
            break;
        }
    }
    chunk.closed_strip_sequence = last_closed;
}

fn seal_written_ec_strips(chunk: &mut Chunk, seal_length: u32, now_ms: u64) {
    let mut remaining = seal_length;
    for strip in &mut chunk.strips {
        let written = remaining.min(strip.capacity);
        if written == 0 {
            break;
        }
        let Some(crowdb_protocol::chunkdb::rpc::Strip::EcStrip(ec)) = strip.strip.as_mut() else {
            continue;
        };
        strip.sealed_length = written;
        strip.sealed_ts_ms = now_ms;
        ec.ec_state = crowdb_protocol::chunkdb::rpc::EcState::Parity as i32;
        remaining -= written;
    }
}
