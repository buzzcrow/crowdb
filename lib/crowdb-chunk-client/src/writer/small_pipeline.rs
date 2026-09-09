// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Single-owner shared chunk worker and whole-object batch commit barrier.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use crowdb_common::ec::{EcScheme, IncrementalParity};
use crowdb_protocol::chunkdb::rpc::{
    AdvanceChunkWriteRequest, AllocateChunkRequest, AllocateReplacementSegmentRequest, AppendChunkRequest,
    Chunk, ChunkState, ChunkStrip, ChunkType, DeleteChunkRequest, DiscardReplacementSegmentRequest, Location,
    MutateStripReservationRequest, PrepareMirrorToEcConversionRequest, QueryChunkRequest,
    ReplaceChunkStripRangeRequest, ReserveStripGroupRequest, SealChunkRequest, Strip, StripReservationAction,
    StripType,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_protocol::{generate_chunk_id, CHUNK_TYPE_REPO};
use tokio::sync::{mpsc, Notify, OwnedSemaphorePermit};

use crate::config::SmallWritePolicy;
use crate::metrics::SmallWriteMetrics;
use crate::negative_list::FailedDiskList;
use crate::{ChunkAllocator, DiskWriter, IoError, Result};

use super::small_pool::{PendingObject, PipelineRoute, SmallPoolRuntime};

static NEXT_WRITER_EPOCH: AtomicU64 = AtomicU64::new(1);

pub(crate) struct ManagedPipeline {
    pub route: Arc<PipelineRoute>,
    pub retire: Arc<AtomicBool>,
    pub wake: Arc<Notify>,
    pub join: tokio::task::JoinHandle<Result<()>>,
}

pub(crate) async fn spawn(runtime: Arc<SmallPoolRuntime>, _id: u64) -> Result<ManagedPipeline> {
    let (sender, receiver) = mpsc::channel(runtime.policy.queue_capacity);
    let route = Arc::new(PipelineRoute::new(
        sender,
        runtime.now_ms(),
        Arc::clone(&runtime.conversion_active),
    ));
    let owned = OwnedChunk::allocate(&runtime, Arc::clone(&route.conversion_active)).await?;
    let shadow_bytes =
        u32::try_from(u64::from(owned.current_strip()?.capacity).saturating_mul(1024)).unwrap_or(u32::MAX);
    let shadow_budget = Arc::clone(&runtime.budget)
        .try_acquire_many_owned(shadow_bytes)
        .map_err(|_| IoError::MemoryBudgetExhausted)?;
    let retire = Arc::new(AtomicBool::new(false));
    let wake = Arc::new(Notify::new());
    let worker = PipelineWorker {
        runtime: Arc::clone(&runtime),
        route: Arc::clone(&route),
        receiver,
        retire: Arc::clone(&retire),
        wake: Arc::clone(&wake),
        chunk: owned,
        replacement: None,
        carry: None,
        _shadow_budget: shadow_budget,
    };
    let join = tokio::spawn(worker.run());
    Ok(ManagedPipeline {
        route,
        retire,
        wake,
        join,
    })
}

impl ManagedPipeline {
    pub fn begin_retire(&self) {
        self.retire.store(true, Ordering::Release);
        self.wake.notify_one();
    }
}

struct PipelineWorker {
    runtime: Arc<SmallPoolRuntime>,
    route: Arc<PipelineRoute>,
    receiver: mpsc::Receiver<PendingObject>,
    retire: Arc<AtomicBool>,
    wake: Arc<Notify>,
    chunk: OwnedChunk,
    replacement: Option<OwnedChunk>,
    carry: Option<PendingObject>,
    _shadow_budget: OwnedSemaphorePermit,
}

impl PipelineWorker {
    async fn run(mut self) -> Result<()> {
        loop {
            if self.retire.load(Ordering::Acquire) {
                self.receiver.close();
            }
            let (first, dequeued) = if let Some(object) = self.carry.take() {
                (Some(object), false)
            } else if self.retire.load(Ordering::Acquire) {
                (self.receiver.recv().await, true)
            } else {
                let object = tokio::select! {
                    object = self.receiver.recv() => object,
                    () = self.wake.notified() => {
                        self.receiver.close();
                        self.receiver.recv().await
                    }
                };
                (object, true)
            };
            let Some(first) = first else {
                break;
            };
            if dequeued {
                self.note_dequeue(&first);
            }
            if let Err(error) = self.ensure_object_fits(first.len).await {
                fail_one(first, &error.to_string(), &self.runtime.metrics);
                self.fail_remaining(&error.to_string()).await;
                let _ = self.finish_chunks().await;
                return Err(error);
            }
            let batch = self.collect_batch(first);
            self.route.busy.store(true, Ordering::Release);
            let result = self.write_batch_with_watchdog(batch).await;
            self.route.busy.store(false, Ordering::Release);
            self.route
                .last_active_ms
                .store(self.runtime.now_ms(), Ordering::Relaxed);
            if let Err(error) = result {
                self.receiver.close();
                self.fail_remaining(&error.to_string()).await;
                let _ = self.finish_chunks().await;
                return Err(error);
            }
            self.prepare_replacement().await;
        }
        self.finish_chunks().await
    }

    async fn write_batch_with_watchdog(&mut self, batch: Vec<PendingObject>) -> Result<()> {
        let object_count = batch.len();
        let logical_bytes: usize = batch.iter().map(|object| object.len).sum();
        let watchdog = self.runtime.policy.batch_watchdog;
        let metrics = Arc::clone(&self.runtime.metrics);
        let write = self.chunk.write_batch(batch, &metrics);
        tokio::pin!(write);
        let mut elapsed = Duration::ZERO;
        loop {
            tokio::select! {
                result = &mut write => return result,
                () = tokio::time::sleep(watchdog) => {
                    elapsed = elapsed.saturating_add(watchdog);
                    metrics
                        .batch_watchdog_expirations
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        object_count,
                        logical_bytes,
                        watchdog_ms = watchdog.as_millis(),
                        elapsed_ms = elapsed.as_millis(),
                        "small-write batch remains in flight after watchdog interval"
                    );
                }
            }
        }
    }

    fn note_dequeue(&self, object: &PendingObject) {
        self.route.dequeued(object.len, self.runtime.now_ms());
        let delay = u64::try_from(object.enqueued_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.runtime
            .metrics
            .queue_delay_ns
            .fetch_add(delay, Ordering::Relaxed);
        self.runtime
            .metrics
            .max_queue_delay_ns
            .fetch_max(delay, Ordering::Relaxed);
    }

    async fn ensure_object_fits(&mut self, object_len: usize) -> Result<()> {
        if self.chunk.remaining_in_chunk() < object_len as u64 {
            let replacement = match self.replacement.take() {
                Some(chunk) => chunk,
                None => {
                    OwnedChunk::allocate(&self.runtime, Arc::clone(&self.route.conversion_active)).await?
                }
            };
            self.chunk.finish().await?;
            self.chunk = replacement;
        }
        if self.chunk.remaining_in_strip() < object_len as u64 {
            if self.chunk.current_strip().is_ok() {
                self.chunk.close_strip(&self.runtime.metrics).await?;
            }
            self.chunk.ensure_strip().await?;
        }
        Ok(())
    }

    async fn prepare_replacement(&mut self) {
        if self.replacement.is_none()
            && self.chunk.remaining_in_chunk() < self.runtime.policy.object_limit as u64
        {
            self.replacement = OwnedChunk::allocate(&self.runtime, Arc::clone(&self.route.conversion_active))
                .await
                .ok();
        }
    }

    async fn finish_chunks(&mut self) -> Result<()> {
        let current_result = self.chunk.finish().await;
        let replacement_result = match self.replacement.as_mut() {
            Some(chunk) => chunk.finish().await,
            None => Ok(()),
        };
        current_result.and(replacement_result)
    }

    fn collect_batch(&mut self, first: PendingObject) -> Vec<PendingObject> {
        let mut bytes = first.len;
        let mut batch = vec![first];
        while batch.len() < self.runtime.policy.max_batch_objects
            && bytes < self.runtime.policy.max_batch_bytes
        {
            let Ok(next) = self.receiver.try_recv() else {
                break;
            };
            self.note_dequeue(&next);
            let candidate_bytes = bytes.saturating_add(next.len);
            let available = self
                .chunk
                .remaining_in_strip()
                .min(self.chunk.remaining_in_chunk());
            if candidate_bytes > self.runtime.policy.max_batch_bytes || candidate_bytes as u64 > available {
                self.carry = Some(next);
                break;
            }
            bytes = candidate_bytes;
            batch.push(next);
        }
        batch
    }

    async fn fail_remaining(&mut self, message: &str) {
        if let Some(object) = self.carry.take() {
            fail_one(object, message, &self.runtime.metrics);
        }
        while let Some(object) = self.receiver.recv().await {
            self.note_dequeue(&object);
            fail_one(object, message, &self.runtime.metrics);
        }
    }
}

fn fail_one(object: PendingObject, message: &str, metrics: &SmallWriteMetrics) {
    metrics.failed.fetch_add(1, Ordering::Relaxed);
    let _ = object
        .completion
        .send(Err(IoError::WriteFailed(message.to_string())));
}

/// Background advance RPC: sends `AdvanceChunkWriteRequest` and retries on
/// `MetadataConflict`. Returns the refreshed `Chunk` so the caller can apply
/// it when the future is awaited.
async fn advance_chunk(
    allocator: Arc<dyn ChunkAllocator>,
    chunk_id: ChunkId,
    writer_epoch: u64,
    mut modify_ts: u64,
    cursor: u64,
    closed_strip_sequence: Option<u32>,
    writer_lease_ms: u64,
) -> Result<Chunk> {
    for attempt in 0..8 {
        let response = allocator
            .advance_chunk_write(AdvanceChunkWriteRequest {
                chunk_id: Some(chunk_id),
                writer_epoch,
                expected_modify_ts: modify_ts,
                acknowledged_cursor: cursor,
                closed_strip_sequence,
                writer_lease_ms,
            })
            .await;
        match response {
            Ok(response) => {
                return response
                    .chunk
                    .ok_or_else(|| IoError::AllocationFailed("cursor advance returned no chunk".into()));
            }
            Err(IoError::MetadataConflict(_)) if attempt < 7 => {
                let response = allocator
                    .query_chunk(QueryChunkRequest {
                        chunk_id: Some(chunk_id),
                    })
                    .await?;
                let refreshed = response.chunk.ok_or_else(|| {
                    IoError::MetadataConflict("shared chunk disappeared during advance".into())
                })?;
                if refreshed.state != ChunkState::Active as i32 || refreshed.writer_epoch != writer_epoch {
                    return Err(IoError::MetadataConflict(
                        "shared chunk ownership changed during advance".into(),
                    ));
                }
                let strip_already_closed = closed_strip_sequence.map_or(true, |requested| {
                    refreshed
                        .closed_strip_sequence
                        .is_some_and(|actual| actual >= requested)
                });
                if refreshed.acknowledged_cursor >= cursor && strip_already_closed {
                    return Ok(refreshed);
                }
                modify_ts = refreshed.modify_ts;
            }
            Err(error) => return Err(error),
        }
    }
    Err(IoError::MetadataConflict(
        "shared chunk metadata kept changing during advance".into(),
    ))
}

#[allow(clippy::too_many_arguments)]
async fn confirm_reserved_strip(
    allocator: Arc<dyn ChunkAllocator>,
    chunk_id: ChunkId,
    group_id: ChunkId,
    writer_epoch: u64,
    lease_generation: u64,
    mut modify_ts: u64,
    strip_sequence: u32,
    cursor: u64,
    closed_strip_sequence: Option<u32>,
    writer_lease_ms: u64,
) -> Result<Chunk> {
    for attempt in 0..8 {
        let response = allocator
            .mutate_strip_reservation(MutateStripReservationRequest {
                chunk_id: Some(chunk_id),
                expected_modify_ts: modify_ts,
                group_id: Some(group_id),
                writer_epoch,
                lease_generation,
                strip_sequence,
                action: StripReservationAction::Confirm as i32,
                acknowledged_cursor: cursor,
                closed_strip_sequence,
                lease_ms: writer_lease_ms,
            })
            .await;
        match response {
            Ok(response) => {
                return response.chunk.ok_or_else(|| {
                    IoError::MetadataConflict("reservation confirmation returned no chunk".into())
                });
            }
            Err(IoError::MetadataConflict(_)) if attempt < 7 => {
                let response = allocator
                    .query_chunk(QueryChunkRequest {
                        chunk_id: Some(chunk_id),
                    })
                    .await?;
                let refreshed = response.chunk.ok_or_else(|| {
                    IoError::MetadataConflict("reserved chunk disappeared during confirmation".into())
                })?;
                if refreshed
                    .strips
                    .iter()
                    .any(|strip| strip.strip_sequence == strip_sequence)
                    && refreshed.acknowledged_cursor >= cursor
                {
                    return Ok(refreshed);
                }
                if refreshed.state != ChunkState::Active as i32 || refreshed.writer_epoch != writer_epoch {
                    return Err(IoError::MetadataConflict(
                        "reserved chunk ownership changed during confirmation".into(),
                    ));
                }
                modify_ts = refreshed.modify_ts;
            }
            Err(error) => return Err(error),
        }
    }
    Err(IoError::MetadataConflict(
        "reserved chunk metadata kept changing during confirmation".into(),
    ))
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn publish_reserved_conversion(
    allocator: Arc<dyn ChunkAllocator>,
    disk_writer: Arc<dyn DiskWriter>,
    pending_confirm: Option<tokio::task::JoinHandle<Result<Chunk>>>,
    group: PendingEcGroup,
    writer_epoch: u64,
    lease_generation: u64,
    writer_lease_ms: u64,
    cursor: u64,
) -> Result<Chunk> {
    let chunk = pending_confirm
        .ok_or_else(|| IoError::MetadataConflict("conversion has no final confirmation".into()))?
        .await
        .map_err(|join_error| {
            IoError::WriteFailed(format!("background confirmation task panicked: {join_error}"))
        })??;
    let group_id = group
        .reservation_group_id
        .ok_or_else(|| IoError::MetadataConflict("conversion reservation id is missing".into()))?;
    let first_sequence = group
        .old_strips
        .first()
        .map(|strip| strip.strip_sequence)
        .ok_or_else(|| IoError::Internal("conversion group is empty".into()))?;
    let closed_sequence = group.old_strips.last().map(|strip| strip.strip_sequence);
    let parity = group
        .parity
        .finish()
        .map_err(|error| IoError::EcEncodeFailed(error.to_string()))?;
    if group.parity_segments.len() != parity.len() {
        return Err(IoError::AllocationFailed(format!(
            "conversion reserved {} parity segments for {} shards",
            group.parity_segments.len(),
            parity.len()
        )));
    }
    let unit_bytes = u64::from(group.old_strips[0].unit_kb) * 1024;
    let mut parity_io_error = None;
    let mut parity_writes = tokio::task::JoinSet::new();
    for (segment, shard) in group.parity_segments.iter().zip(parity) {
        let disk_writer = Arc::clone(&disk_writer);
        let segment = *segment;
        let shard = Bytes::from(shard);
        parity_writes.spawn(async move {
            for (index, part) in shard.chunks(64 * 1024).enumerate() {
                let offset = u64::try_from(index).unwrap_or(u64::MAX).saturating_mul(64 * 1024);
                disk_writer
                    .write_priority_at_byte_offset(&segment, unit_bytes, offset, Bytes::copy_from_slice(part))
                    .await?;
            }
            Ok::<_, IoError>(())
        });
    }
    while let Some(result) = parity_writes.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(%error, "reserved parity write failed");
                parity_io_error.get_or_insert_with(|| error.to_string());
            }
            Err(join_error) => {
                parity_io_error.get_or_insert_with(|| format!("parity writer failed: {join_error}"));
            }
        }
    }
    let chunk_id = chunk
        .id
        .ok_or_else(|| IoError::MetadataConflict("conversion chunk id is missing".into()))?;
    let current_old_strips = group
        .old_strips
        .iter()
        .map(|old| {
            chunk
                .strips
                .iter()
                .find(|current| current.strip_sequence == old.strip_sequence)
                .cloned()
                .ok_or_else(|| IoError::MetadataConflict("conversion source disappeared".into()))
        })
        .collect::<Result<Vec<_>>>()?;
    if parity_io_error.is_none() {
        let mut parity_fsyncs = tokio::task::JoinSet::new();
        for segment in &group.parity_segments {
            let disk_writer = Arc::clone(&disk_writer);
            let segment = *segment;
            parity_fsyncs.spawn(async move { disk_writer.fsync_priority(&segment).await });
        }
        while let Some(result) = parity_fsyncs.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => parity_io_error = Some(error.to_string()),
                Err(join_error) => {
                    parity_io_error = Some(format!("parity fsync failed: {join_error}"));
                }
            }
        }
    }
    if parity_io_error.is_some() {
        tracing::warn!(
            ?chunk_id,
            "admitting conversion fallback after parity I/O failure"
        );
        admit_conversion_fallback(
            &allocator,
            &chunk,
            &current_old_strips,
            writer_epoch,
            writer_lease_ms,
        )
        .await?;
        return Ok(chunk);
    }
    let response = allocator
        .mutate_strip_reservation(MutateStripReservationRequest {
            chunk_id: Some(chunk_id),
            expected_modify_ts: chunk.modify_ts,
            group_id: Some(group_id),
            writer_epoch,
            lease_generation,
            strip_sequence: first_sequence,
            action: StripReservationAction::Publish as i32,
            acknowledged_cursor: cursor,
            closed_strip_sequence: closed_sequence,
            lease_ms: writer_lease_ms,
        })
        .await;
    match response {
        Ok(response) => response
            .chunk
            .ok_or_else(|| IoError::MetadataConflict("conversion publication returned no chunk".into())),
        Err(publish_error) => {
            let start_index = chunk
                .strips
                .iter()
                .position(|strip| strip.strip_sequence == first_sequence)
                .and_then(|index| u32::try_from(index).ok())
                .ok_or_else(|| IoError::MetadataConflict("conversion source disappeared".into()))?;
            let data_num = u32::try_from(current_old_strips.len()).unwrap_or(u32::MAX);
            let code_num = u32::try_from(group.parity_segments.len()).unwrap_or(u32::MAX);
            allocator
                .prepare_mirror_to_ec_conversion(PrepareMirrorToEcConversionRequest {
                    chunk_id: Some(chunk_id),
                    expected_modify_ts: chunk.modify_ts,
                    start_index,
                    old_strips: current_old_strips,
                    data_num,
                    code_num,
                    client_owner: writer_epoch,
                    claim_lease_ms: writer_lease_ms,
                })
                .await
                .map_err(|task_error| {
                    IoError::MetadataConflict(format!(
                        "optimal conversion publication failed ({publish_error}); durable fallback failed ({task_error})"
                    ))
                })?;
            Ok(chunk)
        }
    }
}

async fn admit_conversion_fallback(
    allocator: &Arc<dyn ChunkAllocator>,
    chunk: &Chunk,
    old_strips: &[ChunkStrip],
    writer_epoch: u64,
    writer_lease_ms: u64,
) -> Result<()> {
    let chunk_id = chunk
        .id
        .ok_or_else(|| IoError::MetadataConflict("conversion chunk id is missing".into()))?;
    let first_sequence = old_strips
        .first()
        .map(|strip| strip.strip_sequence)
        .ok_or_else(|| IoError::MetadataConflict("conversion source is empty".into()))?;
    let start_index = chunk
        .strips
        .iter()
        .position(|strip| strip.strip_sequence == first_sequence)
        .and_then(|index| u32::try_from(index).ok())
        .ok_or_else(|| IoError::MetadataConflict("conversion source disappeared".into()))?;
    allocator
        .prepare_mirror_to_ec_conversion(PrepareMirrorToEcConversionRequest {
            chunk_id: Some(chunk_id),
            expected_modify_ts: chunk.modify_ts,
            start_index,
            old_strips: old_strips.to_vec(),
            data_num: u32::try_from(old_strips.len()).unwrap_or(u32::MAX),
            code_num: 4,
            client_owner: writer_epoch,
            claim_lease_ms: writer_lease_ms,
        })
        .await?;
    Ok(())
}

async fn append_mirror_strips(
    allocator: &dyn ChunkAllocator,
    mut chunk: Chunk,
    strip_count: u32,
    copy_count: u32,
) -> Result<Chunk> {
    let chunk_id = chunk
        .id
        .ok_or_else(|| IoError::AllocationFailed("shared chunk missing id".into()))?;
    let unit_count = chunk.strips.last().map_or(1, |last| {
        last.capacity.checked_div(last.unit_kb).unwrap_or(0).max(1)
    });
    for _ in 0..8 {
        let response = allocator
            .append_chunk(AppendChunkRequest {
                chunk_id: Some(chunk_id),
                modify_ts: chunk.modify_ts,
                strip_size: unit_count,
                strip_count,
                strip_type: StripType::Mirror as i32,
                data_num: 0,
                code_num: 0,
                copy_count,
            })
            .await?;
        if let Some(refreshed) = response.chunk {
            if refreshed.id != Some(chunk_id) {
                return Err(IoError::AllocationFailed(
                    "append_chunk refresh returned a different shared chunk".into(),
                ));
            }
            chunk = refreshed;
            continue;
        }
        if response.strips.is_empty() {
            return Err(IoError::AllocationFailed(
                "append_chunk response missing prefetched mirror strips".into(),
            ));
        }
        chunk.modify_ts = response.modify_ts;
        chunk.capacity = chunk
            .capacity
            .saturating_add(response.strips.iter().map(|strip| strip.capacity).sum::<u32>());
        chunk.strips.extend(response.strips);
        return Ok(chunk);
    }
    Err(IoError::MetadataConflict(
        "shared chunk metadata kept changing during strip prefetch".into(),
    ))
}

#[allow(clippy::too_many_arguments)]
async fn close_and_prefetch(
    allocator: Arc<dyn ChunkAllocator>,
    pending: Option<tokio::task::JoinHandle<Result<Chunk>>>,
    mut chunk: Chunk,
    writer_epoch: u64,
    cursor: u64,
    closed_strip_sequence: u32,
    writer_lease_ms: u64,
    prefetch_count: u32,
    copy_count: u32,
    chunk_capacity: u64,
) -> Result<Chunk> {
    if let Some(pending) = pending {
        chunk = pending.await.map_err(|join_error| {
            IoError::WriteFailed(format!("background advance task panicked: {join_error}"))
        })??;
    }
    let chunk_id = chunk
        .id
        .ok_or_else(|| IoError::AllocationFailed("shared chunk missing id".into()))?;
    let already_closed = chunk.acknowledged_cursor >= cursor
        && chunk
            .closed_strip_sequence
            .is_some_and(|sequence| sequence >= closed_strip_sequence);
    if !already_closed {
        chunk = advance_chunk(
            Arc::clone(&allocator),
            chunk_id,
            writer_epoch,
            chunk.modify_ts,
            cursor,
            Some(closed_strip_sequence),
            writer_lease_ms,
        )
        .await?;
    }
    let strip_bytes = chunk
        .strips
        .last()
        .map_or(0, |strip| u64::from(strip.capacity) * 1024);
    if strip_bytes == 0 {
        return Err(IoError::AllocationFailed(
            "prefetched strip has zero capacity".into(),
        ));
    }
    let attached_bytes = u64::from(chunk.capacity) * 1024;
    let strips_ahead = attached_bytes.saturating_sub(cursor) / strip_bytes;
    let available_slots = chunk_capacity.saturating_sub(attached_bytes) / strip_bytes;
    if strips_ahead > u64::from(prefetch_count / 2) || available_slots == 0 {
        return Ok(chunk);
    }
    let strip_count = prefetch_count.min(u32::try_from(available_slots).unwrap_or(u32::MAX));
    append_mirror_strips(&*allocator, chunk, strip_count, copy_count).await
}

struct OwnedChunk {
    allocator: Arc<dyn ChunkAllocator>,
    disk_writer: Arc<dyn DiskWriter>,
    policy: Arc<SmallWritePolicy>,
    chunk: Chunk,
    cursor: u64,
    writer_epoch: u64,
    shadow: Option<BytesMut>,
    failed_disks: Arc<FailedDiskList>,
    metrics: Arc<SmallWriteMetrics>,
    budget: Arc<tokio::sync::Semaphore>,
    conversion_active: Arc<AtomicBool>,
    conversion_group: Option<PendingEcGroup>,
    pending_conversion_update: Option<tokio::task::JoinHandle<Result<PendingEcGroup>>>,
    pending_advance: Option<tokio::task::JoinHandle<Result<Chunk>>>,
    reservation_mode: bool,
    reservation_group_id: Option<ChunkId>,
    reservation_generation: u64,
    reserved_strips: VecDeque<ChunkStrip>,
    active_reservation: Option<(ChunkId, u64, u32)>,
    staged_reservation: Option<(ChunkId, u64, u32)>,
    reservation_first_sequence: Option<u32>,
    reservation_parity_segments: Vec<Segment>,
    owns_conversion_gate: bool,
}

struct PendingEcGroup {
    old_strips: Vec<crowdb_protocol::chunkdb::rpc::ChunkStrip>,
    parity: IncrementalParity,
    reservation_group_id: Option<ChunkId>,
    parity_segments: Vec<Segment>,
    _budget: OwnedSemaphorePermit,
}

struct MirrorBatchStats {
    object_count: usize,
    buffer_count: usize,
    logical_bytes: usize,
}

impl OwnedChunk {
    async fn allocate(runtime: &SmallPoolRuntime, conversion_active: Arc<AtomicBool>) -> Result<Self> {
        let writer_epoch = next_writer_epoch();
        let lease_ms = u64::try_from(runtime.policy.writer_lease.as_millis()).unwrap_or(u64::MAX);
        let initial_strip_count = u32::from(!runtime.policy.conversion_enabled);
        let response = runtime
            .allocator
            .allocate_chunk(AllocateChunkRequest {
                chunk_id: None,
                write_granularity: 1024,
                strip_count: initial_strip_count,
                strip_type: StripType::Mirror as i32,
                data_num: 0,
                code_num: 0,
                copy_count: runtime.policy.mirror_copies,
                chunk_type: ChunkType::Repo as i32,
                writer_epoch,
                writer_lease_ms: lease_ms,
            })
            .await?;
        let chunk = response
            .chunk
            .ok_or_else(|| IoError::AllocationFailed("shared chunk allocation returned no chunk".into()))?;
        let mut owned = Self {
            allocator: Arc::clone(&runtime.allocator),
            disk_writer: Arc::clone(&runtime.disk_writer),
            policy: Arc::clone(&runtime.policy),
            chunk,
            cursor: 0,
            writer_epoch,
            shadow: None,
            failed_disks: Arc::clone(&runtime.failed_disks),
            metrics: Arc::clone(&runtime.metrics),
            budget: Arc::clone(&runtime.conversion_budget),
            conversion_active,
            conversion_group: None,
            pending_conversion_update: None,
            pending_advance: None,
            reservation_mode: true,
            reservation_group_id: None,
            reservation_generation: 1,
            reserved_strips: VecDeque::new(),
            active_reservation: None,
            staged_reservation: None,
            reservation_first_sequence: None,
            reservation_parity_segments: Vec::new(),
            owns_conversion_gate: false,
        };
        if let Err(error) = owned.reserve_more().await {
            tracing::warn!(%error, "reserved-strip prefetch unavailable; using attached strips");
            owned.reservation_mode = false;
            let desired = runtime
                .policy
                .small_strip_prefetch_count
                .saturating_sub(initial_strip_count);
            if desired > 0 {
                owned.chunk = append_mirror_strips(
                    &*owned.allocator,
                    owned.chunk,
                    desired,
                    runtime.policy.mirror_copies,
                )
                .await?;
            }
        }
        if owned.current_strip().is_err() {
            owned.stage_reserved_strip()?;
        }
        Ok(owned)
    }

    fn remaining_in_chunk(&self) -> u64 {
        self.policy.chunk_capacity.saturating_sub(self.cursor)
    }

    fn current_strip(&self) -> Result<&crowdb_protocol::chunkdb::rpc::ChunkStrip> {
        self.chunk
            .strips
            .iter()
            .find(|strip| {
                let start = u64::from(strip.chunk_offset) * 1024;
                let end = start + u64::from(strip.capacity) * 1024;
                start <= self.cursor && self.cursor < end
            })
            .ok_or_else(|| {
                IoError::AllocationFailed(format!(
                    "shared chunk has no strip at cursor {} ({} strips, capacity {} KiB)",
                    self.cursor,
                    self.chunk.strips.len(),
                    self.chunk.capacity
                ))
            })
    }

    fn remaining_in_strip(&self) -> u64 {
        self.current_strip().map_or(0, |strip| {
            let end = u64::from(strip.chunk_offset.saturating_add(strip.capacity)) * 1024;
            end.saturating_sub(self.cursor)
        })
    }

    async fn ensure_strip(&mut self) -> Result<()> {
        self.refresh_pending_advance().await?;
        if self.current_strip().is_ok() {
            return Ok(());
        }
        self.flush_pending_advance().await?;
        if self.current_strip().is_ok() {
            return Ok(());
        }
        if self.reservation_mode {
            if self.reserved_strips.is_empty() {
                self.reserve_more().await?;
            }
            self.stage_reserved_strip()?;
            return Ok(());
        }
        Err(IoError::AllocationFailed(
            "strip prefetch did not stay ahead of the write cursor".into(),
        ))
    }

    fn stage_reserved_strip(&mut self) -> Result<()> {
        let strip = self
            .reserved_strips
            .pop_front()
            .ok_or_else(|| IoError::AllocationFailed("reservation prefetch returned no strips".into()))?;
        let group_id = self
            .reservation_group_id
            .ok_or_else(|| IoError::Internal("reservation group id is missing".into()))?;
        self.staged_reservation = Some((group_id, self.reservation_generation, strip.strip_sequence));
        self.chunk.strips.push(strip);
        Ok(())
    }

    async fn consume_staged_reservation(&mut self) -> Result<()> {
        let Some((group_id, generation, sequence)) = self.staged_reservation.take() else {
            return Ok(());
        };
        self.mutate_reservation(
            group_id,
            generation,
            sequence,
            StripReservationAction::Consume,
            self.chunk.acknowledged_cursor,
            None,
        )
        .await?;
        self.active_reservation = Some((group_id, generation, sequence));
        Ok(())
    }

    async fn reserve_more(&mut self) -> Result<()> {
        if !self.reserved_strips.is_empty()
            || self.active_reservation.is_some()
            || self.staged_reservation.is_some()
        {
            return Ok(());
        }
        let last = self.chunk.strips.last();
        let strip_kb = u64::from(last.map_or(1024, |strip| match strip.strip.as_ref() {
            Some(Strip::EcStrip(ec)) => strip.capacity.checked_div(ec.data_num).unwrap_or(0),
            Some(Strip::MirrorStrip(_)) | None => strip.capacity,
        }));
        let remaining_kb = self
            .policy
            .chunk_capacity
            .saturating_sub(u64::from(self.chunk.capacity) * 1024)
            / 1024;
        let ordinary_count = self
            .policy
            .small_strip_prefetch_count
            .min(u32::try_from(remaining_kb / strip_kb).unwrap_or(u32::MAX));
        let conversion_width = u32::try_from(self.policy.conversion_data_num).unwrap_or(u32::MAX);
        let conversion_fits = u64::from(conversion_width).saturating_mul(strip_kb) <= remaining_kb;
        let acquired_conversion_gate = self.policy.conversion_enabled
            && conversion_fits
            && !self.owns_conversion_gate
            && self
                .conversion_active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
        self.owns_conversion_gate |= acquired_conversion_gate;
        let conversion_group = self.policy.conversion_enabled && conversion_fits && self.owns_conversion_gate;
        let strip_count = if conversion_group {
            conversion_width
        } else {
            ordinary_count
        };
        if strip_count == 0 {
            return Ok(());
        }
        let chunk_id = self
            .chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("shared chunk missing id".into()))?;
        let group_id = generate_chunk_id(CHUNK_TYPE_REPO).to_proto();
        let unit_count = last.map_or(1, |strip| {
            u32::try_from(strip_kb)
                .unwrap_or(u32::MAX)
                .checked_div(strip.unit_kb)
                .unwrap_or(0)
                .max(1)
        });
        let response = self
            .allocator
            .reserve_strip_group(ReserveStripGroupRequest {
                chunk_id: Some(chunk_id),
                expected_modify_ts: self.chunk.modify_ts,
                group_id: Some(group_id),
                writer_epoch: self.writer_epoch,
                lease_generation: self.reservation_generation,
                lease_ms: u64::try_from(self.policy.writer_lease.as_millis()).unwrap_or(u64::MAX),
                strip_size: unit_count,
                strip_count,
                copy_count: self.policy.mirror_copies,
                conversion_data_num: if conversion_group { conversion_width } else { 0 },
                conversion_code_num: if conversion_group {
                    u32::try_from(self.policy.conversion_code_num).unwrap_or(u32::MAX)
                } else {
                    0
                },
            })
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                if acquired_conversion_gate {
                    self.owns_conversion_gate = false;
                    self.conversion_active.store(false, Ordering::Release);
                }
                return Err(error);
            }
        };
        self.chunk = response
            .chunk
            .ok_or_else(|| IoError::AllocationFailed("reservation response missing chunk".into()))?;
        let group = response
            .group
            .ok_or_else(|| IoError::AllocationFailed("reservation response missing group".into()))?;
        self.reservation_group_id = Some(group_id);
        self.reservation_first_sequence = group.strips.first().map(|strip| strip.strip_sequence);
        self.reservation_parity_segments = group.parity_segments;
        self.reserved_strips = group.strips.into();
        Ok(())
    }

    async fn mutate_reservation(
        &self,
        group_id: ChunkId,
        generation: u64,
        strip_sequence: u32,
        action: StripReservationAction,
        cursor: u64,
        closed_strip_sequence: Option<u32>,
    ) -> Result<Option<Chunk>> {
        let chunk_id = self
            .chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("shared chunk missing id".into()))?;
        let response = self
            .allocator
            .mutate_strip_reservation(MutateStripReservationRequest {
                chunk_id: Some(chunk_id),
                expected_modify_ts: self.chunk.modify_ts,
                group_id: Some(group_id),
                writer_epoch: self.writer_epoch,
                lease_generation: generation,
                strip_sequence,
                action: action as i32,
                acknowledged_cursor: cursor,
                closed_strip_sequence,
                lease_ms: u64::try_from(self.policy.writer_lease.as_millis()).unwrap_or(u64::MAX),
            })
            .await?;
        Ok(response.chunk)
    }

    async fn close_strip(&mut self, metrics: &SmallWriteMetrics) -> Result<()> {
        let strip = self.current_strip()?.clone();
        let strip_end = u64::from(strip.chunk_offset.saturating_add(strip.capacity)) * 1024;
        let tail = strip_end.saturating_sub(self.cursor);
        if tail > 0 {
            metrics.tail_waste_bytes.fetch_add(tail, Ordering::Relaxed);
        }
        self.cursor = strip_end;
        let closed = self
            .chunk
            .strips
            .iter()
            .find(|current| current.strip_sequence == strip.strip_sequence)
            .cloned()
            .ok_or_else(|| IoError::MetadataConflict("closed mirror strip disappeared".into()))?;
        self.schedule_closed_advance(strip_end, strip.strip_sequence);
        if let Err(error) = self.retain_closed_strip(closed).await {
            tracing::warn!(%error, "mirror-to-EC fast path deferred to chunkdb");
        }
        Ok(())
    }

    async fn write_batch(&mut self, batch: Vec<PendingObject>, metrics: &SmallWriteMetrics) -> Result<()> {
        match self.try_write_batch(&batch, metrics).await {
            Ok(locations) => {
                for (object, location) in batch.into_iter().zip(locations) {
                    metrics.completed.fetch_add(1, Ordering::Relaxed);
                    let _ = object.completion.send(Ok(vec![location]));
                }
                Ok(())
            }
            Err(error) => {
                let message = error.to_string();
                for object in batch {
                    fail_one(object, &message, metrics);
                }
                Err(error)
            }
        }
    }

    async fn try_write_batch(
        &mut self,
        batch: &[PendingObject],
        metrics: &SmallWriteMetrics,
    ) -> Result<Vec<Location>> {
        self.consume_staged_reservation().await?;
        let strip = self.current_strip()?.clone();
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        let (logical_bytes, buffer_count) = batch_shape(batch);
        if logical_bytes as u64 > self.remaining_in_strip()
            || logical_bytes as u64 > self.remaining_in_chunk()
        {
            return Err(IoError::Internal(
                "assembled batch crosses mirror strip or chunk".into(),
            ));
        }
        let start = self.cursor;
        let strip_bytes = usize::try_from(strip.capacity)
            .unwrap_or(usize::MAX)
            .saturating_mul(1024);
        let strip_start = u64::from(strip.chunk_offset) * 1024;
        let block_offset = start.saturating_sub(strip_start);
        let block_offset_us = usize::try_from(block_offset).unwrap_or(usize::MAX);

        // Single shadow buffer: allocated once with full strip capacity, no
        // zeroing. Fragments are copied in sequentially; each batch sends a
        // view (slice) of the written portion, not the whole buffer.
        let mut shadow = self.take_shadow(strip_bytes, block_offset_us);

        let chunk_id = self
            .chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("shared chunk missing id".into()))?;
        let mut copied = 0usize;
        let mut locations = Vec::with_capacity(batch.len());
        for object in batch {
            let object_start = copied;
            for fragment in &object.fragments {
                shadow.extend_from_slice(fragment);
                copied += fragment.len();
            }
            locations.push(Location {
                chunk_id: Some(chunk_id),
                offset: start + object_start as u64,
                length: object.len as u64,
                logical_offset: 0,
                logical_length: object.len as u64,
            });
        }
        let written_end = block_offset_us + logical_bytes;
        debug_assert_eq!(shadow.len(), written_end);

        // Freeze the buffer, take a view of the written portion, and send
        // views to mirrors. After all mirrors complete, reclaim the buffer.
        let frozen = shadow.freeze();
        let view = frozen.slice(block_offset_us..written_end);
        let full_image = frozen.slice(0..written_end);
        let (_, write_result) = self
            .write_mirrors_with_repair(
                &strip,
                view,
                full_image,
                unit_bytes,
                block_offset,
                MirrorBatchStats {
                    object_count: batch.len(),
                    buffer_count,
                    logical_bytes,
                },
            )
            .await;
        self.shadow = Some(
            frozen
                .try_into_mut()
                .unwrap_or_else(|shared| BytesMut::from(shared.as_ref())),
        );
        write_result?;
        let end = start + logical_bytes as u64;
        let strip_end = u64::from(strip.chunk_offset.saturating_add(strip.capacity)) * 1024;
        let closed = (end == strip_end).then_some(strip.strip_sequence);
        self.cursor = end;
        if let Some(sequence) = closed {
            let closed_strip = self
                .chunk
                .strips
                .iter()
                .find(|current| current.strip_sequence == sequence)
                .cloned()
                .ok_or_else(|| IoError::MetadataConflict("closed mirror strip disappeared".into()))?;
            self.schedule_closed_advance(end, sequence);
            if let Err(error) = self.retain_closed_strip(closed_strip).await {
                tracing::warn!(%error, "mirror-to-EC fast path deferred to chunkdb");
            }
        } else {
            self.refresh_pending_advance().await?;
            self.start_pending_advance(end)?;
        }
        metrics.batches.fetch_add(1, Ordering::Relaxed);
        metrics
            .batch_objects
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        metrics
            .max_batch_objects
            .fetch_max(batch.len() as u64, Ordering::Relaxed);
        metrics
            .batch_bytes
            .fetch_add(logical_bytes as u64, Ordering::Relaxed);
        metrics
            .max_batch_bytes
            .fetch_max(logical_bytes as u64, Ordering::Relaxed);
        Ok(locations)
    }

    fn take_shadow(&mut self, strip_bytes: usize, block_offset: usize) -> BytesMut {
        let mut shadow = if let Some(shadow) = self.shadow.take() {
            shadow
        } else {
            self.metrics
                .shadow_bytes
                .fetch_add(strip_bytes as u64, Ordering::Relaxed);
            BytesMut::with_capacity(strip_bytes)
        };
        if shadow.len() < block_offset {
            let previous = shadow.len();
            shadow.resize(block_offset, 0);
            self.metrics
                .shadow_bytes
                .fetch_add((block_offset - previous) as u64, Ordering::Relaxed);
        }
        shadow
    }

    #[allow(clippy::too_many_lines)]
    async fn retain_closed_strip(&mut self, strip: crowdb_protocol::chunkdb::rpc::ChunkStrip) -> Result<()> {
        let mut shadow = self
            .shadow
            .take()
            .ok_or_else(|| IoError::Internal("closed mirror strip has no retained image".into()))?;
        self.metrics
            .shadow_bytes
            .fetch_sub(shadow.capacity() as u64, Ordering::Relaxed);
        if !self.policy.conversion_enabled {
            return Ok(());
        }
        let strip_bytes = usize::try_from(strip.capacity)
            .unwrap_or(usize::MAX)
            .saturating_mul(1024);
        if shadow.len() > strip_bytes {
            return Err(IoError::Internal(
                "retained mirror image exceeds strip capacity".into(),
            ));
        }
        shadow.resize(strip_bytes, 0);
        let image = shadow.freeze();

        if image.is_empty() {
            return Err(IoError::Internal(
                "closed mirror strip has an empty retained image".into(),
            ));
        }
        self.resolve_conversion_update().await?;
        if self.conversion_group.is_none() {
            let Some(first_sequence) = self.reservation_first_sequence else {
                return Ok(());
            };
            if strip.strip_sequence != first_sequence || self.reservation_parity_segments.is_empty() {
                return Ok(());
            }
            let scheme = EcScheme::new(self.policy.conversion_data_num, self.policy.conversion_code_num);
            let bytes = scheme
                .code_num
                .checked_mul(image.len())
                .ok_or(IoError::MemoryBudgetExhausted)?;
            let permits = u32::try_from(bytes).map_err(|_| IoError::MemoryBudgetExhausted)?;
            if bytes > self.policy.memory_budget / 2 {
                tracing::warn!(
                    parity_bytes = bytes,
                    strip_bytes = image.len(),
                    memory_budget = self.policy.memory_budget,
                    "conversion group exceeds reserved memory"
                );
                return Err(IoError::MemoryBudgetExhausted);
            }
            let budget = Arc::clone(&self.budget)
                .acquire_many_owned(permits)
                .await
                .map_err(|_| IoError::MemoryBudgetExhausted)?;
            self.conversion_group = Some(PendingEcGroup {
                old_strips: Vec::with_capacity(scheme.data_num),
                parity: IncrementalParity::new(scheme)
                    .map_err(|error| IoError::EcEncodeFailed(error.to_string()))?,
                reservation_group_id: self.reservation_group_id,
                parity_segments: self.reservation_parity_segments.clone(),
                _budget: budget,
            });
        }
        let mut group = self
            .conversion_group
            .take()
            .unwrap_or_else(|| unreachable!("conversion group initialized"));
        let completes_group = group.old_strips.len().saturating_add(1) == self.policy.conversion_data_num;
        let update = tokio::task::spawn_blocking(move || -> Result<PendingEcGroup> {
            group
                .parity
                .push(&image)
                .map_err(|error| IoError::EcEncodeFailed(error.to_string()))?;
            group.old_strips.push(strip);
            Ok(group)
        });
        if completes_group {
            let pending_confirm = self.pending_advance.take();
            let allocator = Arc::clone(&self.allocator);
            let disk_writer = Arc::clone(&self.disk_writer);
            let conversion_active = Arc::clone(&self.conversion_active);
            let writer_epoch = self.writer_epoch;
            let lease_generation = self.reservation_generation;
            let writer_lease_ms = u64::try_from(self.policy.writer_lease.as_millis()).unwrap_or(u64::MAX);
            let cursor = self.cursor;
            self.pending_advance = Some(tokio::spawn(async move {
                let result = async {
                    let group = update.await.map_err(|join_error| {
                        IoError::EcEncodeFailed(format!("parity worker failed: {join_error}"))
                    })??;
                    publish_reserved_conversion(
                        allocator,
                        disk_writer,
                        pending_confirm,
                        group,
                        writer_epoch,
                        lease_generation,
                        writer_lease_ms,
                        cursor,
                    )
                    .await
                }
                .await;
                conversion_active.store(false, Ordering::Release);
                result
            }));
            self.owns_conversion_gate = false;
            self.reservation_group_id = None;
            self.reservation_first_sequence = None;
            self.reservation_parity_segments.clear();
        } else {
            self.pending_conversion_update = Some(update);
        }
        Ok(())
    }

    async fn resolve_conversion_update(&mut self) -> Result<()> {
        let Some(update) = self.pending_conversion_update.take() else {
            return Ok(());
        };
        match update.await {
            Ok(Ok(group)) => {
                self.conversion_group = Some(group);
                Ok(())
            }
            Ok(Err(error)) => {
                self.owns_conversion_gate = false;
                self.conversion_active.store(false, Ordering::Release);
                Err(error)
            }
            Err(join_error) => {
                self.owns_conversion_gate = false;
                self.conversion_active.store(false, Ordering::Release);
                Err(IoError::EcEncodeFailed(format!(
                    "parity worker failed: {join_error}"
                )))
            }
        }
    }

    async fn write_mirrors_with_repair(
        &mut self,
        strip: &crowdb_protocol::chunkdb::rpc::ChunkStrip,
        data: Bytes,
        full_image: Bytes,
        unit_bytes: u64,
        block_offset: u64,
        stats: MirrorBatchStats,
    ) -> (Bytes, Result<()>) {
        let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
            return (
                data,
                Err(IoError::Internal("shared chunk strip is not mirrored".into())),
            );
        };
        let request_count = mirror.segments.len() as u64;
        self.metrics.record_aggregate_write(
            request_count,
            stats.object_count,
            stats.buffer_count,
            stats.logical_bytes,
            data.len(),
        );
        let mut write_tasks = tokio::task::JoinSet::new();
        for segment in &mirror.segments {
            let segment = *segment;
            let disk_writer = Arc::clone(&self.disk_writer);
            let data = data.clone();
            write_tasks.spawn(async move {
                (
                    segment,
                    disk_writer
                        .write_at_byte_offset(&segment, unit_bytes, block_offset, data)
                        .await,
                )
            });
        }
        let mut failed = Vec::new();
        while let Some(result) = write_tasks.join_next().await {
            let (segment, result) = match result {
                Ok(result) => result,
                Err(error) => {
                    return (
                        data,
                        Err(IoError::WriteFailed(format!(
                            "mirror writer task failed: {error}"
                        ))),
                    );
                }
            };
            if result.is_err() {
                failed.push(segment);
            }
        }
        for segment in failed {
            // Repair writes the full shadow image (offset 0 to written_end)
            // to the replacement segment, starting at offset 0.
            if let Err(error) = self
                .repair_replica(strip.strip_sequence, segment, full_image.clone(), unit_bytes, 0)
                .await
            {
                return (data, Err(error));
            }
        }
        (data, Ok(()))
    }

    async fn repair_replica(
        &mut self,
        strip_sequence: u32,
        failed: Segment,
        image: Bytes,
        unit_bytes: u64,
        block_offset: u64,
    ) -> Result<()> {
        let _repair = RepairMetricGuard::new(Arc::clone(&self.metrics));
        self.try_repair_replica(strip_sequence, failed, image, unit_bytes, block_offset)
            .await
    }

    #[allow(clippy::too_many_lines)]
    async fn try_repair_replica(
        &mut self,
        strip_sequence: u32,
        failed: Segment,
        image: Bytes,
        unit_bytes: u64,
        block_offset: u64,
    ) -> Result<()> {
        self.flush_pending_advance().await?;
        let failed_disk = failed
            .disk_id
            .ok_or_else(|| IoError::Internal("failed mirror segment has no disk id".into()))?;
        self.failed_disks.insert(failed_disk);
        let mut last_error = "no replacement attempt completed".to_string();
        for _ in 0..self.policy.repair_attempts_per_replica {
            self.metrics.repair_attempts.fetch_add(1, Ordering::Relaxed);
            let strip_index = self
                .chunk
                .strips
                .iter()
                .position(|strip| strip.strip_sequence == strip_sequence)
                .ok_or_else(|| IoError::MetadataConflict("mirror strip disappeared during repair".into()))?;
            let old_strip = self.chunk.strips[strip_index].clone();
            let Some(Strip::MirrorStrip(mut mirror)) = old_strip.strip.clone() else {
                return Err(IoError::MetadataConflict(
                    "mirror strip changed type during repair".into(),
                ));
            };
            let survivors: Vec<_> = mirror
                .segments
                .iter()
                .copied()
                .filter(|segment| *segment != failed)
                .collect();
            let excluded = self.failed_disks.live();
            self.metrics
                .negative_list_hits
                .fetch_add(excluded.len() as u64, Ordering::Relaxed);
            let response = self
                .allocator
                .allocate_replacement_segment(AllocateReplacementSegmentRequest {
                    chunk_id: self.chunk.id,
                    old_segment: Some(failed),
                    surviving_segments: survivors,
                    exclude_disk_ids: excluded,
                })
                .await;
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    last_error = error.to_string();
                    continue;
                }
            };
            let Some(replacement) = response.segment else {
                last_error = "replacement allocation returned no segment".into();
                continue;
            };
            if let Err(error) = self
                .disk_writer
                .write_at_byte_offset(&replacement, unit_bytes, block_offset, image.clone())
                .await
            {
                last_error = error.to_string();
                if let Some(disk) = replacement.disk_id {
                    self.failed_disks.insert(disk);
                }
                self.discard_replacement(replacement).await;
                continue;
            }
            let Some(slot) = mirror.segments.iter_mut().find(|segment| **segment == failed) else {
                self.discard_replacement(replacement).await;
                return Err(IoError::MetadataConflict(
                    "failed segment no longer belongs to strip".into(),
                ));
            };
            *slot = replacement;
            let mut new_strip = old_strip.clone();
            new_strip.strip = Some(Strip::MirrorStrip(mirror));
            let operation_id = crowdb_protocol::common::ChunkId {
                high: self.writer_epoch ^ self.chunk.modify_ts,
                low: u64::from(strip_sequence) ^ failed.unit_offset,
            };
            let request = ReplaceChunkStripRangeRequest {
                chunk_id: self.chunk.id,
                expected_modify_ts: self.chunk.modify_ts,
                start_index: u32::try_from(strip_index).unwrap_or(u32::MAX),
                old_strips: vec![old_strip],
                replacement_strips: vec![new_strip.clone()],
                operation_id: Some(operation_id),
            };
            match self.install_replacement(request, replacement).await {
                Ok(chunk) => {
                    if chunk
                        .strips
                        .iter()
                        .any(|strip| strip.strip_sequence == strip_sequence)
                    {
                        self.chunk = chunk;
                    } else {
                        self.chunk.strips[strip_index] = new_strip;
                        self.chunk.modify_ts = chunk.modify_ts;
                        self.chunk.cleanup_intents = chunk.cleanup_intents;
                        self.chunk.last_strip_replacement = chunk.last_strip_replacement;
                    }
                    self.metrics.repaired_replicas.fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .repairs_avoiding_rotation
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                Err(error @ IoError::MetadataConflict(_)) => return Err(error),
                Err(_) => {}
            }
            // The replacement may already be installed after an ambiguous
            // response, so never allocate or discard a different candidate.
            break;
        }
        self.metrics.exhausted_repairs.fetch_add(1, Ordering::Relaxed);
        self.mark_replica_unavailable(strip_sequence, failed).await?;
        Err(IoError::WriteFailed(format!(
            "mirror replica repair exhausted: {last_error}"
        )))
    }

    async fn install_replacement(
        &self,
        request: ReplaceChunkStripRangeRequest,
        replacement: Segment,
    ) -> Result<Chunk> {
        for attempt in 0..self.policy.repair_attempts_per_replica {
            if attempt > 0 {
                self.metrics.repair_attempts.fetch_add(1, Ordering::Relaxed);
            }
            match self.allocator.replace_chunk_strip_range(request.clone()).await {
                Ok(response) => {
                    return response.chunk.ok_or_else(|| {
                        IoError::MetadataConflict("range replacement returned no chunk".into())
                    });
                }
                Err(error @ IoError::MetadataConflict(_)) => {
                    self.discard_replacement(replacement).await;
                    return Err(error);
                }
                Err(_) => {}
            }
        }
        Err(IoError::WriteFailed(
            "replacement metadata retry exhausted".into(),
        ))
    }

    async fn discard_replacement(&self, replacement: Segment) {
        let _ = self
            .allocator
            .discard_replacement_segment(DiscardReplacementSegmentRequest {
                chunk_id: self.chunk.id,
                segment: Some(replacement),
            })
            .await;
    }

    async fn mark_replica_unavailable(&mut self, strip_sequence: u32, failed: Segment) -> Result<()> {
        let strip_index = self
            .chunk
            .strips
            .iter()
            .position(|strip| strip.strip_sequence == strip_sequence)
            .ok_or_else(|| IoError::MetadataConflict("failed strip disappeared".into()))?;
        let old = self.chunk.strips[strip_index].clone();
        let strip_start = u64::from(old.chunk_offset) * 1024;
        if self.cursor <= strip_start {
            return Ok(());
        }
        let mut degraded = old.clone();
        if !degraded.unavailable_segments.contains(&failed) {
            degraded.unavailable_segments.push(failed);
        }
        let operation_id = crowdb_protocol::common::ChunkId {
            high: self.writer_epoch ^ self.chunk.modify_ts ^ u64::MAX,
            low: u64::from(strip_sequence) ^ failed.unit_offset,
        };
        let response = self
            .allocator
            .replace_chunk_strip_range(ReplaceChunkStripRangeRequest {
                chunk_id: self.chunk.id,
                expected_modify_ts: self.chunk.modify_ts,
                start_index: u32::try_from(strip_index).unwrap_or(u32::MAX),
                old_strips: vec![old],
                replacement_strips: vec![degraded],
                operation_id: Some(operation_id),
            })
            .await?;
        self.chunk = response
            .chunk
            .ok_or_else(|| IoError::MetadataConflict("degraded marker returned no chunk".into()))?;
        Ok(())
    }

    async fn advance(&mut self, cursor: u64, closed_strip_sequence: Option<u32>) -> Result<()> {
        let chunk_id = self
            .chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("shared chunk missing id".into()))?;
        for attempt in 0..8 {
            let response = self
                .allocator
                .advance_chunk_write(AdvanceChunkWriteRequest {
                    chunk_id: Some(chunk_id),
                    writer_epoch: self.writer_epoch,
                    expected_modify_ts: self.chunk.modify_ts,
                    acknowledged_cursor: cursor,
                    closed_strip_sequence,
                    writer_lease_ms: u64::try_from(self.policy.writer_lease.as_millis()).unwrap_or(u64::MAX),
                })
                .await;
            match response {
                Ok(response) => {
                    self.chunk = response.chunk.ok_or_else(|| {
                        IoError::AllocationFailed("cursor advance returned no chunk".into())
                    })?;
                    self.cursor = cursor;
                    return Ok(());
                }
                Err(IoError::MetadataConflict(_)) if attempt < 7 => {
                    let response = self
                        .allocator
                        .query_chunk(QueryChunkRequest {
                            chunk_id: Some(chunk_id),
                        })
                        .await?;
                    let refreshed = response.chunk.ok_or_else(|| {
                        IoError::MetadataConflict("shared chunk disappeared during advance".into())
                    })?;
                    if refreshed.state != ChunkState::Active as i32
                        || refreshed.writer_epoch != self.writer_epoch
                    {
                        return Err(IoError::MetadataConflict(
                            "shared chunk ownership changed during advance".into(),
                        ));
                    }
                    let strip_already_closed = closed_strip_sequence.map_or(true, |requested| {
                        refreshed
                            .closed_strip_sequence
                            .is_some_and(|actual| actual >= requested)
                    });
                    if refreshed.acknowledged_cursor >= cursor && strip_already_closed {
                        self.chunk = refreshed;
                        self.cursor = cursor;
                        return Ok(());
                    }
                    self.chunk = refreshed;
                }
                Err(error) => return Err(error),
            }
        }
        Err(IoError::MetadataConflict(
            "shared chunk metadata kept changing during advance".into(),
        ))
    }

    /// Resolve any pending background advance and apply the result.
    async fn flush_pending_advance(&mut self) -> Result<()> {
        if let Some(pending) = self.pending_advance.take() {
            self.chunk = pending.await.map_err(|join_error| {
                IoError::WriteFailed(format!("background advance task panicked: {join_error}"))
            })??;
        }
        Ok(())
    }

    async fn refresh_pending_advance(&mut self) -> Result<()> {
        if self
            .pending_advance
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            self.flush_pending_advance().await?;
        }
        Ok(())
    }

    fn start_pending_advance(&mut self, cursor: u64) -> Result<()> {
        if self.pending_advance.is_some() {
            return Ok(());
        }
        let chunk_id = self
            .chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("shared chunk missing id".into()))?;
        let writer_lease_ms = u64::try_from(self.policy.writer_lease.as_millis()).unwrap_or(u64::MAX);
        self.pending_advance = Some(
            if let Some((group_id, generation, sequence)) = self.active_reservation.take() {
                tokio::spawn(confirm_reserved_strip(
                    Arc::clone(&self.allocator),
                    chunk_id,
                    group_id,
                    self.writer_epoch,
                    generation,
                    self.chunk.modify_ts,
                    sequence,
                    cursor,
                    None,
                    writer_lease_ms,
                ))
            } else {
                tokio::spawn(advance_chunk(
                    Arc::clone(&self.allocator),
                    chunk_id,
                    self.writer_epoch,
                    self.chunk.modify_ts,
                    cursor,
                    None,
                    writer_lease_ms,
                ))
            },
        );
        Ok(())
    }

    fn schedule_closed_advance(&mut self, cursor: u64, closed_strip_sequence: u32) {
        let writer_lease_ms = u64::try_from(self.policy.writer_lease.as_millis()).unwrap_or(u64::MAX);
        if let Some((group_id, generation, sequence)) = self.active_reservation.take() {
            let allocator = Arc::clone(&self.allocator);
            let chunk_id = self.chunk.id.unwrap_or_default();
            let modify_ts = self.chunk.modify_ts;
            let writer_epoch = self.writer_epoch;
            let pending = self.pending_advance.take();
            self.pending_advance = Some(tokio::spawn(async move {
                let chunk = if let Some(pending) = pending {
                    pending.await.map_err(|join_error| {
                        IoError::WriteFailed(format!("background metadata task panicked: {join_error}"))
                    })??
                } else {
                    return confirm_reserved_strip(
                        allocator,
                        chunk_id,
                        group_id,
                        writer_epoch,
                        generation,
                        modify_ts,
                        sequence,
                        cursor,
                        Some(closed_strip_sequence),
                        writer_lease_ms,
                    )
                    .await;
                };
                confirm_reserved_strip(
                    allocator,
                    chunk_id,
                    group_id,
                    writer_epoch,
                    generation,
                    chunk.modify_ts,
                    sequence,
                    cursor,
                    Some(closed_strip_sequence),
                    writer_lease_ms,
                )
                .await
            }));
        } else {
            let pending = self.pending_advance.take();
            if self.reservation_mode {
                let allocator = Arc::clone(&self.allocator);
                let chunk_id = self.chunk.id.unwrap_or_default();
                let writer_epoch = self.writer_epoch;
                let modify_ts = self.chunk.modify_ts;
                self.pending_advance = Some(tokio::spawn(async move {
                    let modify_ts = if let Some(pending) = pending {
                        pending
                            .await
                            .map_err(|join_error| {
                                IoError::WriteFailed(format!(
                                    "background metadata task panicked: {join_error}"
                                ))
                            })??
                            .modify_ts
                    } else {
                        modify_ts
                    };
                    advance_chunk(
                        allocator,
                        chunk_id,
                        writer_epoch,
                        modify_ts,
                        cursor,
                        Some(closed_strip_sequence),
                        writer_lease_ms,
                    )
                    .await
                }));
            } else {
                self.pending_advance = Some(tokio::spawn(close_and_prefetch(
                    Arc::clone(&self.allocator),
                    pending,
                    self.chunk.clone(),
                    self.writer_epoch,
                    cursor,
                    closed_strip_sequence,
                    writer_lease_ms,
                    self.policy.small_strip_prefetch_count,
                    self.policy.mirror_copies,
                    self.policy.chunk_capacity,
                )));
            }
        }
    }

    async fn finish(&mut self) -> Result<()> {
        if let Err(error) = self.resolve_conversion_update().await {
            tracing::warn!(%error, "incomplete foreground conversion left mirrored");
        }
        if let Err(error) = self.flush_pending_advance().await {
            if self.accept_external_seal().await? {
                self.release_local_state();
                return Ok(());
            }
            return Err(IoError::MetadataConflict(format!(
                "flush reserved metadata failed: {error}"
            )));
        }
        if self.chunk.acknowledged_cursor < self.cursor {
            self.advance(self.cursor, None).await.map_err(|error| {
                IoError::MetadataConflict(format!("final reserved cursor advance failed: {error}"))
            })?;
        }
        if let Some(group_id) = self.reservation_group_id {
            if let Some((_, _, sequence)) = self.staged_reservation.take() {
                if let Some(index) = self
                    .chunk
                    .strips
                    .iter()
                    .position(|strip| strip.strip_sequence == sequence)
                {
                    self.reserved_strips.push_front(self.chunk.strips.remove(index));
                }
            }
            while let Some(strip) = self.reserved_strips.pop_front() {
                self.mutate_reservation(
                    group_id,
                    self.reservation_generation,
                    strip.strip_sequence,
                    StripReservationAction::Cancel,
                    self.chunk.acknowledged_cursor,
                    None,
                )
                .await
                .map_err(|error| {
                    IoError::MetadataConflict(format!(
                        "cancel surplus reservation {} failed: {error}",
                        strip.strip_sequence
                    ))
                })?;
            }
        }
        let Some(chunk_id) = self.chunk.id else {
            return Ok(());
        };
        self.release_local_state();
        if self.cursor == 0 {
            self.allocator
                .delete_chunk(DeleteChunkRequest {
                    chunk_id: Some(chunk_id),
                })
                .await?;
        } else {
            let seal_length = u32::try_from(self.cursor.div_ceil(1024)).unwrap_or(u32::MAX);
            self.allocator
                .seal_chunk(SealChunkRequest {
                    chunk_id: Some(chunk_id),
                    seal_length,
                })
                .await
                .map_err(|error| IoError::MetadataConflict(format!("seal reserved chunk failed: {error}")))?;
        }
        Ok(())
    }

    async fn accept_external_seal(&mut self) -> Result<bool> {
        let Some(chunk_id) = self.chunk.id else {
            return Ok(false);
        };
        let response = self
            .allocator
            .query_chunk(QueryChunkRequest {
                chunk_id: Some(chunk_id),
            })
            .await?;
        let Some(chunk) = response.chunk else {
            return Ok(false);
        };
        let sealed = chunk.state == ChunkState::Sealed as i32 && chunk.acknowledged_cursor >= self.cursor;
        if sealed {
            self.chunk = chunk;
        }
        Ok(sealed)
    }

    fn release_local_state(&mut self) {
        self.clear_shadow();
        self.conversion_group = None;
        self.pending_conversion_update = None;
        if self.owns_conversion_gate {
            self.conversion_active.store(false, Ordering::Release);
        }
        self.owns_conversion_gate = false;
    }

    fn clear_shadow(&mut self) {
        if let Some(shadow) = self.shadow.take() {
            self.metrics
                .shadow_bytes
                .fetch_sub(shadow.capacity() as u64, Ordering::Relaxed);
        }
    }
}

fn batch_shape(batch: &[PendingObject]) -> (usize, usize) {
    (
        batch.iter().map(|object| object.len).sum(),
        batch.iter().map(|object| object.fragments.len()).sum(),
    )
}

struct RepairMetricGuard {
    metrics: Arc<SmallWriteMetrics>,
    started: Instant,
}

impl RepairMetricGuard {
    fn new(metrics: Arc<SmallWriteMetrics>) -> Self {
        metrics.active_repairs.fetch_add(1, Ordering::Relaxed);
        Self {
            metrics,
            started: Instant::now(),
        }
    }
}

impl Drop for RepairMetricGuard {
    fn drop(&mut self) {
        let elapsed = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.metrics.active_repairs.fetch_sub(1, Ordering::Relaxed);
        self.metrics
            .repair_latency_ns
            .fetch_add(elapsed, Ordering::Relaxed);
        self.metrics
            .max_repair_latency_ns
            .fetch_max(elapsed, Ordering::Relaxed);
    }
}

fn next_writer_epoch() -> u64 {
    let nonce = NEXT_WRITER_EPOCH.fetch_add(1, Ordering::Relaxed);
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
        });
    (time.rotate_left(21) ^ nonce.wrapping_mul(0x9e37_79b9_7f4a_7c15)).max(1)
}
