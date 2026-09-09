// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Single-owner shared chunk worker and whole-object batch commit barrier.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use crowdb_common::ec::{EcScheme, IncrementalParity};
use crowdb_protocol::chunkdb::rpc::{
    AdvanceChunkWriteRequest, AllocateChunkRequest, AllocateReplacementSegmentRequest, AppendChunkRequest,
    Chunk, ChunkState, ChunkType, CompleteMirrorToEcConversionRequest, DeleteChunkRequest,
    DiscardReplacementSegmentRequest, Location, PrepareMirrorToEcConversionRequest, QueryChunkRequest,
    ReplaceChunkStripRangeRequest, SealChunkRequest, Strip, StripType,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::Segment;
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
    let route = Arc::new(PipelineRoute::new(sender, runtime.now_ms()));
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

async fn append_mirror_strips(
    allocator: &dyn ChunkAllocator,
    mut chunk: Chunk,
    strip_count: u32,
    copy_count: u32,
) -> Result<Chunk> {
    let chunk_id = chunk
        .id
        .ok_or_else(|| IoError::AllocationFailed("shared chunk missing id".into()))?;
    let last = chunk
        .strips
        .last()
        .ok_or_else(|| IoError::AllocationFailed("shared chunk has no initial strip".into()))?;
    let unit_count = last.capacity.checked_div(last.unit_kb).unwrap_or(0).max(1);
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
    pending_advance: Option<tokio::task::JoinHandle<Result<Chunk>>>,
}

struct PendingEcGroup {
    old_strips: Vec<crowdb_protocol::chunkdb::rpc::ChunkStrip>,
    data_shards: Vec<Bytes>,
    parity: IncrementalParity,
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
        let initial_strip_count = runtime.policy.small_strip_prefetch_count.min(
            u32::try_from(
                runtime.policy.chunk_capacity
                    / u64::try_from(runtime.policy.object_limit).unwrap_or(u64::MAX),
            )
            .unwrap_or(u32::MAX)
            .max(1),
        );
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
        Ok(Self {
            allocator: Arc::clone(&runtime.allocator),
            disk_writer: Arc::clone(&runtime.disk_writer),
            policy: Arc::clone(&runtime.policy),
            chunk,
            cursor: 0,
            writer_epoch,
            shadow: None,
            failed_disks: Arc::clone(&runtime.failed_disks),
            metrics: Arc::clone(&runtime.metrics),
            budget: Arc::clone(&runtime.budget),
            conversion_active,
            conversion_group: None,
            pending_advance: None,
        })
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
        Err(IoError::AllocationFailed(
            "strip prefetch did not stay ahead of the write cursor".into(),
        ))
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
        if let Err(error) = self.retain_closed_strip(closed).await {
            tracing::warn!(%error, "mirror-to-EC fast path deferred to chunkdb");
        }
        self.schedule_closed_advance(strip_end, strip.strip_sequence);
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
            if let Err(error) = self.retain_closed_strip(closed_strip).await {
                tracing::warn!(%error, "mirror-to-EC fast path deferred to chunkdb");
            }
            self.schedule_closed_advance(end, sequence);
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
        if self.conversion_group.is_none() {
            let scheme = EcScheme::new(self.policy.conversion_data_num, self.policy.conversion_code_num);
            let remaining_shards = self.remaining_in_chunk() / image.len() as u64;
            if 1_u64.saturating_add(remaining_shards) < scheme.data_num as u64 {
                return Ok(());
            }
            let bytes = scheme
                .total_blocks()
                .checked_mul(image.len())
                .ok_or(IoError::MemoryBudgetExhausted)?;
            let permits = u32::try_from(bytes).map_err(|_| IoError::MemoryBudgetExhausted)?;
            let budget = Arc::clone(&self.budget)
                .try_acquire_many_owned(permits)
                .map_err(|_| IoError::MemoryBudgetExhausted)?;
            self.conversion_group = Some(PendingEcGroup {
                old_strips: Vec::with_capacity(scheme.data_num),
                data_shards: Vec::with_capacity(scheme.data_num),
                parity: IncrementalParity::new(scheme)
                    .map_err(|error| IoError::EcEncodeFailed(error.to_string()))?,
                _budget: budget,
            });
            self.conversion_active.store(true, Ordering::Release);
        }
        let group = self
            .conversion_group
            .as_mut()
            .unwrap_or_else(|| unreachable!("conversion group initialized"));
        group
            .parity
            .push(&image)
            .map_err(|error| IoError::EcEncodeFailed(error.to_string()))?;
        group.old_strips.push(strip);
        group.data_shards.push(image);
        if group.parity.is_complete() {
            let closed_strip_sequence = group
                .old_strips
                .last()
                .map(|strip| strip.strip_sequence)
                .ok_or_else(|| IoError::Internal("conversion group is empty".into()))?;
            self.flush_pending_advance().await?;
            self.advance(self.cursor, Some(closed_strip_sequence)).await?;
            if let Some(group) = self.conversion_group.as_mut() {
                for old in &mut group.old_strips {
                    *old = self
                        .chunk
                        .strips
                        .iter()
                        .find(|current| current.strip_sequence == old.strip_sequence)
                        .cloned()
                        .ok_or_else(|| IoError::MetadataConflict("conversion source disappeared".into()))?;
                }
            }
            let result = self.convert_retained_group().await;
            self.conversion_active.store(false, Ordering::Release);
            return result;
        }
        Ok(())
    }

    async fn convert_retained_group(&mut self) -> Result<()> {
        let group = self
            .conversion_group
            .take()
            .unwrap_or_else(|| unreachable!("complete conversion group exists"));
        let chunk_id = self
            .chunk
            .id
            .ok_or_else(|| IoError::MetadataConflict("shared chunk has no id".into()))?;
        let first_sequence = group
            .old_strips
            .first()
            .map(|strip| strip.strip_sequence)
            .ok_or_else(|| IoError::Internal("conversion group is empty".into()))?;
        let start_index = self
            .chunk
            .strips
            .iter()
            .position(|strip| strip.strip_sequence == first_sequence)
            .ok_or_else(|| IoError::MetadataConflict("conversion source disappeared".into()))?;
        let prepared = self
            .allocator
            .prepare_mirror_to_ec_conversion(PrepareMirrorToEcConversionRequest {
                chunk_id: Some(chunk_id),
                expected_modify_ts: self.chunk.modify_ts,
                start_index: u32::try_from(start_index).unwrap_or(u32::MAX),
                old_strips: group.old_strips,
                data_num: u32::try_from(self.policy.conversion_data_num).unwrap_or(u32::MAX),
                code_num: u32::try_from(self.policy.conversion_code_num).unwrap_or(u32::MAX),
                client_owner: self.writer_epoch,
                claim_lease_ms: u64::try_from(self.policy.writer_lease.as_millis()).unwrap_or(u64::MAX),
            })
            .await?;
        let task_id = prepared
            .task_id
            .ok_or_else(|| IoError::AllocationFailed("conversion preparation returned no task id".into()))?;
        let replacement = prepared.replacement_strip.ok_or_else(|| {
            IoError::AllocationFailed("conversion preparation returned no replacement".into())
        })?;
        let Some(Strip::EcStrip(ec)) = &replacement.strip else {
            return Err(IoError::AllocationFailed(
                "conversion preparation returned a non-EC strip".into(),
            ));
        };
        let parity = group
            .parity
            .finish()
            .map_err(|error| IoError::EcEncodeFailed(error.to_string()))?;
        let mut shards = group.data_shards;
        shards.extend(parity.into_iter().map(Bytes::from));
        if ec.segments.len() != shards.len() {
            return Err(IoError::AllocationFailed(format!(
                "conversion returned {} segments for {} shards",
                ec.segments.len(),
                shards.len()
            )));
        }
        let unit_bytes = u64::from(replacement.unit_kb) * 1024;
        let mut writes = tokio::task::JoinSet::new();
        for (segment, shard) in ec.segments.iter().copied().zip(shards) {
            let disk_writer = Arc::clone(&self.disk_writer);
            writes.spawn(async move { disk_writer.write_at(&segment, unit_bytes, 0, shard).await });
        }
        while let Some(result) = writes.join_next().await {
            result.map_err(|error| IoError::WriteFailed(format!("EC writer task failed: {error}")))??;
        }
        let mut syncs = tokio::task::JoinSet::new();
        for segment in &ec.segments {
            let segment = *segment;
            let disk_writer = Arc::clone(&self.disk_writer);
            syncs.spawn(async move { disk_writer.fsync(&segment).await });
        }
        while let Some(result) = syncs.join_next().await {
            result.map_err(|error| IoError::WriteFailed(format!("EC fsync task failed: {error}")))??;
        }
        let response = self
            .allocator
            .complete_mirror_to_ec_conversion(CompleteMirrorToEcConversionRequest {
                chunk_id: Some(chunk_id),
                task_id: Some(task_id),
                client_owner: self.writer_epoch,
            })
            .await?;
        self.chunk = response
            .chunk
            .ok_or_else(|| IoError::MetadataConflict("conversion completion returned no chunk".into()))?;
        Ok(())
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
            let Ok(response) = response else {
                continue;
            };
            let Some(replacement) = response.segment else {
                continue;
            };
            if self
                .disk_writer
                .write_at_byte_offset(&replacement, unit_bytes, block_offset, image.clone())
                .await
                .is_err()
            {
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
                replacement_strips: vec![new_strip],
                operation_id: Some(operation_id),
            };
            match self.install_replacement(request, replacement).await {
                Ok(chunk) => {
                    self.chunk = chunk;
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
        Err(IoError::WriteFailed("mirror replica repair exhausted".into()))
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
        self.pending_advance = Some(tokio::spawn(advance_chunk(
            Arc::clone(&self.allocator),
            chunk_id,
            self.writer_epoch,
            self.chunk.modify_ts,
            cursor,
            None,
            writer_lease_ms,
        )));
        Ok(())
    }

    fn schedule_closed_advance(&mut self, cursor: u64, closed_strip_sequence: u32) {
        let writer_lease_ms = u64::try_from(self.policy.writer_lease.as_millis()).unwrap_or(u64::MAX);
        let pending = self.pending_advance.take();
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

    async fn finish(&mut self) -> Result<()> {
        self.flush_pending_advance().await?;
        if self.chunk.acknowledged_cursor < self.cursor {
            self.advance(self.cursor, None).await?;
        }
        let Some(chunk_id) = self.chunk.id else {
            return Ok(());
        };
        self.clear_shadow();
        self.conversion_group = None;
        self.conversion_active.store(false, Ordering::Release);
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
                .await?;
        }
        Ok(())
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
