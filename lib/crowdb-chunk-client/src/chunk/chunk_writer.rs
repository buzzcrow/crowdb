// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkWriter` — chunk wrapper + write ability.
//!
//! Owns the current `Chunk` protobuf (in `Arc`, shared with
//! `EcStripWriter`). Owns the strip-level drive loop in `push`
//! (auto-rotates strips: finish + open next when full). All chunkdb
//! chunk operations (seal, delete, append) go through this class.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::chunk::ec_strip_writer::EcStripWriter;
use crate::chunk::mirror_strip_writer::{MirrorIoConcurrency, MirrorStripWriter};
use crate::chunk::segment_writer::{FailedSegmentWrite, SegmentRepair};
use crate::chunk::strip::{StripResult, StripWriter};
use crate::config::ChunkClientConfig;
use crate::disk_io::DiskWriter;
use crate::io::FeedStatus;
use crate::metrics::LargeWriteRepairMetrics;
use crate::negative_list::FailedDiskList;
use crate::traits::ChunkAllocator;
use crate::{IoError, Result};
use crowdb_common::ec::EcScheme;
use crowdb_protocol::chunkdb::rpc::{
    Chunk, DeleteChunkRequest, Location as ProtoLocation, SealChunkRequest, Strip,
};
use crowdb_protocol::common::ChunkId;

mod prefetch;
mod recovery;

use prefetch::{append_strips, compute_strips_remaining};

/// Chunk wrapper + write ability. Owns `Arc<Chunk>`; the strip-level
/// drive loop is in `push` (auto-rotates strips). Collects write
/// completion handles from each `finish_strip` and joins them at
/// `seal` time. Runs an internal strip-prefetch task that
/// appends strips ahead of `write_cursor`, bounded by
/// `prefetch_strips_per_chunk`.
pub struct ChunkWriter {
    pub(crate) allocator: Arc<dyn ChunkAllocator>,
    pub(crate) disk_writer: Arc<dyn DiskWriter>,
    pub(crate) ec_scheme: EcScheme,
    pub(crate) config: Arc<ChunkClientConfig>,
    pub(crate) chunk: Option<Arc<Chunk>>,
    pub(crate) write_cursor: u32,
    pub(crate) bytes_in_chunk: u64,
    pub(crate) object_size: Option<u64>,
    pub(crate) strips_remaining: Option<usize>,
    pub(crate) current_strip: Option<StripWriter>,
    pub(crate) completion_handles: VecDeque<JoinHandle<Result<Vec<FailedSegmentWrite>>>>,
    mirror_completions: VecDeque<MirrorPending>,
    pub(crate) prefetch_handle: Option<JoinHandle<()>>,
    pub(crate) prefetch_rx: Option<mpsc::Receiver<Result<Chunk>>>,
    prefetch_plan: Option<StripPrefetchPlan>,
    prefetch_trigger: Option<mpsc::Sender<()>>,
    prefetch_trigger_index: Option<u32>,
    pub(crate) preparation_stalls: u64,
    pub(crate) preparation_stall_time: Duration,
    pub(crate) strip_write_successes: u64,
    pub(crate) strip_write_success_time: Duration,
    pub(crate) strip_write_success_max: Duration,
    pub(crate) mirror_uncommitted_peak: u64,
    committed_mirror_strips: u32,
    replaying_mirror: bool,
    framed_input: bool,
    pub(crate) ec_encode_time: Duration,
    pub(crate) completion_wait_time: Duration,
    pub(crate) failed_disks: Arc<FailedDiskList>,
    pub(crate) repair_metrics: Arc<LargeWriteRepairMetrics>,
    mirror_io_concurrency: Arc<MirrorIoConcurrency>,
}

struct MirrorCompletion {
    result: StripResult,
    elapsed: Duration,
    failures: Vec<FailedSegmentWrite>,
    // Later completions retain their data until every preceding strip commits.
    buffer: Bytes,
}

enum MirrorPending {
    Running {
        handle: JoinHandle<Result<MirrorCompletion>>,
        buffer: Bytes,
    },
    Completed(MirrorCompletion),
    Failed(Bytes),
}

impl MirrorPending {
    fn is_finished(&self) -> bool {
        match self {
            Self::Running { handle, .. } => handle.is_finished(),
            Self::Completed(_) | Self::Failed(_) => true,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct StripPrefetchPlan {
    pub total_strips: u32,
    pub batch_max: u32,
}

impl ChunkWriter {
    pub(crate) fn set_framed_input(&mut self) {
        self.framed_input = true;
    }

    /// Construct a new chunk writer (no chunk open yet).
    pub fn new(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        ec_scheme: EcScheme,
        config: Arc<ChunkClientConfig>,
    ) -> Self {
        Self::new_with_repair(
            allocator,
            disk_writer,
            ec_scheme,
            config,
            Arc::new(FailedDiskList::new(Duration::from_secs(60))),
            Arc::new(LargeWriteRepairMetrics::default()),
        )
    }

    pub(crate) fn new_with_repair(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        ec_scheme: EcScheme,
        config: Arc<ChunkClientConfig>,
        failed_disks: Arc<FailedDiskList>,
        repair_metrics: Arc<LargeWriteRepairMetrics>,
    ) -> Self {
        Self {
            allocator,
            disk_writer,
            ec_scheme,
            config,
            chunk: None,
            write_cursor: 0,
            bytes_in_chunk: 0,
            object_size: None,
            strips_remaining: None,
            current_strip: None,
            completion_handles: VecDeque::new(),
            mirror_completions: VecDeque::new(),
            prefetch_handle: None,
            prefetch_rx: None,
            prefetch_plan: None,
            prefetch_trigger: None,
            prefetch_trigger_index: None,
            preparation_stalls: 0,
            preparation_stall_time: Duration::ZERO,
            strip_write_successes: 0,
            strip_write_success_time: Duration::ZERO,
            strip_write_success_max: Duration::ZERO,
            mirror_uncommitted_peak: 0,
            committed_mirror_strips: 0,
            replaying_mirror: false,
            framed_input: false,
            ec_encode_time: Duration::ZERO,
            completion_wait_time: Duration::ZERO,
            failed_disks,
            repair_metrics,
            mirror_io_concurrency: Arc::default(),
        }
    }

    /// Open a chunk from a pre-allocated `Chunk` protobuf. Wraps it in
    /// `Arc`, opens the first strip (already present from
    /// `allocate_chunk`), and starts the internal strip-prefetch task
    /// that appends strips ahead of `write_cursor` (bounded by
    /// `prefetch_strips_per_chunk`). `object_size` drives prefetch planning:
    /// known-size objects stop pre-appending when enough strips are
    /// allocated; unknown-size objects pre-append up to
    /// `strips_per_chunk`.
    pub fn open(&mut self, chunk: Chunk, object_size: Option<u64>) -> Result<()> {
        self.open_with_prefetch_plan(chunk, object_size, None)
    }

    pub(crate) fn open_with_prefetch_plan(
        &mut self,
        chunk: Chunk,
        object_size: Option<u64>,
        plan: Option<StripPrefetchPlan>,
    ) -> Result<()> {
        if chunk.id.is_none() {
            return Err(IoError::AllocationFailed("open: chunk missing id".into()));
        }
        if chunk.strips.is_empty() {
            return Err(IoError::AllocationFailed("open: chunk has no strips".into()));
        }
        self.object_size = object_size;
        self.strips_remaining = plan.map_or_else(
            || compute_strips_remaining(object_size, &chunk),
            |plan| Some((plan.total_strips as usize).saturating_sub(chunk.strips.len())),
        );
        self.prefetch_plan = plan;
        self.prefetch_trigger_index = None;
        let chunk = Arc::new(chunk);
        let strip = self.make_strip_writer(Arc::clone(&chunk), 0)?;
        self.chunk = Some(chunk);
        self.write_cursor = 0;
        self.bytes_in_chunk = 0;
        self.committed_mirror_strips = 0;
        self.current_strip = Some(strip);
        // Start the internal strip-prefetch task.
        self.start_strip_prefetch();
        Ok(())
    }

    /// Continue with a new strip on the same chunk. `chunk` is the
    /// cumulative `Chunk` protobuf (from `append_chunk` response) with
    /// the next strip appended. Arc-swaps `self.chunk` and opens the
    /// strip at `write_cursor + 1`.
    pub(crate) fn continue_strip(&mut self, chunk: Chunk) -> Result<()> {
        let new_id = chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("continue_strip: chunk missing id".into()))?;
        let cur_id = self.current_chunk_id();
        if cur_id != Some(new_id) {
            return Err(IoError::Internal("continue_strip with different chunk_id".into()));
        }
        let next_index = self.write_cursor + 1;
        let chunk = Arc::new(chunk);
        let strip = self.make_strip_writer(Arc::clone(&chunk), next_index)?;
        self.chunk = Some(chunk);
        self.write_cursor = next_index;
        self.current_strip = Some(strip);
        Ok(())
    }

    /// Push a data block to the current strip. Auto-rotates strips:
    /// if the current strip is full, finishes it, advances
    /// `write_cursor`, opens the next strip (from `chunk.strips` if
    /// pre-appended, or via `append_chunk` RPC), then pushes the
    /// block to the new strip. Returns `Pause` if the chunk is full
    /// after finishing the current strip — the block is NOT pushed
    /// (caller rotates chunks, then re-pushes).
    pub async fn push(&mut self, mut buffer: Bytes) -> Result<FeedStatus> {
        if self.current_strip.is_none() && self.chunk.is_none() {
            return Err(IoError::Internal("push with no open strip".into()));
        }
        // A public frame can span several EC data blocks. Feed each strip only
        // the bytes it owns; never leave an overflow tail in a completed
        // strip, which would otherwise be encoded as an extra data shard.
        let mut offset = 0usize;
        let mut input_chunk_id = self.current_chunk_id();
        while offset < buffer.len() {
            if self.is_strip_full() {
                if self.current_strip.is_some() {
                    self.finish_strip().await?;
                }
                if self.is_full() {
                    return Ok(FeedStatus::Pause);
                }
                self.open_next_strip().await?;
            }
            if self.current_chunk_id() != input_chunk_id {
                let old_id = input_chunk_id
                    .ok_or_else(|| IoError::Internal("mirror replay lost source chunk ID".into()))?;
                let new_id = self
                    .current_chunk_id()
                    .ok_or_else(|| IoError::Internal("mirror replay lost destination chunk ID".into()))?;
                if self.framed_input {
                    buffer = recovery::rewrite_frames(&buffer, old_id, new_id)?;
                }
                input_chunk_id = Some(new_id);
            }
            let strip = self
                .current_strip
                .as_mut()
                .ok_or_else(|| IoError::Internal("push: strip vanished after rotate".into()))?;
            let remaining = usize::try_from(strip.remaining_capacity())
                .map_err(|_| IoError::Internal("strip capacity exceeds usize".into()))?;
            if remaining == 0 {
                continue;
            }
            let end = offset.saturating_add(remaining).min(buffer.len());
            if matches!(strip, StripWriter::Mirror(_))
                && strip.accepted_bytes() == 0
                && end - offset == remaining
            {
                self.ensure_mirror_capacity().await?;
                if self.current_chunk_id() != input_chunk_id {
                    let old_id = input_chunk_id
                        .ok_or_else(|| IoError::Internal("mirror replay lost source chunk ID".into()))?;
                    let new_id = self
                        .current_chunk_id()
                        .ok_or_else(|| IoError::Internal("mirror replay lost destination chunk ID".into()))?;
                    if self.framed_input {
                        buffer = recovery::rewrite_frames(&buffer, old_id, new_id)?;
                    }
                    input_chunk_id = Some(new_id);
                    continue;
                }
                let mut strip = self
                    .current_strip
                    .take()
                    .ok_or_else(|| IoError::Internal("mirror strip vanished before dispatch".into()))?;
                let bytes = buffer.slice(offset..end);
                let retained = bytes.clone();
                let handle = tokio::spawn(async move {
                    let started = Instant::now();
                    let StripWriter::Mirror(mirror) = &mut strip else {
                        return Err(IoError::Internal("mirror dispatch changed strip type".into()));
                    };
                    let retained = bytes.clone();
                    let (result, failures) = mirror.write_full_repairable(bytes).await?;
                    Ok(MirrorCompletion {
                        result,
                        elapsed: started.elapsed(),
                        failures,
                        buffer: retained,
                    })
                });
                self.mirror_completions.push_back(MirrorPending::Running {
                    handle,
                    buffer: retained,
                });
                self.mirror_uncommitted_peak = self
                    .mirror_uncommitted_peak
                    .max(u64::try_from(self.mirror_completions.len()).unwrap_or(u64::MAX));
                self.bytes_in_chunk += remaining as u64;
                offset = end;
                continue;
            }
            let started = Instant::now();
            strip.push(buffer.slice(offset..end)).await?;
            let elapsed = started.elapsed();
            self.strip_write_successes += 1;
            self.strip_write_success_time += elapsed;
            self.strip_write_success_max = self.strip_write_success_max.max(elapsed);
            offset = end;
        }
        Ok(FeedStatus::Continue)
    }

    pub(crate) fn mirror_write_capacity(&self) -> bool {
        self.mirror_completions.len() < self.config.large_parallel_strip_writes
    }

    pub(crate) async fn ensure_mirror_capacity(&mut self) -> Result<()> {
        self.commit_ready_mirrors().await?;
        if !self.mirror_write_capacity() {
            self.commit_oldest_mirror().await?;
            self.commit_ready_mirrors().await?;
        }
        Ok(())
    }

    async fn commit_oldest_mirror(&mut self) -> Result<()> {
        // Keep the handle in the queue across cancellation of this await.
        let pending = self
            .mirror_completions
            .front_mut()
            .ok_or_else(|| IoError::Internal("missing mirror completion".into()))?;
        if let MirrorPending::Running { handle, buffer } = pending {
            match handle.await {
                Ok(Ok(completion)) => *pending = MirrorPending::Completed(completion),
                Ok(Err(error)) => {
                    *pending = MirrorPending::Failed(buffer.clone());
                    return Err(error);
                }
                Err(error) => {
                    *pending = MirrorPending::Failed(buffer.clone());
                    return Err(IoError::Internal(format!("mirror write task panicked: {error}")));
                }
            }
        }
        let MirrorPending::Completed(completion) = self
            .mirror_completions
            .front()
            .ok_or_else(|| IoError::Internal("missing mirror completion".into()))?
        else {
            return Err(IoError::Internal(
                "mirror write task failed before completion".into(),
            ));
        };
        if !completion.result.completion_handles.is_empty() {
            return Err(IoError::Internal(
                "mirror strip returned unexpected completion handles".into(),
            ));
        }
        // Retain this strip ahead of later completions if replacement fails.
        let failures = completion.failures.clone();
        let elapsed = completion.elapsed;
        if let Err(error) = self.repair_mirror_failures(failures).await {
            if matches!(error, IoError::ReplicaRepairExhausted(_)) && !self.replaying_mirror {
                self.rotate_failed_mirror(None).await?;
                return Ok(());
            }
            return Err(error);
        }
        self.mirror_completions.pop_front();
        self.committed_mirror_strips += 1;
        self.strip_write_successes += 1;
        self.strip_write_success_time += elapsed;
        self.strip_write_success_max = self.strip_write_success_max.max(elapsed);
        Ok(())
    }

    async fn commit_ready_mirrors(&mut self) -> Result<()> {
        while self
            .mirror_completions
            .front()
            .is_some_and(MirrorPending::is_finished)
        {
            self.commit_oldest_mirror().await?;
        }
        Ok(())
    }

    async fn repair_mirror_failures(&mut self, failures: Vec<FailedSegmentWrite>) -> Result<()> {
        let chunk_id = self
            .current_chunk_id()
            .ok_or_else(|| IoError::Internal("mirror repair has no active chunk".into()))?;
        for failure in failures {
            let repair = SegmentRepair {
                allocator: &self.allocator,
                disk_writer: &self.disk_writer,
                failed_disks: &self.failed_disks,
                metrics: &self.repair_metrics,
                attempts: self.config.large_write_repair_attempts,
            };
            self.chunk = Some(Arc::new(repair.repair(chunk_id, failure).await?));
        }
        Ok(())
    }

    /// Open the next strip on the current chunk. First drains the
    /// prefetch channel (non-blocking) to pick up any pre-appended
    /// chunks. If the next strip is in `chunk.strips`, opens it
    /// directly. Otherwise waits for the prefetch channel to deliver
    /// it (blocking) — this avoids a race where both the prefetch task
    /// and an inline `append_chunk` RPC append the same strip. Falls
    /// back to inline `append_chunk` only if the channel is closed
    /// (prefetch task finished or errored).
    async fn open_next_strip(&mut self) -> Result<()> {
        let next_index = self.write_cursor + 1;
        // Drain prefetch channel (non-blocking) — pick up latest chunk.
        self.drain_prefetch();
        loop {
            let ready = {
                let chunk = self
                    .chunk
                    .as_ref()
                    .ok_or_else(|| IoError::Internal("open_next_strip with no chunk".into()))?;
                (next_index as usize) < chunk.strips.len()
            };
            if ready {
                // Next strip is pre-appended — open it directly.
                let chunk = self
                    .chunk
                    .as_ref()
                    .ok_or_else(|| IoError::Internal("open_next_strip with no chunk".into()))?;
                let strip = self.make_strip_writer(Arc::clone(chunk), next_index)?;
                self.write_cursor = next_index;
                self.current_strip = Some(strip);
                self.maybe_trigger_prefetch();
                return Ok(());
            }
            // Next strip not ready — wait for the prefetch task to
            // deliver it instead of appending inline (avoids duplicate
            // append_chunk calls).
            let Some(rx) = self.prefetch_rx.as_mut() else {
                // No prefetch channel — inline append as last resort.
                let started = Instant::now();
                self.preparation_stalls += 1;
                let result = self.append_strip().await;
                self.preparation_stall_time += started.elapsed();
                let new_chunk = result?;
                self.continue_strip(new_chunk)?;
                return Ok(());
            };
            let started = Instant::now();
            self.preparation_stalls += 1;
            let result = rx.recv().await;
            self.preparation_stall_time += started.elapsed();
            match result {
                Some(Ok(new_chunk)) => {
                    self.accept_prefetched_chunk(new_chunk);
                    // Loop back: check if the next strip is now available.
                }
                Some(Err(e)) => return Err(e),
                None => {
                    // Channel closed — prefetch is done. Inline append.
                    self.prefetch_rx = None;
                    let new_chunk = self.append_strip().await?;
                    self.continue_strip(new_chunk)?;
                    return Ok(());
                }
            }
        }
    }

    /// Drain the prefetch channel (non-blocking) and Arc-swap to the
    /// latest cumulative `Chunk` from the prefetch task.
    fn drain_prefetch(&mut self) {
        loop {
            let result = self.prefetch_rx.as_mut().and_then(|rx| rx.try_recv().ok());
            match result {
                Some(Ok(new_chunk)) => self.accept_prefetched_chunk(new_chunk),
                Some(Err(error)) => {
                    warn!("strip prefetch error: {error}");
                    break;
                }
                None => break,
            }
        }
    }

    fn accept_prefetched_chunk(&mut self, chunk: Chunk) {
        if self.prefetch_plan.is_some() {
            let previous = self.chunk.as_ref().map_or(0, |current| current.strips.len());
            let batch = chunk.strips.len().saturating_sub(previous);
            let half = batch.div_ceil(2);
            self.prefetch_trigger_index =
                Some(u32::try_from(chunk.strips.len().saturating_sub(half)).unwrap_or(u32::MAX));
        }
        self.chunk = Some(Arc::new(chunk));
    }

    fn maybe_trigger_prefetch(&mut self) {
        if self
            .prefetch_trigger_index
            .is_some_and(|index| self.write_cursor >= index)
        {
            if let Some(trigger) = &self.prefetch_trigger {
                let _ = trigger.try_send(());
            }
            self.prefetch_trigger_index = None;
        }
    }

    /// Stop the strip-prefetch task: drop the receiver (task's
    /// `tx.send` fails → task exits) + abort the handle.
    fn stop_prefetch(&mut self) {
        self.prefetch_rx.take();
        self.prefetch_trigger.take();
        self.prefetch_trigger_index = None;
        if let Some(handle) = self.prefetch_handle.take() {
            handle.abort();
        }
    }

    /// Start the internal strip-prefetch background task. Appends
    /// strips to the chunk ahead of `write_cursor`, bounded by
    /// `prefetch_strips_per_chunk`. Known-size objects stop when
    /// `strips_remaining` hits 0; unknown-size objects pre-append up
    /// to `strips_per_chunk`. Sends cumulative `Chunk` values via a
    /// channel; `drain_prefetch` picks them up.
    fn start_strip_prefetch(&mut self) {
        let Some(mut chunk) = self.chunk.as_deref().cloned() else {
            return;
        };
        let plan = self.prefetch_plan;
        let capacity = if plan.is_some() {
            1
        } else {
            self.config.prefetch_strips_per_chunk
        };
        let (tx, rx) = mpsc::channel::<Result<Chunk>>(capacity);
        self.prefetch_rx = Some(rx);
        let (trigger_tx, mut trigger_rx) = mpsc::channel::<()>(1);
        self.prefetch_trigger = plan.map(|_| trigger_tx);
        let allocator = Arc::clone(&self.allocator);
        let ec_scheme = self.ec_scheme;
        let config = Arc::clone(&self.config);
        let max_chunk_size = config.max_chunk_size;
        let unit_bytes = u64::from((config.read_buffer_size / 1024) as u32) * 1024;
        let strip_data_bytes = chunk
            .strips
            .first()
            .map_or(ec_scheme.data_num as u64 * unit_bytes, |strip| {
                u64::from(strip.capacity) * 1024
            })
            .max(1);
        let strips_per_chunk = (max_chunk_size / strip_data_bytes) as u32;
        let mut strips_remaining = self.strips_remaining;
        let mut next_strip_index = chunk.strips.len() as u32;
        let handle: JoinHandle<()> = tokio::spawn(async move {
            loop {
                // Stop conditions:
                // - known-size and all strips allocated
                if let Some(remaining) = strips_remaining {
                    if remaining == 0 {
                        break;
                    }
                }
                // - chunk full (enough strips for max_chunk_size)
                if next_strip_index >= strips_per_chunk {
                    break;
                }
                let Ok(permit) = tx.reserve().await else {
                    break;
                };
                let runway = strips_per_chunk.saturating_sub(next_strip_index);
                let remaining =
                    strips_remaining.map_or(u32::MAX, |value| u32::try_from(value).unwrap_or(u32::MAX));
                // For larger objects (more strips to allocate), batch 2
                // strips per append to reduce RPC count. For smaller objects,
                // allocate 1 at a time so the first strip is ready sooner.
                let batch = plan.map_or_else(
                    || match strips_remaining.as_ref() {
                        Some(total) if *total > 4 => 2u32,
                        _ => 1u32,
                    },
                    |plan| plan.batch_max,
                );
                let strip_count = batch.min(runway).min(remaining);
                if strip_count == 0 {
                    break;
                }
                let result = append_strips(&*allocator, chunk, strip_count).await;
                match result {
                    Ok(new_chunk) => {
                        chunk = new_chunk.clone();
                        permit.send(Ok(new_chunk));
                        next_strip_index = next_strip_index.saturating_add(strip_count);
                        if let Some(remaining) = strips_remaining.as_mut() {
                            *remaining = remaining.saturating_sub(strip_count as usize);
                        }
                        if plan.is_some() && trigger_rx.recv().await.is_none() {
                            break;
                        }
                    }
                    Err(e) => {
                        permit.send(Err(e));
                        break;
                    }
                }
            }
        });
        self.prefetch_handle = Some(handle);
    }

    /// Finish the current strip. Records bytes written + collects
    /// write completion handles (joined at `seal` time, not here).
    pub async fn finish_strip(&mut self) -> Result<StripResult> {
        self.await_parity_capacity().await?;
        if matches!(self.current_strip, Some(StripWriter::Mirror(_))) {
            // A partial mirror strip completes inline. Commit every earlier
            // detached strip first so its repair and release stay ordered.
            while !self.mirror_completions.is_empty() {
                self.commit_oldest_mirror().await?;
                self.commit_ready_mirrors().await?;
            }
        }
        let mut strip = self
            .current_strip
            .take()
            .ok_or_else(|| IoError::Internal("finish_strip with no open strip".into()))?;
        let partial_replay = match &strip {
            StripWriter::Mirror(mirror) => mirror.replay_views(),
            StripWriter::Ec(_) => Vec::new(),
        };
        let (mut strip_result, mirror_failures) = match &mut strip {
            StripWriter::Mirror(mirror) => {
                let result = mirror.finish().await?;
                (result, mirror.take_failures()?)
            }
            StripWriter::Ec(_) => (strip.finish().await?, Vec::new()),
        };
        self.ec_encode_time += strip_result.ec_encode_time;
        self.bytes_in_chunk += strip_result.bytes_written;
        if let Err(error) = self.repair_mirror_failures(mirror_failures).await {
            if matches!(error, IoError::ReplicaRepairExhausted(_)) && !self.replaying_mirror {
                self.rotate_failed_mirror(Some(partial_replay)).await?;
                return Box::pin(self.finish_strip()).await;
            }
            return Err(error);
        }
        if matches!(strip, StripWriter::Mirror(_))
            && strip_result.bytes_written
                == self
                    .chunk
                    .as_ref()
                    .and_then(|chunk| chunk.strips.get(strip_result.strip_index_in_chunk as usize))
                    .map_or(0, |strip| u64::from(strip.capacity) * 1024)
        {
            self.committed_mirror_strips += 1;
        }
        // One queue entry represents one completed strip. This keeps
        // `parity_depth` expressed in strips instead of accidentally counting
        // every data and parity shard as an independent depth unit.
        let handles = std::mem::take(&mut strip_result.completion_handles);
        if !handles.is_empty() {
            self.completion_handles.push_back(tokio::spawn(async move {
                let mut failures = Vec::new();
                for handle in handles {
                    if let Some(failure) = handle
                        .await
                        .map_err(|error| IoError::Internal(format!("strip write task panicked: {error}")))?
                    {
                        failures.push(failure);
                    }
                }
                Ok(failures)
            }));
        }
        Ok(strip_result)
    }

    async fn await_parity_capacity(&mut self) -> Result<()> {
        let depth = self.config.parity_depth.max(1);
        while self.completion_handles.len() >= depth {
            let started = Instant::now();
            let handle = self
                .completion_handles
                .pop_front()
                .ok_or_else(|| IoError::Internal("missing write completion".into()))?;
            self.finish_completion(handle).await?;
            self.completion_wait_time += started.elapsed();
        }
        Ok(())
    }

    /// Is the current strip full (all data_num blocks written)?
    pub(crate) fn is_strip_full(&self) -> bool {
        match &self.current_strip {
            Some(s) => !s.ready(),
            None => true,
        }
    }

    /// Is the chunk full (bytes written >= max_chunk_size)? The object
    /// layer checks this after each push to decide chunk rotation.
    pub fn is_full(&self) -> bool {
        self.bytes_in_chunk >= self.config.max_chunk_size
    }

    pub(crate) fn preparation_metrics(&self) -> (u64, Duration) {
        (self.preparation_stalls, self.preparation_stall_time)
    }

    pub(crate) fn active_mirror_write_peak(&self) -> u64 {
        self.mirror_io_concurrency.peak()
    }

    /// Append a new strip to the current chunk via `append_chunk` RPC.
    /// Returns the full cumulative `Chunk` (with the new strip
    /// appended). Used by the internal strip prefetch + the inline
    /// fallback in `open_next_strip`.
    pub(crate) async fn append_strip(&mut self) -> Result<Chunk> {
        let chunk = self
            .chunk
            .as_deref()
            .cloned()
            .ok_or_else(|| IoError::Internal("append_strip with no open chunk".into()))?;
        append_strips(&*self.allocator, chunk, 1).await
    }

    /// Seal the chunk: finish the current strip (if open with data),
    /// join all in-flight writes, then `seal_chunk`
    /// RPC, return the chunk's Location. Parity writes from all strips
    /// in this chunk are joined here (decoupled from strip finish in
    /// Phase 3.1).
    pub async fn seal(&mut self) -> Result<ProtoLocation> {
        // Stop the strip-prefetch task (drop receiver → task exits).
        self.stop_prefetch();
        // Finish the current strip if it's open and has data (the
        // last strip may be partial or full — push doesn't auto-finish
        // on EOF). Skip empty strips (no blocks written → no parity).
        if let Some(strip) = &self.current_strip {
            if strip.has_data() {
                self.finish_strip().await?;
            }
        }
        while !self.mirror_completions.is_empty() {
            self.commit_oldest_mirror().await?;
            self.commit_ready_mirrors().await?;
        }
        let chunk_id = self.current_chunk_id();
        let bytes_in_chunk = self.bytes_in_chunk;

        let location = match chunk_id {
            Some(cid) if bytes_in_chunk > 0 => {
                // Join all in-flight writes before sealing.
                let wait_started = Instant::now();
                while let Some(handle) = self.completion_handles.pop_front() {
                    self.finish_completion(handle).await?;
                }
                self.completion_wait_time += wait_started.elapsed();
                let sealed_length_kb = bytes_in_chunk.div_ceil(1024) as u32;
                self.allocator
                    .seal_chunk(SealChunkRequest {
                        chunk_id: Some(cid),
                        seal_length: sealed_length_kb,
                        seal_bytes: bytes_in_chunk,
                    })
                    .await?;
                ProtoLocation {
                    chunk_id: Some(cid),
                    offset: 0,
                    length: bytes_in_chunk,
                    logical_offset: 0,
                    logical_length: bytes_in_chunk,
                }
            }
            Some(cid) => {
                warn!("seal: deleting empty chunk");
                let _ = self
                    .allocator
                    .delete_chunk(DeleteChunkRequest { chunk_id: Some(cid) })
                    .await;
                ProtoLocation {
                    chunk_id: Some(cid),
                    offset: 0,
                    length: 0,
                    logical_offset: 0,
                    logical_length: 0,
                }
            }
            None => {
                return Err(IoError::Internal("seal with no open chunk".into()));
            }
        };

        Ok(location)
    }

    async fn finish_completion(&mut self, handle: JoinHandle<Result<Vec<FailedSegmentWrite>>>) -> Result<()> {
        let failures = handle
            .await
            .map_err(|error| IoError::Internal(format!("strip completion task panicked: {error}")))??;
        let Some(chunk_id) = self.current_chunk_id() else {
            return Err(IoError::Internal(
                "segment write failed without an active chunk".into(),
            ));
        };
        for failure in failures {
            let repair = SegmentRepair {
                allocator: &self.allocator,
                disk_writer: &self.disk_writer,
                failed_disks: &self.failed_disks,
                metrics: &self.repair_metrics,
                attempts: self.config.large_write_repair_attempts,
            };
            self.chunk = Some(Arc::new(repair.repair(chunk_id, failure).await?));
        }
        Ok(())
    }

    /// Abort: cancel in-flight parity writes, stop the strip-prefetch
    /// task, drop the current strip, delete the partial (unsealed)
    /// chunk.
    pub async fn abort(&mut self) -> Result<()> {
        self.stop_prefetch();
        let had_strip = self.current_strip.is_some();
        if let Some(mut strip) = self.current_strip.take() {
            let _ = strip.abort().await;
        }
        // Submitted DiskIO RPCs are not cancellable. Drain finalization before
        // freeing segments so a late parity write cannot hit reused storage.
        for handle in self.completion_handles.drain(..) {
            let _ = handle.await;
        }
        for pending in self.mirror_completions.drain(..) {
            if let MirrorPending::Running { handle, .. } = pending {
                let _ = handle.await;
            }
        }
        // Delete the chunk if it was opened and has any data — either
        // finished strips (bytes_in_chunk > 0), an in-progress strip
        // (had_strip), or prior finished strips (write_cursor > 0).
        if let Some(chunk_id) = self.current_chunk_id() {
            if self.bytes_in_chunk > 0 || had_strip || self.write_cursor > 0 {
                warn!("abort: deleting partial chunk");
                let _ = self
                    .allocator
                    .delete_chunk(DeleteChunkRequest {
                        chunk_id: Some(chunk_id),
                    })
                    .await;
            }
        }
        Ok(())
    }

    /// Non-async capacity hint. True if the current strip has room.
    pub fn ready(&self) -> bool {
        match &self.current_strip {
            Some(s) => s.ready(),
            None => false,
        }
    }

    /// Bytes written to the current chunk so far.
    pub fn bytes_in_chunk(&self) -> u64 {
        self.bytes_in_chunk
    }

    /// Physical bytes that may still be appended without crossing a chunk.
    pub fn remaining_capacity(&self) -> u64 {
        let current = self.current_strip.as_ref().map_or(0, StripWriter::accepted_bytes);
        self.config
            .max_chunk_size
            .saturating_sub(self.bytes_in_chunk.saturating_add(current))
    }

    /// Current chunk id (if any), derived from the owned `Chunk`.
    pub fn current_chunk_id(&self) -> Option<ChunkId> {
        self.chunk.as_ref().and_then(|c| c.id)
    }

    fn make_strip_writer(&self, chunk: Arc<Chunk>, index: u32) -> Result<StripWriter> {
        let strip = chunk
            .strips
            .get(index as usize)
            .ok_or_else(|| IoError::AllocationFailed("strip index is absent".into()))?;
        match &strip.strip {
            Some(Strip::MirrorStrip(_)) => {
                Ok(StripWriter::Mirror(MirrorStripWriter::new_with_io_concurrency(
                    chunk,
                    index,
                    Arc::clone(&self.disk_writer),
                    Arc::clone(&self.mirror_io_concurrency),
                )))
            }
            Some(Strip::EcStrip(ec)) if ec.data_num > 0 && ec.code_num > 0 => {
                let scheme = EcScheme::new(ec.data_num as usize, ec.code_num as usize);
                Ok(StripWriter::Ec(EcStripWriter::new(
                    chunk,
                    index,
                    Arc::clone(&self.disk_writer),
                    scheme,
                )))
            }
            _ => Err(IoError::AllocationFailed("unsupported strip layout".into())),
        }
    }

    /// Strips opened in the current chunk so far (= write_cursor + 1
    /// when a chunk is open).
    pub fn strips_in_chunk(&self) -> u32 {
        if self.chunk.is_some() {
            self.write_cursor + 1
        } else {
            0
        }
    }
}
