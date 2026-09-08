// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Single-owner shared chunk worker and whole-object batch commit barrier.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use crowdb_protocol::chunkdb::rpc::{
    AdvanceChunkWriteRequest, AllocateChunkRequest, AppendChunkRequest, Chunk, ChunkType, DeleteChunkRequest,
    Location, SealChunkRequest, Strip, StripType,
};
use tokio::sync::{mpsc, Notify};

use crate::config::SmallWritePolicy;
use crate::metrics::SmallWriteMetrics;
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
    let owned = OwnedChunk::allocate(&runtime).await?;
    let (sender, receiver) = mpsc::channel(runtime.policy.queue_capacity);
    let route = Arc::new(PipelineRoute::new(sender, runtime.now_ms()));
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
}

impl PipelineWorker {
    async fn run(mut self) -> Result<()> {
        loop {
            if self.retire.load(Ordering::Acquire) {
                self.receiver.close();
            }
            let first = if let Some(object) = self.carry.take() {
                Some(object)
            } else if self.retire.load(Ordering::Acquire) {
                self.receiver.recv().await
            } else {
                tokio::select! {
                    object = self.receiver.recv() => object,
                    () = self.wake.notified() => {
                        self.receiver.close();
                        self.receiver.recv().await
                    }
                }
            };
            let Some(first) = first else {
                break;
            };
            self.note_dequeue(&first);
            if let Err(error) = self.ensure_object_fits(first.len).await {
                fail_one(first, &error.to_string(), &self.runtime.metrics);
                self.fail_remaining(&error.to_string()).await;
                let _ = self.finish_chunks().await;
                return Err(error);
            }
            let batch = self.collect_batch(first).await;
            self.route.busy.store(true, Ordering::Release);
            let result = self.chunk.write_batch(batch, &self.runtime.metrics).await;
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
                None => OwnedChunk::allocate(&self.runtime).await?,
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
            self.replacement = OwnedChunk::allocate(&self.runtime).await.ok();
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

    async fn collect_batch(&mut self, first: PendingObject) -> Vec<PendingObject> {
        let deadline = tokio::time::Instant::now() + self.runtime.policy.batch_deadline;
        let mut bytes = first.len;
        let mut batch = vec![first];
        while batch.len() < self.runtime.policy.max_batch_objects
            && bytes < self.runtime.policy.max_batch_bytes
        {
            let Ok(Some(next)) = tokio::time::timeout_at(deadline, self.receiver.recv()).await else {
                break;
            };
            self.note_dequeue(&next);
            let limit = self
                .runtime
                .policy
                .max_batch_bytes
                .min(usize::try_from(self.chunk.remaining_in_strip()).unwrap_or(usize::MAX));
            if bytes.saturating_add(next.len) > limit {
                self.carry = Some(next);
                break;
            }
            bytes += next.len;
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

struct OwnedChunk {
    allocator: Arc<dyn ChunkAllocator>,
    disk_writer: Arc<dyn DiskWriter>,
    policy: Arc<SmallWritePolicy>,
    chunk: Chunk,
    cursor: u64,
    writer_epoch: u64,
}

impl OwnedChunk {
    async fn allocate(runtime: &SmallPoolRuntime) -> Result<Self> {
        let writer_epoch = next_writer_epoch();
        let lease_ms = u64::try_from(runtime.policy.writer_lease.as_millis()).unwrap_or(u64::MAX);
        let response = runtime
            .allocator
            .allocate_chunk(AllocateChunkRequest {
                chunk_id: None,
                write_granularity: 1024,
                strip_count: 1,
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
            .ok_or_else(|| IoError::AllocationFailed("shared chunk has no strip at cursor".into()))
    }

    fn remaining_in_strip(&self) -> u64 {
        self.current_strip().map_or(0, |strip| {
            let end = u64::from(strip.chunk_offset.saturating_add(strip.capacity)) * 1024;
            end.saturating_sub(self.cursor)
        })
    }

    async fn ensure_strip(&mut self) -> Result<()> {
        if self.current_strip().is_ok() {
            return Ok(());
        }
        let last = self
            .chunk
            .strips
            .last()
            .ok_or_else(|| IoError::AllocationFailed("shared chunk has no initial strip".into()))?;
        let unit_count = last.capacity.checked_div(last.unit_kb).unwrap_or(0).max(1);
        let chunk_id = self
            .chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("shared chunk missing id".into()))?;
        let response = self
            .allocator
            .append_chunk(AppendChunkRequest {
                chunk_id: Some(chunk_id),
                modify_ts: self.chunk.modify_ts,
                strip_size: unit_count,
                strip_count: 1,
                strip_type: StripType::Mirror as i32,
                data_num: 0,
                code_num: 0,
                copy_count: self.policy.mirror_copies,
            })
            .await?;
        if let Some(chunk) = response.chunk {
            self.chunk = chunk;
        } else {
            self.chunk.modify_ts = response.modify_ts;
            self.chunk.capacity = self
                .chunk
                .capacity
                .saturating_add(response.strips.iter().map(|strip| strip.capacity).sum::<u32>());
            self.chunk.strips.extend(response.strips);
        }
        Ok(())
    }

    async fn close_strip(&mut self, metrics: &SmallWriteMetrics) -> Result<()> {
        let strip = self.current_strip()?.clone();
        let strip_end = u64::from(strip.chunk_offset.saturating_add(strip.capacity)) * 1024;
        let tail = strip_end.saturating_sub(self.cursor);
        if tail > 0 {
            let unit_bytes = u64::from(strip.unit_kb) * 1024;
            self.write_mirrors(
                &strip,
                self.cursor - u64::from(strip.chunk_offset) * 1024,
                Bytes::from(vec![0; tail as usize]),
                unit_bytes,
            )
            .await?;
            metrics.tail_waste_bytes.fetch_add(tail, Ordering::Relaxed);
        }
        self.advance(strip_end, Some(strip.strip_sequence)).await
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
        let logical_bytes: usize = batch.iter().map(|object| object.len).sum();
        let physical_bytes = align_up(logical_bytes as u64, unit_bytes)? as usize;
        if physical_bytes as u64 > self.remaining_in_strip() {
            return Err(IoError::Internal("assembled batch crosses mirror strip".into()));
        }
        let start = self.cursor;
        let mut buffer = BytesMut::zeroed(physical_bytes);
        let mut copied = 0;
        let mut locations = Vec::with_capacity(batch.len());
        let chunk_id = self
            .chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("shared chunk missing id".into()))?;
        for object in batch {
            let object_start = copied;
            for fragment in &object.fragments {
                buffer[copied..copied + fragment.len()].copy_from_slice(fragment);
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
        let segment_offset = start - u64::from(strip.chunk_offset) * 1024;
        self.write_mirrors(&strip, segment_offset, buffer.freeze(), unit_bytes)
            .await?;
        let end = start + physical_bytes as u64;
        let strip_end = u64::from(strip.chunk_offset.saturating_add(strip.capacity)) * 1024;
        let closed = (end == strip_end).then_some(strip.strip_sequence);
        self.advance(end, closed).await?;
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

    async fn write_mirrors(
        &self,
        strip: &crowdb_protocol::chunkdb::rpc::ChunkStrip,
        segment_offset: u64,
        data: Bytes,
        unit_bytes: u64,
    ) -> Result<()> {
        let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
            return Err(IoError::Internal("shared chunk strip is not mirrored".into()));
        };
        let mut write_tasks = tokio::task::JoinSet::new();
        for segment in &mirror.segments {
            let segment = *segment;
            let disk_writer = Arc::clone(&self.disk_writer);
            let data = data.clone();
            write_tasks.spawn(async move {
                disk_writer
                    .write_at(&segment, unit_bytes, segment_offset, data)
                    .await
            });
        }
        while let Some(result) = write_tasks.join_next().await {
            result.map_err(|error| IoError::WriteFailed(format!("mirror writer task failed: {error}")))??;
        }
        Ok(())
    }

    async fn advance(&mut self, cursor: u64, closed_strip_sequence: Option<u32>) -> Result<()> {
        let chunk_id = self
            .chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("shared chunk missing id".into()))?;
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
            .await?;
        self.chunk = response
            .chunk
            .ok_or_else(|| IoError::AllocationFailed("cursor advance returned no chunk".into()))?;
        self.cursor = cursor;
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        let Some(chunk_id) = self.chunk.id else {
            return Ok(());
        };
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
}

fn align_up(value: u64, alignment: u64) -> Result<u64> {
    value
        .checked_add(alignment.saturating_sub(1))
        .map(|sum| sum / alignment * alignment)
        .ok_or_else(|| IoError::Internal("batch alignment overflow".into()))
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
