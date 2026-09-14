// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `LargeAsyncObjectWriter` — async stream variant, chunk-level drive
//! loop.
//!
//! Accepts an `AsyncRead` stream — a more complex flow with a fetch
//! stage + backpressure. The strip-level drive loop is in
//! `ChunkWriter::push` (auto-rotates strips). This writer owns the
//! chunk-level drive loop: pulls `Chunk` values from `ChunkPrefetch`,
//! opens `ChunkWriter` with `object_size`, pushes blocks, rotates
//! chunks when `is_full()`, seals at EOF. Implements `ChunkIoWriter`
//! for push mode.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use crowdb_protocol::frame::{encode_frame, FrameMagic, MAX_FRAME_PAYLOAD_BYTES};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::chunk::chunk_prefetch::ChunkPrefetch;
use crate::chunk::chunk_writer::ChunkWriter;
use crate::config::ChunkClientConfig;
use crate::disk_io::DiskWriter;
use crate::io::{ChunkIoWriter, FeedStatus};
use crate::metrics::LargeWriteRepairMetrics;
use crate::negative_list::FailedDiskList;
use crate::traits::ChunkAllocator;
use crate::writer::fetch::run_fetch_stage;
use crate::{IoError, Result};
use crowdb_common::ec::EcScheme;
use crowdb_protocol::chunkdb::rpc::{Chunk, DeleteChunkRequest, Location as ProtoLocation};

/// Large-object writer — async stream. Owns the chunk-level drive
/// loop + fetch stage; strip-level rotation is in `ChunkWriter::push`.
pub struct LargeAsyncObjectWriter {
    pub(crate) allocator: Arc<dyn ChunkAllocator>,
    pub(crate) disk_writer: Arc<dyn DiskWriter>,
    pub(crate) ec_scheme: EcScheme,
    pub(crate) config: Arc<ChunkClientConfig>,
    pub(crate) chunk_writer: Option<ChunkWriter>,
    pub(crate) chunk_prefetch_rx: Option<mpsc::Receiver<Result<Chunk>>>,
    pub(crate) chunk_prefetch_handle: Option<JoinHandle<()>>,
    pub(crate) prepared_chunk: Option<Chunk>,
    pub(crate) locations: Vec<ProtoLocation>,
    pub(crate) logical_offset: u64,
    pub(crate) logical_bytes_in_chunk: u64,
    pub(crate) frame_tail: BytesMut,
    pub(crate) object_size: Option<u64>,
    pub(crate) finished: bool,
    pub(crate) preparation_stalls: u64,
    pub(crate) preparation_stall_time: Duration,
    pub(crate) source_reads: u64,
    pub(crate) source_read_time: Duration,
    pub(crate) assembly_copies: u64,
    pub(crate) assembly_copy_bytes: u64,
    pub(crate) assembly_copy_time: Duration,
    pub(crate) ec_encode_time: Duration,
    pub(crate) completion_wait_time: Duration,
    pub(crate) failed_disks: Arc<FailedDiskList>,
    pub(crate) repair_metrics: Arc<LargeWriteRepairMetrics>,
}

impl LargeAsyncObjectWriter {
    /// Construct a new writer.
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
            chunk_writer: None,
            chunk_prefetch_rx: None,
            chunk_prefetch_handle: None,
            prepared_chunk: None,
            locations: Vec::new(),
            logical_offset: 0,
            logical_bytes_in_chunk: 0,
            frame_tail: BytesMut::new(),
            object_size: None,
            finished: false,
            preparation_stalls: 0,
            preparation_stall_time: Duration::ZERO,
            source_reads: 0,
            source_read_time: Duration::ZERO,
            assembly_copies: 0,
            assembly_copy_bytes: 0,
            assembly_copy_time: Duration::ZERO,
            ec_encode_time: Duration::ZERO,
            completion_wait_time: Duration::ZERO,
            failed_disks,
            repair_metrics,
        }
    }

    /// Per-writer memory footprint.
    pub fn per_writer_memory(&self) -> usize {
        self.config.per_writer_memory(&self.ec_scheme)
    }

    /// Number of times the data path waited for chunk preparation.
    pub fn preparation_stalls(&self) -> u64 {
        self.preparation_stalls
    }

    /// Total time the data path waited for chunk preparation.
    pub fn preparation_stall_time(&self) -> Duration {
        self.preparation_stall_time
    }

    /// Start bounded chunk preparation before source consumption begins.
    pub(crate) fn prepare(&mut self, object_size: Option<u64>) {
        self.object_size = object_size;
        let prefetch = ChunkPrefetch::new(
            self.allocator.clone(),
            self.ec_scheme,
            self.config.clone(),
            crowdb_protocol::chunk_id::CHUNK_TYPE_REPO,
        );
        let (chunk_rx, prefetch_handle) = prefetch.spawn(object_size);
        self.chunk_prefetch_rx = Some(chunk_rx);
        self.chunk_prefetch_handle = Some(prefetch_handle);
    }

    /// Wait until the first chunk is allocated without opening the writer.
    /// Applications use this before admitting load so initial placement is not
    /// charged to the data path.
    pub(crate) async fn wait_until_prepared(&mut self) -> Result<()> {
        if self.prepared_chunk.is_some() || self.chunk_writer.is_some() {
            return Ok(());
        }
        let rx = self
            .chunk_prefetch_rx
            .as_mut()
            .ok_or_else(|| IoError::Internal("chunk preparation is not running".into()))?;
        match rx.recv().await {
            Some(Ok(chunk)) => {
                self.prepared_chunk = Some(chunk);
                Ok(())
            }
            Some(Err(error)) => Err(error),
            None => Err(IoError::Internal(
                "chunk preparation ended before producing a chunk".into(),
            )),
        }
    }

    /// Seal the current chunk (if any) and record its ProtoLocation.
    pub(crate) async fn seal_current(&mut self) -> Result<()> {
        if let Some(mut cw) = self.chunk_writer.take() {
            let location = match cw.seal().await {
                Ok(location) => location,
                Err(error) => {
                    let _ = cw.abort().await;
                    return Err(error);
                }
            };
            let (stalls, stall_time) = cw.preparation_metrics();
            self.preparation_stalls += stalls;
            self.preparation_stall_time += stall_time;
            self.ec_encode_time += cw.ec_encode_time;
            self.completion_wait_time += cw.completion_wait_time;
            if location.length > 0 {
                self.locations.push(ProtoLocation {
                    logical_offset: self.logical_offset,
                    logical_length: self.logical_bytes_in_chunk,
                    ..location
                });
                self.logical_offset += self.logical_bytes_in_chunk;
                self.logical_bytes_in_chunk = 0;
            }
        }
        Ok(())
    }

    /// Pull the next `Chunk` from the prefetch channel (or on-demand
    /// if the channel is exhausted).
    pub(crate) async fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        if let Some(chunk) = self.prepared_chunk.take() {
            return Ok(Some(chunk));
        }
        if let Some(rx) = self.chunk_prefetch_rx.as_mut() {
            match rx.try_recv() {
                Ok(result) => return result.map(Some),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    self.chunk_prefetch_rx = None;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
            }
        }
        if let Some(rx) = self.chunk_prefetch_rx.as_mut() {
            let started = Instant::now();
            self.preparation_stalls += 1;
            let result = rx.recv().await;
            self.preparation_stall_time += started.elapsed();
            match result {
                Some(Ok(c)) => return Ok(Some(c)),
                Some(Err(e)) => return Err(e),
                None => {
                    self.chunk_prefetch_rx = None;
                }
            }
        }
        // On-demand allocation (prefetch channel exhausted).
        let pf = ChunkPrefetch::new(
            self.allocator.clone(),
            self.ec_scheme,
            self.config.clone(),
            crowdb_protocol::chunk_id::CHUNK_TYPE_REPO,
        );
        let started = Instant::now();
        self.preparation_stalls += 1;
        let chunk = pf.on_demand().await?;
        self.preparation_stall_time += started.elapsed();
        Ok(Some(chunk))
    }

    /// Ensure a `ChunkWriter` is open. If none, pull the next `Chunk`
    /// + open a new `ChunkWriter` with `object_size`.
    pub(crate) async fn ensure_open(&mut self) -> Result<()> {
        if self.chunk_writer.is_some() {
            return Ok(());
        }
        let chunk = self
            .next_chunk()
            .await?
            .ok_or_else(|| IoError::Internal("no chunk available".into()))?;
        let mut cw = ChunkWriter::new_with_repair(
            self.allocator.clone(),
            self.disk_writer.clone(),
            self.ec_scheme,
            self.config.clone(),
            Arc::clone(&self.failed_disks),
            Arc::clone(&self.repair_metrics),
        );
        cw.open(chunk, self.object_size)?;
        self.chunk_writer = Some(cw);
        Ok(())
    }

    /// Rotate: seal the current chunk, pull the next `Chunk`, open a
    /// new `ChunkWriter`.
    pub(crate) async fn rotate_chunk(&mut self) -> Result<()> {
        self.seal_current().await?;
        self.ensure_open().await
    }

    async fn stop_chunk_prefetch(&mut self) {
        if let Some(handle) = self.chunk_prefetch_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
        if let Some(chunk) = self.prepared_chunk.take() {
            if let Some(chunk_id) = chunk.id {
                let _ = self
                    .allocator
                    .delete_chunk(DeleteChunkRequest {
                        chunk_id: Some(chunk_id),
                    })
                    .await;
            }
        }
        if let Some(mut rx) = self.chunk_prefetch_rx.take() {
            while let Ok(result) = rx.try_recv() {
                if let Ok(chunk) = result {
                    if let Some(chunk_id) = chunk.id {
                        let _ = self
                            .allocator
                            .delete_chunk(DeleteChunkRequest {
                                chunk_id: Some(chunk_id),
                            })
                            .await;
                    }
                }
            }
        }
    }

    /// Async stream write. Runs fetch stage + chunk-level drive loop
    /// concurrently. The strip-level drive loop is in
    /// `ChunkWriter::push` (auto-rotates strips).
    pub async fn write_stream(
        &mut self,
        reader: impl tokio::io::AsyncRead + Unpin + Send,
        object_size: Option<u64>,
    ) -> Result<Vec<ProtoLocation>> {
        if self.finished {
            return Err(IoError::Finished);
        }
        self.finished = true;

        if object_size == Some(0) {
            return Ok(Vec::new());
        }

        if self.chunk_prefetch_rx.is_none() {
            self.prepare(object_size);
        }

        let channel_cap = (self.config.max_cached_buffer / self.config.read_buffer_size).max(1);
        let (block_tx, mut block_rx) = mpsc::channel::<Bytes>(channel_cap);
        let fetch_fut = run_fetch_stage(reader, block_tx, self.config.read_buffer_size);

        let drive_fut = async {
            loop {
                // Ensure a ChunkWriter is open.
                self.ensure_open().await?;
                // Receive the next block from the fetch stage.
                match block_rx.recv().await {
                    Some(buffer) => {
                        self.push_buffer(buffer).await?;
                    }
                    None => {
                        // EOF — break out, seal the current chunk.
                        break;
                    }
                }
            }
            if !self.frame_tail.is_empty() {
                let tail = self.frame_tail.split().freeze();
                self.push_payload_frame(tail).await?;
            }
            Ok::<(), IoError>(())
        };

        let pipeline_result = tokio::try_join!(
            async {
                fetch_fut
                    .await
                    .map_err(|error| IoError::SourceRead(error.to_string()))
            },
            drive_fut,
        );
        let (fetch_stats, ()) = match pipeline_result {
            Ok(result) => result,
            Err(error) => {
                let _ = self.abort_pipeline().await;
                return Err(error);
            }
        };
        self.source_reads += fetch_stats.source_reads;
        self.source_read_time += fetch_stats.source_read_time;
        self.assembly_copies += fetch_stats.assembly_copies;
        self.assembly_copy_bytes += fetch_stats.assembly_copy_bytes;
        self.assembly_copy_time += fetch_stats.assembly_copy_time;

        self.seal_current().await?;
        self.stop_chunk_prefetch().await;

        Ok(std::mem::take(&mut self.locations))
    }

    /// Abort: cancel in-flight, return already-sealed Locations.
    pub(crate) async fn abort_pipeline(&mut self) -> Result<Vec<ProtoLocation>> {
        if let Some(mut cw) = self.chunk_writer.take() {
            let _ = cw.abort().await;
        }
        self.stop_chunk_prefetch().await;
        Ok(std::mem::take(&mut self.locations))
    }
}

#[async_trait::async_trait]
impl ChunkIoWriter for LargeAsyncObjectWriter {
    async fn on_data(&mut self, buffer: Bytes) -> Result<FeedStatus> {
        if self.finished {
            return Err(IoError::Finished);
        }
        // Lazy-start prefetch on first push.
        if self.chunk_prefetch_rx.is_none() && self.chunk_writer.is_none() {
            let prefetch = ChunkPrefetch::new(
                self.allocator.clone(),
                self.ec_scheme,
                self.config.clone(),
                crowdb_protocol::chunk_id::CHUNK_TYPE_REPO,
            );
            let (rx, handle) = prefetch.spawn(None);
            self.chunk_prefetch_rx = Some(rx);
            self.chunk_prefetch_handle = Some(handle);
        }
        self.push_buffer(buffer).await?;
        Ok(FeedStatus::Continue)
    }

    async fn on_finish(&mut self) -> Result<Vec<ProtoLocation>> {
        if self.finished {
            return Err(IoError::Finished);
        }
        self.finished = true;
        if !self.frame_tail.is_empty() {
            let tail = self.frame_tail.split().freeze();
            self.push_payload_frame(tail).await?;
        }
        self.seal_current().await?;
        self.stop_chunk_prefetch().await;
        Ok(std::mem::take(&mut self.locations))
    }

    async fn on_error(&mut self) -> Result<Vec<ProtoLocation>> {
        self.finished = true;
        self.abort_pipeline().await
    }

    fn require_data(&self) -> bool {
        if self.finished {
            return false;
        }
        self.chunk_writer.as_ref().map_or(true, ChunkWriter::ready)
    }
}

impl LargeAsyncObjectWriter {
    async fn push_buffer(&mut self, buffer: Bytes) -> Result<()> {
        self.frame_tail.extend_from_slice(&buffer);
        while self.frame_tail.len() >= MAX_FRAME_PAYLOAD_BYTES {
            let payload = self.frame_tail.split_to(MAX_FRAME_PAYLOAD_BYTES).freeze();
            self.push_payload_frame(payload).await?;
        }
        Ok(())
    }

    async fn push_payload_frame(&mut self, payload: Bytes) -> Result<()> {
        loop {
            self.ensure_open().await?;
            let chunk_id = self
                .chunk_writer
                .as_ref()
                .and_then(ChunkWriter::current_chunk_id)
                .ok_or_else(|| IoError::Internal("large async writer has no chunk ID".into()))?;
            let write_time_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |duration| {
                    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
                });
            let frame = encode_frame(FrameMagic::RepoLargeV1, chunk_id, &payload, write_time_ms)
                .map_err(|error| IoError::WriteFailed(error.to_string()))?;
            let status = self
                .chunk_writer
                .as_mut()
                .ok_or_else(|| IoError::Internal("large async writer has no chunk writer".into()))?
                .push(Bytes::from(frame))
                .await?;
            if status == FeedStatus::Pause {
                self.rotate_chunk().await?;
                continue;
            }
            self.logical_bytes_in_chunk = self.logical_bytes_in_chunk.saturating_add(payload.len() as u64);
            return Ok(());
        }
    }
}
