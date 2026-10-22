// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use crowdb_protocol::chunk_stream::{
    StreamBinding, StreamBindingState, StreamExtentPage, StreamExtentPageFence, StreamManifest, StreamName,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{FrameMagic, FRAME_FOOTER_BYTES};
use tokio::sync::{mpsc, oneshot};

use crate::extent_cache::ExtentPageCache;
use crate::metadata::validate_manifest;
use crate::metrics::{StreamMetrics, StreamMetricsSnapshot};
use crate::mirror_shadow::MirrorShadow;
use crate::storage::{StreamChunkStore, StreamMetadataStore, StreamRegistry};
use crate::{Result, StreamError};

mod append;
mod capacity;
mod liveness;
mod observation;
mod publication;
mod read;
mod recovery;
mod worker;
pub use observation::StreamMetadataObservation;
pub use read::StreamReader;

use crate::perf::{observe, perf};
use publication::{build_extent_pages, fences_for};
use worker::run_worker;

use capacity::rollover;

#[derive(Clone, Debug)]
pub struct StreamConfig {
    pub chunk_capacity_bytes: u64,
    pub queue_requests: usize,
    pub queue_bytes: u64,
    pub batch_requests: usize,
    pub batch_bytes: usize,
    pub max_append_bytes: usize,
    pub extent_page_entries: usize,
    pub read_window_bytes: usize,
    pub read_request_bytes: usize,
    pub read_concurrency: usize,
    pub gc_bytes_per_pass: u64,
    pub watchdog_interval: Duration,
    pub liveness_interval: Duration,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            chunk_capacity_bytes: crowdb_chunk_client::STREAM_CHUNK_BYTES,
            queue_requests: 1_024,
            queue_bytes: 64 * 1024 * 1024,
            batch_requests: 64,
            batch_bytes: 1024 * 1024,
            max_append_bytes: 64 * 1024 * 1024,
            extent_page_entries: 1_024,
            read_window_bytes: 8 * 1024 * 1024,
            read_request_bytes: 1024 * 1024,
            read_concurrency: 8,
            gc_bytes_per_pass: 64 * 1024 * 1024,
            watchdog_interval: Duration::from_millis(500),
            liveness_interval: Duration::from_secs(12 * 60),
        }
    }
}

impl StreamConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.queue_requests == 0
            || !(1024 * 1024..=crowdb_chunk_client::STREAM_CHUNK_BYTES).contains(&self.chunk_capacity_bytes)
            || self.queue_bytes == 0
            || self.batch_requests == 0
            || self.batch_bytes == 0
            || self.max_append_bytes == 0
            || self.extent_page_entries == 0
            || self.read_window_bytes == 0
            || self.read_request_bytes == 0
            || self.read_concurrency == 0
            || self.gc_bytes_per_pass == 0
            || self.watchdog_interval.is_zero()
            || self.liveness_interval.is_zero()
            || self.batch_bytes > self.max_append_bytes
            || self.queue_bytes < self.max_append_bytes as u64
        {
            return Err(StreamError::InvalidRequest(
                "stream bounds must be nonzero and consistent".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendRange {
    pub stream_name: StreamName,
    pub chunk_id: Option<ChunkId>,
    pub begin: u64,
    pub end: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadSegment {
    pub chunk_id: ChunkId,
    pub logical_start: u64,
    pub data: Bytes,
}

#[derive(Clone, Copy)]
struct PhysicalRead {
    chunk_id: ChunkId,
    logical_start: u64,
    frame_start: u64,
    frame_length: usize,
    payload_start: usize,
    length: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadHint {
    Bytes(u64),
    ToEnd,
}

struct AppendRequest {
    data: Bytes,
    enqueued_at: Instant,
    completion: oneshot::Sender<Result<AppendRange>>,
}

impl AppendRequest {
    fn logical_len(&self) -> usize {
        self.data.len()
    }

    fn physical_len(&self) -> Result<usize> {
        let header = FrameMagic::StreamV1.ordinary_header_bytes();
        let count = self
            .data
            .len()
            .div_ceil(FrameMagic::StreamV1.ordinary_payload_capacity());
        count
            .checked_mul(header + FRAME_FOOTER_BYTES)
            .and_then(|overhead| self.data.len().checked_add(overhead))
            .ok_or_else(|| StreamError::InvalidRequest("framed append exceeds address space".into()))
    }
}

enum Command {
    Append(AppendRequest),
    Trim {
        offset: u64,
        completion: oneshot::Sender<Result<u64>>,
    },
    Close(oneshot::Sender<Result<()>>),
}

#[derive(Clone)]
pub struct ChunkStream {
    stream_name: StreamName,
    writer_epoch: u64,
    config: Arc<StreamConfig>,
    metadata: Arc<dyn StreamMetadataStore>,
    chunks: Arc<dyn StreamChunkStore>,
    sender: mpsc::Sender<Command>,
    queued_bytes: Arc<AtomicU64>,
    queued_requests: Arc<AtomicUsize>,
    tail: Arc<AtomicU64>,
    manifest: Arc<ArcSwap<StreamManifest>>,
    closed: Arc<AtomicBool>,
    metrics: Arc<StreamMetrics>,
    extent_cache: Arc<ExtentPageCache>,
}

#[derive(Clone)]
struct Extent {
    chunk_id: ChunkId,
    logical_start: u64,
    logical_end: u64,
    frame_start: u64,
    frame_length: u32,
}

struct WorkerState {
    stream_name: StreamName,
    writer_epoch: u64,
    config: Arc<StreamConfig>,
    metadata: Arc<dyn StreamMetadataStore>,
    chunks: Arc<dyn StreamChunkStore>,
    manifest_view: Arc<ArcSwap<StreamManifest>>,
    tail_view: Arc<AtomicU64>,
    queued_bytes: Arc<AtomicU64>,
    queued_requests: Arc<AtomicUsize>,
    closed_view: Arc<AtomicBool>,
    manifest: StreamManifest,
    extents: Vec<Extent>,
    mirror_shadow: MirrorShadow,
    stalled: bool,
    last_unavailable_log: Option<Instant>,
    metrics: Arc<StreamMetrics>,
}

/// Internal classification that permits recovery only before a failed mirror
/// write reaches the fenced cursor advance. Metadata and cursor failures are
/// deliberately not replayed.
enum BatchFailure {
    MirrorWrite(StreamError),
    Other(StreamError),
}

impl ChunkStream {
    /// Durable chunk purpose of this stream.
    #[must_use]
    pub fn purpose(&self) -> crowdb_protocol::chunk_stream::StreamPurpose {
        self.manifest.load().purpose
    }

    #[must_use]
    pub fn stream_name(&self) -> StreamName {
        self.stream_name
    }

    /// Creates and publishes a new registered stream.
    ///
    /// # Errors
    ///
    /// Returns a typed storage, validation, fencing, or duplicate-name error.
    pub async fn create(
        binding: StreamBinding,
        writer_epoch: u64,
        config: StreamConfig,
        registry: Arc<dyn StreamRegistry>,
        metadata: Arc<dyn StreamMetadataStore>,
        chunks: Arc<dyn StreamChunkStore>,
    ) -> Result<Self> {
        config.validate()?;
        if binding.state != StreamBindingState::Active
            || binding.metadata_group_id == 0
            || binding.binding_generation == 0
            || writer_epoch == 0
        {
            return Err(StreamError::InvalidRequest(
                "stream binding or writer epoch is invalid".into(),
            ));
        }
        if registry.load(binding.stream_name).await?.is_some() {
            return Err(StreamError::InvalidRequest("stream already exists".into()));
        }
        registry.create(binding.clone()).await?;
        Self::initialize(binding, writer_epoch, config, metadata, chunks).await
    }

    /// Initializes stream metadata for an active binding already published by
    /// the control plane.
    ///
    /// # Errors
    ///
    /// Returns a typed error if the binding is absent or inactive, metadata
    /// already exists, or initial metadata publication fails.
    pub async fn create_registered(
        stream_name: StreamName,
        writer_epoch: u64,
        config: StreamConfig,
        registry: Arc<dyn StreamRegistry>,
        metadata: Arc<dyn StreamMetadataStore>,
        chunks: Arc<dyn StreamChunkStore>,
    ) -> Result<Self> {
        config.validate()?;
        let binding = registry
            .load(stream_name)
            .await?
            .ok_or_else(|| StreamError::InvalidRequest("stream binding does not exist".into()))?;
        if binding.state != StreamBindingState::Active
            || binding.metadata_group_id == 0
            || binding.binding_generation == 0
            || writer_epoch == 0
        {
            return Err(StreamError::InvalidRequest(
                "stream binding or writer epoch is invalid".into(),
            ));
        }
        if metadata.load_current(stream_name).await?.is_some() {
            return Err(StreamError::InvalidRequest(
                "stream metadata already exists".into(),
            ));
        }
        Self::initialize(binding, writer_epoch, config, metadata, chunks).await
    }

    async fn initialize(
        binding: StreamBinding,
        writer_epoch: u64,
        config: StreamConfig,
        metadata: Arc<dyn StreamMetadataStore>,
        chunks: Arc<dyn StreamChunkStore>,
    ) -> Result<Self> {
        let manifest = StreamManifest {
            purpose: binding.purpose,
            stream_name: binding.stream_name,
            metadata_group_id: binding.metadata_group_id,
            writer_epoch,
            generation: 1,
            trim_offset: 0,
            sealed_tail: 0,
            active: None,
            extent_pages: Vec::new(),
            previous_generation: None,
            closed: false,
        };
        metadata.publish(None, manifest.clone(), Vec::new()).await?;
        Self::start(config, metadata, chunks, manifest, Vec::new(), &[])
    }

    /// Opens an existing stream and recovers its durable active cursor.
    ///
    /// # Errors
    ///
    /// Returns a typed storage, corruption, or stale-writer error.
    pub async fn open(
        stream_name: StreamName,
        writer_epoch: u64,
        config: StreamConfig,
        registry: Arc<dyn StreamRegistry>,
        metadata: Arc<dyn StreamMetadataStore>,
        chunks: Arc<dyn StreamChunkStore>,
    ) -> Result<Self> {
        config.validate()?;
        let binding = registry
            .load(stream_name)
            .await?
            .ok_or_else(|| StreamError::InvalidRequest("stream binding does not exist".into()))?;
        if binding.state != StreamBindingState::Active {
            return Err(StreamError::InvalidRequest("stream binding is not active".into()));
        }
        let mut manifest = metadata
            .load_current(stream_name)
            .await?
            .ok_or_else(|| StreamError::Corruption("stream manifest does not exist".into()))?;
        if manifest.purpose != binding.purpose {
            return Err(StreamError::Corruption(
                "stream binding purpose differs from manifest".into(),
            ));
        }
        if manifest.metadata_group_id != binding.metadata_group_id || manifest.writer_epoch > writer_epoch {
            return Err(StreamError::StaleWriter);
        }
        let pages = load_extent_pages(metadata.as_ref(), &manifest).await?;
        validate_manifest(&manifest, &pages)?;
        let expected = (manifest.writer_epoch, manifest.generation);
        let mut needs_publish = manifest.writer_epoch < writer_epoch;
        let mut rotate_active = false;
        let mut extents = collect_extents(&pages);
        let mut tail = manifest.sealed_tail;
        if let Some(active) = &mut manifest.active {
            let durable = chunks.durable_cursor(active.chunk_id, writer_epoch).await?;
            if let Some(capacity) = chunks.current_capacity(active.chunk_id).await? {
                active.capacity = capacity;
            }
            if durable.offset < active.physical_start || durable.offset > active.capacity {
                return Err(StreamError::Corruption(
                    "recovered active cursor is outside chunk bounds".into(),
                ));
            }
            if durable.offset < active.acknowledged_cursor {
                return Err(StreamError::Corruption(
                    "recovered active cursor precedes its published cursor".into(),
                ));
            }
            tail = recovery::recover_active_frames(chunks.as_ref(), active, durable.offset, &mut extents)
                .await?;
            if !durable.sealed {
                chunks.seal(active.chunk_id, writer_epoch, durable.offset).await?;
            }
            active.acknowledged_cursor = durable.offset;
            rotate_active = true;
        }
        manifest.writer_epoch = writer_epoch;
        if rotate_active {
            manifest.active.take();
            manifest.sealed_tail = tail;
            let mut successor = chunks
                .allocate_mirrored(stream_name, writer_epoch, manifest.purpose)
                .await?;
            successor.logical_start = tail;
            manifest.active = Some(successor);
            needs_publish = true;
        }
        if needs_publish {
            manifest.previous_generation = Some(manifest.generation);
            manifest.generation = manifest
                .generation
                .checked_add(1)
                .ok_or_else(|| StreamError::Corruption("manifest generation overflows".into()))?;
            let adopted_pages = build_extent_pages(
                &extents,
                manifest.stream_name,
                writer_epoch,
                manifest.generation,
                config.extent_page_entries,
            );
            manifest.extent_pages = fences_for(&adopted_pages);
            validate_manifest(&manifest, &adopted_pages)?;
            metadata
                .publish(Some(expected), manifest.clone(), adopted_pages)
                .await?;
        }
        let stream = Self::start(config, metadata, chunks, manifest, extents, &pages)?;
        stream.tail.store(tail, Ordering::Release);
        Ok(stream)
    }

    /// Opens one immutable view at the exact current writer epoch.
    ///
    /// The handle observes the durable active cursor without sealing, rotating,
    /// adopting, or publishing the stream. Append and trim remain disabled.
    ///
    /// # Errors
    ///
    /// Returns a typed storage, corruption, or epoch error.
    pub async fn open_read_only(
        stream_name: StreamName,
        writer_epoch: u64,
        config: StreamConfig,
        registry: Arc<dyn StreamRegistry>,
        metadata: Arc<dyn StreamMetadataStore>,
        chunks: Arc<dyn StreamChunkStore>,
    ) -> Result<Self> {
        config.validate()?;
        let binding = registry
            .load(stream_name)
            .await?
            .ok_or_else(|| StreamError::InvalidRequest("stream binding does not exist".into()))?;
        if binding.state != StreamBindingState::Active || writer_epoch == 0 {
            return Err(StreamError::InvalidRequest(
                "stream binding or reader epoch is invalid".into(),
            ));
        }
        let mut manifest = metadata
            .load_current(stream_name)
            .await?
            .ok_or_else(|| StreamError::Corruption("stream manifest does not exist".into()))?;
        if manifest.purpose != binding.purpose {
            return Err(StreamError::Corruption(
                "stream binding purpose differs from manifest".into(),
            ));
        }
        if manifest.metadata_group_id != binding.metadata_group_id || manifest.writer_epoch != writer_epoch {
            return Err(StreamError::StaleWriter);
        }
        let pages = load_extent_pages(metadata.as_ref(), &manifest).await?;
        validate_manifest(&manifest, &pages)?;
        if let Some(active) = &mut manifest.active {
            let durable = chunks.durable_cursor(active.chunk_id, writer_epoch).await?;
            if let Some(capacity) = chunks.current_capacity(active.chunk_id).await? {
                active.capacity = capacity;
            }
            if durable.offset < active.physical_start || durable.offset > active.capacity {
                return Err(StreamError::Corruption(
                    "observed active cursor is outside chunk bounds".into(),
                ));
            }
            if durable.offset < active.acknowledged_cursor {
                return Err(StreamError::Corruption(
                    "observed active cursor precedes the published cursor".into(),
                ));
            }
            // A concurrent append can advance the physical cursor before its
            // extent pages are published. Keep this reader at the captured
            // manifest's acknowledged boundary.
        }
        validate_manifest(&manifest, &pages)?;
        let stream = Self::start(
            config,
            metadata,
            chunks,
            manifest,
            collect_extents(&pages),
            &pages,
        )?;
        stream.closed.store(true, Ordering::Release);
        Ok(stream)
    }

    fn start(
        config: StreamConfig,
        metadata: Arc<dyn StreamMetadataStore>,
        chunks: Arc<dyn StreamChunkStore>,
        manifest: StreamManifest,
        extents: Vec<Extent>,
        pages: &[StreamExtentPage],
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(config.queue_requests);
        let config = Arc::new(config);
        let queued_bytes = Arc::new(AtomicU64::new(0));
        let queued_requests = Arc::new(AtomicUsize::new(0));
        let tail = Arc::new(AtomicU64::new(durable_tail(&manifest)?));
        let manifest_view = Arc::new(ArcSwap::from_pointee(manifest.clone()));
        let closed = Arc::new(AtomicBool::new(manifest.closed));
        let metrics = Arc::new(StreamMetrics::default());
        perf();
        let extent_cache = Arc::new(ExtentPageCache::new(&manifest, pages));
        let state = WorkerState {
            stream_name: manifest.stream_name,
            writer_epoch: manifest.writer_epoch,
            config: Arc::clone(&config),
            metadata: Arc::clone(&metadata),
            chunks: Arc::clone(&chunks),
            manifest_view: Arc::clone(&manifest_view),
            tail_view: Arc::clone(&tail),
            queued_bytes: Arc::clone(&queued_bytes),
            queued_requests: Arc::clone(&queued_requests),
            closed_view: Arc::clone(&closed),
            manifest,
            extents,
            mirror_shadow: MirrorShadow::default(),
            stalled: false,
            last_unavailable_log: None,
            metrics: Arc::clone(&metrics),
        };
        tokio::spawn(run_worker(state, receiver));
        Ok(Self {
            stream_name: state_name(&manifest_view),
            writer_epoch: state_epoch(&manifest_view),
            config,
            metadata,
            chunks,
            sender,
            queued_bytes,
            queued_requests,
            tail,
            manifest: manifest_view,
            closed,
            metrics,
            extent_cache,
        })
    }

    /// Returns the durable end of the published read snapshot.
    ///
    /// # Panics
    ///
    /// Panics if an internally installed manifest violates the validated
    /// active-cursor bounds.
    #[must_use]
    pub fn tail(&self) -> u64 {
        durable_tail(&self.manifest.load()).expect("validated stream manifest has a durable tail")
    }

    /// Appends one logical request after its mirror data and cursor are durable.
    ///
    /// # Errors
    ///
    /// Returns a typed admission, storage, fencing, or durability error.
    pub async fn append(&self, buffers: &[Bytes]) -> Result<AppendRange> {
        if self.closed.load(Ordering::Acquire) {
            return Err(StreamError::WriteStalled);
        }
        let length = buffers
            .iter()
            .try_fold(0_usize, |total, bytes| total.checked_add(bytes.len()))
            .ok_or_else(|| StreamError::InvalidRequest("append length overflows".into()))?;
        if length > self.config.max_append_bytes {
            return Err(StreamError::InvalidRequest(
                "append exceeds maximum chunk capacity".into(),
            ));
        }
        if length == 0 {
            let tail = self.tail();
            return Ok(AppendRange {
                stream_name: self.stream_name,
                chunk_id: None,
                begin: tail,
                end: tail,
            });
        }
        let length_u64 = u64::try_from(length)
            .map_err(|_| StreamError::InvalidRequest("append length exceeds addressable range".into()))?;
        reserve_requests(&self.queued_requests, self.config.queue_requests)?;
        if let Err(error) = reserve_bytes(&self.queued_bytes, self.config.queue_bytes, length_u64) {
            self.queued_requests.fetch_sub(1, Ordering::AcqRel);
            return Err(error);
        }
        self.record_queue_high_watermarks();
        self.metrics.submitted.fetch_add(1, Ordering::Relaxed);
        let mut data = BytesMut::with_capacity(length);
        for bytes in buffers {
            data.extend_from_slice(bytes);
        }
        let (completion, result) = oneshot::channel();
        if self
            .sender
            .try_send(Command::Append(AppendRequest {
                data: data.freeze(),
                enqueued_at: Instant::now(),
                completion,
            }))
            .is_err()
        {
            self.queued_bytes.fetch_sub(length_u64, Ordering::AcqRel);
            self.queued_requests.fetch_sub(1, Ordering::AcqRel);
            self.metrics.failed.fetch_add(1, Ordering::Relaxed);
            return Err(StreamError::Backpressure);
        }
        if let Ok(result) = result.await {
            result
        } else {
            self.queued_bytes.fetch_sub(length_u64, Ordering::AcqRel);
            self.queued_requests.fetch_sub(1, Ordering::AcqRel);
            self.metrics.failed.fetch_add(1, Ordering::Relaxed);
            Err(StreamError::WriteStalled)
        }
    }

    /// Appends one journal record. The selected chunk identity and CRC are
    /// carried by its outer public Stream frame rather than a payload trailer.
    ///
    /// # Errors
    ///
    /// Returns a typed admission, storage, fencing, or durability error.
    pub async fn append_chunk_bound(&self, buffers: &[Bytes]) -> Result<AppendRange> {
        let length = buffers
            .iter()
            .try_fold(0_usize, |total, bytes| total.checked_add(bytes.len()))
            .ok_or_else(|| StreamError::InvalidRequest("stream append length overflows".into()))?;
        if length > self.config.max_append_bytes {
            return Err(StreamError::InvalidRequest(
                "chunk-bound append exceeds maximum chunk capacity".into(),
            ));
        }
        let mut data = BytesMut::with_capacity(length);
        for bytes in buffers {
            data.extend_from_slice(bytes);
        }
        self.enqueue_append(data.freeze()).await
    }

    /// Enqueues several chunk-bound records in input order and returns their
    /// exact ranges. Enqueue completes for the entire slice before awaiting
    /// results, allowing the worker to batch already-queued records.
    ///
    /// # Errors
    ///
    /// Returns a typed admission, storage, fencing, or durability error.
    pub async fn append_chunk_bound_batch(&self, records: &[Bytes]) -> Result<Vec<AppendRange>> {
        let mut pending = Vec::with_capacity(records.len());
        for record in records {
            pending.push(self.enqueue_append_request(record.clone())?);
        }
        let mut ranges = Vec::with_capacity(pending.len());
        for result in pending {
            ranges.push(result.await.map_err(|_| StreamError::WriteStalled)??);
        }
        Ok(ranges)
    }

    async fn enqueue_append(&self, data: Bytes) -> Result<AppendRange> {
        let result = self.enqueue_append_request(data)?;
        result.await.map_err(|_| StreamError::WriteStalled)?
    }

    fn enqueue_append_request(&self, data: Bytes) -> Result<oneshot::Receiver<Result<AppendRange>>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(StreamError::WriteStalled);
        }
        let length = data.len();
        if length == 0 || length > self.config.max_append_bytes {
            return Err(StreamError::InvalidRequest("append length is invalid".into()));
        }
        let length_u64 = length as u64;
        reserve_requests(&self.queued_requests, self.config.queue_requests)?;
        if let Err(error) = reserve_bytes(&self.queued_bytes, self.config.queue_bytes, length_u64) {
            self.queued_requests.fetch_sub(1, Ordering::AcqRel);
            return Err(error);
        }
        self.record_queue_high_watermarks();
        let (completion, result) = oneshot::channel();
        if self
            .sender
            .try_send(Command::Append(AppendRequest {
                data,
                enqueued_at: Instant::now(),
                completion,
            }))
            .is_err()
        {
            self.queued_bytes.fetch_sub(length_u64, Ordering::AcqRel);
            self.queued_requests.fetch_sub(1, Ordering::AcqRel);
            return Err(StreamError::Backpressure);
        }
        self.metrics.submitted.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

    /// Advances the durable logical trim point and reclaims complete extents.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid watermark or failed metadata/chunk IO.
    pub async fn trim_prefix(&self, offset: u64) -> Result<u64> {
        let (completion, result) = oneshot::channel();
        self.sender
            .send(Command::Trim { offset, completion })
            .await
            .map_err(|_| StreamError::WriteStalled)?;
        result
            .await
            .map_err(|_| StreamError::Internal("trim worker stopped".into()))?
    }

    /// Returns the immutable metadata generation currently backing reads.
    #[must_use]
    pub fn manifest_generation(&self) -> u64 {
        self.manifest.load().generation
    }

    /// Reclaims immutable extent pages older than a caller-proven retained
    /// manifest generation.
    ///
    /// # Errors
    ///
    /// Returns an error if the watermark could remove the current generation,
    /// the pass bound is zero, or metadata deletion fails.
    pub async fn reclaim_metadata_before(&self, retained_generation: u64, max_pages: usize) -> Result<u64> {
        let current = self.manifest.load().generation;
        if retained_generation == 0 || retained_generation > current || max_pages == 0 {
            return Err(StreamError::InvalidRequest(
                "metadata reclaim watermark is outside retained generations".into(),
            ));
        }
        self.metadata
            .reclaim_extent_pages_before(self.stream_name, retained_generation, max_pages)
            .await
    }

    /// Seals the active chunk and publishes a closed manifest.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer is stalled or storage publication fails.
    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let (completion, result) = oneshot::channel();
        self.sender
            .send(Command::Close(completion))
            .await
            .map_err(|_| StreamError::WriteStalled)?;
        result
            .await
            .map_err(|_| StreamError::Internal("close worker stopped".into()))?
    }
    #[must_use]
    pub fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }

    #[must_use]
    pub fn metrics(&self) -> StreamMetricsSnapshot {
        self.metrics.snapshot()
    }

    fn record_queue_high_watermarks(&self) {
        self.metrics.max_queued_requests.fetch_max(
            self.queued_requests.load(Ordering::Acquire) as u64,
            Ordering::Relaxed,
        );
        self.metrics
            .max_queued_bytes
            .fetch_max(self.queued_bytes.load(Ordering::Acquire), Ordering::Relaxed);
    }
}

async fn load_extent_pages(
    metadata: &dyn StreamMetadataStore,
    manifest: &StreamManifest,
) -> Result<Vec<StreamExtentPage>> {
    let mut pages = Vec::with_capacity(manifest.extent_pages.len());
    for fence in &manifest.extent_pages {
        pages.push(
            metadata
                .load_extent_page(
                    manifest.stream_name,
                    manifest.writer_epoch,
                    manifest.generation,
                    fence.page_index,
                )
                .await?
                .ok_or_else(|| StreamError::Corruption("referenced extent page is missing".into()))?,
        );
    }
    Ok(pages)
}

fn collect_extents(pages: &[StreamExtentPage]) -> Vec<Extent> {
    let mut extents = Vec::new();
    for page in pages {
        for index in 0..page.chunk_ids.len() {
            extents.push(Extent {
                chunk_id: page.chunk_ids[index],
                logical_start: page.logical_offsets[index],
                logical_end: page.logical_offsets[index + 1],
                frame_start: page.physical_offsets[index],
                frame_length: page.frame_lengths[index],
            });
        }
    }
    extents
}

fn find_extent_fence(manifest: &StreamManifest, offset: u64) -> Result<usize> {
    let index = manifest
        .extent_pages
        .partition_point(|fence| fence.end_logical <= offset);
    let fence = manifest
        .extent_pages
        .get(index)
        .ok_or_else(|| StreamError::Corruption(format!(
            "sealed offset is not covered by extent directory: offset={offset}, sealed_tail={}, trim={}, generation={}, epoch={}, fences={}",
            manifest.sealed_tail, manifest.trim_offset, manifest.generation, manifest.writer_epoch, manifest.extent_pages.len()
        )))?;
    if offset < fence.first_logical {
        return Err(StreamError::Corruption(
            "extent directory has a logical gap".into(),
        ));
    }
    Ok(index)
}

fn validate_page_fence(
    manifest: &StreamManifest,
    fence: &StreamExtentPageFence,
    page: &StreamExtentPage,
) -> Result<()> {
    if page.stream_name != manifest.stream_name
        || page.writer_epoch != manifest.writer_epoch
        || page.generation != manifest.generation
        || page.page_index != fence.page_index
        || page.logical_offsets.first() != Some(&fence.first_logical)
        || page.logical_offsets.last() != Some(&fence.end_logical)
    {
        return Err(StreamError::Corruption(
            "extent page fence or identity mismatch".into(),
        ));
    }
    Ok(())
}

fn finish_failed(state: &WorkerState, requests: Vec<AppendRequest>, error: &StreamError) {
    for request in requests {
        observe(&perf().append, request.enqueued_at);
        state
            .queued_bytes
            .fetch_sub(request.logical_len() as u64, Ordering::AcqRel);
        state.queued_requests.fetch_sub(1, Ordering::AcqRel);
        state.metrics.failed.fetch_add(1, Ordering::Relaxed);
        let _ = request.completion.send(Err(error.clone()));
    }
}

fn reserve_bytes(counter: &AtomicU64, limit: u64, bytes: u64) -> Result<()> {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        let Some(next) = current.checked_add(bytes) else {
            return Err(StreamError::Backpressure);
        };
        if next > limit {
            return Err(StreamError::Backpressure);
        }
        match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(()),
            Err(observed) => current = observed,
        }
    }
}

fn reserve_requests(counter: &AtomicUsize, limit: usize) -> Result<()> {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        if current >= limit {
            return Err(StreamError::Backpressure);
        }
        match counter.compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(()),
            Err(observed) => current = observed,
        }
    }
}

fn durable_tail(manifest: &StreamManifest) -> Result<u64> {
    match &manifest.active {
        Some(active) => {
            let durable = active
                .acknowledged_cursor
                .checked_sub(active.physical_start)
                .ok_or_else(|| StreamError::Corruption("active cursor precedes its physical start".into()))?;
            active
                .logical_start
                .checked_add(durable)
                .ok_or_else(|| StreamError::Corruption("active tail overflows".into()))
        }
        None => Ok(manifest.sealed_tail),
    }
}

fn state_name(manifest: &ArcSwap<StreamManifest>) -> StreamName {
    manifest.load().stream_name
}

fn state_epoch(manifest: &ArcSwap<StreamManifest>) -> u64 {
    manifest.load().writer_epoch
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
