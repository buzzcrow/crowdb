// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use crowdb_protocol::chunk_stream::{ActiveChunkDescriptor, StreamName, StreamRecord, StreamRecordState};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::FrameLayout;
use tokio::sync::{mpsc, oneshot};

use crate::metrics::{StreamMetrics, StreamMetricsSnapshot};
use crate::mirror_shadow::MirrorShadow;
use crate::storage::{StreamChunkStore, StreamMetadataStore, StreamRegistry};
use crate::{Result, StreamError};
use crate::{StreamChunkRecordSnapshot, StreamChunkRecordStore};

mod append;
mod capacity;
mod capacity_plan;
#[cfg(feature = "test-util")]
pub use capacity_plan::capacity_target_for_tests;
mod checkpoint;
mod collection;
mod liveness;
mod observation;
mod open;
mod preparation;
mod publication;
mod state;
use state::{StreamViewState, WriterProgress};
mod read;
mod successor;
mod view;
mod worker;
pub use observation::StreamMetadataObservation;
pub use read::StreamReader;
use view::ReadView;
pub use view::StreamViewSource;

use crate::perf::{observe, perf};
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
    group_start: u64,
    group_length: u64,
    ordinal: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadHint {
    Bytes(u64),
    ToEnd,
}

struct AppendRequest {
    data: Bytes,
    authority: crate::StreamWriteContext,
    enqueued_at: Instant,
    completion: oneshot::Sender<Result<AppendRange>>,
}

impl AppendRequest {
    fn logical_len(&self) -> usize {
        self.data.len()
    }

    fn physical_len(&self) -> Result<usize> {
        let length = FrameLayout::StreamGroupV2
            .physical_length(self.data.len() as u64)
            .map_err(|error| StreamError::InvalidRequest(error.to_string()))?;
        usize::try_from(length)
            .map_err(|_| StreamError::InvalidRequest("framed append exceeds address space".into()))
    }
}

enum Command {
    Append(AppendRequest),
    Capture(oneshot::Sender<Result<Arc<ReadView>>>),
    Trim {
        offset: u64,
        completion: oneshot::Sender<Result<u64>>,
    },
    Close(oneshot::Sender<Result<()>>),
    Release(oneshot::Sender<Result<()>>),
    Reclaim {
        retained_generation: u64,
        max_records: usize,
        completion: oneshot::Sender<Result<u64>>,
    },
}

#[derive(Clone)]
pub struct ChunkStream {
    authority: Option<Arc<crate::StreamWriterSession>>,
    stream_name: StreamName,
    writer_epoch: u64,
    config: Arc<StreamConfig>,
    metadata: StreamChunkRecordStore,
    chunks: Arc<dyn StreamChunkStore>,
    sender: mpsc::Sender<Command>,
    queued_bytes: Arc<AtomicU64>,
    queued_requests: Arc<AtomicUsize>,
    state: Arc<ArcSwap<StreamViewState>>,
    read_view: Arc<ArcSwap<ReadView>>,
    view_source: StreamViewSource,
    closed: Arc<AtomicBool>,
    metrics: Arc<StreamMetrics>,
}

#[derive(Clone)]
struct Extent {
    chunk_id: ChunkId,
    logical_start: u64,
    logical_end: u64,
    frame_start: u64,
}

struct WorkerState {
    authority: Option<Arc<crate::StreamWriterSession>>,
    stream_name: StreamName,
    writer_epoch: u64,
    config: Arc<StreamConfig>,
    metadata: StreamChunkRecordStore,
    chunks: Arc<dyn StreamChunkStore>,
    published_view: Arc<ArcSwap<StreamViewState>>,
    read_view: Arc<ArcSwap<ReadView>>,
    tail_view: Arc<AtomicU64>,
    queued_bytes: Arc<AtomicU64>,
    queued_requests: Arc<AtomicUsize>,
    closed_view: Arc<AtomicBool>,
    progress: WriterProgress,
    snapshot: StreamChunkRecordSnapshot,
    collection_after: Option<u64>,
    owner_session: [u8; 16],
    extents: Vec<Extent>,
    mirror_shadow: MirrorShadow,
    checkpoint: Option<checkpoint::PendingCheckpoint>,
    capacity_preparation: Option<preparation::CapacityPreparation>,
    capacity_failure: Option<(ChunkId, StreamError)>,
    capacity_exhausted: Option<ChunkId>,
    capacity_plan: capacity_plan::CapacityPlan,
    successor: Option<successor::SuccessorPreparation>,
    liveness_failed: Arc<AtomicBool>,
    liveness_changed: Arc<tokio::sync::Notify>,
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
    #[cfg(feature = "test-util")]
    pub fn expire_read_lease_for_tests(&self) {
        self.read_view.load().pin.expire_for_tests();
    }
    /// Durable chunk purpose of this stream.
    #[must_use]
    pub fn purpose(&self) -> crowdb_protocol::chunk_stream::StreamPurpose {
        self.state.load().purpose
    }

    #[must_use]
    pub fn stream_name(&self) -> StreamName {
        self.stream_name
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
        let record = registry
            .load(stream_name)
            .await?
            .ok_or_else(|| StreamError::InvalidRequest("stream record does not exist".into()))?;
        if record.state != StreamRecordState::Active || writer_epoch == 0 {
            return Err(StreamError::InvalidRequest(
                "stream record or reader epoch is invalid".into(),
            ));
        }
        let metadata = metadata.record_store(record.metadata_group_id)?;
        let snapshot = metadata
            .load_tail(stream_name)
            .await?
            .ok_or_else(|| StreamError::Corruption("Stream tail chunk record does not exist".into()))?;
        let published = StreamViewState::new(snapshot.clone(), metadata.group_id());
        if published.purpose != record.purpose {
            return Err(StreamError::Corruption(
                "stream record purpose differs from published".into(),
            ));
        }
        if published.metadata_group_id != record.metadata_group_id || published.writer_epoch != writer_epoch {
            return Err(StreamError::StaleWriter);
        }
        let active = state::load_active(&snapshot, chunks.as_ref()).await?;
        let mut stream = Self::start(config, metadata, chunks, snapshot, active, None).await?;
        stream.closed.store(true, Ordering::Release);
        stream.view_source = StreamViewSource::PersistedCheckpoint;
        Ok(stream)
    }

    async fn start(
        config: StreamConfig,
        metadata: StreamChunkRecordStore,
        chunks: Arc<dyn StreamChunkStore>,
        snapshot: StreamChunkRecordSnapshot,
        active: Option<ActiveChunkDescriptor>,
        authority: Option<Arc<crate::StreamWriterSession>>,
    ) -> Result<Self> {
        let published = StreamViewState::new(snapshot.clone(), metadata.group_id());
        let (sender, receiver) = mpsc::channel(config.queue_requests);
        let config = Arc::new(config);
        let queued_bytes = Arc::new(AtomicU64::new(0));
        let queued_requests = Arc::new(AtomicUsize::new(0));
        let tail = Arc::new(AtomicU64::new(published.published_tail));
        let published_view = Arc::new(ArcSwap::from_pointee(published));
        let read_view = Arc::new(ArcSwap::from_pointee(
            ReadView::new(metadata.clone(), published_view.load_full()).await?,
        ));
        let closed = Arc::new(AtomicBool::new(published_view.load().closed));
        let metrics = Arc::new(StreamMetrics::default());
        metrics
            .metadata_publications
            .store(u64::from(authority.is_some()), Ordering::Relaxed);
        perf();
        let state = WorkerState {
            authority: authority.clone(),
            stream_name: published_view.load().stream_name,
            writer_epoch: published_view.load().writer_epoch,
            config: Arc::clone(&config),
            metadata: metadata.clone(),
            chunks: Arc::clone(&chunks),
            published_view: Arc::clone(&published_view),
            read_view: Arc::clone(&read_view),
            tail_view: Arc::clone(&tail),
            queued_bytes: Arc::clone(&queued_bytes),
            queued_requests: Arc::clone(&queued_requests),
            closed_view: Arc::clone(&closed),
            progress: WriterProgress::new(snapshot.record(), active),
            owner_session: snapshot.record().owner_session,
            snapshot,
            collection_after: None,
            extents: Vec::new(),
            mirror_shadow: MirrorShadow::default(),
            checkpoint: None,
            capacity_preparation: None,
            capacity_failure: None,
            capacity_exhausted: None,
            capacity_plan: capacity_plan::CapacityPlan::default(),
            successor: None,
            liveness_failed: Arc::new(AtomicBool::new(false)),
            liveness_changed: Arc::new(tokio::sync::Notify::new()),
            stalled: false,
            last_unavailable_log: None,
            metrics: Arc::clone(&metrics),
        };
        if authority.is_some() {
            tokio::spawn(run_worker(state, receiver));
        }
        Ok(Self {
            authority,
            stream_name: published_view.load().stream_name,
            writer_epoch: published_view.load().writer_epoch,
            config,
            metadata,
            chunks,
            sender,
            queued_bytes,
            queued_requests,
            state: Arc::clone(&published_view),
            read_view,
            view_source: StreamViewSource::LiveOwner,
            closed,
            metrics,
        })
    }

    pub(crate) fn writer_context(&self) -> Result<crate::StreamWriteContext> {
        self.authority
            .as_ref()
            .ok_or(StreamError::WriteStalled)?
            .capture()
    }

    /// Stop admission, drain and checkpoint before durably releasing this owner.
    /// # Errors
    /// Returns a fencing, checkpoint or release error; an error is not a release proof.
    pub async fn release_writer(&self) -> Result<()> {
        if self
            .authority
            .as_ref()
            .is_some_and(|authority| authority.is_released())
        {
            return Ok(());
        }
        if self.authority.is_none() {
            return Err(StreamError::WriteStalled);
        }
        self.closed.store(true, Ordering::Release);
        let (completion, result) = oneshot::channel();
        self.sender
            .send(Command::Release(completion))
            .await
            .map_err(|_| StreamError::WriteStalled)?;
        result.await.map_err(|_| StreamError::WriteStalled)?
    }

    /// Returns the synchronized logical end visible to this handle.
    #[must_use]
    pub fn tail(&self) -> u64 {
        self.read_view.load().end()
    }

    /// Appends one logical request after every required mirror is synchronized.
    ///
    /// # Errors
    ///
    /// Returns a typed admission, storage, fencing, or durability error.
    pub async fn append(&self, buffers: &[Bytes]) -> Result<AppendRange> {
        if self.closed.load(Ordering::Acquire) {
            return Err(StreamError::WriteStalled);
        }
        let authority = self.writer_context()?;
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
        let data = owned_payload(buffers, length);
        let (completion, result) = oneshot::channel();
        if self
            .sender
            .try_send(Command::Append(AppendRequest {
                data,
                authority,
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
        let data = owned_payload(buffers, length);
        self.enqueue_append(data).await
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
        let authority = self.writer_context()?;
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
                authority,
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
    pub fn record_generation(&self) -> u64 {
        self.state.load().generation
    }

    /// Reclaims Stream chunk records older than a caller-proven retained
    /// record generation.
    ///
    /// # Errors
    ///
    /// Returns an error if the watermark could remove the current generation,
    /// the pass bound is zero, or metadata deletion fails.
    pub async fn reclaim_metadata_before(&self, retained_generation: u64, max_records: usize) -> Result<u64> {
        let current = self.state.load().generation;
        if retained_generation == 0 || retained_generation > current || max_records == 0 {
            return Err(StreamError::InvalidRequest(
                "metadata reclaim watermark is outside retained generations".into(),
            ));
        }
        if self
            .authority
            .as_ref()
            .is_some_and(|authority| authority.is_released())
        {
            return Ok(0);
        }
        self.writer_context()?;
        let (completion, result) = oneshot::channel();
        self.sender
            .send(Command::Reclaim {
                retained_generation,
                max_records,
                completion,
            })
            .await
            .map_err(|_| StreamError::WriteStalled)?;
        result.await.map_err(|_| StreamError::WriteStalled)?
    }

    /// Seals the active chunk and publishes a closed state.
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

fn owned_payload(buffers: &[Bytes], length: usize) -> Bytes {
    if let [only] = buffers {
        return only.clone();
    }
    let mut data = BytesMut::with_capacity(length);
    for bytes in buffers {
        data.extend_from_slice(bytes);
    }
    data.freeze()
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

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
