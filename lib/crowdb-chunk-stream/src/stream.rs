// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use crowdb_protocol::chunk_stream::{
    StreamBinding, StreamBindingState, StreamExtentPage, StreamExtentPageFence, StreamManifest, StreamName,
};
use crowdb_protocol::common::ChunkId;
use tokio::sync::{mpsc, oneshot};

use crate::metadata::{resolve_extent, validate_manifest};
use crate::metrics::{StreamMetrics, StreamMetricsSnapshot};
use crate::storage::{CursorAdvance, StreamChunkStore, StreamMetadataStore, StreamRegistry};
use crate::{Result, StreamError};

#[derive(Clone, Debug)]
pub struct StreamConfig {
    pub queue_requests: usize,
    pub queue_bytes: u64,
    pub batch_requests: usize,
    pub batch_bytes: usize,
    pub max_append_bytes: usize,
    pub extent_page_entries: usize,
    pub read_window_bytes: usize,
    pub gc_bytes_per_pass: u64,
    pub watchdog_interval: Duration,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            queue_requests: 1_024,
            queue_bytes: 16 * 1024 * 1024,
            batch_requests: 64,
            batch_bytes: 1024 * 1024,
            max_append_bytes: 64 * 1024 * 1024,
            extent_page_entries: 256,
            read_window_bytes: 8 * 1024 * 1024,
            gc_bytes_per_pass: 64 * 1024 * 1024,
            watchdog_interval: Duration::from_millis(500),
        }
    }
}

impl StreamConfig {
    fn validate(&self) -> Result<()> {
        if self.queue_requests == 0
            || self.queue_bytes == 0
            || self.batch_requests == 0
            || self.batch_bytes == 0
            || self.max_append_bytes == 0
            || self.extent_page_entries == 0
            || self.read_window_bytes == 0
            || self.gc_bytes_per_pass == 0
            || self.watchdog_interval.is_zero()
            || self.batch_bytes > self.max_append_bytes
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
    pub begin: u64,
    pub end: u64,
}

struct AppendRequest {
    data: Bytes,
    enqueued_at: Instant,
    completion: oneshot::Sender<Result<AppendRange>>,
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
}

#[derive(Clone)]
struct Extent {
    chunk_id: ChunkId,
    logical_start: u64,
    logical_end: u64,
    physical_start: u64,
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
    stalled: bool,
    metrics: Arc<StreamMetrics>,
}

impl ChunkStream {
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
        if binding.state != StreamBindingState::Active || binding.metadata_group_id == 0 || writer_epoch == 0
        {
            return Err(StreamError::InvalidRequest(
                "stream binding or writer epoch is invalid".into(),
            ));
        }
        if registry.load(binding.stream_name).await?.is_some() {
            return Err(StreamError::InvalidRequest("stream already exists".into()));
        }
        registry.create(binding.clone()).await?;
        let manifest = StreamManifest {
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
        Self::start(config, metadata, chunks, manifest, Vec::new())
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
        if manifest.metadata_group_id != binding.metadata_group_id || manifest.writer_epoch > writer_epoch {
            return Err(StreamError::StaleWriter);
        }
        let pages = load_extent_pages(metadata.as_ref(), &manifest).await?;
        let mut tail = validate_manifest(&manifest, &pages)?;
        let expected = (manifest.writer_epoch, manifest.generation);
        let mut needs_publish = manifest.writer_epoch < writer_epoch;
        manifest.writer_epoch = writer_epoch;
        let mut recovered_sealed = false;
        if let Some(active) = &mut manifest.active {
            let durable = chunks.durable_cursor(active.chunk_id, writer_epoch).await?;
            if durable.offset < active.physical_start || durable.offset > active.capacity {
                return Err(StreamError::Corruption(
                    "recovered active cursor is outside chunk bounds".into(),
                ));
            }
            active.acknowledged_cursor = durable.offset;
            tail = active
                .logical_start
                .checked_add(durable.offset - active.physical_start)
                .ok_or_else(|| StreamError::Corruption("recovered tail overflows".into()))?;
            recovered_sealed = durable.sealed;
        }
        let mut extents = collect_extents(&pages);
        if recovered_sealed {
            let active = manifest
                .active
                .take()
                .ok_or_else(|| StreamError::Internal("sealed recovery lost active chunk".into()))?;
            if tail > active.logical_start {
                extents.push(Extent {
                    chunk_id: active.chunk_id,
                    logical_start: active.logical_start,
                    logical_end: tail,
                    physical_start: active.physical_start,
                });
                manifest.sealed_tail = tail;
            }
            let mut successor = chunks.allocate_mirrored(stream_name, writer_epoch).await?;
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
            metadata
                .publish(Some(expected), manifest.clone(), adopted_pages)
                .await?;
        }
        let stream = Self::start(config, metadata, chunks, manifest, extents)?;
        stream.tail.store(tail, Ordering::Release);
        Ok(stream)
    }

    fn start(
        config: StreamConfig,
        metadata: Arc<dyn StreamMetadataStore>,
        chunks: Arc<dyn StreamChunkStore>,
        manifest: StreamManifest,
        extents: Vec<Extent>,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(config.queue_requests);
        let config = Arc::new(config);
        let queued_bytes = Arc::new(AtomicU64::new(0));
        let queued_requests = Arc::new(AtomicUsize::new(0));
        let tail = Arc::new(AtomicU64::new(durable_tail(&manifest)?));
        let manifest_view = Arc::new(ArcSwap::from_pointee(manifest.clone()));
        let closed = Arc::new(AtomicBool::new(manifest.closed));
        let metrics = Arc::new(StreamMetrics::default());
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
            stalled: false,
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
        })
    }

    #[must_use]
    pub fn tail(&self) -> u64 {
        self.tail.load(Ordering::Acquire)
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

    /// Reads an exact retained durable logical range.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid range, corrupt metadata, or unavailable data.
    pub async fn read_at(&self, offset: u64, length: usize) -> Result<Bytes> {
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| StreamError::InvalidRequest("read range overflows".into()))?;
        let manifest = self.manifest.load_full();
        if offset < manifest.trim_offset || end > self.tail() {
            return Err(StreamError::InvalidRequest(
                "read is outside retained durable range".into(),
            ));
        }
        let mut pages = HashMap::new();
        let mut cursor = offset;
        let mut output = BytesMut::with_capacity(length);
        while cursor < end {
            if cursor >= manifest.sealed_tail {
                let active = manifest
                    .active
                    .as_ref()
                    .ok_or_else(|| StreamError::Corruption("active range has no chunk".into()))?;
                let physical = active
                    .physical_start
                    .checked_add(cursor - active.logical_start)
                    .ok_or_else(|| StreamError::Corruption("active read offset overflows".into()))?;
                let available = usize::try_from(end - cursor).map_err(|_| {
                    StreamError::InvalidRequest("read length exceeds addressable range".into())
                })?;
                output.extend_from_slice(
                    &self
                        .read_chunk_with_watchdog(active.chunk_id, physical, available, cursor)
                        .await?,
                );
                cursor = end;
                continue;
            }
            let fence_index = find_extent_fence(&manifest, cursor)?;
            let fence = &manifest.extent_pages[fence_index];
            if let std::collections::hash_map::Entry::Vacant(entry) = pages.entry(fence.page_index) {
                let page = self
                    .metadata
                    .load_extent_page(
                        manifest.stream_name,
                        manifest.writer_epoch,
                        manifest.generation,
                        fence.page_index,
                    )
                    .await?
                    .ok_or_else(|| StreamError::Corruption("referenced extent page is missing".into()))?;
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
                entry.insert(page);
            }
            let page = pages
                .get(&fence.page_index)
                .ok_or_else(|| StreamError::Internal("loaded extent page is absent".into()))?;
            let location = resolve_extent(page, cursor)?;
            let read_len = usize::try_from(location.available.min(end - cursor))
                .map_err(|_| StreamError::InvalidRequest("read length exceeds addressable range".into()))?;
            output.extend_from_slice(
                &self
                    .read_chunk_with_watchdog(location.chunk_id, location.physical_offset, read_len, cursor)
                    .await?,
            );
            cursor += read_len as u64;
        }
        if output.len() != length {
            return Err(StreamError::Corruption(
                "chunk reads returned an unexpected length".into(),
            ));
        }
        self.metrics
            .read_bytes
            .fetch_add(length as u64, Ordering::Relaxed);
        Ok(output.freeze())
    }

    /// Creates a bounded sequential reader ending at the current durable tail.
    ///
    /// # Errors
    ///
    /// Returns an error if `offset` is outside the retained durable range.
    pub fn read_from(&self, offset: u64) -> Result<StreamReader> {
        let manifest = self.manifest.load();
        if offset < manifest.trim_offset || offset > self.tail() {
            return Err(StreamError::InvalidRequest(
                "read cursor is outside retained range".into(),
            ));
        }
        Ok(StreamReader {
            stream: self.clone(),
            offset,
            end: self.tail(),
        })
    }

    #[must_use]
    pub fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }

    #[must_use]
    pub fn metrics(&self) -> StreamMetricsSnapshot {
        self.metrics.snapshot()
    }

    async fn read_chunk_with_watchdog(
        &self,
        chunk_id: ChunkId,
        physical_offset: u64,
        length: usize,
        logical_offset: u64,
    ) -> Result<Bytes> {
        let read = self.chunks.read(chunk_id, physical_offset, length);
        tokio::pin!(read);
        let started = Instant::now();
        loop {
            tokio::select! {
                result = &mut read => return result,
                () = tokio::time::sleep(self.config.watchdog_interval) => {
                    self.metrics.watchdog_observations.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        stream_high = self.stream_name.high,
                        stream_low = self.stream_name.low,
                        writer_epoch = self.writer_epoch,
                        logical_offset,
                        read_bytes = length,
                        operation_age_ms = started.elapsed().as_millis(),
                        stage = "chunk_read",
                        "chunk-stream read window remains in flight"
                    );
                }
            }
        }
    }
}

pub struct StreamReader {
    stream: ChunkStream,
    offset: u64,
    end: u64,
}

impl StreamReader {
    /// Returns the next bounded logical window, or `None` at the captured tail.
    ///
    /// # Errors
    ///
    /// Returns a typed metadata or chunk-read error.
    pub async fn next(&mut self) -> Result<Option<Bytes>> {
        if self.offset == self.end {
            return Ok(None);
        }
        let length =
            usize::try_from((self.end - self.offset).min(self.stream.config.read_window_bytes as u64))
                .map_err(|_| StreamError::InvalidRequest("read window exceeds addressable range".into()))?;
        let bytes = self.stream.read_at(self.offset, length).await?;
        self.offset += length as u64;
        Ok(Some(bytes))
    }
}

async fn run_worker(mut state: WorkerState, mut receiver: mpsc::Receiver<Command>) {
    let mut pending = None;
    loop {
        let command = match pending.take() {
            Some(command) => command,
            None => match receiver.recv().await {
                Some(command) => command,
                None => break,
            },
        };
        match command {
            Command::Append(first) => {
                process_append_batch(&mut state, first, &mut receiver, &mut pending).await;
            }
            Command::Trim { offset, completion } => {
                let _ = completion.send(process_trim(&mut state, offset).await);
            }
            Command::Close(completion) => {
                let result = process_close(&mut state).await;
                let _ = completion.send(result);
                break;
            }
        }
    }
    state.closed_view.store(true, Ordering::Release);
}

async fn process_append_batch(
    state: &mut WorkerState,
    first: AppendRequest,
    receiver: &mut mpsc::Receiver<Command>,
    pending: &mut Option<Command>,
) {
    if state.stalled || state.manifest.closed {
        finish_failed(state, vec![first], &StreamError::WriteStalled);
        return;
    }
    if let Err(error) = ensure_active(state).await {
        state.stalled = true;
        finish_failed(state, vec![first], &error);
        return;
    }
    let first_len = first.data.len();
    if !fits_active(state, first_len) {
        if let Err(error) = rollover(state).await {
            state.stalled = true;
            finish_failed(state, vec![first], &error);
            return;
        }
    }
    if !fits_active(state, first_len) {
        finish_failed(
            state,
            vec![first],
            &StreamError::InvalidRequest("append exceeds allocated chunk capacity".into()),
        );
        return;
    }

    let mut requests = vec![first];
    let mut bytes = first_len;
    while requests.len() < state.config.batch_requests && bytes < state.config.batch_bytes {
        match receiver.try_recv() {
            Ok(Command::Append(request))
                if bytes
                    .checked_add(request.data.len())
                    .is_some_and(|total| total <= state.config.batch_bytes && fits_active(state, total)) =>
            {
                bytes += request.data.len();
                requests.push(request);
            }
            Ok(command) => {
                *pending = Some(command);
                break;
            }
            Err(_) => break,
        }
    }
    let result = write_batch_with_watchdog(state, &requests, bytes).await;
    match result {
        Ok(ranges) => {
            for (request, range) in requests.into_iter().zip(ranges) {
                state
                    .queued_bytes
                    .fetch_sub(request.data.len() as u64, Ordering::AcqRel);
                state.queued_requests.fetch_sub(1, Ordering::AcqRel);
                state.metrics.completed.fetch_add(1, Ordering::Relaxed);
                state
                    .metrics
                    .logical_append_bytes
                    .fetch_add(request.data.len() as u64, Ordering::Relaxed);
                let _ = request.completion.send(Ok(range));
            }
        }
        Err(error) => finish_failed(state, requests, &error),
    }
}

async fn write_batch_with_watchdog(
    state: &mut WorkerState,
    requests: &[AppendRequest],
    bytes: usize,
) -> Result<Vec<AppendRange>> {
    let interval = state.config.watchdog_interval;
    let metrics = Arc::clone(&state.metrics);
    let stream_name = state.stream_name;
    let writer_epoch = state.writer_epoch;
    let logical_begin = state.tail_view.load(Ordering::Acquire);
    let logical_end = logical_begin.saturating_add(bytes as u64);
    let oldest_queue_age = requests
        .first()
        .map_or(Duration::ZERO, |request| request.enqueued_at.elapsed());
    metrics.batches.fetch_add(1, Ordering::Relaxed);
    metrics
        .batch_requests
        .fetch_add(requests.len() as u64, Ordering::Relaxed);
    let write = write_batch(state, requests, bytes);
    tokio::pin!(write);
    let started = Instant::now();
    loop {
        tokio::select! {
            result = &mut write => return result,
            () = tokio::time::sleep(interval) => {
                metrics.watchdog_observations.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    stream_high = stream_name.high,
                    stream_low = stream_name.low,
                    writer_epoch,
                    logical_begin,
                    logical_end,
                    batch_requests = requests.len(),
                    batch_bytes = bytes,
                    operation_age_ms = started.elapsed().as_millis(),
                    oldest_queue_age_ms = oldest_queue_age.as_millis(),
                    stage = "append_durability",
                    "chunk-stream append batch remains in flight"
                );
            }
        }
    }
}

async fn write_batch(
    state: &mut WorkerState,
    requests: &[AppendRequest],
    bytes: usize,
) -> Result<Vec<AppendRange>> {
    let active = state
        .manifest
        .active
        .as_ref()
        .ok_or_else(|| StreamError::Internal("append has no active chunk".into()))?
        .clone();
    let expected_cursor = active.acknowledged_cursor;
    let new_cursor = expected_cursor
        .checked_add(bytes as u64)
        .ok_or_else(|| StreamError::InvalidRequest("physical cursor overflows".into()))?;
    let mut staging = BytesMut::with_capacity(bytes);
    for request in requests {
        staging.extend_from_slice(&request.data);
    }
    let staging = staging.freeze();
    if let Err(error) = state
        .chunks
        .write_mirrors(
            state.stream_name,
            state.writer_epoch,
            active.chunk_id,
            expected_cursor,
            staging.clone(),
        )
        .await
    {
        state.stalled = true;
        return Err(error);
    }
    state
        .metrics
        .physical_append_bytes
        .fetch_add((bytes as u64).saturating_mul(3), Ordering::Relaxed);
    resolve_cursor_advance(
        state,
        active.chunk_id,
        expected_cursor,
        new_cursor,
        crc32fast::hash(&staging),
    )
    .await?;

    let begin = state.tail_view.load(Ordering::Acquire);
    let mut cursor = begin;
    let mut ranges = Vec::with_capacity(requests.len());
    for request in requests {
        let end = cursor + request.data.len() as u64;
        ranges.push(AppendRange {
            stream_name: state.stream_name,
            begin: cursor,
            end,
        });
        cursor = end;
    }
    if let Some(active) = &mut state.manifest.active {
        active.acknowledged_cursor = new_cursor;
    }
    state.tail_view.store(cursor, Ordering::Release);
    state.manifest_view.store(Arc::new(state.manifest.clone()));
    Ok(ranges)
}

async fn resolve_cursor_advance(
    state: &mut WorkerState,
    chunk_id: ChunkId,
    expected_cursor: u64,
    new_cursor: u64,
    checksum: u32,
) -> Result<()> {
    let outcome = state
        .chunks
        .advance_cursor(
            state.stream_name,
            state.writer_epoch,
            chunk_id,
            expected_cursor,
            new_cursor,
            checksum,
        )
        .await
        .map_err(|error| {
            state.stalled = true;
            error
        })?;
    match outcome {
        CursorAdvance::Committed => Ok(()),
        CursorAdvance::DefinitelyNotCommitted => {
            state.stalled = true;
            Err(StreamError::DefinitelyNotCommitted(
                "durable cursor did not advance".into(),
            ))
        }
        CursorAdvance::Ambiguous => {
            let durable = state
                .chunks
                .durable_cursor(chunk_id, state.writer_epoch)
                .await
                .map_err(|error| {
                    state.stalled = true;
                    error
                })?;
            state.stalled = true;
            if durable.offset == expected_cursor {
                Err(StreamError::DefinitelyNotCommitted(
                    "ambiguous write proved absent".into(),
                ))
            } else if durable.offset == new_cursor && durable.last_advance_checksum == Some(checksum) {
                state.stalled = false;
                Ok(())
            } else {
                Err(StreamError::WriteStalled)
            }
        }
    }
}

async fn ensure_active(state: &mut WorkerState) -> Result<()> {
    if state.manifest.active.is_some() {
        return Ok(());
    }
    let mut active = state
        .chunks
        .allocate_mirrored(state.stream_name, state.writer_epoch)
        .await?;
    active.logical_start = state.tail_view.load(Ordering::Acquire);
    if active.acknowledged_cursor < active.physical_start || active.acknowledged_cursor > active.capacity {
        return Err(StreamError::Corruption(
            "allocated chunk cursor is invalid".into(),
        ));
    }
    state.manifest.active = Some(active);
    publish_state(state).await
}

async fn rollover(state: &mut WorkerState) -> Result<()> {
    let active = state
        .manifest
        .active
        .take()
        .ok_or_else(|| StreamError::Internal("rollover has no active chunk".into()))?;
    state
        .chunks
        .seal(active.chunk_id, state.writer_epoch, active.acknowledged_cursor)
        .await?;
    let tail = state.tail_view.load(Ordering::Acquire);
    state.extents.push(Extent {
        chunk_id: active.chunk_id,
        logical_start: active.logical_start,
        logical_end: tail,
        physical_start: active.physical_start,
    });
    state.manifest.sealed_tail = tail;
    let mut successor = state
        .chunks
        .allocate_mirrored(state.stream_name, state.writer_epoch)
        .await?;
    successor.logical_start = tail;
    state.manifest.active = Some(successor);
    publish_state(state).await?;
    state.metrics.rollovers.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

async fn process_trim(state: &mut WorkerState, offset: u64) -> Result<u64> {
    if state.stalled {
        return Err(StreamError::WriteStalled);
    }
    let tail = state.tail_view.load(Ordering::Acquire);
    if offset < state.manifest.trim_offset || offset > tail {
        return Err(StreamError::InvalidRequest(
            "trim offset regresses or exceeds durable tail".into(),
        ));
    }
    if offset != state.manifest.trim_offset {
        state.manifest.trim_offset = offset;
        publish_state(state).await?;
    }
    let mut reclaimed = 0_u64;
    let mut removed = 0_usize;
    for extent in &state.extents {
        if extent.logical_end > offset || (removed > 0 && reclaimed >= state.config.gc_bytes_per_pass) {
            break;
        }
        reclaimed = reclaimed.saturating_add(
            state
                .chunks
                .release_trimmed(extent.chunk_id, extent.logical_end)
                .await?
                .reclaimed_bytes,
        );
        removed += 1;
    }
    if removed > 0 {
        state.extents.drain(..removed);
        publish_state(state).await?;
    }
    state
        .metrics
        .reclaimed_bytes
        .fetch_add(reclaimed, Ordering::Relaxed);
    Ok(reclaimed)
}

async fn process_close(state: &mut WorkerState) -> Result<()> {
    if state.stalled {
        return Err(StreamError::WriteStalled);
    }
    if state.manifest.closed {
        return Ok(());
    }
    if let Some(active) = state.manifest.active.take() {
        state
            .chunks
            .seal(active.chunk_id, state.writer_epoch, active.acknowledged_cursor)
            .await?;
        let tail = state.tail_view.load(Ordering::Acquire);
        if tail > active.logical_start {
            state.extents.push(Extent {
                chunk_id: active.chunk_id,
                logical_start: active.logical_start,
                logical_end: tail,
                physical_start: active.physical_start,
            });
            state.manifest.sealed_tail = tail;
        }
    }
    state.manifest.closed = true;
    publish_state(state).await?;
    state.closed_view.store(true, Ordering::Release);
    Ok(())
}

async fn publish_state(state: &mut WorkerState) -> Result<()> {
    let expected = state.manifest.generation;
    state.manifest.previous_generation = Some(expected);
    state.manifest.generation = expected + 1;
    let pages = build_extent_pages(
        &state.extents,
        state.stream_name,
        state.writer_epoch,
        state.manifest.generation,
        state.config.extent_page_entries,
    );
    state.manifest.extent_pages = fences_for(&pages);
    if let Err(error) = state
        .metadata
        .publish(
            Some((state.writer_epoch, expected)),
            state.manifest.clone(),
            pages,
        )
        .await
    {
        state.manifest.generation = expected;
        state.stalled = true;
        return Err(error);
    }
    state.manifest_view.store(Arc::new(state.manifest.clone()));
    Ok(())
}

fn fences_for(pages: &[StreamExtentPage]) -> Vec<StreamExtentPageFence> {
    pages
        .iter()
        .map(|page| StreamExtentPageFence {
            page_index: page.page_index,
            first_logical: page.logical_offsets[0],
            end_logical: *page
                .logical_offsets
                .last()
                .expect("built extent page is nonempty"),
        })
        .collect()
}

fn build_extent_pages(
    extents: &[Extent],
    stream_name: StreamName,
    writer_epoch: u64,
    generation: u64,
    page_entries: usize,
) -> Vec<StreamExtentPage> {
    extents
        .chunks(page_entries)
        .enumerate()
        .map(|(page_index, chunk)| {
            let mut logical_offsets = Vec::with_capacity(chunk.len() + 1);
            logical_offsets.push(chunk[0].logical_start);
            for extent in chunk {
                logical_offsets.push(extent.logical_end);
            }
            StreamExtentPage {
                stream_name,
                writer_epoch,
                generation,
                page_index: page_index as u64,
                chunk_ids: chunk.iter().map(|extent| extent.chunk_id).collect(),
                logical_offsets,
                physical_offsets: chunk.iter().map(|extent| extent.physical_start).collect(),
            }
        })
        .collect()
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
                physical_start: page.physical_offsets[index],
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
        .ok_or_else(|| StreamError::Corruption("sealed offset is not covered by extent directory".into()))?;
    if offset < fence.first_logical {
        return Err(StreamError::Corruption(
            "extent directory has a logical gap".into(),
        ));
    }
    Ok(index)
}

fn fits_active(state: &WorkerState, bytes: usize) -> bool {
    state.manifest.active.as_ref().is_some_and(|active| {
        active
            .acknowledged_cursor
            .checked_add(bytes as u64)
            .is_some_and(|end| end <= active.capacity)
    })
}

fn finish_failed(state: &WorkerState, requests: Vec<AppendRequest>, error: &StreamError) {
    for request in requests {
        state
            .queued_bytes
            .fetch_sub(request.data.len() as u64, Ordering::AcqRel);
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
