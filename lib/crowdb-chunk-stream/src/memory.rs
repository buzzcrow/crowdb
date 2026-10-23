// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! In-memory storage and deterministic fault controls for stream tests.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_protocol::chunk_stream::{
    ActiveChunkDescriptor, StreamBinding, StreamExtentPage, StreamManifest, StreamName,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::FrameLayout;
use tokio::sync::{Mutex, Notify};

mod allocation;
mod authority;
mod directory_records;
mod metadata;
mod recovery;

use crate::{
    CursorAdvance, DurableCursor, Result, StreamChunkStore, StreamError, StreamMirrorWrite, StreamRegistry,
    TrimmedChunk,
};

#[derive(Clone, Copy, Debug)]
struct CursorFault {
    outcome: CursorAdvance,
    commit: bool,
}

struct ActiveRead<'a>(&'a AtomicUsize);

impl Drop for ActiveRead<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug)]
struct Chunk {
    bytes: Vec<u8>,
    cursor: u64,
    synchronized_cursor: u64,
    capacity: u64,
    sealed: bool,
    released: bool,
    writer_epoch: u64,
    last_advance_checksum: Option<u32>,
}

#[derive(Debug, Default)]
struct MemoryState {
    bindings: HashMap<StreamName, StreamBinding>,
    owners: HashMap<StreamName, (u64, Arc<crate::StreamWriterSession>)>,
    manifests: HashMap<StreamName, StreamManifest>,
    pages: HashMap<(StreamName, u64, u64, u64), StreamExtentPage>,
    chunks: HashMap<ChunkId, Chunk>,
    cursor_faults: VecDeque<CursorFault>,
    metadata_records: BTreeMap<(u64, Vec<u8>), directory_records::MemoryRecord>,
    metadata_revision: u64,
    metadata_heads: HashMap<StreamName, u64>,
}

/// Test-only implementation of all three injected storage contracts.
#[derive(Debug)]
pub struct MemoryStreamStore {
    state: Arc<Mutex<MemoryState>>,
    chunk_capacity: u64,
    next_chunk: AtomicU64,
    metadata_publishes: Arc<AtomicU64>,
    extent_page_loads: AtomicU64,
    chunk_writes: AtomicU64,
    cursor_advances: AtomicU64,
    liveness_renews: AtomicU64,
    liveness_attempts: AtomicU64,
    pause_writes: AtomicBool,
    pause_reads: AtomicBool,
    active_reads: AtomicUsize,
    max_active_reads: AtomicUsize,
    fail_next_publish: Arc<AtomicBool>,
    fail_next_writes: AtomicUsize,
    fail_next_renewals: AtomicUsize,
    write_started: Notify,
    resume_write: Notify,
    read_started: Notify,
    resume_reads: Notify,
}

impl MemoryStreamStore {
    /// Creates a test store with fixed logical-payload capacity chunks. The
    /// backing capacity includes canonical group framing, so tiny
    /// rollover tests retain their payload-oriented meaning.
    ///
    /// # Panics
    ///
    /// Panics if `chunk_capacity` is zero or its physical geometry overflows.
    #[must_use]
    pub fn new(chunk_capacity: u64) -> Self {
        assert!(chunk_capacity > 0);
        Self {
            state: Arc::new(Mutex::new(MemoryState::default())),
            chunk_capacity: FrameLayout::StreamGroupV2
                .physical_length(chunk_capacity)
                .and_then(|length| {
                    length
                        .checked_add(crowdb_protocol::frame::FRAME_HEADER_PREFIX_BYTES as u64)
                        .ok_or(crowdb_protocol::frame::FrameError::LengthOverflow)
                })
                .expect("valid test chunk geometry"),
            next_chunk: AtomicU64::new(1),
            metadata_publishes: Arc::new(AtomicU64::new(0)),
            extent_page_loads: AtomicU64::new(0),
            chunk_writes: AtomicU64::new(0),
            cursor_advances: AtomicU64::new(0),
            liveness_renews: AtomicU64::new(0),
            liveness_attempts: AtomicU64::new(0),
            pause_writes: AtomicBool::new(false),
            pause_reads: AtomicBool::new(false),
            active_reads: AtomicUsize::new(0),
            max_active_reads: AtomicUsize::new(0),
            fail_next_publish: Arc::new(AtomicBool::new(false)),
            fail_next_writes: AtomicUsize::new(0),
            fail_next_renewals: AtomicUsize::new(0),
            write_started: Notify::new(),
            resume_write: Notify::new(),
            read_started: Notify::new(),
            resume_reads: Notify::new(),
        }
    }

    pub async fn queue_cursor_outcome(&self, outcome: CursorAdvance, commit: bool) {
        self.state
            .lock()
            .await
            .cursor_faults
            .push_back(CursorFault { outcome, commit });
    }

    pub fn pause_writes(&self) {
        self.pause_writes.store(true, Ordering::Release);
    }

    pub fn fail_next_publish(&self) {
        self.fail_next_publish.store(true, Ordering::Release);
    }

    pub fn fail_next_write(&self) {
        self.fail_next_writes(1);
    }

    pub fn fail_next_writes(&self, count: usize) {
        self.fail_next_writes.store(count, Ordering::Release);
    }

    pub fn fail_next_renewals(&self, count: usize) {
        self.fail_next_renewals.store(count, Ordering::Release);
    }

    pub async fn wait_for_write(&self) {
        self.write_started.notified().await;
    }

    pub fn resume_writes(&self) {
        self.pause_writes.store(false, Ordering::Release);
        self.resume_write.notify_one();
    }

    pub fn pause_reads(&self) {
        self.pause_reads.store(true, Ordering::Release);
    }

    pub async fn wait_for_concurrent_reads(&self, target: usize) {
        loop {
            let notified = self.read_started.notified();
            if self.active_reads.load(Ordering::Acquire) >= target {
                return;
            }
            notified.await;
        }
    }

    pub fn resume_reads(&self) {
        self.pause_reads.store(false, Ordering::Release);
        self.resume_reads.notify_waiters();
    }

    #[must_use]
    pub fn max_concurrent_reads(&self) -> usize {
        self.max_active_reads.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn metadata_publish_count(&self) -> u64 {
        self.metadata_publishes.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn extent_page_load_count(&self) -> u64 {
        self.extent_page_loads.load(Ordering::Acquire)
    }

    pub async fn extent_page_count(&self) -> usize {
        self.state.lock().await.pages.len()
    }

    /// Returns retained physical bytes independently of the metadata checkpoint.
    /// # Panics
    /// Panics if the test chunk is absent or its cursor exceeds addressable memory.
    pub async fn synchronized_bytes_for_tests(&self, chunk_id: ChunkId) -> Bytes {
        let state = self.state.lock().await;
        let chunk = state.chunks.get(&chunk_id).expect("allocated test chunk");
        Bytes::copy_from_slice(&chunk.bytes[..usize::try_from(chunk.synchronized_cursor).unwrap()])
    }

    /// Writes actual encoded groups through the same image contract as production.
    /// # Errors
    /// Returns invalid bounds, owner or write failures.
    /// # Panics
    /// Panics if the test chunk is absent or its cursor exceeds addressable memory.
    pub async fn write_groups_for_tests(
        &self,
        name: StreamName,
        epoch: u64,
        chunk_id: ChunkId,
        offset: u64,
        data: Bytes,
    ) -> Result<()> {
        let start = usize::try_from(offset)
            .map_err(|_| StreamError::InvalidRequest("test offset overflows".into()))?;
        let mut prefix = self.synchronized_bytes_for_tests(chunk_id).await.to_vec();
        if prefix.len() != start {
            return Err(StreamError::InvalidRequest("test write is not contiguous".into()));
        }
        prefix.extend_from_slice(&data);
        let complete_end = u64::try_from(prefix.len())
            .map_err(|_| StreamError::InvalidRequest("test write overflows".into()))?;
        let strip_bytes = usize::try_from(crowdb_chunk_client::STREAM_STRIP_BYTES).expect("bounded strip");
        let first = start / strip_bytes * strip_bytes;
        let images: Vec<_> = (first..prefix.len())
            .step_by(strip_bytes)
            .map(|strip| {
                let end = (strip + strip_bytes).min(prefix.len());
                let offset = start.saturating_sub(strip);
                let full_image = Bytes::copy_from_slice(&prefix[strip..end]);
                crate::MirrorStripImage {
                    block_offset: offset as u64,
                    data: full_image.slice(offset..),
                    full_image,
                }
            })
            .collect();
        self.write_mirrors(
            name,
            epoch,
            chunk_id,
            StreamMirrorWrite {
                authority: None,
                physical_offset: offset,
                complete_end,
                images: &images,
            },
        )
        .await
    }

    #[must_use]
    pub fn chunk_write_count(&self) -> u64 {
        self.chunk_writes.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn cursor_advance_count(&self) -> u64 {
        self.cursor_advances.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn liveness_renew_count(&self) -> u64 {
        self.liveness_renews.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn liveness_attempt_count_for_tests(&self) -> u64 {
        self.liveness_attempts.load(Ordering::Acquire)
    }

    pub async fn is_released(&self, chunk_id: ChunkId) -> bool {
        self.state
            .lock()
            .await
            .chunks
            .get(&chunk_id)
            .is_some_and(|chunk| chunk.released)
    }

    /// Flips one durable byte for corruption-path tests.
    ///
    /// # Panics
    ///
    /// Panics when the test chunk or durable byte does not exist.
    pub async fn flip_durable_byte(&self, chunk_id: ChunkId, offset: usize) {
        let mut state = self.state.lock().await;
        let chunk = state.chunks.get_mut(&chunk_id).expect("test chunk must exist");
        assert!(offset < usize::try_from(chunk.synchronized_cursor).expect("test cursor must fit usize"));
        chunk.bytes[offset] ^= 1;
    }
}

#[async_trait]
impl StreamRegistry for MemoryStreamStore {
    async fn claim_writer(&self, name: StreamName, epoch: u64) -> Result<Arc<crate::StreamWriterSession>> {
        self.claim_memory_writer(name, epoch).await
    }
    async fn load(&self, stream_name: StreamName) -> Result<Option<StreamBinding>> {
        Ok(self.state.lock().await.bindings.get(&stream_name).cloned())
    }

    async fn create(&self, binding: StreamBinding) -> Result<()> {
        let mut state = self.state.lock().await;
        if state.bindings.insert(binding.stream_name, binding).is_some() {
            return Err(StreamError::InvalidRequest(
                "stream binding already exists".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl StreamChunkStore for MemoryStreamStore {
    fn bind_writer(&self, _name: StreamName, _epoch: u64, _session: Arc<crate::StreamWriterSession>) {}
    fn mirror_copies(&self) -> u32 {
        1
    }
    async fn current_capacity(&self, chunk_id: ChunkId) -> Result<u64> {
        self.state
            .lock()
            .await
            .chunks
            .get(&chunk_id)
            .map(|chunk| chunk.capacity)
            .ok_or_else(|| StreamError::ReadUnavailable("chunk is missing".into()))
    }
    async fn allocate_mirrored(
        &self,
        _stream_name: StreamName,
        writer_epoch: u64,
        purpose: crowdb_protocol::chunk_stream::StreamPurpose,
    ) -> Result<ActiveChunkDescriptor> {
        self.allocate_active_chunk(writer_epoch, purpose).await
    }

    async fn write_mirrors(
        &self,
        _stream_name: StreamName,
        writer_epoch: u64,
        chunk_id: ChunkId,
        write: StreamMirrorWrite<'_>,
    ) -> Result<()> {
        self.chunk_writes.fetch_add(1, Ordering::AcqRel);
        if self
            .fail_next_writes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| count.checked_sub(1))
            .is_ok()
        {
            return Err(StreamError::Internal("injected mirror write failure".into()));
        }
        if self.pause_writes.load(Ordering::Acquire) {
            self.write_started.notify_one();
            while self.pause_writes.load(Ordering::Acquire) {
                self.resume_write.notified().await;
            }
        }
        let physical_offset = write.physical_offset;
        let start = usize::try_from(physical_offset)
            .map_err(|_| StreamError::InvalidRequest("physical offset exceeds addressable range".into()))?;
        let length = write
            .images
            .iter()
            .try_fold(0_usize, |sum, image| sum.checked_add(image.data.len()))
            .ok_or_else(|| StreamError::InvalidRequest("stream image length overflows".into()))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| StreamError::InvalidRequest("physical write range overflows".into()))?;
        let mut state = self.state.lock().await;
        let chunk = state
            .chunks
            .get_mut(&chunk_id)
            .ok_or_else(|| StreamError::ReadUnavailable("chunk is missing".into()))?;
        if writer_epoch != chunk.writer_epoch {
            return Err(StreamError::StaleWriter);
        }
        if chunk.sealed || u64::try_from(end).unwrap_or(u64::MAX) > chunk.capacity {
            return Err(StreamError::InvalidRequest("write exceeds writable chunk".into()));
        }
        let prefix: Vec<_> = write
            .images
            .iter()
            .flat_map(|image| image.data.iter().copied())
            .take(15)
            .collect();
        let grouped = crowdb_protocol::frame::parse_header(&prefix)
            .is_ok_and(|header| header.magic == crowdb_protocol::frame::FrameMagic::StreamV2)
            && prefix.get(14) == Some(&1);
        if crate::storage::validate_write_bounds(write, chunk.capacity)? != end as u64 {
            return Err(StreamError::InvalidRequest(
                "image bytes differ from complete append end".into(),
            ));
        }
        if !grouped || physical_offset != chunk.synchronized_cursor {
            return Err(StreamError::InvalidRequest(
                "stream writes require contiguous complete append groups".into(),
            ));
        }
        chunk.bytes.resize(chunk.bytes.len().max(end), 0);
        let mut cursor = start;
        for image in write.images {
            let next = cursor + image.data.len();
            chunk.bytes[cursor..next].copy_from_slice(&image.data);
            cursor = next;
        }
        chunk.synchronized_cursor = chunk.synchronized_cursor.max(write.complete_end);
        Ok(())
    }

    async fn advance_cursor(
        &self,
        _stream_name: StreamName,
        writer_epoch: u64,
        chunk_id: ChunkId,
        expected_cursor: u64,
        new_cursor: u64,
        checksum: u32,
    ) -> Result<CursorAdvance> {
        self.cursor_advances.fetch_add(1, Ordering::AcqRel);
        let mut state = self.state.lock().await;
        let fault = state.cursor_faults.pop_front().unwrap_or(CursorFault {
            outcome: CursorAdvance::Committed,
            commit: true,
        });
        let chunk = state
            .chunks
            .get_mut(&chunk_id)
            .ok_or_else(|| StreamError::ReadUnavailable("chunk is missing".into()))?;
        if writer_epoch != chunk.writer_epoch
            || chunk.cursor != expected_cursor
            || new_cursor > chunk.capacity
        {
            return Err(StreamError::StaleWriter);
        }
        let start = usize::try_from(expected_cursor)
            .map_err(|_| StreamError::InvalidRequest("cursor exceeds addressable range".into()))?;
        let end = usize::try_from(new_cursor)
            .map_err(|_| StreamError::InvalidRequest("cursor exceeds addressable range".into()))?;
        if end > chunk.bytes.len() || crc32fast::hash(&chunk.bytes[start..end]) != checksum {
            return Err(StreamError::Corruption(
                "cursor checksum does not match staged bytes".into(),
            ));
        }
        if fault.commit {
            chunk.cursor = new_cursor;
            chunk.synchronized_cursor = chunk.synchronized_cursor.max(new_cursor);
            chunk.last_advance_checksum = Some(checksum);
        }
        Ok(fault.outcome)
    }

    async fn durable_cursor(&self, chunk_id: ChunkId, writer_epoch: u64) -> Result<DurableCursor> {
        let mut state = self.state.lock().await;
        let chunk = state
            .chunks
            .get_mut(&chunk_id)
            .ok_or_else(|| StreamError::ReadUnavailable("chunk is missing".into()))?;
        if writer_epoch < chunk.writer_epoch {
            return Err(StreamError::StaleWriter);
        }
        chunk.writer_epoch = writer_epoch;
        Ok(DurableCursor {
            offset: chunk.cursor,
            last_advance_checksum: chunk.last_advance_checksum,
            sealed: chunk.sealed,
        })
    }

    async fn recover_tail(
        &self,
        _name: StreamName,
        epoch: u64,
        active: &ActiveChunkDescriptor,
    ) -> Result<crate::RecoveredStreamTail> {
        self.recover_chunk_tail(epoch, active).await
    }

    async fn renew_liveness(&self, chunk_id: ChunkId, writer_epoch: u64) -> Result<()> {
        self.liveness_attempts.fetch_add(1, Ordering::AcqRel);
        if self
            .fail_next_renewals
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| count.checked_sub(1))
            .is_ok()
        {
            return Err(StreamError::ReadUnavailable("injected liveness failure".into()));
        }
        let state = self.state.lock().await;
        let chunk = state
            .chunks
            .get(&chunk_id)
            .ok_or_else(|| StreamError::ReadUnavailable("chunk is missing".into()))?;
        if chunk.writer_epoch != writer_epoch || chunk.sealed {
            return Err(StreamError::StaleWriter);
        }
        self.liveness_renews.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    async fn seal(&self, chunk_id: ChunkId, writer_epoch: u64, cursor: u64) -> Result<()> {
        let mut state = self.state.lock().await;
        let chunk = state
            .chunks
            .get_mut(&chunk_id)
            .ok_or_else(|| StreamError::ReadUnavailable("chunk is missing".into()))?;
        if writer_epoch != chunk.writer_epoch {
            return Err(StreamError::StaleWriter);
        }
        if chunk.synchronized_cursor != cursor {
            return Err(StreamError::Corruption(
                "seal cursor differs from durable cursor".into(),
            ));
        }
        chunk.cursor = cursor;
        chunk.sealed = true;
        Ok(())
    }

    async fn read(&self, chunk_id: ChunkId, physical_offset: u64, length: usize) -> Result<Bytes> {
        let active = self.active_reads.fetch_add(1, Ordering::AcqRel) + 1;
        let _active_read = ActiveRead(&self.active_reads);
        self.max_active_reads.fetch_max(active, Ordering::AcqRel);
        self.read_started.notify_waiters();
        while self.pause_reads.load(Ordering::Acquire) {
            self.resume_reads.notified().await;
        }
        async {
            let start = usize::try_from(physical_offset)
                .map_err(|_| StreamError::InvalidRequest("read offset exceeds addressable range".into()))?;
            let end = start
                .checked_add(length)
                .ok_or_else(|| StreamError::InvalidRequest("read range overflows".into()))?;
            let state = self.state.lock().await;
            let chunk = state
                .chunks
                .get(&chunk_id)
                .ok_or_else(|| StreamError::ReadUnavailable("chunk is missing".into()))?;
            if chunk.released || u64::try_from(end).unwrap_or(u64::MAX) > chunk.synchronized_cursor {
                return Err(StreamError::ReadUnavailable("chunk range is not durable".into()));
            }
            Ok(Bytes::copy_from_slice(&chunk.bytes[start..end]))
        }
        .await
    }

    async fn release_trimmed(
        &self,
        chunk_id: ChunkId,
        _logical_end: u64,
        physical_end: u64,
    ) -> Result<TrimmedChunk> {
        let mut state = self.state.lock().await;
        let chunk = state
            .chunks
            .get_mut(&chunk_id)
            .ok_or_else(|| StreamError::ReadUnavailable("chunk is missing".into()))?;
        if !chunk.released && (!chunk.sealed || chunk.cursor != physical_end) {
            return Err(StreamError::Corruption(
                "chunk release requires its exact sealed boundary".into(),
            ));
        }
        let reclaimed_bytes = if chunk.released { 0 } else { chunk.cursor };
        chunk.released = true;
        Ok(TrimmedChunk {
            chunk_id,
            reclaimed_bytes,
        })
    }
}
