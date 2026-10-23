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
use crowdb_protocol::frame::{FRAME_FOOTER_BYTES, FRAME_HEADER_PREFIX_BYTES};
use tokio::sync::{Mutex, Notify};

mod allocation;
mod directory_records;
mod metadata;

use crate::{
    CursorAdvance, DurableCursor, Result, StreamChunkStore, StreamError, StreamRegistry, TrimmedChunk,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChunkWriteMode {
    Fresh,
    Legacy,
    Grouped,
}

#[derive(Debug)]
struct Chunk {
    bytes: Vec<u8>,
    cursor: u64,
    synchronized_cursor: u64,
    write_mode: ChunkWriteMode,
    capacity: u64,
    sealed: bool,
    released: bool,
    writer_epoch: u64,
    last_advance_checksum: Option<u32>,
}

#[derive(Debug, Default)]
struct MemoryState {
    bindings: HashMap<StreamName, StreamBinding>,
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
    /// backing capacity includes one public frame header and footer, so tiny
    /// rollover tests retain their payload-oriented meaning.
    ///
    /// # Panics
    ///
    /// Panics if `chunk_capacity` is zero.
    #[must_use]
    pub fn new(chunk_capacity: u64) -> Self {
        assert!(chunk_capacity > 0);
        Self {
            state: Arc::new(Mutex::new(MemoryState::default())),
            chunk_capacity: chunk_capacity.saturating_add(
                u64::try_from(FRAME_HEADER_PREFIX_BYTES + FRAME_FOOTER_BYTES).unwrap_or(u64::MAX),
            ),
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
        assert!(offset < usize::try_from(chunk.cursor).expect("test cursor must fit usize"));
        chunk.bytes[offset] ^= 1;
    }
}

#[async_trait]
impl StreamRegistry for MemoryStreamStore {
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

    async fn write_mirrors_with_images(
        &self,
        _stream_name: StreamName,
        writer_epoch: u64,
        chunk_id: ChunkId,
        physical_offset: u64,
        data: Bytes,
        _images: &[crate::MirrorStripImage],
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
        let start = usize::try_from(physical_offset)
            .map_err(|_| StreamError::InvalidRequest("physical offset exceeds addressable range".into()))?;
        let end = start
            .checked_add(data.len())
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
        let grouped = crowdb_protocol::frame::parse_header(&data)
            .is_ok_and(|header| header.magic == crowdb_protocol::frame::FrameMagic::StreamV2);
        if chunk.write_mode == ChunkWriteMode::Grouped
            && (!grouped || physical_offset != chunk.synchronized_cursor)
            || grouped
                && chunk.write_mode != ChunkWriteMode::Grouped
                && (chunk.write_mode != ChunkWriteMode::Fresh
                    || physical_offset != 0
                    || chunk.synchronized_cursor != 0)
        {
            return Err(StreamError::InvalidRequest(
                "grouped writes require a fresh chunk and contiguous synchronized progress".into(),
            ));
        }
        chunk.write_mode = ChunkWriteMode::Legacy;
        chunk.bytes.resize(chunk.bytes.len().max(end), 0);
        chunk.bytes[start..end].copy_from_slice(&data);
        chunk.synchronized_cursor = chunk.synchronized_cursor.max(end as u64);
        if grouped {
            chunk.write_mode = ChunkWriteMode::Grouped;
        }
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
        if (if chunk.write_mode == ChunkWriteMode::Grouped {
            chunk.synchronized_cursor
        } else {
            chunk.cursor
        }) != cursor
        {
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
            if chunk.released || u64::try_from(end).unwrap_or(u64::MAX) > chunk.cursor {
                return Err(StreamError::ReadUnavailable("chunk range is not durable".into()));
            }
            Ok(Bytes::copy_from_slice(&chunk.bytes[start..end]))
        }
        .await
    }

    async fn release_trimmed(&self, chunk_id: ChunkId, _logical_end: u64) -> Result<TrimmedChunk> {
        let mut state = self.state.lock().await;
        let chunk = state
            .chunks
            .get_mut(&chunk_id)
            .ok_or_else(|| StreamError::ReadUnavailable("chunk is missing".into()))?;
        let reclaimed_bytes = if chunk.released { 0 } else { chunk.cursor };
        chunk.released = true;
        Ok(TrimmedChunk {
            chunk_id,
            reclaimed_bytes,
        })
    }
}
