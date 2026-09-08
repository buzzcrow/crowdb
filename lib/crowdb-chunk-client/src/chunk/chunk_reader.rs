// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Object and range reads over current chunk layouts.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, Location, QueryChunkRequest};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::strip_reader::StripReader;
use crate::{ChunkAllocator, DiskWriter, IoError, ReadError, ReadResult};

const KIB: u64 = 1024;
const DEFAULT_STREAM_WINDOW: usize = 64 * 1024 * 1024;

/// Bounded retry, streaming, and EC-recovery memory policy.
#[derive(Debug, Clone)]
pub struct ChunkReadPolicy {
    pub stream_window_bytes: usize,
    pub recovery_memory_bytes: usize,
    pub layout_safety_margin: Duration,
    pub max_layout_retries: usize,
}

impl Default for ChunkReadPolicy {
    fn default() -> Self {
        Self {
            stream_window_bytes: DEFAULT_STREAM_WINDOW,
            recovery_memory_bytes: DEFAULT_STREAM_WINDOW,
            layout_safety_margin: Duration::from_millis(5),
            max_layout_retries: 3,
        }
    }
}

impl ChunkReadPolicy {
    fn validate(&self) -> ReadResult<()> {
        if self.stream_window_bytes == 0
            || self.recovery_memory_bytes == 0
            || self.recovery_memory_bytes > u32::MAX as usize
            || self.max_layout_retries == 0
        {
            return Err(ReadError::InvalidLocations("invalid chunk read policy".into()));
        }
        Ok(())
    }
}

/// Unified mirror/EC reader for location arrays produced by both writers.
#[derive(Clone)]
pub struct ChunkReader {
    chunkdb: Arc<dyn ChunkAllocator>,
    strip_reader: StripReader,
    policy: ChunkReadPolicy,
}

impl ChunkReader {
    pub fn new(
        chunkdb: Arc<dyn ChunkAllocator>,
        disk_io: Arc<dyn DiskWriter>,
        policy: ChunkReadPolicy,
    ) -> ReadResult<Self> {
        policy.validate()?;
        let recovery_memory = Arc::new(Semaphore::new(policy.recovery_memory_bytes));
        Ok(Self {
            chunkdb,
            strip_reader: StripReader::new(disk_io, recovery_memory, policy.recovery_memory_bytes),
            policy,
        })
    }

    pub async fn read_object(&self, locations: &[Location]) -> ReadResult<Bytes> {
        let (_, object_length) = normalize_locations(locations)?;
        self.read_range(locations, 0, object_length).await
    }

    pub async fn read_range(&self, locations: &[Location], start: u64, end: u64) -> ReadResult<Bytes> {
        let (locations, object_length) = normalize_locations(locations)?;
        if start > end || end > object_length {
            return Err(ReadError::InvalidRange {
                start,
                end,
                object_length,
            });
        }
        if start == end {
            return Ok(Bytes::new());
        }

        let mut reads = JoinSet::new();
        for (order, location) in locations.into_iter().enumerate() {
            let loc_start = location.logical_offset;
            let loc_end = loc_start + location.logical_length;
            let overlap_start = start.max(loc_start);
            let overlap_end = end.min(loc_end);
            if overlap_start >= overlap_end {
                continue;
            }
            let reader = self.clone();
            reads.spawn(async move {
                let local_start = overlap_start - loc_start;
                let length = overlap_end - overlap_start;
                let data = reader.read_location(&location, local_start, length).await;
                (order, data)
            });
        }
        let expected = usize::try_from(end - start).map_err(|_| ReadError::InvalidRange {
            start,
            end,
            object_length,
        })?;
        collect_locations(reads, expected).await
    }

    pub fn read_stream(&self, locations: &[Location]) -> ReadResult<ChunkReadStream> {
        let (locations, object_length) = normalize_locations(locations)?;
        Ok(ChunkReadStream {
            reader: self.clone(),
            locations: Arc::from(locations),
            cursor: 0,
            end: object_length,
            window_bytes: self.policy.stream_window_bytes as u64,
        })
    }

    async fn read_location(&self, location: &Location, local_start: u64, length: u64) -> ReadResult<Bytes> {
        let chunk_id = location
            .chunk_id
            .ok_or_else(|| ReadError::InvalidLocations("location has no chunk ID".into()))?;
        let physical_start = location
            .offset
            .checked_add(local_start)
            .ok_or_else(|| ReadError::InvalidLocations("location physical offset overflows".into()))?;
        for _ in 0..self.policy.max_layout_retries {
            let query_started = Instant::now();
            let response = self
                .chunkdb
                .query_chunk(QueryChunkRequest {
                    chunk_id: Some(chunk_id),
                })
                .await
                .map_err(map_metadata_error)?;
            let validity = Duration::from_millis(response.layout_validity_ms);
            let usable = validity.saturating_sub(self.policy.layout_safety_margin);
            let deadline = query_started + usable;
            let chunk = response
                .chunk
                .ok_or_else(|| ReadError::ChunkDeleted(format!("{}:{}", chunk_id.high, chunk_id.low)))?;
            let data = self.read_chunk_range(&chunk, physical_start, length).await?;
            if Instant::now() < deadline {
                return Ok(data);
            }
        }
        Err(ReadError::LayoutExpired)
    }

    async fn read_chunk_range(&self, chunk: &Chunk, start: u64, length: u64) -> ReadResult<Bytes> {
        if chunk.state == ChunkState::Deleted as i32 {
            let id = chunk.id.unwrap_or_default();
            return Err(ReadError::ChunkDeleted(format!("{}:{}", id.high, id.low)));
        }
        validate_strip_layout(chunk)?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| ReadError::InvalidLocations("chunk read end overflows".into()))?;
        let first = chunk
            .strips
            .partition_point(|strip| strip_end(strip).is_ok_and(|strip_end| strip_end <= start));
        let mut cursor = start;
        let mut output = BytesMut::with_capacity(
            usize::try_from(length)
                .map_err(|_| ReadError::InvalidLocations("chunk read exceeds usize".into()))?,
        );
        for strip in &chunk.strips[first..] {
            let strip_start = u64::from(strip.chunk_offset) * KIB;
            if strip_start >= end {
                break;
            }
            if strip_start > cursor {
                return Err(ReadError::InvalidLocations(format!(
                    "chunk layout has a gap at byte {cursor}"
                )));
            }
            let overlap_end = end.min(strip_end(strip)?);
            if overlap_end <= cursor {
                continue;
            }
            let acknowledged = chunk.acknowledged_cursor.saturating_sub(strip_start);
            let durable_bytes = acknowledged.min(u64::from(strip.capacity) * KIB);
            let data = self
                .strip_reader
                .read(strip, durable_bytes, cursor - strip_start, overlap_end - cursor)
                .await?;
            output.extend_from_slice(&data);
            cursor = overlap_end;
        }
        if cursor != end {
            return Err(ReadError::InvalidLocations(format!(
                "chunk layout ends at byte {cursor}, requested {end}"
            )));
        }
        Ok(output.freeze())
    }
}

/// Pull-based stream whose emitted item never exceeds the configured window.
pub struct ChunkReadStream {
    reader: ChunkReader,
    locations: Arc<[Location]>,
    cursor: u64,
    end: u64,
    window_bytes: u64,
}

impl ChunkReadStream {
    pub async fn next_chunk(&mut self) -> Option<ReadResult<Bytes>> {
        if self.cursor >= self.end {
            return None;
        }
        let next = self.cursor.saturating_add(self.window_bytes).min(self.end);
        let result = self.reader.read_range(&self.locations, self.cursor, next).await;
        if result.is_ok() {
            self.cursor = next;
        } else {
            self.cursor = self.end;
        }
        Some(result)
    }
}

async fn collect_locations(
    mut reads: JoinSet<(usize, ReadResult<Bytes>)>,
    expected: usize,
) -> ReadResult<Bytes> {
    let mut parts = Vec::new();
    while let Some(result) = reads.join_next().await {
        let (order, data) = result.map_err(|error| ReadError::DiskIo(error.to_string()))?;
        parts.push((order, data?));
    }
    parts.sort_unstable_by_key(|(order, _)| *order);
    let actual = parts.iter().map(|(_, data)| data.len()).sum::<usize>();
    if actual != expected {
        return Err(ReadError::InvalidLocations(format!(
            "read assembled {actual} bytes, expected {expected}"
        )));
    }
    let mut output = BytesMut::with_capacity(actual);
    for (_, data) in parts {
        output.extend_from_slice(&data);
    }
    Ok(output.freeze())
}

fn normalize_locations(locations: &[Location]) -> ReadResult<(Vec<Location>, u64)> {
    let mut locations: Vec<_> = locations
        .iter()
        .filter(|location| location.logical_length != 0 && location.length != 0)
        .cloned()
        .collect();
    locations.sort_unstable_by_key(|location| location.logical_offset);
    let mut cursor = 0u64;
    for location in &locations {
        if location.chunk_id.is_none() || location.logical_length > location.length {
            return Err(ReadError::InvalidLocations(
                "location is missing a chunk or logical length exceeds physical length".into(),
            ));
        }
        if location.logical_offset != cursor {
            return Err(ReadError::InvalidLocations(format!(
                "logical locations are not contiguous at byte {cursor}"
            )));
        }
        location
            .offset
            .checked_add(location.length)
            .ok_or_else(|| ReadError::InvalidLocations("physical location overflows".into()))?;
        cursor = cursor
            .checked_add(location.logical_length)
            .ok_or_else(|| ReadError::InvalidLocations("object length overflows".into()))?;
    }
    Ok((locations, cursor))
}

fn validate_strip_layout(chunk: &Chunk) -> ReadResult<()> {
    let mut previous_end = 0;
    for (index, strip) in chunk.strips.iter().enumerate() {
        let start = u64::from(strip.chunk_offset) * KIB;
        let end = strip_end(strip)?;
        if strip.capacity == 0 || strip.unit_kb == 0 || end <= start || (index > 0 && start < previous_end) {
            return Err(ReadError::InvalidLocations(format!(
                "chunk strip {} has invalid or overlapping geometry",
                strip.strip_sequence
            )));
        }
        previous_end = end;
    }
    Ok(())
}

fn strip_end(strip: &crowdb_protocol::chunkdb::rpc::ChunkStrip) -> ReadResult<u64> {
    u64::from(strip.chunk_offset)
        .checked_add(u64::from(strip.capacity))
        .and_then(|kib| kib.checked_mul(KIB))
        .ok_or_else(|| ReadError::InvalidLocations("strip interval overflows".into()))
}

fn map_metadata_error(error: IoError) -> ReadError {
    match error {
        IoError::ChunkNotFound(message) => ReadError::ChunkDeleted(message),
        other => ReadError::Metadata(other.to_string()),
    }
}
