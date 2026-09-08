// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Object and range reads over current chunk layouts.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use crowdb_protocol::chunkdb::rpc::{
    Chunk, ChunkState, Location, QueryChunkRequest, ReplaceChunkStripRangeRequest,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::Segment;
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

/// A successfully read logical sub-range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRangeData {
    pub start: u64,
    pub end: u64,
    pub data: Bytes,
}

/// A logical sub-range that could not be reconstructed.
#[derive(Debug)]
pub struct FailedReadRange {
    pub start: u64,
    pub end: u64,
    pub error: ReadError,
}

/// Explicit successes and failures for a range read.
#[derive(Debug, Default)]
pub struct PartialReadResult {
    pub ranges: Vec<ReadRangeData>,
    pub failures: Vec<FailedReadRange>,
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
        let partial = self.read_range_partial(locations, start, end).await?;
        if let Some(failure) = partial.failures.into_iter().next() {
            return Err(ReadError::FailedRange {
                start: failure.start,
                end: failure.end,
                message: failure.error.to_string(),
            });
        }
        let expected = usize::try_from(end - start).map_err(|_| ReadError::InvalidRange {
            start,
            end,
            object_length: end,
        })?;
        assemble_ranges(partial.ranges, start, expected)
    }

    pub async fn read_range_partial(
        &self,
        locations: &[Location],
        start: u64,
        end: u64,
    ) -> ReadResult<PartialReadResult> {
        let (locations, object_length) = normalize_locations(locations)?;
        if start > end || end > object_length {
            return Err(ReadError::InvalidRange {
                start,
                end,
                object_length,
            });
        }
        if start == end {
            return Ok(PartialReadResult::default());
        }

        let mut reads = JoinSet::new();
        for location in locations {
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
                reader
                    .read_location_partial(&location, local_start, length, overlap_start)
                    .await
            });
        }
        let mut partial = PartialReadResult::default();
        while let Some(result) = reads.join_next().await {
            let location = result.map_err(|error| ReadError::DiskIo(error.to_string()))??;
            partial.ranges.extend(location.ranges);
            partial.failures.extend(location.failures);
        }
        partial.ranges.sort_unstable_by_key(|range| range.start);
        partial.failures.sort_unstable_by_key(|range| range.start);
        Ok(partial)
    }

    pub fn read_stream(&self, locations: &[Location]) -> ReadResult<ChunkReadStream> {
        let (locations, object_length) = normalize_locations(locations)?;
        Ok(ChunkReadStream {
            reader: self.clone(),
            locations: Arc::from(locations),
            cursor: 0,
            end: object_length,
            window_bytes: self.policy.stream_window_bytes as u64,
            pending_error: None,
        })
    }

    async fn read_location_partial(
        &self,
        location: &Location,
        local_start: u64,
        length: u64,
        logical_start: u64,
    ) -> ReadResult<PartialReadResult> {
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
            let mut chunk = response
                .chunk
                .ok_or_else(|| ReadError::ChunkDeleted(format!("{}:{}", chunk_id.high, chunk_id.low)))?;
            let (partial, observations) = self
                .read_chunk_range_partial(&chunk, physical_start, length, logical_start)
                .await?;
            if Instant::now() >= deadline {
                continue;
            }
            if self
                .mark_observed_failures(&mut chunk, observations)
                .await
                .is_err()
            {
                continue;
            }
            if Instant::now() < deadline {
                return Ok(partial);
            }
        }
        Err(ReadError::LayoutExpired)
    }

    async fn read_chunk_range_partial(
        &self,
        chunk: &Chunk,
        start: u64,
        length: u64,
        logical_start: u64,
    ) -> ReadResult<(PartialReadResult, Vec<StripFailureObservation>)> {
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
        let mut partial = PartialReadResult::default();
        let mut observations = Vec::new();
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
            for (part_start, part_end) in strip_read_parts(strip, cursor, overlap_end)? {
                let range_start = logical_start + (part_start - start);
                let range_end = range_start + (part_end - part_start);
                let observed = self
                    .strip_reader
                    .read_observed(
                        strip,
                        durable_bytes,
                        part_start - strip_start,
                        part_end - part_start,
                    )
                    .await;
                if !observed.failed_segments.is_empty() {
                    observations.push(StripFailureObservation {
                        strip_sequence: strip.strip_sequence,
                        failed_segments: observed.failed_segments,
                    });
                }
                match observed.result {
                    Ok(data) => partial.ranges.push(ReadRangeData {
                        start: range_start,
                        end: range_end,
                        data,
                    }),
                    Err(error) => partial.failures.push(FailedReadRange {
                        start: range_start,
                        end: range_end,
                        error,
                    }),
                }
            }
            cursor = overlap_end;
        }
        if cursor != end {
            return Err(ReadError::InvalidLocations(format!(
                "chunk layout ends at byte {cursor}, requested {end}"
            )));
        }
        Ok((partial, observations))
    }

    async fn mark_observed_failures(
        &self,
        chunk: &mut Chunk,
        observations: Vec<StripFailureObservation>,
    ) -> ReadResult<()> {
        let chunk_id = chunk
            .id
            .ok_or_else(|| ReadError::InvalidLocations("chunk has no ID".into()))?;
        for observation in observations {
            let Some(index) = chunk
                .strips
                .iter()
                .position(|strip| strip.strip_sequence == observation.strip_sequence)
            else {
                return Err(ReadError::Metadata("failed strip disappeared".into()));
            };
            let old = chunk.strips[index].clone();
            let mut replacement = old.clone();
            for segment in observation.failed_segments {
                if !replacement.unavailable_segments.contains(&segment) {
                    replacement.unavailable_segments.push(segment);
                }
            }
            if replacement == old {
                continue;
            }
            replacement.unavailable_segments.sort_by_key(segment_identity);
            let operation_id = failure_operation_id(chunk_id, &replacement);
            let response = self
                .chunkdb
                .replace_chunk_strip_range(ReplaceChunkStripRangeRequest {
                    chunk_id: Some(chunk_id),
                    expected_modify_ts: chunk.modify_ts,
                    start_index: u32::try_from(index).unwrap_or(u32::MAX),
                    old_strips: vec![old],
                    replacement_strips: vec![replacement],
                    operation_id: Some(operation_id),
                })
                .await
                .map_err(|error| ReadError::Metadata(error.to_string()))?;
            *chunk = response
                .chunk
                .ok_or_else(|| ReadError::Metadata("failure marker returned no chunk".into()))?;
        }
        Ok(())
    }
}

struct StripFailureObservation {
    strip_sequence: u32,
    failed_segments: Vec<Segment>,
}

/// Pull-based stream whose emitted item never exceeds the configured window.
pub struct ChunkReadStream {
    reader: ChunkReader,
    locations: Arc<[Location]>,
    cursor: u64,
    end: u64,
    window_bytes: u64,
    pending_error: Option<ReadError>,
}

impl ChunkReadStream {
    pub async fn next_chunk(&mut self) -> Option<ReadResult<Bytes>> {
        if let Some(error) = self.pending_error.take() {
            self.cursor = self.end;
            return Some(Err(error));
        }
        if self.cursor >= self.end {
            return None;
        }
        let next = self.cursor.saturating_add(self.window_bytes).min(self.end);
        let partial = match self
            .reader
            .read_range_partial(&self.locations, self.cursor, next)
            .await
        {
            Ok(partial) => partial,
            Err(error) => {
                self.cursor = self.end;
                return Some(Err(error));
            }
        };
        if let Some(failure) = partial.failures.into_iter().next() {
            let error = ReadError::FailedRange {
                start: failure.start,
                end: failure.end,
                message: failure.error.to_string(),
            };
            if failure.start == self.cursor {
                self.cursor = self.end;
                return Some(Err(error));
            }
            let prefix_len = usize::try_from(failure.start - self.cursor).unwrap_or(usize::MAX);
            let prefix = partial
                .ranges
                .into_iter()
                .filter(|range| range.end <= failure.start)
                .collect();
            let data = assemble_ranges(prefix, self.cursor, prefix_len);
            self.cursor = failure.start;
            self.pending_error = Some(error);
            return Some(data);
        }
        let expected = usize::try_from(next - self.cursor).unwrap_or(usize::MAX);
        let result = assemble_ranges(partial.ranges, self.cursor, expected);
        if result.is_ok() {
            self.cursor = next;
        } else {
            self.cursor = self.end;
        }
        Some(result)
    }
}

fn assemble_ranges(mut ranges: Vec<ReadRangeData>, start: u64, expected: usize) -> ReadResult<Bytes> {
    ranges.sort_unstable_by_key(|range| range.start);
    let actual = ranges.iter().map(|range| range.data.len()).sum::<usize>();
    if actual != expected {
        return Err(ReadError::InvalidLocations(format!(
            "read assembled {actual} bytes, expected {expected}"
        )));
    }
    let mut output = BytesMut::with_capacity(actual);
    let mut cursor = start;
    for range in ranges {
        if range.start != cursor || range.end - range.start != range.data.len() as u64 {
            return Err(ReadError::InvalidLocations(format!(
                "read ranges are not contiguous at byte {cursor}"
            )));
        }
        cursor = range.end;
        output.extend_from_slice(&range.data);
    }
    Ok(output.freeze())
}

fn segment_identity(segment: &Segment) -> (u64, u64, u32, u64, u64) {
    let disk = segment.disk_id.unwrap_or_default();
    (
        disk.high,
        disk.low,
        segment.zone_index,
        segment.unit_offset,
        segment.allocation_ts,
    )
}

fn failure_operation_id(chunk_id: ChunkId, strip: &crowdb_protocol::chunkdb::rpc::ChunkStrip) -> ChunkId {
    let mut high = chunk_id.high ^ 0x111f_a11e_d000_0001;
    let mut low = chunk_id.low ^ u64::from(strip.strip_sequence);
    for segment in &strip.unavailable_segments {
        let disk = segment.disk_id.unwrap_or_default();
        high = high.rotate_left(13) ^ disk.high ^ segment.unit_offset;
        low = low.rotate_left(17) ^ disk.low ^ segment.allocation_ts;
    }
    ChunkId { high, low }
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

fn strip_read_parts(
    strip: &crowdb_protocol::chunkdb::rpc::ChunkStrip,
    start: u64,
    end: u64,
) -> ReadResult<Vec<(u64, u64)>> {
    let Some(crowdb_protocol::chunkdb::rpc::Strip::EcStrip(ec)) = strip.strip.as_ref() else {
        return Ok(vec![(start, end)]);
    };
    let first = ec
        .segments
        .first()
        .ok_or_else(|| ReadError::InvalidLocations("EC strip has no segments".into()))?;
    let shard_bytes = u64::from(first.unit_count)
        .checked_mul(u64::from(strip.unit_kb))
        .and_then(|value| value.checked_mul(KIB))
        .ok_or_else(|| ReadError::InvalidLocations("EC shard size overflows".into()))?;
    if shard_bytes == 0 {
        return Err(ReadError::InvalidLocations("EC shard size is zero".into()));
    }
    let strip_start = u64::from(strip.chunk_offset) * KIB;
    let mut parts = Vec::new();
    let mut cursor = start;
    while cursor < end {
        let local = cursor - strip_start;
        let next_boundary = strip_start.saturating_add(
            (local / shard_bytes)
                .saturating_add(1)
                .saturating_mul(shard_bytes),
        );
        let part_end = end.min(next_boundary);
        parts.push((cursor, part_end));
        cursor = part_end;
    }
    Ok(parts)
}

fn map_metadata_error(error: IoError) -> ReadError {
    match error {
        IoError::ChunkNotFound(message) => ReadError::ChunkDeleted(message),
        other => ReadError::Metadata(other.to_string()),
    }
}
