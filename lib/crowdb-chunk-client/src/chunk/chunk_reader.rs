// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Object and range reads over current chunk layouts.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use crowdb_protocol::chunkdb::rpc::{
    AdHocEcRecoveryDisposition, AdHocEcRecoveryRequest, Chunk, ChunkState, Location, QueryChunkRequest,
    QueryChunkResponse,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_protocol::frame::{
    parse_frame, parse_frame_views, ChunkLocation, FrameMagic, FRAME_FOOTER_BYTES, FRAME_HEADER_PREFIX_BYTES,
    MAX_FRAME_BYTES, MAX_FRAME_PAYLOAD_BYTES,
};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::client_recovery::ClientRecovery;
use super::read_credit::{retain, ReadBudget, ReadLease, StreamSlots};
use super::strip_reader::StripReader;
use crate::metrics::ReadFlowMetrics;
use crate::{ChunkAllocator, DiskWriter, IoError, ReadError, ReadResult};

const KIB: u64 = 1024;
const DEFAULT_STREAM_WINDOW: usize = 1024 * 1024;

/// Bounded retry, streaming, and EC-recovery memory policy.
#[derive(Debug, Clone)]
pub struct ChunkReadPolicy {
    pub stream_window_bytes: usize,
    pub stream_slots: usize,
    pub global_stream_bytes: usize,
    pub recovery_memory_bytes: usize,
    pub layout_safety_margin: Duration,
    pub max_layout_retries: usize,
    pub ad_hoc_max_jobs: usize,
    pub ad_hoc_threshold_bytes: u64,
    pub ad_hoc_window: Duration,
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

#[derive(Clone)]
struct LayoutSnapshot {
    chunk_id: ChunkId,
    chunk: Arc<Chunk>,
    deadline: Instant,
    valid: Arc<AtomicBool>,
}

impl LayoutSnapshot {
    fn usable(&self) -> bool {
        self.valid.load(Ordering::Acquire) && Instant::now() < self.deadline
    }

    fn invalidate(&self) {
        self.valid.store(false, Ordering::Release);
    }
}

impl Default for ChunkReadPolicy {
    fn default() -> Self {
        Self {
            stream_window_bytes: DEFAULT_STREAM_WINDOW,
            stream_slots: 3,
            global_stream_bytes: 256 * 1024 * 1024,
            recovery_memory_bytes: 256 * 1024 * 1024,
            layout_safety_margin: Duration::from_millis(5),
            max_layout_retries: 3,
            ad_hoc_max_jobs: 32,
            ad_hoc_threshold_bytes: 1024 * 1024,
            ad_hoc_window: Duration::from_secs(1),
        }
    }
}

impl ChunkReadPolicy {
    fn validate(&self) -> ReadResult<()> {
        if self.stream_window_bytes == 0
            || self.stream_slots == 0
            || self.global_stream_bytes < DEFAULT_STREAM_WINDOW
            || self.recovery_memory_bytes == 0
            || self.recovery_memory_bytes > u32::MAX as usize
            || self.max_layout_retries == 0
            || self.ad_hoc_max_jobs == 0
            || self.ad_hoc_threshold_bytes == 0
            || self.ad_hoc_window.is_zero()
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
    flow_metrics: Arc<crate::metrics::ReadFlowMetrics>,
    stream_budget: Arc<ReadBudget>,
}

impl ChunkReader {
    pub fn new(
        chunkdb: Arc<dyn ChunkAllocator>,
        disk_io: Arc<dyn DiskWriter>,
        policy: ChunkReadPolicy,
    ) -> ReadResult<Self> {
        Self::new_with_metrics(chunkdb, disk_io, policy, Arc::default(), Arc::default())
    }

    pub(crate) fn new_with_metrics(
        chunkdb: Arc<dyn ChunkAllocator>,
        disk_io: Arc<dyn DiskWriter>,
        policy: ChunkReadPolicy,
        metrics: Arc<crate::metrics::ReadRecoveryMetrics>,
        flow_metrics: Arc<crate::metrics::ReadFlowMetrics>,
    ) -> ReadResult<Self> {
        policy.validate()?;
        let recovery_memory = Arc::new(Semaphore::new(policy.recovery_memory_bytes));
        let ad_hoc = Arc::new(ClientRecovery::new(
            Arc::clone(&chunkdb),
            Arc::clone(&recovery_memory),
            policy.ad_hoc_max_jobs,
            policy.ad_hoc_threshold_bytes,
            policy.ad_hoc_window,
            metrics,
        ));
        let stream_budget = Arc::new(ReadBudget::new(
            policy.global_stream_bytes,
            Arc::clone(&flow_metrics),
        ));
        Ok(Self {
            chunkdb,
            strip_reader: StripReader::new(disk_io, recovery_memory, policy.recovery_memory_bytes)
                .with_ad_hoc(ad_hoc),
            policy,
            flow_metrics,
            stream_budget,
        })
    }

    /// Cumulative work counters for the shared read path.
    pub fn flow_metrics_snapshot(&self) -> crate::metrics::ReadFlowMetricsSnapshot {
        self.flow_metrics.snapshot()
    }

    async fn query_chunk_timed(&self, chunk_id: ChunkId) -> crate::Result<QueryChunkResponse> {
        let started = Instant::now();
        self.flow_metrics.layout_queries.inc();
        let result = self
            .chunkdb
            .query_chunk(QueryChunkRequest {
                chunk_id: Some(chunk_id),
            })
            .await;
        self.flow_metrics
            .layout_query_wait_ns
            .inc_by(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        result
    }

    async fn query_layout(&self, chunk_id: ChunkId) -> ReadResult<LayoutSnapshot> {
        let started = Instant::now();
        let response = self
            .query_chunk_timed(chunk_id)
            .await
            .map_err(map_metadata_error)?;
        let deadline = started
            + Duration::from_millis(response.layout_validity_ms)
                .saturating_sub(self.policy.layout_safety_margin);
        let chunk = response
            .chunk
            .ok_or_else(|| ReadError::ChunkDeleted(format!("{}:{}", chunk_id.high, chunk_id.low)))?;
        Ok(LayoutSnapshot {
            chunk_id,
            chunk: Arc::new(chunk),
            deadline,
            valid: Arc::new(AtomicBool::new(true)),
        })
    }

    pub async fn read_object(&self, locations: &[Location]) -> ReadResult<Vec<Bytes>> {
        self.flow_metrics.location_normalizations.inc();
        self.flow_metrics
            .locations_examined
            .inc_by(u64::try_from(locations.len()).unwrap_or(u64::MAX));
        let (locations, object_length) = normalize_locations(locations)?;
        let partial = self
            .read_range_partial_normalized(&locations, object_length, 0, object_length)
            .await?;
        complete_read(partial, 0, object_length)
    }

    pub async fn read_range(&self, locations: &[Location], start: u64, end: u64) -> ReadResult<Vec<Bytes>> {
        let partial = self.read_range_partial(locations, start, end).await?;
        complete_read(partial, start, end)
    }

    pub async fn read_range_partial(
        &self,
        locations: &[Location],
        start: u64,
        end: u64,
    ) -> ReadResult<PartialReadResult> {
        self.flow_metrics.location_normalizations.inc();
        self.flow_metrics
            .locations_examined
            .inc_by(u64::try_from(locations.len()).unwrap_or(u64::MAX));
        let (locations, object_length) = normalize_locations(locations)?;
        self.read_range_partial_normalized(&locations, object_length, start, end)
            .await
    }

    async fn read_range_partial_normalized(
        &self,
        locations: &[Location],
        object_length: u64,
        start: u64,
        end: u64,
    ) -> ReadResult<PartialReadResult> {
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
        let first = locations.partition_point(|location| {
            location.logical_offset.saturating_add(location.logical_length) <= start
        });
        let mut examined = 0u64;
        for location in &locations[first..] {
            let loc_start = location.logical_offset;
            if loc_start >= end {
                break;
            }
            examined += 1;
            let loc_end = loc_start + location.logical_length;
            let overlap_start = start.max(loc_start);
            let overlap_end = end.min(loc_end);
            if overlap_start >= overlap_end {
                continue;
            }
            let reader = self.clone();
            let location = location.clone();
            reads.spawn(async move {
                let local_start = overlap_start - loc_start;
                let length = overlap_end - overlap_start;
                reader
                    .read_location_partial(&location, local_start, length, overlap_start)
                    .await
            });
        }
        self.flow_metrics.range_locations_examined.inc_by(examined);
        let mut partial = PartialReadResult::default();
        while let Some(result) = reads.join_next().await {
            let location = result.map_err(|error| ReadError::DiskIo(error.to_string()))??;
            partial.ranges.extend(location.ranges);
            partial.failures.extend(location.failures);
        }
        partial.ranges.sort_unstable_by_key(|range| range.start);
        partial.failures.sort_unstable_by_key(|range| range.start);
        let mut failures: Vec<FailedReadRange> = Vec::with_capacity(partial.failures.len());
        for failure in partial.failures {
            if let Some(previous) = failures
                .last_mut()
                .filter(|previous| previous.end == failure.start)
            {
                previous.end = failure.end;
            } else {
                failures.push(failure);
            }
        }
        partial.failures = failures;
        Ok(partial)
    }

    pub fn read_stream(&self, locations: &[Location]) -> ReadResult<ChunkReadStream> {
        self.flow_metrics.location_normalizations.inc();
        self.flow_metrics
            .locations_examined
            .inc_by(u64::try_from(locations.len()).unwrap_or(u64::MAX));
        let (locations, object_length) = normalize_locations(locations)?;
        self.range_stream(locations, 0, object_length, object_length)
    }

    /// Builds a pull-based stream for the exact logical half-open range.
    pub fn read_range_stream(
        &self,
        locations: &[Location],
        start: u64,
        end: u64,
    ) -> ReadResult<ChunkReadStream> {
        self.flow_metrics.location_normalizations.inc();
        self.flow_metrics
            .locations_examined
            .inc_by(u64::try_from(locations.len()).unwrap_or(u64::MAX));
        let (locations, object_length) = normalize_locations(locations)?;
        self.range_stream(locations, start, end, object_length)
    }

    fn range_stream(
        &self,
        locations: Vec<Location>,
        start: u64,
        end: u64,
        object_length: u64,
    ) -> ReadResult<ChunkReadStream> {
        if start > end || end > object_length {
            return Err(ReadError::InvalidRange {
                start,
                end,
                object_length,
            });
        }
        Ok(ChunkReadStream {
            reader: self.clone(),
            locations: Arc::from(locations),
            cursor: start,
            fetch_cursor: start,
            delivery_cursor: start,
            end,
            window_bytes: self.policy.stream_window_bytes as u64,
            slots: Arc::new(StreamSlots::new(self.policy.stream_slots)),
            reads: JoinSet::new(),
            completed: BTreeMap::new(),
            layouts: Vec::new(),
            pending_error: None,
            pending_ranges: Vec::new().into_iter(),
        })
    }

    /// Reads one variable-length frame through the normal mirror/EC recovery
    /// path and verifies both its integrity trailer and its public kind.
    ///
    /// Stream and tree metadata keep a compact physical frame location rather
    /// than a synthetic fixed-stride `Location`.  They use this entry point so
    /// a checksum mismatch still marks the serving replica unavailable and
    /// retries from a protected source before any payload is exposed.
    #[allow(
        clippy::too_many_lines,
        reason = "layout acquisition, validation, failure observation, and retries share one frame-read state machine"
    )]
    pub async fn read_verified_frame(
        &self,
        chunk_id: ChunkId,
        frame_offset: u64,
        frame_length: u64,
        expected_magic: FrameMagic,
    ) -> ReadResult<Bytes> {
        if frame_length < (FRAME_HEADER_PREFIX_BYTES + FRAME_FOOTER_BYTES) as u64
            || frame_length > MAX_FRAME_BYTES as u64
        {
            return Err(ReadError::InvalidLocations(
                "frame length is outside the public frame bounds".into(),
            ));
        }
        let frame_end = frame_offset
            .checked_add(frame_length)
            .ok_or_else(|| ReadError::InvalidLocations("frame end overflows".into()))?;
        for attempt in 1..=self.policy.max_layout_retries {
            let query_started = Instant::now();
            let response = match self.query_chunk_timed(chunk_id).await {
                Ok(response) => response,
                Err(error) => {
                    tracing::warn!(
                        chunk_high = chunk_id.high,
                        chunk_low = chunk_id.low,
                        frame_offset,
                        frame_length,
                        attempt,
                        error = %error,
                        "verified frame layout query failed"
                    );
                    return Err(map_metadata_error(error));
                }
            };
            let layout_validity_ms = response.layout_validity_ms;
            let validity = Duration::from_millis(layout_validity_ms);
            let deadline = query_started + validity.saturating_sub(self.policy.layout_safety_margin);
            let mut chunk = response
                .chunk
                .ok_or_else(|| ReadError::ChunkDeleted(format!("{}:{}", chunk_id.high, chunk_id.low)))?;
            tracing::debug!(
                chunk_high = chunk_id.high,
                chunk_low = chunk_id.low,
                frame_offset,
                frame_length,
                attempt,
                layout_validity_ms,
                strip_count = chunk.strips.len(),
                "verified frame layout acquired"
            );
            let (physical, observations) = match self
                .read_chunk_range_partial(&chunk, frame_offset, frame_length, frame_offset)
                .await
            {
                Ok(result) => result,
                Err(error) => {
                    tracing::warn!(
                        chunk_high = chunk_id.high,
                        chunk_low = chunk_id.low,
                        frame_offset,
                        frame_length,
                        attempt,
                        error = %error,
                        "verified frame physical read failed"
                    );
                    return Err(error);
                }
            };
            let bytes = match extract_physical_range(&physical.ranges, frame_offset..frame_end) {
                Ok(bytes) if physical.failures.is_empty() => bytes,
                Ok(_) | Err(_) => {
                    let failed_start = physical.failures.first().map(|failure| failure.start);
                    let failed_end = physical.failures.first().map(|failure| failure.end);
                    let failed_error = physical.failures.first().map(|failure| failure.error.to_string());
                    let expired = Instant::now() >= deadline;
                    let observation_error = if expired {
                        None
                    } else {
                        self.mark_observed_failures(&mut chunk, observations).await.err()
                    };
                    tracing::warn!(
                        chunk_high = chunk_id.high,
                        chunk_low = chunk_id.low,
                        frame_offset,
                        frame_length,
                        attempt,
                        completed_ranges = physical.ranges.len(),
                        failed_ranges = physical.failures.len(),
                        failed_start = ?failed_start,
                        failed_end = ?failed_end,
                        failed_error = ?failed_error,
                        expired,
                        observation_error = ?observation_error,
                        "verified frame read did not produce a complete range"
                    );
                    if expired || observation_error.is_some() {
                        return Err(ReadError::DataLoss(
                            "frame bytes could not be reconstructed".into(),
                        ));
                    }
                    continue;
                }
            };
            let decode_started = Instant::now();
            let parsed_frame = parse_frame(&bytes, chunk_id);
            let parse_ns = u64::try_from(decode_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            self.flow_metrics.frame_decode_wait_ns.inc_by(parse_ns);
            self.flow_metrics.frame_parse_wait_ns.inc_by(parse_ns);
            match parsed_frame {
                Ok(frame) if frame.header.magic == expected_magic => {
                    if Instant::now() >= deadline {
                        tracing::warn!(
                            chunk_high = chunk_id.high,
                            chunk_low = chunk_id.low,
                            frame_offset,
                            frame_length,
                            attempt,
                            elapsed_ms = query_started.elapsed().as_millis(),
                            layout_validity_ms,
                            "verified frame read exceeded layout validity"
                        );
                        continue;
                    }
                    self.mark_observed_failures(&mut chunk, observations).await?;
                    return Ok(bytes);
                }
                Ok(frame) => {
                    tracing::warn!(
                        chunk_high = chunk_id.high,
                        chunk_low = chunk_id.low,
                        frame_offset,
                        frame_length,
                        attempt,
                        expected_magic = ?expected_magic,
                        actual_magic = ?frame.header.magic,
                        "verified frame kind disagrees with its location"
                    );
                    return Err(ReadError::DataLoss(
                        "frame kind disagrees with its location".into(),
                    ));
                }
                Err(error) => {
                    let served_segments: Vec<Segment> = observations
                        .iter()
                        .flat_map(|observation| observation.served_segments.iter().copied())
                        .collect();
                    let corrupt = mark_served_segments_corrupt(observations);
                    let corrupt_segments = corrupt.len();
                    let expired = Instant::now() >= deadline;
                    let observation_error = if corrupt_segments == 0 || expired {
                        None
                    } else {
                        self.mark_observed_failures(&mut chunk, corrupt).await.err()
                    };
                    tracing::warn!(
                        chunk_high = chunk_id.high,
                        chunk_low = chunk_id.low,
                        frame_offset,
                        frame_length,
                        attempt,
                        error = %error,
                        corrupt_segments,
                        served_segments = ?served_segments,
                        expired,
                        observation_error = ?observation_error,
                        "verified frame parsing failed"
                    );
                    if corrupt_segments == 0 || expired || observation_error.is_some() {
                        return Err(ReadError::DataLoss(format!("invalid chunk frame: {error}")));
                    }
                }
            }
        }
        tracing::warn!(
            chunk_high = chunk_id.high,
            chunk_low = chunk_id.low,
            frame_offset,
            frame_length,
            attempts = self.policy.max_layout_retries,
            "verified frame read exhausted valid chunk layouts"
        );
        Err(ReadError::LayoutExpired)
    }

    async fn read_location_partial(
        &self,
        location: &Location,
        local_start: u64,
        length: u64,
        logical_start: u64,
    ) -> ReadResult<PartialReadResult> {
        self.read_location_partial_cached(location, local_start, length, logical_start, None)
            .await
    }

    async fn read_location_partial_cached(
        &self,
        location: &Location,
        local_start: u64,
        length: u64,
        logical_start: u64,
        mut first_layout: Option<LayoutSnapshot>,
    ) -> ReadResult<PartialReadResult> {
        if location.length != location.logical_length {
            return self
                .read_framed_location(location, local_start, length, logical_start, first_layout)
                .await;
        }
        let chunk_id = location
            .chunk_id
            .ok_or_else(|| ReadError::InvalidLocations("location has no chunk ID".into()))?;
        let physical_start = location
            .offset
            .checked_add(local_start)
            .ok_or_else(|| ReadError::InvalidLocations("location physical offset overflows".into()))?;
        for _ in 0..self.policy.max_layout_retries {
            let layout = match first_layout.take().filter(LayoutSnapshot::usable) {
                Some(layout) => layout,
                None => self.query_layout(chunk_id).await?,
            };
            let (partial, observations) = self
                .read_chunk_range_partial(&layout.chunk, physical_start, length, logical_start)
                .await?;
            if Instant::now() >= layout.deadline {
                layout.invalidate();
                continue;
            }
            if observations
                .iter()
                .any(|observation| !observation.failed_segments.is_empty())
            {
                layout.invalidate();
                let mut chunk = (*layout.chunk).clone();
                if self
                    .mark_observed_failures(&mut chunk, observations)
                    .await
                    .is_err()
                {
                    continue;
                }
            }
            if Instant::now() < layout.deadline {
                return Ok(partial);
            }
        }
        Err(ReadError::LayoutExpired)
    }

    async fn read_framed_location(
        &self,
        location: &Location,
        local_start: u64,
        length: u64,
        logical_start: u64,
        first_layout: Option<LayoutSnapshot>,
    ) -> ReadResult<PartialReadResult> {
        let chunk_id = location
            .chunk_id
            .ok_or_else(|| ReadError::InvalidLocations("location has no chunk ID".into()))?;
        if local_start
            .checked_add(length)
            .map_or(true, |end| end > location.logical_length)
        {
            return Err(ReadError::InvalidLocations(
                "framed location logical range is invalid".into(),
            ));
        }
        let framed = ChunkLocation {
            chunk_id,
            frame_offset: location.offset,
            logical_length: location.logical_length,
        };
        if framed
            .physical_length()
            .map_err(|error| ReadError::InvalidLocations(error.to_string()))?
            != location.length
        {
            return Err(ReadError::InvalidLocations(
                "framed location length is inconsistent".into(),
            ));
        }
        let physical_range = framed
            .physical_range_for_subrange(local_start..local_start.saturating_add(length))
            .map_err(|error| ReadError::InvalidLocations(error.to_string()))?;
        let selected_logical_start =
            (local_start / MAX_FRAME_PAYLOAD_BYTES as u64) * MAX_FRAME_PAYLOAD_BYTES as u64;
        self.read_verified_framed_range(
            framed,
            physical_range,
            selected_logical_start,
            local_start..local_start + length,
            logical_start,
            first_layout,
        )
        .await
    }

    async fn read_verified_framed_range(
        &self,
        framed: ChunkLocation,
        physical_range: Range<u64>,
        selected_logical_start: u64,
        requested: Range<u64>,
        output_logical_start: u64,
        mut first_layout: Option<LayoutSnapshot>,
    ) -> ReadResult<PartialReadResult> {
        let chunk_id = framed.chunk_id;
        for _ in 0..self.policy.max_layout_retries {
            let layout = match first_layout.take().filter(LayoutSnapshot::usable) {
                Some(layout) => layout,
                None => self.query_layout(chunk_id).await?,
            };
            let (physical, observations) = self
                .read_chunk_range_partial(
                    &layout.chunk,
                    physical_range.start,
                    physical_range.end - physical_range.start,
                    physical_range.start,
                )
                .await?;
            let decode_started = Instant::now();
            let decoded = decode_framed_partial(
                &physical,
                framed,
                selected_logical_start,
                requested.clone(),
                output_logical_start,
                &self.flow_metrics,
            );
            self.flow_metrics
                .frame_decode_wait_ns
                .inc_by(u64::try_from(decode_started.elapsed().as_nanos()).unwrap_or(u64::MAX));
            let parsed = match decoded {
                Ok(parsed) => parsed,
                Err(error) => {
                    layout.invalidate();
                    let corrupt = mark_served_segments_corrupt(observations);
                    if corrupt.is_empty() || Instant::now() >= layout.deadline {
                        return Err(error);
                    }
                    let mut chunk = (*layout.chunk).clone();
                    if self.mark_observed_failures(&mut chunk, corrupt).await.is_err() {
                        return Err(error);
                    }
                    continue;
                }
            };
            if Instant::now() >= layout.deadline {
                layout.invalidate();
                continue;
            }
            if observations
                .iter()
                .any(|observation| !observation.failed_segments.is_empty())
            {
                layout.invalidate();
                let mut chunk = (*layout.chunk).clone();
                if self
                    .mark_observed_failures(&mut chunk, observations)
                    .await
                    .is_err()
                {
                    continue;
                }
            }
            return Ok(parsed);
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
        let started = Instant::now();
        let result = self
            .read_chunk_range_partial_inner(chunk, start, length, logical_start)
            .await;
        self.flow_metrics
            .chunk_read_wait_ns
            .inc_by(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        result
    }

    async fn read_chunk_range_partial_inner(
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
                let read_started = Instant::now();
                let observed = self
                    .strip_reader
                    .read_observed(
                        strip,
                        chunk.modify_ts,
                        durable_bytes,
                        part_start - strip_start,
                        part_end - part_start,
                    )
                    .await;
                self.flow_metrics
                    .strip_read_wait_ns
                    .inc_by(u64::try_from(read_started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                if !observed.failed_segments.is_empty() {
                    observations.push(StripFailureObservation {
                        strip_sequence: strip.strip_sequence,
                        failed_segments: observed.failed_segments,
                        served_segments: observed.served_segments,
                    });
                } else if !observed.served_segments.is_empty() {
                    observations.push(StripFailureObservation {
                        strip_sequence: strip.strip_sequence,
                        failed_segments: Vec::new(),
                        served_segments: observed.served_segments,
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
            for segment in observation.failed_segments {
                if old.unavailable_segments.contains(&segment) {
                    continue;
                }
                let mut replacement = chunk.strips[index].clone();
                replacement.unavailable_segments.push(segment);
                replacement.unavailable_segments.sort_by_key(segment_identity);
                let operation_id = failure_operation_id(chunk_id, &replacement);
                let response = self
                    .chunkdb
                    .ad_hoc_ec_recovery(AdHocEcRecoveryRequest {
                        version: 1,
                        chunk_id: Some(chunk_id),
                        expected_modify_ts: chunk.modify_ts,
                        strip_sequence: observation.strip_sequence,
                        failed_segment: Some(segment),
                        operation_id: Some(operation_id),
                        request_full_block: false,
                    })
                    .await
                    .map_err(|error| ReadError::Metadata(error.to_string()))?;
                if response.disposition != AdHocEcRecoveryDisposition::Marked {
                    return Err(ReadError::Metadata(format!(
                        "corrupt segment report was {:?}",
                        response.disposition
                    )));
                }
                *chunk = self
                    .query_chunk_timed(chunk_id)
                    .await
                    .map_err(|error| ReadError::Metadata(error.to_string()))?
                    .chunk
                    .ok_or_else(|| ReadError::Metadata("corrupt report returned no chunk".into()))?;
            }
        }
        Ok(())
    }
}

fn decode_framed_partial(
    physical: &PartialReadResult,
    framed: ChunkLocation,
    selected_logical_start: u64,
    requested: Range<u64>,
    output_logical_start: u64,
    flow_metrics: &ReadFlowMetrics,
) -> ReadResult<PartialReadResult> {
    let first_frame = selected_logical_start / MAX_FRAME_PAYLOAD_BYTES as u64;
    let last_frame = (requested.end - 1) / MAX_FRAME_PAYLOAD_BYTES as u64;
    let mut result = PartialReadResult::default();
    let mut magic = None;
    for frame_index in first_frame..=last_frame {
        let frame_logical_start = frame_index * MAX_FRAME_PAYLOAD_BYTES as u64;
        let payload_len = (framed.logical_length - frame_logical_start).min(MAX_FRAME_PAYLOAD_BYTES as u64);
        let frame_start = framed
            .frame_offset
            .checked_add(frame_index * MAX_FRAME_BYTES as u64)
            .ok_or_else(|| ReadError::InvalidLocations("frame offset overflows".into()))?;
        let frame_end = frame_start
            .checked_add(FRAME_HEADER_PREFIX_BYTES as u64 + payload_len + FRAME_FOOTER_BYTES as u64)
            .ok_or_else(|| ReadError::InvalidLocations("frame end overflows".into()))?;
        let wanted_start = requested.start.max(frame_logical_start);
        let wanted_end = requested.end.min(frame_logical_start + payload_len);
        let output_start = output_logical_start + wanted_start - requested.start;
        let output_end = output_logical_start + wanted_end - requested.start;
        if physical
            .failures
            .iter()
            .any(|failure| failure.start < frame_end && failure.end > frame_start)
        {
            result.failures.push(FailedReadRange {
                start: output_start,
                end: output_end,
                error: ReadError::DataLoss("frame bytes could not be reconstructed".into()),
            });
            continue;
        }
        let views = extract_physical_views(&physical.ranges, frame_start..frame_end)?;
        let parse_started = Instant::now();
        let parsed = if views.len() == 1 {
            parse_frame(&views[0], framed.chunk_id).map(|frame| (frame.header, frame.physical_length))
        } else {
            let slices: Vec<_> = views.iter().map(Bytes::as_ref).collect();
            parse_frame_views(&slices, framed.chunk_id).map(|frame| (frame.header, frame.physical_length))
        };
        flow_metrics
            .frame_parse_wait_ns
            .inc_by(u64::try_from(parse_started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        let (header, physical_length) =
            parsed.map_err(|error| ReadError::DataLoss(format!("invalid chunk frame: {error}")))?;
        if !matches!(header.magic, FrameMagic::RepoSmallV1 | FrameMagic::RepoLargeV1) {
            return Err(ReadError::DataLoss(
                "framed location has unsupported frame kind".into(),
            ));
        }
        match magic {
            Some(previous) if previous != header.magic => {
                return Err(ReadError::DataLoss("framed location mixes frame kinds".into()));
            }
            None => magic = Some(header.magic),
            Some(_) => {}
        }
        if physical_length as u64 != frame_end - frame_start {
            return Err(ReadError::DataLoss("frame length disagrees with location".into()));
        }
        let payload_start = usize::try_from(wanted_start - frame_logical_start)
            .map_err(|_| ReadError::InvalidLocations("frame payload range overflows".into()))?;
        let payload_end = usize::try_from(wanted_end - frame_logical_start)
            .map_err(|_| ReadError::InvalidLocations("frame payload range overflows".into()))?;
        let physical_payload_start = usize::from(header.payload_offset) + payload_start;
        let physical_payload_end = usize::from(header.payload_offset) + payload_end;
        let mut logical_cursor = output_start;
        for view in slice_views(&views, physical_payload_start..physical_payload_end)? {
            let view_end = logical_cursor + view.len() as u64;
            result.ranges.push(ReadRangeData {
                start: logical_cursor,
                end: view_end,
                data: view,
            });
            logical_cursor = view_end;
        }
        if logical_cursor != output_end {
            return Err(ReadError::InvalidLocations(
                "frame payload views are incomplete".into(),
            ));
        }
    }
    Ok(result)
}

fn extract_physical_range(ranges: &[ReadRangeData], wanted: Range<u64>) -> ReadResult<Bytes> {
    let first = ranges.partition_point(|range| range.end <= wanted.start);
    if let Some(range) = ranges.get(first) {
        if range.start <= wanted.start && range.end >= wanted.end {
            let offset = usize::try_from(wanted.start - range.start)
                .map_err(|_| ReadError::InvalidLocations("physical frame offset overflows".into()))?;
            let length = usize::try_from(wanted.end - wanted.start)
                .map_err(|_| ReadError::InvalidLocations("physical frame length overflows".into()))?;
            return Ok(range.data.slice(offset..offset + length));
        }
    }
    let mut output =
        BytesMut::with_capacity(usize::try_from(wanted.end - wanted.start).unwrap_or(usize::MAX));
    let mut cursor = wanted.start;
    for range in &ranges[first..] {
        if range.end <= cursor || range.start >= wanted.end {
            continue;
        }
        let start = cursor.max(range.start);
        if start != cursor {
            return Err(ReadError::InvalidLocations(
                "physical frame data has a gap".into(),
            ));
        }
        let end = wanted.end.min(range.end);
        let offset = usize::try_from(start - range.start)
            .map_err(|_| ReadError::InvalidLocations("physical frame offset overflows".into()))?;
        let length = usize::try_from(end - start)
            .map_err(|_| ReadError::InvalidLocations("physical frame length overflows".into()))?;
        output.extend_from_slice(&range.data[offset..offset + length]);
        cursor = end;
        if cursor == wanted.end {
            break;
        }
    }
    if cursor != wanted.end {
        return Err(ReadError::InvalidLocations(
            "physical frame data is incomplete".into(),
        ));
    }
    Ok(output.freeze())
}

fn extract_physical_views(ranges: &[ReadRangeData], wanted: Range<u64>) -> ReadResult<Vec<Bytes>> {
    let first = ranges.partition_point(|range| range.end <= wanted.start);
    let mut output = Vec::new();
    let mut cursor = wanted.start;
    for range in &ranges[first..] {
        if range.start >= wanted.end {
            break;
        }
        if range.end <= cursor {
            continue;
        }
        if range.start > cursor {
            return Err(ReadError::InvalidLocations(
                "physical frame data has a gap".into(),
            ));
        }
        let end = wanted.end.min(range.end);
        let offset = usize::try_from(cursor - range.start)
            .map_err(|_| ReadError::InvalidLocations("physical frame offset overflows".into()))?;
        let length = usize::try_from(end - cursor)
            .map_err(|_| ReadError::InvalidLocations("physical frame length overflows".into()))?;
        output.push(range.data.slice(offset..offset + length));
        cursor = end;
        if cursor == wanted.end {
            break;
        }
    }
    if cursor != wanted.end {
        return Err(ReadError::InvalidLocations(
            "physical frame data is incomplete".into(),
        ));
    }
    Ok(output)
}

fn slice_views(views: &[Bytes], wanted: Range<usize>) -> ReadResult<Vec<Bytes>> {
    let mut output = Vec::new();
    let mut cursor = 0_usize;
    let mut covered = wanted.start;
    for view in views {
        let view_end = cursor.saturating_add(view.len());
        if view_end > covered && cursor < wanted.end {
            let start = covered.saturating_sub(cursor);
            let end = (wanted.end - cursor).min(view.len());
            output.push(view.slice(start..end));
            covered += end - start;
        }
        cursor = view_end;
        if covered == wanted.end {
            break;
        }
    }
    if covered != wanted.end {
        return Err(ReadError::InvalidLocations(
            "frame payload views are incomplete".into(),
        ));
    }
    Ok(output)
}

struct StripFailureObservation {
    strip_sequence: u32,
    failed_segments: Vec<Segment>,
    served_segments: Vec<Segment>,
}

fn mark_served_segments_corrupt(
    mut observations: Vec<StripFailureObservation>,
) -> Vec<StripFailureObservation> {
    let serving_count: usize = observations
        .iter()
        .map(|observation| observation.served_segments.len())
        .sum();
    if serving_count != 1 {
        return Vec::new();
    }
    for observation in &mut observations {
        for segment in std::mem::take(&mut observation.served_segments) {
            if !observation.failed_segments.contains(&segment) {
                observation.failed_segments.push(segment);
            }
        }
    }
    observations.retain(|observation| !observation.failed_segments.is_empty());
    observations
}

type StreamRead = (u64, u64, ReadResult<PartialReadResult>, Arc<ReadLease>);

/// Ordered, bounded read pipeline whose frames retain their read credits.
pub struct ChunkReadStream {
    reader: ChunkReader,
    locations: Arc<[Location]>,
    cursor: u64,
    fetch_cursor: u64,
    delivery_cursor: u64,
    end: u64,
    window_bytes: u64,
    slots: Arc<StreamSlots>,
    reads: JoinSet<StreamRead>,
    completed: BTreeMap<u64, StreamRead>,
    layouts: Vec<LayoutSnapshot>,
    pending_error: Option<ReadError>,
    pending_ranges: std::vec::IntoIter<ReadRangeData>,
}

impl ChunkReadStream {
    pub async fn next_chunk(&mut self) -> Option<ReadResult<Bytes>> {
        loop {
            if let Some(pending) = self.next_pending() {
                return Some(pending);
            }
            if self.delivery_cursor >= self.end {
                return None;
            }
            let budget = Arc::clone(&self.reader.stream_budget);
            let _registration = budget.register();
            let notified = budget.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Err(error) = self.fill_slots().await {
                self.abort(error);
                continue;
            }
            if let Some((start, end, result, lease)) = self.completed.remove(&self.delivery_cursor) {
                self.load_unit(start, end, result, &lease);
                continue;
            }
            if self.reads.is_empty() {
                let started = Instant::now();
                notified.await;
                self.reader
                    .flow_metrics
                    .stream_credit_wait_ns
                    .inc_by(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                continue;
            }
            match self.reads.join_next().await {
                Some(Ok(completed)) => {
                    self.reader.flow_metrics.stream_units_completed.inc();
                    if completed.0 != self.delivery_cursor {
                        self.reader.flow_metrics.stream_out_of_order.inc();
                    }
                    self.completed.insert(completed.0, completed);
                }
                Some(Err(error)) => self.abort(ReadError::DiskIo(error.to_string())),
                None => {}
            }
        }
    }

    async fn fill_slots(&mut self) -> ReadResult<()> {
        while self.fetch_cursor < self.end {
            let (next, physical_bytes) = self.next_unit(self.fetch_cursor)?;
            let Some(lease) = self.slots.try_reserve(&self.reader.stream_budget, physical_bytes) else {
                self.reader.flow_metrics.stream_credit_stalls.inc();
                break;
            };
            let start = self.fetch_cursor;
            let index = self.locations.partition_point(|location| {
                location.logical_offset.saturating_add(location.logical_length) <= start
            });
            let location = self.locations[index].clone();
            let layout = self
                .layout_for(
                    location
                        .chunk_id
                        .ok_or_else(|| ReadError::InvalidLocations("location has no chunk ID".into()))?,
                )
                .await?;
            let reader = self.reader.clone();
            self.reader.flow_metrics.stream_windows.inc();
            self.reader.flow_metrics.range_locations_examined.inc();
            self.reads.spawn(async move {
                let local_start = start - location.logical_offset;
                let result = reader
                    .read_location_partial_cached(&location, local_start, next - start, start, Some(layout))
                    .await;
                (start, next, result, lease)
            });
            self.fetch_cursor = next;
        }
        Ok(())
    }

    async fn layout_for(&mut self, chunk_id: ChunkId) -> ReadResult<LayoutSnapshot> {
        if let Some(layout) = self
            .layouts
            .iter()
            .find(|layout| layout.chunk_id == chunk_id && layout.usable())
        {
            return Ok(layout.clone());
        }
        let reader = self.reader.clone();
        let layout = tokio::spawn(async move { reader.query_layout(chunk_id).await })
            .await
            .map_err(|error| ReadError::Metadata(error.to_string()))??;
        self.layouts.retain(|cached| cached.chunk_id != chunk_id);
        if self.layouts.len() == 8 {
            self.layouts.remove(0);
        }
        self.layouts.push(layout.clone());
        Ok(layout)
    }

    fn next_unit(&self, start: u64) -> ReadResult<(u64, usize)> {
        let first = self.locations.partition_point(|location| {
            location.logical_offset.saturating_add(location.logical_length) <= start
        });
        let location = self
            .locations
            .get(first)
            .ok_or_else(|| ReadError::InvalidLocations("stream has no location for requested byte".into()))?;
        let local = start - location.logical_offset;
        let window = self.window_bytes.min(DEFAULT_STREAM_WINDOW as u64);
        if location.length == location.logical_length {
            let length = window.min(location.logical_length - local).min(self.end - start);
            return Ok((start + length, length as usize));
        }
        let frames = (window / MAX_FRAME_BYTES as u64).max(1);
        let frame_index = local / MAX_FRAME_PAYLOAD_BYTES as u64;
        let local_end = ((frame_index + frames) * MAX_FRAME_PAYLOAD_BYTES as u64)
            .min(location.logical_length)
            .min(self.end - location.logical_offset);
        let frame_location = ChunkLocation {
            chunk_id: location
                .chunk_id
                .ok_or_else(|| ReadError::InvalidLocations("location has no chunk ID".into()))?,
            frame_offset: location.offset,
            logical_length: location.logical_length,
        };
        let physical = frame_location
            .physical_range_for_subrange(local..local_end)
            .map_err(|error| ReadError::InvalidLocations(error.to_string()))?;
        let bytes = usize::try_from(physical.end - physical.start)
            .map_err(|_| ReadError::InvalidLocations("stream unit exceeds address space".into()))?;
        Ok((location.logical_offset + local_end, bytes))
    }

    fn load_unit(
        &mut self,
        start: u64,
        end: u64,
        result: ReadResult<PartialReadResult>,
        lease: &Arc<ReadLease>,
    ) {
        self.delivery_cursor = end;
        let partial = match result {
            Ok(partial) => partial,
            Err(error) => {
                self.abort(error);
                return;
            }
        };
        let failure = partial.failures.into_iter().next();
        let verified_end = failure.as_ref().map_or(end, |failure| failure.start);
        let mut ranges = partial.ranges;
        let prefix_len = ranges.partition_point(|range| range.end <= verified_end);
        let mut cursor = start;
        for range in &ranges[..prefix_len] {
            if range.start != cursor || range.end - range.start != range.data.len() as u64 {
                self.abort(ReadError::InvalidLocations(format!(
                    "read ranges are not contiguous at byte {cursor}"
                )));
                return;
            }
            cursor = range.end;
        }
        if cursor != verified_end {
            self.abort(ReadError::InvalidLocations(format!(
                "read ranges are not contiguous at byte {cursor}"
            )));
            return;
        }
        ranges.truncate(prefix_len);
        for range in &mut ranges {
            range.data = retain(std::mem::take(&mut range.data), Arc::clone(lease));
        }
        self.pending_ranges = ranges.into_iter();
        if let Some(failure) = failure {
            self.reads.abort_all();
            self.completed.clear();
            self.fetch_cursor = self.end;
            self.delivery_cursor = self.end;
            self.pending_error = Some(ReadError::FailedRange {
                start: failure.start,
                end: failure.end,
                message: failure.error.to_string(),
            });
        }
    }

    fn abort(&mut self, error: ReadError) {
        self.reads.abort_all();
        self.completed.clear();
        self.fetch_cursor = self.end;
        self.delivery_cursor = self.end;
        self.pending_error = Some(error);
    }

    fn next_pending(&mut self) -> Option<ReadResult<Bytes>> {
        if let Some(range) = self.pending_ranges.next() {
            self.cursor = range.end;
            return Some(Ok(range.data));
        }
        if let Some(error) = self.pending_error.take() {
            self.cursor = self.end;
            return Some(Err(error));
        }
        None
    }
}

fn complete_read(partial: PartialReadResult, start: u64, end: u64) -> ReadResult<Vec<Bytes>> {
    if let Some(failure) = partial.failures.into_iter().next() {
        return Err(ReadError::FailedRange {
            start: failure.start,
            end: failure.end,
            message: failure.error.to_string(),
        });
    }
    let mut cursor = start;
    let mut buffers = Vec::with_capacity(partial.ranges.len());
    for range in partial.ranges {
        if range.start != cursor || range.end - range.start != range.data.len() as u64 {
            return Err(ReadError::InvalidLocations(format!(
                "read ranges are not contiguous at byte {cursor}"
            )));
        }
        cursor = range.end;
        buffers.push(range.data);
    }
    if cursor != end {
        return Err(ReadError::InvalidLocations(format!(
            "read ranges end at byte {cursor}, expected {end}"
        )));
    }
    Ok(buffers)
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
