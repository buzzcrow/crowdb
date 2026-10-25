// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Object and range reads over current chunk layouts.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crowdb_protocol::chunkdb::rpc::{Chunk, Location, QueryChunkRequest, QueryChunkResponse};
use crowdb_protocol::common::ChunkId;
use crowdb_rpc_ffi::OwnedPayload;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::client_recovery::ClientRecovery;
use super::read_credit::ReadBudget;
use super::strip_reader::StripReader;
use crate::{ChunkAllocator, DiskWriter, IoError, ReadError, ReadResult};

mod frame_views;
mod framing;
mod layout;
mod layout_cache;
mod location;
mod stream;
mod verified_frame;

use layout::{normalize_locations, StripFailureObservation};
use layout_cache::FrameLayoutCache;
pub use stream::ChunkReadStream;

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
    pub data: OwnedPayload,
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
    frame_layouts: Arc<FrameLayoutCache>,
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
            frame_layouts: Arc::default(),
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
        buffers.push(range.data.into_bytes());
    }
    if cursor != end {
        return Err(ReadError::InvalidLocations(format!(
            "read ranges end at byte {cursor}, expected {end}"
        )));
    }
    Ok(buffers)
}

fn map_metadata_error(error: IoError) -> ReadError {
    match error {
        IoError::ChunkNotFound(message) => ReadError::ChunkDeleted(message),
        other => ReadError::Metadata(other.to_string()),
    }
}
