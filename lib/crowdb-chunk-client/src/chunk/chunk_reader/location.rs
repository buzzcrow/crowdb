// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Logical location resolution and protected physical reads.

use super::layout::{
    failure_operation_id, segment_identity, strip_end, strip_read_parts,
    validate_strip_layout, StripFailureObservation,
};
use super::{ChunkReader, FailedReadRange, LayoutSnapshot, PartialReadResult, ReadRangeData, KIB};
use crate::{ChunkAllocator, ReadError, ReadResult};
use crowdb_protocol::chunkdb::rpc::{
    AdHocEcRecoveryDisposition, AdHocEcRecoveryRequest, Chunk, ChunkState, Location,
};
use std::time::Instant;

impl ChunkReader {
    pub(super) async fn read_location_partial(
        &self,
        location: &Location,
        local_start: u64,
        length: u64,
        logical_start: u64,
    ) -> ReadResult<PartialReadResult> {
        self.read_location_partial_cached(location, local_start, length, logical_start, None)
            .await
    }

    pub(super) async fn read_location_partial_cached(
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

    pub(super) async fn read_chunk_range_partial(
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

    pub(super) async fn read_chunk_range_partial_inner(
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

    pub(super) async fn mark_observed_failures(
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
