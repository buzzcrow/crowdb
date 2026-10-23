// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Ordered delivery and logical read-credit lifetime.

use super::super::read_credit::{retain, ReadLease, StreamSlots};
use super::layout::normalize_locations;
use super::framing::object_layout;
use super::{ChunkReader, LayoutSnapshot, PartialReadResult, ReadRangeData, DEFAULT_STREAM_WINDOW};
use crate::{ReadError, ReadResult};
use bytes::Bytes;
use crowdb_protocol::chunkdb::rpc::Location;
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{ChunkLocation, MAX_FRAME_BYTES};
use crowdb_rpc_ffi::OwnedPayload;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;

impl ChunkReader {
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

    pub(super) fn range_stream(
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
        self.next_payload()
            .await
            .map(|result| result.map(OwnedPayload::into_bytes))
    }

    /// Pull a verified logical view retaining its native allocation metadata.
    pub async fn next_payload(&mut self) -> Option<ReadResult<OwnedPayload>> {
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
        let geometry = object_layout(location)?;
        let capacity = geometry.payload_capacity();
        let frame_index = local / capacity;
        let local_end = ((frame_index + frames) * capacity)
            .min(location.logical_length)
            .min(self.end - location.logical_offset);
        let frame_location = ChunkLocation {
            chunk_id: location
                .chunk_id
                .ok_or_else(|| ReadError::InvalidLocations("location has no chunk ID".into()))?,
            frame_offset: location.offset,
            logical_length: location.logical_length,
        };
        let physical = geometry
            .physical_range(frame_location, local..local_end)
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

    fn next_pending(&mut self) -> Option<ReadResult<OwnedPayload>> {
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
