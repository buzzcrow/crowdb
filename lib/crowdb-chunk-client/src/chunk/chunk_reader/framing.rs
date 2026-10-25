// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::frame_views::{extract_physical_views, slice_views};
use super::layout::mark_served_segments_corrupt;
use super::{ChunkReader, FailedReadRange, LayoutSnapshot, PartialReadResult, ReadRangeData};
use crate::metrics::ReadFlowMetrics;
use crate::{ReadError, ReadResult};
use crowdb_protocol::chunkdb::rpc::Location;
use crowdb_protocol::frame::{
    parse_frame, parse_frame_views, ChunkLocation, FrameLayout, FrameMagic, FRAME_FOOTER_BYTES,
    MAX_FRAME_BYTES,
};
use crowdb_rpc_ffi::OwnedPayload;
use std::ops::Range;
use std::time::Instant;

#[derive(Clone, Copy)]
struct FramedLocation {
    location: ChunkLocation,
    geometry: FrameLayout,
}

pub(super) fn object_layout(location: &Location) -> ReadResult<FrameLayout> {
    if FrameLayout::OrdinaryV2
        .physical_length(location.logical_length)
        .ok()
        == Some(location.length)
    {
        return Ok(FrameLayout::OrdinaryV2);
    }
    Err(ReadError::InvalidLocations(
        "framed location length is inconsistent".into(),
    ))
}

impl ChunkReader {
    pub(super) async fn read_framed_location(
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
        let geometry = object_layout(location)?;
        let framed = FramedLocation {
            location: ChunkLocation {
                chunk_id,
                frame_offset: location.offset,
                logical_length: location.logical_length,
            },
            geometry,
        };
        let physical_range = geometry
            .physical_range(framed.location, local_start..local_start + length)
            .map_err(|error| ReadError::InvalidLocations(error.to_string()))?;
        let capacity = geometry.payload_capacity();
        let selected_logical_start = (local_start / capacity) * capacity;
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
        framed: FramedLocation,
        physical_range: Range<u64>,
        selected_logical_start: u64,
        requested: Range<u64>,
        output_logical_start: u64,
        mut first_layout: Option<LayoutSnapshot>,
    ) -> ReadResult<PartialReadResult> {
        let chunk_id = framed.location.chunk_id;
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
}

fn decode_framed_partial(
    physical: &PartialReadResult,
    framed: FramedLocation,
    selected_logical_start: u64,
    requested: Range<u64>,
    output_logical_start: u64,
    flow_metrics: &ReadFlowMetrics,
) -> ReadResult<PartialReadResult> {
    let first_frame = selected_logical_start / framed.geometry.payload_capacity();
    let last_frame = (requested.end - 1) / framed.geometry.payload_capacity();
    let mut result = PartialReadResult::default();
    let mut magic = None;
    for frame_index in first_frame..=last_frame {
        let frame_logical_start = frame_index * framed.geometry.payload_capacity();
        let payload_len =
            (framed.location.logical_length - frame_logical_start).min(framed.geometry.payload_capacity());
        let frame_start = framed
            .location
            .frame_offset
            .checked_add(frame_index * MAX_FRAME_BYTES as u64)
            .ok_or_else(|| ReadError::InvalidLocations("frame offset overflows".into()))?;
        let frame_end = frame_start
            .checked_add(framed.geometry.header_bytes() as u64 + payload_len + FRAME_FOOTER_BYTES as u64)
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
            parse_frame(&views[0], framed.location.chunk_id)
                .map(|frame| (frame.header, frame.physical_length))
        } else {
            let slices: Vec<_> = views.iter().map(OwnedPayload::as_ref).collect();
            parse_frame_views(&slices, framed.location.chunk_id)
                .map(|frame| (frame.header, frame.physical_length))
        };
        flow_metrics
            .frame_parse_wait_ns
            .inc_by(u64::try_from(parse_started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        let (header, physical_length) =
            parsed.map_err(|error| ReadError::DataLoss(format!("invalid chunk frame: {error}")))?;
        if !matches!(header.magic, FrameMagic::RepoSmallV2 | FrameMagic::RepoLargeV2) {
            return Err(ReadError::DataLoss(
                "framed location has unsupported frame kind".into(),
            ));
        }
        if usize::from(header.payload_offset) != framed.geometry.header_bytes() {
            return Err(ReadError::DataLoss(
                "frame revision disagrees with location geometry".into(),
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
