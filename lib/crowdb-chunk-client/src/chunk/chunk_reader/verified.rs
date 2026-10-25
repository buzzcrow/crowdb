// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Verified frame reads under a persisted or owner-synchronized prefix.

use super::{
    extract_physical_range, mark_served_segments_corrupt, Bytes, ChunkId, ChunkReader, ChunkState, Instant,
    ReadError, ReadResult, Segment,
};
use crowdb_protocol::frame::{
    parse_frame, FrameMagic, FRAME_FOOTER_BYTES, FRAME_HEADER_PREFIX_BYTES, MAX_FRAME_BYTES,
};

impl ChunkReader {
    /// Read and verify a frame under the ordinary persisted chunk boundary.
    /// # Errors
    /// Returns invalid geometry, unavailable IO or failed frame verification.
    pub async fn read_verified_frame(
        &self,
        chunk_id: ChunkId,
        offset: u64,
        length: u64,
        magic: FrameMagic,
    ) -> ReadResult<Bytes> {
        self.read_verified_frame_bounded(chunk_id, offset, length, magic, None)
            .await
    }

    /// Read a frame from an already synchronized Stream/Wal prefix supplied by
    /// its owning stream. This bound is local to this call and never enters the
    /// ordinary reader's layout cache or persisted metadata.
    /// # Errors
    /// Rejects a non-stream chunk, an invalid bound or failed frame verification.
    pub async fn read_synchronized_stream_frame(
        &self,
        chunk_id: ChunkId,
        offset: u64,
        length: u64,
        synchronized_end: u64,
    ) -> ReadResult<Bytes> {
        use crowdb_protocol::chunk_stream::StreamPurpose;
        if !StreamPurpose::Stream.matches(chunk_id) && !StreamPurpose::Wal.matches(chunk_id) {
            return Err(ReadError::InvalidLocations(
                "local stream boundary has a different chunk purpose".into(),
            ));
        }
        self.read_verified_frame_bounded(
            chunk_id,
            offset,
            length,
            FrameMagic::StreamV2,
            Some(synchronized_end),
        )
        .await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "layout acquisition, validation, failure observation, and retries share one frame-read state machine"
    )]
    async fn read_verified_frame_bounded(
        &self,
        chunk_id: ChunkId,
        frame_offset: u64,
        frame_length: u64,
        expected_magic: FrameMagic,
        synchronized_end: Option<u64>,
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
            let layout = match self
                .frame_layout(chunk_id, frame_end, synchronized_end.is_some())
                .await
            {
                Ok(layout) => layout,
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
                    return Err(error);
                }
            };
            let deadline = layout.deadline;
            let layout_validity_ms = deadline.saturating_duration_since(Instant::now()).as_millis();
            let mut chunk = (*layout.chunk).clone();
            if let Some(end) = synchronized_end {
                if frame_end > end
                    || (chunk.state == ChunkState::Sealed as i32 && end > chunk.acknowledged_cursor)
                {
                    return Err(ReadError::InvalidLocations(
                        "frame exceeds synchronized stream prefix".into(),
                    ));
                }
                chunk.acknowledged_cursor = chunk.acknowledged_cursor.max(end);
            }
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
                    layout.invalidate();
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
                    layout.invalidate();
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
                    if !layout.usable() {
                        layout.invalidate();
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
                    if layout.has_new_failures(&observations) {
                        layout.invalidate();
                        self.mark_observed_failures(&mut chunk, observations).await?;
                    }
                    return Ok(bytes);
                }
                Ok(frame) => {
                    layout.invalidate();
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
                    layout.invalidate();
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
}
