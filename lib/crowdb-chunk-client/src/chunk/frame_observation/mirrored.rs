// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fixed current-layout physical readers for an excluded Stream/Wal owner.

use crowdb_protocol::chunk_stream::{StreamName, StreamPurpose};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, Strip};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{FrameScanConfig, FrameScanMode};
use std::sync::Arc;
use tokio::time::Instant;

use super::{
    FrameObservationError, FrameObservationResult, FrameWindowReader, MAX_FRAME_OBSERVATION_REPLICAS,
};
use crate::{DiskWriter, STREAM_CHUNK_BYTES};

mod read;
use read::MirroredWindowReader;

struct MirroredLayout {
    chunk: Arc<Chunk>,
    disk_io: Arc<dyn DiskWriter>,
    chunk_id: ChunkId,
    capacity: u64,
    deadline: Instant,
}

/// Owns one immutable layout snapshot. Construction requires caller-proven owner
/// exclusion; clones of its readers retain the same conservative local deadline.
pub struct MirroredFrameReaders {
    layout: Arc<MirroredLayout>,
    readers: Vec<Arc<dyn FrameWindowReader>>,
}

impl MirroredFrameReaders {
    pub fn new(
        chunk: Arc<Chunk>,
        stream_name: StreamName,
        disk_io: Arc<dyn DiskWriter>,
        deadline: Instant,
    ) -> FrameObservationResult<Self> {
        if deadline <= Instant::now() {
            return Err(FrameObservationError::DeadlineExpired);
        }
        let chunk_id = chunk.id.ok_or_else(|| invalid_layout("chunk has no identity"))?;
        let purpose = if StreamPurpose::Wal.matches(chunk_id) {
            StreamPurpose::Wal
        } else if StreamPurpose::Stream.matches(chunk_id) {
            StreamPurpose::Stream
        } else {
            return Err(invalid_layout("physical tail reader requires a Stream/Wal chunk"));
        };
        if chunk.chunk_type != purpose.chunk_type() as i32
            || chunk.owner_key != stream_name.chunk_owner_key()
            || chunk.writer_epoch == 0
            || ![ChunkState::Active as i32, ChunkState::Sealed as i32].contains(&chunk.state)
        {
            return Err(invalid_layout("chunk purpose, owner, epoch or state is invalid"));
        }
        let (capacity, replicas) = validate_layout(&chunk, chunk_id)?;
        let layout = Arc::new(MirroredLayout {
            chunk,
            disk_io,
            chunk_id,
            capacity,
            deadline,
        });
        let readers = (0..replicas)
            .map(|replica| {
                Arc::new(MirroredWindowReader {
                    layout: Arc::clone(&layout),
                    replica,
                }) as Arc<dyn FrameWindowReader>
            })
            .collect();
        Ok(Self { layout, readers })
    }

    #[must_use]
    pub fn readers(&self) -> &[Arc<dyn FrameWindowReader>] {
        &self.readers
    }
    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.layout.capacity
    }
    #[must_use]
    pub fn deadline(&self) -> Instant {
        self.layout.deadline
    }

    pub fn scan_config(
        &self,
        physical_start: u64,
        logical_start: u64,
        required_end: u64,
        max_append_bytes: u64,
    ) -> FrameObservationResult<FrameScanConfig> {
        if physical_start > required_end || required_end > self.layout.capacity || max_append_bytes == 0 {
            return Err(FrameObservationError::InvalidRequest);
        }
        Ok(FrameScanConfig {
            chunk_id: self.layout.chunk_id,
            mode: FrameScanMode::Grouped,
            physical_start,
            physical_limit: self.layout.capacity,
            required_end,
            logical_start,
            max_append_bytes,
        })
    }
}

fn validate_layout(chunk: &Chunk, chunk_id: ChunkId) -> FrameObservationResult<(u64, usize)> {
    if chunk.strips.is_empty() {
        return Err(invalid_layout("chunk has no allocated strips"));
    }
    let mut end = 0_u64;
    let mut replicas = 0;
    for strip in &chunk.strips {
        let start = u64::from(strip.chunk_offset) * 1024;
        let length = u64::from(strip.capacity) * 1024;
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        if start != end || length == 0 || unit_bytes == 0 || start + length > STREAM_CHUNK_BYTES {
            return Err(invalid_layout(
                "chunk strips have gaps, overlaps or invalid capacity",
            ));
        }
        let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
            return Err(invalid_layout("tail layout is not mirrored"));
        };
        if mirror.segments.is_empty() || mirror.segments.len() > MAX_FRAME_OBSERVATION_REPLICAS {
            return Err(invalid_layout("mirror replica count is invalid"));
        }
        for (index, segment) in mirror.segments.iter().enumerate() {
            if segment.disk_id.is_none()
                || segment.owner_chunk != Some(chunk_id)
                || segment.allocation_ts == 0
                || u64::from(segment.unit_count)
                    .checked_mul(unit_bytes)
                    .map_or(true, |capacity| capacity < length)
                || segment
                    .unit_offset
                    .checked_add(u64::from(segment.unit_count))
                    .and_then(|end| end.checked_mul(unit_bytes))
                    .is_none()
                || mirror.segments[..index]
                    .iter()
                    .any(|previous| previous.disk_id == segment.disk_id)
            {
                return Err(invalid_layout(
                    "mirror allocation identity or capacity is invalid",
                ));
            }
        }
        replicas = replicas.max(mirror.segments.len());
        end = start + length;
    }
    if chunk.acknowledged_cursor > end {
        return Err(invalid_layout("checkpoint exceeds allocated layout"));
    }
    Ok((end, replicas))
}

fn invalid_layout(message: &str) -> FrameObservationError {
    FrameObservationError::InvalidLayout(message.into())
}
