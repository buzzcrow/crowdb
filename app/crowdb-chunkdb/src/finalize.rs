// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Finalization of abandoned Active chunks.

use std::sync::Arc;

use crowdb_protocol::chunk_task::{ChunkTaskValue, FINALIZE_CHUNK_KIND_VERSION, TASK_KIND_FINALIZE_CHUNK};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, Strip};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{frame_length, parse_frame, parse_header};

use crate::conversion::io::ConversionDiskIo;
use crate::lifecycle::LifecycleHandler;
use crate::task::executor::TaskFuture;
use crate::task::{TaskHandler, TaskOutcome};

/// Executes the durable liveness task. Empty chunks have no possible frame
/// boundary and can be reclaimed immediately. Nonempty chunks are retained
/// until the DiskIO-backed frame scanner is available to derive their exact
/// sealed boundary.
pub struct FinalizeChunkTaskHandler {
    lifecycle: Arc<LifecycleHandler>,
    io: Arc<ConversionDiskIo>,
}

impl FinalizeChunkTaskHandler {
    #[must_use]
    pub fn new(lifecycle: Arc<LifecycleHandler>, io: Arc<ConversionDiskIo>) -> Self {
        Self { lifecycle, io }
    }
}

impl TaskHandler for FinalizeChunkTaskHandler {
    fn kind(&self) -> u16 {
        TASK_KIND_FINALIZE_CHUNK
    }

    fn supports_version(&self, version: u16) -> bool {
        version == FINALIZE_CHUNK_KIND_VERSION
    }

    fn execute<'a>(&'a self, task: &'a ChunkTaskValue) -> TaskFuture<'a> {
        Box::pin(async move {
            let chunk_id = task.partition_id;
            let chunk = match self.lifecycle.query_chunk(&chunk_id).await {
                Ok(chunk) => chunk,
                Err(error) => {
                    return TaskOutcome::Retry {
                        delay_ms: 1_000,
                        error_code: 1,
                        error: error.to_string(),
                    };
                }
            };
            if chunk.state == ChunkState::Deleted as i32 || chunk.state == ChunkState::Sealed as i32 {
                return TaskOutcome::Complete;
            }
            let boundary = match scan_complete_frame_boundary(&self.io, &chunk, chunk_id).await {
                Ok(boundary) => boundary,
                Err(error) => {
                    return TaskOutcome::Retry {
                        delay_ms: 1_000,
                        error_code: 3,
                        error,
                    };
                }
            };
            if boundary == 0 {
                return match self.lifecycle.delete_chunk(&chunk_id).await {
                    Ok(_) => TaskOutcome::Complete,
                    Err(error) => TaskOutcome::Retry {
                        delay_ms: 1_000,
                        error_code: 2,
                        error: error.to_string(),
                    },
                };
            }
            let seal_length = u32::try_from(boundary.div_ceil(1024)).unwrap_or(u32::MAX);
            match self.lifecycle.seal_chunk(&chunk_id, seal_length).await {
                Ok(_) => TaskOutcome::Complete,
                Err(error) => TaskOutcome::Retry {
                    delay_ms: 1_000,
                    error_code: 4,
                    error: error.to_string(),
                },
            }
        })
    }
}

async fn scan_complete_frame_boundary(
    io: &ConversionDiskIo,
    chunk: &Chunk,
    chunk_id: ChunkId,
) -> Result<u64, String> {
    let mut cursor = 0_u64;
    loop {
        let Some(strip) = chunk.strips.iter().find(|strip| {
            let start = u64::from(strip.chunk_offset) * 1024;
            let end = start.saturating_add(u64::from(strip.capacity) * 1024);
            start <= cursor && cursor < end
        }) else {
            return Ok(cursor);
        };
        let Strip::MirrorStrip(mirror) = strip
            .strip
            .as_ref()
            .ok_or_else(|| format!("strip {} has no physical layout", strip.strip_sequence))?
        else {
            return Err(format!(
                "frame finalization for EC strip {} is not available",
                strip.strip_sequence
            ));
        };
        let segment = mirror
            .segments
            .iter()
            .find(|segment| !strip.unavailable_segments.contains(segment))
            .ok_or_else(|| format!("all mirrors unavailable for strip {}", strip.strip_sequence))?;
        let strip_start = u64::from(strip.chunk_offset) * 1024;
        let strip_end = strip_start.saturating_add(u64::from(strip.capacity) * 1024);
        let local_offset = cursor.saturating_sub(strip_start);
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        let header = match io
            .read_segment_range(
                segment,
                unit_bytes,
                local_offset,
                u32::try_from(crowdb_protocol::frame::FRAME_HEADER_PREFIX_BYTES)
                    .expect("frame header fits u32"),
            )
            .await
        {
            Ok(header) => header,
            Err(error) => return Err(error.to_string()),
        };
        let Ok(header) = parse_header(&header) else {
            return Ok(cursor);
        };
        let Ok(length) = frame_length(header) else {
            return Ok(cursor);
        };
        let frame_end = cursor.saturating_add(u64::try_from(length).expect("frame length fits u64"));
        if frame_end > strip_end {
            return Ok(cursor);
        }
        let frame = io
            .read_segment_range(
                segment,
                unit_bytes,
                local_offset,
                u32::try_from(length).expect("frame length fits u32"),
            )
            .await
            .map_err(|error| error.to_string())?;
        if parse_frame(&frame, chunk_id).is_err() {
            return Ok(cursor);
        }
        cursor = frame_end;
    }
}
