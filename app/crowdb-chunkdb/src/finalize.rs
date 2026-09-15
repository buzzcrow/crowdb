// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Finalization of abandoned Active chunks.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crowdb_protocol::chunk_task::{ChunkTaskValue, FINALIZE_CHUNK_KIND_VERSION, TASK_KIND_FINALIZE_CHUNK};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, ChunkStrip, Strip};
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
            let safe_at = task
                .eligible_at_ms
                .saturating_add(crowdb_protocol::timing::DEFAULT_MAX_WRITE_REQUEST_AGE_MS)
                .saturating_add(crowdb_protocol::timing::DEFAULT_MAX_CLOCK_SKEW_MS)
                .saturating_add(crowdb_protocol::timing::DEFAULT_FINALIZER_SCANNER_MARGIN_MS);
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |duration| {
                    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
                });
            if now_ms < safe_at {
                return TaskOutcome::Retry {
                    delay_ms: safe_at.saturating_sub(now_ms),
                    error_code: 0,
                    error: "waiting for pre-expiry writes to age out".into(),
                };
            }
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
        let header = match io
            .read_chunk_range(chunk, cursor, crowdb_protocol::frame::FRAME_HEADER_PREFIX_BYTES)
            .await
        {
            Ok(header) => header,
            Err(error) => return Err(error.clone()),
        };
        let Ok(header) = parse_header(&header) else {
            return Ok(cursor);
        };
        let Ok(length) = frame_length(header) else {
            return Ok(cursor);
        };
        let frame_end = cursor.saturating_add(u64::try_from(length).expect("frame length fits u64"));
        let frame = io.read_chunk_range(chunk, cursor, length).await?;
        if parse_frame(&frame, chunk_id).is_err() {
            return Ok(cursor);
        }
        cursor = frame_end;
    }
}

trait FinalizerRead {
    fn read_chunk_range<'a>(
        &'a self,
        chunk: &'a Chunk,
        offset: u64,
        length: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send + 'a>>;
}

impl FinalizerRead for ConversionDiskIo {
    fn read_chunk_range<'a>(
        &'a self,
        chunk: &'a Chunk,
        offset: u64,
        length: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send + 'a>> {
        Box::pin(async move {
            let end = offset
                .checked_add(u64::try_from(length).map_err(|_| "frame read length overflows")?)
                .ok_or("frame read range overflows")?;
            let mut cursor = offset;
            let mut bytes = Vec::with_capacity(length);
            while cursor < end {
                let strip = chunk
                    .strips
                    .iter()
                    .find(|strip| strip_contains(strip, cursor))
                    .ok_or("chunk layout ends before frame")?;
                let strip_start = u64::from(strip.chunk_offset) * 1024;
                let strip_end = strip_start.saturating_add(u64::from(strip.capacity) * 1024);
                let take = usize::try_from((end.min(strip_end)).saturating_sub(cursor))
                    .map_err(|_| "frame part exceeds addressable memory")?;
                bytes.extend(read_strip_data(self, strip, cursor - strip_start, take).await?);
                cursor = cursor.saturating_add(u64::try_from(take).expect("usize fits u64"));
            }
            Ok(bytes)
        })
    }
}

fn strip_contains(strip: &ChunkStrip, offset: u64) -> bool {
    let start = u64::from(strip.chunk_offset) * 1024;
    let end = start.saturating_add(u64::from(strip.capacity) * 1024);
    start <= offset && offset < end
}

async fn read_strip_data(
    io: &ConversionDiskIo,
    strip: &ChunkStrip,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, String> {
    let unit_bytes = u64::from(strip.unit_kb) * 1024;
    match strip.strip.as_ref() {
        Some(Strip::MirrorStrip(mirror)) => {
            let segment = mirror
                .segments
                .iter()
                .find(|segment| !strip.unavailable_segments.contains(segment))
                .ok_or_else(|| format!("all mirrors unavailable for strip {}", strip.strip_sequence))?;
            io.read_segment_range(
                segment,
                unit_bytes,
                offset,
                u32::try_from(length).map_err(|_| "frame range exceeds RPC size")?,
            )
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|error| error.to_string())
        }
        Some(Strip::EcStrip(ec)) => {
            let data_num = usize::try_from(ec.data_num).map_err(|_| "EC data count overflows")?;
            if data_num == 0 || ec.segments.len() < data_num {
                return Err(format!("invalid EC strip {}", strip.strip_sequence));
            }
            let shard_bytes = u64::from(ec.segments[0].unit_count).saturating_mul(unit_bytes);
            if shard_bytes == 0 {
                return Err(format!("empty EC shard {}", strip.strip_sequence));
            }
            let mut cursor = offset;
            let end = offset.saturating_add(u64::try_from(length).expect("usize fits u64"));
            let mut bytes = Vec::with_capacity(length);
            while cursor < end {
                let shard = usize::try_from(cursor / shard_bytes).map_err(|_| "EC shard index overflows")?;
                if shard >= data_num {
                    return Err(format!("EC data range exceeds strip {}", strip.strip_sequence));
                }
                let segment = &ec.segments[shard];
                if strip.unavailable_segments.contains(segment) {
                    return Err(format!("EC data shard {shard} is unavailable"));
                }
                let local = cursor % shard_bytes;
                let take = (end - cursor).min(shard_bytes - local);
                bytes.extend(
                    io.read_segment_range(
                        segment,
                        unit_bytes,
                        local,
                        u32::try_from(take).map_err(|_| "EC frame range exceeds RPC size")?,
                    )
                    .await
                    .map_err(|error| error.to_string())?,
                );
                cursor = cursor.saturating_add(take);
            }
            Ok(bytes)
        }
        None => Err(format!("strip {} has no physical layout", strip.strip_sequence)),
    }
}
