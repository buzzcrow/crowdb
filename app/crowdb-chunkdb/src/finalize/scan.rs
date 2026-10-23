// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Content authority for abandoned grouped chunks; checkpoint authority for objects.

use std::sync::Arc;
use std::time::Duration;

use crowdb_chunk_client::{select_frame_prefix, FrameWindowReader, MAX_FRAME_OBSERVATION_REPLICAS};
use crowdb_protocol::chunk_stream::chunk_owner_key_matches_type;
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkType, Strip};
use crowdb_protocol::frame::{FrameScanConfig, FrameScanMode, MAX_FRAME_SCAN_INPUT_BYTES};
use tokio::time::Instant;

use crate::conversion::io::ConversionDiskIo;

mod read;
use read::FinalizerWindowReader;

pub(super) async fn complete_boundary(io: Arc<ConversionDiskIo>, chunk: Arc<Chunk>) -> Result<u64, String> {
    let chunk_id = chunk.id.ok_or("finalizer chunk has no identity")?;
    let purpose = ChunkType::try_from(chunk.chunk_type).map_err(|()| "invalid finalizer chunk type")?;
    if chunk_id.high >> 56 != purpose as u64 || !chunk_owner_key_matches_type(purpose, &chunk.owner_key) {
        return Err("finalizer chunk identity or owner does not match purpose".into());
    }
    let grouped = [ChunkType::Stream as i32, ChunkType::Wal as i32].contains(&chunk.chunk_type);
    // Ordinary object publication already persists its exact physical cursor.
    // Only grouped streams have acknowledged data newer than that checkpoint.
    if !grouped && chunk.acknowledged_cursor == 0 {
        return Ok(0);
    }
    let (capacity, replicas) = layout_bounds(&chunk)?;
    let config = FrameScanConfig {
        chunk_id,
        mode: if grouped {
            FrameScanMode::Grouped
        } else {
            FrameScanMode::Ordinary
        },
        physical_start: 0,
        physical_limit: if grouped {
            capacity
        } else {
            chunk.acknowledged_cursor
        },
        required_end: chunk.acknowledged_cursor,
        logical_start: 0,
        max_append_bytes: capacity,
    };
    let readers: Vec<Arc<dyn FrameWindowReader>> = (0..replicas)
        .map(|replica| {
            Arc::new(FinalizerWindowReader::new(
                Arc::clone(&io),
                Arc::clone(&chunk),
                replica,
            )) as _
        })
        .collect();
    let deadline = Instant::now() + Duration::from_secs(30);
    let window = u32::try_from(MAX_FRAME_SCAN_INPUT_BYTES).expect("bounded observation window");
    select_frame_prefix(&readers, config, window, deadline)
        .await
        .map(|selected| selected.observation().summary().physical_end)
        .map_err(|error| {
            format!(
                "chunk {chunk_id:?}: cannot prove complete prefix covering acknowledged cursor {}: {error}",
                chunk.acknowledged_cursor
            )
        })
}

fn layout_bounds(chunk: &Chunk) -> Result<(u64, usize), String> {
    let mut end = 0;
    let mut replicas = 1;
    for strip in &chunk.strips {
        let start = u64::from(strip.chunk_offset) * 1024;
        let length = u64::from(strip.capacity) * 1024;
        if start != end || length == 0 || strip.unit_kb == 0 {
            return Err("finalizer layout has gaps, overlaps or empty strips".into());
        }
        let segments = match &strip.strip {
            Some(Strip::MirrorStrip(mirror)) => {
                if mirror.segments.is_empty() || mirror.segments.len() > MAX_FRAME_OBSERVATION_REPLICAS {
                    return Err("finalizer mirror count is invalid".into());
                }
                replicas = replicas.max(mirror.segments.len());
                &mirror.segments
            }
            Some(Strip::EcStrip(ec)) if ec.data_num > 0 && ec.segments.len() >= ec.data_num as usize => {
                &ec.segments
            }
            _ => return Err("finalizer strip has no valid physical layout".into()),
        };
        for segment in segments {
            if segment.disk_id.is_none() || segment.owner_chunk != chunk.id || segment.allocation_ts == 0 {
                return Err("finalizer segment has invalid allocation identity".into());
            }
        }
        end = start.checked_add(length).ok_or("finalizer layout overflows")?;
    }
    if end == 0 || chunk.acknowledged_cursor > end || end != u64::from(chunk.capacity) * 1024 {
        return Err("finalizer checkpoint or capacity exceeds physical layout".into());
    }
    Ok((end, replicas))
}
