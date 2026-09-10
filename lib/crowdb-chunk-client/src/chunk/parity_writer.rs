// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `spawn_parity_writes` — durable parity-write spawn helper.
//!
//! Extracted from `EcStripWriter::finish`. Spawns parallel parity
//! write tasks for a finished strip and
//! returns the `JoinHandle`s **without joining** — the caller
//! (`ChunkWriter`) collects them and joins at `seal()` time. This
//! decouples parity durability from strip finish: strip N+1's data
//! writes overlap with strip N's parity writes (root design
//! §3). Replaces the old `ParityBatch` (per-strip join) — the
//! batch-join semantics are gone; `ChunkWriter` owns the handles.

use std::sync::Arc;

use crate::chunk::segment_writer::{spawn_segment_write, SegmentWriteHandle};
use crate::disk_io::DiskWriter;
use crate::{IoError, Result};
use crowdb_common::ec::EcScheme;
use crowdb_protocol::chunkdb::rpc::Chunk;
use crowdb_protocol::chunkdb::rpc::Strip as StripOneof;

/// Spawn durable parity-write tasks for a finished strip. Returns
/// `JoinHandle`s without joining — caller joins at seal time.
///
/// For each parity shard `i`, writes the shard to segment `data_num + i` via
/// `DiskWriter::write`. Production write completion is durable.
pub(crate) fn spawn_parity_writes(
    chunk: &Arc<Chunk>,
    strip_index: u32,
    parity_shards: Vec<Vec<u8>>,
    disk_writer: &Arc<dyn DiskWriter>,
    ec_scheme: &EcScheme,
) -> Result<Vec<SegmentWriteHandle>> {
    let strip = chunk
        .strips
        .get(strip_index as usize)
        .ok_or_else(|| IoError::Internal(format!("strip {strip_index} missing from chunk")))?;
    let ec = match strip.strip.as_ref() {
        Some(StripOneof::EcStrip(ec)) => ec,
        Some(StripOneof::MirrorStrip(_)) => {
            return Err(IoError::Internal("expected EC strip, got mirror".into()));
        }
        None => return Err(IoError::Internal("chunk strip missing oneof".into())),
    };
    let unit_bytes = u64::from(strip.unit_kb) * 1024;
    let data_num = ec_scheme.data_num;

    let mut handles = Vec::with_capacity(parity_shards.len());

    // Parallel parity write tasks (one per parity shard).
    for (i, shard) in parity_shards.into_iter().enumerate() {
        let seg_index = data_num + i;
        let seg = *ec
            .segments
            .get(seg_index)
            .ok_or_else(|| IoError::Internal(format!("segment {seg_index} missing")))?;
        handles.push(spawn_segment_write(
            disk_writer.clone(),
            strip.strip_sequence,
            seg,
            unit_bytes,
            bytes::Bytes::from(shard),
        ));
    }
    Ok(handles)
}
