// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Exact decimal counter encoding for browser observations.

use crowdb_chunk_kv::{Result, TreeObservation};
use crowdb_chunk_stream::StreamMetadataObservation;
use serde_json::{json, Value};

pub(super) fn tree(observation: Result<TreeObservation>) -> Value {
    let value = match observation {
        Ok(value) => value,
        Err(error) => return json!({"error":error.to_string()}),
    };
    let runtime = value.runtime.map(|stats| {
        json!({
            "io_failed":stats.io_failed,
            "snapshot_pages_total":stats.snapshot_pages_total.to_string(),
            "snapshot_pages_written":stats.snapshot_pages_written.to_string(),
            "buffer_pool_resident":stats.buffer_pool_resident.to_string(),
            "buffer_pool_dirty":stats.buffer_pool_dirty.to_string(),
            "buffer_pool_num_frames":stats.buffer_pool_num_frames.to_string(),
            "buffer_pool_hits":stats.buffer_pool_hits.to_string(),
            "buffer_pool_misses":stats.buffer_pool_misses.to_string(),
        })
    });
    let stats = value.maintenance;
    let maintenance = json!({
        "checkpoints":stats.checkpoints.to_string(),
        "reclaimed_tree_bytes":stats.reclaimed_tree_bytes.to_string(),
        "reclaimed_journal_bytes":stats.reclaimed_journal_bytes.to_string(),
        "split_pages_reused":stats.split_pages_reused.to_string(),
        "split_pages_rebuilt":stats.split_pages_rebuilt.to_string(),
        "materialization_passes":stats.materialization_passes.to_string(),
        "materialization_failures":stats.materialization_failures.to_string(),
        "materialization_bytes":stats.materialization_bytes.to_string(),
    });
    json!({
        "checkpoint_manifest":value.checkpoint_manifest.to_string(),
        "checkpoint_applied_seq":value.checkpoint_applied_seq.to_string(),
        "runtime":runtime, "maintenance":maintenance,
    })
}

pub(super) fn journal(value: &StreamMetadataObservation) -> Value {
    let chunk = &value.chunk;
    let chunk = json!({
        "chunk_id":format!("{:016x}{:016x}",chunk.chunk_id.high,chunk.chunk_id.low),
        "state":format!("{:?}",chunk.state),
        "logical_start":chunk.logical_start.to_string(),"logical_end":chunk.logical_end.to_string(),
        "mapping_count":chunk.mapping_count.to_string(),"mapping_bytes":chunk.mapping_bytes.to_string(),
    });
    json!({
        "generation":value.generation.to_string(), "writer_epoch":value.writer_epoch.to_string(),
        "metadata_group_id":value.metadata_group_id.to_string(), "trim_offset":value.trim_offset.to_string(),
        "published_tail":value.published_tail.to_string(), "closed":value.closed,
        "chunk_record":chunk, "offset":value.offset, "next_offset":value.next_offset,
    })
}
