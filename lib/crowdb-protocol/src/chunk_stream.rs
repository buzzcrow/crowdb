// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable metadata shared by chunk-stream registry and storage adapters.

use crate::common::ChunkId;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Stable opaque identity of one logical stream.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StreamName {
    pub high: u64,
    pub low: u64,
}

impl StreamName {
    /// Generates a process-unique, time-ordered stream identifier.
    #[must_use]
    pub fn generate() -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(1);
        let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        Self {
            high: u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
            low: (u64::from(duration.subsec_nanos()) << 32)
                | (SEQUENCE.fetch_add(1, Ordering::Relaxed) & u64::from(u32::MAX)),
        }
    }
}

impl std::fmt::Display for StreamName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}{:016x}", self.high, self.low)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamBindingState {
    #[default]
    Creating,
    Active,
    Closed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamBinding {
    pub stream_name: StreamName,
    pub metadata_group_id: u64,
    pub binding_generation: u64,
    pub state: StreamBindingState,
    pub owner_kind: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveChunkDescriptor {
    pub chunk_id: ChunkId,
    pub physical_start: u64,
    pub logical_start: u64,
    pub acknowledged_cursor: u64,
    pub capacity: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamExtentPageFence {
    pub page_index: u64,
    pub first_logical: u64,
    pub end_logical: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamManifest {
    pub stream_name: StreamName,
    pub metadata_group_id: u64,
    pub writer_epoch: u64,
    pub generation: u64,
    pub trim_offset: u64,
    pub sealed_tail: u64,
    pub active: Option<ActiveChunkDescriptor>,
    pub extent_pages: Vec<StreamExtentPageFence>,
    pub previous_generation: Option<u64>,
    pub closed: bool,
}

/// Parallel logical-to-physical extent arrays. Entry `i` maps
/// `[logical_offsets[i], logical_offsets[i + 1])` into `chunk_ids[i]`
/// starting at `physical_offsets[i]`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamExtentPage {
    pub stream_name: StreamName,
    pub writer_epoch: u64,
    pub generation: u64,
    pub page_index: u64,
    pub chunk_ids: Vec<ChunkId>,
    pub logical_offsets: Vec<u64>,
    pub physical_offsets: Vec<u64>,
}
