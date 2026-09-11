// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fenced, mirrored logical byte streams assembled from finite chunks.

mod error;
mod kv;
mod metadata;
mod metrics;
mod storage;
mod stream;

#[cfg(feature = "test-util")]
pub mod memory;

pub use error::{Result, StreamError};
pub use kv::{KvStreamMetadataStore, KvStreamRegistry};
pub use metadata::{resolve_extent, validate_manifest, ExtentLocation};
pub use metrics::{StreamMetrics, StreamMetricsSnapshot};
pub use storage::{
    CursorAdvance, DurableCursor, StreamChunkStore, StreamMetadataStore, StreamRegistry, TrimmedChunk,
};
pub use stream::{AppendRange, ChunkStream, ReadHint, ReadSegment, StreamConfig, StreamReader};

pub use crowdb_protocol::chunk_stream::{
    ActiveChunkDescriptor, StreamBinding, StreamBindingState, StreamExtentPage, StreamExtentPageFence,
    StreamManifest, StreamName,
};
pub use crowdb_protocol::common::ChunkId;
