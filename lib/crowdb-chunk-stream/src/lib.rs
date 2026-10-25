// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fenced, mirrored logical byte streams assembled from finite chunks.

mod authority;
mod error;
mod kv;
mod metadata;
mod metrics;
mod mirror_shadow;
mod perf;
mod production;
mod production_chunk;
mod records;
pub use records::{StreamChunkRecordSnapshot, StreamChunkRecordStore, StreamCollectionResult, StreamReadPin};
mod storage;
mod stream;

#[cfg(feature = "test-util")]
pub mod memory;

pub use authority::{StreamOwnerAuthority, StreamWriteContext, StreamWriterSession};
pub use error::{Result, StreamError};
pub use kv::{KvStreamMetadataStore, KvStreamRegistry};
pub use metadata::ExtentLocation;
pub use metrics::{StreamMetrics, StreamMetricsSnapshot};
pub use perf::{register_stream_perf_metrics, stream_perf_metrics};
pub use production::ProductionStreamRuntime;
pub use production_chunk::ProductionStreamChunkStore;
pub use storage::{
    CursorAdvance, DurableCursor, MirrorStripImage, RecoveredStreamTail, StreamChunkStore,
    StreamMetadataStore, StreamMirrorWrite, StreamRegistry, TrimmedChunk,
};
#[cfg(feature = "test-util")]
pub use stream::capacity_target_for_tests;
pub use stream::{
    AppendRange, ChunkStream, ReadHint, ReadSegment, StreamConfig, StreamMetadataObservation, StreamReader,
    StreamViewSource,
};

pub use crowdb_protocol::chunk_stream::{
    ActiveChunkDescriptor, StreamName, StreamPurpose, StreamRecord, StreamRecordState,
    DEFAULT_STREAM_METADATA_GROUP_ID,
};
pub use crowdb_protocol::common::ChunkId;
