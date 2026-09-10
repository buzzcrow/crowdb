// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fenced, mirrored logical byte streams assembled from finite chunks.

mod error;
mod metadata;
mod storage;

pub use error::{Result, StreamError};
pub use metadata::{resolve_extent, validate_manifest, ExtentLocation};
pub use storage::{CursorAdvance, StreamChunkStore, StreamMetadataStore, StreamRegistry, TrimmedChunk};

pub use crowdb_protocol::chunk_stream::{
    ActiveChunkDescriptor, StreamBinding, StreamBindingState, StreamExtentPage, StreamExtentPageFence,
    StreamManifest, StreamName,
};
