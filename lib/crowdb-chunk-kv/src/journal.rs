// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_stream::{ChunkStream, StreamError, StreamName};

use crate::{ChunkKvError, JournalPosition, Result};

#[async_trait]
pub trait PartitionJournal: Send + Sync {
    async fn append_frames(&self, frames: &[Bytes]) -> Result<Vec<JournalPosition>>;
    fn stream_name(&self) -> StreamName;
    fn tail(&self) -> u64;
}

pub struct StreamPartitionJournal {
    stream: ChunkStream,
    stream_name: StreamName,
}

impl StreamPartitionJournal {
    #[must_use]
    pub fn new(stream: ChunkStream, stream_name: StreamName) -> Self {
        Self { stream, stream_name }
    }
}

#[async_trait]
impl PartitionJournal for StreamPartitionJournal {
    async fn append_frames(&self, frames: &[Bytes]) -> Result<Vec<JournalPosition>> {
        if frames.is_empty() {
            return Ok(Vec::new());
        }
        let range = self.stream.append(frames).await.map_err(map_stream_error)?;
        if range.stream_name != self.stream_name {
            return Err(ChunkKvError::Internal("stream returned another identity".into()));
        }
        let mut offset = range.begin;
        let mut positions = Vec::with_capacity(frames.len());
        for frame in frames {
            positions.push(JournalPosition {
                stream_name: self.stream_name,
                offset,
            });
            offset = offset
                .checked_add(frame.len() as u64)
                .ok_or_else(|| ChunkKvError::Internal("journal position overflows".into()))?;
        }
        if offset != range.end {
            return Err(ChunkKvError::Internal(
                "stream append range length mismatch".into(),
            ));
        }
        Ok(positions)
    }

    fn stream_name(&self) -> StreamName {
        self.stream_name
    }

    fn tail(&self) -> u64 {
        self.stream.tail()
    }
}

fn map_stream_error(error: StreamError) -> ChunkKvError {
    match error {
        StreamError::InvalidRequest(message) => ChunkKvError::InvalidRequest(message),
        StreamError::Backpressure => ChunkKvError::Overloaded,
        StreamError::StaleWriter => ChunkKvError::StaleEpoch,
        StreamError::DefinitelyNotCommitted(_)
        | StreamError::ResolvingAmbiguousAppend
        | StreamError::WriteStalled => ChunkKvError::WriteStalled,
        StreamError::ReadUnavailable(message) => ChunkKvError::TreeUnavailable(message),
        StreamError::Corruption(message) => ChunkKvError::JournalCorruption(message),
        StreamError::Internal(message) => ChunkKvError::Internal(message),
    }
}
