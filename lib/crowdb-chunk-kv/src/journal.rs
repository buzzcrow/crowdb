// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_stream::{ChunkStream, StreamError, StreamName};

use crate::{ChunkKvError, JournalPosition, Result};

#[async_trait]
pub trait PartitionJournal: Send + Sync {
    async fn append_frames(&self, frames: &[Bytes]) -> Result<Vec<JournalPosition>>;
    async fn read_window(&self, offset: u64, max_bytes: usize) -> Result<Bytes>;
    async fn trim_prefix(&self, offset: u64) -> Result<u64>;
    async fn close(&self) -> Result<()>;
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

    async fn read_window(&self, offset: u64, max_bytes: usize) -> Result<Bytes> {
        let available =
            self.stream.tail().checked_sub(offset).ok_or_else(|| {
                ChunkKvError::InvalidRequest("journal read begins beyond durable tail".into())
            })?;
        let length = usize::try_from(available.min(max_bytes as u64)).map_err(|_| {
            ChunkKvError::InvalidRequest("journal read window exceeds addressable range".into())
        })?;
        self.stream
            .read_at(offset, length)
            .await
            .map_err(map_stream_error)
    }

    async fn trim_prefix(&self, offset: u64) -> Result<u64> {
        self.stream.trim_prefix(offset).await.map_err(map_stream_error)
    }

    async fn close(&self) -> Result<()> {
        self.stream.close().await.map_err(map_stream_error)
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
