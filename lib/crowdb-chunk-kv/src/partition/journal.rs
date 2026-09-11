// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_stream::{ChunkId, ChunkStream, StreamError, StreamName};

use crate::{ChunkKvError, JournalPosition, Result};

#[async_trait]
pub trait PartitionJournal: Send + Sync {
    async fn append_frames(&self, frames: &[Bytes]) -> Result<Vec<JournalPosition>>;
    async fn read_window(&self, offset: u64, max_bytes: usize) -> Result<Bytes>;
    async fn validate_frame_source(&self, _offset: u64, _length: usize, _chunk_id: ChunkId) -> Result<()> {
        Ok(())
    }
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
        let ranges = self
            .stream
            .append_chunk_bound_batch(frames)
            .await
            .map_err(map_stream_error)?;
        if ranges.len() != frames.len() {
            return Err(ChunkKvError::Internal(
                "stream returned wrong chunk-bound range count".into(),
            ));
        }
        let mut positions = Vec::with_capacity(ranges.len());
        let mut previous_end = None;
        for (frame, range) in frames.iter().zip(ranges) {
            if range.stream_name != self.stream_name || range.chunk_id.is_none() {
                return Err(ChunkKvError::Internal(
                    "stream returned invalid chunk-bound range".into(),
                ));
            }
            let expected_length = u64::try_from(frame.len())
                .ok()
                .and_then(|length| length.checked_add(16));
            if range.end.checked_sub(range.begin) != expected_length
                || previous_end.is_some_and(|end| end != range.begin)
            {
                return Err(ChunkKvError::Internal(
                    "stream returned discontinuous chunk-bound ranges".into(),
                ));
            }
            positions.push(JournalPosition {
                stream_name: self.stream_name,
                offset: range.begin,
            });
            previous_end = Some(range.end);
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

    async fn validate_frame_source(&self, offset: u64, length: usize, chunk_id: ChunkId) -> Result<()> {
        let segments = self
            .stream
            .read_at_with_provenance(offset, length)
            .await
            .map_err(map_stream_error)?;
        if segments.len() != 1 || segments[0].chunk_id != chunk_id || segments[0].data.len() != length {
            return Err(ChunkKvError::JournalCorruption(
                "WAL frame chunk identity does not match read provenance".into(),
            ));
        }
        Ok(())
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
