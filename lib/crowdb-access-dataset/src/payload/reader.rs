// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{ChunkReadError, ChunkReader, DatasetUploads, ReadCancellation, UploadError};
use async_trait::async_trait;
use bytes::BytesMut;
use std::ops::Range;

/// Compatibility reader for bounded native/legacy reads. Large fields use streams.
pub struct UploadedChunkReader {
    uploads: DatasetUploads,
}
impl UploadedChunkReader {
    #[must_use]
    pub fn new(uploads: DatasetUploads) -> Self {
        Self { uploads }
    }
}
#[async_trait]
impl ChunkReader for UploadedChunkReader {
    async fn read(
        &self,
        location: &[u8],
        range: Option<Range<u64>>,
        cancel: &ReadCancellation,
    ) -> Result<Vec<u8>, ChunkReadError> {
        let record = self.uploads.metadata(location).await.map_err(|e| map_error(&e))?;
        let range = range.unwrap_or(0..record.length);
        if range.start > range.end || range.end > record.length || range.end - range.start > 8 * 1024 * 1024 {
            return Err(ChunkReadError::Failed("bounded field range required".into()));
        }
        let mut output = BytesMut::new();
        let first = range.start / u64::from(record.part_size);
        let end = range.end.div_ceil(u64::from(record.part_size));
        for index in first..end {
            if cancel.is_cancelled() {
                return Err(ChunkReadError::Cancelled);
            }
            let bytes = self
                .uploads
                .read_part(
                    location,
                    u32::try_from(index).map_err(|_| ChunkReadError::Truncated)?,
                )
                .await
                .map_err(|e| map_error(&e))?;
            let offset = index * u64::from(record.part_size);
            let start =
                usize::try_from(range.start.saturating_sub(offset)).map_err(|_| ChunkReadError::Truncated)?;
            let finish = usize::try_from((range.end - offset).min(bytes.len() as u64))
                .map_err(|_| ChunkReadError::Truncated)?;
            output.extend_from_slice(bytes.get(start..finish).ok_or(ChunkReadError::Truncated)?);
        }
        if cancel.is_cancelled() {
            return Err(ChunkReadError::Cancelled);
        }
        Ok(output.to_vec())
    }
}
fn map_error(error: &UploadError) -> ChunkReadError {
    match error {
        UploadError::NotFound => ChunkReadError::NotFound,
        UploadError::Integrity => ChunkReadError::Truncated,
        _ => ChunkReadError::Transient("Dataset payload unavailable".into()),
    }
}
