// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable writes for one persisted mirror strip.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crowdb_protocol::chunkdb::rpc::{Chunk, Strip};
use crowdb_protocol::diskdb::rpc::Segment;
use tokio::task::JoinSet;

use crate::chunk::segment_writer::FailedSegmentWrite;
use crate::chunk::strip::StripResult;
use crate::disk_io::DiskWriter;
use crate::io::FeedStatus;
use crate::{IoError, Result};

pub struct MirrorStripWriter {
    chunk: Arc<Chunk>,
    strip_index: u32,
    disk_writer: Arc<dyn DiskWriter>,
    accepted: u64,
    finished: bool,
    history: Vec<Bytes>,
    failed_segments: Vec<(Segment, String)>,
}

impl MirrorStripWriter {
    #[must_use]
    pub fn new(chunk: Arc<Chunk>, strip_index: u32, disk_writer: Arc<dyn DiskWriter>) -> Self {
        Self {
            chunk,
            strip_index,
            disk_writer,
            accepted: 0,
            finished: false,
            history: Vec::new(),
            failed_segments: Vec::new(),
        }
    }

    fn geometry(&self) -> Result<(u64, u64, u32, Vec<Segment>)> {
        let strip = self
            .chunk
            .strips
            .get(self.strip_index as usize)
            .ok_or_else(|| IoError::Internal("mirror strip index is missing".into()))?;
        let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
            return Err(IoError::Internal("expected persisted mirror strip".into()));
        };
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        let capacity = u64::from(strip.capacity) * 1024;
        if mirror.segments.is_empty() || unit_bytes == 0 || capacity == 0 {
            return Err(IoError::Internal("invalid mirror strip geometry".into()));
        }
        Ok((
            unit_bytes,
            capacity,
            strip.strip_sequence,
            mirror.segments.clone(),
        ))
    }

    pub async fn push(&mut self, buffer: Bytes) -> Result<FeedStatus> {
        if self.finished {
            return Err(IoError::Finished);
        }
        let (unit_bytes, capacity, _, segments) = self.geometry()?;
        let length = u64::try_from(buffer.len())
            .map_err(|_| IoError::WriteFailed("mirror write is too large".into()))?;
        if length > capacity.saturating_sub(self.accepted) {
            return Err(IoError::WriteFailed("mirror strip capacity exceeded".into()));
        }
        self.history.push(buffer.clone());
        if segments.len() == 1 {
            let segment = segments[0];
            if !self.failed_segments.iter().any(|(failed, _)| *failed == segment) {
                if let Err(error) = self
                    .disk_writer
                    .write_at_byte_offset(&segment, unit_bytes, self.accepted, buffer)
                    .await
                {
                    self.failed_segments.push((segment, error.to_string()));
                }
            }
        } else {
            let mut writes = JoinSet::new();
            for segment in segments {
                if self.failed_segments.iter().any(|(failed, _)| *failed == segment) {
                    continue;
                }
                let disk_io = Arc::clone(&self.disk_writer);
                let bytes = buffer.clone();
                let offset = self.accepted;
                writes.spawn(async move {
                    (
                        segment,
                        disk_io
                            .write_at_byte_offset(&segment, unit_bytes, offset, bytes)
                            .await,
                    )
                });
            }
            while let Some(result) = writes.join_next().await {
                let (segment, write) = result
                    .map_err(|error| IoError::WriteFailed(format!("mirror replica task failed: {error}")))?;
                if let Err(error) = write {
                    self.failed_segments.push((segment, error.to_string()));
                }
            }
        }
        self.accepted += length;
        Ok(if self.accepted == capacity {
            FeedStatus::Pause
        } else {
            FeedStatus::Continue
        })
    }

    /// Write a complete strip while retaining the failed replica and its data
    /// for ordered replacement before the chunk can be sealed.
    pub(crate) async fn write_full_repairable(
        &mut self,
        buffer: Bytes,
    ) -> Result<(StripResult, Vec<FailedSegmentWrite>)> {
        if self.finished || self.accepted != 0 {
            return Err(IoError::Finished);
        }
        let (_, capacity, _, _) = self.geometry()?;
        if buffer.len() as u64 != capacity {
            return Err(IoError::WriteFailed("full mirror strip length mismatch".into()));
        }
        self.push(buffer).await?;
        let result = self.finish().await?;
        Ok((result, self.take_failures()?))
    }

    pub async fn finish(&mut self) -> Result<StripResult> {
        if self.finished {
            return Err(IoError::Finished);
        }
        let (unit_bytes, _, _, segments) = self.geometry()?;
        self.finished = true;
        let mut syncs = JoinSet::new();
        for segment in segments {
            if self.failed_segments.iter().any(|(failed, _)| *failed == segment) {
                continue;
            }
            let writer = Arc::clone(&self.disk_writer);
            syncs.spawn(async move { (segment, writer.fsync(&segment).await) });
        }
        while let Some(result) = syncs.join_next().await {
            let (segment, sync) =
                result.map_err(|error| IoError::WriteFailed(format!("mirror sync task failed: {error}")))?;
            if let Err(error) = sync {
                self.failed_segments.push((segment, error.to_string()));
            }
        }
        Ok(StripResult {
            chunk_id: self.chunk.id.unwrap_or_default(),
            strip_index_in_chunk: self.strip_index,
            data_blocks_written: u32::try_from(self.accepted.div_ceil(unit_bytes)).unwrap_or(u32::MAX),
            bytes_written: self.accepted,
            partial: self.accepted % unit_bytes != 0,
            ec_encode_time: Duration::ZERO,
            completion_handles: Vec::new(),
        })
    }

    pub(crate) fn take_failures(&mut self) -> Result<Vec<FailedSegmentWrite>> {
        if !self.finished {
            return Err(IoError::Internal("mirror repair requested before finish".into()));
        }
        let (unit_bytes, _, strip_sequence, _) = self.geometry()?;
        let data = std::mem::take(&mut self.history);
        Ok(std::mem::take(&mut self.failed_segments)
            .into_iter()
            .map(|(segment, error)| FailedSegmentWrite {
                strip_sequence,
                segment,
                unit_bytes,
                data: data.clone(),
                error,
            })
            .collect())
    }

    pub fn abort(&mut self) -> Result<StripResult> {
        self.finished = true;
        Ok(StripResult {
            chunk_id: self.chunk.id.unwrap_or_default(),
            strip_index_in_chunk: self.strip_index,
            data_blocks_written: 0,
            bytes_written: self.accepted,
            partial: false,
            ec_encode_time: Duration::ZERO,
            completion_handles: Vec::new(),
        })
    }

    #[must_use]
    pub fn ready(&self) -> bool {
        !self.finished && self.remaining_capacity() > 0
    }

    #[must_use]
    pub fn has_data(&self) -> bool {
        self.accepted > 0
    }

    #[must_use]
    pub fn remaining_capacity(&self) -> u64 {
        self.chunk
            .strips
            .get(self.strip_index as usize)
            .map_or(0, |strip| u64::from(strip.capacity) * 1024)
            .saturating_sub(self.accepted)
    }

    #[must_use]
    pub fn accepted_bytes(&self) -> u64 {
        self.accepted
    }
}
