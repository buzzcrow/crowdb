// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::MemoryStreamStore;
use crate::{ActiveChunkDescriptor, RecoveredStreamTail, Result, StreamError};
use crowdb_protocol::frame::{
    FramePrefixScanner, FrameScanConfig, FrameScanMode, FrameScanStop, MAX_FRAME_SCAN_EXTENTS,
    MAX_FRAME_SCAN_INPUT_BYTES,
};

impl MemoryStreamStore {
    /// Stage an interrupted physical suffix without granting synchronization.
    /// # Panics
    /// Panics when the test chunk is absent or its cursor cannot fit in memory.
    pub async fn stage_incomplete_tail_for_tests(&self, id: crate::ChunkId, bytes: bytes::Bytes) {
        let mut state = self.state.lock().await;
        let chunk = state.chunks.get_mut(&id).expect("test chunk exists");
        chunk
            .bytes
            .truncate(usize::try_from(chunk.synchronized_cursor).expect("test cursor fits memory"));
        chunk.bytes.extend_from_slice(&bytes);
    }

    /// # Panics
    /// Panics when the test owner is absent.
    pub async fn fence_writer_for_tests(&self, name: crate::StreamName) {
        self.state
            .lock()
            .await
            .owners
            .get(&name)
            .expect("test owner exists")
            .1
            .fence();
    }

    pub(super) async fn recover_chunk_tail(
        &self,
        epoch: u64,
        active: &ActiveChunkDescriptor,
    ) -> Result<RecoveredStreamTail> {
        let mut state = self.state.lock().await;
        let chunk = state
            .chunks
            .get_mut(&active.chunk_id)
            .ok_or_else(|| StreamError::ReadUnavailable("recovery chunk is missing".into()))?;
        if chunk.writer_epoch > epoch {
            return Err(StreamError::StaleWriter);
        }
        let limit = if chunk.sealed {
            chunk.cursor
        } else {
            chunk.capacity
        };
        let mut scanner = FramePrefixScanner::new(FrameScanConfig {
            chunk_id: active.chunk_id,
            mode: FrameScanMode::Grouped,
            physical_start: active.acknowledged_cursor,
            physical_limit: if chunk.sealed {
                chunk.cursor
            } else {
                chunk.capacity
            },
            required_end: active.acknowledged_cursor.max(chunk.cursor),
            logical_start: active.logical_start,
            max_append_bytes: chunk.capacity,
        })
        .map_err(|error| frame_error(&error))?;
        let mut offset = usize::try_from(active.acknowledged_cursor)
            .map_err(|_| StreamError::Corruption("memory cursor exceeds address space".into()))?;
        let limit = usize::try_from(limit)
            .map_err(|_| StreamError::Corruption("memory capacity exceeds address space".into()))?;
        let mut extents = Vec::new();
        while offset < limit && scanner.stop() == FrameScanStop::Running {
            let end = offset.saturating_add(MAX_FRAME_SCAN_INPUT_BYTES).min(limit);
            let mut window = vec![0; end - offset];
            let available = chunk.bytes.len().saturating_sub(offset).min(window.len());
            if available > 0 {
                window[..available].copy_from_slice(&chunk.bytes[offset..offset + available]);
            }
            let batch = scanner
                .push(offset as u64, &window, MAX_FRAME_SCAN_EXTENTS)
                .map_err(|error| frame_error(&error))?;
            offset += batch.consumed;
            extents.extend(batch.extents);
        }
        let summary = scanner.finish().map_err(|error| frame_error(&error))?;
        chunk.writer_epoch = epoch;
        chunk.synchronized_cursor = summary.physical_end;
        Ok(RecoveredStreamTail {
            physical_end: summary.physical_end,
            logical_end: summary.logical_end,
            extents,
        })
    }
}

fn frame_error(error: &crowdb_protocol::frame::FrameScanError) -> StreamError {
    StreamError::Corruption(error.to_string())
}
