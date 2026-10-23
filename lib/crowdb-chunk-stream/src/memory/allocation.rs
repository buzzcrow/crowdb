// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{ActiveChunkDescriptor, Chunk, ChunkId, MemoryStreamStore, Ordering, Result};

impl MemoryStreamStore {
    pub(super) async fn allocate_active_chunk(
        &self,
        writer_epoch: u64,
        purpose: crowdb_protocol::chunk_stream::StreamPurpose,
    ) -> Result<ActiveChunkDescriptor> {
        let low = self.next_chunk.fetch_add(1, Ordering::AcqRel);
        let chunk_id = ChunkId {
            high: (purpose.chunk_type() as u64) << 56,
            low,
        };
        self.state.lock().await.chunks.insert(
            chunk_id,
            Chunk {
                bytes: Vec::new(),
                cursor: 0,
                synchronized_cursor: 0,
                capacity: self.chunk_capacity,
                sealed: false,
                released: false,
                writer_epoch,
                last_advance_checksum: None,
            },
        );
        Ok(ActiveChunkDescriptor {
            chunk_id,
            physical_start: 0,
            logical_start: 0,
            acknowledged_cursor: 0,
            capacity: self.chunk_capacity,
        })
    }
}
