// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    io_error, ActiveChunkDescriptor, Arc, MirrorChunkWriter, Ordering, ProductionStreamChunkStore, Result,
    StreamName,
};

impl ProductionStreamChunkStore {
    pub(super) async fn allocate_active_chunk(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
        purpose: crowdb_protocol::chunk_stream::StreamPurpose,
    ) -> Result<ActiveChunkDescriptor> {
        let writer = MirrorChunkWriter::allocate_with_copy_count(
            Arc::clone(&self.allocator),
            Arc::clone(&self.disk_writer),
            stream_name,
            writer_epoch,
            self.writer_lease_ms,
            self.mirror_copies,
            purpose,
        )
        .await
        .map_err(io_error)?;
        let chunk_id = writer.chunk_id();
        let capacity = writer.capacity();
        let cursor = writer.cursor();
        self.install(writer.chunk().clone())?
            .fresh_grouped_allocation
            .store(true, Ordering::Release);
        Ok(ActiveChunkDescriptor {
            chunk_id,
            physical_start: 0,
            logical_start: 0,
            acknowledged_cursor: cursor,
            capacity,
        })
    }
}
