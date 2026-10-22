// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    Arc, ChunkAllocator, DiskWriter, IoError, MirrorChunkWriter, Result, StreamName, StreamPurpose,
    StripType, STREAM_STRIP_BYTES,
};
use crowdb_protocol::chunkdb::rpc::AllocateChunkRequest;

impl MirrorChunkWriter {
    /// Allocates one stream chunk with the default mirror policy.
    ///
    /// # Errors
    ///
    /// Returns an allocation or layout error when chunkdb cannot supply an
    /// active mirror strip.
    pub async fn allocate(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        stream_name: StreamName,
        writer_epoch: u64,
        writer_lease_ms: u64,
        purpose: StreamPurpose,
    ) -> Result<Self> {
        Self::allocate_with_copy_count(
            allocator,
            disk_writer,
            stream_name,
            writer_epoch,
            writer_lease_ms,
            2,
            purpose,
        )
        .await
    }

    /// Allocates one stream chunk with an explicit mirror count.
    ///
    /// # Errors
    ///
    /// Returns an allocation or layout error for a zero count or unavailable placement.
    pub async fn allocate_with_copy_count(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        stream_name: StreamName,
        writer_epoch: u64,
        writer_lease_ms: u64,
        copy_count: u32,
        purpose: StreamPurpose,
    ) -> Result<Self> {
        if writer_epoch == 0 || writer_lease_ms == 0 || !(1..=5).contains(&copy_count) {
            return Err(IoError::AllocationFailed(
                "stream mirror writer requires a nonzero epoch and lease".into(),
            ));
        }
        let response = allocator
            .allocate_chunk(AllocateChunkRequest {
                chunk_id: None,
                write_granularity: u32::try_from(STREAM_STRIP_BYTES / 1024).unwrap_or(u32::MAX),
                strip_count: 1,
                strip_type: StripType::Mirror as i32,
                data_num: 0,
                code_num: 0,
                copy_count,
                chunk_type: purpose.chunk_type() as i32,
                writer_epoch,
                writer_lease_ms,
                owner_key: stream_name.chunk_owner_key(),
            })
            .await?;
        let chunk = response
            .chunk
            .ok_or_else(|| IoError::AllocationFailed("stream chunk allocation returned no chunk".into()))?;
        Self::open_with_copy_count(
            allocator,
            disk_writer,
            chunk,
            stream_name,
            writer_epoch,
            writer_lease_ms,
            copy_count,
            purpose,
        )
    }
}
