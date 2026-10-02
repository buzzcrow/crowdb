// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    Bytes, DiskioClient, DiskioError, DiskioResult, Durability, OperationOptions, SegmentTarget, WritePayload,
};

impl DiskioClient {
    /// Write caller-owned immutable views as one exact segment-relative range.
    ///
    /// # Errors
    ///
    /// Returns a typed input, topology, backpressure, disk, durability, or
    /// ambiguous-outcome error. Larger view lists use consecutive bounded RPC
    /// frames without copying payload. Frames complete in byte order; requested
    /// fsync follows the whole range.
    pub async fn write_views(
        &self,
        target: SegmentTarget,
        offset: u64,
        data: Vec<Bytes>,
        durability: Durability,
        options: OperationOptions,
    ) -> DiskioResult<()> {
        let total = data.iter().try_fold(0usize, |total, view| {
            if view.is_empty() {
                return Err(DiskioError::InvalidInput("write views must be nonempty".into()));
            }
            total
                .checked_add(view.len())
                .ok_or_else(|| DiskioError::InvalidInput("write view length overflow".into()))
        })?;
        if total == 0 {
            return Err(DiskioError::InvalidInput("write data must be nonempty".into()));
        }
        target.checked_range(offset, total)?;
        if data.len() <= crowdb_rpc_ffi::BufferChain::maximum_views() {
            return self
                .write_payload(
                    target,
                    offset,
                    WritePayload::Views(data.into()),
                    durability,
                    options,
                )
                .await;
        }
        let mut position = offset;
        let mut views = data.into_iter();
        // All frames belong to one strip's disk block. Wait for each reply
        // before submitting the next frame, even at aligned boundaries: RPC
        // arrival order across connections is not guaranteed, and server-side
        // partial-block padding can overwrite a later frame's stored bytes.
        // Callers may overlap independent strip blocks and different disks
        // holding the same strip's shards or mirror replicas.
        loop {
            let batch: Vec<_> = views
                .by_ref()
                .take(crowdb_rpc_ffi::BufferChain::maximum_views())
                .collect();
            if batch.is_empty() {
                break;
            }
            let length: usize = batch.iter().map(Bytes::len).sum();
            self.write_payload(
                target,
                position,
                WritePayload::Views(batch.into()),
                Durability::Buffered,
                options,
            )
            .await?;
            position += length as u64;
        }
        if durability == Durability::Fsync {
            self.fsync(target.disk_id, options).await?;
        }
        Ok(())
    }
}
