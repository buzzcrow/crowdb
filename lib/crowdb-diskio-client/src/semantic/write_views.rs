// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    Bytes, DiskioClient, DiskioError, DiskioResult, Durability, OperationOptions, SegmentTarget, WritePayload,
};
use futures::{stream::FuturesUnordered, StreamExt};

impl DiskioClient {
    /// Write caller-owned immutable views as one exact segment-relative range.
    ///
    /// # Errors
    ///
    /// Returns a typed input, topology, backpressure, disk, durability, or
    /// ambiguous-outcome error. Larger view lists use consecutive bounded RPC
    /// frames without copying payload; requested fsync follows the whole range.
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
        let mut pending = FuturesUnordered::new();
        let mut failure = None;
        let depth = self.config.max_pending_calls.min(4);
        loop {
            while failure.is_none() && pending.len() < depth {
                let batch: Vec<_> = views
                    .by_ref()
                    .take(crowdb_rpc_ffi::BufferChain::maximum_views())
                    .collect();
                if batch.is_empty() {
                    break;
                }
                let length: usize = batch.iter().map(Bytes::len).sum();
                pending.push(self.write_payload(
                    target,
                    position,
                    WritePayload::Views(batch.into()),
                    Durability::Buffered,
                    options,
                ));
                position += length as u64;
            }
            let Some(result) = pending.next().await else {
                break;
            };
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        if durability == Durability::Fsync {
            self.fsync(target.disk_id, options).await?;
        }
        Ok(())
    }
}
