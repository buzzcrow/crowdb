// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::AsyncCrowdbtree;
use crate::{
    error::check,
    reactor::{drive_completion, FutureGuard},
    sys, CtError,
};

/// Progress for one bounded catalog and physical-chunk maintenance pass.
/// Root and WAL retention watermarks are unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkCollectionReport {
    pub status: Result<(), CtError>,
    pub more: bool,
    pub candidates_examined: u64,
    pub reference_windows: u64,
    pub stages_examined: u64,
    pub pins_examined: u64,
    pub pins_released: u64,
    pub stages_removed: u64,
    /// Requested logical pack capacity removed after acknowledged physical deletion.
    /// Excludes physical strip rounding and mirror replication.
    pub logical_capacity_bytes: u64,
}

impl AsyncCrowdbtree {
    /// Continue one finite sweep on this Tree's paid backend lane. Busy or
    /// rejected admission is outer `Err`; accepted progress includes its status.
    /// # Errors
    /// Returns admission, unsupported-backend, or malformed native result errors.
    pub async fn collect_chunk_storage(&self) -> Result<ChunkCollectionReport, CtError> {
        let future = {
            let mut raw = std::ptr::null_mut();
            check(unsafe { sys::ct_chunk_collect_async(self.inner.as_ptr(), &mut raw) })?;
            FutureGuard(raw)
        };
        drive_completion(future, &self.inner, poll_collection).await
    }
}

fn poll_collection(guard: &mut FutureGuard) -> Option<Result<ChunkCollectionReport, CtError>> {
    let mut raw = sys::ct_chunk_collection_result_v1 {
        struct_size: std::mem::size_of::<sys::ct_chunk_collection_result_v1>() as u32,
        version: 1,
        ..Default::default()
    };
    let mut done = 0;
    if let Err(error) = check(unsafe { sys::ct_future_poll_chunk_collection(guard.0, &mut done, &mut raw) }) {
        return Some(Err(error));
    }
    if done == 0 {
        return None;
    }
    guard.0 = std::ptr::null_mut();
    Some(Ok(ChunkCollectionReport {
        status: check(raw.terminal_status),
        more: raw.more != 0,
        candidates_examined: raw.candidates_examined,
        reference_windows: raw.reference_windows,
        stages_examined: raw.stages_examined,
        pins_examined: raw.pins_examined,
        pins_released: raw.pins_released,
        stages_removed: raw.stages_removed,
        logical_capacity_bytes: raw.logical_capacity_bytes,
    }))
}
