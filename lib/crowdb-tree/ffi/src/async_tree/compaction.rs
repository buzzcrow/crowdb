// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::AsyncCrowdbtree;
use crate::error::{check, CtError};
use crate::reactor::{drive_completion, FutureGuard};
use crate::sys;

/// Bounded source-block selection for one explicit compaction pass.
#[derive(Debug, Clone, Copy)]
pub struct CompactionOptions {
    /// Select only blocks whose free fraction strictly exceeds this value.
    pub free_threshold: f64,
    /// Zero disables relocation. The first eligible block may exceed a
    /// nonzero budget; each pass selects at most one block.
    pub max_relocation_bytes: u64,
}
impl Default for CompactionOptions {
    fn default() -> Self {
        Self {
            free_threshold: 0.70,
            max_relocation_bytes: 1,
        }
    }
}
/// Progress survives a terminal failure after partial block relocation.
#[derive(Debug, Clone)]
pub struct CompactionReport {
    pub status: Result<(), CtError>,
    pub blocks_selected: u64,
    pub pages_relocated: u64,
    pub bytes_relocated: u64,
    pub blocks_deleted: u64,
}
impl AsyncCrowdbtree {
    /// Await admitted native CPU/backend continuations without blocking the host executor.
    /// # Errors
    /// Returns an admission or transport error before a progress report is available.
    pub async fn compact_sparse_blocks(&self) -> Result<CompactionReport, CtError> {
        self.compact_sparse_blocks_with(CompactionOptions::default())
            .await
    }
    /// Await one explicitly configured compaction pass.
    /// # Errors
    /// Returns an admission or transport error before a progress report is available.
    pub async fn compact_sparse_blocks_with(
        &self,
        options: CompactionOptions,
    ) -> Result<CompactionReport, CtError> {
        let future = {
            let options = sys::ct_compaction_options_v1 {
                struct_size: std::mem::size_of::<sys::ct_compaction_options_v1>() as u32,
                version: 1,
                free_threshold: options.free_threshold,
                max_relocation_bytes: options.max_relocation_bytes,
            };
            let mut raw = std::ptr::null_mut();
            check(unsafe { sys::ct_compact_sparse_blocks_async(self.inner.as_ptr(), &options, &mut raw) })?;
            FutureGuard(raw)
        };
        drive_completion(future, &self.inner, poll).await
    }
}
fn poll(guard: &mut FutureGuard) -> Option<Result<CompactionReport, CtError>> {
    let mut raw = sys::ct_compaction_result_v1 {
        struct_size: std::mem::size_of::<sys::ct_compaction_result_v1>() as u32,
        version: 1,
        ..Default::default()
    };
    let mut done = 0;
    if let Err(error) = check(unsafe { sys::ct_future_poll_compaction(guard.0, &mut done, &mut raw) }) {
        return Some(Err(error));
    }
    if done == 0 {
        return None;
    }
    guard.0 = std::ptr::null_mut();
    Some(Ok(CompactionReport {
        status: check(raw.terminal_status),
        blocks_selected: raw.blocks_selected,
        pages_relocated: raw.pages_relocated,
        bytes_relocated: raw.bytes_relocated,
        blocks_deleted: raw.blocks_deleted,
    }))
}
