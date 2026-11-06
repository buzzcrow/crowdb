// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::AsyncCrowdbtree;
use crate::error::{check, CtError};
use crate::maintenance::{CheckpointReport, FlushReport};
use crate::reactor::{drive_completion, FutureGuard};
use crate::sys;

impl AsyncCrowdbtree {
    /// Await both flush phases. Admission errors are outer `Err`; accepted
    /// operation errors remain in the returned report alongside progress.
    pub async fn flush_report(&self) -> Result<FlushReport, CtError> {
        let future = {
            let mut raw = std::ptr::null_mut();
            check(unsafe { sys::ct_flush_report_async(self.inner.as_ptr(), &mut raw) })?;
            FutureGuard(raw)
        };
        drive_completion(future, &self.inner, poll_flush).await
    }

    /// Materialize reloadable local pages without committing a durable anchor.
    pub async fn writeback_report(&self) -> Result<CheckpointReport, CtError> {
        self.checkpoint_with(sys::ct_writeback_async).await
    }

    /// Await a durable checkpoint and retain its progress even on failure.
    pub async fn checkpoint_report(&self) -> Result<CheckpointReport, CtError> {
        self.checkpoint_with(sys::ct_checkpoint_report_async).await
    }

    async fn checkpoint_with(
        &self,
        start: unsafe extern "C" fn(*mut sys::ct_tree, *mut *mut sys::ct_future) -> i32,
    ) -> Result<CheckpointReport, CtError> {
        let future = {
            let mut raw = std::ptr::null_mut();
            check(unsafe { start(self.inner.as_ptr(), &mut raw) })?;
            FutureGuard(raw)
        };
        drive_completion(future, &self.inner, poll_checkpoint).await
    }
}

fn poll_flush(guard: &mut FutureGuard) -> Option<Result<FlushReport, CtError>> {
    let mut raw = sys::ct_flush_result_v1 {
        struct_size: std::mem::size_of::<sys::ct_flush_result_v1>() as u32,
        version: 1,
        ..Default::default()
    };
    let mut done = 0;
    if let Err(error) = check(unsafe { sys::ct_future_poll_flush_report(guard.0, &mut done, &mut raw) }) {
        return Some(Err(error));
    }
    if done == 0 {
        return None;
    }
    guard.0 = std::ptr::null_mut();
    Some(Ok(raw.into()))
}

fn poll_checkpoint(guard: &mut FutureGuard) -> Option<Result<CheckpointReport, CtError>> {
    let mut raw = sys::ct_checkpoint_result_v1 {
        struct_size: std::mem::size_of::<sys::ct_checkpoint_result_v1>() as u32,
        version: 1,
        ..Default::default()
    };
    let mut done = 0;
    if let Err(error) = check(unsafe { sys::ct_future_poll_checkpoint_report(guard.0, &mut done, &mut raw) })
    {
        return Some(Err(error));
    }
    if done == 0 {
        return None;
    }
    guard.0 = std::ptr::null_mut();
    Some(Ok(raw.into()))
}
