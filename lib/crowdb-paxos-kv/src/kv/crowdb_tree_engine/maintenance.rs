// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::CrowdbTreeEngine;
use crate::kv::{EngineCheckpointReport, EngineCheckpointWork, EngineFlushReport, KVFuture};

// The low bit retains work after errors or abandonment. A later attempt owns
// a new stamp, so polling an older future cannot erase newer pending work.
fn begin(pending: &AtomicU64) -> u64 {
    let previous = pending
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
            Some(value.wrapping_add(2) | 1)
        })
        .expect("unconditional update");
    previous.wrapping_add(2) | 1
}
fn complete(pending: &AtomicU64, attempt: u64, success: bool) {
    if success {
        let _ = pending.compare_exchange(attempt, attempt & !1, Ordering::AcqRel, Ordering::Acquire);
    }
}
fn checkpoint_report(report: crowdb_tree_ffi::CheckpointReport) -> EngineCheckpointReport {
    EngineCheckpointReport {
        status: report.status.map_err(|error| error.to_string()),
        durable_committed: report.durable_committed,
        durable_coverage: report.durable_coverage,
        work: Some(EngineCheckpointWork {
            writeback_completed: report.writeback_completed,
            captured_coverage: report.captured_coverage,
            written_frames: report.written_frames,
            reused_frames: report.reused_frames,
            rebase_deferred: report.rebase_deferred,
        }),
    }
}

impl CrowdbTreeEngine {
    pub(super) fn async_flush(&self) -> KVFuture<Result<EngineFlushReport, String>> {
        let tree = self.inner.clone();
        let pending = Arc::clone(&self.maintenance_pending);
        let attempt = begin(&pending);
        KVFuture::Pending(Box::pin(async move {
            let report = tree.flush_report().await.map_err(|error| error.to_string())?;
            complete(
                &pending,
                attempt,
                report.status.is_ok() && report.deferred_pages == 0,
            );
            Ok(EngineFlushReport {
                handoff_status: report.handoff_status.map_err(|error| error.to_string()),
                maintenance_status: report.maintenance_status.map_err(|error| error.to_string()),
                handoff_completed: report.handoff_completed,
                handoff_advanced: report.handoff_advanced,
                floor: report.floor,
                frontier: report.frontier,
                published_coverage: report.published_coverage,
                deferred_pages: report.deferred_pages,
            })
        }))
    }

    pub(super) fn async_checkpoint(&self, durable: bool) -> KVFuture<Result<EngineCheckpointReport, String>> {
        if durable && !self.has_backend {
            // A volatile engine has no durability barrier. Preserve the trait's
            // zero-coverage result without invoking unsupported native persistence.
            return KVFuture::ready(Ok(EngineCheckpointReport {
                status: Ok(()),
                durable_committed: false,
                durable_coverage: 0,
                work: None,
            }));
        }
        let tree = self.inner.clone();
        let pending = Arc::clone(&self.maintenance_pending);
        let attempt = begin(&pending);
        KVFuture::Pending(Box::pin(async move {
            let report = if durable {
                tree.checkpoint_report().await
            } else {
                tree.writeback_report().await
            }
            .map_err(|error| error.to_string())?;
            complete(
                &pending,
                attempt,
                report.status.is_ok() && report.rebase_deferred == 0,
            );
            Ok(checkpoint_report(report))
        }))
    }
}
