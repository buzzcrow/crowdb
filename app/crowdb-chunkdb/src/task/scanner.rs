// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Event-driven task index scan, expired-claim recovery, and dispatch.

use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use tracing::{error, info, warn};

use super::{TaskExecutor, TaskManager, TaskStore, TaskStoreError};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TaskScanSummary {
    pub expired_claims_requeued: u64,
    pub ready_indexes_seen: u64,
    pub tasks_claimed: u64,
    pub tasks_completed_or_requeued: u64,
    pub dispatch_errors: u64,
}

pub struct TaskScanner {
    store: Arc<TaskStore>,
    manager: Arc<TaskManager>,
    executor: Arc<TaskExecutor>,
    scan_limit: u32,
    safety_interval: Duration,
}

impl TaskScanner {
    #[must_use]
    pub fn new(
        store: Arc<TaskStore>,
        manager: Arc<TaskManager>,
        executor: Arc<TaskExecutor>,
        scan_limit: u32,
        safety_interval: Duration,
    ) -> Self {
        Self {
            store,
            manager,
            executor,
            scan_limit: scan_limit.max(1),
            safety_interval: safety_interval.max(Duration::from_millis(1)),
        }
    }

    /// Recover expired claims, claim eligible tasks, and await their bounded
    /// dispatch.
    ///
    /// # Errors
    /// Returns an index-scan error. Individual stale claims and handler errors
    /// are isolated and reported in the summary.
    pub async fn run_once(&self, now_ms: u64) -> Result<TaskScanSummary, TaskStoreError> {
        let mut summary = TaskScanSummary::default();
        for expired in self.store.scan_expired_leases(now_ms, self.scan_limit).await? {
            match self.manager.recover_expired(&expired, now_ms).await {
                Ok(true) => summary.expired_claims_requeued += 1,
                Ok(false) => {}
                Err(error) => warn!(%error, "failed to recover expired task claim"),
            }
        }

        let ready = self.store.scan_ready(now_ms, self.scan_limit).await?;
        summary.ready_indexes_seen = u64::try_from(ready.len()).unwrap_or(u64::MAX);
        let capacity = self.executor.available_capacity();
        let mut claims = Vec::with_capacity(ready.len().min(capacity));
        for index in ready {
            if claims.len() == capacity {
                break;
            }
            match self.manager.claim(&index, now_ms).await {
                Ok(Some(claim)) => claims.push(claim),
                Ok(None) => {}
                Err(error) => warn!(%error, "failed to claim ready task"),
            }
        }
        summary.tasks_claimed = u64::try_from(claims.len()).unwrap_or(u64::MAX);
        let futures = claims.into_iter().map(|claim| self.executor.execute(claim));
        for result in join_all(futures).await {
            match result {
                Ok(()) => summary.tasks_completed_or_requeued += 1,
                Err(error) => {
                    summary.dispatch_errors += 1;
                    error!(%error, "task dispatch outcome could not be persisted");
                }
            }
        }
        Ok(summary)
    }

    /// Run until shutdown, waking on admission or the periodic safety scan.
    pub async fn run(&self, mut stop: tokio::sync::watch::Receiver<bool>) {
        let wake = self.manager.wake_handle();
        loop {
            let stopped = tokio::select! {
                biased;
                changed = stop.changed() => changed.is_err() || *stop.borrow(),
                () = wake.notified() => false,
                () = tokio::time::sleep(self.safety_interval) => false,
            };
            if stopped {
                info!("chunk task scanner stopped");
                return;
            }
            if let Err(error) = self.run_once(unix_time_ms()).await {
                error!(%error, "chunk task scan failed");
            }
        }
    }
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
