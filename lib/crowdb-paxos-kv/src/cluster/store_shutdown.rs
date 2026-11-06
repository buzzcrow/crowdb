// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::PxKvStore;
use crate::common::report::OperationReport;
use std::time::Duration;

impl PxKvStore {
    /// Whether this store hosts an engine whose accepted apply must be drained
    /// before stopping its consensus transport or deleting its durable files.
    #[must_use]
    pub fn has_runtime_engine(&self) -> bool {
        self.groups_snapshot()
            .values()
            .any(|group| group.local_replica().learner.engine().requires_contiguous_apply())
    }

    /// Close every hosted runtime proposal window without stopping consensus
    /// communication. A host calls this for every store before draining any.
    pub fn close_runtime_proposals(&self) {
        for group in self.groups_snapshot().values() {
            group.close_runtime_proposals();
        }
    }

    /// Drain accepted runtime work while all peer transports remain live.
    /// Failure reports the incomplete drain and retains every repair owner.
    pub async fn prepare_runtime_shutdown(&self, timeout: Duration) -> OperationReport {
        self.drain_runtime_groups(timeout).await
    }

    /// True while accepted shutdown is waiting with repair/communication live.
    #[must_use]
    pub fn runtime_shutdown_pending(&self) -> bool {
        self.groups_snapshot()
            .values()
            .any(|group| group.runtime_shutdown_pending())
    }

    /// Await actual accepted-prefix progress after a reported timeout. This
    /// retains repair owners and suspends the caller without blocking workers.
    /// It does not turn an unavailable peer into a successful shutdown.
    pub async fn await_runtime_shutdown_ready(&self) {
        for group in self.groups_snapshot().values() {
            if group.local_replica().learner.engine().requires_contiguous_apply() {
                group.drain_runtime_chosen().await;
            }
        }
    }

    pub(super) async fn drain_runtime_groups(&self, timeout: Duration) -> OperationReport {
        let groups = self.groups_snapshot();
        for group in groups.values() {
            group.close_runtime_proposals();
        }
        let mut report = OperationReport::new();
        for group in groups.values() {
            if let Err(error) = group.drain_runtime_before_shutdown(timeout).await {
                report.push_error(error);
            }
        }
        // No teardown has started: a timeout in any group preserves every
        // group's communication and repair owners, including legacy peers.
        report
    }
}
