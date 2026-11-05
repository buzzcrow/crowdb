// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::group::{PxGroup, RepairOutcome};
use std::time::Duration;

impl PxGroup {
    pub(super) fn close_runtime_proposals(&self) {
        if self.local_replica.learner.engine().requires_contiguous_apply() {
            self.inflight.close();
        }
    }

    pub(super) async fn drain_runtime_before_shutdown(&self, timeout: Duration) -> Result<(), String> {
        if !self.local_replica.learner.engine().requires_contiguous_apply() {
            return Ok(());
        }
        self.inflight.close();
        let drained = tokio::time::timeout(timeout, self.drain_runtime_chosen()).await;
        if drained.is_err() {
            return Err(format!("critical: group {} accepted Tree work did not drain within {} ms; repair remains running, retry shutdown after restoring progress (applied {}, chosen {})",
                self.group_id, timeout.as_millis(), self.local_replica.contiguous_applied(), self.local_replica.last_chosen_slot()));
        }
        Ok(())
    }

    pub(super) fn runtime_shutdown_pending(&self) -> bool {
        self.local_replica.learner.engine().requires_contiguous_apply()
            && self.inflight.closed()
            && !self.tenure_cancel.is_cancelled()
    }

    pub(super) async fn drain_runtime_chosen(&self) {
        let replica = &self.local_replica;
        loop {
            let target = replica.shutdown_apply_target();
            replica.wake_apply_loop();
            if self.inflight.occupied() == 0
                && replica.contiguous_applied() >= target
                && replica.learner.applying_tasks() == 0
            {
                return;
            }
            // Use the existing Prepare/Accept protocol. It adopts accepted
            // values and can only choose an empty gap after a real quorum.
            if replica.is_leader()
                && replica.contiguous_chosen() < replica.last_chosen_slot()
                && matches!(self.repair_once().await, RepairOutcome::Filled { .. })
            {
                continue;
            }
            tokio::select! {
                () = replica.learner.await_applied(target), if replica.contiguous_applied() < target => {},
                () = self.inflight.drain(), if self.inflight.occupied() != 0 => {},
                () = replica.learner.drain_apply_tasks(), if replica.learner.applying_tasks() != 0 => {},
                () = tokio::time::sleep(Duration::from_millis(10)) => {},
            }
        }
    }
}
