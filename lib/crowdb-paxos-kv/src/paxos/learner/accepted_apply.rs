// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::PxLearner;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Default)]
pub(super) struct ApplyTasks {
    active: AtomicUsize,
    changed: Notify,
}
pub(crate) struct ApplyTask {
    tasks: Arc<ApplyTasks>,
}
impl Drop for ApplyTask {
    fn drop(&mut self) {
        if self.tasks.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.tasks.changed.notify_waiters();
        }
    }
}
impl PxLearner {
    // Register detached work synchronously, before chosen frontier publication.
    pub(crate) fn track_apply_task(&self) -> ApplyTask {
        self.apply_tasks.active.fetch_add(1, Ordering::AcqRel);
        ApplyTask {
            tasks: self.apply_tasks.clone(),
        }
    }
    pub(crate) fn applying_tasks(&self) -> usize {
        self.apply_tasks.active.load(Ordering::Acquire)
    }
    pub(crate) async fn drain_apply_tasks(&self) {
        loop {
            let changed = self.apply_tasks.changed.notified();
            if self.applying_tasks() == 0 {
                return;
            }
            changed.await;
        }
    }
}
