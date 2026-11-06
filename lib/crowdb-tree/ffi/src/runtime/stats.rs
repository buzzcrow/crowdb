// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::TreeResourceLimits;
use crate::sys;

/// Cumulative executor samples. Parallel durations may exceed elapsed time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TreeExecutorSnapshot {
    pub queued: u64,
    pub inflight: u64,
    pub submitted: u64,
    pub completed: u64,
    pub rejected: u64,
    pub queue_ns: u64,
    pub execute_ns: u64,
}

impl From<sys::ct_executor_snapshot_v1> for TreeExecutorSnapshot {
    fn from(value: sys::ct_executor_snapshot_v1) -> Self {
        Self {
            queued: value.queued,
            inflight: value.inflight,
            submitted: value.submitted,
            completed: value.completed,
            rejected: value.rejected,
            queue_ns: value.queue_ns,
            execute_ns: value.execute_ns,
        }
    }
}

/// Approximate host-wide resource and execution samples; sampling never drains counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TreeRuntimeSnapshot {
    pub reserved: TreeResourceLimits,
    pub allocated: TreeResourceLimits,
    pub bindings: u64,
    pub stopped: bool,
    pub cpu: TreeExecutorSnapshot,
    pub backend: TreeExecutorSnapshot,
    pub budget_waiters: u64,
    pub budget_grants: u64,
    pub budget_cancelled: u64,
    pub budget_wait_ns: u64,
}

impl From<sys::ct_runtime_snapshot_v1> for TreeRuntimeSnapshot {
    fn from(value: sys::ct_runtime_snapshot_v1) -> Self {
        Self {
            reserved: value.reserved.into(),
            allocated: value.allocated.into(),
            bindings: value.bindings,
            stopped: value.stopped != 0,
            cpu: value.cpu.into(),
            backend: value.backend.into(),
            budget_waiters: value.budget_waiters,
            budget_grants: value.budget_grants,
            budget_cancelled: value.budget_cancelled,
            budget_wait_ns: value.budget_wait_ns,
        }
    }
}
