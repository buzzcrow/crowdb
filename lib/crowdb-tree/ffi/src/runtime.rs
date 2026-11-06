// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared host CPU capacity and resource limits for budgeted runtime Trees.
//! Cloned runtime owners share the same CPU pool and allocation ledger.

use std::ptr::NonNull;

use crate::error::{check, CtError};
use crate::sys;

mod stats;
mod usage;
pub use stats::{TreeExecutorSnapshot, TreeRuntimeSnapshot};
pub use usage::{ResourceUsage, ResourceUsageLog, ResourceUsageSnapshot, TREE_RESOURCE_NAMES};

/// Independent partitions; `io_operations` is a count, all other limits bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TreeResourceLimits {
    pub handoff_bytes: usize,
    pub routing_bytes: usize,
    pub reclaim_bytes: usize,
    pub writer_bytes: usize,
    pub gap_repair_bytes: usize,
    pub io_operations: usize,
}

impl From<sys::ct_resource_limits> for TreeResourceLimits {
    fn from(value: sys::ct_resource_limits) -> Self {
        Self {
            handoff_bytes: value.handoff_bytes,
            routing_bytes: value.routing_bytes,
            reclaim_bytes: value.reclaim_bytes,
            writer_bytes: value.writer_bytes,
            gap_repair_bytes: value.gap_repair_bytes,
            io_operations: value.io_operations,
        }
    }
}

impl From<TreeResourceLimits> for sys::ct_resource_limits {
    fn from(value: TreeResourceLimits) -> Self {
        Self {
            handoff_bytes: value.handoff_bytes,
            routing_bytes: value.routing_bytes,
            reclaim_bytes: value.reclaim_bytes,
            writer_bytes: value.writer_bytes,
            gap_repair_bytes: value.gap_repair_bytes,
            io_operations: value.io_operations,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TreeRuntimeOptions {
    pub backend_workers: usize,
    pub backend_queue_limit: usize,
    pub workers: usize,
    pub cpu_queue_limit: usize,
    pub max_waiters_per_class: usize,
    pub lifecycle_bytes: usize,
    pub limits: TreeResourceLimits,
}

impl Default for TreeRuntimeOptions {
    fn default() -> Self {
        let mut raw = sys::ct_runtime_options::default();
        unsafe { sys::ct_runtime_options_default(&mut raw) };
        Self {
            backend_workers: raw.backend_workers,
            backend_queue_limit: raw.backend_queue_limit,
            workers: raw.workers,
            cpu_queue_limit: raw.cpu_queue_limit,
            max_waiters_per_class: raw.max_waiters_per_class,
            lifecycle_bytes: raw.lifecycle_bytes,
            limits: raw.limits.into(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TreeBindingOptions {
    pub backend_queue_limit: usize,
    pub cpu_queue_limit: usize,
    pub lifecycle_bytes: usize,
    pub limits: TreeResourceLimits,
}

impl Default for TreeBindingOptions {
    fn default() -> Self {
        let mut raw = sys::ct_tree_runtime_options::default();
        unsafe { sys::ct_tree_runtime_options_default(&mut raw) };
        Self {
            backend_queue_limit: raw.backend_queue_limit,
            cpu_queue_limit: raw.cpu_queue_limit,
            lifecycle_bytes: raw.lifecycle_bytes,
            limits: raw.limits.into(),
        }
    }
}

impl TreeBindingOptions {
    pub(crate) fn as_raw(self) -> sys::ct_tree_runtime_options {
        sys::ct_tree_runtime_options {
            struct_size: std::mem::size_of::<sys::ct_tree_runtime_options>(),
            cpu_queue_limit: self.cpu_queue_limit,
            lifecycle_bytes: self.lifecycle_bytes,
            limits: self.limits.into(),
            backend_queue_limit: self.backend_queue_limit,
        }
    }
}

/// Share with `Arc<TreeRuntime>` in multiple [`crate::Config`] values.
/// Native bindings retain independent ownership; dropping this handle does
/// not stop live Trees. The last owner must be released at a host boundary.
#[derive(Debug)]
pub struct TreeRuntime {
    ptr: NonNull<sys::ct_runtime>,
}

impl TreeRuntime {
    /// Read cumulative host counters without resetting a collector window.
    pub fn snapshot(&self) -> Result<TreeRuntimeSnapshot, CtError> {
        let mut raw = sys::ct_runtime_snapshot_v1 {
            struct_size: std::mem::size_of::<sys::ct_runtime_snapshot_v1>(),
            version: 1,
            ..Default::default()
        };
        check(unsafe { sys::ct_runtime_snapshot(self.as_ptr(), &mut raw) })?;
        Ok(raw.into())
    }

    pub fn new(options: TreeRuntimeOptions) -> Result<Self, CtError> {
        let raw = sys::ct_runtime_options {
            struct_size: std::mem::size_of::<sys::ct_runtime_options>(),
            workers: options.workers,
            cpu_queue_limit: options.cpu_queue_limit,
            max_waiters_per_class: options.max_waiters_per_class,
            lifecycle_bytes: options.lifecycle_bytes,
            limits: options.limits.into(),
            backend_workers: options.backend_workers,
            backend_queue_limit: options.backend_queue_limit,
        };
        let mut out = std::ptr::null_mut();
        check(unsafe { sys::ct_runtime_create(&raw, &mut out) })?;
        Ok(Self {
            ptr: NonNull::new(out).ok_or(CtError::Internal)?,
        })
    }

    /// Reject new bindings/reservations and cancel budget waiters. Accepted
    /// operations can still submit CPU continuations using retained credit.
    pub fn stop_admission(&self) {
        unsafe { sys::ct_runtime_stop_admission(self.as_ptr()) };
    }

    /// Blocking host boundary after Tree operations and backend tickets end.
    /// Closes CPU submission and waits for CPU jobs only, not external I/O.
    pub fn drain_cpu(&self) {
        unsafe { sys::ct_runtime_drain_cpu(self.as_ptr()) };
    }

    pub(crate) fn as_ptr(&self) -> *mut sys::ct_runtime {
        self.ptr.as_ptr()
    }
}

// Native runtime methods share atomic admission/queue state. The opaque handle
// stays alive under Rust ownership for every call; Drop needs exclusive access.
unsafe impl Send for TreeRuntime {}
unsafe impl Sync for TreeRuntime {}

impl Drop for TreeRuntime {
    fn drop(&mut self) {
        unsafe { sys::ct_runtime_free(self.as_ptr()) };
    }
}
