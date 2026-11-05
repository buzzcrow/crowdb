// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::error::check;
use crate::{sys, Crowdbtree, CtError};

mod window;
pub use window::ResourceUsageLog;

pub const TREE_RESOURCE_NAMES: [&str; 21] = [
    "handoff",
    "routing",
    "reclaim",
    "writer",
    "gap_repair",
    "io",
    "cpu_queue_slots",
    "backend_queue_slots",
    "cpu_workers",
    "backend_workers",
    "cache_bytes",
    "cache_pages",
    "cache_request_slots",
    "backend_operation_slots",
    "apply_operation_slots",
    "replay_operation_slots",
    "source_entry_slots",
    "source_key_bytes",
    "gap_source_entry_slots",
    "gap_source_key_bytes",
    "chunk_pin_release_slots",
];

/// Relaxed, non-draining admission gauges and cumulative contention counters.
/// Denials count failed capacity attempts, including waiter retries, rather
/// than failed user requests. Waits count submitted budget wait requests.
/// For host queues, waits/wait_ns mean submitted jobs and cumulative queue
/// time, while waiters means runnable queued jobs. Tree queue contention and
/// worker contention counters are not measured by this schema. Operation
/// rows track accepted owners, including callback tails. Source rows report
/// active-source admission, including unapplied writers; their contention
/// counters are not measured.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceUsage {
    pub limit: u64,
    pub used: u64,
    pub reserved: u64,
    pub allocated: u64,
    pub denied_tree: u64,
    pub denied_host: u64,
    pub waits: u64,
    pub wait_ns: u64,
    pub waiters: u64,
}

impl From<sys::ct_resource_usage_v1> for ResourceUsage {
    fn from(value: sys::ct_resource_usage_v1) -> Self {
        Self {
            limit: value.limit,
            used: value.used,
            reserved: value.reserved,
            allocated: value.allocated,
            denied_tree: value.denied_tree,
            denied_host: value.denied_host,
            waits: value.waits,
            wait_ns: value.wait_ns,
            waiters: value.waiters,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceUsageSnapshot {
    /// Supported resource indices. Hosts expose shared workers; Trees expose
    /// their cache capacities instead. See `TREE_RESOURCE_NAMES` for indices.
    /// The Chunk host adds its pin-release service at index 20; the native
    /// runtime/Tree v1 snapshots retain their original 20-row ABI.
    pub resource_mask: u32,
    pub scope_id: u64,
    pub resources: [ResourceUsage; TREE_RESOURCE_NAMES.len()],
}

fn sample(
    f: impl FnOnce(*mut sys::ct_resource_usage_snapshot_v1) -> i32,
) -> Result<ResourceUsageSnapshot, CtError> {
    let mut raw = sys::ct_resource_usage_snapshot_v1 {
        struct_size: std::mem::size_of::<sys::ct_resource_usage_snapshot_v1>(),
        version: 1,
        ..Default::default()
    };
    check(f(&mut raw))?;
    if raw.resource_mask & !((1_u32 << raw.resources.len()) - 1) != 0 {
        return Err(CtError::Internal);
    }
    let mut resources = [ResourceUsage::default(); TREE_RESOURCE_NAMES.len()];
    for (target, value) in resources.iter_mut().zip(raw.resources) {
        *target = value.into();
    }
    Ok(ResourceUsageSnapshot {
        resource_mask: raw.resource_mask,
        scope_id: raw.scope_id,
        resources,
    })
}

impl super::TreeRuntime {
    /// Reads gauges without resetting any periodic sampler or work window.
    pub fn resource_usage(&self) -> Result<ResourceUsageSnapshot, CtError> {
        sample(|out| unsafe { sys::ct_runtime_resource_usage(self.as_ptr(), out) })
    }
}

impl Crowdbtree {
    /// Reads Tree admission and queue permits without page walks or I/O.
    pub fn resource_usage(&self) -> Result<ResourceUsageSnapshot, CtError> {
        sample(|out| unsafe { sys::ct_tree_resource_usage(self.as_ptr(), out) })
    }
}
