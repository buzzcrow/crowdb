// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::TreeWorkSnapshot;
use crate::{error::CtError, sys};

/// Fixed cause and quantity order shared by native, C and immutable host samples.
pub const TREE_WORK_CAUSE_NAMES: [&str; 8] = [
    "other",
    "flush",
    "cache_pressure",
    "checkpoint",
    "gc",
    "read",
    "snapshot",
    "allocation",
];
pub const TREE_WORK_COUNTER_NAMES: [&str; 55] = [
    "flush.input.tables",
    "flush.range.jobs",
    "flush.candidate.versions",
    "flush.winning.versions",
    "flush.winning.key.bytes",
    "flush.winning.value.bytes",
    "flush.handoff.bytes",
    "source.detached",
    "candidate.checks",
    "candidate.skips",
    "publication.mismatches",
    "split.operations",
    "split.output.pages",
    "merge.operations",
    "merge.input.pages",
    "merge.output.pages",
    "coalesce.operations",
    "coalesce.input.runs",
    "coalesce.output.runs",
    "coalesce.input.entries",
    "coalesce.output.entries",
    "payload.references.dropped",
    "memory.reclaimed.bytes",
    "key.copy.bytes",
    "handoff.value.copy.bytes",
    "cow.metadata.copy.bytes",
    "materialize.copy.bytes",
    "backend.bounce.copy.bytes",
    "encode.frame.bytes",
    "encode.primary.frames",
    "encode.overflow.frames",
    "encode.key.copy.bytes",
    "encode.value.copy.bytes",
    "read.operations",
    "read.bytes",
    "read.errors",
    "write.operations",
    "write.bytes",
    "write.errors",
    "reused.frames",
    "reused.backing.bytes",
    "proxy.avoided.loads",
    "cache.hits",
    "cache.misses",
    "cache.joined.loads",
    "cache.evictions",
    "callbacks.stale",
    "rebase.mismatches",
    "admission.rejected",
    "admission.waits",
    "snapshot.retirement.delays",
    "backing.deleted.bytes",
    "checkpoint.failures",
    "retries",
    "memory.logical.retirement.bytes",
];
/// Completed physical quantities and committed logical outcomes. Values are
/// cumulative relaxed samples, not transactionally consistent with stage ages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TreeWorkCounters {
    pub sampled_at_ns: u64,
    pub values: [[u64; 55]; 8],
}
impl Default for TreeWorkCounters {
    fn default() -> Self {
        Self {
            sampled_at_ns: 0,
            values: [[0; 55]; 8],
        }
    }
}
impl TreeWorkSnapshot {
    pub(super) fn with_counters(mut self, raw: sys::ct_tree_work_counters_v1) -> Self {
        self.counters = TreeWorkCounters {
            sampled_at_ns: raw.sampled_at_ns,
            values: raw.values,
        };
        self
    }
}
pub(super) fn raw_counters() -> Result<sys::ct_tree_work_counters_v1, CtError> {
    Ok(sys::ct_tree_work_counters_v1 {
        struct_size: u32::try_from(std::mem::size_of::<sys::ct_tree_work_counters_v1>())
            .map_err(|_| CtError::Internal)?,
        version: 1,
        ..Default::default()
    })
}
