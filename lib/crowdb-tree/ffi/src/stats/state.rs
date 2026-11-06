// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::error::{check, CtError};
use crate::{sys, Crowdbtree};

/// Exact and unknown logical subtotals from one immutable publication.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TreeLogicalStats {
    pub leaf_pages: u64,
    pub inner_pages: u64,
    pub height: u64,
    pub dirty_pages: u64,
    pub overlay_pages: u64,
    pub overlay_runs: u64,
    pub overlay_entries: u64,
    pub page_size_known_bytes: u64,
    pub page_size_unknown_pages: u64,
    pub page_size_estimate_bytes: u64,
    pub page_size_max_observed_bytes: u64,
    pub kv_known_count: u64,
    pub kv_known_bytes: u64,
    pub kv_unknown_pages: u64,
    pub consolidate_candidate_pages: u64,
    pub split_candidate_pages: u64,
    pub split_ready_pages: u64,
    pub merge_candidate_pages: u64,
    pub merge_candidate_pairs: u64,
    pub merge_ready_pairs: u64,
    pub writeback_candidate_pages: u64,
    pub evict_candidate_pages: u64,
    pub checkpoint_pending_pages: u64,
    pub checkpoint_mapping_pending: bool,
}

/// Approximate allocation/reference gauges. Referenced subsets overlap.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TreeResourceStats {
    pub allocated_bytes: u64,
    pub reserved_bytes: u64,
    pub payload_bytes: u64,
    pub frame_bytes: u64,
    pub metadata_bytes: u64,
    pub io_bytes: u64,
    pub scratch_bytes: u64,
    pub retired_bytes: u64,
    pub resident_frames: u64,
    pub cache_bytes: u64,
    pub cache_pinned_bytes: u64,
    pub proxy_pages: u64,
    pub read_inflight_bytes: u64,
    pub write_inflight_bytes: u64,
    pub materialize_inflight_bytes: u64,
    pub allocated_backing_bytes: u64,
    pub maintenance_queued: u64,
    pub maintenance_inflight: u64,
}

/// Constant-cost immutable logical snapshot plus independent resource samples.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TreeStatsSnapshot {
    pub instance_generation: u64,
    pub commit_seq: u64,
    pub sampled_at_ns: u64,
    pub logical: TreeLogicalStats,
    pub resources: TreeResourceStats,
}

impl Crowdbtree {
    /// Samples state in O(number of metrics), without loading or walking pages.
    /// Unknown summaries stay explicit.
    pub fn state_stats(&self) -> Result<TreeStatsSnapshot, CtError> {
        let mut raw = sys::ct_tree_stats_snapshot_v1 {
            struct_size: u32::try_from(std::mem::size_of::<sys::ct_tree_stats_snapshot_v1>())
                .map_err(|_| CtError::Internal)?,
            version: 1,
            ..Default::default()
        };
        check(unsafe { sys::ct_get_tree_state_stats(self.as_ptr(), &mut raw) })?;
        Ok(TreeStatsSnapshot {
            instance_generation: raw.instance_generation,
            commit_seq: raw.commit_seq,
            sampled_at_ns: raw.sampled_at_ns,
            logical: TreeLogicalStats {
                leaf_pages: raw.logical.leaf_pages,
                inner_pages: raw.logical.inner_pages,
                height: raw.logical.height,
                dirty_pages: raw.logical.dirty_pages,
                overlay_pages: raw.logical.overlay_pages,
                overlay_runs: raw.logical.overlay_runs,
                overlay_entries: raw.logical.overlay_entries,
                page_size_known_bytes: raw.logical.page_size_known_bytes,
                page_size_unknown_pages: raw.logical.page_size_unknown_pages,
                page_size_estimate_bytes: raw.logical.page_size_estimate_bytes,
                page_size_max_observed_bytes: raw.logical.page_size_max_observed_bytes,
                kv_known_count: raw.logical.kv_known_count,
                kv_known_bytes: raw.logical.kv_known_bytes,
                kv_unknown_pages: raw.logical.kv_unknown_pages,
                consolidate_candidate_pages: raw.logical.consolidate_candidate_pages,
                split_candidate_pages: raw.logical.split_candidate_pages,
                split_ready_pages: raw.logical.split_ready_pages,
                merge_candidate_pages: raw.logical.merge_candidate_pages,
                merge_candidate_pairs: raw.logical.merge_candidate_pairs,
                merge_ready_pairs: raw.logical.merge_ready_pairs,
                writeback_candidate_pages: raw.logical.writeback_candidate_pages,
                evict_candidate_pages: raw.logical.evict_candidate_pages,
                checkpoint_pending_pages: raw.logical.checkpoint_pending_pages,
                checkpoint_mapping_pending: raw.logical.checkpoint_mapping_pending != 0,
            },
            resources: TreeResourceStats {
                allocated_bytes: raw.resources.allocated_bytes,
                reserved_bytes: raw.resources.reserved_bytes,
                payload_bytes: raw.resources.payload_bytes,
                frame_bytes: raw.resources.frame_bytes,
                metadata_bytes: raw.resources.metadata_bytes,
                io_bytes: raw.resources.io_bytes,
                scratch_bytes: raw.resources.scratch_bytes,
                retired_bytes: raw.resources.retired_bytes,
                resident_frames: raw.resources.resident_frames,
                cache_bytes: raw.resources.cache_bytes,
                cache_pinned_bytes: raw.resources.cache_pinned_bytes,
                proxy_pages: raw.resources.proxy_pages,
                read_inflight_bytes: raw.resources.read_inflight_bytes,
                write_inflight_bytes: raw.resources.write_inflight_bytes,
                materialize_inflight_bytes: raw.resources.materialize_inflight_bytes,
                allocated_backing_bytes: raw.resources.allocated_backing_bytes,
                maintenance_queued: raw.resources.maintenance_queued,
                maintenance_inflight: raw.resources.maintenance_inflight,
            },
        })
    }
}

/// Incrementally maintained backlog ages at one immutable publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TreeProgressSnapshot {
    pub instance_generation: u64,
    pub commit_seq: u64,
    pub sampled_at_ns: u64,
    pub candidate_oldest_age_ns: u64,
    pub checkpoint_oldest_pending_age_ns: u64,
    pub maintenance_oldest_age_ns: u64,
}

impl Crowdbtree {
    /// Reads maintained timestamps without loading or walking pages.
    pub fn progress_stats(&self) -> Result<TreeProgressSnapshot, CtError> {
        let mut raw = sys::ct_tree_progress_snapshot_v1 {
            struct_size: std::mem::size_of::<sys::ct_tree_progress_snapshot_v1>() as u32,
            version: 1,
            ..Default::default()
        };
        check(unsafe { sys::ct_get_tree_progress_stats(self.as_ptr(), &mut raw) })?;
        Ok(TreeProgressSnapshot {
            instance_generation: raw.instance_generation,
            commit_seq: raw.commit_seq,
            sampled_at_ns: raw.sampled_at_ns,
            candidate_oldest_age_ns: raw.candidate_oldest_age_ns,
            checkpoint_oldest_pending_age_ns: raw.checkpoint_oldest_pending_age_ns,
            maintenance_oldest_age_ns: raw.maintenance_oldest_age_ns,
        })
    }
}
