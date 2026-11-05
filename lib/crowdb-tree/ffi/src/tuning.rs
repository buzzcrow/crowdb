// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
//! Engine work windows used by open and range rebuild.
use crate::sys;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TreeTuningOptions {
    pub memtable_flush_bytes: usize,
    pub memtable_flush_entries: u32,
    pub max_memtable_count: u32,
    pub leaf_split_bytes: usize,
    pub leaf_merge_bytes: usize,
    pub inner_max_keys: u32,
    pub inner_merge_keys: u32,
    pub max_key_size: usize,
    pub source_max_entries: usize,
    pub source_max_key_bytes: usize,
    pub source_max_gap_entries: usize,
    pub source_max_gap_key_bytes: usize,
    pub flush_parallelism: usize,
    pub run_max_entries: usize,
    pub run_max_key_bytes: usize,
    pub run_key_block_bytes: usize,
    pub run_max_bytes: usize,
    pub install_run_window: usize,
    pub maintenance_visits_per_step: usize,
    pub maintenance_max_visits: usize,
    pub maintenance_max_candidates: usize,
    pub maintenance_max_steps: usize,
    pub maintenance_metadata_bytes: usize,
    pub cache_max_pages: usize,
}

impl Default for TreeTuningOptions {
    fn default() -> Self {
        let mut raw = sys::ct_tree_tuning::default();
        unsafe { sys::ct_tree_tuning_default(&mut raw) };
        Self {
            memtable_flush_bytes: raw.memtable_flush_bytes,
            memtable_flush_entries: raw.memtable_flush_entries,
            max_memtable_count: raw.max_memtable_count,
            leaf_split_bytes: raw.leaf_split_bytes,
            leaf_merge_bytes: raw.leaf_merge_bytes,
            inner_max_keys: raw.inner_max_keys,
            inner_merge_keys: raw.inner_merge_keys,
            max_key_size: raw.max_key_size,
            source_max_entries: raw.source_max_entries,
            source_max_key_bytes: raw.source_max_key_bytes,
            source_max_gap_entries: raw.source_max_gap_entries,
            source_max_gap_key_bytes: raw.source_max_gap_key_bytes,
            flush_parallelism: raw.flush_parallelism,
            run_max_entries: raw.run_max_entries,
            run_max_key_bytes: raw.run_max_key_bytes,
            run_key_block_bytes: raw.run_key_block_bytes,
            run_max_bytes: raw.run_max_bytes,
            install_run_window: raw.install_run_window,
            maintenance_visits_per_step: raw.maintenance_visits_per_step,
            maintenance_max_visits: raw.maintenance_max_visits,
            maintenance_max_candidates: raw.maintenance_max_candidates,
            maintenance_max_steps: raw.maintenance_max_steps,
            maintenance_metadata_bytes: raw.maintenance_metadata_bytes,
            cache_max_pages: raw.cache_max_pages,
        }
    }
}

impl TreeTuningOptions {
    pub(crate) fn as_raw(self) -> sys::ct_tree_tuning {
        sys::ct_tree_tuning {
            struct_size: std::mem::size_of::<sys::ct_tree_tuning>(),
            memtable_flush_bytes: self.memtable_flush_bytes,
            memtable_flush_entries: self.memtable_flush_entries,
            max_memtable_count: self.max_memtable_count,
            leaf_split_bytes: self.leaf_split_bytes,
            leaf_merge_bytes: self.leaf_merge_bytes,
            inner_max_keys: self.inner_max_keys,
            inner_merge_keys: self.inner_merge_keys,
            max_key_size: self.max_key_size,
            source_max_entries: self.source_max_entries,
            source_max_key_bytes: self.source_max_key_bytes,
            source_max_gap_entries: self.source_max_gap_entries,
            source_max_gap_key_bytes: self.source_max_gap_key_bytes,
            flush_parallelism: self.flush_parallelism,
            run_max_entries: self.run_max_entries,
            run_max_key_bytes: self.run_max_key_bytes,
            run_key_block_bytes: self.run_key_block_bytes,
            run_max_bytes: self.run_max_bytes,
            install_run_window: self.install_run_window,
            maintenance_visits_per_step: self.maintenance_visits_per_step,
            maintenance_max_visits: self.maintenance_max_visits,
            maintenance_max_candidates: self.maintenance_max_candidates,
            maintenance_max_steps: self.maintenance_max_steps,
            maintenance_metadata_bytes: self.maintenance_metadata_bytes,
            cache_max_pages: self.cache_max_pages,
        }
    }
}
