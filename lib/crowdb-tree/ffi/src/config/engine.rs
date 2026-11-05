// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Startup engine settings. Native work-window defaults come through the FFI.
use super::TreeRuntimeConfig;
use crate::{Config, TreeTuningOptions};

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TreeEngineConfig {
    pub frame_bytes: u32,
    pub buffer_pool_bytes: u64,
    pub max_inline_value: u64,
    pub compression_lz4: bool,
    pub block_size: u64,
    pub iu_size: u32,
    pub tuning: TreeTuningOptions,
}

impl Default for TreeEngineConfig {
    fn default() -> Self {
        Self {
            frame_bytes: 64 * 1024,
            buffer_pool_bytes: 64 * 1024 * 1024,
            max_inline_value: 0,
            compression_lz4: false,
            block_size: 64 * 1024 * 1024,
            iu_size: 0,
            tuning: TreeTuningOptions::default(),
        }
    }
}

impl TreeEngineConfig {
    pub fn apply_to(&self, options: &mut Config) {
        options.frame_bytes = self.frame_bytes;
        options.buffer_pool_bytes = self.buffer_pool_bytes;
        options.max_inline_value = self.max_inline_value;
        options.compression_lz4 = self.compression_lz4;
        options.block_size = self.block_size;
        options.iu_size = self.iu_size;
        options.tuning = self.tuning;
    }

    /// Validate before creating or recovering storage.
    /// # Errors
    /// Returns the incompatible configuration field or window.
    pub fn validate(&self, runtime: &TreeRuntimeConfig) -> Result<(), String> {
        if !(512..=65536).contains(&self.frame_bytes) || self.buffer_pool_bytes < u64::from(self.frame_bytes)
        {
            return Err("tree.frame_bytes must be in 512..=65536 and fit buffer_pool_bytes".to_owned());
        }
        if self.iu_size != 0
            && (!self.iu_size.is_power_of_two() || self.frame_bytes.checked_rem(self.iu_size) != Some(0))
        {
            return Err(
                "tree.iu_size must be a power of two dividing frame_bytes, or zero for the backend default"
                    .to_owned(),
            );
        }
        let t = &self.tuning;
        for (name, value) in [
            ("leaf_split_bytes", t.leaf_split_bytes),
            ("inner_max_keys", t.inner_max_keys as usize),
            ("source_max_entries", t.source_max_entries),
            ("source_max_key_bytes", t.source_max_key_bytes),
            ("source_max_gap_entries", t.source_max_gap_entries),
            ("source_max_gap_key_bytes", t.source_max_gap_key_bytes),
            ("run_max_entries", t.run_max_entries),
            ("run_max_key_bytes", t.run_max_key_bytes),
            ("run_key_block_bytes", t.run_key_block_bytes),
            ("run_max_bytes", t.run_max_bytes),
            ("install_run_window", t.install_run_window),
            ("maintenance_visits_per_step", t.maintenance_visits_per_step),
            ("maintenance_max_visits", t.maintenance_max_visits),
            ("maintenance_max_candidates", t.maintenance_max_candidates),
            ("maintenance_max_steps", t.maintenance_max_steps),
            ("maintenance_metadata_bytes", t.maintenance_metadata_bytes),
            ("cache_max_pages", t.cache_max_pages),
        ] {
            if value == 0 {
                return Err(format!("tree.tuning.{name} must be positive"));
            }
        }
        if t.max_memtable_count < 2 || t.max_memtable_count > 64 {
            return Err("tree.tuning.max_memtable_count must be in 2..=64 (source window)".to_owned());
        }
        if t.run_key_block_bytes > t.run_max_key_bytes
            || t.maintenance_visits_per_step > t.maintenance_max_visits
        {
            return Err("tree.tuning step must fit its enclosing work window".to_owned());
        }
        if t.flush_parallelism > runtime.runtime_options().workers
            || t.flush_parallelism > runtime.binding_options().limits.io_operations
        {
            return Err("tree.tuning.flush_parallelism exceeds host workers or Tree I/O permits".to_owned());
        }
        Ok(())
    }
}
