// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{ct_options, ct_runtime, ct_tree, ct_tree_runtime_options};
extern "C" {
    pub fn ct_migrate_legacy_chunk_with_runtime(
        options: *const ct_options,
        runtime: *mut ct_runtime,
        binding: *const ct_tree_runtime_options,
        expected_generation: u64,
        out: *mut *mut ct_tree,
    ) -> i32;
}
