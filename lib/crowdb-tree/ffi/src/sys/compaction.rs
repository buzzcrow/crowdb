// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{ct_future, ct_tree};
#[repr(C)]
pub struct ct_compaction_options_v1 {
    pub struct_size: u32,
    pub version: u32,
    pub free_threshold: f64,
    pub max_relocation_bytes: u64,
}
#[repr(C)]
#[derive(Default)]
pub struct ct_compaction_result_v1 {
    pub struct_size: u32,
    pub version: u32,
    pub terminal_status: i32,
    pub blocks_selected: u64,
    pub pages_relocated: u64,
    pub bytes_relocated: u64,
    pub blocks_deleted: u64,
}
extern "C" {
    pub fn ct_compact_sparse_blocks_async(
        tree: *mut ct_tree,
        options: *const ct_compaction_options_v1,
        out: *mut *mut ct_future,
    ) -> i32;
    pub fn ct_future_poll_compaction(
        future: *mut ct_future,
        done: *mut i32,
        out: *mut ct_compaction_result_v1,
    ) -> i32;
}
