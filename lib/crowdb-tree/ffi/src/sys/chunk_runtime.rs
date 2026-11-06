// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{ct_chunk_transport, ct_future, ct_page_store, ct_root_catalog, ct_runtime, ct_tree};

#[repr(C)]
pub struct ct_chunk_runtime_options {
    pub version: u32,
    pub struct_bytes: u32,
    pub tree_id: u64,
    pub owner_epoch: u64,
    pub open_generation: u64,
    pub iu_size: u32,
    pub mirror_copies: u32,
    pub max_chunk_bytes: u64,
}

extern "C" {
    pub fn ct_chunk_runtime_page_store_open(
        runtime: *mut ct_runtime,
        options: *const ct_chunk_runtime_options,
        catalog: *mut ct_root_catalog,
        transport: *mut ct_chunk_transport,
        out: *mut *mut ct_page_store,
    ) -> i32;
}

#[repr(C)]
#[derive(Default)]
pub struct ct_chunk_collection_result_v1 {
    pub struct_size: u32,
    pub version: u32,
    pub terminal_status: i32,
    pub more: u32,
    pub candidates_examined: u64,
    pub reference_windows: u64,
    pub stages_examined: u64,
    pub pins_examined: u64,
    pub pins_released: u64,
    pub stages_removed: u64,
    pub logical_capacity_bytes: u64,
}
extern "C" {
    pub fn ct_chunk_collect_async(tree: *mut ct_tree, out: *mut *mut ct_future) -> i32;
    pub fn ct_future_poll_chunk_collection(
        future: *mut ct_future,
        done: *mut i32,
        out: *mut ct_chunk_collection_result_v1,
    ) -> i32;
}
