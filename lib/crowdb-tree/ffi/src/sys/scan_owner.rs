// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{ct_future, ct_tree};

#[repr(C)]
pub struct ct_scan_result {
    _private: [u8; 0],
}

#[repr(C)]
pub struct ct_scan_options_v2 {
    pub struct_size: usize,
    pub prefix: *const u8,
    pub prefix_len: usize,
    pub start: *const u8,
    pub start_len: usize,
    pub end: *const u8,
    pub end_len: usize,
    pub begin: *const u8,
    pub begin_len: usize,
    pub limit: usize,
    pub byte_budget: usize,
    pub deadline_ms: u64,
    pub reverse: i32,
    pub keys_only: i32,
    pub include_tombstones: i32,
    pub has_start_bound: i32,
    pub start_inclusive: i32,
    pub strict_byte_budget: i32,
}

extern "C" {
    pub fn ct_scan_async_v2(
        tree: *mut ct_tree,
        options: *const ct_scan_options_v2,
        out: *mut *mut ct_future,
    ) -> i32;
    pub fn ct_scan_v2(
        tree: *mut ct_tree,
        options: *const ct_scan_options_v2,
        out: *mut *mut ct_scan_result,
    ) -> i32;
    pub fn ct_future_poll_scan_v2(
        future: *mut ct_future,
        done: *mut i32,
        out: *mut *mut ct_scan_result,
    ) -> i32;
    pub fn ct_scan_result_data(
        result: *const ct_scan_result,
        data: *mut *const u8,
        len: *mut usize,
        count: *mut u64,
        truncated: *mut i32,
    ) -> i32;
    pub fn ct_scan_result_free(result: *mut ct_scan_result);
}
