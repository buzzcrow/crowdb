// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{ct_future, ct_range_rebuild_stats, ct_scan_options_v2, ct_scan_result, ct_tree};
use std::ffi::c_int;

#[repr(C)]
pub struct ct_immutable_snapshot {
    _private: [u8; 0],
}
extern "C" {
    pub fn ct_immutable_snapshot_capture(
        tree: *mut ct_tree,
        expected_sequence: u64,
        expected_coverage: u64,
        out: *mut *mut ct_immutable_snapshot,
    ) -> c_int;
    pub fn ct_immutable_snapshot_free(snapshot: *mut ct_immutable_snapshot);
    pub fn ct_immutable_snapshot_get(
        snapshot: *mut ct_immutable_snapshot,
        key: *const u8,
        size: usize,
        out: *mut *mut ct_future,
    ) -> c_int;
    pub fn ct_immutable_snapshot_scan(
        snapshot: *mut ct_immutable_snapshot,
        options: *const ct_scan_options_v2,
        out: *mut *mut ct_scan_result,
    ) -> c_int;
    pub fn ct_immutable_snapshot_rebuild(
        snapshot: *mut ct_immutable_snapshot,
        destination: *mut ct_tree,
        stats: *mut ct_range_rebuild_stats,
    ) -> c_int;
}
