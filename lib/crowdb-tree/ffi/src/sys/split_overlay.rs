// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::ct_tree;
use std::os::raw::c_int;

#[repr(C)]
pub struct ct_prepared_split_overlay {
    _private: [u8; 0],
}

#[repr(C)]
pub struct ct_split_overlay_limits_v1 {
    pub version: u32,
    pub struct_size: u32,
    pub max_records: u64,
    pub max_bytes: u64,
}

#[repr(C)]
#[derive(Default)]
pub struct ct_installed_split_overlay_v1 {
    pub version: u32,
    pub struct_size: u32,
    pub generation: u64,
    pub frontier: u64,
}

unsafe extern "C" {
    pub fn ct_arm_split_overlay(
        source: *mut ct_tree,
        destination: *mut ct_tree,
        limits: *const ct_split_overlay_limits_v1,
        out: *mut *mut ct_prepared_split_overlay,
        start_frontier: *mut u64,
    ) -> c_int;
    pub fn ct_prepared_split_overlay_prepare(
        prepared: *mut ct_prepared_split_overlay,
        replayed_prefix: u64,
    ) -> c_int;
    pub fn ct_prepared_split_overlay_try_install(
        prepared: *mut ct_prepared_split_overlay,
        frontier: u64,
        out: *mut ct_installed_split_overlay_v1,
    ) -> c_int;
    pub fn ct_prepared_split_overlay_committed_frontier(
        prepared: *const ct_prepared_split_overlay,
        out: *mut u64,
    ) -> i32;
    pub fn ct_prepared_split_overlay_destroy(prepared: *mut ct_prepared_split_overlay);
}
