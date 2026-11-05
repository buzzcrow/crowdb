// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{ct_future, ct_kv_ref, ct_tree};

#[repr(C)]
pub struct ct_apply_permit {
    _private: [u8; 0],
}

extern "C" {
    pub fn ct_apply_replay(
        tree: *mut ct_tree,
        slot: u64,
        count: usize,
        read: unsafe extern "C" fn(*mut std::ffi::c_void, usize, *mut ct_kv_ref),
        drop_context: unsafe extern "C" fn(*mut std::ffi::c_void),
        context: *mut std::ffi::c_void,
        owner_control_bytes: usize,
        out: *mut *mut ct_future,
    ) -> i32;
    pub fn ct_apply_prepare(
        tree: *mut ct_tree,
        operations: *const ct_kv_ref,
        count: usize,
        gap_repair: i32,
        out: *mut *mut ct_apply_permit,
    ) -> i32;
    pub fn ct_apply_arm(permit: *mut ct_apply_permit) -> i32;
    pub fn ct_apply_submit(permit: *mut ct_apply_permit, slot: u64, out: *mut *mut ct_future) -> i32;
    pub fn ct_apply_permit_free(permit: *mut ct_apply_permit);
}
