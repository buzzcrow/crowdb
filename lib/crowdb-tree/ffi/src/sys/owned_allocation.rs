// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{c_int, ct_tree};

#[repr(C)]
pub struct ct_owned_allocation {
    _private: [u8; 0],
}

#[repr(C)]
pub struct ct_owned_kv_ref {
    pub key: *const u8,
    pub key_len: usize,
    pub owner: *mut ct_owned_allocation,
    pub offset: usize,
    pub length: usize,
    pub kind: u8,
}

extern "C" {
    pub fn ct_owned_allocation_create(
        tree: *mut ct_tree,
        data: *const u8,
        length: usize,
        capacity: usize,
        drop_context: unsafe extern "C" fn(*mut std::ffi::c_void),
        context: *mut std::ffi::c_void,
        control_bytes: usize,
        out: *mut *mut ct_owned_allocation,
    ) -> c_int;
    pub fn ct_owned_allocation_retain(owner: *mut ct_owned_allocation);
    pub fn ct_owned_allocation_free(owner: *mut ct_owned_allocation);
    pub fn ct_apply_batch_owned_reader(
        tree: *mut ct_tree,
        slot: u64,
        count: usize,
        read: unsafe extern "C" fn(*const std::ffi::c_void, usize) -> ct_owned_kv_ref,
        context: *const std::ffi::c_void,
    ) -> c_int;
}
