// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{c_void, crowdb_rpc_buffer_t};
#[repr(C)]
pub struct crowdb_rpc_control_allocator_v1 {
    pub version: u32,
    pub context: *mut c_void,
    pub claim: Option<unsafe extern "C" fn(*mut c_void, usize, usize) -> i32>,
    pub record: Option<unsafe extern "C" fn(*mut c_void, usize, usize, i32)>,
    pub release: Option<unsafe extern "C" fn(*mut c_void, usize, usize)>,
}
unsafe extern "C" {
    pub fn crowdb_rpc_buffer_create_external_accounted(
        data: *const u8,
        len: u32,
        free_cb: Option<unsafe extern "C" fn(*mut c_void)>,
        free_ctx: *mut c_void,
        allocator: *const crowdb_rpc_control_allocator_v1,
    ) -> crowdb_rpc_buffer_t;
}
