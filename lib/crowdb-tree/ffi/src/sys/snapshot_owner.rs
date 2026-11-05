// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::ct_export;
use std::os::raw::c_int;

#[repr(C)]
pub struct ct_snapshot_chunk {
    _private: [u8; 0],
}

extern "C" {
    pub fn ct_snapshot_export_next_v2(
        export: *mut ct_export,
        offset: u64,
        out: *mut *mut ct_snapshot_chunk,
        done: *mut c_int,
    ) -> c_int;
    pub fn ct_snapshot_chunk_data(
        chunk: *const ct_snapshot_chunk,
        data: *mut *const u8,
        len: *mut usize,
    ) -> c_int;
    pub fn ct_snapshot_chunk_free(chunk: *mut ct_snapshot_chunk);
}
