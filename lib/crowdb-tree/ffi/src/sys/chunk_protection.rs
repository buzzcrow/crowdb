// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{
    ct_chunk_stage_id, ct_root_catalog, ct_root_catalog_callbacks, ct_root_catalog_staging_callbacks,
};
use std::ffi::c_void;
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ct_chunk_candidate_status {
    pub id: ct_chunk_stage_id,
    pub generation: u64,
    pub reference_count: u64,
    pub prepared_references: u64,
    pub prepared_windows: u64,
    pub released_windows: u64,
    pub state: u32,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ct_chunk_pin_progress {
    pub after: ct_chunk_stage_id,
    pub examined: u32,
    pub released: u32,
    pub has_after: u8,
    pub more: u8,
}
#[repr(C)]
pub struct ct_root_catalog_protection_callbacks {
    pub version: u32,
    pub struct_bytes: u32,
    pub capabilities: u64,
    pub check_reader: Option<unsafe extern "C" fn(*mut c_void) -> i32>,
    pub writer_quiesced: Option<unsafe extern "C" fn(*mut c_void, u64, u64) -> i32>,
    pub begin: Option<
        unsafe extern "C" fn(*mut c_void, u64, ct_chunk_stage_id, u64, u64, u32, *const u8, usize) -> i32,
    >,
    pub load: Option<
        unsafe extern "C" fn(*mut c_void, u64, ct_chunk_stage_id, *mut ct_chunk_candidate_status) -> i32,
    >,
    pub list: Option<
        unsafe extern "C" fn(
            *mut c_void,
            u64,
            *const ct_chunk_stage_id,
            u32,
            *mut ct_chunk_candidate_status,
            *mut usize,
        ) -> i32,
    >,
    pub prepare: Option<
        unsafe extern "C" fn(
            *mut c_void,
            u64,
            ct_chunk_stage_id,
            u64,
            *const ct_chunk_stage_id,
            usize,
        ) -> i32,
    >,
    pub finish: Option<unsafe extern "C" fn(*mut c_void, u64, ct_chunk_stage_id, u32) -> i32>,
    pub release: Option<unsafe extern "C" fn(*mut c_void, u64, ct_chunk_stage_id, u64) -> i32>,
    pub pin: Option<unsafe extern "C" fn(*mut c_void, u64, ct_chunk_stage_id, ct_chunk_stage_id, u8) -> i32>,
    pub prune: Option<
        unsafe extern "C" fn(
            *mut c_void,
            u64,
            ct_chunk_stage_id,
            *const ct_chunk_stage_id,
            u32,
            *mut ct_chunk_pin_progress,
        ) -> i32,
    >,
}
unsafe extern "C" {
    pub fn ct_callback_root_catalog_open_v3(
        callbacks: *const ct_root_catalog_callbacks,
        staging: *const ct_root_catalog_staging_callbacks,
        protection: *const ct_root_catalog_protection_callbacks,
        context: *mut c_void,
        out: *mut *mut ct_root_catalog,
    ) -> i32;
    pub fn ct_root_catalog_protection_capabilities(catalog: *const ct_root_catalog) -> u64;
}
