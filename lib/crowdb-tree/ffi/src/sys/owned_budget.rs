// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{ct_future, ct_scan_result, ct_snapshot_chunk};

#[repr(C)]
pub struct ct_owned_budget {
    _private: [u8; 0],
}

extern "C" {
    pub fn ct_future_owned_budget(future: *const ct_future, out: *mut *mut ct_owned_budget) -> i32;
    pub fn ct_scan_result_owned_budget(result: *const ct_scan_result, out: *mut *mut ct_owned_budget) -> i32;
    pub fn ct_snapshot_chunk_owned_budget(
        chunk: *const ct_snapshot_chunk,
        out: *mut *mut ct_owned_budget,
    ) -> i32;
    pub fn ct_owned_budget_claim(budget: *mut ct_owned_budget, bytes: usize) -> i32;
    pub fn ct_owned_budget_record(budget: *mut ct_owned_budget, bytes: usize, acquired: i32);
    pub fn ct_owned_budget_release(budget: *mut ct_owned_budget, bytes: usize);
    pub fn ct_owned_budget_free(budget: *mut ct_owned_budget);
}
