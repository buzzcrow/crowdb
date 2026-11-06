// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{ct_future, ct_tree};

#[repr(C)]
#[derive(Default)]
pub struct ct_flush_result_v1 {
    pub struct_size: u32,
    pub version: u32,
    pub terminal_status: i32,
    pub handoff_status: i32,
    pub maintenance_status: i32,
    pub phase: u32,
    pub batch_id: u64,
    pub floor: u64,
    pub frontier: u64,
    pub published_coverage: u64,
    pub deferred_pages: u64,
    pub handoff_completed: i32,
    pub handoff_advanced: i32,
}

#[repr(C)]
#[derive(Default)]
pub struct ct_checkpoint_result_v1 {
    pub struct_size: u32,
    pub version: u32,
    pub terminal_status: i32,
    pub durable_committed: i32,
    pub writeback_completed: i32,
    pub durable_coverage: u64,
    pub snapshot_sequence: u64,
    pub checkpoint_token: u64,
    pub captured_coverage: u64,
    pub primary_frames: u64,
    pub written_frames: u64,
    pub reused_frames: u64,
    pub rebased_pages: u64,
    pub rebase_deferred: u64,
}

extern "C" {
    pub fn ct_durable_coverage(tree: *mut ct_tree) -> u64;
    pub fn ct_flush_report_async(tree: *mut ct_tree, out: *mut *mut ct_future) -> i32;
    pub fn ct_writeback_async(tree: *mut ct_tree, out: *mut *mut ct_future) -> i32;
    pub fn ct_checkpoint_report_async(tree: *mut ct_tree, out: *mut *mut ct_future) -> i32;
    pub fn ct_future_poll_flush_report(
        future: *mut ct_future,
        done: *mut i32,
        out: *mut ct_flush_result_v1,
    ) -> i32;
    pub fn ct_future_poll_checkpoint_report(
        future: *mut ct_future,
        done: *mut i32,
        out: *mut ct_checkpoint_result_v1,
    ) -> i32;
}
