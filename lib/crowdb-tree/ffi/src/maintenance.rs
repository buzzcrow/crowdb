// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crate::error::{check, CtError};
use crate::sys;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushPhase {
    Captured,
    Handoff,
    RepairRequired,
    Covered,
    Maintenance,
    Complete,
    Unknown(u32),
}

/// An accepted flush retains both phase results, including partial progress.
/// A maintenance failure does not undo an already completed L0-to-L1 handoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushReport {
    pub status: Result<(), CtError>,
    pub handoff_status: Result<(), CtError>,
    pub maintenance_status: Result<(), CtError>,
    pub phase: FlushPhase,
    pub batch_id: u64,
    pub floor: u64,
    pub frontier: u64,
    pub published_coverage: u64,
    pub deferred_pages: u64,
    pub handoff_completed: bool,
    /// This attempt first advanced coverage; retries never count it twice.
    pub handoff_advanced: bool,
}

impl From<sys::ct_flush_result_v1> for FlushReport {
    fn from(raw: sys::ct_flush_result_v1) -> Self {
        Self {
            status: check(raw.terminal_status),
            handoff_status: check(raw.handoff_status),
            maintenance_status: check(raw.maintenance_status),
            phase: match raw.phase {
                0 => FlushPhase::Captured,
                1 => FlushPhase::Handoff,
                2 => FlushPhase::RepairRequired,
                3 => FlushPhase::Covered,
                4 => FlushPhase::Maintenance,
                5 => FlushPhase::Complete,
                other => FlushPhase::Unknown(other),
            },
            batch_id: raw.batch_id,
            floor: raw.floor,
            frontier: raw.frontier,
            published_coverage: raw.published_coverage,
            deferred_pages: raw.deferred_pages,
            handoff_completed: raw.handoff_completed != 0,
            handoff_advanced: raw.handoff_advanced != 0,
        }
    }
}

/// Writeback keeps durable coverage unchanged. Only a completed durable
/// checkpoint may advance recovery and WAL reclamation watermarks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointReport {
    pub status: Result<(), CtError>,
    pub durable_committed: bool,
    pub writeback_completed: bool,
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

impl From<sys::ct_checkpoint_result_v1> for CheckpointReport {
    fn from(raw: sys::ct_checkpoint_result_v1) -> Self {
        Self {
            status: check(raw.terminal_status),
            durable_committed: raw.durable_committed != 0,
            writeback_completed: raw.writeback_completed != 0,
            durable_coverage: raw.durable_coverage,
            snapshot_sequence: raw.snapshot_sequence,
            checkpoint_token: raw.checkpoint_token,
            captured_coverage: raw.captured_coverage,
            primary_frames: raw.primary_frames,
            written_frames: raw.written_frames,
            reused_frames: raw.reused_frames,
            rebased_pages: raw.rebased_pages,
            rebase_deferred: raw.rebase_deferred,
        }
    }
}
