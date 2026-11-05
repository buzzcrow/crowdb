// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crate::error::{check, CtError};
use crate::{sys, Crowdbtree};

/// Fixed work-v1 stage order. Enables only this Tree's selected distribution.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeWorkStage {
    FlushBatch = 0,
    FlushAdmission = 1,
    FlushCapture = 2,
    FlushWriterDrain = 3,
    FlushHandoff = 4,
    FlushRange = 5,
    PublishQueue = 6,
    PublishCommit = 7,
    MaintenancePlan = 8,
    OverlayCoalesce = 9,
    PageSplit = 10,
    PageMerge = 11,
    PageLayout = 12,
    PageEncode = 13,
    BudgetWait = 14,
    PageLoadPipeline = 15,
    PageWritebackPipeline = 16,
    SnapshotCapture = 17,
    SnapshotCommit = 18,
    MemoryRetireWait = 19,
    FlushRepair = 20,
}

/// Real nanosecond distribution from common LatencyHistogram. Quantiles are
/// bucket upper bounds, capped at observed max; overflow quantiles are u64::MAX.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TreeHistogramStats {
    pub count: u64,
    pub sum_ns: u128,
    pub min_ns: u64,
    pub max_ns: u64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub zero_count: u64,
    pub overflow_count: u64,
    pub total_count: u64,
}

/// Small immutable results, not bucket storage. Default has no enabled stages.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TreeWorkHistograms {
    pub window_sequence: u64,
    pub sampled_at_ns: u64,
    pub enabled_stages: u64,
    pub stages: [TreeHistogramStats; 21],
}

impl TreeWorkHistograms {
    #[must_use]
    pub fn stage(&self, stage: TreeWorkStage) -> Option<&TreeHistogramStats> {
        let index = stage as usize;
        (self.enabled_stages & (1 << index) != 0).then_some(&self.stages[index])
    }
}

impl From<sys::ct_tree_work_histograms_v1> for TreeWorkHistograms {
    fn from(raw: sys::ct_tree_work_histograms_v1) -> Self {
        Self {
            window_sequence: raw.window_sequence,
            sampled_at_ns: raw.sampled_at_ns,
            enabled_stages: raw.enabled_stages,
            stages: raw.stages.map(|stage| TreeHistogramStats {
                count: stage.count,
                sum_ns: (u128::from(stage.sum_high) << 64) | u128::from(stage.sum_low),
                min_ns: stage.min_ns,
                max_ns: stage.max_ns,
                p50_ns: stage.p50_ns,
                p90_ns: stage.p90_ns,
                p95_ns: stage.p95_ns,
                p99_ns: stage.p99_ns,
                p999_ns: stage.p999_ns,
                zero_count: stage.zero_count,
                overflow_count: stage.overflow_count,
                total_count: stage.total_count,
            }),
        }
    }
}

pub(super) fn raw_histograms() -> Result<sys::ct_tree_work_histograms_v1, CtError> {
    Ok(sys::ct_tree_work_histograms_v1 {
        struct_size: u32::try_from(std::mem::size_of::<sys::ct_tree_work_histograms_v1>())
            .map_err(|_| CtError::Internal)?,
        version: 1,
        ..Default::default()
    })
}

impl Crowdbtree {
    /// Additively enables one distribution. Repeated calls are idempotent.
    /// Bucket storage is charged to this Tree; a budget failure leaves the
    /// selected set and business state unchanged. Terminal events allocate nothing.
    pub fn enable_work_histogram(&self, stage: TreeWorkStage) -> Result<(), CtError> {
        check(unsafe { sys::ct_work_histogram_enable_v1(self.as_ptr(), stage as u32) })
    }

    /// Cumulative distribution sampling never drains the collector window.
    pub fn work_histogram_totals(&self) -> Result<TreeWorkHistograms, CtError> {
        let mut raw = raw_histograms()?;
        check(unsafe { sys::ct_work_histograms_totals_v1(self.as_ptr(), &mut raw) })?;
        Ok(raw.into())
    }
}
