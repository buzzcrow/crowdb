// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_iceberg::{
    catalog::CatalogContext,
    gc::{BudgetedGcStore, GcIoBudget, GcLimits, GcPhase, GcRepository, GcTask, GcWorker},
    key::CatalogId,
};
use std::sync::Arc;

mod lanes;

pub(super) type ScanError = Box<dyn std::error::Error + Send + Sync>;
type ScanResult = Result<(), ScanError>;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct WindowReport {
    pub busy: bool,
    pub progressed: bool,
    pub retry_at_ms: Option<u64>,
    pub failed: bool,
}

#[derive(Clone, Default)]
pub(super) struct ScanPosition {
    pub task: Vec<u8>,
    pub purge: Vec<u8>,
    pub system: Vec<u8>,
    pub progress: u64,
    next_lane: u8,
    // A lane closes only after a validated empty page. Repeats belong to this
    // discovery round, so terminal rows never start another round by themselves.
    pending: u8,
    repeat: u8,
    retry_at_ms: Option<u64>,
    failed_lanes: u8,
    pass_failed: u8,
    // Dropping a window retains its source lane for a fair, discardable retry.
    in_flight: Option<u8>,
}

impl ScanPosition {
    fn begin(&mut self, active: bool) {
        if let Some(lane) = self.in_flight.take() {
            self.record_error(lane);
        }
        if self.pending == 0 {
            self.pending = if active { 15 } else { 8 };
            self.repeat = 0;
            self.retry_at_ms = None;
            self.failed_lanes = 0;
            self.pass_failed = 0;
        } else if !active {
            self.pending &= 8;
            self.repeat &= 8;
            self.failed_lanes &= 8;
            self.pass_failed &= 8;
        }
    }

    pub fn report(&mut self, before: u64, failed: bool) -> WindowReport {
        if failed {
            if let Some(lane) = self.in_flight.take() {
                self.record_error(lane);
            }
        }
        WindowReport {
            busy: self.pending != 0,
            progressed: self.progress != before,
            retry_at_ms: self.retry_at_ms,
            failed: failed || self.failed_lanes != 0,
        }
    }

    fn after(&mut self, lane: u8) -> &mut Vec<u8> {
        match lane {
            0 => &mut self.purge,
            2 => &mut self.system,
            _ => &mut self.task,
        }
    }

    fn observed(&mut self, lane: u8, key: &[u8]) {
        if self.after(lane).as_slice() != key {
            self.progress = self.progress.wrapping_add(1);
            self.after(lane).clear();
            self.after(lane).extend_from_slice(key);
        }
    }

    fn exhausted(&mut self, lane: u8) {
        self.after(lane).clear();
        // An empty page after a failed row is not a successful verification pass.
        if self.pass_failed & (1 << lane) == 0 {
            self.failed_lanes &= !(1 << lane);
        }
        self.pass_failed &= !(1 << lane);
        if self.repeat & (1 << lane) == 0 {
            self.pending &= !(1 << lane);
        }
        self.repeat &= !(1 << lane);
        self.progress = self.progress.wrapping_add(1);
    }

    fn queued(&mut self, lane: u8) {
        if self.pending & (1 << lane) != 0 {
            self.repeat |= 1 << lane;
        }
        self.pending |= 1 << lane;
    }

    fn record_error(&mut self, lane: u8) {
        self.failed_lanes |= 1 << lane;
        self.pass_failed |= 1 << lane;
        self.queued(lane);
    }

    fn runnable(&mut self, task: &GcTask, now_ms: u64) -> bool {
        if matches!(task.phase, GcPhase::Complete | GcPhase::Quarantined) || task.paused {
            return false;
        }
        if now_ms < task.retry_at_ms {
            self.retry_at_ms = Some(
                self.retry_at_ms
                    .map_or(task.retry_at_ms, |at| at.min(task.retry_at_ms)),
            );
            return false;
        }
        true
    }

    fn advanced(&mut self, lane: u8, before: &GcTask, next: &GcTask, now_ms: u64) {
        if next != before {
            self.progress = self.progress.wrapping_add(1);
            if self.runnable(next, now_ms) {
                self.queued(lane);
            }
        }
    }
}

pub(super) async fn scan_and_advance(
    store: Arc<BudgetedGcStore>,
    worker: &GcWorker,
    budget: &GcIoBudget,
    catalog: CatalogId,
    cursor: &mut ScanPosition,
    active: Option<CatalogContext>,
    limits: GcLimits,
) -> Option<ScanError> {
    budget.reset();
    cursor.begin(active.is_some());
    let first = cursor.next_lane;
    cursor.next_lane = (first + 1) % 4;
    let mut attempted = false;
    let mut first_error = None;
    for offset in 0..4 {
        let lane = (first + offset) % 4;
        if cursor.pending & (1 << lane) == 0 {
            continue;
        }
        cursor.in_flight = Some(lane);
        let result = match (lane, active) {
            (0, Some(context)) => lanes::scan_purge(store.clone(), context, cursor, limits).await,
            (1, Some(context)) => admit_multipart(store.clone(), context, cursor, limits).await,
            (2, Some(_)) if !attempted => {
                lanes::scan_retired(store.clone(), worker, cursor, limits, &mut attempted).await
            }
            (3, _) if !attempted => {
                lanes::scan_tasks(store.clone(), worker, catalog, cursor, limits, &mut attempted).await
            }
            _ => Ok(()),
        };
        cursor.in_flight = None;
        if let Err(error) = result {
            cursor.record_error(lane);
            tracing::error!(catalog = %catalog, lane = lane_name(lane), %error,
                "GC discovery lane failed; observed local positions and durable facts retained");
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    first_error
}

async fn admit_multipart(
    store: Arc<BudgetedGcStore>,
    context: CatalogContext,
    cursor: &mut ScanPosition,
    limits: GcLimits,
) -> ScanResult {
    let now_ms = super::super::runtime::now_ms()?;
    let task = GcRepository::new(store)
        .admit_multipart(context, now_ms, limits)
        .await?;
    // Admission reopens Complete tasks; run it once per periodic round, never
    // once per short backlog window.
    cursor.pending &= !2;
    cursor.repeat &= !2;
    cursor.failed_lanes &= !2;
    cursor.pass_failed &= !2;
    cursor.progress = cursor.progress.wrapping_add(1);
    if cursor.runnable(&task, now_ms) {
        cursor.queued(3);
    }
    Ok(())
}

fn lane_name(lane: u8) -> &'static str {
    match lane {
        0 => "purge",
        1 => "multipart",
        2 => "retired",
        _ => "tasks",
    }
}
