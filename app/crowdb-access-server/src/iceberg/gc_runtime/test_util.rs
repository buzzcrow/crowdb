// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{scan_and_advance, BudgetedGcStore, CatalogId, GcIoBudget, GcLimits, GcWorker, ScanPosition};
use super::{
    schedule::{next_delay, Schedule},
    sweep::{ScanError, WindowReport},
};
use crowdb_access_iceberg::{
    catalog::CatalogContext,
    file::FileBlockStore,
    gc::{BudgetedGcBlocks, GcRepository, GcStore},
};
use std::{sync::Arc, time::Duration};

pub struct TestGcSweep {
    store: Arc<BudgetedGcStore>,
    worker: GcWorker,
    budget: Arc<GcIoBudget>,
    catalog: CatalogId,
    active: Option<CatalogContext>,
    limits: GcLimits,
    cursor: ScanPosition,
    failures: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct TestGcWindow {
    pub busy: bool,
    pub progressed: bool,
    pub failed: bool,
    pub retry_at_ms: Option<u64>,
    pub delay_ms: u64,
}

pub struct TestGcSchedule(Schedule);

impl TestGcSweep {
    /// Creates the production scheduler with in-process storage boundaries.
    /// # Errors
    /// Rejects invalid worker limits.
    pub fn new(
        store: Arc<dyn GcStore>,
        blocks: Arc<dyn FileBlockStore>,
        catalog: CatalogId,
        active: Option<CatalogContext>,
        limits: GcLimits,
    ) -> Result<Self, crowdb_access_iceberg::error::ValidationError> {
        let budget = Arc::new(GcIoBudget::new(64 * 1024 * 1024, 128, 8 * 1024 * 1024, 128));
        let store = Arc::new(BudgetedGcStore::new(store, budget.clone()));
        let blocks = Arc::new(BudgetedGcBlocks::new(blocks, budget.clone()));
        let worker = GcWorker::new(GcRepository::new(store.clone()), blocks, limits)?;
        Ok(Self {
            store,
            worker,
            budget,
            catalog,
            active,
            limits,
            cursor: ScanPosition::default(),
            failures: 0,
        })
    }

    /// Returns a discardable discovery position for purge, retired ledger or tasks.
    #[must_use]
    pub fn position_for_tests(&self, lane: u8) -> Vec<u8> {
        match lane {
            0 => self.cursor.purge.clone(),
            2 => self.cursor.system.clone(),
            _ => self.cursor.task.clone(),
        }
    }

    /// Advances one production scheduler window with the production deadline.
    /// # Errors
    /// Returns the original scan or action error, retaining durable resource facts.
    pub async fn advance_for_tests(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (_, error) = self.window().await;
        error.map_or(Ok(()), Err)
    }

    /// Returns the real sweep's scheduling decision without hiding failed lanes.
    pub async fn window_for_tests(&mut self, idle_ms: u64) -> TestGcWindow {
        let (report, _) = self.window().await;
        let delay_ms = next_delay(
            report,
            &mut self.failures,
            idle_ms,
            self.limits,
            super::super::runtime::now_ms().unwrap_or(0),
        );
        TestGcWindow {
            busy: report.busy,
            progressed: report.progressed,
            failed: report.failed,
            retry_at_ms: report.retry_at_ms,
            delay_ms,
        }
    }

    /// Creates the production catalog scheduler without starting a background task.
    #[must_use]
    pub fn schedule_for_tests(configured: &[CatalogId], active: CatalogId) -> TestGcSchedule {
        let mut schedule = Schedule::default();
        schedule.retain_scopes(configured, active);
        TestGcSchedule(schedule)
    }

    async fn window(&mut self) -> (WindowReport, Option<ScanError>) {
        let before = self.cursor.progress;
        let error = match tokio::time::timeout(
            Duration::from_millis(u64::from(self.limits.step_ms) * 2 + 1000),
            scan_and_advance(
                self.store.clone(),
                &self.worker,
                self.budget.as_ref(),
                self.catalog,
                &mut self.cursor,
                self.active,
                self.limits,
            ),
        )
        .await
        {
            Ok(error) => error,
            Err(error) => Some(Box::new(error) as ScanError),
        };
        (self.cursor.report(before, error.is_some()), error)
    }
}

impl TestGcSchedule {
    pub fn retain_scopes_for_tests(&mut self, configured: &[CatalogId], active: CatalogId) {
        self.0.retain_scopes(configured, active);
    }

    #[must_use]
    pub fn scopes_for_tests(&self) -> usize {
        self.0.len()
    }

    pub fn ready_for_tests(&mut self) -> Option<CatalogId> {
        self.0.ready().map(|slot| self.0.catalog(slot))
    }

    /// Applies a production scheduling result to a configured scope.
    /// # Panics
    /// Panics when the catalog is outside the configured and active scopes.
    pub fn finish_for_tests(&mut self, catalog: CatalogId, window: TestGcWindow, idle_ms: u64) {
        let slot = (0..self.0.len())
            .find(|slot| self.0.catalog(*slot) == catalog)
            .unwrap();
        self.0.finish(
            slot,
            WindowReport {
                busy: window.busy,
                progressed: window.progressed,
                failed: window.failed,
                retry_at_ms: window.retry_at_ms,
            },
            idle_ms,
            GcLimits::default(),
        );
    }

    #[must_use]
    pub fn wait_ms_for_tests(&self) -> u64 {
        u64::try_from(
            self.0
                .next_due()
                .saturating_duration_since(tokio::time::Instant::now())
                .as_millis(),
        )
        .unwrap_or(u64::MAX)
    }
}
