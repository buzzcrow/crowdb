// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{scan_and_advance, BudgetedGcStore, CatalogId, GcIoBudget, GcLimits, GcWorker, ScanPosition};
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
}

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
        let budget = Arc::new(GcIoBudget::new(256 * 1024 * 1024, 128, 64 * 1024 * 1024, 128));
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
        tokio::time::timeout(
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
        .await??;
        Ok(())
    }
}
