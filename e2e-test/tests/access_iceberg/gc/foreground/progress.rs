// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{super::seed::seed_purge_marker, pressure::TestSdkPressure};
use crate::access_iceberg::frontend::TestFrontend;
use crowdb_access_iceberg::{
    catalog::{CatalogContext, RoutedCatalogStore},
    gc::{GcRepository, GcTask},
    key::{OperationId, TableId},
};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

pub(super) struct TestGcProgress {
    store: Arc<RoutedCatalogStore>,
    context: CatalogContext,
    table: TableId,
    worker: Option<TestFrontend>,
    pub started: Instant,
    pub revisions: Vec<(u128, u64, String)>,
    pub baseline: Option<(u64, usize, usize)>,
    pub overlapped: bool,
}

impl TestGcProgress {
    pub(super) fn new(store: Arc<RoutedCatalogStore>, context: CatalogContext, table: TableId) -> Self {
        Self {
            store,
            context,
            table,
            worker: None,
            started: Instant::now(),
            revisions: Vec::new(),
            baseline: None,
            overlapped: false,
        }
    }

    fn repository(&self) -> GcRepository {
        GcRepository::new(self.store.clone())
    }

    fn identity(&self) -> OperationId {
        OperationId::from_bytes(self.table.as_bytes()).unwrap()
    }

    pub(super) async fn observe(&mut self, sdk: &TestSdkPressure, writes: &AtomicUsize, stop: &AtomicBool) {
        if self.worker.is_none() && sdk.ready() && writes.load(Ordering::Acquire) > 0 {
            // The SDK frontend is already warm. Start the other ordinary
            // frontend with a backlog, before releasing table lifecycle work.
            seed_purge_marker(self.store.as_ref(), self.context, self.table).await;
            self.worker = Some(TestFrontend::start(12).await);
        }
        if self.worker.is_none() {
            return;
        }
        if let Some(task) = self
            .repository()
            .task(self.context.catalog, self.identity())
            .await
            .unwrap()
        {
            self.record(&task, sdk, writes, stop);
        }
    }

    fn record(&mut self, task: &GcTask, sdk: &TestSdkPressure, writes: &AtomicUsize, stop: &AtomicBool) {
        if self.revisions.is_empty() {
            sdk.release();
        }
        if self
            .revisions
            .last()
            .map_or(true, |(_, revision, _)| *revision != task.revision)
        {
            self.revisions.push((
                self.started.elapsed().as_millis(),
                task.revision,
                format!("{:?}", task.phase),
            ));
        }
        let sdk_cycles = sdk.progress();
        let writes = writes.load(Ordering::Acquire);
        if let Some((revision, before_sdk, before_writes)) = self.baseline {
            if task.revision > revision && sdk_cycles > before_sdk && writes > before_writes {
                self.overlapped = true;
                sdk.stop();
                stop.store(true, Ordering::Release);
            }
        } else if sdk_cycles > 0 && writes > 0 {
            self.baseline = Some((task.revision, sdk_cycles, writes));
        }
    }

    pub(super) async fn continued(&self) -> GcTask {
        let first_revision = self.revisions.first().expect("GC task was never observed").1;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let current = self
                    .repository()
                    .task(self.context.catalog, self.identity())
                    .await
                    .unwrap()
                    .unwrap();
                if current.revision >= first_revision + 2 {
                    break current;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("backlogged GC did not continue beyond its first scheduled step")
    }
}
