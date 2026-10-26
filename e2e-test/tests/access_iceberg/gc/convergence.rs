// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::seed::{seed_expired_backlog, seed_table, TestGcBacklog};
use crate::access_iceberg::{frontend::TestFrontend, native::TestNativeStorage, now_ms};
use crowdb_access_iceberg::{
    catalog::{CatalogContext, CatalogStore, RoutedCatalogStore},
    file::{canonical_file_key, FileRecord},
    gc::{CandidatePhase, GcLimits, GcPhase, GcRepository, GcStalledReason, GcTask, GcTaskKind},
    key::{CatalogScope, IcebergKey, OperationId, TableId},
    record::StorageRecord,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

pub(in crate::access_iceberg::gc) mod pressure;
use pressure::TestGcPressure;

struct TestGcConvergence {
    store: Arc<RoutedCatalogStore>,
    context: CatalogContext,
    table: TableId,
    backlog: TestGcBacklog,
    started: Instant,
    task_gets: u64,
    verification_gets: u64,
    observations: Vec<serde_json::Value>,
    last: Option<GcTask>,
    pressure: Option<TestGcPressure>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release GC worker and pinned PyIceberg environment"]
async fn expired_inline_backlog_completes_with_foreground_progress() {
    let stack = TestNativeStorage::new();
    let (store, context, table) = seed_table(&stack).await;
    let backlog = seed_expired_backlog(store.as_ref(), context, table, 128).await;
    assert_eq!(backlog.files.len(), 128);
    assert!(backlog.files.len() > usize::from(GcLimits::default().page_items));
    assert!(backlog.dropped_ms + GcTask::PURGE_DELAY_MS < now_ms());
    let server = TestFrontend::start(12).await;
    let mut check = TestGcConvergence {
        store,
        context,
        table,
        backlog,
        started: Instant::now(),
        task_gets: 0,
        verification_gets: 0,
        observations: Vec::new(),
        last: None,
        pressure: None,
    };
    check.report("seeded");
    let result =
        tokio::time::timeout(Duration::from_secs(3300), check.run(&server.origin, &stack.seeds)).await;
    let result = result.unwrap_or_else(|_| Err("inline GC convergence exceeded 3300 seconds".into()));
    if let Err(error) = result {
        check.report("failed");
        if tokio::time::timeout(Duration::from_secs(10), check.diagnose())
            .await
            .is_err()
        {
            eprintln!("gc_inline_convergence remaining-file diagnostic exceeded 10 seconds");
        }
        if let Some(pressure) = &mut check.pressure {
            pressure.stop().await;
        }
        server.stop().await;
        panic!("inline GC convergence failed: {error}");
    }
    check.report("complete");
    server.stop().await;
}

impl TestGcConvergence {
    async fn run(&mut self, origin: &str, seeds: &[String]) -> Result<(), String> {
        let identity = OperationId::from_bytes(self.table.as_bytes()).map_err(|error| error.to_string())?;
        let repository = GcRepository::new(self.store.clone());
        loop {
            self.task_gets += 1;
            if let Some(task) = repository
                .task(self.context.catalog, identity)
                .await
                .map_err(|error| error.to_string())?
            {
                self.observe(&task)?;
                if let Some(pressure) = &mut self.pressure {
                    pressure.observe(task.deleted).await?;
                } else if task.deleted > 0 {
                    if task.phase == GcPhase::Complete {
                        return Err("GC completed before the foreground overlap could be started".into());
                    }
                    println!(
                        "gc_inline_foreground_start {}",
                        serde_json::json!({
                            "elapsed_ms":self.started.elapsed().as_millis(),"deleted":task.deleted,
                            "revision":task.revision,"phase":format!("{:?}",task.phase),
                        })
                    );
                    self.pressure = Some(TestGcPressure::start(origin, seeds.to_vec())?);
                }
                if task.phase == GcPhase::Complete
                    && self.pressure.as_ref().is_some_and(TestGcPressure::complete)
                {
                    self.verify_complete(&task).await?;
                    return self
                        .pressure
                        .take()
                        .ok_or("foreground pressure is missing")?
                        .finish();
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    fn observe(&mut self, task: &GcTask) -> Result<(), String> {
        if self
            .last
            .as_ref()
            .map_or(true, |previous| previous.revision != task.revision)
        {
            self.observations.push(serde_json::json!({
                "elapsed_ms":self.started.elapsed().as_millis(),"revision":task.revision,
                "phase":format!("{:?}",task.phase),"deleted":task.deleted,
                "reclaimed_bytes":task.reclaimed_bytes,"retry_at_ms":task.retry_at_ms,
                "stalled":format!("{:?}",task.stalled),
            }));
        }
        self.last = Some(task.clone());
        if task.context != self.context
            || task.kind != GcTaskKind::PurgeTable
            || task.head.as_ref().map_or(true, |head| head.table != self.table)
            || task.created_ms != self.backlog.dropped_ms
            || task.not_before_ms != self.backlog.dropped_ms + GcTask::PURGE_DELAY_MS
        {
            return Err(format!(
                "GC changed the expired resource identity or fixed deadline: {task:?}"
            ));
        }
        if task.phase == GcPhase::Quarantined || task.paused {
            return Err(format!("GC stopped before convergence: {task:?}"));
        }
        Ok(())
    }

    async fn verify_complete(&mut self, task: &GcTask) -> Result<(), String> {
        if task.deleted != self.backlog.files.len() as u64
            || task.reclaimed_bytes != self.backlog.logical_bytes
            || task.deferred_ranges
            || task.stalled != GcStalledReason::None
            || task.retry_at_ms != 0
            || task.fenced
        {
            return Err(format!(
                "terminal task did not discharge the exact inline backlog: {task:?}"
            ));
        }
        let generation = task
            .head
            .as_ref()
            .ok_or("purge task lost its table head")?
            .generation;
        for file in &self.backlog.files {
            self.verification_gets += 1;
            let key = canonical_file_key(&file.location)
                .encode()
                .map_err(|error| error.to_string())?;
            if self
                .store
                .get(&key)
                .await
                .map_err(|error| error.to_string())?
                .is_some()
            {
                return Err(format!("completed task left canonical file {}", file.file));
            }
            self.verification_gets += 1;
            self.verify_candidate(file, generation, task).await?;
        }
        Ok(())
    }

    async fn verify_candidate(
        &self,
        file: &FileRecord,
        generation: u64,
        task: &GcTask,
    ) -> Result<(), String> {
        let mut suffix = self.table.as_bytes().to_vec();
        suffix.extend_from_slice(&generation.to_be_bytes());
        suffix.extend_from_slice(file.file.as_bytes());
        let key = IcebergKey::Catalog {
            catalog: self.context.catalog,
            scope: CatalogScope::GcCandidate,
            suffix,
        };
        let value = self
            .store
            .get(&key.encode().map_err(|error| error.to_string())?)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("missing candidate for file {}", file.file))?;
        let StorageRecord::GcCandidate(candidate) =
            StorageRecord::decode(&key, &value.bytes).map_err(|error| error.to_string())?
        else {
            return Err(format!("invalid candidate record for file {}", file.file));
        };
        if candidate.file != *file
            || candidate.task != task.identity
            || candidate.generation != generation
            || candidate.phase != CandidatePhase::Complete
            || candidate.completed_round == 0
            || candidate.completed_round > task.sweep_round
        {
            return Err(format!("inline candidate did not finish: {candidate:?}"));
        }
        Ok(())
    }

    async fn diagnose(&self) {
        let mut remaining = Vec::new();
        let mut errors = Vec::new();
        for file in &self.backlog.files {
            let key = canonical_file_key(&file.location).encode().unwrap();
            match self.store.get(&key).await {
                Ok(Some(_)) => remaining.push(file.file.to_string()),
                Ok(None) => {}
                Err(error) => errors.push(format!("{}: {error}", file.file)),
            }
        }
        eprintln!(
            "gc_inline_convergence_remaining {}",
            serde_json::json!({
                "remaining_count":remaining.len(),"file_ids":remaining,"read_errors":errors,
                "diagnostic_gets":self.backlog.files.len(),"last_task":format!("{:?}",self.last),
            })
        );
    }

    fn report(&self, outcome: &str) {
        println!(
            "gc_inline_convergence {}",
            serde_json::json!({
                "outcome":outcome,"source_files":self.backlog.files.len(),
                "catalog_id":self.context.catalog.to_string(),"table_id":self.table.to_string(),
                "task_id":OperationId::from_bytes(self.table.as_bytes()).unwrap().to_string(),
                "source_logical_bytes":self.backlog.logical_bytes,"dropped_ms":self.backlog.dropped_ms,
                "not_before_ms":self.backlog.dropped_ms+GcTask::PURGE_DELAY_MS,
                "elapsed_ms":self.started.elapsed().as_millis(),"observer_task_gets":self.task_gets,
                "verification_gets":self.verification_gets,"observations":self.observations,
                "last_task":format!("{:?}",self.last),
                "page_items":GcLimits::default().page_items,"idle_interval_ms":1_800_000,
                "scope":"exact inline metadata cleanup and bounded foreground overlap; no chunk range notification or physical capacity claim; no isolated latency baseline",
            })
        );
    }
}
