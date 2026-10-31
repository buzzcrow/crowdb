// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod records;
use super::large::TestLargeRange;
use crate::{
    access_iceberg::{
        frontend::TestFrontend,
        gc::{convergence::pressure::TestGcPressure, seed::seed_table},
        native::TestNativeStorage,
        now_ms,
    },
    common::control,
};
use crowdb_access_iceberg::gc::{GcPhase, GcTask};
use futures::FutureExt;
use records::TestMatrixRecords;
use std::time::{Duration, Instant};

struct TestMatrix {
    records: TestMatrixRecords,
    large: TestLargeRange,
    servers: Vec<TestFrontend>,
    pressure: Option<TestGcPressure>,
    started: Instant,
    observations: Vec<serde_json::Value>,
    last_revision: Option<(u64, u64)>,
    restarts: u8,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires two owned release GC workers, PyIceberg, and real sealed ChunkDB ranges"]
async fn restarted_cleaners_finish_large_and_sparse_backlogs_beside_an_invalid_catalog() {
    let stack = TestNativeStorage::new();
    let (store, context, table) = seed_table(&stack).await;
    let large = TestLargeRange::write(&stack, store.clone(), context, table).await;
    let records = TestMatrixRecords::seed(store, context, table, large.file.clone()).await;
    let mut check = TestMatrix {
        records,
        large,
        servers: Vec::new(),
        pressure: None,
        started: Instant::now(),
        observations: Vec::new(),
        last_revision: None,
        restarts: 0,
    };
    let result = tokio::time::timeout(
        Duration::from_secs(720),
        std::panic::AssertUnwindSafe(check.run(&stack.seeds)).catch_unwind(),
    )
    .await;
    let result = match result {
        Ok(Ok(result)) => result,
        Ok(Err(payload)) => Err(payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                payload
                    .downcast_ref::<&str>()
                    .map(|message| (*message).to_owned())
            })
            .unwrap_or_else(|| "GC matrix panicked without a string payload".into())),
        Err(_) => Err("GC restart matrix exceeded 720 seconds".into()),
    };
    if let (Err(_), Some(pressure)) = (&result, &mut check.pressure) {
        pressure.stop().await;
    }
    for node in [12, 13] {
        control::node("stop", node).await;
    }
    check.servers.clear();
    println!(
        "gc_restart_matrix {}",
        serde_json::json!({
            "outcome":if result.is_ok() { "complete" } else { "failed" },"error":result.as_ref().err(),
            "elapsed_ms":check.started.elapsed().as_millis(),"catalog_id":context.catalog.to_string(),"table_id":table.to_string(),
            "planned_restarts":3,"completed_restarts":check.restarts,"configured_cleaners":[12,13],"source_files":check.records.files.len(),"observations":check.observations,
            "page_items":1,"idle_interval_ms":1_800_000,"worker_concurrency":1,
            "scope":"bounded real cleanup and frontend overlap, no isolated latency baseline or physical capacity claim",
        })
    );
    if let Err(error) = result {
        panic!("GC restart matrix failed: {error}");
    }
}

impl TestMatrix {
    async fn start(&mut self) -> Instant {
        self.servers.push(TestFrontend::start(12).await);
        self.servers.push(TestFrontend::start(13).await);
        Instant::now()
    }
    async fn run(&mut self, seeds: &[String]) -> Result<(), String> {
        let mut uninterrupted = self.start().await;
        let mut longest = 0;
        for round in 1..=3 {
            let revision = self.records.task().await?.map_or(0, |task| task.revision);
            let task = self.wait_revision(revision).await?;
            if task.phase == GcPhase::Complete {
                return Err("purge finished before all three restart boundaries".into());
            }
            longest = longest.max(uninterrupted.elapsed().as_millis());
            let boundary = serde_json::json!({"event":"kill_both","round":round,"elapsed_ms":self.started.elapsed().as_millis(),"unix_ms":now_ms(),"revision":task.revision,"phase":format!("{:?}",task.phase),"deleted":task.deleted,"created_ms":task.created_ms,"not_before_ms":task.not_before_ms});
            println!("gc_matrix_restart {boundary}");
            self.observations.push(boundary);
            // No pressure is active while either HTTP endpoint is intentionally killed.
            for node in [12, 13] {
                control::node("kill", node).await;
            }
            self.servers.clear();
            uninterrupted = self.start().await;
            self.restarts += 1;
        }
        let drain = uninterrupted;
        loop {
            let task = self
                .records
                .task()
                .await?
                .ok_or("purge task disappeared after restart")?;
            self.records.validate(&task)?;
            self.records.observe_sparse().await?;
            let multipart = self.records.multipart().await?;
            let deleted = task.deleted;
            self.observe(&task, multipart.as_ref());
            if let Some(pressure) = &mut self.pressure {
                pressure.observe(deleted, task.phase == GcPhase::Complete).await?;
            } else if deleted > 0 {
                let remaining = self.records.remaining().await?;
                if task.phase == GcPhase::Complete || remaining == 0 {
                    return Err("GC left no candidates for actual foreground overlap".into());
                }
                println!(
                    "gc_matrix_foreground_start {}",
                    serde_json::json!({"elapsed_ms":self.started.elapsed().as_millis(),"deleted":deleted,"remaining_canonical":remaining,"revision":task.revision,"phase":format!("{:?}",task.phase)})
                );
                self.pressure = Some(TestGcPressure::start(&self.servers[0].origin, seeds.to_vec())?);
            }
            if task.phase == GcPhase::Complete
                && self.records.sparse_complete().await?
                && self.pressure.as_ref().is_some_and(TestGcPressure::complete)
            {
                let records = self.records.verify(&task).await?;
                let range = self.large.verify().await?;
                self.pressure
                    .take()
                    .ok_or("foreground pressure missing")?
                    .finish()?;
                println!(
                    "gc_matrix_terminal_proof {}",
                    serde_json::json!({"final_continuous_drain_ms":drain.elapsed().as_millis(),"longest_observed_both_cleaners_window_ms":longest.max(drain.elapsed().as_millis()),"task_id":task.identity.to_string(),"revision":task.revision,"deleted":task.deleted,"logical_bytes":task.reclaimed_bytes,"records":records,"large_range":range})
                );
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    async fn wait_revision(&mut self, revision: u64) -> Result<GcTask, String> {
        loop {
            self.records.observe_sparse().await?;
            if let Some(task) = self.records.task().await? {
                self.records.validate(&task)?;
                if task.revision > revision {
                    return Ok(task);
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    fn observe(&mut self, task: &GcTask, multipart: Option<&GcTask>) {
        let revisions = (task.revision, multipart.map_or(0, |task| task.revision));
        if self.last_revision == Some(revisions) {
            return;
        }
        self.last_revision = Some(revisions);
        let value = serde_json::json!({
            "event":"drain","elapsed_ms":self.started.elapsed().as_millis(),"unix_ms":now_ms(),
            "task_id":task.identity.to_string(),"revision":task.revision,"phase":format!("{:?}",task.phase),"deleted":task.deleted,"created_ms":task.created_ms,"not_before_ms":task.not_before_ms,"retry_at_ms":task.retry_at_ms,"stalled":format!("{:?}",task.stalled),
            "multipart":multipart.map(|task| serde_json::json!({"task_id":task.identity.to_string(),"revision":task.revision,"phase":format!("{:?}",task.phase),"deleted":task.deleted,"retry_at_ms":task.retry_at_ms,"stalled":format!("{:?}",task.stalled)})),
        });
        println!("gc_matrix_progress {value}");
        self.observations.push(value);
    }
}
