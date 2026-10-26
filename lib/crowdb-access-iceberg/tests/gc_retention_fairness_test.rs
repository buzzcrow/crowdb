// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_iceberg::{
    catalog::CatalogStore,
    file::canonical_file_key,
    gc::{CandidatePhase, GcLimits, GcRepository, GcWorker},
};
use std::sync::Arc;

#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/gc_store.rs"]
mod gc_store;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/gc_retention_fairness.rs"]
mod retention;
use retention::{TestRetention, NOW};

#[tokio::test]
async fn a_low_key_future_candidate_cannot_block_an_already_expired_later_file() {
    let test = TestRetention::new().await;
    let worker = GcWorker::new(
        GcRepository::new(test.fixture.store.clone()),
        Arc::new(blocks::TestBlocks::default()),
        GcLimits::default(),
    )
    .unwrap();
    let mut task = test.until_sweep(&worker).await;
    let future = test.candidate(&test.future).await;
    let expired = test.candidate(&test.expired).await;
    assert_eq!(
        future.not_before_ms,
        NOW + crowdb_access_iceberg::gc::GcTask::PURGE_DELAY_MS
    );
    assert_eq!(expired.not_before_ms, NOW);
    for _ in 0..32 {
        // Fixed time is essential: never jump to retry_at_ms and silently expire the blocker.
        task = worker.step(&task, NOW).await.unwrap();
        if test.candidate(&test.expired).await.phase == CandidatePhase::Complete {
            break;
        }
    }
    assert_eq!(
        test.candidate(&test.expired).await.phase,
        CandidatePhase::Complete,
        "eligible later resource stayed blocked: {task:?}"
    );
    assert_eq!(test.candidate(&test.future).await, future);
    assert!(test
        .fixture
        .store
        .get(&canonical_file_key(&test.expired.location).encode().unwrap())
        .await
        .unwrap()
        .is_none());
    assert!(test
        .fixture
        .store
        .get(&canonical_file_key(&test.future.location).encode().unwrap())
        .await
        .unwrap()
        .is_some());
    assert!(NOW < future.not_before_ms);
}

#[tokio::test]
async fn periodic_multipart_reopening_keeps_the_fixed_deadline_and_counts_each_file_once() {
    let test = TestRetention::new().await;
    let first = finish_at(&test, test.task.clone(), NOW).await;
    assert_eq!(first.deleted, 1, "only the expired resource can finish");
    assert_eq!(first.reclaimed_bytes, test.expired.length);
    let future = test.candidate(&test.future).await;
    assert_eq!(future.phase, CandidatePhase::Retained);
    let repository = GcRepository::new(test.fixture.store.clone());
    let before_due = future.not_before_ms - 1;
    let next = repository
        .admit_multipart(test.fixture.context, before_due, GcLimits::default())
        .await
        .unwrap();
    assert!(next.revision > first.revision);
    let second = finish_at(&test, next, before_due).await;
    assert_eq!(
        second.deleted, 1,
        "reopened rounds must not account old completions again"
    );
    assert_eq!(second.reclaimed_bytes, test.expired.length);
    assert_eq!(test.candidate(&test.future).await, future);
    let repository = GcRepository::new(test.fixture.store.clone());
    let next = repository
        .admit_multipart(test.fixture.context, future.not_before_ms, GcLimits::default())
        .await
        .unwrap();
    let final_task = finish_at(&test, next, future.not_before_ms).await;
    assert_eq!(final_task.deleted, 2);
    assert_eq!(
        final_task.reclaimed_bytes,
        test.expired.length + test.future.length
    );
    let completed = test.candidate(&test.future).await;
    assert_eq!(completed.phase, CandidatePhase::Complete);
    assert_eq!(completed.not_before_ms, future.not_before_ms);
    assert_eq!(completed.first_seen_ms, future.first_seen_ms);
    assert_eq!(completed.file, future.file);
    assert!(completed.completed_round <= final_task.sweep_round);
}

async fn finish_at(
    test: &TestRetention,
    mut task: crowdb_access_iceberg::gc::GcTask,
    now_ms: u64,
) -> crowdb_access_iceberg::gc::GcTask {
    for _ in 0..128 {
        if task.phase == crowdb_access_iceberg::gc::GcPhase::Complete {
            return task;
        }
        // Every step uses a fresh worker; only the durable resource facts survive.
        let worker = GcWorker::new(
            GcRepository::new(test.fixture.store.clone()),
            Arc::new(blocks::TestBlocks::default()),
            GcLimits::default(),
        )
        .unwrap();
        task = worker.step(&task, now_ms).await.unwrap();
    }
    panic!("multipart eligibility pass did not finish at fixed time: {task:?}");
}
