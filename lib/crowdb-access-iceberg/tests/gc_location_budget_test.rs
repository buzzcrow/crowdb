// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::{
    file::{FileBlockStore, FileIoError},
    gc::{BudgetedGcBlocks, GcIoBudget},
};
use crowdb_chunk_client::{IoError, ReclaimOutcome};

#[path = "common/gc_location_budget.rs"]
mod blocks;
use blocks::TestRangeBlocks;

#[tokio::test]
async fn a_large_range_notifies_without_reading_or_spending_the_payload_byte_budget() {
    let budget = Arc::new(GcIoBudget::new(1024, 8, 8 * 1024 * 1024, 1));
    let mut location = TestRangeBlocks::location();
    location.length = 128 * 1024 * 1024;
    location.logical_length = location.length - 34;
    let inner = Arc::new(TestRangeBlocks::new(location.clone(), TestRangeBlocks::root()));
    let blocks = BudgetedGcBlocks::new(inner.clone(), budget);
    assert_eq!(
        blocks.reclaim_location(&location).await.unwrap(),
        ReclaimOutcome::Accepted
    );
    assert_eq!(inner.calls.load(Ordering::Relaxed), 1);
    assert_eq!(inner.reads.load(Ordering::Relaxed), 0);
    assert!(matches!(
        blocks.reclaim_location(&location).await,
        Err(FileIoError::Bounds)
    ));
    assert_eq!(inner.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn a_large_tree_root_notifies_without_reading_its_referenced_payload() {
    let budget = Arc::new(GcIoBudget::new(1024, 8, 8 * 1024 * 1024, 1));
    let mut root = TestRangeBlocks::root();
    root.physical_length = 128 * 1024 * 1024;
    root.logical_length = root.physical_length - 34;
    let inner = Arc::new(TestRangeBlocks::new(TestRangeBlocks::location(), root.clone()));
    let blocks = BudgetedGcBlocks::new(inner.clone(), budget.clone());
    assert_eq!(blocks.reclaim(&root).await.unwrap(), ReclaimOutcome::Accepted);
    assert_eq!(inner.roots.load(Ordering::Relaxed), 1);
    assert_eq!(inner.reads.load(Ordering::Relaxed), 0);
    assert!(matches!(blocks.reclaim(&root).await, Err(FileIoError::Bounds)));
    assert_eq!(inner.roots.load(Ordering::Relaxed), 1);
    budget.reset();
    inner.result.store(2, Ordering::Relaxed);
    assert!(matches!(blocks.reclaim(&root).await,
        Err(FileIoError::Write(IoError::Internal(message))) if message == "lost range notification reply"));
    assert_eq!(
        inner.roots.load(Ordering::Relaxed),
        2,
        "unknown must not retry or read"
    );
    assert_eq!(inner.reads.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn budgeted_location_reclamation_dispatches_once_and_preserves_the_inner_outcome() {
    let budget = Arc::new(GcIoBudget::new(1024, 8, 16, 1));
    let inner = Arc::new(TestRangeBlocks::default());
    let blocks = BudgetedGcBlocks::new(inner.clone(), budget.clone());
    let location = TestRangeBlocks::location();

    assert_eq!(
        blocks.reclaim_location(&location).await.unwrap(),
        ReclaimOutcome::Accepted
    );
    assert_eq!(inner.calls.load(Ordering::Relaxed), 1);
    budget.reset();
    inner.result.store(1, Ordering::Relaxed);
    assert_eq!(
        blocks.reclaim_location(&location).await.unwrap(),
        ReclaimOutcome::Deferred
    );
    assert_eq!(inner.calls.load(Ordering::Relaxed), 2);
    budget.reset();
    inner.result.store(2, Ordering::Relaxed);
    assert!(matches!(
        blocks.reclaim_location(&location).await,
        Err(FileIoError::Write(IoError::Internal(message))) if message == "lost range notification reply"
    ));
    assert_eq!(
        inner.calls.load(Ordering::Relaxed),
        3,
        "no hidden retry or confirmation call"
    );
}

#[tokio::test]
async fn location_notification_keeps_the_payload_byte_budget_available_for_reads() {
    let budget = Arc::new(GcIoBudget::new(1024, 8, 15, 3));
    let inner = Arc::new(TestRangeBlocks::default());
    let blocks = BudgetedGcBlocks::new(inner.clone(), budget);
    let location = TestRangeBlocks::location();
    assert_eq!(location.logical_length, 7);
    assert_eq!(location.length, 16);
    assert_eq!(
        blocks.reclaim_location(&location).await.unwrap(),
        ReclaimOutcome::Accepted
    );
    assert_eq!(blocks.read(&TestRangeBlocks::root()).await.unwrap().len(), 8);
    assert!(matches!(
        blocks.read(&TestRangeBlocks::root()).await,
        Err(FileIoError::Bounds)
    ));
    assert_eq!(inner.calls.load(Ordering::Relaxed), 1);
    assert_eq!(inner.reads.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn location_notification_shares_chunk_request_admission_and_resets_per_step() {
    let budget = Arc::new(GcIoBudget::new(1024, 8, 24, 2));
    let inner = Arc::new(TestRangeBlocks::default());
    let blocks = BudgetedGcBlocks::new(inner.clone(), budget.clone());
    let root = TestRangeBlocks::root();
    let location = TestRangeBlocks::location();
    assert_eq!(blocks.read(&root).await.unwrap().len(), 8);
    assert_eq!(
        blocks.reclaim_location(&location).await.unwrap(),
        ReclaimOutcome::Accepted
    );
    assert!(matches!(
        blocks.reclaim_location(&location).await,
        Err(FileIoError::Bounds)
    ));
    assert_eq!(inner.calls.load(Ordering::Relaxed), 1);
    assert_eq!(inner.reads.load(Ordering::Relaxed), 1);
    budget.reset();
    assert_eq!(
        blocks.reclaim_location(&location).await.unwrap(),
        ReclaimOutcome::Accepted
    );
    assert_eq!(inner.calls.load(Ordering::Relaxed), 2);

    let request_budget = Arc::new(GcIoBudget::new(1024, 8, 1024, 1));
    let blocks = BudgetedGcBlocks::new(inner.clone(), request_budget);
    assert_eq!(blocks.read(&root).await.unwrap().len(), 8);
    assert!(matches!(
        blocks.reclaim_location(&location).await,
        Err(FileIoError::Bounds)
    ));
    assert_eq!(inner.calls.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn an_exhausted_payload_byte_budget_still_allows_one_bounded_notification() {
    let budget = Arc::new(GcIoBudget::new(1024, 8, 8, 3));
    let inner = Arc::new(TestRangeBlocks::default());
    let blocks = BudgetedGcBlocks::new(inner.clone(), budget);
    assert_eq!(blocks.read(&TestRangeBlocks::root()).await.unwrap().len(), 8);
    assert_eq!(
        blocks
            .reclaim_location(&TestRangeBlocks::location())
            .await
            .unwrap(),
        ReclaimOutcome::Accepted
    );
    assert!(matches!(
        blocks.read(&TestRangeBlocks::root()).await,
        Err(FileIoError::Bounds)
    ));
    assert_eq!(inner.reads.load(Ordering::Relaxed), 1);
    assert_eq!(inner.calls.load(Ordering::Relaxed), 1);
}
