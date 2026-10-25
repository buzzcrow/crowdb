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
async fn location_byte_budget_uses_the_notified_range_and_rejects_before_dispatch() {
    let budget = Arc::new(GcIoBudget::new(1024, 8, 15, 2));
    let inner = Arc::new(TestRangeBlocks::default());
    let blocks = BudgetedGcBlocks::new(inner.clone(), budget);
    let location = TestRangeBlocks::location();
    assert_eq!(location.logical_length, 7);
    assert_eq!(location.length, 16);
    assert!(matches!(
        blocks.reclaim_location(&location).await,
        Err(FileIoError::Bounds)
    ));
    assert_eq!(inner.calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn location_notification_shares_chunk_request_and_byte_budgets_and_resets_per_step() {
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
