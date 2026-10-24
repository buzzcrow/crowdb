// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_iceberg::{
    catalog::CatalogStore,
    gc::{GcLimits, GcRepository},
    key::{CatalogId, CatalogScope, IcebergKey, OperationId, SystemScope},
    operation::{ledger_key, mutation_identity},
};
use crowdb_access_server::iceberg::TestGcSweep;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

#[path = "common/iceberg_gc_scheduler.rs"]
mod fixture;
use fixture::{advance, assert_progress, purge, purge_task_key, put, scheduler, setup, TestBlocks};

#[tokio::test]
async fn corrupt_task_row_cannot_block_a_later_healthy_task_forever() {
    let (store, context, task) = setup().await;
    let poison = IcebergKey::Catalog {
        catalog: context.catalog,
        scope: CatalogScope::GcTask,
        suffix: vec![1; 16],
    }
    .encode()
    .unwrap();
    store
        .compare_exchange(
            &poison,
            None,
            b"broken",
            mutation_identity(&poison, None, b"broken"),
        )
        .await
        .unwrap();
    let mut sweep = TestGcSweep::new(
        store.clone(),
        Arc::new(TestBlocks),
        context.catalog,
        Some(context),
        GcLimits::default(),
    )
    .unwrap();
    for _ in 0..8 {
        if let Err(error) = sweep.advance_for_tests().await {
            eprintln!("scheduler retained error: {error}");
        }
    }
    let current = GcRepository::new(store.clone())
        .task(context.catalog, task.identity)
        .await
        .unwrap()
        .unwrap();
    assert!(
        current.revision > task.revision,
        "healthy task never advanced past poison: {current:?}"
    );
    assert!(store.get(&poison).await.unwrap().is_some());
}

#[tokio::test]
async fn malformed_long_raw_key_is_retained_and_skipped_by_local_position() {
    let (store, context, task) = setup().await;
    let mut poison = IcebergKey::catalog_range(context.catalog).start;
    poison.push(CatalogScope::GcTask as u8);
    poison.resize(4097, 1);
    put(store.as_ref(), &poison, b"bad-key").await;
    let mut sweep = scheduler(store.clone(), context);
    sweep.advance_for_tests().await.unwrap_err();
    assert_eq!(sweep.position_for_tests(3), poison);
    advance(&mut sweep, 5).await;
    assert_progress(store.clone(), &task).await;
    assert!(store.get(&poison).await.unwrap().is_some());
}

#[tokio::test]
async fn purge_and_retired_poison_do_not_block_task_or_later_purge() {
    let (store, context, task) = setup().await;
    let bad = purge(store.as_ref(), context, 1).await;
    let poison = bad.key().encode().unwrap();
    let original = store.get(&poison).await.unwrap().unwrap();
    store
        .compare_exchange(
            &poison,
            Some(&original.bytes),
            b"broken",
            mutation_identity(&poison, Some(&original.bytes), b"broken"),
        )
        .await
        .unwrap();
    let healthy = purge(store.as_ref(), context, 2).await;
    let ledger = ledger_key(SystemScope::ManagementOperation, OperationId::random())
        .unwrap()
        .encode()
        .unwrap();
    put(store.as_ref(), &ledger, b"bad-ledger").await;
    let mut sweep = scheduler(store.clone(), context);
    assert!(sweep.advance_for_tests().await.is_err());
    assert_eq!(sweep.position_for_tests(0), poison);
    assert_eq!(sweep.position_for_tests(2), ledger);
    assert_progress(store.clone(), &task).await;
    advance(&mut sweep, 4).await;
    assert!(store.get(&purge_task_key(&healthy)).await.unwrap().is_some());
    assert_eq!(store.get(&poison).await.unwrap().unwrap().bytes, b"broken");
    assert_eq!(store.get(&ledger).await.unwrap().unwrap().bytes, b"bad-ledger");
}

#[tokio::test]
async fn failed_action_keeps_position_retries_next_round_and_restart_rediscovers() {
    let (store, context, task) = setup().await;
    let marker = purge(store.as_ref(), context, 3).await;
    let marker_key = marker.key().encode().unwrap();
    store.fault_at(marker_key.clone());
    store.read_failure.store(1, Ordering::SeqCst);
    let mut sweep = scheduler(store.clone(), context);
    sweep.advance_for_tests().await.unwrap_err();
    assert_eq!(sweep.position_for_tests(0), marker_key);
    assert!(store.get(&purge_task_key(&marker)).await.unwrap().is_none());
    assert_progress(store.clone(), &task).await;
    advance(&mut sweep, 3).await;
    assert!(store.get(&purge_task_key(&marker)).await.unwrap().is_some());
    // A new worker deliberately loses all hints, then recovers solely from durable marker/head.
    let mut restarted = scheduler(store.clone(), context);
    advance(&mut restarted, 2).await;
    assert!(store.get(&purge_task_key(&marker)).await.unwrap().is_some());
    advance(&mut sweep, 4).await;
    assert!(store.get(&marker_key).await.unwrap().is_some());
}

#[tokio::test]
async fn unknown_applied_admission_keeps_fact_and_healthy_lanes_continue() {
    let (store, context, task) = setup().await;
    let marker = purge(store.as_ref(), context, 4).await;
    store.fault_at(purge_task_key(&marker));
    store.write_failure.store(2, Ordering::SeqCst);
    let mut sweep = scheduler(store.clone(), context);
    advance(&mut sweep, 6).await;
    assert!(store.get(&purge_task_key(&marker)).await.unwrap().is_some());
    assert!(store
        .get(&marker.key().encode().unwrap())
        .await
        .unwrap()
        .is_some());
    assert_progress(store.clone(), &task).await;
}

#[tokio::test]
async fn cancellation_during_action_retains_observed_key_and_rotates_lanes() {
    let (store, context, task) = setup().await;
    let marker = purge(store.as_ref(), context, 5).await;
    store.fault_at(purge_task_key(&marker));
    store.pause_write.store(true, Ordering::SeqCst);
    let mut sweep = scheduler(store.clone(), context);
    {
        let window = sweep.advance_for_tests();
        tokio::pin!(window);
        tokio::select! {
            result = &mut window => panic!("action did not pause: {result:?}"),
            () = store.entered.notified() => {},
            () = tokio::time::sleep(Duration::from_secs(3)) => panic!("action not reached"),
        }
    }
    assert_eq!(sweep.position_for_tests(0), marker.key().encode().unwrap());
    // It stays paused: the next rotating window must still run the healthy task lane first.
    advance(&mut sweep, 1).await;
    assert_progress(store.clone(), &task).await;
    store.pause_write.store(false, Ordering::SeqCst);
    advance(&mut sweep, 4).await;
    assert!(store.get(&purge_task_key(&marker)).await.unwrap().is_some());
}

#[tokio::test]
async fn timeout_in_first_lane_does_not_starve_other_lanes() {
    let (store, context, task) = setup().await;
    store
        .pause_scope
        .store(CatalogScope::Reclamation as usize, Ordering::SeqCst);
    let limits = GcLimits {
        step_ms: 1,
        ..GcLimits::default()
    };
    let mut sweep = TestGcSweep::new(
        store.clone(),
        Arc::new(TestBlocks),
        context.catalog,
        Some(context),
        limits,
    )
    .unwrap();
    let error = sweep.advance_for_tests().await.unwrap_err();
    eprintln!("expected scheduler deadline: {error}");
    assert!(sweep.position_for_tests(0).is_empty());
    assert!(sweep.advance_for_tests().await.is_err());
    assert_progress(store.clone(), &task).await;
}

#[tokio::test]
async fn unvalidated_page_never_advances_and_another_catalog_is_independent() {
    let (store, context, task) = setup().await;
    store.invalid_page.store(true, Ordering::SeqCst);
    let mut sweep = scheduler(store.clone(), context);
    sweep.advance_for_tests().await.unwrap_err();
    assert!(sweep.position_for_tests(3).is_empty());
    let poisoned_catalog = CatalogId::random();
    let poison = IcebergKey::Catalog {
        catalog: poisoned_catalog,
        scope: CatalogScope::GcTask,
        suffix: vec![1; 16],
    }
    .encode()
    .unwrap();
    put(store.as_ref(), &poison, b"broken").await;
    let mut failed = TestGcSweep::new(
        store.clone(),
        Arc::new(TestBlocks),
        poisoned_catalog,
        None,
        GcLimits::default(),
    )
    .unwrap();
    failed.advance_for_tests().await.unwrap_err();
    let mut healthy = scheduler(store.clone(), context);
    advance(&mut healthy, 1).await;
    assert_progress(store.clone(), &task).await;
    assert!(store.get(&poison).await.unwrap().is_some());
    advance(&mut sweep, 2).await;
    assert_progress(store, &task).await;
}
