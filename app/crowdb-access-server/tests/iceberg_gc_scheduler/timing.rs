// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::fixture::{
    self, scheduler, setup,
    timing::{cleared_catalog, now_ms, replace_task, IDLE_MS},
};
use crowdb_access_iceberg::{
    gc::{GcLimits, GcPhase, GcRepository},
    key::{CatalogId, OperationId},
};
use crowdb_access_server::iceberg::TestGcSweep;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

#[tokio::test]
async fn default_backlog_uses_one_second_windows_until_idle() {
    let (store, context, initial) = setup().await;
    let mut sweep = scheduler(store.clone(), context);
    let mut previous = initial;
    let mut steps = 0;
    for _ in 0..200 {
        let window = sweep.window_for_tests(IDLE_MS).await;
        assert!(!window.failed, "{window:?}");
        let task = GcRepository::new(store.clone())
            .task(context.catalog, previous.identity)
            .await
            .unwrap()
            .unwrap();
        assert!(
            task.revision <= previous.revision + 1,
            "more than one worker step: {previous:?} -> {task:?}"
        );
        steps += usize::from(task.revision > previous.revision);
        previous = task;
        if !window.busy {
            assert_eq!(window.delay_ms, IDLE_MS);
            assert_eq!(previous.phase, GcPhase::Complete);
            assert!(steps > 4, "only {steps} steps");
            return;
        }
        assert_eq!(window.delay_ms, 1_000);
    }
    panic!("backlog did not finish: {previous:?}");
}

#[tokio::test]
async fn terminal_paused_and_future_rows_finish_a_round_without_busy_polling() {
    let (store, context, task) = setup().await;
    let deadline = now_ms() + 120_000;
    let mut future = task.clone();
    future.retry_at_ms = deadline;
    replace_task(&store, &future).await;
    for (phase, paused) in [
        (GcPhase::Complete, false),
        (GcPhase::Discover, true),
        (GcPhase::Quarantined, false),
    ] {
        let mut extra = task.clone();
        extra.identity = OperationId::random();
        extra.phase = phase;
        extra.paused = paused;
        extra.quarantined_from = (phase == GcPhase::Quarantined).then_some(GcPhase::Discover);
        fixture::put(
            store.as_ref(),
            &extra.key().encode().unwrap(),
            &crowdb_access_iceberg::record::StorageRecord::GcTask(Box::new(extra))
                .encode()
                .unwrap(),
        )
        .await;
    }
    let mut sweep = scheduler(store.clone(), context);
    for _ in 0..10 {
        let window = sweep.window_for_tests(IDLE_MS).await;
        assert!(!window.failed, "{window:?}");
        if !window.busy {
            assert_eq!(window.retry_at_ms, Some(deadline));
            assert!((110_000..=120_000).contains(&window.delay_ms));
            assert_eq!(
                GcRepository::new(store)
                    .task(context.catalog, task.identity)
                    .await
                    .unwrap()
                    .unwrap(),
                future
            );
            return;
        }
        assert_eq!(window.delay_ms, 1_000);
    }
    panic!("nonrunnable rows kept sweep busy");
}

#[tokio::test]
async fn persistent_purge_and_clear_markers_do_not_reopen_terminal_work() {
    let (store, active, mut retired) = cleared_catalog().await;
    retired.phase = GcPhase::Complete;
    replace_task(&store, &retired).await;
    let marker = fixture::purge(&store, active, 25).await;
    let repository = GcRepository::new(store.clone());
    let mut purge = repository
        .admit_purge(active, &marker, now_ms(), GcLimits::default())
        .await
        .unwrap();
    purge.phase = GcPhase::Complete;
    replace_task(&store, &purge).await;
    let mut multipart = repository
        .admit_multipart(active, now_ms(), GcLimits::default())
        .await
        .unwrap();
    multipart.paused = true;
    replace_task(&store, &multipart).await;
    let mut sweep = scheduler(store.clone(), active);
    for _ in 0..30 {
        let window = sweep.window_for_tests(IDLE_MS).await;
        assert!(!window.failed, "{window:?}");
        if !window.busy {
            assert_eq!(window.delay_ms, IDLE_MS);
            assert_eq!(
                repository
                    .task(retired.context.catalog, retired.identity)
                    .await
                    .unwrap()
                    .unwrap(),
                retired
            );
            assert_eq!(
                repository
                    .task(active.catalog, purge.identity)
                    .await
                    .unwrap()
                    .unwrap(),
                purge
            );
            assert_eq!(
                repository
                    .task(active.catalog, multipart.identity)
                    .await
                    .unwrap()
                    .unwrap(),
                multipart
            );
            assert!(store
                .inner
                .values
                .load()
                .contains_key(&marker.key().encode().unwrap()));
            return;
        }
    }
    panic!("persistent terminal marker kept sweep busy");
}

#[tokio::test]
async fn retired_worker_rescans_system_lane_and_respects_future_retention() {
    let (store, active, initial) = cleared_catalog().await;
    let mut sweep = scheduler(store.clone(), active);
    let repository = GcRepository::new(store);
    for _ in 0..100 {
        let window = sweep.window_for_tests(IDLE_MS).await;
        assert!(!window.failed, "{window:?}");
        let task = repository
            .task(initial.context.catalog, initial.identity)
            .await
            .unwrap()
            .unwrap();
        if !window.busy {
            assert!(
                task.revision > initial.revision + 3,
                "old catalog did not advance: {task:?}"
            );
            assert!(task.retry_at_ms > now_ms(), "retention was skipped: {task:?}");
            assert_eq!(window.retry_at_ms, Some(task.retry_at_ms));
            assert_eq!(window.delay_ms, IDLE_MS);
            return;
        }
        assert_eq!(window.delay_ms, 1_000);
    }
    panic!("retired sweep failed to reach its grace boundary");
}

#[tokio::test(start_paused = true)]
async fn ready_catalog_round_robin_does_not_sleep_behind_an_idle_or_failed_scope() {
    let (store, context, mut task) = setup().await;
    task.paused = true;
    replace_task(&store, &task).await;
    let mut sweep = scheduler(store, context);
    let mut idle = sweep.window_for_tests(IDLE_MS).await;
    while idle.busy {
        idle = sweep.window_for_tests(IDLE_MS).await;
    }
    let a = CatalogId::random();
    let b = CatalogId::random();
    let mut schedule = TestGcSweep::schedule_for_tests(&[a], b);
    assert_eq!(schedule.ready_for_tests(), Some(a));
    schedule.finish_for_tests(a, idle, IDLE_MS);
    assert_eq!(schedule.ready_for_tests(), Some(b));
    let mut busy = idle;
    busy.busy = true;
    busy.progressed = true;
    schedule.finish_for_tests(b, busy, IDLE_MS);
    assert_eq!(schedule.ready_for_tests(), None);
    assert_eq!(schedule.wait_ms_for_tests(), 1_000);
    tokio::time::advance(Duration::from_millis(1_000)).await;
    assert_eq!(schedule.ready_for_tests(), Some(b));
    busy.failed = true;
    busy.progressed = false;
    schedule.finish_for_tests(b, busy, IDLE_MS);
    tokio::time::advance(Duration::from_millis(1_000)).await;
    schedule.finish_for_tests(b, busy, IDLE_MS);
    assert_eq!(schedule.wait_ms_for_tests(), 2_000);
    let configured: Vec<_> = (0..64).map(|_| CatalogId::random()).collect();
    for _ in 0..100 {
        schedule.retain_scopes_for_tests(&configured, CatalogId::random());
        assert_eq!(schedule.scopes_for_tests(), 65);
    }
    schedule.retain_scopes_for_tests(&configured, configured[0]);
    assert_eq!(schedule.scopes_for_tests(), 64);
}

#[tokio::test]
async fn repeated_bad_row_backs_off_and_empty_catalog_is_idle() {
    let (store, context, mut task) = setup().await;
    task.paused = true;
    replace_task(&store, &task).await;
    let poison = crowdb_access_iceberg::key::IcebergKey::Catalog {
        catalog: context.catalog,
        scope: crowdb_access_iceberg::key::CatalogScope::GcTask,
        suffix: vec![1; 16],
    }
    .encode()
    .unwrap();
    fixture::put(&store, &poison, b"broken").await;
    let mut sweep = TestGcSweep::new(
        store.clone(),
        Arc::new(fixture::TestBlocks),
        context.catalog,
        None,
        GcLimits {
            page_items: 1,
            ..GcLimits::default()
        },
    )
    .unwrap();
    let mut delay = 0;
    for _ in 0..10 {
        let window = sweep.window_for_tests(IDLE_MS).await;
        assert!(window.failed, "{window:?}");
        assert!(window.delay_ms >= delay, "{window:?}, previous delay {delay}");
        delay = window.delay_ms;
    }
    assert_eq!(delay, 60_000);
    let mut repaired = task;
    repaired.identity = OperationId::from_bytes(&[1; 16]).unwrap();
    replace_task(&store, &repaired).await;
    let mut recovered = false;
    for _ in 0..8 {
        let window = sweep.window_for_tests(IDLE_MS).await;
        if !window.busy && !window.failed {
            assert_eq!(window.delay_ms, IDLE_MS);
            recovered = true;
            break;
        }
    }
    assert!(recovered, "a repaired row never cleared its error backoff");
    let mut empty = TestGcSweep::new(
        store,
        Arc::new(fixture::TestBlocks),
        CatalogId::random(),
        None,
        GcLimits::default(),
    )
    .unwrap();
    let window = empty.window_for_tests(IDLE_MS).await;
    assert!(!window.busy && !window.failed, "{window:?}");
    assert_eq!(window.delay_ms, IDLE_MS);
}

#[tokio::test]
async fn several_tasks_share_one_worker_step_and_the_original_io_quota_each_window() {
    let (store, context, mut multipart) = setup().await;
    multipart.paused = true;
    replace_task(&store, &multipart).await;
    let repository = GcRepository::new(store.clone());
    let mut tasks = Vec::new();
    for identity in [21, 22, 23] {
        let marker = fixture::purge(&store, context, identity).await;
        tasks.push(
            repository
                .admit_purge(context, &marker, now_ms(), GcLimits::default())
                .await
                .unwrap(),
        );
    }
    let mut sweep = scheduler(store.clone(), context);
    let mut per_task = [0; 3];
    for _ in 0..40 {
        store.requests.store(0, Ordering::SeqCst);
        let window = sweep.window_for_tests(IDLE_MS).await;
        assert!(!window.failed, "{window:?}");
        let requests = store.requests.load(Ordering::SeqCst);
        assert!(requests <= 128 + 16, "per-window quota grew: {requests}");
        let mut steps = 0;
        for (index, previous) in tasks.iter_mut().enumerate() {
            let current = repository
                .task(context.catalog, previous.identity)
                .await
                .unwrap()
                .unwrap();
            assert!(
                current.revision <= previous.revision + 1,
                "{previous:?} -> {current:?}"
            );
            let advanced = usize::from(current.revision != previous.revision);
            per_task[index] += advanced;
            steps += advanced;
            *previous = current;
        }
        assert!(steps <= 1, "more than one worker in a window: {steps}");
        if per_task.iter().all(|steps| *steps >= 4) {
            return;
        }
    }
    panic!("task lane was unfair: {per_task:?}");
}
