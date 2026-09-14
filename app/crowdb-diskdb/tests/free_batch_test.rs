// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crowdb_diskdb::metrics::DiskdbMetrics;
use crowdb_diskdb::model::alloc::{prepare_free, FreeError, FreeRecord, PreparedFree};
use crowdb_diskdb::model::disk_group::DdbDiskGroup;
use crowdb_diskdb::persistence::{FreeBatchPersist, FreeBatcher};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;
use tokio::sync::Notify;

#[derive(Clone)]
struct PersistCall {
    bind: (u64, u64),
    records: Vec<FreeRecord>,
}

struct TestPersistence {
    calls: Mutex<Vec<PersistCall>>,
    hold_first: AtomicBool,
    fail_call: AtomicUsize,
    first_entered: Notify,
    release_first: Notify,
}

impl TestPersistence {
    fn new(hold_first: bool) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            hold_first: AtomicBool::new(hold_first),
            fail_call: AtomicUsize::new(usize::MAX),
            first_entered: Notify::new(),
            release_first: Notify::new(),
        }
    }

    fn snapshot(&self) -> Vec<PersistCall> {
        self.calls.lock().unwrap().clone()
    }
}

impl FreeBatchPersist for TestPersistence {
    fn persist<'a>(
        &'a self,
        bind: (u64, u64),
        records: &'a [FreeRecord],
    ) -> crowdb_diskdb::persistence::PersistFuture<'a> {
        Box::pin(async move {
            let call_index = {
                let mut calls = self.calls.lock().unwrap();
                let index = calls.len();
                calls.push(PersistCall {
                    bind,
                    records: records.to_vec(),
                });
                index
            };
            if call_index == 0 && self.hold_first.load(Ordering::Acquire) {
                self.first_entered.notify_one();
                self.release_first.notified().await;
            }
            if self.fail_call.load(Ordering::Acquire) == call_index {
                Err(FreeError::OutcomeUnknown)
            } else {
                Ok(())
            }
        })
    }
}

fn segment(id: u64) -> Segment {
    Segment {
        disk_id: Some(DiskId { high: 0, low: id }),
        zone_index: 0,
        unit_offset: id,
        unit_count: 1,
        owner_chunk: Some(ChunkId { high: 1, low: id }),
        allocation_ts: id + 10,
    }
}

fn prepared(bind: (u64, u64), ids: &[u64]) -> PreparedFree {
    let dg = Arc::new(DdbDiskGroup::new(ids.first().copied().unwrap_or(1), 1, 1));
    dg.set_bind(bind);
    let segments: Vec<_> = ids.iter().copied().map(segment).collect();
    prepare_free(&dg, &segments).unwrap()
}

async fn wait_for_queue<P: FreeBatchPersist>(batcher: &FreeBatcher<P>, expected: usize) {
    for _ in 0..1_000 {
        if batcher.state_for_tests().0 == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("queue did not reach {expected}: {:?}", batcher.state_for_tests());
}

#[tokio::test]
async fn singleton_flushes_immediately_and_ack_waits_for_persist() {
    let persistence = Arc::new(TestPersistence::new(true));
    let batcher = Arc::new(FreeBatcher::new(
        Arc::clone(&persistence),
        Arc::new(DiskdbMetrics::disabled()),
    ));
    let entered = persistence.first_entered.notified();
    let submit = tokio::spawn({
        let batcher = Arc::clone(&batcher);
        async move { batcher.submit(prepared((1, 1), &[1]), 16).await }
    });

    entered.await;
    assert!(!submit.is_finished(), "success must wait for durable persistence");
    persistence.release_first.notify_one();
    assert_eq!(submit.await.unwrap().unwrap().freed_count, 1);
    assert_eq!(persistence.snapshot().len(), 1);
}

#[tokio::test]
async fn direct_mode_keeps_one_request_per_kv_batch() {
    let persistence = Arc::new(TestPersistence::new(false));
    let metrics = Arc::new(DiskdbMetrics::disabled());
    let batcher = Arc::new(FreeBatcher::new(Arc::clone(&persistence), Arc::clone(&metrics)));

    batcher.submit_direct(prepared((1, 1), &[1, 2])).await.unwrap();
    batcher.submit_direct(prepared((1, 1), &[3])).await.unwrap();

    assert_eq!(persistence.snapshot().len(), 2);
    assert_eq!(metrics.free_batch_input_requests.snapshot().total, 2);
    assert_eq!(metrics.free_batch_output_batches.snapshot().total, 2);
    assert_eq!(metrics.free_batch_coalescing_ratio_x1000.snapshot(), 1_000);
}

#[tokio::test]
async fn concurrent_backlog_coalesces_to_cap_without_splitting_requests() {
    let persistence = Arc::new(TestPersistence::new(true));
    let metrics = Arc::new(DiskdbMetrics::disabled());
    let batcher = Arc::new(FreeBatcher::new(Arc::clone(&persistence), Arc::clone(&metrics)));
    let entered = persistence.first_entered.notified();
    let first = tokio::spawn({
        let batcher = Arc::clone(&batcher);
        async move { batcher.submit(prepared((1, 1), &[1]), 3).await }
    });
    entered.await;

    let mut waiters = Vec::new();
    for id in 2..=6 {
        let batcher = Arc::clone(&batcher);
        waiters.push(tokio::spawn(async move {
            batcher.submit(prepared((1, 1), &[id]), 3).await
        }));
    }
    wait_for_queue(&batcher, 5).await;
    persistence.release_first.notify_one();
    first.await.unwrap().unwrap();
    for waiter in waiters {
        waiter.await.unwrap().unwrap();
    }

    let calls = persistence.snapshot();
    assert_eq!(
        calls.iter().map(|call| call.records.len()).collect::<Vec<_>>(),
        [1, 3, 2]
    );
    assert_eq!(metrics.free_batch_input_requests.snapshot().total, 6);
    assert_eq!(metrics.free_batch_output_batches.snapshot().total, 3);
    assert_eq!(metrics.free_batch_output_records.snapshot().total, 6);
}

#[tokio::test]
async fn each_queued_request_keeps_its_captured_batch_limit() {
    let persistence = Arc::new(TestPersistence::new(true));
    let batcher = Arc::new(FreeBatcher::new(
        Arc::clone(&persistence),
        Arc::new(DiskdbMetrics::disabled()),
    ));
    let entered = persistence.first_entered.notified();
    let first = tokio::spawn({
        let batcher = Arc::clone(&batcher);
        async move { batcher.submit(prepared((1, 1), &[1]), 8).await }
    });
    entered.await;
    let old_limit = tokio::spawn({
        let batcher = Arc::clone(&batcher);
        async move { batcher.submit(prepared((1, 1), &[2]), 8).await }
    });
    let new_limit = tokio::spawn({
        let batcher = Arc::clone(&batcher);
        async move { batcher.submit(prepared((1, 1), &[3]), 1).await }
    });
    wait_for_queue(&batcher, 2).await;
    persistence.release_first.notify_one();
    first.await.unwrap().unwrap();
    old_limit.await.unwrap().unwrap();
    new_limit.await.unwrap().unwrap();

    assert_eq!(
        persistence
            .snapshot()
            .iter()
            .map(|call| call.records.len())
            .collect::<Vec<_>>(),
        [1, 1, 1]
    );
}

#[tokio::test]
async fn batches_do_not_cross_binds_and_oversized_request_stays_atomic() {
    let persistence = Arc::new(TestPersistence::new(true));
    let metrics = Arc::new(DiskdbMetrics::disabled());
    let batcher = Arc::new(FreeBatcher::new(Arc::clone(&persistence), Arc::clone(&metrics)));
    let entered = persistence.first_entered.notified();
    let first = tokio::spawn({
        let batcher = Arc::clone(&batcher);
        async move { batcher.submit(prepared((1, 1), &[1]), 2).await }
    });
    entered.await;

    let cases = [((2, 2), vec![2]), ((1, 1), vec![3, 4, 5, 6]), ((1, 1), vec![7])];
    let mut waiters = Vec::new();
    for (bind, ids) in cases {
        let batcher = Arc::clone(&batcher);
        waiters.push(tokio::spawn(async move {
            batcher.submit(prepared(bind, &ids), 2).await
        }));
    }
    wait_for_queue(&batcher, 3).await;
    persistence.release_first.notify_one();
    first.await.unwrap().unwrap();
    for waiter in waiters {
        waiter.await.unwrap().unwrap();
    }

    let calls = persistence.snapshot();
    assert!(calls.iter().all(|call| {
        let expected = call.bind;
        call.records.iter().all(|record| {
            if record.0.low == 2 {
                expected == (2, 2)
            } else {
                expected == (1, 1)
            }
        })
    }));
    assert!(calls.iter().any(|call| call.records.len() == 4));
    assert_eq!(metrics.free_batch_oversize_requests.snapshot().total, 1);
}

#[tokio::test]
async fn duplicate_incarnations_across_requests_are_persisted_once() {
    let persistence = Arc::new(TestPersistence::new(true));
    let metrics = Arc::new(DiskdbMetrics::disabled());
    let batcher = Arc::new(FreeBatcher::new(Arc::clone(&persistence), Arc::clone(&metrics)));
    let entered = persistence.first_entered.notified();
    let first = tokio::spawn({
        let batcher = Arc::clone(&batcher);
        async move { batcher.submit(prepared((1, 1), &[1]), 8).await }
    });
    entered.await;
    let duplicate_a = tokio::spawn({
        let batcher = Arc::clone(&batcher);
        async move { batcher.submit(prepared((1, 1), &[2]), 8).await }
    });
    let duplicate_b = tokio::spawn({
        let batcher = Arc::clone(&batcher);
        async move { batcher.submit(prepared((1, 1), &[2]), 8).await }
    });
    wait_for_queue(&batcher, 2).await;
    persistence.release_first.notify_one();

    first.await.unwrap().unwrap();
    duplicate_a.await.unwrap().unwrap();
    duplicate_b.await.unwrap().unwrap();
    let calls = persistence.snapshot();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].records.len(), 1);
    assert_eq!(metrics.free_batch_output_records.snapshot().total, 2);
}

#[tokio::test]
async fn failure_fans_out_without_requeue_and_retry_is_explicit() {
    let persistence = Arc::new(TestPersistence::new(true));
    persistence.fail_call.store(1, Ordering::Release);
    let metrics = Arc::new(DiskdbMetrics::disabled());
    let batcher = Arc::new(FreeBatcher::new(Arc::clone(&persistence), Arc::clone(&metrics)));

    let entered = persistence.first_entered.notified();
    let first = tokio::spawn({
        let batcher = Arc::clone(&batcher);
        async move { batcher.submit(prepared((1, 1), &[1]), 8).await }
    });
    entered.await;
    let second = tokio::spawn({
        let batcher = Arc::clone(&batcher);
        async move { batcher.submit(prepared((1, 1), &[2]), 8).await }
    });
    let third = tokio::spawn({
        let batcher = Arc::clone(&batcher);
        async move { batcher.submit(prepared((1, 1), &[3]), 8).await }
    });
    wait_for_queue(&batcher, 2).await;
    persistence.release_first.notify_one();
    first.await.unwrap().unwrap();
    assert!(matches!(second.await.unwrap(), Err(FreeError::OutcomeUnknown)));
    assert!(matches!(third.await.unwrap(), Err(FreeError::OutcomeUnknown)));
    assert_eq!(persistence.snapshot().len(), 2);
    assert_eq!(metrics.free_batch_failures.snapshot().total, 1);

    batcher.submit(prepared((1, 1), &[2, 3]), 8).await.unwrap();
    assert_eq!(persistence.snapshot().len(), 3);
}

#[tokio::test]
async fn close_rejects_new_work_and_waits_for_admitted_request() {
    let persistence = Arc::new(TestPersistence::new(true));
    let batcher = Arc::new(FreeBatcher::new(
        Arc::clone(&persistence),
        Arc::new(DiskdbMetrics::disabled()),
    ));
    let entered = persistence.first_entered.notified();
    let submit = tokio::spawn({
        let batcher = Arc::clone(&batcher);
        async move { batcher.submit(prepared((1, 1), &[1]), 8).await }
    });
    entered.await;
    let close = tokio::spawn({
        let batcher = Arc::clone(&batcher);
        async move { batcher.close().await }
    });
    tokio::task::yield_now().await;

    assert!(matches!(
        batcher.submit(prepared((1, 1), &[2]), 8).await,
        Err(FreeError::Closed)
    ));
    assert!(!close.is_finished());
    persistence.release_first.notify_one();
    submit.await.unwrap().unwrap();
    close.await.unwrap();
    assert_eq!(batcher.state_for_tests(), (0, false, 0, true));
}

#[tokio::test]
async fn cancelled_waiter_does_not_cancel_admitted_persistence() {
    let persistence = Arc::new(TestPersistence::new(true));
    let batcher = Arc::new(FreeBatcher::new(
        Arc::clone(&persistence),
        Arc::new(DiskdbMetrics::disabled()),
    ));
    let entered = persistence.first_entered.notified();
    let submit = tokio::spawn({
        let batcher = Arc::clone(&batcher);
        async move { batcher.submit(prepared((1, 1), &[1]), 8).await }
    });
    entered.await;
    submit.abort();
    persistence.release_first.notify_one();
    batcher.close().await;

    assert_eq!(persistence.snapshot().len(), 1);
    assert_eq!(batcher.state_for_tests(), (0, false, 0, true));
}

#[tokio::test]
async fn cancelled_direct_waiter_does_not_block_graceful_close() {
    let persistence = Arc::new(TestPersistence::new(true));
    let batcher = Arc::new(FreeBatcher::new(
        Arc::clone(&persistence),
        Arc::new(DiskdbMetrics::disabled()),
    ));
    let entered = persistence.first_entered.notified();
    let submit = tokio::spawn({
        let batcher = Arc::clone(&batcher);
        async move { batcher.submit_direct(prepared((1, 1), &[1])).await }
    });
    entered.await;
    submit.abort();
    persistence.release_first.notify_one();
    batcher.close().await;

    assert_eq!(persistence.snapshot().len(), 1);
    assert_eq!(batcher.state_for_tests(), (0, false, 0, true));
}
