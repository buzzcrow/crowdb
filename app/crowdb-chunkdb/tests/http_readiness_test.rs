// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/http_readiness.rs"]
mod http_readiness;
#[path = "common/slot_groups.rs"]
#[allow(dead_code)]
mod slot_groups;

use crowdb_chunkdb::{range_guard::RangeGuard, readiness::RangeReadiness};
use crowdb_paxos_kv_client::ChunkSlotMapClient;
use http_readiness::TestReadiness;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

#[tokio::test]
async fn http_readers_share_real_validated_ownership_without_group_zero_work() {
    let groups = slot_groups::TestGroups::start().await;
    ChunkSlotMapClient::new(Arc::clone(&groups.kv))
        .initialize_service(&http_readiness::layout().service_map().unwrap())
        .await
        .unwrap();
    let guard = Arc::new(RangeGuard::new());
    let readiness = Arc::new(RangeReadiness::new(guard));
    assert_eq!(http_readiness::read(Arc::clone(&readiness)).await, 503);
    let (stop, receiver) = tokio::sync::watch::channel(false);
    let worker = Arc::clone(&readiness).start(Arc::clone(&groups.kv), 1, receiver);
    tokio::time::timeout(Duration::from_secs(3), async {
        while http_readiness::read(Arc::clone(&readiness)).await != 200 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("background ownership sample did not finish");
    stop.send(true).unwrap();
    worker.await.unwrap();
    let _ = groups.kv.drain_window();
    let responses =
        futures::future::join_all((0..32).map(|_| http_readiness::read(Arc::clone(&readiness)))).await;
    assert!(responses.iter().all(|status| *status == 200));
    let work = groups.kv.drain_window();
    assert_eq!(
        work.get.len() + work.scan.len() + work.put.len() + work.batch_write.len(),
        0
    );
    tokio::time::sleep(Duration::from_millis(3050)).await;
    assert_eq!(
        http_readiness::read(readiness).await,
        503,
        "readers cannot renew evidence"
    );
}

#[tokio::test(start_paused = true)]
async fn slow_ownership_refresh_is_independent_of_http_readers() {
    let test = TestReadiness::new(true, 1);
    test.mode.store(1, Ordering::Relaxed);
    TestReadiness::settle().await;
    assert_eq!(test.calls.load(Ordering::Relaxed), 1);
    for _ in 0..32 {
        assert_eq!(test.read().await, 503);
    }
    assert_eq!(test.calls.load(Ordering::Relaxed), 1);
    tokio::time::advance(Duration::from_millis(1500)).await;
    TestReadiness::settle().await;
    assert_eq!(
        test.read().await,
        200,
        "a completed valid sample is readable within one second"
    );
    test.stop.send(true).unwrap();
    test.worker.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn stalled_authority_expires_without_renewal_and_recovers_without_reader_traffic() {
    let test = TestReadiness::new(true, 1);
    TestReadiness::settle().await;
    assert_eq!(test.read().await, 200);
    test.mode.store(2, Ordering::Relaxed);
    tokio::time::advance(Duration::from_secs(1)).await;
    TestReadiness::settle().await;
    assert_eq!(test.read().await, 200);
    tokio::time::advance(Duration::from_millis(2050)).await;
    TestReadiness::settle().await;
    assert_eq!(test.read().await, 503);
    tokio::time::advance(Duration::from_secs(1)).await;
    TestReadiness::settle().await;
    assert_eq!(
        test.read().await,
        503,
        "deadline errors cannot publish healthy evidence"
    );
    test.mode.store(0, Ordering::Relaxed);
    tokio::time::advance(Duration::from_secs(3)).await;
    TestReadiness::settle().await;
    assert_eq!(test.read().await, 200);
    test.stop.send(true).unwrap();
    test.worker.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn authority_errors_and_uninitialized_maps_are_unready_but_idle_initialized_owners_are_ready() {
    for (initialized, instance, expected) in [(false, 1, 503), (true, 2, 200)] {
        let test = TestReadiness::new(initialized, instance);
        TestReadiness::settle().await;
        assert_eq!(test.read().await, expected);
        assert!(test.guard.is_empty());
        test.mode.store(3, Ordering::Relaxed);
        tokio::time::advance(Duration::from_secs(1)).await;
        TestReadiness::settle().await;
        assert_eq!(test.read().await, 503);
        test.stop.send(true).unwrap();
        test.worker.await.unwrap();
    }
}
