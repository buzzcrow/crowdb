// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#![cfg(feature = "s3")]

#[path = "common/access_probe_budget.rs"]
mod fixture;
use fixture::{advance, failure_warnings, settle, TestSampler, HELD_OUTER};
use std::sync::atomic::Ordering;

#[tokio::test(start_paused = true)]
async fn production_metadata_and_chunk_probes_accept_reads_over_750ms_within_two_seconds() {
    let (warnings, _subscriber) = failure_warnings();
    let fixture = TestSampler::new(1200);
    settle().await;
    assert_eq!(fixture.operations.calls.load(Ordering::SeqCst), 1);
    advance(750).await;
    assert_eq!(
        fixture.generation(),
        0,
        "a valid slower dependency must remain in progress"
    );
    for _ in 0..32 {
        assert_eq!(fixture.read(503).await["observation"]["state"], "pending");
    }
    assert_eq!(fixture.operations.calls.load(Ordering::SeqCst), 1);
    advance(450).await;
    let ready = fixture.read(200).await;
    assert_eq!(ready["metadata"], "ready");
    assert_eq!(ready["chunks"], "ready");
    assert_eq!(ready["observation"]["age_ms"], 1200);
    assert_eq!(fixture.operations.metrics.snapshot().chunkdb.errors, 0);
    assert_eq!(fixture.operations.metrics.snapshot().diskio.errors, 0);
    assert_eq!(warnings.count.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn two_second_failure_is_visible_then_the_same_sampler_recovers_automatically() {
    let (warnings, _subscriber) = failure_warnings();
    let fixture = TestSampler::new(3000);
    settle().await;
    advance(1999).await;
    assert_eq!(fixture.generation(), 0);
    fixture.operations.delay_ms.store(100, Ordering::SeqCst);
    advance(1).await;
    let failed = fixture.read(503).await;
    assert_eq!(failed["observation"]["generation"], 1);
    assert_eq!(failed["metadata"], "unavailable");
    assert_eq!(failed["chunks"], "unavailable");
    assert!(failed["details"]["metadata"]
        .as_str()
        .unwrap()
        .contains("2000 ms"));
    assert!(failed["details"]["chunks"]
        .as_str()
        .unwrap()
        .contains("ChunkDB read exceeded"));
    assert_eq!(fixture.operations.metrics.snapshot().chunkdb.errors, 1);
    assert_eq!(warnings.count.load(Ordering::SeqCst), 1);
    assert_eq!(warnings.generation.load(Ordering::SeqCst), 1);
    assert_eq!(warnings.budget_ms.load(Ordering::SeqCst), 2000);
    assert_eq!(warnings.elapsed_ms.load(Ordering::SeqCst), 2000);
    assert!(warnings.checked_at_unix_ms.load(Ordering::SeqCst) > 0);
    assert!(fixture.dispatcher.start_health_observer().is_err());
    advance(100).await;
    let ready = fixture.read(200).await;
    assert_eq!(ready["observation"]["generation"], 2);
    assert_eq!(ready["ready"], true);
    assert_eq!(fixture.operations.calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.operations.peak.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.operations.cancelled.load(Ordering::SeqCst), 0);
    assert_eq!(warnings.count.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn outer_watchdog_cancels_at_2100ms_then_sampling_continues_without_overlap_or_burst() {
    let (warnings, _subscriber) = failure_warnings();
    let mut fixture = TestSampler::new(HELD_OUTER);
    settle().await;
    advance(2099).await;
    assert_eq!(fixture.generation(), 0);
    fixture.operations.delay_ms.store(100, Ordering::SeqCst);
    advance(1).await;
    let failed = fixture.read(503).await;
    assert!(failed["details"]["metadata"]
        .as_str()
        .unwrap()
        .contains("2100 ms"));
    assert_eq!(fixture.operations.cancelled.load(Ordering::SeqCst), 1);
    assert_eq!(warnings.count.load(Ordering::SeqCst), 1);
    assert_eq!(warnings.elapsed_ms.load(Ordering::SeqCst), 2100);
    assert_eq!(fixture.operations.calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.operations.active.load(Ordering::SeqCst), 1);
    advance(100).await;
    assert_eq!(fixture.read(200).await["observation"]["generation"], 2);
    advance(799).await;
    assert_eq!(fixture.operations.calls.load(Ordering::SeqCst), 2);
    advance(1).await;
    assert_eq!(fixture.operations.calls.load(Ordering::SeqCst), 3);
    assert_eq!(fixture.operations.peak.load(Ordering::SeqCst), 1);
    fixture.observer.take();
    settle().await;
    assert_eq!(fixture.operations.active.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.operations.disk.active.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.operations.metrics.snapshot().chunkdb.inflight, 0);
    assert_eq!(fixture.operations.metrics.snapshot().diskio.inflight, 0);
    assert_eq!(warnings.count.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn consecutive_slow_successes_keep_the_three_second_start_age_expiry_and_recover() {
    let (warnings, _subscriber) = failure_warnings();
    let fixture = TestSampler::new(1800);
    settle().await;
    advance(1800).await;
    let first = fixture.read(200).await;
    assert_eq!(first["observation"]["generation"], 1);
    assert_eq!(first["observation"]["max_age_ms"], 3000);
    advance(1200).await;
    assert_eq!(fixture.read(200).await["observation"]["age_ms"], 3000);
    advance(1).await;
    let stale = fixture.read(503).await;
    assert_eq!(stale["observation"]["state"], "expired");
    assert_eq!(stale["metadata"], "unknown");
    assert_eq!(stale["chunks"], "unknown");
    assert_eq!(
        stale["observation"]["checked_at_unix_ms"],
        first["observation"]["checked_at_unix_ms"]
    );
    assert_eq!(stale["observation"]["generation"], 1);
    advance(599).await;
    let recovered = fixture.read(200).await;
    assert_eq!(recovered["observation"]["generation"], 2);
    assert_eq!(recovered["observation"]["age_ms"], 1800);
    assert_eq!(fixture.operations.peak.load(Ordering::SeqCst), 1);
    assert_eq!(warnings.count.load(Ordering::SeqCst), 0);
}
