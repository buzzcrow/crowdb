// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/node_health.rs"]
mod fixture;
use fixture::TestNodeHealth;
use std::{sync::atomic::Ordering, time::Duration};

#[tokio::test]
async fn node_status_queries_reuse_backend_evidence_and_do_not_refresh_its_age() {
    let mut fixture = TestNodeHealth::new();
    assert!(fixture.read(502).await["error"]
        .as_str()
        .unwrap()
        .contains("pending"));
    assert_eq!(fixture.calls.load(Ordering::Relaxed), 0);
    fixture.start();
    fixture.after(0).await;
    assert!(crowdb_web::start_node_observer(&fixture.state).is_err());
    let initial = fixture.read(200).await;
    fixture.observer.take();
    tokio::task::yield_now().await;
    let calls = fixture.calls.load(Ordering::Relaxed);
    for value in futures::future::join_all((0..32).map(|_| fixture.read(200))).await {
        assert_eq!(value, initial);
    }
    assert_eq!(fixture.calls.load(Ordering::Relaxed), calls);
    tokio::time::sleep(Duration::from_millis(3050)).await;
    assert!(fixture.read(502).await["error"]
        .as_str()
        .unwrap()
        .contains("expired"));
    assert_eq!(
        crowdb_web::node_observation_time_for_tests(&fixture.state),
        initial["observed_at_unix_ms"].as_u64()
    );
    assert_eq!(fixture.calls.load(Ordering::Relaxed), calls);
}

#[tokio::test]
async fn backend_observation_failure_and_recovery_replace_cache_without_queries() {
    let mut fixture = TestNodeHealth::new();
    fixture.fail.store(true, Ordering::Relaxed);
    fixture.start();
    fixture.after(0).await;
    assert!(fixture.read(502).await["error"]
        .as_str()
        .unwrap()
        .contains("backend dependency unavailable"));
    let calls = fixture.calls.load(Ordering::Relaxed);
    fixture.fail.store(false, Ordering::Relaxed);
    fixture.after(calls).await;
    assert!(crowdb_web::node_observation_time_for_tests(&fixture.state).is_some());
    assert!(fixture.read(200).await["observed_at_unix_ms"].as_u64().is_some());
}
