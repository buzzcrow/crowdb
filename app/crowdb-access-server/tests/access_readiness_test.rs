// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#![cfg(feature = "s3")]

#[path = "common/access_health.rs"]
mod fixture;
use crowdb_access_s3::auth::{
    AuthError, CredentialCache, RawAuthRequest, RequestAuthenticator, SigV4Verifier,
    TrustedNetworkAuthenticator,
};
use fixture::TestHealth;
use serde_json::Value;
use std::{
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[tokio::test]
async fn backend_recovers_without_queries_and_preserves_stop() {
    let fixture = TestHealth::new(Arc::new(TrustedNetworkAuthenticator::new()), 1, true).await;
    let failed = fixture.read(503).await;
    assert_eq!(failed["metadata"], "unavailable");
    assert_eq!(failed["chunks"], "ready");
    assert_eq!(failed["details"]["metadata"], "metadata owner unavailable");
    let generation = fixture.generation();
    fixture.operations.phase.store(2, Ordering::Relaxed);
    fixture.after(generation).await;
    let failed = fixture.read(503).await;
    assert_eq!(failed["metadata"], "ready");
    assert_eq!(failed["chunks"], "unavailable");
    let generation = fixture.generation();
    fixture.operations.phase.store(0, Ordering::Relaxed);
    fixture.after(generation).await;
    assert_eq!(fixture.read(200).await["ready"], true);
    fixture.health.stop();
    let stopped = fixture.read(503).await;
    assert!(stopped["reasons"]
        .as_array()
        .unwrap()
        .contains(&Value::from("live")));
}

#[tokio::test]
async fn pending_and_expired_evidence_remain_unknown_and_queries_never_probe() {
    let mut fixture = TestHealth::new(Arc::new(TrustedNetworkAuthenticator::new()), 0, false).await;
    let pending = fixture.read(503).await;
    assert_eq!(pending["metadata"], "unknown");
    assert_eq!(pending["observation"]["state"], "pending");
    assert_eq!(fixture.operations.calls.load(Ordering::Relaxed), 0);
    fixture.observer = Some(fixture.dispatcher.start_health_observer().unwrap());
    fixture.after(0).await;
    let before = fixture.read(200).await;
    fixture.observer.take();
    tokio::task::yield_now().await;
    let calls = fixture.operations.calls.load(Ordering::Relaxed);
    let reads = futures::future::join_all((0..32).map(|_| fixture.read(200))).await;
    for read in reads {
        assert_eq!(
            read["observation"]["generation"],
            before["observation"]["generation"]
        );
    }
    assert_eq!(fixture.operations.calls.load(Ordering::Relaxed), calls);
    tokio::time::sleep(Duration::from_millis(3050)).await;
    let expired = fixture.read(503).await;
    assert_eq!(expired["metadata"], "unknown");
    assert_eq!(expired["chunks"], "unknown");
    assert_eq!(expired["observation"]["state"], "expired");
    assert_eq!(
        expired["observation"]["checked_at_unix_ms"],
        before["observation"]["checked_at_unix_ms"]
    );
    assert_eq!(fixture.operations.calls.load(Ordering::Relaxed), calls);
}

#[tokio::test]
async fn stalled_sampler_is_bounded_single_flight_and_queries_stay_fast() {
    let mut fixture = TestHealth::new(Arc::new(TrustedNetworkAuthenticator::new()), 3, true).await;
    assert!(fixture.dispatcher.start_health_observer().is_err());
    let started = Instant::now();
    let failed = fixture.read(503).await;
    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(failed["details"]["metadata"]
        .as_str()
        .unwrap()
        .contains("deadline"));
    let generation = fixture.generation();
    fixture.after(generation).await;
    assert_eq!(fixture.operations.peak.load(Ordering::Relaxed), 1);
    fixture.observer.take();
    tokio::task::yield_now().await;
    assert_eq!(fixture.operations.in_flight.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn credential_refresh_and_cleanup_update_in_background() {
    let cache = Arc::new(CredentialCache::new(90));
    let fixture = TestHealth::new(
        Arc::new(SigV4Verifier::new(cache.clone(), "us-east-1".into(), 900)),
        0,
        true,
    )
    .await;
    assert_eq!(fixture.read(503).await["authentication"], "unavailable");
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    cache.install([], now - 91).unwrap();
    let generation = fixture.generation();
    fixture.after(generation).await;
    assert_eq!(fixture.read(503).await["authentication"], "unavailable");
    let generation = fixture.generation();
    cache.install([], now).unwrap();
    fixture.after(generation).await;
    assert_eq!(fixture.read(200).await["authentication"], "ready");
    let generation = fixture.generation();
    fixture.metrics.enqueue_cleanup(2);
    fixture.after(generation).await;
    assert_eq!(fixture.read(503).await["cleanup"], "unavailable");
    let generation = fixture.generation();
    fixture.metrics.complete_cleanup(2);
    fixture.after(generation).await;
    assert_eq!(fixture.read(200).await["cleanup"], "ready");
}

struct TestUnavailableAuthenticator;
#[async_trait::async_trait]
impl RequestAuthenticator for TestUnavailableAuthenticator {
    fn is_ready(&self) -> Option<bool> {
        Some(true)
    }
    async fn authenticate(&self, _: RawAuthRequest<'_>) -> Result<(), AuthError> {
        Err(AuthError::Unavailable)
    }
}
#[tokio::test]
async fn business_request_failure_cannot_change_cached_health() {
    let mut fixture = TestHealth::new(Arc::new(TestUnavailableAuthenticator), 0, true).await;
    fixture.observer.take();
    let before = fixture.read(200).await;
    fixture.business_failure().await;
    let after = fixture.read(200).await;
    assert_eq!(before["authentication"], after["authentication"]);
    assert_eq!(
        before["observation"]["generation"],
        after["observation"]["generation"]
    );
    assert_eq!(
        before["observation"]["checked_at_unix_ms"],
        after["observation"]["checked_at_unix_ms"]
    );
}
