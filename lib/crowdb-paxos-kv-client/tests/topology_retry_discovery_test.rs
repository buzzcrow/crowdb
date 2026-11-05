// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/topology_recovery.rs"]
mod topology_recovery;

use crowdb_paxos_kv_client::{Error, GetOutcome, ReadMode};
use std::time::Duration;
use topology_recovery::TestDiscovery;

#[tokio::test]
async fn a_reachable_stale_seed_cannot_hide_a_replacement_after_transport_failure() {
    let test = TestDiscovery::start(true, true).await;
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        test.client.put(11, 1, b"recovered", b"retained", None),
    )
    .await;
    let read = if matches!(result, Ok(Ok(_))) {
        Some(
            test.client
                .get(11, 1, b"recovered", ReadMode::Linearizable, None)
                .await,
        )
    } else {
        None
    };
    test.stop().await;
    assert!(
        matches!(result, Ok(Ok(_))),
        "stale first seed retained failed route/backoff: {result:?}"
    );
    assert!(matches!(read, Some(Ok(GetOutcome::Found { value, .. })) if value.as_ref() == b"retained"));
}

#[tokio::test]
async fn a_reachable_stale_seed_cannot_hide_a_replacement_after_no_hint_rejection() {
    let test = TestDiscovery::start(false, true).await;
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        test.client.put_cas(11, 1, b"recovered", b"retained", 0),
    )
    .await;
    test.stop().await;
    assert!(
        matches!(result, Ok(Ok(_))),
        "stale first seed retained rejected route: {result:?}"
    );
}

#[tokio::test]
async fn rejecting_stale_routes_does_not_remove_the_retry_budget() {
    let test = TestDiscovery::start(false, false).await;
    let result = test.client.put_cas(11, 1, b"bounded", b"value", 0).await;
    test.stop().await;
    assert!(
        matches!(result, Err(Error::RetriesExhausted { attempts: 3, .. })),
        "{result:?}"
    );
}

#[tokio::test]
async fn concurrent_route_failures_share_the_replacement_discovery() {
    let test = TestDiscovery::start(true, true).await;
    let result = tokio::time::timeout(Duration::from_millis(500), async {
        let mut requests = tokio::task::JoinSet::new();
        for id in 0u8..32 {
            let client = std::sync::Arc::clone(&test.client);
            requests.spawn(async move { client.put(11, 1, &[id], b"retained", None).await });
        }
        let mut results = Vec::new();
        while let Some(result) = requests.join_next().await {
            results.push(result.unwrap());
        }
        results
    })
    .await;
    let fetches = test.fetches.load(std::sync::atomic::Ordering::Relaxed);
    test.stop().await;
    assert!(
        matches!(result, Ok(ref results) if results.iter().all(Result::is_ok)),
        "{result:?}"
    );
    assert_eq!(
        fetches, 2,
        "the stale and replacement seeds should each be fetched once"
    );
}
