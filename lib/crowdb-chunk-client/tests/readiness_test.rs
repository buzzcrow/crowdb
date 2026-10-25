// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use crowdb_chunk_client::{client::observe_readiness_phase_for_tests, metrics::ReadinessMetrics};
use tokio::time::Instant;

#[tokio::test(start_paused = true)]
async fn phase_deadlines_share_the_remaining_budget_and_identify_the_dependency() {
    let metrics = ReadinessMetrics::default();
    let deadline = Instant::now() + Duration::from_millis(750);
    tokio::time::advance(Duration::from_millis(600)).await;
    let result = observe_readiness_phase_for_tests(&metrics, deadline, async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(())
    })
    .await;
    assert!(result.unwrap_err().to_string().contains("DiskIO read exceeded"));
    assert_eq!(Instant::now(), deadline);
    let snapshot = metrics.snapshot().diskio;
    assert_eq!(snapshot.observations, 1);
    assert_eq!(snapshot.errors, 1);
    assert_eq!(snapshot.inflight, 0);
}

#[tokio::test]
async fn cancelled_probes_record_latency_and_error_without_leaking_inflight_work() {
    let metrics = Arc::new(ReadinessMetrics::default());
    let worker_metrics = Arc::clone(&metrics);
    let (started, observing) = tokio::sync::oneshot::channel();
    let worker = tokio::spawn(async move {
        observe_readiness_phase_for_tests(&worker_metrics, Instant::now() + Duration::from_secs(10), async {
            started.send(()).unwrap();
            std::future::pending().await
        })
        .await
    });
    observing.await.unwrap();
    assert_eq!(metrics.snapshot().diskio.inflight, 1);
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    let snapshot = metrics.snapshot().diskio;
    assert_eq!(snapshot.observations, 1);
    assert_eq!(snapshot.errors, 1);
    assert_eq!(snapshot.inflight, 0);
    observe_readiness_phase_for_tests(&metrics, Instant::now() + Duration::from_secs(1), async {
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(metrics.snapshot().diskio.observations, 2);
    assert_eq!(metrics.snapshot().diskio.errors, 1);
}
