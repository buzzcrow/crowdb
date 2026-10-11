// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::future::{pending, ready};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crowdb_access_server::listeners::{run_pair, run_three, serve_s3, AccessError};
use tokio::sync::watch;

struct TestListener(Arc<AtomicUsize>);

impl Drop for TestListener {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

async fn listener(
    guard: TestListener,
    mut shutdown: watch::Receiver<bool>,
    failed: bool,
) -> Result<(), AccessError> {
    let _guard = guard;
    if failed {
        return Err("protocol failed".into());
    }
    while !*shutdown.borrow() {
        shutdown.changed().await.unwrap();
    }
    Ok(())
}

#[tokio::test]
async fn terminal_storage_failure_drops_s3_data_health_and_stops_iceberg() {
    let stopped = Arc::new(AtomicUsize::new(0));
    let (shutdown, receiver) = watch::channel(false);
    let data_guard = TestListener(Arc::clone(&stopped));
    let health_guard = TestListener(Arc::clone(&stopped));
    let data = async move {
        let _guard = data_guard;
        pending::<std::io::Result<()>>().await
    };
    let health = async move {
        let _guard = health_guard;
        pending::<std::io::Result<()>>().await
    };
    let s3 = serve_s3(data, health, ready(()));
    let iceberg = listener(TestListener(Arc::clone(&stopped)), receiver, false);
    let error = run_pair(s3, iceberg, shutdown).await.unwrap_err();
    assert_eq!(error.to_string(), "S3 small-write manager stopped unexpectedly");
    assert_eq!(stopped.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn either_protocol_failure_drains_its_peer_without_replacing_the_error() {
    for failed in 0..2 {
        let stopped = Arc::new(AtomicUsize::new(0));
        let (shutdown, receiver) = watch::channel(false);
        let first = listener(TestListener(Arc::clone(&stopped)), receiver.clone(), failed == 0);
        let second = listener(TestListener(Arc::clone(&stopped)), receiver, failed == 1);
        let error = run_pair(first, second, shutdown).await.unwrap_err();
        assert_eq!(error.to_string(), "protocol failed");
        assert_eq!(stopped.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn each_of_three_protocols_can_stop_and_drain_the_other_services() {
    for failed in 0..3 {
        let stopped = Arc::new(AtomicUsize::new(0));
        let (shutdown, receiver) = watch::channel(false);
        let first = listener(TestListener(Arc::clone(&stopped)), receiver.clone(), failed == 0);
        let second = listener(TestListener(Arc::clone(&stopped)), receiver.clone(), failed == 1);
        let third = listener(TestListener(Arc::clone(&stopped)), receiver, failed == 2);
        let error = run_three(first, second, third, shutdown).await.unwrap_err();
        assert_eq!(error.to_string(), "protocol failed");
        assert_eq!(stopped.load(Ordering::SeqCst), 3);
    }
}
