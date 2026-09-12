// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crowdb_kv_server::background::binding_monitor_wiring::spawn_restarting_monitor;

#[tokio::test]
async fn failed_monitor_task_is_restarted_and_shutdown_is_drained() {
    let starts = Arc::new(AtomicUsize::new(0));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let supervisor = spawn_restarting_monitor(stop_rx, {
        let starts = Arc::clone(&starts);
        move |mut child_stop| {
            let starts = Arc::clone(&starts);
            async move {
                assert_ne!(
                    starts.fetch_add(1, Ordering::AcqRel),
                    0,
                    "injected monitor failure"
                );
                while !*child_stop.borrow() && child_stop.changed().await.is_ok() {}
            }
        }
    });

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while starts.load(Ordering::Acquire) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    stop_tx.send(true).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), supervisor)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(starts.load(Ordering::Acquire), 2);
}
