// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::NodeDispatchForTests;
use std::time::Duration;
use tokio::net::{UnixListener, UnixStream};

#[tokio::test]
async fn queued_control_and_overdue_health_both_make_progress() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-dispatch");
    let socket = root.path().join("control.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let mut queued = Vec::new();
    for _ in 0..16 {
        queued.push(UnixStream::connect(&socket).await.unwrap());
    }
    let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
    let mut dispatch = NodeDispatchForTests::default();
    let mut controls = 0;
    let mut health = 0;
    for _ in 0..16 {
        heartbeat.reset_at(tokio::time::Instant::now() - Duration::from_secs(1));
        if dispatch.next(&listener, &mut heartbeat).await.unwrap().is_some() {
            controls += 1;
        } else {
            health += 1;
        }
    }
    assert!(controls >= 8, "overdue health starved queued control: {controls}");
    assert!(
        health >= 8,
        "control traffic starved service reconciliation: {health}"
    );
}
