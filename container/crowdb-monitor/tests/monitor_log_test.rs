// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::path::{Path, PathBuf};

use crowdb_monitor::{LogProfile, MonitorEvent, MonitorEventKind, MonitorLog};
use uuid::Uuid;

struct TestLogs(PathBuf);

impl TestLogs {
    fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("monitor-events-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        Self(root.canonicalize().unwrap())
    }
}

impl Drop for TestLogs {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

#[tokio::test]
async fn important_events_are_persisted_under_monitor_log() {
    let logs = TestLogs::new();
    let policy = LogProfile {
        max_file_bytes: 1024 * 1024,
        max_files: 2,
        mirror_warnings_to_stderr: false,
    };
    let mut monitor = MonitorLog::open(&logs.0, policy).await.unwrap();
    monitor
        .record(&MonitorEvent {
            kind: MonitorEventKind::ChildStarted,
            service: Some("kv"),
            pid: Some(123),
            attempt: None,
        })
        .await
        .unwrap();
    monitor
        .record(&MonitorEvent {
            kind: MonitorEventKind::Restarting,
            service: Some("kv"),
            pid: None,
            attempt: Some(2),
        })
        .await
        .unwrap();
    let body = fs::read_to_string(logs.0.join("monitor/monitor.log")).unwrap();
    let entries = body
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0]["kind"], "child_started");
    assert_eq!(entries[1]["kind"], "restarting");
    assert_eq!(entries[1]["level"], "warn");
    assert_eq!(entries[1]["attempt"], 2);
}
