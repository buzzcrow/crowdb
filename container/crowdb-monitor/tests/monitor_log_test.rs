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

#[tokio::test]
async fn rotation_keeps_whole_events_within_file_and_count_bounds() {
    let logs = TestLogs::new();
    let mut monitor = MonitorLog::open(
        &logs.0,
        LogProfile {
            max_file_bytes: 512,
            max_files: 3,
            mirror_warnings_to_stderr: false,
        },
    )
    .await
    .unwrap();
    for attempt in 0..100 {
        monitor
            .record(&MonitorEvent {
                kind: MonitorEventKind::Restarting,
                service: Some("web"),
                pid: None,
                attempt: Some(attempt),
            })
            .await
            .unwrap();
    }
    drop(monitor);
    let files = fs::read_dir(logs.0.join("monitor"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(files.len(), 3);
    let mut attempts = Vec::new();
    for file in files {
        assert!(file.metadata().unwrap().len() <= 512);
        for line in fs::read_to_string(file.path()).unwrap().lines() {
            let event: serde_json::Value = serde_json::from_str(line).unwrap();
            attempts.push(event["attempt"].as_u64().unwrap());
        }
    }
    attempts.sort_unstable();
    assert_eq!(attempts.last(), Some(&99));
    assert!(attempts.len() < 100);
    assert!(attempts.windows(2).all(|pair| pair[1] == pair[0] + 1));
}
