// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crowdb_monitor::{LogProfile, ProbeKind, ProbeProfile, ProcessManager, RestartProfile, ServiceProfile};
use uuid::Uuid;

struct TestLogs(PathBuf);

impl TestLogs {
    fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("monitor-process-{}", Uuid::new_v4()));
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

fn policy(max_files: u16) -> LogProfile {
    LogProfile {
        max_file_bytes: 1024 * 1024,
        max_files,
        mirror_warnings_to_stderr: false,
    }
}

fn service(script: &str) -> ServiceProfile {
    ServiceProfile {
        id: "fake".into(),
        program: PathBuf::from("/bin/sh"),
        args: vec!["-c".into(), script.into()],
        env: BTreeMap::new(),
        dependencies: Vec::new(),
        fence_listeners: Vec::new(),
        config_template: None,
        probe: ProbeProfile {
            kind: ProbeKind::Tcp,
            target: "127.0.0.1:1".into(),
            timeout_ms: 100,
            failure_threshold: 1,
        },
        restart: RestartProfile {
            max_attempts: 1,
            backoff_base_ms: 1,
            backoff_max_ms: 1,
            stable_after_ms: 60_000,
        },
    }
}

#[tokio::test]
async fn owned_child_is_reaped_before_replacement() {
    let logs = TestLogs::new();
    let mut manager = ProcessManager::new(logs.0.clone(), policy(2)).await.unwrap();
    let service = service("printf 'started\\n'; exec sleep 30");
    let first_pid = manager.start(&service, &BTreeMap::new()).await.unwrap();
    assert!(manager.start(&service, &BTreeMap::new()).await.is_err());
    assert_eq!(manager.pid("fake"), Some(first_pid));
    assert!(manager.alive("fake").unwrap());
    let log_path = logs.0.join("fake/service.log");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if fs::read_to_string(&log_path).is_ok_and(|body| body.contains("started")) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    manager.stop("fake", Duration::from_secs(2)).await.unwrap();
    assert_eq!(manager.pid("fake"), None);
    let second_pid = manager.start(&service, &BTreeMap::new()).await.unwrap();
    assert_ne!(second_pid, first_pid);
    manager.stop("fake", Duration::from_secs(2)).await.unwrap();
    assert!(fs::read_to_string(log_path).unwrap().contains("started"));
    let events = fs::read_to_string(logs.0.join("monitor/monitor.log")).unwrap();
    assert_eq!(events.matches("child_started").count(), 2);
    assert_eq!(events.matches("child_stopped").count(), 2);
}

#[tokio::test]
async fn logs_rotate_with_total_file_and_byte_limits() {
    let logs = TestLogs::new();
    let mut manager = ProcessManager::new(logs.0.clone(), policy(2)).await.unwrap();
    let service =
        service("count=0; while [ \"$count\" -lt 2500 ]; do printf '%01024d\\n' 0; count=$((count+1)); done");
    manager.start(&service, &BTreeMap::new()).await.unwrap();
    for _ in 0..100 {
        if !manager.alive("fake").unwrap() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!manager.alive("fake").unwrap());
    manager.stop("fake", Duration::from_secs(2)).await.unwrap();
    let files = fs::read_dir(logs.0.join("fake"))
        .unwrap()
        .map(|entry| entry.unwrap())
        .collect::<Vec<_>>();
    assert!(files.len() <= 2);
    assert!(files
        .iter()
        .all(|entry| entry.metadata().unwrap().len() <= 1024 * 1024));
}
