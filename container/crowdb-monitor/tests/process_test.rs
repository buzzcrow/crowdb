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
            bearer_env: None,
            timeout_ms: 100,
            failure_threshold: 1,
        },
        additional_probes: Vec::new(),
        restart: RestartProfile {
            max_attempts: 1,
            backoff_base_ms: 1,
            backoff_max_ms: 1,
            stable_after_ms: 60_000,
        },
    }
}

#[tokio::test]
async fn failed_child_retains_its_exit_status_until_cleanup() {
    let logs = TestLogs::new();
    let mut manager = ProcessManager::new(logs.0.clone(), policy(2)).await.unwrap();
    manager.start(&service("exit 7"), &BTreeMap::new()).await.unwrap();
    let status = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(status) = manager.exit_status("fake").unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(status.code(), Some(7));
    assert!(!manager.alive("fake").unwrap());
    assert_eq!(manager.exit_status("fake").unwrap().unwrap().code(), Some(7));
    manager.stop("fake", Duration::from_secs(2)).await.unwrap();
    assert!(manager.exit_status("fake").is_err());
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
    assert_eq!(
        manager.live_pids().unwrap(),
        BTreeMap::from([("fake".into(), first_pid)])
    );
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
    assert!(manager.live_pids().unwrap().is_empty());
    let second_pid = manager.start(&service, &BTreeMap::new()).await.unwrap();
    assert_ne!(second_pid, first_pid);
    assert_eq!(
        manager.live_pids().unwrap(),
        BTreeMap::from([("fake".into(), second_pid)])
    );
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

#[tokio::test]
async fn child_restarts_prune_old_pid_logs_without_crossing_log_channels() {
    let logs = TestLogs::new();
    let directory = logs.0.join("fake");
    fs::create_dir_all(&directory).unwrap();
    for prefix in ["crowdb-fake", "crowdb-fake-rpc"] {
        for pid in 1..=6 {
            fs::write(
                directory.join(format!("{prefix}-20260101-010101.000-{pid}.log")),
                "old",
            )
            .unwrap();
        }
    }
    fs::write(directory.join("operator-notes.txt"), "keep").unwrap();
    let mut manager = ProcessManager::new(logs.0.clone(), policy(2)).await.unwrap();
    let child = service("printf 'new' > \"$LOG_DIR/crowdb-fake-20260102-010101.000-$$.log\"; exec sleep 30");
    let pid = manager
        .start(
            &child,
            &BTreeMap::from([("LOG_DIR".into(), directory.to_string_lossy().into_owned())]),
        )
        .await
        .unwrap();
    let active = directory.join(format!("crowdb-fake-20260102-010101.000-{pid}.log"));
    tokio::time::timeout(Duration::from_secs(2), async {
        while !active.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    manager.stop("fake", Duration::from_secs(2)).await.unwrap();
    let names = fs::read_dir(&directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(
        names
            .iter()
            .filter(|name| name.starts_with("crowdb-fake-2026"))
            .count()
            <= 2
    );
    assert_eq!(
        names
            .iter()
            .filter(|name| name.starts_with("crowdb-fake-rpc-"))
            .count(),
        1
    );
    assert!(directory.join("operator-notes.txt").exists());
    assert_eq!(fs::read_to_string(active).unwrap(), "new");
}

fn exit_evidence(logs: &TestLogs, pid: u32) -> serde_json::Value {
    let body = fs::read_to_string(logs.0.join("monitor/monitor.log")).unwrap();
    let rows = body
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|row| row["kind"] == "child_exit_observed" && row["pid"] == pid)
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]["level"],
        if rows[0]["process_exit"]["success"] == true {
            "info"
        } else {
            "warn"
        }
    );
    assert_eq!(rows[0]["process_exit"]["original_pid"], pid);
    rows[0]["process_exit"].clone()
}

async fn wait_finished(manager: &mut ProcessManager) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while manager.exit_status("fake").unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

async fn wait_marker(logs: &TestLogs) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if fs::read_to_string(logs.0.join("fake/service.log")).is_ok_and(|text| text.contains("ready")) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn completed_child_evidence_preserves_original_pid_and_real_exit_codes() {
    for code in [0, 7] {
        let logs = TestLogs::new();
        let mut manager = ProcessManager::new(logs.0.clone(), policy(2)).await.unwrap();
        let pid = manager
            .start(&service(&format!("exit {code}")), &BTreeMap::new())
            .await
            .unwrap();
        wait_finished(&mut manager).await;
        assert_eq!(manager.pid("fake"), None);
        manager.stop("fake", Duration::from_secs(2)).await.unwrap();
        let row = exit_evidence(&logs, pid);
        assert_eq!(row["status_observed"], true);
        assert_eq!(row["exit_code"], code);
        assert_eq!(row["success"], code == 0);
        assert!(row["exit_signal"].is_null());
        assert!(row["term_sent_at_unix_ms"].is_null());
        assert!(row["kill_requested_at_unix_ms"].is_null());
        assert_eq!(row["stop_path"], "already_exited_before_stop");
    }
}

#[tokio::test]
async fn owned_term_stop_records_the_real_signal_without_success() {
    let logs = TestLogs::new();
    let mut manager = ProcessManager::new(logs.0.clone(), policy(2)).await.unwrap();
    let pid = manager
        .start(&service("printf ready; exec sleep 30"), &BTreeMap::new())
        .await
        .unwrap();
    wait_marker(&logs).await;
    manager.stop("fake", Duration::from_secs(2)).await.unwrap();
    let row = exit_evidence(&logs, pid);
    assert_eq!(row["exit_signal"], 15);
    assert_eq!(row["success"], false);
    assert!(row["exit_code"].is_null());
    assert!(row["term_sent_at_unix_ms"].is_number());
    assert!(row["kill_requested_at_unix_ms"].is_null());
    assert_eq!(row["stop_path"], "owned_stop_requested");
    assert_eq!(row["grace_ms"], 2000);
}

#[tokio::test]
async fn ignored_term_retains_original_grace_and_reports_kill_escalation() {
    let logs = TestLogs::new();
    let mut manager = ProcessManager::new(logs.0.clone(), policy(2)).await.unwrap();
    let pid = manager
        .start(
            &service("trap '' TERM; printf ready; exec sleep 30"),
            &BTreeMap::new(),
        )
        .await
        .unwrap();
    wait_marker(&logs).await;
    manager.stop("fake", Duration::from_millis(30)).await.unwrap();
    let row = exit_evidence(&logs, pid);
    assert_eq!(row["exit_signal"], 9);
    assert_eq!(row["success"], false);
    assert!(row["exit_code"].is_null());
    assert!(row["term_sent_at_unix_ms"].is_number());
    assert!(row["kill_requested_at_unix_ms"].is_number());
    assert_eq!(row["stop_path"], "owned_stop_grace_elapsed");
    assert_eq!(row["grace_ms"], 30);
    assert!(row["elapsed_since_term_ms"].as_u64().unwrap() >= 30);
}

#[tokio::test]
async fn self_signaled_exit_is_not_mislabeled_as_supervisor_kill() {
    let logs = TestLogs::new();
    let mut manager = ProcessManager::new(logs.0.clone(), policy(2)).await.unwrap();
    let pid = manager
        .start(&service("kill -KILL $$"), &BTreeMap::new())
        .await
        .unwrap();
    wait_finished(&mut manager).await;
    manager.stop("fake", Duration::from_secs(2)).await.unwrap();
    let row = exit_evidence(&logs, pid);
    assert_eq!(row["exit_signal"], 9);
    assert_eq!(row["success"], false);
    assert!(row["exit_code"].is_null());
    assert!(row["term_sent_at_unix_ms"].is_null());
    assert!(row["kill_requested_at_unix_ms"].is_null());
    assert_eq!(row["stop_path"], "already_exited_before_stop");
}

#[tokio::test]
async fn unowned_stop_does_not_fabricate_exit_evidence() {
    let logs = TestLogs::new();
    let mut manager = ProcessManager::new(logs.0.clone(), policy(2)).await.unwrap();
    assert!(manager.stop("missing", Duration::from_secs(2)).await.is_err());
    let body = fs::read_to_string(logs.0.join("monitor/monitor.log")).unwrap();
    assert!(!body.contains("child_exit_observed"));
    assert!(!body.contains("\"success\":true"));
}
