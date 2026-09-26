// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use crowdb_monitor::{MonitorPhase, MonitorStatus, ServiceStatus, StatusStore};
use uuid::Uuid;

struct TestRunRoot(PathBuf);

impl TestRunRoot {
    fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("monitor-status-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        Self(root.canonicalize().unwrap())
    }
}

impl Drop for TestRunRoot {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn command(name: &str, root: &TestRunRoot) -> bool {
    Command::new(env!("CARGO_BIN_EXE_crowdb-monitor"))
        .args([name, "--run-root"])
        .arg(&root.0)
        .output()
        .unwrap()
        .status
        .success()
}

#[test]
fn health_commands_require_fresh_ready_snapshot() {
    let root = TestRunRoot::new();
    assert!(!command("liveness", &root));
    assert!(!root.0.join("status").exists());
    let store = StatusStore::new(&root.0).unwrap();
    let mut status = MonitorStatus::new(Uuid::new_v4(), MonitorPhase::Initializing);
    store.publish(&mut status).unwrap();
    assert!(command("liveness", &root));
    assert!(!command("readiness", &root));
    status.phase = MonitorPhase::Ready;
    status.services.insert(
        "kv".into(),
        ServiceStatus {
            pid: Some(123),
            generation: 1,
            healthy: true,
            restart_attempts: 0,
        },
    );
    store.publish(&mut status).unwrap();
    assert_eq!(status.revision, 2);
    assert!(command("readiness", &root));
    let path = root.0.join("status/monitor.json");
    let mut stale: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    stale["updated_at_ms"] = 0.into();
    fs::write(&path, serde_json::to_vec(&stale).unwrap()).unwrap();
    assert!(store.read(Duration::from_secs(10)).is_err());
    status.phase = MonitorPhase::Restarting;
    store.publish(&mut status).unwrap();
    assert!(!command("readiness", &root));
}

#[test]
fn corrupt_or_symlinked_status_fails_closed() {
    let root = TestRunRoot::new();
    let store = StatusStore::new(&root.0).unwrap();
    let mut status = MonitorStatus::new(Uuid::new_v4(), MonitorPhase::Ready);
    store.publish(&mut status).unwrap();
    fs::write(root.0.join("status/monitor.json"), b"not json").unwrap();
    assert!(!command("liveness", &root));
    fs::remove_file(root.0.join("status/monitor.json")).unwrap();
    std::os::unix::fs::symlink("/etc/passwd", root.0.join("status/monitor.json")).unwrap();
    assert!(!command("liveness", &root));
}
