// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crowdb_monitor::{DeploymentProfile, MonitorPhase, ProbeKind, Supervisor};
use tokio::net::TcpListener;
use uuid::Uuid;

struct TestRoots(PathBuf);

impl TestRoots {
    fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("monitor-supervisor-{}", Uuid::new_v4()));
        for directory in ["bin", "templates", "data/log", "data/disks", "run"] {
            fs::create_dir_all(root.join(directory)).unwrap();
        }
        symlink("/bin/sh", root.join("bin/sh")).unwrap();
        Self(root.canonicalize().unwrap())
    }

    fn profile(&self, script: String, port: u16, max_attempts: u32) -> DeploymentProfile {
        let mut profile = DeploymentProfile::load(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-preview/profile.toml"),
        )
        .unwrap();
        profile.paths.install_root.clone_from(&self.0);
        profile.paths.bin_root = self.0.join("bin");
        profile.paths.template_root = self.0.join("templates");
        profile.paths.data_root = self.0.join("data");
        profile.paths.run_root = self.0.join("run");
        profile.paths.log_root = self.0.join("data/log");
        for disk in &mut profile.disks {
            disk.path = self.0.join("data/disks").join(disk.path.file_name().unwrap());
        }
        profile.services.retain(|service| service.id == "kv");
        let service = &mut profile.services[0];
        service.program = self.0.join("bin/sh");
        service.args = vec!["-c".into(), script];
        service.config_template = None;
        service.probe.kind = ProbeKind::Tcp;
        service.probe.target = format!("127.0.0.1:{port}");
        service.probe.failure_threshold = 1;
        service.restart.max_attempts = max_attempts;
        service.restart.backoff_base_ms = 10;
        service.restart.backoff_max_ms = 20;
        profile.logs.mirror_warnings_to_stderr = false;
        profile.validate().unwrap();
        profile
    }
}

impl Drop for TestRoots {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

#[tokio::test]
async fn exited_service_restarts_with_same_identity_and_event_log() {
    let roots = TestRoots::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let marker = roots.0.join("first-exit");
    let script = format!(
        "if [ ! -e '{}' ]; then : > '{}'; sleep 0.2; exit 0; fi; exec sleep 30",
        marker.display(),
        marker.display()
    );
    let profile = roots.profile(script, listener.local_addr().unwrap().port(), 2);
    let identity = Uuid::new_v4();
    let mut supervisor = Supervisor::new(profile, identity, &roots.0.join("data/log"), &roots.0.join("run"))
        .await
        .unwrap();
    supervisor.start_service("kv", BTreeMap::new()).await.unwrap();
    supervisor.mark_ready().await.unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;
    supervisor.poll_once().await.unwrap();
    assert_eq!(supervisor.status().deployment_id, identity);
    assert_eq!(supervisor.status().phase, MonitorPhase::Ready);
    assert_eq!(supervisor.status().services["kv"].generation, 2);
    supervisor.shutdown().await.unwrap();
    let body = fs::read_to_string(roots.0.join("data/log/monitor/monitor.log")).unwrap();
    for event in [
        "starting",
        "child_exited",
        "restarting",
        "child_stopped",
        "draining",
        "stopped",
    ] {
        assert!(body.contains(&format!("\"kind\":\"{event}\"")), "missing {event}");
    }
}

#[tokio::test]
async fn repeated_exits_exhaust_budget_and_leave_unready() {
    let roots = TestRoots::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let profile = roots.profile(
        "sleep 0.2; exit 1".into(),
        listener.local_addr().unwrap().port(),
        1,
    );
    let mut supervisor = Supervisor::new(
        profile,
        Uuid::new_v4(),
        &roots.0.join("data/log"),
        &roots.0.join("run"),
    )
    .await
    .unwrap();
    supervisor.start_service("kv", BTreeMap::new()).await.unwrap();
    supervisor.mark_ready().await.unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;
    supervisor.poll_once().await.unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(supervisor.poll_once().await.is_err());
    assert_eq!(supervisor.status().phase, MonitorPhase::Failed);
    assert!(supervisor.status().services["kv"].pid.is_none());
    let body = fs::read_to_string(roots.0.join("data/log/monitor/monitor.log")).unwrap();
    assert!(body.contains("restart_exhausted"));
}

#[tokio::test]
async fn stable_health_resets_crash_loop_budget() {
    let roots = TestRoots::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut profile = roots.profile(
        "sleep 0.3; exit 1".into(),
        listener.local_addr().unwrap().port(),
        1,
    );
    profile.services[0].restart.stable_after_ms = 50;
    let mut supervisor = Supervisor::new(
        profile,
        Uuid::new_v4(),
        &roots.0.join("data/log"),
        &roots.0.join("run"),
    )
    .await
    .unwrap();
    supervisor.start_service("kv", BTreeMap::new()).await.unwrap();
    supervisor.mark_ready().await.unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;
    supervisor.poll_once().await.unwrap();
    assert_eq!(supervisor.status().services["kv"].restart_attempts, 1);
    tokio::time::sleep(Duration::from_millis(80)).await;
    supervisor.poll_once().await.unwrap();
    assert_eq!(supervisor.status().services["kv"].restart_attempts, 0);
    tokio::time::sleep(Duration::from_millis(300)).await;
    supervisor.poll_once().await.unwrap();
    assert_eq!(supervisor.status().services["kv"].generation, 3);
    supervisor.shutdown().await.unwrap();
    let body = fs::read_to_string(roots.0.join("data/log/monitor/monitor.log")).unwrap();
    assert!(body.contains("restart_budget_reset"));
}

#[tokio::test]
async fn transient_probe_failure_clears_readiness_without_restarting() {
    let roots = TestRoots::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut profile = roots.profile("exec sleep 30".into(), address.port(), 2);
    profile.services[0].probe.failure_threshold = 2;
    let mut supervisor = Supervisor::new(
        profile,
        Uuid::new_v4(),
        &roots.0.join("data/log"),
        &roots.0.join("run"),
    )
    .await
    .unwrap();
    supervisor.start_service("kv", BTreeMap::new()).await.unwrap();
    supervisor.mark_ready().await.unwrap();
    let original_pid = supervisor.status().services["kv"].pid;
    drop(listener);
    supervisor.poll_once().await.unwrap();
    assert_eq!(supervisor.status().phase, MonitorPhase::Restarting);
    assert!(!supervisor.status().services["kv"].healthy);
    let listener = TcpListener::bind(address).await.unwrap();
    supervisor.poll_once().await.unwrap();
    assert_eq!(supervisor.status().phase, MonitorPhase::Ready);
    assert_eq!(supervisor.status().services["kv"].pid, original_pid);
    assert_eq!(supervisor.status().services["kv"].generation, 1);
    supervisor.shutdown().await.unwrap();
    drop(listener);
}

#[tokio::test]
async fn dependency_restart_stops_dependents_before_replacement() {
    let roots = TestRoots::new();
    let root_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dependent_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let marker = roots.0.join("first-exit");
    let script = format!(
        "if [ ! -e '{}' ]; then : > '{}'; sleep 0.2; exit 0; fi; exec sleep 30",
        marker.display(),
        marker.display()
    );
    let mut profile = roots.profile(script, root_listener.local_addr().unwrap().port(), 2);
    let mut dependent = profile.services[0].clone();
    dependent.id = "web".into();
    dependent.dependencies = vec!["kv".into()];
    dependent.args = vec!["-c".into(), "exec sleep 30".into()];
    dependent.probe.target = dependent_listener.local_addr().unwrap().to_string();
    profile.services.push(dependent);
    profile.validate().unwrap();
    let mut supervisor = Supervisor::new(
        profile,
        Uuid::new_v4(),
        &roots.0.join("data/log"),
        &roots.0.join("run"),
    )
    .await
    .unwrap();
    supervisor.start_service("kv", BTreeMap::new()).await.unwrap();
    supervisor.start_service("web", BTreeMap::new()).await.unwrap();
    supervisor.mark_ready().await.unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;
    supervisor.poll_once().await.unwrap();
    assert_eq!(supervisor.status().phase, MonitorPhase::Ready);
    assert_eq!(supervisor.status().services["kv"].generation, 2);
    assert_eq!(supervisor.status().services["web"].generation, 2);
    supervisor.shutdown().await.unwrap();
    let body = fs::read_to_string(roots.0.join("data/log/monitor/monitor.log")).unwrap();
    let events = body
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let stops = events
        .iter()
        .filter(|event| event["kind"] == "child_stopped")
        .map(|event| event["service"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&stops[..2], &["web", "kv"]);
}
