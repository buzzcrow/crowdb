// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![cfg(target_os = "linux")]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;

use crowdb_console_shared::config::web::{LaunchRecord, LaunchRegistry};
use crowdb_console_shared::launch::LaunchRuntime;
use crowdb_console_shared::lifecycle;
use crowdb_test_harness::test_dirs::tempdir_in_test_data;

struct TestProcesses(Vec<u32>);

impl Drop for TestProcesses {
    fn drop(&mut self) {
        for pid in &self.0 {
            if lifecycle::process_is_alive(*pid) {
                let _ = lifecycle::stop_pid_with_timeout(*pid, Duration::from_secs(2));
            }
        }
    }
}

fn launch(root: &Path, body: &str) -> LaunchRecord {
    let binary = root.join("service");
    std::fs::write(&binary, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = root.join("service.toml");
    std::fs::write(&config, "").unwrap();
    LaunchRecord {
        node_id: 701,
        service_id: "kv".into(),
        host: "localhost".into(),
        ssh_credential_ref: None,
        ssh_user: None,
        ssh_port: 22,
        binary_path: binary,
        service_config_path: config,
        workspace: root.to_owned(),
        auto_start: false,
        args: vec!["--label".into(), "space and 'quote'".into()],
        readiness_url: None,
    }
}

#[tokio::test]
async fn registry_launch_survives_console_runtime_recreation_and_restarts() {
    let dir = tempdir_in_test_data("launch-runtime");
    let record = launch(dir.path(), "printf '%s\\n' \"$@\" > arguments\nexec sleep 60");
    let runtime_root = dir.path().join("runtime");
    let runtime = LaunchRuntime::new(runtime_root.clone());
    let registry = LaunchRegistry {
        version: 1,
        launches: vec![record.clone()],
    };
    assert!(runtime.start_enabled(&registry).await.unwrap().is_empty());
    let mut guard = TestProcesses(Vec::new());
    let first = runtime.start(&record).await.unwrap();
    guard.0.push(first.pid);
    let arguments = std::fs::read_to_string(dir.path().join("arguments")).unwrap();
    assert_eq!(arguments.lines().collect::<Vec<_>>(), record.command_args());
    let recreated = LaunchRuntime::new(runtime_root);
    assert_eq!(recreated.status(&record).await.unwrap(), Some(first));
    assert_eq!(recreated.start(&record).await.unwrap(), first);
    let next = recreated.restart(&record).await.unwrap();
    guard.0.push(next.pid);
    assert_ne!(first, next);
    assert!(!lifecycle::process_is_alive(first.pid));
    recreated.stop(&record).await.unwrap();
    assert_eq!(recreated.status(&record).await.unwrap(), None);
    let serialized = toml::to_string(&registry).unwrap();
    assert!(!serialized.contains("pid"));
    assert!(!serialized.contains("start_ticks"));
}

#[tokio::test]
async fn stale_process_identity_is_never_signalled() {
    let dir = tempdir_in_test_data("launch-stale");
    let record = launch(dir.path(), "exec sleep 60");
    let runtime = LaunchRuntime::new(dir.path().join("runtime"));
    let identity = runtime.start(&record).await.unwrap();
    let _guard = TestProcesses(vec![identity.pid]);
    let path = runtime.root().join("701-kv.json");
    let mut state: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    state["identity"]["start_ticks"] = (identity.start_ticks + 1).into();
    std::fs::write(&path, serde_json::to_vec(&state).unwrap()).unwrap();
    assert_eq!(runtime.status(&record).await.unwrap(), None);
    runtime.stop(&record).await.unwrap();
    assert!(lifecycle::process_is_alive(identity.pid));
}

#[tokio::test]
async fn failed_launch_does_not_publish_runtime_identity() {
    let dir = tempdir_in_test_data("launch-failed");
    let record = launch(dir.path(), "exit 7");
    let runtime = LaunchRuntime::new(dir.path().join("runtime"));
    let error = runtime.start(&record).await.unwrap_err();
    assert!(error.to_string().contains("exited"), "{error}");
    assert!(!runtime.root().join("701-kv.json").exists());
}

#[test]
fn registry_rejects_remote_hosts_without_ssh_and_config_overrides() {
    let dir = tempdir_in_test_data("launch-validation");
    let record = launch(dir.path(), "exit 0");
    for record in [
        LaunchRecord {
            host: "remote.example".into(),
            ..record.clone()
        },
        LaunchRecord {
            args: vec!["--config=/other.toml".into()],
            ..record.clone()
        },
        LaunchRecord {
            ssh_credential_ref: Some("../key".into()),
            ..record
        },
    ] {
        assert!(LaunchRegistry {
            version: 1,
            launches: vec![record]
        }
        .validate()
        .is_err());
    }
}

// Packaged KV auto-start, configuration, restart and stop are covered by the
// release-container Console registry flow. Keep unmatched live ownership local.
#[tokio::test]
async fn live_readiness_without_matching_identity_rejects_launch() {
    let dir = tempdir_in_test_data("launch-unmatched-owner");
    let mut record = launch(dir.path(), "touch started\nexec sleep 60");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = axum::Router::new().route("/health", axum::routing::get(|| async { "ok" }));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    record.readiness_url = Some(format!("http://{address}/health"));
    record.auto_start = true;
    let runtime = LaunchRuntime::new(dir.path().join("unrelated-runtime"));
    assert!(matches!(
        runtime
            .start_enabled(&LaunchRegistry {
                version: 1,
                launches: vec![record.clone()]
            })
            .await,
        Err(crowdb_console_shared::error::Error::Conflict { .. })
    ));
    assert_eq!(runtime.status(&record).await.unwrap(), None);
    assert!(!runtime.root().exists(), "conflict must not publish identity");
    assert!(
        !dir.path().join("started").exists(),
        "foreign listener must not spawn a child"
    );
    let reply = reqwest::get(record.readiness_url.as_ref().unwrap())
        .await
        .unwrap();
    assert!(reply.status().is_success(), "foreign listener must remain alive");
    server.abort();
}
