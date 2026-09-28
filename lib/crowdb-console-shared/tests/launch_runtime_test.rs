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

#[tokio::test]
async fn launch_registry_starts_native_kv_with_referenced_config() {
    let dir = tempdir_in_test_data("launch-native-kv");
    let mut record = launch(dir.path(), "exit 0");
    record.binary_path = lifecycle::crowdb_kv_server_bin().expect("KV server binary must be built");
    let port = crowdb_protocol::port::alloc::alloc_test_port(crowdb_protocol::ServicePort::KvServerMgmt);
    record.args = vec![
        "--root".into(),
        dir.path().join("node").to_string_lossy().into_owned(),
        "--management-addr".into(),
        "127.0.0.1".into(),
        "--management-port".into(),
        port.to_string(),
        "--node-id".into(),
        record.node_id.to_string(),
        "--keepalive-interval".into(),
        "0".into(),
    ];
    record.readiness_url = Some(format!("http://127.0.0.1:{port}/health"));
    record.auto_start = true;
    let runtime = LaunchRuntime::new(dir.path().join("runtime"));
    let identities = runtime
        .start_enabled(&LaunchRegistry {
            version: 1,
            launches: vec![record.clone()],
        })
        .await
        .unwrap();
    let _guard = TestProcesses(identities.iter().map(|identity| identity.pid).collect());
    assert_eq!(identities.len(), 1);
    assert_eq!(runtime.status(&record).await.unwrap(), Some(identities[0]));
    let unrelated = LaunchRuntime::new(dir.path().join("unrelated-runtime"));
    assert!(matches!(
        unrelated.start(&record).await,
        Err(crowdb_console_shared::error::Error::Conflict { .. })
    ));
    assert_eq!(runtime.status(&record).await.unwrap(), Some(identities[0]));
    runtime.stop(&record).await.unwrap();
}
