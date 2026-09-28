// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![cfg(target_os = "linux")]

mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
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

fn run(registry: &Path, port: u16, args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_crowdb-cli"))
        .arg("--registry")
        .arg(registry)
        .arg("--system-port")
        .arg(port.to_string())
        .env("CROWDB_CLI_STATE", registry.with_file_name("invalid-legacy.toml"))
        .args(["kv", "server"])
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_uses_launch_registry_and_runtime_identity_without_legacy_state() {
    let g0 = common::direct::spawn_group0()
        .await
        .expect("KV server binary must be built");
    let dir = tempdir_in_test_data("cli-launch-registry");
    std::fs::write(dir.path().join("invalid-legacy.toml"), "invalid legacy config").unwrap();
    let binary = dir.path().join("service");
    std::fs::write(&binary, "#!/bin/sh\nexec sleep 60\n").unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = dir.path().join("service.toml");
    std::fs::write(&config, "").unwrap();
    let record = LaunchRecord {
        node_id: 701,
        service_id: "kv".into(),
        host: "localhost".into(),
        ssh_credential_ref: None,
        ssh_user: None,
        ssh_port: 22,
        binary_path: binary,
        service_config_path: config,
        workspace: dir.path().to_owned(),
        auto_start: false,
        args: Vec::new(),
        readiness_url: None,
    };
    let path = dir.path().join("launches.toml");
    LaunchRegistry {
        version: 1,
        launches: vec![record.clone()],
    }
    .save(&path)
    .unwrap();
    let runtime = LaunchRuntime::for_registry(&path).unwrap();
    let mut guard = TestProcesses(Vec::new());
    run(&path, g0.mgmt_port, &["deploy", "--node", "701"]);
    let first = runtime.status(&record).await.unwrap().unwrap();
    guard.0.push(first.pid);
    run(&path, g0.mgmt_port, &["start", "--node", "701"]);
    assert_eq!(runtime.status(&record).await.unwrap(), Some(first));
    assert!(run(&path, g0.mgmt_port, &["list"]).contains(&first.pid.to_string()));
    run(&path, g0.mgmt_port, &["restart", "--node", "701"]);
    let next = runtime.status(&record).await.unwrap().unwrap();
    guard.0.push(next.pid);
    assert_ne!(first, next);
    run(&path, g0.mgmt_port, &["stop", "--node", "701"]);
    assert!(runtime.status(&record).await.unwrap().is_none());
    run(&path, g0.mgmt_port, &["delete", "--node", "701"]);
    assert!(LaunchRegistry::load(&path).unwrap().launches.is_empty());
    assert_eq!(
        std::fs::read_to_string(dir.path().join("invalid-legacy.toml")).unwrap(),
        "invalid legacy config"
    );
}
