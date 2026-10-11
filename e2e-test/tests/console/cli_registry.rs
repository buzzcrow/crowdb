// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![cfg(target_os = "linux")]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use super::cli_fixture::{crowdb_cli_bin, spawn_group0};
use super::fixture::client_data_dir;
use crowdb_console_shared::bootstrap_intent::BootstrapIntent;
use crowdb_console_shared::config::web::{LaunchRecord, LaunchRegistry};
use crowdb_console_shared::launch::LaunchRuntime;
use crowdb_console_shared::lifecycle;

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
    let args: Vec<_> = ["kv", "server"].into_iter().chain(args.iter().copied()).collect();
    run_command(registry, port, &args)
}

fn run_command(registry: &Path, port: u16, args: &[&str]) -> String {
    let output = Command::new(crowdb_cli_bin())
        .args(["--system-ip", &crate::common::management::addresses(3)[0]])
        .arg("--registry")
        .arg(registry)
        .arg("--system-port")
        .arg(port.to_string())
        .env("CROWDB_CLI_STATE", registry.with_file_name("invalid-legacy.toml"))
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

fn record(directory: &Path, service: &str) -> LaunchRecord {
    let binary = directory.join("service");
    std::fs::write(&binary, "#!/bin/sh\nexec sleep 60\n").unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = directory.join("service.toml");
    std::fs::write(&config, "").unwrap();
    LaunchRecord {
        node_id: 701,
        service_id: service.into(),
        host: "localhost".into(),
        ssh_credential_ref: None,
        ssh_user: None,
        ssh_port: 22,
        binary_path: binary,
        service_config_path: config,
        workspace: directory.to_owned(),
        auto_start: false,
        args: Vec::new(),
        readiness_url: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned release KV containers"]
async fn registry_hardware_uses_group_zero_across_cli_invocations() {
    let g0 = spawn_group0().await;
    let dir = client_data_dir("cli-registry-hardware");
    let path = dir.as_path().join("launches.toml");
    LaunchRegistry {
        version: 1,
        launches: Vec::new(),
    }
    .save(&path)
    .unwrap();

    run_command(
        &path,
        g0.mgmt_port,
        &["cluster", "rack", "add", "--id", "2", "--name", "rack-two"],
    );
    assert!(run_command(&path, g0.mgmt_port, &["cluster", "rack", "list"]).contains("rack-two"));
    run_command(
        &path,
        g0.mgmt_port,
        &[
            "cluster",
            "node",
            "add",
            "--id",
            "2",
            "--rack",
            "2",
            "--host",
            "10.0.0.2",
            "--ssh-credential-ref",
            "ops-key",
        ],
    );
    assert!(run_command(&path, g0.mgmt_port, &["cluster", "node", "list"]).contains("10.0.0.2"));
    run_command(
        &path,
        g0.mgmt_port,
        &["cluster", "rack", "add", "--id", "3", "--name", "empty"],
    );
    run_command(&path, g0.mgmt_port, &["cluster", "rack", "remove", "--id", "3"]);
    assert!(!run_command(&path, g0.mgmt_port, &["cluster", "rack", "list"]).contains("empty"));
    run_command(&path, g0.mgmt_port, &["cluster", "node", "remove", "--id", "2"]);
    assert!(!run_command(&path, g0.mgmt_port, &["cluster", "node", "list"]).contains("10.0.0.2"));
    run_command(&path, g0.mgmt_port, &["cluster", "rack", "remove", "--id", "2"]);
    assert!(!dir.as_path().join("invalid-legacy.toml").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned release KV containers"]
async fn registry_bootstrap_uses_sealed_intent_without_legacy_topology_file() {
    let g0 = spawn_group0().await;
    let dir = client_data_dir("cli-registry-bootstrap");
    let path = dir.as_path().join("launches.toml");
    LaunchRegistry {
        version: 1,
        launches: Vec::new(),
    }
    .save(&path)
    .unwrap();
    let source = dir.as_path().join("bootstrap-source.toml");
    let intent = BootstrapIntent::capture(&g0.bootstrap_config, &[1]).unwrap();
    intent.seal(&source).unwrap();
    std::fs::write(dir.as_path().join("invalid-legacy.toml"), "invalid legacy config").unwrap();

    run_command(
        &path,
        g0.mgmt_port,
        &[
            "cluster",
            "init",
            "--nodes",
            "1",
            "--bootstrap-file",
            source.to_str().unwrap(),
        ],
    );
    assert!(!path.with_extension("bootstrap-intent.toml").exists());
    intent
        .seal(&path.with_extension("bootstrap-intent.toml"))
        .unwrap();
    run_command(&path, g0.mgmt_port, &["cluster", "init", "--nodes", "1"]);
    assert!(!path.with_extension("bootstrap-intent.toml").exists());
    assert_eq!(
        std::fs::read_to_string(dir.as_path().join("invalid-legacy.toml")).unwrap(),
        "invalid legacy config"
    );
    assert!(run_command(&path, g0.mgmt_port, &["cluster", "rack", "list"]).contains("rack-1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned release KV containers"]
async fn cli_uses_launch_registry_and_runtime_identity_without_legacy_state() {
    let g0 = spawn_group0().await;
    let dir = client_data_dir("cli-launch-registry");
    std::fs::write(dir.as_path().join("invalid-legacy.toml"), "invalid legacy config").unwrap();
    let record = record(dir.as_path(), "kv");
    let path = dir.as_path().join("launches.toml");
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
        std::fs::read_to_string(dir.as_path().join("invalid-legacy.toml")).unwrap(),
        "invalid legacy config"
    );
}
