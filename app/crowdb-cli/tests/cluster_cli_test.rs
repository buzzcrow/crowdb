// Copyright 2026-present Gian <crow.db@outlook.com>.

//! CLI e2e for cluster observation: `cluster status` and `cluster
//! topology` through a system-group endpoint against a real
//! `crowdb-kv-server` with the system group initialized.

mod common;

use std::time::Duration;
use std::{fs, process::Command};

use common::direct::{crowdb_cli_bin, run, spawn_group0, tempdir};

#[test]
fn non_benchmark_command_does_not_create_log_directory() {
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }
    let root = tempdir("cli-output-prefix");
    let config = root.join("missing-console.toml");
    let output = Command::new(cli)
        .current_dir(&root)
        .env("CROWDB_RUNTIME_ROOT", root.join(".crowdb-runtime"))
        .env("CROWDB_CLI_STATE", config)
        .args(["cluster", "local-deploy", "--service-type", "invalid"])
        .output()
        .expect("run crowdb-cli");
    assert!(!output.status.success());
    assert!(!root.join(".crowdb-runtime/artifacts/cli").exists());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn benchmark_replaces_its_previous_log_directory() {
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }
    let root = tempdir("bench-latest-log");
    let log_dir = root.join(".crowdb-runtime/artifacts/cli/bench-rpc");
    fs::create_dir_all(&log_dir).expect("create old benchmark log dir");
    fs::write(log_dir.join("old-run.marker"), b"old").expect("write old marker");

    let output = Command::new(cli)
        .current_dir(&root)
        .env("CROWDB_RUNTIME_ROOT", root.join(".crowdb-runtime"))
        .args(["bench", "rpc", "--server-port", "1", "--metrics-interval", "0"])
        .output()
        .expect("run benchmark against closed port");
    assert!(!output.status.success());
    assert!(log_dir.is_dir());
    assert!(!log_dir.join("old-run.marker").exists());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("log dir:"), "stderr={stderr}");
    let _ = fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn cluster_status_topology_via_direct_group0() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    // cluster init — writes store/group/replica topology into group-0
    // sysdata (idempotent: group 0 already exists from spawn_group0,
    // init handles the 409 conflict and still writes topology).
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["cluster", "init", "-n", "1"],
    );
    assert_eq!(code, 0, "cluster init stderr={stderr}");

    // status — lists stores from group-0 sysdata.
    let (code, stdout, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "status"]);
    assert_eq!(code, 0, "status stderr={stderr}");
    assert!(stdout.contains('0'), "stdout={stdout}");

    // topology — from a node's /topology endpoint.
    let (code, stdout, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["cluster", "topology", "-n", "1"],
    );
    assert_eq!(code, 0, "topology stderr={stderr}");
    assert!(stdout.contains("store"), "stdout={stdout}");

    // Status always uses the human-readable console table.
    let (code, stdout, _) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "status"]);
    assert_eq!(code, 0);
    assert!(stdout.contains("STORE"), "stdout={stdout}");

    tokio::time::sleep(Duration::from_millis(50)).await;
}
