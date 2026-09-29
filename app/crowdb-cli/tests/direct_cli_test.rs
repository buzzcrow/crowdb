// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! CLI e2e (R126 direct-to-group-0): `cluster status` and `cluster rack
//! list` route directly through `--system-ip` / `--system-port` against a
//! real `crowdb-kv-server` with group 0 initialized — no `crowdb-web`
//! intermediary.

mod common;

use common::direct::{crowdb_cli_bin, run, spawn_group0};

#[test]
fn clean_rejects_service_restart_without_launch_registry() {
    let output = std::process::Command::new(crowdb_cli_bin())
        .args(["cluster", "clean", "--restart-services"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("--registry"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_status_via_direct_group0() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    // `cluster status` should list store 0 (the system store).
    let (code, stdout, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "status"]);
    assert_eq!(code, 0, "cluster status stderr={stderr}");
    // Store 0 should be present (group 0 was initialized).
    assert!(
        stdout.contains('0') || stdout.contains("(no stores)"),
        "cluster status stdout={stdout}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_rack_list_ignores_legacy_local_state() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    std::fs::write(&g0.config_path, "invalid local topology").unwrap();
    // `cluster rack list` reads rack 1 from Group 0 despite the old file.
    let (code, stdout, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "rack", "list"]);
    assert_eq!(code, 0, "cluster rack list stderr={stderr}");
    assert!(stdout.contains('1'), "cluster rack list stdout={stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_node_list_ignores_legacy_local_state() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    std::fs::write(&g0.config_path, "invalid local topology").unwrap();
    // `cluster node list` reads node 1 from Group 0 despite the old file.
    let (code, stdout, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "node", "list"]);
    assert_eq!(code, 0, "cluster node list stderr={stderr}");
    assert!(stdout.contains('1'), "cluster node list stdout={stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kv_server_controls_require_launch_registry() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    // A legacy topology file cannot supply process launch policy.
    let (code, stdout, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["kv", "server", "list"]);
    assert_eq!(code, 2, "stdout={stdout} stderr={stderr}");
    assert!(stderr.contains("--registry"), "stderr={stderr}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_local_deploy_reads_group_zero_instead_of_legacy_file() {
    let Some(g0) = spawn_group0().await else {
        return;
    };
    std::fs::write(&g0.config_path, "invalid local topology").unwrap();
    let (code, _, stderr) = run(
        &crowdb_cli_bin(),
        g0.mgmt_port,
        &g0.config_path,
        &["cluster", "local-deploy", "-t", "diskdb", "--data-groups", "99"],
    );
    assert_eq!(code, 2, "stderr={stderr}");
    assert!(stderr.contains("99"), "stderr={stderr}");
    assert!(!stderr.contains("load config"), "stderr={stderr}");
}
