// Copyright 2026-present Gian <crow.db@outlook.com>

//! CLI e2e for `cluster rack/node` round-trips through a system-group endpoint against a real
//! `crowdb-kv-server` with the system group
//! initialized — no `crowdb-web` intermediary.

mod common;

use common::direct::{crowdb_cli_bin, run, spawn_group0};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn rack_node_lifecycle() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    // rack list — rack 1 already exists (from spawn_group0 config).
    let (code, stdout, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "rack", "list"]);
    assert_eq!(code, 0, "rack list stderr={stderr}");
    assert!(stdout.contains('1'), "stdout={stdout}");

    // rack add — add a second rack.
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["cluster", "rack", "add", "--id", "2", "--name", "rack-two"],
    );
    assert_eq!(code, 0, "rack add stderr={stderr}");
    let (code, stdout, _) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "rack", "list"]);
    assert_eq!(code, 0);
    assert!(stdout.contains('2'), "stdout={stdout}");

    // node add — add node 2 on rack 2.
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["cluster", "node", "add", "--id", "2", "--rack", "2"],
    );
    assert_eq!(code, 0, "node add stderr={stderr}");
    let (code, stdout, _) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "node", "list"]);
    assert_eq!(code, 0);
    assert!(stdout.contains('1') && stdout.contains('2'), "stdout={stdout}");

    // node remove — remove node 2 (no server deployed on it).
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["cluster", "node", "remove", "--id", "2"],
    );
    assert_eq!(code, 0, "node remove stderr={stderr}");
    let (code, stdout, _) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "node", "list"]);
    assert_eq!(code, 0);
    assert!(
        !stdout.contains("2                 2"),
        "node 2 should be gone: stdout={stdout}"
    );
}
