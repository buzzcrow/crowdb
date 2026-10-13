// Copyright 2026-present Gian <crow.db@outlook.com>.

//! CLI e2e for cluster observation: `cluster status` and `cluster
//! topology` through a system-group endpoint against a real
//! `crowdb-kv-server` with the system group initialized.

use std::time::Duration;

use super::cli_fixture::{crowdb_cli_bin, run, spawn_group0};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned release KV containers"]
#[allow(clippy::too_many_lines)]
async fn cluster_status_topology_via_direct_group0() {
    let g0 = spawn_group0().await;
    let cli = crowdb_cli_bin();

    // Bootstrap requires an independent versioned intent and launch registry.
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["cluster", "init", "-n", "1"],
    );
    assert_eq!(code, 2, "cluster init stderr={stderr}");
    assert!(stderr.contains("--registry"), "stderr={stderr}");
    std::fs::write(&g0.config_path, "invalid local topology").unwrap();

    // status — lists stores from group-0 sysdata.
    let (code, stdout, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "status"]);
    assert_eq!(code, 0, "status stderr={stderr}");
    assert!(stdout.contains("(no stores)"), "stdout={stdout}");

    // topology — from a node's /topology endpoint.
    let (code, stdout, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["cluster", "topology", "-n", "1"],
    );
    assert_eq!(code, 0, "topology stderr={stderr}");
    assert!(stdout.contains("store"), "stdout={stdout}");

    // Status reflects the uninitialized authority without a local fallback.
    let (code, stdout, _) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "status"]);
    assert_eq!(code, 0);
    assert!(stdout.contains("(no stores)"), "stdout={stdout}");

    tokio::time::sleep(Duration::from_millis(50)).await;
}
