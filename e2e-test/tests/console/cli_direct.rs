// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! CLI direct Group-0 acceptance: `cluster status` and `cluster rack
//! list` route directly through `--system-ip` / `--system-port` against a
//! real `crowdb-kv-server` with group 0 initialized — no `crowdb-web`
//! intermediary.

use super::cli_fixture::{crowdb_cli_bin, run, spawn_group0};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned release KV containers"]
async fn cluster_status_via_direct_group0() {
    let g0 = spawn_group0().await;
    let cli = crowdb_cli_bin();

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
#[ignore = "requires the owned release KV containers"]
async fn cluster_rack_list_ignores_legacy_local_state() {
    let g0 = spawn_group0().await;
    let cli = crowdb_cli_bin();

    std::fs::write(&g0.config_path, "invalid local topology").unwrap();
    // `cluster rack list` reads rack 1 from Group 0 despite the old file.
    let (code, stdout, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "rack", "list"]);
    assert_eq!(code, 0, "cluster rack list stderr={stderr}");
    assert!(stdout.contains('1'), "cluster rack list stdout={stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned release KV containers"]
async fn cluster_node_list_ignores_legacy_local_state() {
    let g0 = spawn_group0().await;
    let cli = crowdb_cli_bin();

    std::fs::write(&g0.config_path, "invalid local topology").unwrap();
    // `cluster node list` reads node 1 from Group 0 despite the old file.
    let (code, stdout, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "node", "list"]);
    assert_eq!(code, 0, "cluster node list stderr={stderr}");
    assert!(stdout.contains('1'), "cluster node list stdout={stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned release KV containers"]
async fn kv_server_controls_require_launch_registry() {
    let g0 = spawn_group0().await;
    let cli = crowdb_cli_bin();

    // A legacy topology file cannot supply process launch policy.
    let (code, stdout, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["kv", "server", "list"]);
    assert_eq!(code, 2, "stdout={stdout} stderr={stderr}");
    assert!(stderr.contains("--registry"), "stderr={stderr}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned release KV containers"]
async fn incremental_local_deploy_reads_group_zero_instead_of_legacy_file() {
    let g0 = spawn_group0().await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned release KV containers"]
async fn destroy_reads_group_zero_and_requires_launch_registry() {
    let g0 = spawn_group0().await;
    std::fs::write(&g0.config_path, "invalid local topology").unwrap();
    let (code, _, stderr) = run(
        &crowdb_cli_bin(),
        g0.mgmt_port,
        &g0.config_path,
        &["cluster", "destroy"],
    );
    assert_eq!(code, 2, "stderr={stderr}");
    assert!(stderr.contains("--registry"), "stderr={stderr}");

    let context = crowdb_console_shared::ops::OpContext::new(
        g0.rpc_url.trim_start_matches("http://").to_string(),
        vec![g0.mgmt_url.clone()],
        g0.bootstrap_config.clone(),
    );
    crowdb_console_shared::ops::cluster::init(&context, &[1])
        .await
        .expect("publish confirmed bootstrap metadata");

    let registry_path = g0.config_path.with_file_name("launches.toml");
    crowdb_console_shared::config::web::LaunchRegistry {
        version: 1,
        launches: Vec::new(),
    }
    .save(&registry_path)
    .unwrap();
    let output = std::process::Command::new(crowdb_cli_bin())
        .args([
            "--registry",
            registry_path.to_str().unwrap(),
            "--system-ip",
            &g0.system_ip,
            "--system-port",
        ])
        .arg(g0.mgmt_port.to_string())
        .args(["cluster", "destroy"])
        .env("CROWDB_CLI_STATE", &g0.config_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
