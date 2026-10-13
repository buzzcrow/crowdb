// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{bootstrap_authority, fixture};
use crowdb_console_shared::ConsoleConfig;
use std::path::PathBuf;

pub(super) struct TestGroup0 {
    pub system_ip: String,
    pub mgmt_url: String,
    pub rpc_url: String,
    pub mgmt_port: u16,
    pub config_path: PathBuf,
    pub bootstrap_config: ConsoleConfig,
}

pub(super) fn crowdb_cli_bin() -> PathBuf {
    let path = PathBuf::from(std::env::var("CROWDB_E2E_CLI_BIN").expect("pinned host CLI"));
    assert!(path.is_file(), "missing CLI {}", path.display());
    path
}

pub(super) async fn spawn_group0() -> TestGroup0 {
    let cluster = fixture::TestKvCluster::start().await;
    let context = bootstrap_authority::context(&cluster).await;
    context.config_mut().racks[0].name = "rack-1".into();
    crowdb_console_shared::ops::hardware::add_rack_to_group0(&context, 1, "rack-1")
        .await
        .unwrap();
    let node = context.config().nodes[0].clone();
    crowdb_console_shared::ops::hardware::add_node_to_group0(&context, node)
        .await
        .unwrap();
    let bootstrap_config = context.config().clone();
    TestGroup0 {
        system_ip: cluster.mgmt_endpoints[0]
            .trim_start_matches("http://")
            .trim_end_matches(":7000")
            .into(),
        mgmt_url: cluster.mgmt_endpoints[0].clone(),
        rpc_url: cluster.group0_leader_endpoint,
        mgmt_port: 7000,
        config_path: fixture::client_data_dir("cli").join("legacy-console.toml"),
        bootstrap_config,
    }
}

pub(super) fn run(cli: &PathBuf, port: u16, state: &PathBuf, args: &[&str]) -> (i32, String, String) {
    let addresses = crate::common::management::addresses(3);
    let output = std::process::Command::new(cli)
        .current_dir(state.parent().unwrap())
        .args(["--system-ip", &addresses[0], "--system-port", &port.to_string()])
        .env("CROWDB_CLI_STATE", state)
        .args(args)
        .output()
        .expect("run pinned host CLI");
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}
