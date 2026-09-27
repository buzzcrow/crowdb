// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::config::{ConsoleConfig, NodeEntry, RackEntry, ServerEntry, ServiceType};
use crowdb_web::mgmt::startup_topology_check;
use crowdb_web::AppState;

#[tokio::test]
async fn startup_does_not_replay_local_diskdb_launch_policy_without_group0() {
    let mut config = ConsoleConfig::default();
    config
        .add_rack(RackEntry {
            id: 1,
            name: "test-rack".into(),
        })
        .unwrap();
    config
        .add_node(NodeEntry {
            id: 7777,
            rack_id: 1,
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_key: None,
            ssh_password: None,
        })
        .unwrap();
    config
        .add_server(ServerEntry {
            id: "diskdb-7777".into(),
            url: "http://127.0.0.1:1".into(),
            node_id: Some(7777),
            rpc_url: None,
            rest_port: None,
            rpc_port: Some(1),
            auto_start: true,
            binary: None,
            election_profile: None,
            pid: None,
            service_type: ServiceType::Diskdb,
            rpc_workers: None,
            no_fsync: false,
        })
        .unwrap();

    let state = AppState::with_config(config, None);
    startup_topology_check(&state).await;
    assert_eq!(state.diskdb_runtime_pid(7777), None);
}
