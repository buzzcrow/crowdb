// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::config::{ConsoleConfig, NodeEntry};
use crowdb_console_shared::error::Error;
use crowdb_console_shared::ops::{cluster as cluster_ops, hardware, OpContext};
use crowdb_test_harness::cluster::KvCluster;

#[path = "common/bootstrap_authority.rs"]
mod bootstrap_authority;

#[tokio::test]
async fn separate_consoles_confirm_matching_hardware_and_reject_conflicts() {
    let cluster = KvCluster::start().await;
    let bootstrap = bootstrap_authority::context(&cluster).await;
    cluster_ops::init(&bootstrap, &[1]).await.unwrap();

    let first = OpContext::new(
        cluster.group0_leader_endpoint.clone(),
        cluster.mgmt_endpoints.clone(),
        ConsoleConfig::default(),
    );
    let second = OpContext::new(
        cluster.group0_leader_endpoint.clone(),
        cluster.mgmt_endpoints.clone(),
        ConsoleConfig::default(),
    );

    hardware::add_rack_to_group0(&first, 2, "rack-two").await.unwrap();
    hardware::add_rack_to_group0(&second, 2, "rack-two")
        .await
        .unwrap();
    let conflict = hardware::add_rack_to_group0(&second, 2, "other")
        .await
        .unwrap_err();
    assert!(matches!(conflict, Error::Conflict { .. }), "{conflict:?}");
    assert_eq!(
        second.sysmd().get_rack(2).await.unwrap().unwrap().name,
        "rack-two"
    );
    assert_eq!(
        hardware::list_racks_from_group0(&second).await.unwrap()[1].name,
        "rack-two"
    );
    assert!(first.config().racks.is_empty());
    assert!(second.config().racks.is_empty());

    let node = NodeEntry {
        id: 2,
        rack_id: 2,
        host: "10.0.0.2".into(),
        ssh_port: 2222,
        ssh_user: "operator".into(),
        ssh_key: None,
        ssh_password: None,
        ssh_credential_ref: Some("ops-key".into()),
    };
    hardware::add_node_to_group0(&first, node.clone()).await.unwrap();
    hardware::add_node_to_group0(&second, node.clone()).await.unwrap();
    let changed = NodeEntry {
        host: "10.0.0.3".into(),
        ..node
    };
    let conflict = hardware::add_node_to_group0(&second, changed).await.unwrap_err();
    assert!(matches!(conflict, Error::Conflict { .. }), "{conflict:?}");
    let actual = second.sysmd().get_node(2, 2).await.unwrap().unwrap();
    assert_eq!(actual.management_host, "10.0.0.2");
    assert_eq!(actual.ssh_credential_ref.as_deref(), Some("ops-key"));
    let listed = hardware::list_nodes_from_group0(&second, Some(2)).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].host, "10.0.0.2");
    assert!(listed[0].ssh_key.is_none());
    assert!(listed[0].ssh_password.is_none());
    assert!(second.config().nodes.is_empty());
}
