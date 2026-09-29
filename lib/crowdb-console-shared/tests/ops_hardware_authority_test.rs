// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::config::{ConsoleConfig, NodeEntry};
use crowdb_console_shared::error::Error;
use crowdb_console_shared::ops::{cluster as cluster_ops, hardware, OpContext};
use crowdb_test_harness::cluster::KvCluster;
use std::sync::atomic::Ordering;

#[path = "common/bootstrap_authority.rs"]
mod bootstrap_authority;
#[path = "common/rpc_response_proxy.rs"]
mod rpc_response_proxy;

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
    second
        .sysmd()
        .set_node_status(2, 2, crowdb_protocol::common::HwStatus::Maintenance)
        .await
        .unwrap();
    hardware::add_node_to_group0(&first, node.clone()).await.unwrap();
    assert_eq!(
        first.sysmd().get_node(2, 2).await.unwrap().unwrap().status,
        crowdb_protocol::common::HwStatus::Maintenance as i32
    );
    assert_eq!(
        second.sysmd().get_rack(2).await.unwrap().unwrap().node_ids,
        vec![2]
    );
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

    let occupied = hardware::remove_rack_from_group0(&second, 2).await.unwrap_err();
    assert!(matches!(occupied, Error::Conflict { .. }), "{occupied:?}");
    assert!(second.sysmd().get_rack(2).await.unwrap().is_some());

    hardware::add_rack_to_group0(&first, 3, "empty").await.unwrap();
    hardware::remove_rack_from_group0(&second, 3).await.unwrap();
    assert!(first.sysmd().get_rack(3).await.unwrap().is_none());

    // A repeated rack create preserves its confirmed child membership.
    hardware::add_rack_to_group0(&first, 2, "rack-two").await.unwrap();
    assert_eq!(
        first.sysmd().get_rack(2).await.unwrap().unwrap().node_ids,
        vec![2]
    );

    let occupied = hardware::remove_node_from_group0(&second, 1).await.unwrap_err();
    assert!(matches!(occupied, Error::Conflict { .. }), "{occupied:?}");
    hardware::remove_node_from_group0(&second, 2).await.unwrap();
    assert!(first.sysmd().get_node(2, 2).await.unwrap().is_none());
    assert!(first
        .sysmd()
        .get_rack(2)
        .await
        .unwrap()
        .unwrap()
        .node_ids
        .is_empty());
    hardware::remove_rack_from_group0(&first, 2).await.unwrap();
    assert!(second.sysmd().get_rack(2).await.unwrap().is_none());
}

#[tokio::test]
async fn concurrent_node_creates_preserve_both_rack_memberships() {
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
    hardware::add_rack_to_group0(&first, 7, "rack-seven")
        .await
        .unwrap();
    let node = |id| NodeEntry {
        id,
        rack_id: 7,
        host: format!("10.0.0.{id}"),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
        ssh_credential_ref: None,
    };
    let (a, b) = tokio::join!(
        hardware::add_node_to_group0(&first, node(8)),
        hardware::add_node_to_group0(&second, node(9))
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(
        first.sysmd().get_rack(7).await.unwrap().unwrap().node_ids,
        vec![8, 9]
    );
    assert_eq!(first.sysmd().list_nodes_in_rack(7).await.unwrap().len(), 2);
}

#[tokio::test]
async fn committed_rack_survives_a_lost_conditional_write_response() {
    let cluster = KvCluster::start().await;
    let proxy = rpc_response_proxy::TestResponseProxy::start(cluster.group0_leader_endpoint.clone()).await;
    let ctx = OpContext::new(
        proxy.endpoint.clone(),
        vec![proxy.management_endpoint.clone()],
        ConsoleConfig::default(),
    );
    proxy.armed.store(true, Ordering::SeqCst);
    hardware::add_rack_to_group0(&ctx, 9, "rack-nine").await.unwrap();
    assert_eq!(proxy.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(ctx.sysmd().get_rack(9).await.unwrap().unwrap().name, "rack-nine");
}

#[tokio::test]
async fn two_consoles_share_disk_groups_and_disks_without_local_topology() {
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
    hardware::add_rack_to_group0(&first, 12, "storage").await.unwrap();
    hardware::add_node_to_group0(
        &first,
        NodeEntry {
            id: 12,
            rack_id: 12,
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_key: None,
            ssh_password: None,
            ssh_credential_ref: None,
        },
    )
    .await
    .unwrap();
    hardware::add_disk_group_to_group0(&first, 12, 5, "hot")
        .await
        .unwrap();
    hardware::add_disk_group_to_group0(&second, 12, 5, "hot")
        .await
        .unwrap();
    let conflict = hardware::add_disk_group_to_group0(&second, 12, 5, "cold")
        .await
        .unwrap_err();
    assert!(matches!(conflict, Error::Conflict { .. }));
    assert_eq!(
        hardware::list_disk_groups_from_group0(&second, 12).await.unwrap()[0].name,
        "hot"
    );
    let disk = hardware::AddDiskInput {
        disk_id: "0000000000000000-000000000000000c".into(),
        disk_type: "Ssd".into(),
        capacity_bytes: 4096,
        zone_size_bytes: 4096,
        unit_size_bytes: 4096,
        device_path: "/dev/test".into(),
    };
    hardware::add_disk_to_group0(&first, 12, 5, &disk).await.unwrap();
    hardware::add_disk_to_group0(&second, 12, 5, &disk).await.unwrap();
    assert_eq!(
        hardware::list_disks_from_group0(&second, 12, 5)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(matches!(
        hardware::remove_disk_group_from_group0(&second, 12, 5).await,
        Err(Error::Conflict { .. })
    ));
    assert!(matches!(
        hardware::remove_node_from_group0(&second, 12).await,
        Err(Error::Conflict { .. })
    ));
    hardware::remove_disk_from_group0(&second, 12, 5, &disk.disk_id)
        .await
        .unwrap();
    hardware::remove_disk_group_from_group0(&first, 12, 5)
        .await
        .unwrap();
    hardware::remove_node_from_group0(&second, 12).await.unwrap();
    assert!(first.config().disk_groups.is_empty());
    assert!(second.config().disks.is_empty());
}

#[tokio::test]
async fn committed_disk_group_survives_a_lost_response() {
    let cluster = KvCluster::start().await;
    let bootstrap = bootstrap_authority::context(&cluster).await;
    cluster_ops::init(&bootstrap, &[1]).await.unwrap();
    let proxy = rpc_response_proxy::TestResponseProxy::start(cluster.group0_leader_endpoint.clone()).await;
    let ctx = OpContext::new(
        proxy.endpoint.clone(),
        vec![proxy.management_endpoint.clone()],
        ConsoleConfig::default(),
    );
    hardware::add_rack_to_group0(&ctx, 13, "storage").await.unwrap();
    hardware::add_node_to_group0(
        &ctx,
        NodeEntry {
            id: 13,
            rack_id: 13,
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_key: None,
            ssh_password: None,
            ssh_credential_ref: None,
        },
    )
    .await
    .unwrap();
    proxy.armed.store(true, Ordering::SeqCst);
    hardware::add_disk_group_to_group0(&ctx, 13, 1, "recovered")
        .await
        .unwrap();
    assert_eq!(proxy.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        hardware::list_disk_groups_from_group0(&ctx, 13).await.unwrap()[0].name,
        "recovered"
    );
}
