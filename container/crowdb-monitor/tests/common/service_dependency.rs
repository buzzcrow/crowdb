// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_paxos_kv_client::{HardwareClient, PaxosKvClient};
use crowdb_protocol::{
    common::{DiskId, HwStatus, NodeValue, RackValue},
    diskdb::rpc::{DiskGroupValue, DiskValue},
};
use std::sync::Arc;

pub async fn publish_online_disk(kv: Arc<PaxosKvClient>) {
    let hardware = HardwareClient::from_shared(kv.clone());
    hardware
        .add_rack(
            1,
            &RackValue {
                node_ids: vec![2],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    hardware
        .add_node(
            1,
            2,
            &NodeValue {
                disk_group_ids: vec![7],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let disk = DiskId { high: 0, low: 7 };
    hardware
        .add_disk_group(
            1,
            2,
            7,
            &DiskGroupValue {
                disk_ids: vec![disk],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    hardware
        .add_disk(
            1,
            2,
            7,
            &disk,
            &DiskValue {
                status: HwStatus::Up as i32,
                ..Default::default()
            },
        )
        .await
        .unwrap();
}

pub async fn ready_listener() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().route("/ready", axum::routing::get(|| async { "ok" })),
        )
        .await
        .unwrap();
    });
    (address, server)
}

pub async fn assert_initial_start_waits(
    kv: &PaxosKvClient,
    profile: &crowdb_monitor::DeploymentProfile,
    local: &crowdb_protocol::mgmt::node::NodeServiceIntent,
    consumer: &crowdb_protocol::mgmt::node::NodeServiceIntent,
) {
    for intent in [local, consumer] {
        kv.put(
            0,
            0,
            format!("/deployment/services/{}/{}", intent.node_id, intent.service_id).as_bytes(),
            &serde_json::to_vec(intent).unwrap(),
            None,
        )
        .await
        .unwrap();
    }
    std::fs::create_dir_all(&profile.paths.log_root).unwrap();
    let mut runtime = crowdb_monitor::NodeServiceForTests::new_for_tests(profile.clone())
        .await
        .unwrap();
    let error = runtime.execute_for_tests(consumer.clone()).await.unwrap_err();
    assert!(
        error.to_string().contains("has not loaded"),
        "initial deployment bypassed dependency readiness: {error}"
    );
    assert!(
        !profile
            .paths
            .data_root
            .join("services")
            .join(&consumer.service_id)
            .join("execution.json")
            .exists(),
        "waiting for dependencies cannot consume a launch attempt"
    );
}
