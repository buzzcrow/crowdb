// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::native_context::TestNativeContext;
use crowdb_paxos_kv_client::HardwareClient;
use reqwest::Method;
use serde_json::json;
use std::time::Duration;

async fn group(context: &TestNativeContext, store: u64, replica: u64) {
    context
        .api(
            Method::POST,
            &format!("/api/stores/{store}/groups"),
            json!({"group_id":1,"replica_id":replica,"nodes":[1,2,3]}),
            201,
        )
        .await;
}

async fn disk_group(context: &TestNativeContext, node: u64) {
    context
        .api(
            Method::POST,
            &format!("/api/nodes/{node}/disk-groups"),
            json!({"id":node,"store_id":u64::from(node == 1),"group_id":1,"name":"storage"}),
            201,
        )
        .await;
}

async fn wait_owner(hardware: &HardwareClient, node: u64) {
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            interval.tick().await;
            if hardware.get_owner(1, node, node).await.unwrap().is_some() {
                return;
            }
        }
    })
    .await
    .unwrap_or_else(|error| panic!("live DiskDB acquires Node {node} disk group: {error}"));
}

async fn repair(context: &TestNativeContext, hardware: &HardwareClient, node: u64, store: u64) {
    hardware.remove_bind(1, node, node).await.unwrap();
    hardware.remove_owner(1, node, node).await.unwrap();
    assert!(hardware.get_bind(1, node, node).await.unwrap().is_none());
    assert!(hardware.get_owner(1, node, node).await.unwrap().is_none());
    let response = context
        .http
        .post(format!("{}/api/nodes/{node}/disk-groups", context.base))
        .json(&json!({"id":node,"store_id":store,"group_id":1,"name":"storage"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 500);
    assert!(response
        .text()
        .await
        .unwrap()
        .contains("Existing DiskGroup binding differs or is absent"));
    context
        .api(
            Method::PUT,
            &format!("/api/disk-groups/1/{node}/{node}/bind"),
            json!({"store_id":store,"group_id":1}),
            204,
        )
        .await;
}

async fn prepare_binding(context: &TestNativeContext, hardware: &HardwareClient) {
    context
        .api(
            Method::POST,
            "/api/stores",
            json!({"store_id":1,"nodes":[1,2,3]}),
            201,
        )
        .await;
    group(context, 1, 10).await;
    disk_group(context, 1).await;
    let binding = hardware.get_bind(1, 1, 1).await.unwrap().unwrap();
    assert_eq!((binding.store_id, binding.group_id), (1, 1));
    group(context, 0, 20).await;
    let binding = hardware.get_bind(1, 1, 1).await.unwrap().unwrap();
    assert_eq!((binding.store_id, binding.group_id), (1, 1));
    assert!(hardware.get_owner(1, 1, 1).await.unwrap().is_none());
}

async fn start_storage(context: &TestNativeContext, hardware: &HardwareClient) {
    for node in 1..=3 {
        context
            .api(
                Method::POST,
                &format!("/api/nodes/{node}/diskdb/deploy"),
                json!({"rpc_port":7100,"keepalive_interval_secs":1}),
                201,
            )
            .await;
    }
    for node in 1..=3 {
        disk_group(context, node).await;
    }
    // Repair an empty group before media admission seals its binding.
    wait_owner(hardware, 3).await;
    repair(context, hardware, 3, 0).await;
    super::native_storage::disks(context, [128 * 1024; 3]).await;
}

async fn verify_bindings(context: &TestNativeContext, hardware: &HardwareClient) {
    for node in 1..=3 {
        wait_owner(hardware, node).await;
        let store = u64::from(node == 1);
        let binding = hardware.get_bind(1, node, node).await.unwrap().unwrap();
        assert_eq!((binding.store_id, binding.group_id), (store, 1));
        let previous = hardware.get_owner(1, node, node).await.unwrap().unwrap();
        if node == 3 {
            let error = hardware.remove_bind(1, node, node).await.unwrap_err();
            assert!(error
                .to_string()
                .contains("retained disks require the Offline/Remove lifecycle"));
        }
        disk_group(context, node).await;
        wait_owner(hardware, node).await;
        let renewed = hardware.get_owner(1, node, node).await.unwrap().unwrap();
        assert_eq!((renewed.rack_id, renewed.node_id, renewed.dg_id), (1, node, node));
        assert_eq!(renewed.instance_id, previous.instance_id);
        let binding = hardware.get_bind(1, node, node).await.unwrap().unwrap();
        assert_eq!((binding.store_id, binding.group_id), (store, 1));
        assert!(renewed.lease_expiry_ms >= previous.lease_expiry_ms);
    }
}

#[tokio::test]
#[ignore = "requires three admitted release nodes and normal DiskDB deployment"]
async fn prebound_groups_acquire_live_owners_and_require_explicit_binding_repair() {
    let context = TestNativeContext::connect().await;
    let hardware = HardwareClient::from_shared(context.kv.clone());
    prepare_binding(&context, &hardware).await;
    start_storage(&context, &hardware).await;
    verify_bindings(&context, &hardware).await;
}
