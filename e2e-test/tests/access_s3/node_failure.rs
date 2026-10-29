// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Owns persistent/protected mini-cluster restart and three joint node faults.
//! Local launch-state proofs stay in Console-shared; Web health and Group-0
//! preview belong to the Console container flows.

use crowdb_console_shared::ops::s3::S3HttpClient;
use crowdb_kv_client::HardwareClient;
use crowdb_protocol::common::{DiskId, HwStatus, NodeValue, RackValue};
use crowdb_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};
use reqwest::Method;

mod recovery;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned three-node protected storage profile"]
async fn protected_restart_preserves_objects_and_fixed_endpoints() {
    let artifacts = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let manifest = std::fs::read(artifacts.join("s3-node-endpoints.json")).unwrap();
    let endpoints: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
    let nodes = endpoints["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 3);
    for (index, node) in nodes.iter().enumerate() {
        assert_eq!(node["rack"], index + 1);
        assert_eq!(node["node"], index + 1);
        assert_eq!(node["services"].as_array().unwrap().len(), 4);
    }
    let client = S3HttpClient::new(endpoints["endpoint"].as_str().unwrap().to_owned()).unwrap();
    client
        .request(Method::PUT, Some("durable-bucket"), None, &[], None, None)
        .await
        .unwrap();
    client
        .request(
            Method::PUT,
            Some("durable-bucket"),
            Some("nested/object"),
            &[],
            Some(b"durable-object-bytes".to_vec()),
            None,
        )
        .await
        .unwrap();
    recovery::restart().await;
    assert_eq!(
        std::fs::read(artifacts.join("s3-node-endpoints.json")).unwrap(),
        manifest
    );
    let (_, bytes) = client
        .request(
            Method::GET,
            Some("durable-bucket"),
            Some("nested/object"),
            &[],
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(bytes, b"durable-object-bytes");
    let (_, range) = client
        .request(
            Method::GET,
            Some("durable-bucket"),
            Some("nested/object"),
            &[],
            None,
            Some((8, 13)),
        )
        .await
        .unwrap();
    assert_eq!(range, b"object");
}

fn kv_client() -> crowdb_kv_client::CrowdbKvClient {
    let addresses: Vec<String> = serde_json::from_str(&std::env::var("CROWDB_E2E_RPC").unwrap()).unwrap();
    crowdb_kv_client::CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(
        addresses[..3]
            .iter()
            .map(|address| format!("http://{address}:7000"))
            .collect(),
    ))
}

#[tokio::test]
#[ignore = "requires three owned production storage nodes"]
async fn seed() {
    if std::env::var("CROWDB_E2E_PHASE").as_deref() == Ok("storage-ready") {
        recovery::storage_ready().await;
        return;
    }
    if std::env::var("CROWDB_E2E_PHASE").as_deref() == Ok("seed") {
        crate::chunkdb::voting::register_actual_voters().await;
        let maps = crowdb_kv_client::ChunkSlotMapClient::new(std::sync::Arc::new(
            crate::common::hardware::kv_client(),
        ));
        maps.initialize_layout(&crowdb_protocol::chunk_slot::ChunkSlotBootstrap {
            service_instances: vec![20000, 20001, 20002],
            storage_groups: (1..=3)
                .map(|group_id| crowdb_protocol::chunk_slot::ChunkStorageGroup {
                    store_id: 0,
                    group_id,
                })
                .collect(),
        })
        .await
        .unwrap();
        maps.initialize_service_epochs().await.unwrap();
        return;
    }
    assert_eq!(std::env::var("CROWDB_E2E_PHASE").as_deref(), Ok("storage-seed"));
    seed_hardware().await;
}

async fn seed_hardware() {
    let hardware = HardwareClient::new(crate::common::hardware::kv_client());
    for node in 1..=3 {
        let disk = DiskId { high: 0, low: node };
        hardware
            .add_rack(
                node,
                &RackValue {
                    status: HwStatus::Up as i32,
                    node_ids: vec![node],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        hardware
            .add_node(
                node,
                node,
                &NodeValue {
                    status: HwStatus::Up as i32,
                    disk_group_ids: vec![node],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        hardware
            .add_disk_group(
                node,
                node,
                node,
                &DiskGroupValue {
                    status: HwStatus::Up as i32,
                    disk_ids: vec![disk],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        hardware
            .add_disk(
                node,
                node,
                node,
                &disk,
                &DiskValue {
                    disk_type: DiskType::BlockSsd as i32,
                    capacity_units: 5120,
                    zone_size_units: 5120,
                    unit_size_bytes: 1024 * 1024,
                    zone_count: 1,
                    status: HwStatus::Up as i32,
                    device_path: String::new(),
                },
            )
            .await
            .unwrap();
        hardware
            .set_owner(node, node, node, 10000 + node, now_ms() + 3_600_000)
            .await
            .unwrap();
        hardware.set_bind(node, node, node, 0, 1).await.unwrap();
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned three-node storage fault profile"]
async fn node_one_outage_preserves_existing_and_new_objects() {
    outage(1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned three-node storage fault profile"]
async fn node_two_outage_preserves_existing_and_new_objects() {
    outage(2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned three-node storage fault profile"]
async fn node_three_outage_preserves_existing_and_new_objects() {
    outage(3).await;
}

async fn outage(failed_node: u64) {
    let artifacts = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let endpoints: serde_json::Value =
        serde_json::from_slice(&std::fs::read(artifacts.join("s3-node-endpoints.json")).unwrap()).unwrap();
    let client = S3HttpClient::new(endpoints["endpoint"].as_str().unwrap().to_owned()).unwrap();
    client
        .request(Method::PUT, Some("outage-bucket"), None, &[], None, None)
        .await
        .unwrap();
    client
        .request(
            Method::PUT,
            Some("outage-bucket"),
            Some("before-outage"),
            &[],
            Some(b"before-outage-bytes".to_vec()),
            None,
        )
        .await
        .unwrap();
    let logical = usize::try_from(failed_node - 1).unwrap();
    // Stop storage before its voter, preserving the original graceful process-exit ordering.
    for index in [logical + 6, logical + 3, logical + 9, logical] {
        crate::common::control::node("stop", index).await;
    }
    let kv = std::sync::Arc::new(kv_client());
    let hardware = HardwareClient::from_shared(kv.clone());
    hardware
        .set_node_status(failed_node, failed_node, HwStatus::Offline)
        .await
        .unwrap();
    recovery::ownership(kv, hardware, failed_node).await;
    let (_, old) = client
        .request(
            Method::GET,
            Some("outage-bucket"),
            Some("before-outage"),
            &[],
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(old, b"before-outage-bytes");
    let bytes = vec![0x5a; 2 * 1024 * 1024];
    client
        .request(
            Method::PUT,
            Some("outage-bucket"),
            Some("during-outage"),
            &[],
            Some(bytes.clone()),
            None,
        )
        .await
        .unwrap();
    let (_, read) = client
        .request(
            Method::GET,
            Some("outage-bucket"),
            Some("during-outage"),
            &[],
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(read, bytes);
    if failed_node == 2 {
        let chunks = recovery::degraded_chunks(&crate::common::management::addresses(14)).await;
        recovery::restart().await;
        let (_, recovered) = client
            .request(
                Method::GET,
                Some("outage-bucket"),
                Some("during-outage"),
                &[],
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(recovered, bytes);
        recovery::repaired(&chunks).await;
    }
}
