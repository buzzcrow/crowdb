// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/memory_kv.rs"]
mod memory_kv;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crowdb_console_shared::ConsoleConfig;
use crowdb_paxos_kv_client::{ClientConfig, HardwareClient, PaxosKvClient};
use crowdb_protocol::{
    common::{DiskId, NodeValue, RackValue},
    diskdb::rpc::{DiskGroupValue, DiskValue},
    mgmt::{node::NodeBinding, SystemBootstrapIdentity},
};
use crowdb_web::{router, AppState};
use std::sync::Arc;
use tower::ServiceExt;

async fn read(app: &axum::Router, path: &str) -> serde_json::Value {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "{path}");
    serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 65536).await.unwrap()).unwrap()
}

#[tokio::test]
async fn bound_storage_reads_observe_monitor_publication_without_console_cache_refresh() {
    let cluster = memory_kv::TestKvCluster::start().await;
    let kv = Arc::new(PaxosKvClient::new(ClientConfig::new(
        cluster.mgmt_endpoints.clone(),
    )));
    kv.seed_leader(0, 0, cluster.group0_leader_endpoint.clone());
    let hw = HardwareClient::from_shared(kv.clone());
    hw.add_rack(
        1,
        &RackValue {
            node_ids: vec![2],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    hw.add_node(
        1,
        2,
        &NodeValue {
            disk_group_ids: vec![7],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let first = DiskId { high: 1, low: 1 };
    let second = DiskId { high: 1, low: 2 };
    let mut group = DiskGroupValue {
        disk_ids: vec![first],
        ..Default::default()
    };
    hw.add_disk_group(1, 2, 7, &group).await.unwrap();
    hw.add_disk(1, 2, 7, &first, &DiskValue::default()).await.unwrap();
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-storage-authority");
    std::fs::write(
        root.path().join("node-binding.json"),
        serde_json::to_vec(&NodeBinding {
            node_id: 2,
            bootstrap: SystemBootstrapIdentity {
                cluster_id: "storage".into(),
                operation_id: "bootstrap".into(),
                configuration_digest: "a".repeat(64),
            },
            management_seeds: cluster.mgmt_endpoints.clone(),
        })
        .unwrap(),
    )
    .unwrap();
    let state = AppState::with_runtime_root(ConsoleConfig::default(), root.path().join("console"))
        .with_node_monitor("http://127.0.0.1:1")
        .unwrap();
    *state.kv_client.write().await = Some(kv);
    let app = router(state.clone());
    assert!(state.monitor_cache.snapshot().await.is_empty());
    assert_eq!(read(&app, "/api/nodes/2/disk-groups").await[0]["id"], 7);
    assert_eq!(
        read(&app, "/api/nodes/2/disk-groups/7/disks")
            .await
            .as_array()
            .unwrap()
            .len(),
        1
    );
    hw.add_disk(1, 2, 7, &second, &DiskValue::default())
        .await
        .unwrap();
    group.disk_ids.push(second);
    hw.add_disk_group(1, 2, 7, &group).await.unwrap();
    assert!(
        state.config.read().unwrap().disks.is_empty(),
        "the console has no refreshed local projection"
    );
    assert_eq!(
        read(&app, "/api/nodes/2/disk-groups/7/disks")
            .await
            .as_array()
            .unwrap()
            .len(),
        2
    );
    verify_retained_backing(&app, &hw).await;
    let capacity = read(&app, "/api/hardware/capacity").await;
    assert_eq!(capacity["disk_groups"][0]["disks"].as_array().unwrap().len(), 2);
    assert_eq!(read(&app, "/api/nodes/2/disk-groups/7").await["id"], 7);
    assert_eq!(
        read(
            &app,
            "/api/nodes/2/disk-groups/7/disks/0000000000000001-0000000000000002"
        )
        .await["disk_id"],
        "0000000000000001-0000000000000002"
    );
}

async fn verify_retained_backing(app: &axum::Router, hw: &HardwareClient) {
    hw.add_node(
        1,
        2,
        &NodeValue {
            disk_group_ids: vec![7, 8],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    hw.add_disk_group(1, 2, 8, &DiskGroupValue::default())
        .await
        .unwrap();
    let backing_path = "/api/nodes/2/disk-groups/8/disks/ffffffffffffffff-ffffffffffffffff/backing";
    assert!(read(app, backing_path).await.is_null());
    let record = crowdb_protocol::disk_storage::DiskStorageRecord {
        version: 1,
        physical_backing: None,
        operation_id: "backing-observation".into(),
        rack_id: 1,
        node_id: 2,
        disk_group_id: 8,
        disk_id: DiskId {
            high: u64::MAX,
            low: u64::MAX,
        },
        backend: crowdb_protocol::mgmt::node::storage::DiskBackend::File,
        backing_identity: "file:1:9007199254740993".into(),
        media_type: None,
        sparse: true,
        geometry: DiskValue {
            device_path: "/opt/crowdb/data/disks/backing.img".into(),
            capacity_units: 4096,
            unit_size_bytes: 4096,
            zone_size_units: 1024,
            zone_count: 4,
            ..Default::default()
        },
        removed: false,
    };
    hw.admit_disk_storage(&record).await.unwrap();
    let backing = read(app, backing_path).await;
    assert_eq!(backing["backing_identity"], record.backing_identity);
    assert_eq!(backing["capacity_units"], "4096");
    assert_eq!(backing["zone_size_units"], "1024");
    assert!(backing["media_type"].is_null());
    assert_eq!(backing["backend"], "file");
    assert_eq!(backing["sparse"], true);
    assert_eq!(backing["operation_id"], "backing-observation");
}
