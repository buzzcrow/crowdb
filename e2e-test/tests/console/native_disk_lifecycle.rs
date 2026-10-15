// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Real allocation, durable data I/O and retained Offline removal after the browser.

use super::native_context::TestNativeContext;
use bytes::Bytes;
use crowdb_diskdb_client::{DiskdbClient, DiskdbRpcTransport};
use crowdb_diskio_client::{
    DiskId as IoDiskId, DiskioClient, DiskioClientConfig, Durability, OperationOptions, SegmentTarget,
};
use crowdb_kv_client::{GetOutcome, HardwareClient, ReadMode, ServiceRegistryClient};
use crowdb_protocol::{
    diskdb::rpc::{AllocateBlocksRequest, CommitBlocksRequest, Segment},
    key::{BinaryKey, BusyBlockKey, DiskKey},
    DiskIdExt,
};
use reqwest::Method;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

const GROUP: u64 = 592;

fn allocation() -> AllocateBlocksRequest {
    AllocateBlocksRequest {
        disk_group_id: GROUP,
        unit_count: 1,
        count: 1,
        exclude_disk_ids: vec![],
        owner_chunk: Some(crowdb_protocol::common::ChunkId { high: 0x235, low: 1 }),
        allow_disk_reuse: false,
    }
}

pub(super) async fn remove_occupied(context: &TestNativeContext) {
    let allocator = DiskdbClient::new(
        ServiceRegistryClient::from_shared(context.kv.clone()),
        Arc::new(DiskdbRpcTransport::new()),
    );
    allocator.refresh_endpoints().await.unwrap();
    let segments = allocator.allocate_blocks(allocation()).await.unwrap().segments;
    assert_eq!(segments.len(), 1);
    let segment = segments[0];
    let key = DiskKey {
        rack_id: 1,
        node_id: 1,
        disk_group_id: GROUP,
        disk_id: segment.disk_id.unwrap(),
    };
    let hardware = HardwareClient::from_shared(context.kv.clone());
    let original = hardware.disk_storage(&key).await.unwrap().unwrap();
    assert!(!original.removed);
    let (io, target) = write_committed(context, &allocator, &key, &segment, &original).await;
    let metadata = allocation_record(context, &hardware, &key, &segment).await;
    let endpoint = format!(
        "/api/nodes/1/disk-groups/{GROUP}/disks/{}",
        key.disk_id.to_display_string()
    );
    let current = context
        .api(Method::GET, &format!("{endpoint}/lifecycle"), Value::Null, 200)
        .await;
    let offline = context
        .api(
            Method::POST,
            &format!("{endpoint}/offline"),
            json!({"expected_revision":current["disk_revision"]}),
            200,
        )
        .await;
    assert_eq!(offline["offline"], true);
    wait_excluded(context, &endpoint).await;
    assert!(
        io.read(target, 0, 4096, OperationOptions::within(Duration::from_secs(1)))
            .await
            .is_err(),
        "excluded data access must fail"
    );
    let removed = context
        .api(
            Method::POST,
            &format!("{endpoint}/removal"),
            json!({"expected_revision":offline["disk_revision"]}),
            200,
        )
        .await;
    assert_eq!(removed["removed"], true);
    let replay = context
        .api(
            Method::POST,
            &format!("{endpoint}/removal"),
            json!({"expected_revision":offline["disk_revision"]}),
            200,
        )
        .await;
    assert_eq!(replay, removed);
    let mut expected = original.clone();
    expected.removed = true;
    assert_eq!(hardware.disk_storage(&key).await.unwrap(), Some(expected));
    assert!(hardware.get_disk_with_revision(&key).await.unwrap().is_none());
    assert_eq!(
        allocation_record(context, &hardware, &key, &segment).await,
        metadata
    );
    verify_retained_marker(&original, &segment).await;
    let rejoined = rejoin_offline(context, &endpoint, &removed).await;
    assert_eq!(hardware.disk_storage(&key).await.unwrap(), Some(original.clone()));
    assert_eq!(
        allocation_record(context, &hardware, &key, &segment).await,
        metadata
    );
    assert!(
        io.read(target, 0, 4096, OperationOptions::within(Duration::from_secs(1)))
            .await
            .is_err(),
        "Rejoin restores Offline membership without opening data I/O"
    );
    let remaining = allocator.allocate_blocks(allocation()).await.unwrap().segments;
    assert_eq!(remaining.len(), 1);
    assert_ne!(remaining[0].disk_id, Some(key.disk_id));
    let online = online_retained(context, &endpoint, &rejoined, &io, target).await;
    assert_eq!(hardware.disk_storage(&key).await.unwrap(), Some(original));
    assert_eq!(
        allocation_record(context, &hardware, &key, &segment).await,
        metadata
    );
    println!(
        "retained occupied removal: {}",
        json!({"disk_id":key.disk_id.to_display_string(),"operation":removed,
        "allocation_retained":true,"bytes_retained":true,"rejoin":rejoined,"online":online,
        "online_read_retained_bytes":true,"remaining_allocation":remaining})
    );
}

async fn online_retained(
    context: &TestNativeContext,
    endpoint: &str,
    rejoined: &Value,
    io: &DiskioClient,
    target: SegmentTarget,
) -> Value {
    let accepted = context
        .api(
            Method::POST,
            &format!("{endpoint}/online"),
            json!({"expected_revision":rejoined["state"]["disk_revision"]}),
            202,
        )
        .await;
    assert_eq!(accepted["state"], "pending");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let state = context
                .api(Method::GET, &format!("{endpoint}/lifecycle"), Value::Null, 200)
                .await;
            assert_ne!(
                state["online"]["state"], "failed",
                "strict retained validation: {state}"
            );
            if state["online"]["state"] == "complete" && state["offline"] == false {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("accepted retained validation becomes authoritative Online");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match io
                .read(target, 0, 4096, OperationOptions::within(Duration::from_secs(1)))
                .await
            {
                Ok(bytes) => {
                    assert_eq!(bytes, Bytes::from(vec![b'Z'; 4096]));
                    break;
                }
                Err(error) => {
                    eprintln!("retained Online data open pending: {error}");
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("original fsynced bytes become readable after validated Online");
    accepted
}

async fn rejoin_offline(context: &TestNativeContext, endpoint: &str, removed: &Value) -> Value {
    let rejoined = context
        .api(
            Method::POST,
            &format!("{endpoint}/rejoin"),
            json!({"removal_operation_id":removed["operation_id"]}),
            200,
        )
        .await;
    assert_eq!(rejoined["state"]["offline"], true);
    assert_eq!(rejoined["state"]["removed"], false);
    rejoined
}

async fn write_committed(
    context: &TestNativeContext,
    allocator: &DiskdbClient,
    key: &DiskKey,
    segment: &Segment,
    original: &crowdb_protocol::disk_storage::DiskStorageRecord,
) -> (DiskioClient, SegmentTarget) {
    let io = DiskioClient::connect_disk(
        DiskioClientConfig {
            management_seeds: context
                .addresses
                .iter()
                .map(|address| format!("http://{address}:10000"))
                .collect(),
            normal_connections_per_endpoint: 1,
            priority_connections_per_endpoint: 1,
            rpc_workers: 1,
            max_pending_calls: 8,
            retry_attempts: 1,
            default_timeout: Duration::from_secs(1),
            ..Default::default()
        },
        key.clone(),
    )
    .await
    .unwrap();
    let target = SegmentTarget::new(
        IoDiskId::new(key.disk_id.high, key.disk_id.low),
        segment.zone_index,
        segment.unit_offset,
        segment.unit_count,
        original.geometry.unit_size_bytes,
    )
    .unwrap();
    let payload = Bytes::from(vec![b'Z'; 4096]);
    io.write(
        target,
        0,
        payload.clone(),
        Durability::Fsync,
        OperationOptions::within(Duration::from_secs(1)),
    )
    .await
    .unwrap();
    assert_eq!(
        io.read(target, 0, 4096, OperationOptions::within(Duration::from_secs(1)))
            .await
            .unwrap(),
        payload
    );
    assert_eq!(
        allocator
            .commit_blocks(CommitBlocksRequest {
                segments: vec![*segment]
            })
            .await
            .unwrap()
            .committed_count,
        1
    );
    (io, target)
}

async fn wait_excluded(context: &TestNativeContext, endpoint: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let state = context
                .api(Method::GET, &format!("{endpoint}/lifecycle"), Value::Null, 200)
                .await;
            if state["remove_conditions"].as_array().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("owner and DiskIO apply exact-revision exclusion");
}

async fn allocation_record(
    context: &TestNativeContext,
    hardware: &HardwareClient,
    key: &DiskKey,
    segment: &Segment,
) -> Bytes {
    let binding = hardware
        .get_bind(key.rack_id, key.node_id, key.disk_group_id)
        .await
        .unwrap()
        .unwrap();
    let record = BusyBlockKey {
        disk_id: key.disk_id,
        zone_index: segment.zone_index,
        unit_offset: segment.unit_offset,
    }
    .to_bytes();
    let GetOutcome::Found { value, .. } = context
        .kv
        .get(
            binding.store_id,
            binding.group_id,
            &record,
            ReadMode::Linearizable,
            None,
        )
        .await
        .unwrap()
    else {
        panic!("committed allocation must remain present")
    };
    value
}

async fn verify_retained_marker(
    original: &crowdb_protocol::disk_storage::DiskStorageRecord,
    segment: &Segment,
) {
    let offset = u64::from(segment.zone_index)
        * original.geometry.zone_size_units
        * u64::from(original.geometry.unit_size_bytes)
        + segment.unit_offset * u64::from(original.geometry.unit_size_bytes);
    let bytes = crate::common::control::request(json!({"action":"observe-retained-marker", "node":0,
        "path":original.geometry.device_path,"offset":offset}))
    .await;
    assert_eq!(bytes, json!({"matches":true,"length":4096}));
}
