// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv_server::background::domain_monitor::{ChunkdbRangeMonitorDriver, DomainMonitorDriver};
use crowdb_kv_server::group0_control_plane::Group0ControlPlane;
use crowdb_protocol::chunk_kv::{DomainFailurePolicy, DomainMonitorDescriptor};
use crowdb_protocol::chunk_slot::{ChunkSlot, ChunkSlotBinding, ChunkSlotMapHead, ChunkStorageGroup};
use crowdb_protocol::common::InstanceValue;
use crowdb_protocol::key::{
    ChunkServiceSlotsKey, ChunkSlotMapHeadKey, ChunkStorageSlotsKey, InstanceKey, TextKey,
};

fn descriptor() -> DomainMonitorDescriptor {
    DomainMonitorDescriptor {
        domain: "chunkdb".into(),
        service_registry_name: "chunkdb".into(),
        driver_version: 2,
        failure_policy: DomainFailurePolicy::OperatorOnly,
        balance_policy: "fixed-slots-v1".into(),
        ..Default::default()
    }
}

async fn control() -> (Arc<PxKvStore>, Group0ControlPlane) {
    let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
    store.add_group(PxGroup::new(
        0,
        PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
    ));
    let control = Group0ControlPlane::acquire(&store).await.unwrap();
    (store, control)
}

fn entry(key: String, value: &impl serde::Serialize) -> (Bytes, Bytes) {
    (Bytes::from(key), Bytes::from(serde_json::to_vec(value).unwrap()))
}

async fn initialize(control: &Group0ControlPlane) {
    let mut records = vec![
        entry(
            ChunkSlotMapHeadKey::Service.to_path(),
            &ChunkSlotMapHead {
                layout_version: 1,
                generation: 1,
                owner_count: 3,
            },
        ),
        entry(
            ChunkSlotMapHeadKey::Storage.to_path(),
            &ChunkSlotMapHead {
                layout_version: 1,
                generation: 9,
                owner_count: 3,
            },
        ),
    ];
    for owner in 1..=3 {
        records.push(entry(
            ChunkServiceSlotsKey { instance_id: owner }.to_path(),
            &ChunkSlotBinding {
                generation: 1,
                owner,
                slots: ChunkSlot::all()
                    .filter(|slot| u64::from(slot.value()) % 3 + 1 == owner)
                    .collect(),
            },
        ));
        records.push(entry(
            ChunkStorageSlotsKey {
                store_id: 0,
                group_id: owner,
            }
            .to_path(),
            &ChunkSlotBinding {
                generation: 9,
                owner: ChunkStorageGroup {
                    store_id: 0,
                    group_id: owner,
                },
                slots: ChunkSlot::all()
                    .filter(|slot| u64::from(slot.value() / 16) % 3 + 1 == owner)
                    .collect(),
            },
        ));
    }
    control.put_batch(records).await.unwrap();
}

#[tokio::test]
async fn heartbeats_joins_and_absent_owners_never_reassign_slots() {
    let (_store, control) = control().await;
    initialize(&control).await;
    let before = control
        .scan_all_prefix(Bytes::from_static(b"/chunkdb/"), 2)
        .await
        .unwrap();
    assert_eq!(before.len(), 8);
    let driver = ChunkdbRangeMonitorDriver::new();
    driver.tick(&control, &descriptor()).await.unwrap();
    for (instance_id, heartbeat) in [(1, 0), (4, u64::MAX), (1, u64::MAX)] {
        control
            .put_batch(vec![entry(
                InstanceKey {
                    service: "chunkdb".into(),
                    instance_id,
                }
                .to_path(),
                &InstanceValue {
                    instance_id,
                    rpc_endpoint: format!("127.0.0.1:{}", 17000 + instance_id),
                    last_heartbeat_ms: heartbeat,
                    extra: None,
                },
            )])
            .await
            .unwrap();
        driver.tick(&control, &descriptor()).await.unwrap();
        let after = control
            .scan_all_prefix(Bytes::from_static(b"/chunkdb/"), 2)
            .await
            .unwrap();
        assert_eq!(before, after);
    }
}

#[tokio::test]
async fn missing_or_legacy_layout_is_not_automatically_initialized() {
    let (_store, control) = control().await;
    let driver = ChunkdbRangeMonitorDriver::new();
    assert!(driver.tick(&control, &descriptor()).await.is_err());
    control
        .put_batch(vec![(
            Bytes::from_static(b"/chunkdb/range_bind/0"),
            Bytes::from_static(b"legacy"),
        )])
        .await
        .unwrap();
    let before = control
        .scan_all_prefix(Bytes::from_static(b"/chunkdb/"), 2)
        .await
        .unwrap();
    assert!(driver.tick(&control, &descriptor()).await.is_err());
    assert_eq!(
        before,
        control
            .scan_all_prefix(Bytes::from_static(b"/chunkdb/"), 2)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn rejects_automatic_policy_and_corrupt_binding_without_repairing_it() {
    let (_store, control) = control().await;
    initialize(&control).await;
    let driver = ChunkdbRangeMonitorDriver::new();
    let mut automatic = descriptor();
    automatic.failure_policy = DomainFailurePolicy::AutomaticSharedStorage;
    assert!(driver.tick(&control, &automatic).await.is_err());
    control
        .put_batch(vec![entry(
            ChunkServiceSlotsKey { instance_id: 1 }.to_path(),
            &ChunkSlotBinding {
                generation: 2,
                owner: 1u64,
                slots: ChunkSlot::all().collect(),
            },
        )])
        .await
        .unwrap();
    let before = control
        .scan_all_prefix(Bytes::from_static(b"/chunkdb/"), 2)
        .await
        .unwrap();
    assert!(driver.tick(&control, &descriptor()).await.is_err());
    assert_eq!(
        before,
        control
            .scan_all_prefix(Bytes::from_static(b"/chunkdb/"), 2)
            .await
            .unwrap()
    );
}
