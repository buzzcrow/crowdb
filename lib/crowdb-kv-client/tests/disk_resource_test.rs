// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/storage.rs"]
mod storage;
use crowdb_kv_client::{GetOutcome, ReadMode};
use crowdb_protocol::{
    common::{NodeValue, RackValue},
    disk_storage::{DiskStorageRecord, PhysicalBacking},
    diskdb::rpc::DiskGroupValue,
    mgmt::node::storage::DiskBackend,
};
use storage::{key, record, TestStorage};

async fn registry(test: &TestStorage, hosts: &[(u64, u64, &str)], confirmed: bool) {
    let nodes: Vec<_> = hosts
        .iter()
        .map(|(node, rack, host)| {
            serde_json::json!({
                "node_id":node,"rack_id":rack,"physical_host_id":host,"confirmed":confirmed,"cancelled":false,
                "discovery_id":format!("00000000-0000-4000-8000-{node:012x}"),
                "operation_id":format!("10000000-0000-4000-8000-{node:012x}"),
                "host":"127.0.0.1","ssh_port":22,"ssh_user":"fixture",
            })
        })
        .collect();
    test.hardware
        .kv()
        .put(
            0,
            0,
            b"/deployment/nodes",
            &serde_json::to_vec(&serde_json::json!({"cluster_id":"owned-cluster","nodes":nodes})).unwrap(),
            None,
        )
        .await
        .unwrap();
}

fn physical(mut record: DiskStorageRecord, host: &str, resource: &str) -> DiskStorageRecord {
    record.version = 2;
    record.physical_backing = Some(PhysicalBacking {
        physical_host_id: host.into(),
        resource_identity: resource.into(),
        capacity_bytes: record.physical_capacity_bytes().unwrap(),
    });
    record
}

async fn peer(test: &TestStorage) -> DiskStorageRecord {
    let hw = &test.hardware;
    hw.add_rack(
        4,
        &RackValue {
            node_ids: vec![5],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    hw.add_node(
        4,
        5,
        &NodeValue {
            disk_group_ids: vec![6],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    hw.add_disk_group(4, 5, 6, &DiskGroupValue::default())
        .await
        .unwrap();
    let mut other = record(6);
    other.rack_id = 4;
    other.node_id = 5;
    other.disk_group_id = 6;
    other
}

#[tokio::test]
async fn file_and_raw_aliases_on_one_host_cannot_cross_independent_rack_fences() {
    let test = TestStorage::start().await;
    let original = physical(record(5), "host-a", "file:10:20");
    let mut other = physical(peer(&test).await, "host-a", "file:10:20");
    other.backend = DiskBackend::BlockDevice;
    other.backing_identity = "device:47".into();
    registry(&test, &[(2, 1, "host-a"), (5, 4, "host-a")], true).await;
    let (first, second) = tokio::join!(
        test.hardware.admit_disk_storage(&original),
        test.hardware.admit_disk_storage(&other)
    );
    assert_ne!(
        first.is_ok(),
        second.is_ok(),
        "first={first:?}, second={second:?}"
    );
    let count = usize::from(
        test.hardware
            .get_disk_with_revision(&key(&original))
            .await
            .unwrap()
            .is_some(),
    ) + usize::from(
        test.hardware
            .get_disk_with_revision(&key(&other))
            .await
            .unwrap()
            .is_some(),
    );
    assert_eq!(count, 1);
    let winner = if first.is_ok() { original } else { other };
    test.hardware.admit_disk_storage(&winner).await.unwrap();
}

#[tokio::test]
async fn equal_local_device_numbers_on_distinct_physical_hosts_are_independent() {
    let test = TestStorage::start().await;
    let mut original = physical(record(5), "host-a", "device:47");
    original.backend = DiskBackend::BlockDevice;
    original.backing_identity = "device:47".into();
    let mut other = physical(peer(&test).await, "host-b", "device:47");
    other.backend = DiskBackend::BlockDevice;
    other.backing_identity = "device:47".into();
    registry(&test, &[(2, 1, "host-a"), (5, 4, "host-b")], true).await;
    let (first, second) = tokio::join!(
        test.hardware.admit_disk_storage(&original),
        test.hardware.admit_disk_storage(&other)
    );
    first.unwrap();
    second.unwrap();
    assert!(test
        .hardware
        .disk_storage(&key(&original))
        .await
        .unwrap()
        .is_some());
    assert!(test.hardware.disk_storage(&key(&other)).await.unwrap().is_some());
}

#[tokio::test]
async fn interrupted_resource_reservation_never_fabricates_membership_or_releases_its_identity() {
    let test = TestStorage::start().await;
    registry(&test, &[(2, 1, "host-a")], true).await;
    let original = physical(record(5), "host-a", "file:10:20");
    test.hardware.reserve_disk_resource(&original).await.unwrap();
    assert!(test
        .hardware
        .get_disk_with_revision(&key(&original))
        .await
        .unwrap()
        .is_none());
    let rows = test
        .hardware
        .kv()
        .scan(
            0,
            0,
            b"/hw/disk-resource/",
            b"",
            b"",
            16,
            ReadMode::Linearizable,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    assert_eq!(rows.items.len(), 1);
    let resource_key = &rows.items[0].0;
    let before = test
        .hardware
        .kv()
        .get(0, 0, resource_key, ReadMode::Linearizable, None)
        .await
        .unwrap();
    test.hardware.reserve_disk_resource(&original).await.unwrap();
    let mut removed = original.clone();
    removed.removed = true;
    test.hardware.reserve_disk_resource(&removed).await.unwrap();
    let other = physical(record(6), "host-a", "file:10:20");
    assert!(test.hardware.admit_disk_storage(&other).await.is_err());
    assert!(test
        .hardware
        .get_disk_with_revision(&key(&other))
        .await
        .unwrap()
        .is_none());
    match (
        before,
        test.hardware
            .kv()
            .get(0, 0, resource_key, ReadMode::Linearizable, None)
            .await
            .unwrap(),
    ) {
        (
            GetOutcome::Found {
                value: a,
                revision: x,
            },
            GetOutcome::Found {
                value: b,
                revision: y,
            },
        ) => {
            assert_eq!(a, b);
            assert_eq!(x, y);
        }
        _ => panic!("physical claim was removed"),
    }
}

#[tokio::test]
async fn legacy_file_receipts_prevent_cross_node_alias_admission() {
    let test = TestStorage::start().await;
    test.hardware.admit_disk_storage(&record(5)).await.unwrap();
    let other = physical(peer(&test).await, "host-a", "file:10:20");
    registry(&test, &[(2, 1, "host-a"), (5, 4, "host-a")], true).await;
    let error = test.hardware.admit_disk_storage(&other).await.unwrap_err();
    assert!(error.to_string().contains("legacy retained storage"), "{error}");
    assert!(test
        .hardware
        .get_disk_with_revision(&key(&other))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn unconfirmed_or_changed_physical_host_authority_cannot_reserve_a_resource() {
    let test = TestStorage::start().await;
    let original = physical(record(5), "host-a", "file:10:20");
    assert!(test.hardware.reserve_disk_resource(&original).await.is_err());
    registry(&test, &[(2, 1, "host-b")], true).await;
    assert!(test.hardware.reserve_disk_resource(&original).await.is_err());
    registry(&test, &[(2, 1, "host-a")], false).await;
    assert!(test.hardware.reserve_disk_resource(&original).await.is_err());
    let rows = test
        .hardware
        .kv()
        .scan(
            0,
            0,
            b"/hw/disk-resource/",
            b"",
            b"",
            16,
            ReadMode::Linearizable,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    assert!(rows.items.is_empty());
}

#[tokio::test]
async fn unknown_legacy_raw_lineage_requires_migration_without_new_membership() {
    let test = TestStorage::start().await;
    let mut legacy = record(5);
    legacy.backend = DiskBackend::BlockDevice;
    legacy.backing_identity = "device:47".into();
    test.hardware.admit_disk_storage(&legacy).await.unwrap();
    let other = physical(peer(&test).await, "host-a", "file:11:21");
    registry(&test, &[(2, 1, "host-a"), (5, 4, "host-a")], true).await;
    let error = test.hardware.admit_disk_storage(&other).await.unwrap_err();
    assert!(
        error.to_string().contains("legacy raw backing lineage"),
        "{error}"
    );
    assert!(test
        .hardware
        .get_disk_with_revision(&key(&other))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn legacy_physical_membership_without_receipt_cannot_be_guessed_safe() {
    let test = TestStorage::start().await;
    let legacy = record(5);
    test.hardware
        .add_disk(1, 2, 3, &legacy.disk_id, &legacy.geometry)
        .await
        .unwrap();
    let other = physical(peer(&test).await, "host-a", "file:11:21");
    registry(&test, &[(2, 1, "host-a"), (5, 4, "host-a")], true).await;
    let error = test.hardware.admit_disk_storage(&other).await.unwrap_err();
    assert!(
        error.to_string().contains("no verified backing receipt"),
        "{error}"
    );
    assert!(test
        .hardware
        .get_disk_with_revision(&key(&other))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn changed_host_authority_blocks_activation_without_changing_init_membership() {
    let test = TestStorage::start().await;
    registry(&test, &[(2, 1, "host-a")], true).await;
    let original = physical(record(5), "host-a", "file:10:20");
    test.hardware.admit_disk_storage(&original).await.unwrap();
    let before = test
        .hardware
        .get_disk_with_revision(&key(&original))
        .await
        .unwrap();
    registry(&test, &[(2, 1, "host-b")], true).await;
    assert!(test.hardware.activate_disk_storage(&original).await.is_err());
    assert_eq!(
        test.hardware
            .get_disk_with_revision(&key(&original))
            .await
            .unwrap(),
        before
    );
}
