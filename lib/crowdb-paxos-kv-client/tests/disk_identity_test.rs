// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/storage.rs"]
mod storage;
use crowdb_protocol::{
    common::{DiskId, NodeValue, RackValue},
    disk_storage::DiskIdReservation,
    diskdb::rpc::DiskGroupValue,
    key::TextKey,
};
use storage::{key, record, TestStorage};

async fn other_location(test: &TestStorage) -> crowdb_protocol::disk_storage::DiskStorageRecord {
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
    let mut other = record(5);
    other.rack_id = 4;
    other.node_id = 5;
    other.disk_group_id = 6;
    other.operation_id = "another-node".into();
    other
}

#[tokio::test]
async fn different_racks_cannot_concurrently_admit_the_same_global_disk_id() {
    let test = TestStorage::start().await;
    let original = record(5);
    let other = other_location(&test).await;
    let hw = &test.hardware;
    let (first, second) = tokio::join!(hw.admit_disk_storage(&original), hw.admit_disk_storage(&other));
    assert_ne!(
        first.is_ok(),
        second.is_ok(),
        "first={first:?}, second={second:?}"
    );
    let published = usize::from(
        hw.get_disk_with_revision(&key(&original))
            .await
            .unwrap()
            .is_some(),
    ) + usize::from(hw.get_disk_with_revision(&key(&other)).await.unwrap().is_some());
    assert_eq!(published, 1);
    let winner = if first.is_ok() { original } else { other };
    hw.admit_disk_storage(&winner).await.unwrap();
}

#[tokio::test]
async fn an_interrupted_reservation_keeps_identity_without_fabricating_membership() {
    let test = TestStorage::start().await;
    let original = record(5);
    let other = other_location(&test).await;
    let hw = &test.hardware;
    hw.reserve_disk_id(&key(&original)).await.unwrap();
    hw.reserve_disk_id(&key(&original)).await.unwrap();
    assert!(hw
        .get_disk_with_revision(&key(&original))
        .await
        .unwrap()
        .is_none());
    assert!(hw.admit_disk_storage(&other).await.is_err());
    assert!(hw
        .add_disk(4, 5, 6, &other.disk_id, &other.geometry)
        .await
        .is_err());
    hw.admit_disk_storage(&original).await.unwrap();
}

#[tokio::test]
async fn legacy_identity_verification_advances_pages_and_includes_removed_records() {
    let test = TestStorage::start().await;
    let original = record(5);
    let mut other = other_location(&test).await;
    let hw = &test.hardware;
    for low in 0..140 {
        let mut legacy = original.clone();
        legacy.disk_id = DiskId { high: 9, low };
        hw.kv()
            .put(
                0,
                0,
                key(&legacy).to_path().as_bytes(),
                &serde_json::to_vec(&legacy.geometry).unwrap(),
                None,
            )
            .await
            .unwrap();
    }
    let path = key(&other).to_path();
    hw.kv()
        .put(
            0,
            0,
            path.as_bytes(),
            &serde_json::to_vec(&other.geometry).unwrap(),
            None,
        )
        .await
        .unwrap();
    assert!(hw.reserve_disk_id(&key(&original)).await.is_err());
    hw.kv().delete(0, 0, path.as_bytes(), None).await.unwrap();
    other.removed = true;
    hw.kv()
        .put(
            0,
            0,
            other.key().as_bytes(),
            &serde_json::to_vec(&other).unwrap(),
            None,
        )
        .await
        .unwrap();
    assert!(
        hw.reserve_disk_id(&key(&original)).await.is_err(),
        "removed media still owns its global ID"
    );
}

#[tokio::test]
async fn unsupported_identity_reservations_are_not_reinterpreted_as_unclaimed_ids() {
    let test = TestStorage::start().await;
    let original = record(5);
    let mut claim = DiskIdReservation::new(&key(&original));
    claim.version = 2;
    test.hardware
        .kv()
        .put(
            0,
            0,
            DiskIdReservation::key_for(&original.disk_id).as_bytes(),
            &serde_json::to_vec(&claim).unwrap(),
            None,
        )
        .await
        .unwrap();
    assert!(test.hardware.admit_disk_storage(&original).await.is_err());
    assert!(test
        .hardware
        .get_disk_with_revision(&key(&original))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn legacy_physical_media_cannot_disappear_through_direct_or_parent_cleanup() {
    let test = TestStorage::start().await;
    let original = record(5);
    let hw = &test.hardware;
    hw.add_disk(1, 2, 3, &original.disk_id, &original.geometry)
        .await
        .unwrap();
    assert!(hw.remove_disk(1, 2, 3, &original.disk_id).await.is_err());
    assert!(hw.remove_disk_group(1, 2, 3).await.is_err());
    assert!(hw.remove_node(1, 2).await.is_err());
    assert!(hw.remove_rack(1).await.is_err());
    assert!(hw.remove_disk_group_cascade(1, 2, 3).await.is_err());
    assert!(hw.remove_node_cascade(1, 2).await.is_err());
    assert!(hw.remove_rack_cascade(1).await.is_err());
    assert_eq!(
        hw.get_disk(1, 2, 3, &original.disk_id).await.unwrap(),
        Some(original.geometry)
    );
    assert!(hw.get_node(1, 2).await.unwrap().is_some());
    assert!(hw.get_disk_group(1, 2, 3).await.unwrap().is_some());
}
