// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/storage.rs"]
mod storage;
use crowdb_protocol::{
    disk_storage::DiskGroupIdReservation,
    diskdb::rpc::DiskGroupValue,
    key::{DiskGroupKey, TextKey},
};
use storage::TestStorage;

#[tokio::test]
async fn numeric_group_id_cannot_be_reserved_at_two_hardware_locations() {
    let fixture = TestStorage::start().await;
    let first = DiskGroupKey {
        rack_id: 1,
        node_id: 2,
        disk_group_id: 9,
    };
    let second = DiskGroupKey {
        rack_id: 4,
        node_id: 5,
        disk_group_id: 9,
    };
    let (a, b) = tokio::join!(
        fixture.hardware.reserve_disk_group_id(&first),
        fixture.hardware.reserve_disk_group_id(&second)
    );
    assert_ne!(a.is_ok(), b.is_ok(), "one placement must win: {a:?}, {b:?}");
    let winner = if a.is_ok() { first } else { second };
    fixture.hardware.reserve_disk_group_id(&winner).await.unwrap();
    assert!(fixture
        .hardware
        .get_disk_group(winner.rack_id, winner.node_id, winner.disk_group_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn allocation_skips_legacy_groups_and_retains_unique_unpublished_reservations() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let retained = storage::record(1);
    hw.admit_disk_storage(&retained).await.unwrap();
    let (a, b) = tokio::join!(hw.allocate_disk_group_id(1, 2), hw.allocate_disk_group_id(4, 5));
    let a = a.unwrap();
    let b = b.unwrap();
    assert_ne!(a, b);
    assert!(a > 3 && b > 3);
    let conflicting = DiskGroupKey {
        rack_id: 9,
        node_id: 8,
        disk_group_id: a,
    };
    assert!(hw.reserve_disk_group_id(&conflicting).await.is_err());
    assert!(hw.get_disk_group(1, 2, a).await.unwrap().is_none());
    assert!(hw.get_disk_group(4, 5, b).await.unwrap().is_none());
    assert_eq!(
        hw.disk_storage(&storage::key(&retained)).await.unwrap(),
        Some(retained)
    );
}

#[tokio::test]
async fn legacy_conflicts_and_unknown_reservation_versions_are_not_adopted() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let legacy = DiskGroupKey {
        rack_id: 4,
        node_id: 5,
        disk_group_id: 300,
    };
    hw.kv()
        .put(
            0,
            0,
            legacy.to_path().as_bytes(),
            &serde_json::to_vec(&DiskGroupValue::default()).unwrap(),
            None,
        )
        .await
        .unwrap();
    assert!(hw
        .reserve_disk_group_id(&DiskGroupKey {
            rack_id: 1,
            node_id: 2,
            disk_group_id: 300
        })
        .await
        .is_err());
    let key = DiskGroupKey {
        rack_id: 1,
        node_id: 2,
        disk_group_id: 301,
    };
    let mut reserved = DiskGroupIdReservation::new(&key);
    reserved.version = 2;
    hw.kv()
        .put(
            0,
            0,
            DiskGroupIdReservation::key_for(301).as_bytes(),
            &serde_json::to_vec(&reserved).unwrap(),
            None,
        )
        .await
        .unwrap();
    assert!(hw.reserve_disk_group_id(&key).await.is_err());
    assert!(hw.get_disk_group(1, 2, 301).await.unwrap().is_none());
}
