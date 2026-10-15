// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/storage.rs"]
mod storage;
use crowdb_protocol::{common::DiskId, mgmt::node::storage::DiskBackend};
use storage::{key, record, TestStorage};

#[tokio::test]
async fn concurrent_first_disks_cannot_establish_different_media_classes() {
    let fixture = TestStorage::start().await;
    let first = record(1);
    let mut second = record(2);
    second.backing_identity = "block:20".into();
    second.backend = DiskBackend::BlockDevice;
    second.geometry.device_path = "/opt/crowdb/data/disks/raw".into();
    second.media_type = Some(0);
    let (a, b) = tokio::join!(
        fixture.hardware.admit_disk_storage(&first),
        fixture.hardware.admit_disk_storage(&second)
    );
    assert_ne!(a.is_ok(), b.is_ok(), "incompatible first disks: {a:?}, {b:?}");
    assert_eq!(
        fixture.hardware.list_disks_in_group(1, 2, 3).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn retained_removed_class_and_units_still_constrain_new_admission() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let mut removed = record(1);
    removed.removed = true;
    hw.kv()
        .put(
            0,
            0,
            removed.key().as_bytes(),
            &serde_json::to_vec(&removed).unwrap(),
            None,
        )
        .await
        .unwrap();
    assert!(hw.validate_disk_group_storage(1, 2, 3, None, 4096).await.is_ok());
    assert!(hw
        .validate_disk_group_storage(1, 2, 3, Some(0), 4096)
        .await
        .is_err());
    assert!(hw.validate_disk_group_storage(1, 2, 3, None, 8192).await.is_err());
    let mut incompatible = record(2);
    incompatible.backing_identity = "file:10:21".into();
    incompatible.geometry.device_path = "/opt/crowdb/data/disks/new".into();
    incompatible.geometry.unit_size_bytes = 8192;
    assert!(hw.admit_disk_storage(&incompatible).await.is_err());
    assert!(hw
        .get_disk_with_revision(&key(&incompatible))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn backend_does_not_split_compatible_media_but_legacy_type_is_not_unknown() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let mut first = record(1);
    first.media_type = Some(0);
    let legacy = DiskId { high: 8, low: 8 };
    let mut geometry = first.geometry.clone();
    geometry.device_path = "/opt/crowdb/data/disks/legacy".into();
    hw.add_disk(1, 2, 3, &legacy, &geometry).await.unwrap();
    hw.admit_disk_storage(&first).await.unwrap();
    let mut second = record(2);
    second.media_type = Some(0);
    second.backend = DiskBackend::BlockDevice;
    second.backing_identity = "block:20".into();
    second.geometry.device_path = "/opt/crowdb/data/disks/raw".into();
    hw.admit_disk_storage(&second).await.unwrap();
    assert!(hw.validate_disk_group_storage(1, 2, 3, None, 4096).await.is_err());
    assert!(hw
        .validate_disk_group_storage(1, 2, 3, Some(0), 4096)
        .await
        .is_ok());
    assert!(hw
        .validate_disk_group_storage(1, 2, 3, Some(1), 4096)
        .await
        .is_err());
}

#[tokio::test]
async fn owner_status_write_cannot_resurrect_concurrently_removed_membership() {
    use crowdb_kv_client::{GetOutcome, ReadMode};
    use crowdb_protocol::key::{OwnerMapKey, TextKey};
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let disk = record(9);
    hw.add_disk(1, 2, 3, &disk.disk_id, &disk.geometry).await.unwrap();
    hw.set_owner(1, 2, 3, 7, u64::MAX).await.unwrap();
    let owner = OwnerMapKey {
        rack_id: 1,
        node_id: 2,
        disk_group_id: 3,
    }
    .to_path();
    let GetOutcome::Found { revision, .. } = hw
        .kv()
        .get(0, 0, owner.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("owner missing")
    };
    let key = key(&disk);
    let (status, removed) = tokio::join!(
        hw.mark_disk_offline_if_owned(&key, 7, revision),
        hw.remove_disk(1, 2, 3, &disk.disk_id)
    );
    if removed.is_ok() {
        assert!(hw.get_disk_with_revision(&key).await.unwrap().is_none());
    } else {
        assert!(status.is_ok(), "status={status:?}, removal={removed:?}");
        hw.remove_disk(1, 2, 3, &disk.disk_id).await.unwrap();
    }
    assert!(hw.mark_disk_offline_if_owned(&key, 7, revision).await.is_err());
    assert!(hw.get_disk_with_revision(&key).await.unwrap().is_none());
}
