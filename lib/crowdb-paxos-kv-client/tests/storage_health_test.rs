// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/storage.rs"]
mod storage;

use crowdb_protocol::common::HwStatus;
use storage::{key, record, TestStorage};

#[tokio::test]
async fn physical_failure_preserves_backing_and_closes_only_observed_membership() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let record = record(5);
    let key = key(&record);
    hw.admit_disk_storage(&record).await.unwrap();
    hw.activate_disk_storage(&record).await.unwrap();
    let (before, revision) = hw.get_disk_with_revision(&key).await.unwrap().unwrap();
    hw.quarantine_disk_storage(&record, revision).await.unwrap();
    let (after, closed_revision) = hw.get_disk_with_revision(&key).await.unwrap().unwrap();
    assert_eq!(after.status, HwStatus::Offline as i32);
    let mut expected = before;
    expected.status = HwStatus::Offline as i32;
    assert_eq!(after, expected);
    assert!(closed_revision > revision);
    assert_eq!(hw.disk_storage(&key).await.unwrap(), Some(record.clone()));
    assert_eq!(
        hw.get_disk_group(1, 2, 3).await.unwrap().unwrap().value.disk_ids,
        vec![record.disk_id]
    );
    assert!(hw.activate_disk_storage(&record).await.is_err());
    assert!(hw.quarantine_disk_storage(&record, revision).await.is_err());
    hw.quarantine_disk_storage(&record, closed_revision)
        .await
        .unwrap();
    assert_eq!(
        hw.get_disk_with_revision(&key).await.unwrap().unwrap().1,
        closed_revision
    );
}

#[tokio::test]
async fn stale_or_changed_medium_observation_cannot_close_current_membership() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let original = record(5);
    let key = key(&original);
    hw.admit_disk_storage(&original).await.unwrap();
    let (_, old_revision) = hw.get_disk_with_revision(&key).await.unwrap().unwrap();
    hw.activate_disk_storage(&original).await.unwrap();
    let current = hw.get_disk_with_revision(&key).await.unwrap().unwrap();
    assert!(hw.quarantine_disk_storage(&original, old_revision).await.is_err());
    let mut wrong = original.clone();
    wrong.backing_identity = "file:replaced".into();
    assert!(hw.quarantine_disk_storage(&wrong, current.1).await.is_err());
    let mut removed = original.clone();
    removed.removed = true;
    assert!(hw.quarantine_disk_storage(&removed, current.1).await.is_err());
    assert_eq!(hw.get_disk_with_revision(&key).await.unwrap(), Some(current));
    assert_eq!(hw.disk_storage(&key).await.unwrap(), Some(original));
    assert!(hw
        .get_disk_with_revision(&storage::key(&record(99)))
        .await
        .unwrap()
        .is_none());
}
