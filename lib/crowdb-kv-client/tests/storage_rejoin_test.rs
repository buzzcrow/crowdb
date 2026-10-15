// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/storage.rs"]
mod storage;
use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::{
    common::{DiskioHardwareScope, HwStatus},
    disk_storage::{DiskExclusionRecord, DiskExclusionReporter, DiskRemovalRecord},
};
use storage::{key, record, TestStorage};

struct TestRemovedDisk {
    fixture: TestStorage,
    removal: DiskRemovalRecord,
}
impl TestRemovedDisk {
    async fn start() -> Self {
        let fixture = TestStorage::start().await;
        let hw = &fixture.hardware;
        let disk = record(5);
        let key = key(&disk);
        ServiceRegistryClient::from_shared(hw.shared_kv())
            .heartbeat_diskio_scoped(
                7,
                "a:7200",
                &DiskioHardwareScope {
                    version: 1,
                    rack_id: 1,
                    node_id: 2,
                    disk_group_id: 0,
                },
                &[],
                &[],
            )
            .await
            .unwrap();
        hw.admit_disk_storage(&disk).await.unwrap();
        let revision = hw.get_disk_with_revision(&key).await.unwrap().unwrap().1;
        let offline = hw.offline_disk_storage(&key, revision).await.unwrap();
        hw.acknowledge_disk_exclusion(&DiskExclusionRecord {
            version: 1,
            rack_id: 1,
            node_id: 2,
            disk_group_id: 3,
            disk_id: disk.disk_id,
            disk_revision: offline,
            reporter: DiskExclusionReporter::Diskio,
            instance_id: 7,
            owner_revision: None,
        })
        .await
        .unwrap();
        let removal = hw.remove_disk_storage(&key, offline).await.unwrap();
        Self { fixture, removal }
    }
}

#[tokio::test]
async fn explicit_rejoin_restores_original_offline_identity_and_is_idempotent() {
    let test = TestRemovedDisk::start().await;
    let hw = &test.fixture.hardware;
    let key = key(&test.removal.disk);
    let receipt = hw
        .rejoin_disk_storage(&key, &test.removal.operation_id)
        .await
        .unwrap();
    assert_eq!(receipt.source, test.removal);
    let mut expected = test.removal.disk.clone();
    expected.removed = false;
    assert_eq!(hw.disk_storage(&key).await.unwrap(), Some(expected.clone()));
    let (mut restored, revision) = hw.get_disk_with_revision(&key).await.unwrap().unwrap();
    assert_eq!(restored.status, HwStatus::Offline as i32);
    restored.status = expected.geometry.status;
    assert_eq!(restored, expected.geometry);
    assert_eq!(
        hw.rejoin_disk_storage(&key, &test.removal.operation_id)
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        hw.get_disk_with_revision(&key).await.unwrap().unwrap().1,
        revision
    );
    assert!(hw
        .remove_disk_storage(&key, test.removal.offline_revision)
        .await
        .is_err());
    assert!(hw
        .set_disk_status(1, 2, 3, &key.disk_id, HwStatus::Up)
        .await
        .is_err());
    assert!(hw.activate_disk_storage(&expected).await.is_err());
    hw.admit_disk_storage(&expected).await.unwrap();
    let replayed = hw.get_disk_with_revision(&key).await.unwrap().unwrap();
    assert_eq!(replayed.0.status, HwStatus::Offline as i32);
    assert_eq!(
        replayed.1, revision,
        "creation replay must not initialize or reopen the disk"
    );
    assert_eq!(hw.disk_rejoin_receipt(&key).await.unwrap(), Some(receipt));
}

#[tokio::test]
async fn rejoin_rejects_changed_removal_and_legacy_path_aliases() {
    let test = TestRemovedDisk::start().await;
    let hw = &test.fixture.hardware;
    let key = key(&test.removal.disk);
    assert!(hw.rejoin_disk_storage(&key, "another-removal").await.is_err());
    // A legacy writer cannot make a retained medium a second disk during rejoin.
    let mut alias = test.removal.disk.geometry.clone();
    alias.status = HwStatus::Up as i32;
    hw.add_disk(
        1,
        2,
        99,
        &crowdb_protocol::common::DiskId { high: 9, low: 9 },
        &alias,
    )
    .await
    .unwrap();
    assert!(hw
        .rejoin_disk_storage(&key, &test.removal.operation_id)
        .await
        .is_err());
    assert!(hw.get_disk_with_revision(&key).await.unwrap().is_none());
    assert!(hw.disk_storage(&key).await.unwrap().unwrap().removed);
}
