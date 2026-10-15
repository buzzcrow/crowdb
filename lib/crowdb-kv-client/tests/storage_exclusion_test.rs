// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/storage.rs"]
mod storage;
use crowdb_kv_client::{GetOutcome, ReadMode, ServiceRegistryClient};
use crowdb_protocol::{
    common::{HwStatus, ServiceExtra},
    disk_storage::{DiskExclusionRecord, DiskExclusionReporter},
    key::{OwnerMapKey, RackKey, TextKey},
};
use storage::{key, record, TestStorage};

async fn revision(hw: &crowdb_kv_client::HardwareClient, path: &str) -> u64 {
    let GetOutcome::Found { revision, .. } = hw
        .kv()
        .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("missing {path}")
    };
    revision
}

#[tokio::test]
async fn exclusion_requires_exact_offline_revision_and_current_owner_generation() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let disk = record(5);
    hw.admit_disk_storage(&disk).await.unwrap();
    let key = key(&disk);
    let mut proof = DiskExclusionRecord {
        version: 1,
        rack_id: 1,
        node_id: 2,
        disk_group_id: 3,
        disk_id: disk.disk_id,
        disk_revision: revision(hw, &key.to_path()).await,
        reporter: DiskExclusionReporter::Diskio,
        instance_id: 7,
        owner_revision: None,
    };
    assert!(hw.acknowledge_disk_exclusion(&proof).await.is_err());
    hw.set_disk_status(1, 2, 3, &disk.disk_id, HwStatus::Offline)
        .await
        .unwrap();
    proof.disk_revision = revision(hw, &key.to_path()).await;
    assert!(
        hw.acknowledge_disk_exclusion(&proof).await.is_err(),
        "no physical reporter"
    );
    let svc = ServiceRegistryClient::from_shared(hw.shared_kv());
    svc.heartbeat_diskio_at(7, "127.0.0.1:7200", 1, 2, &[3], &[])
        .await
        .unwrap();
    hw.acknowledge_disk_exclusion(&proof).await.unwrap();
    hw.acknowledge_disk_exclusion(&proof).await.unwrap();
    assert_eq!(
        hw.disk_exclusion(&key, DiskExclusionReporter::Diskio, 7)
            .await
            .unwrap(),
        Some(proof.clone())
    );
    hw.set_owner(1, 2, 3, 9, u64::MAX).await.unwrap();
    let owner = OwnerMapKey {
        rack_id: 1,
        node_id: 2,
        disk_group_id: 3,
    }
    .to_path();
    proof.reporter = DiskExclusionReporter::Diskdb;
    proof.instance_id = 9;
    proof.owner_revision = Some(revision(hw, &owner).await);
    hw.acknowledge_disk_exclusion(&proof).await.unwrap();
    hw.set_owner(1, 2, 3, 9, u64::MAX).await.unwrap();
    assert!(
        hw.acknowledge_disk_exclusion(&proof).await.is_err(),
        "renewed owner invalidates captured generation"
    );
    proof.owner_revision = Some(revision(hw, &owner).await);
    hw.acknowledge_disk_exclusion(&proof).await.unwrap();
    let disk_value = hw.get_disk_with_revision(&key).await.unwrap().unwrap().0;
    hw.kv()
        .put(
            0,
            0,
            key.to_path().as_bytes(),
            &serde_json::to_vec(&disk_value).unwrap(),
            None,
        )
        .await
        .unwrap();
    assert!(
        hw.acknowledge_disk_exclusion(&proof).await.is_err(),
        "an equal value at a different revision needs new evidence"
    );
    proof.disk_revision = revision(hw, &key.to_path()).await;
    hw.acknowledge_disk_exclusion(&proof).await.unwrap();
}

#[tokio::test]
async fn diskio_registration_pins_location_and_participates_in_membership_fencing() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let svc = ServiceRegistryClient::from_shared(hw.shared_kv());
    let rack = RackKey { rack_id: 1 }.to_path();
    let before = revision(hw, &rack).await;
    svc.heartbeat_diskio_at(7, "127.0.0.1:7200", 1, 2, &[3], &[])
        .await
        .unwrap();
    assert!(revision(hw, &rack).await > before);
    assert!(svc
        .heartbeat_diskio_at(7, "127.0.0.1:7200", 1, 99, &[3], &[])
        .await
        .is_err());
    assert!(svc
        .register("diskio", 7, "127.0.0.1:7200", &ServiceExtra::default())
        .await
        .is_err());
    assert!(svc.unregister("diskio", 7).await.is_err());
    let legacy = ServiceExtra::default();
    svc.register("diskio", 8, "127.0.0.1:7201", &legacy)
        .await
        .unwrap();
    svc.unregister("diskio", 8).await.unwrap();
}

#[tokio::test]
async fn competing_initial_locations_cannot_share_one_diskio_identity() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let svc = ServiceRegistryClient::from_shared(hw.shared_kv());
    let (a, b) = tokio::join!(
        svc.heartbeat_diskio_at(7, "a:7200", 1, 2, &[3], &[]),
        svc.heartbeat_diskio_at(7, "b:7200", 1, 99, &[3], &[])
    );
    assert_ne!(a.is_ok(), b.is_ok(), "a={a:?}, b={b:?}");
}
