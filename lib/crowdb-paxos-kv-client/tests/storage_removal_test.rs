// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/storage.rs"]
mod storage;
use crowdb_paxos_kv_client::{GetOutcome, HardwareClient, ReadMode, ServiceRegistryClient};
use crowdb_protocol::{
    common::{ChunkId, DiskioHardwareScope, HwStatus, InstanceValue},
    disk_storage::{DiskExclusionRecord, DiskExclusionReporter, DiskRemovalRecord},
    diskdb::rpc::{BusyBlockValue, CommitState},
    key::{BinaryKey, BusyBlockKey, DiskKey, InstanceKey, OwnerMapKey, TextKey},
};
use storage::{key, record, TestStorage};

async fn register(hw: &HardwareClient, instance: u64) {
    ServiceRegistryClient::from_shared(hw.shared_kv())
        .heartbeat_diskio_scoped(
            instance,
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
}

async fn revision(hw: &HardwareClient, path: &str) -> u64 {
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

async fn acknowledge(hw: &HardwareClient, key: &DiskKey, reporter: DiskExclusionReporter, instance: u64) {
    let owner_revision = if reporter == DiskExclusionReporter::Diskdb {
        Some(
            revision(
                hw,
                &OwnerMapKey {
                    rack_id: key.rack_id,
                    node_id: key.node_id,
                    disk_group_id: key.disk_group_id,
                }
                .to_path(),
            )
            .await,
        )
    } else {
        None
    };
    hw.acknowledge_disk_exclusion(&DiskExclusionRecord {
        version: 1,
        rack_id: key.rack_id,
        node_id: key.node_id,
        disk_group_id: key.disk_group_id,
        disk_id: key.disk_id,
        disk_revision: revision(hw, &key.to_path()).await,
        reporter,
        instance_id: instance,
        owner_revision,
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn occupied_removal_retains_interpretation_allocations_and_exact_replay_receipt() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    register(hw, 7).await;
    let original = record(5);
    let key = key(&original);
    hw.admit_disk_storage(&original).await.unwrap();
    hw.set_disk_status(1, 2, 3, &key.disk_id, HwStatus::Up)
        .await
        .unwrap();
    let online = hw.disk_removal_readiness(&key).await.unwrap();
    assert!(online
        .conditions
        .iter()
        .any(|reason| reason.contains("Mark the disk Offline")));
    assert!(hw.remove_disk_storage(&key, online.disk_revision).await.is_err());
    hw.set_bind(1, 2, 3, 0, 1).await.unwrap();
    hw.set_owner(1, 2, 3, 9, u64::MAX).await.unwrap();
    let allocation_key = BusyBlockKey {
        disk_id: key.disk_id,
        zone_index: 0,
        unit_offset: 7,
    }
    .to_bytes();
    let allocation = bincode::serialize(&BusyBlockValue {
        unit_count: 1,
        unit_size: 4096,
        owner_chunk: Some(ChunkId { high: 8, low: 9 }),
        commit_state: CommitState::Committed as i32,
        allocation_ts: 42,
        ..Default::default()
    })
    .unwrap();
    hw.kv()
        .put(0, 1, &allocation_key, &allocation, None)
        .await
        .unwrap();
    hw.set_disk_status(1, 2, 3, &key.disk_id, HwStatus::Offline)
        .await
        .unwrap();
    let offline = hw.disk_removal_readiness(&key).await.unwrap();
    assert!(!offline.conditions.is_empty());
    acknowledge(hw, &key, DiskExclusionReporter::Diskio, 7).await;
    acknowledge(hw, &key, DiskExclusionReporter::Diskdb, 9).await;
    assert!(hw
        .disk_removal_readiness(&key)
        .await
        .unwrap()
        .conditions
        .is_empty());
    let result = hw.remove_disk_storage(&key, offline.disk_revision).await.unwrap();
    assert!(
        hw.reserve_disk_id(&DiskKey {
            rack_id: 99,
            node_id: 99,
            disk_group_id: 99,
            disk_id: key.disk_id
        })
        .await
        .is_err(),
        "confirmed removal does not release global disk identity"
    );
    assert!(hw.get_disk_with_revision(&key).await.unwrap().is_none());
    assert!(hw
        .get_disk_group(1, 2, 3)
        .await
        .unwrap()
        .unwrap()
        .value
        .disk_ids
        .is_empty());
    let mut retained = original;
    retained.removed = true;
    assert_eq!(result.disk, retained);
    assert_eq!(hw.disk_storage(&key).await.unwrap(), Some(retained));
    let GetOutcome::Found { value, .. } = hw
        .kv()
        .get(0, 1, &allocation_key, ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("allocation lost")
    };
    assert_eq!(value.as_ref(), allocation);
    assert!(hw.get_bind(1, 2, 3).await.unwrap().is_some());
    assert_eq!(hw.get_owner(1, 2, 3).await.unwrap().unwrap().instance_id, 9);
    let path = DiskRemovalRecord::key_for(&key);
    let committed = revision(hw, &path).await;
    assert_eq!(
        hw.remove_disk_storage(&key, offline.disk_revision).await.unwrap(),
        result
    );
    assert_eq!(revision(hw, &path).await, committed);
    assert!(hw.remove_disk_storage(&key, online.disk_revision).await.is_err());
    assert!(hw.remove_disk_group_cascade(1, 2, 3).await.is_err());
}

#[tokio::test]
async fn expired_participants_and_renewed_owners_do_not_bypass_confirmation() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    register(hw, 7).await;
    register(hw, 8).await;
    let original = record(5);
    let key = key(&original);
    hw.admit_disk_storage(&original).await.unwrap();
    hw.set_bind(1, 2, 3, 0, 1).await.unwrap();
    hw.set_disk_status(1, 2, 3, &key.disk_id, HwStatus::Offline)
        .await
        .unwrap();
    acknowledge(hw, &key, DiskExclusionReporter::Diskio, 7).await;
    let svc = ServiceRegistryClient::from_shared(hw.shared_kv());
    let mut expired: InstanceValue = svc.read_instance("diskio", 8).await.unwrap().unwrap();
    expired.last_heartbeat_ms = 0;
    let path = InstanceKey {
        service: "diskio".into(),
        instance_id: 8,
    }
    .to_path();
    hw.kv()
        .put(
            0,
            0,
            path.as_bytes(),
            &serde_json::to_vec(&expired).unwrap(),
            None,
        )
        .await
        .unwrap();
    let readiness = hw.disk_removal_readiness(&key).await.unwrap();
    assert!(readiness
        .conditions
        .iter()
        .any(|reason| reason.contains("DiskIO 8")));
    assert!(readiness
        .conditions
        .iter()
        .any(|reason| reason.contains("DiskDB owner is unavailable")));
    acknowledge(hw, &key, DiskExclusionReporter::Diskio, 8).await;
    hw.set_owner(1, 2, 3, 9, u64::MAX).await.unwrap();
    acknowledge(hw, &key, DiskExclusionReporter::Diskdb, 9).await;
    hw.set_owner(1, 2, 3, 9, u64::MAX).await.unwrap();
    assert!(hw
        .remove_disk_storage(&key, readiness.disk_revision)
        .await
        .is_err());
    acknowledge(hw, &key, DiskExclusionReporter::Diskdb, 9).await;
    hw.remove_disk_storage(&key, readiness.disk_revision)
        .await
        .unwrap();
}

#[tokio::test]
async fn starting_diskio_is_observed_or_registers_after_the_removal_commit() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    register(hw, 7).await;
    let original = record(5);
    let key = key(&original);
    hw.admit_disk_storage(&original).await.unwrap();
    hw.set_disk_status(1, 2, 3, &key.disk_id, HwStatus::Offline)
        .await
        .unwrap();
    acknowledge(hw, &key, DiskExclusionReporter::Diskio, 7).await;
    let expected = revision(hw, &key.to_path()).await;
    let (removed, ()) = tokio::join!(hw.remove_disk_storage(&key, expected), register(hw, 8));
    if removed.is_ok() {
        let committed = revision(hw, &DiskRemovalRecord::key_for(&key)).await;
        let registered = revision(
            hw,
            &InstanceKey {
                service: "diskio".into(),
                instance_id: 8,
            }
            .to_path(),
        )
        .await;
        assert!(
            registered > committed,
            "new instance must finish registration after removal before it can acquire media"
        );
    } else {
        assert!(hw.get_disk_with_revision(&key).await.unwrap().is_some());
        assert!(hw
            .disk_removal_readiness(&key)
            .await
            .unwrap()
            .conditions
            .iter()
            .any(|reason| reason.contains("DiskIO 8")));
    }
}
