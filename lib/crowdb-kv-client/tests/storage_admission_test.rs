// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_kv::cluster::{
    group::PxGroup, kv_server::KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient};
use crowdb_protocol::{
    common::{DiskId, HwStatus, NodeValue, RackValue},
    disk_storage::DiskStorageRecord,
    diskdb::rpc::{DiskGroupValue, DiskValue},
    key::DiskKey,
    mgmt::node::storage::DiskBackend,
};

struct TestStorage {
    store: Arc<PxKvStore>,
    hardware: HardwareClient,
}

impl TestStorage {
    async fn start() -> Self {
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        store.add_group(PxGroup::new(
            0,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        store.start().await.unwrap();
        let client = CrowdbKvClient::new(ClientConfig::new(vec![]));
        client.seed_leader(0, 0, store.listen_addr().unwrap().to_string());
        let hardware = HardwareClient::new(client);
        hardware
            .add_rack(
                1,
                &RackValue {
                    node_ids: vec![2],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        hardware
            .add_node(
                1,
                2,
                &NodeValue {
                    disk_group_ids: vec![3],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        hardware
            .add_disk_group(1, 2, 3, &DiskGroupValue::default())
            .await
            .unwrap();
        Self { store, hardware }
    }
}

impl Drop for TestStorage {
    fn drop(&mut self) {
        self.store.stop();
    }
}

fn record(low: u64) -> DiskStorageRecord {
    DiskStorageRecord {
        version: 1,
        operation_id: format!("operation-{low}"),
        rack_id: 1,
        node_id: 2,
        disk_group_id: 3,
        disk_id: DiskId { high: 4, low },
        backend: DiskBackend::File,
        backing_identity: "file:10:20".into(),
        media_type: None,
        sparse: false,
        geometry: DiskValue {
            disk_type: 0,
            capacity_units: 4096,
            zone_size_units: 1024,
            unit_size_bytes: 4096,
            zone_count: 4,
            status: HwStatus::Init as i32,
            device_path: "/opt/crowdb/data/disks/original.img".into(),
        },
        removed: false,
    }
}

fn key(record: &DiskStorageRecord) -> DiskKey {
    DiskKey {
        rack_id: record.rack_id,
        node_id: record.node_id,
        disk_group_id: record.disk_group_id,
        disk_id: record.disk_id,
    }
}

#[tokio::test]
async fn publication_replay_preserves_live_status_and_rejects_changed_intent() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let original = record(5);
    hw.admit_disk_storage(&original).await.unwrap();
    assert_eq!(
        hw.disk_storage(&key(&original)).await.unwrap(),
        Some(original.clone())
    );
    assert_eq!(
        hw.get_disk_group(1, 2, 3).await.unwrap().unwrap().value.disk_ids,
        vec![original.disk_id]
    );
    hw.set_disk_status(1, 2, 3, &original.disk_id, HwStatus::Up)
        .await
        .unwrap();
    hw.admit_disk_storage(&original).await.unwrap();
    assert_eq!(
        hw.get_disk(1, 2, 3, &original.disk_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        HwStatus::Up as i32
    );
    let mut changed = original.clone();
    changed.sparse = true;
    assert!(hw.admit_disk_storage(&changed).await.is_err());
    assert_eq!(hw.disk_storage(&key(&original)).await.unwrap(), Some(original));
}

#[tokio::test]
async fn concurrent_aliases_cannot_both_enter_membership() {
    let fixture = TestStorage::start().await;
    let first = record(5);
    let second = record(6);
    let (a, b) = tokio::join!(
        fixture.hardware.admit_disk_storage(&first),
        fixture.hardware.admit_disk_storage(&second)
    );
    assert_ne!(
        a.is_ok(),
        b.is_ok(),
        "exactly one alias may be published: {a:?}, {b:?}"
    );
    let members = fixture
        .hardware
        .get_disk_group(1, 2, 3)
        .await
        .unwrap()
        .unwrap()
        .value
        .disk_ids;
    assert_eq!(members.len(), 1);
    assert_eq!(
        fixture.hardware.list_disks_in_group(1, 2, 3).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn missing_parent_and_retained_removed_identity_never_create_new_disks() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let mut missing = record(5);
    missing.node_id = 99;
    assert!(hw.admit_disk_storage(&missing).await.is_err());
    assert!(hw.get_disk(1, 99, 3, &missing.disk_id).await.unwrap().is_none());
    let mut removed = record(5);
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
    assert!(hw.admit_disk_storage(&record(5)).await.is_err());
    assert!(hw.get_disk_with_revision(&key(&removed)).await.unwrap().is_none());
    assert!(hw
        .get_disk_group(1, 2, 3)
        .await
        .unwrap()
        .unwrap()
        .value
        .disk_ids
        .is_empty());
}

#[tokio::test]
async fn legacy_path_cannot_be_adopted_as_a_blank_medium() {
    let fixture = TestStorage::start().await;
    let original = record(5);
    fixture
        .hardware
        .add_disk(1, 2, 3, &DiskId { high: 7, low: 8 }, &original.geometry)
        .await
        .unwrap();
    assert!(fixture.hardware.admit_disk_storage(&original).await.is_err());
    assert!(fixture
        .hardware
        .disk_storage(&key(&original))
        .await
        .unwrap()
        .is_none());
    assert!(fixture
        .hardware
        .get_disk_with_revision(&key(&original))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn direct_and_cascade_deletion_preserve_retained_disks_and_legacy_siblings() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let original = record(5);
    let sibling = DiskId { high: 9, low: 9 };
    let mut legacy = original.geometry.clone();
    legacy.device_path = "/data/legacy.img".into();
    hw.add_disk(1, 2, 3, &sibling, &legacy).await.unwrap();
    hw.admit_disk_storage(&original).await.unwrap();
    assert!(hw.remove_disk(1, 2, 3, &original.disk_id).await.is_err());
    assert!(hw.remove_disk_group(1, 2, 3).await.is_err());
    assert!(hw.remove_node(1, 2).await.is_err());
    assert!(hw.remove_rack(1).await.is_err());
    assert!(hw.remove_disk_group_cascade(1, 2, 3).await.is_err());
    assert!(hw.remove_node_cascade(1, 2).await.is_err());
    assert!(hw.remove_rack_cascade(1).await.is_err());
    assert_eq!(hw.get_disk(1, 2, 3, &sibling).await.unwrap(), Some(legacy));
    assert_eq!(
        hw.get_disk(1, 2, 3, &original.disk_id).await.unwrap(),
        Some(original.geometry.clone())
    );
    assert_eq!(hw.disk_storage(&key(&original)).await.unwrap(), Some(original));
}

#[tokio::test]
async fn concurrent_parent_cleanup_cannot_orphan_a_successful_admission() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let original = record(5);
    let (admitted, deleted) = tokio::join!(
        hw.admit_disk_storage(&original),
        hw.remove_disk_group_cascade(1, 2, 3)
    );
    assert!(
        !(admitted.is_ok() && deleted.is_ok()),
        "conflicting mutations both committed"
    );
    if admitted.is_ok() {
        assert!(hw.get_disk_group(1, 2, 3).await.unwrap().is_some());
        assert_eq!(
            hw.disk_storage(&key(&original)).await.unwrap(),
            Some(original.clone())
        );
        assert!(hw
            .get_disk_with_revision(&key(&original))
            .await
            .unwrap()
            .is_some());
    } else {
        assert!(hw.disk_storage(&key(&original)).await.unwrap().is_none());
        assert!(hw
            .get_disk_with_revision(&key(&original))
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn initial_activation_requires_the_original_receipt_and_never_overrides_offline() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let original = record(5);
    hw.admit_disk_storage(&original).await.unwrap();
    let mut changed = original.clone();
    changed.backing_identity = "file:10:99".into();
    assert!(hw.activate_disk_storage(&changed).await.is_err());
    assert_eq!(
        hw.get_disk(1, 2, 3, &original.disk_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        HwStatus::Init as i32
    );
    hw.activate_disk_storage(&original).await.unwrap();
    assert_eq!(
        hw.get_disk(1, 2, 3, &original.disk_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        HwStatus::Up as i32
    );
    hw.set_disk_status(1, 2, 3, &original.disk_id, HwStatus::Offline)
        .await
        .unwrap();
    assert!(hw.activate_disk_storage(&original).await.is_err());
    assert_eq!(
        hw.get_disk(1, 2, 3, &original.disk_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        HwStatus::Offline as i32
    );
}
