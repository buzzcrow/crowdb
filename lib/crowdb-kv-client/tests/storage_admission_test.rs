// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/storage.rs"]
mod storage;
use crowdb_protocol::{
    common::{DiskId, HwStatus, NodeValue, RackValue},
    diskdb::rpc::DiskGroupValue,
    key::TextKey,
};
use storage::{key, record, TestStorage};

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
    let mut original = record(5);
    original.media_type = Some(0);
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

#[tokio::test]
async fn diskio_observations_retain_exact_revisions_and_reject_changed_geometry() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let mut original = record(u64::MAX);
    original.disk_id.high = u64::MAX;
    hw.admit_disk_storage(&original).await.unwrap();
    let before = hw.disk_io_inventory(1, 2, None).await.unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].storage, Some(original.clone()));
    let wire = serde_json::to_value(&before).unwrap();
    assert_eq!(wire[0]["disk_id"]["high"], u64::MAX.to_string());
    assert_eq!(wire[0]["storage"]["disk_id"]["high"], u64::MAX.to_string());
    assert_eq!(wire[0]["storage"]["disk_id"]["low"], u64::MAX.to_string());
    assert_eq!(wire[0]["revision"], before[0].revision.to_string());
    hw.set_disk_status(1, 2, 3, &original.disk_id, HwStatus::Offline)
        .await
        .unwrap();
    let after = hw.disk_io_inventory(1, 2, Some(3)).await.unwrap();
    assert!(after[0].revision > before[0].revision);
    assert_eq!(after[0].value.status, HwStatus::Offline as i32);
    assert_eq!(after[0].storage, Some(original.clone()));
    let mut changed = original.geometry.clone();
    changed.capacity_units += 1;
    assert!(hw.add_disk(1, 2, 3, &original.disk_id, &changed).await.is_err());
    hw.kv()
        .put(
            0,
            0,
            key(&original).to_path().as_bytes(),
            &serde_json::to_vec(&changed).unwrap(),
            None,
        )
        .await
        .unwrap();
    assert!(hw.disk_io_inventory(1, 2, None).await.is_err());
}

#[tokio::test]
async fn compatibility_writes_cannot_replace_retained_placement_or_erase_interpretation_metadata() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let original = record(5);
    hw.admit_disk_storage(&original).await.unwrap();
    let rack = hw.get_rack(1).await.unwrap().unwrap();
    let node = hw.get_node(1, 2).await.unwrap().unwrap();
    let group = hw.get_disk_group(1, 2, 3).await.unwrap().unwrap().value;
    hw.add_rack(1, &rack).await.unwrap();
    hw.add_node(1, 2, &node).await.unwrap();
    hw.add_disk_group(1, 2, 3, &group).await.unwrap();
    assert!(hw.add_rack(1, &RackValue::default()).await.is_err());
    assert!(hw.add_node(1, 2, &NodeValue::default()).await.is_err());
    assert!(hw
        .add_disk_group(1, 2, 3, &DiskGroupValue::default())
        .await
        .is_err());
    assert!(hw
        .add_disk_group_with_owner(1, 2, 3, &DiskGroupValue::default(), 8, 100)
        .await
        .is_err());
    hw.add_disk_group_with_owner(1, 2, 3, &group, 8, 100)
        .await
        .unwrap();
    hw.set_bind(1, 2, 3, 1, 1).await.unwrap();
    hw.set_bind(1, 2, 3, 1, 1).await.unwrap();
    assert!(hw.set_bind(1, 2, 3, 1, 2).await.is_err());
    assert!(hw.remove_bind(1, 2, 3).await.is_err());
    assert!(hw.remove_owner(1, 2, 3).await.is_err());
    assert_eq!(hw.get_disk_group(1, 2, 3).await.unwrap().unwrap().value, group);
    assert_eq!(hw.get_node(1, 2).await.unwrap().unwrap(), node);
    assert_eq!(hw.disk_storage(&key(&original)).await.unwrap(), Some(original));
}

#[tokio::test]
async fn duplicate_checks_cover_every_inventory_page() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    for page in 0..20_u64 {
        let mut operations = Vec::new();
        for offset in 0..16_u64 {
            let id = 1000 + page * 16 + offset;
            let mut retained = record(id);
            retained.backing_identity = format!("file:10:{id}");
            retained.geometry.device_path = format!("/opt/crowdb/data/disks/{id}.img");
            retained.removed = true;
            operations.push(crowdb_kv_client::BatchOp::Put {
                key: retained.key().into_bytes().into(),
                value: serde_json::to_vec(&retained).unwrap().into(),
            });
        }
        hw.kv().batch_write(0, 0, &operations).await.unwrap();
    }
    hw.admit_disk_storage(&record(5)).await.unwrap();
    let mut alias = record(6);
    alias.backing_identity = "file:10:1319".into();
    alias.geometry.device_path = "/opt/crowdb/data/disks/alias.img".into();
    assert!(hw.admit_disk_storage(&alias).await.is_err());
    assert!(hw.get_disk_with_revision(&key(&alias)).await.unwrap().is_none());
}
