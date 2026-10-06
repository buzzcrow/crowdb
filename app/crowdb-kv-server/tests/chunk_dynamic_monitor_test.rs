// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dynamic_slots.rs"]
mod dynamic_slots;
use bytes::Bytes;
use crowdb_kv_server::background::domain_monitor::{ChunkdbDynamicMonitorDriver, DomainMonitorDriver};
use crowdb_protocol::key::{ChunkSlotMapHeadKey, TextKey};
use dynamic_slots::{control, descriptor, entry, initialize, TestDynamicOwners};

#[tokio::test]
async fn join_balances_service_slots_in_bounded_batches_without_changing_storage() {
    let (_store, control) = control().await;
    initialize(&control).await;
    let storage = control
        .scan_all_prefix(Bytes::from_static(b"/chunkdb/slot_storage/"), 8)
        .await
        .unwrap();
    let storage_head = control
        .get(ChunkSlotMapHeadKey::Storage.to_path().as_bytes())
        .await
        .unwrap();
    for owner in 1..=4 {
        TestDynamicOwners::register(&control, owner, u64::MAX).await;
    }
    let driver = ChunkdbDynamicMonitorDriver;
    driver.tick(&control, &descriptor()).await.unwrap();
    let first = TestDynamicOwners::bindings(&control).await;
    assert_eq!(
        first
            .iter()
            .find(|binding| binding.owner == 4)
            .unwrap()
            .slots
            .slots()
            .count(),
        64
    );
    for _ in 0..10 {
        driver.tick(&control, &descriptor()).await.unwrap();
    }
    let bindings = TestDynamicOwners::bindings(&control).await;
    assert_eq!(bindings.len(), 4);
    for binding in bindings {
        assert_eq!(binding.slots.slots().count(), 256);
    }
    assert_eq!(
        storage,
        control
            .scan_all_prefix(Bytes::from_static(b"/chunkdb/slot_storage/"), 8)
            .await
            .unwrap()
    );
    let after = control
        .get(ChunkSlotMapHeadKey::Storage.to_path().as_bytes())
        .await
        .unwrap();
    assert_eq!(storage_head.value, after.value);
    assert_eq!(storage_head.revision, after.revision);
}

#[tokio::test]
async fn missing_owner_grace_survives_controller_replacement_and_resets_after_recovery() {
    let (_store, control) = control().await;
    initialize(&control).await;
    for owner in 2..=3 {
        TestDynamicOwners::register(&control, owner, u64::MAX).await;
    }
    let before = TestDynamicOwners::bindings(&control).await;
    ChunkdbDynamicMonitorDriver
        .tick(&control, &descriptor())
        .await
        .unwrap();
    ChunkdbDynamicMonitorDriver
        .tick(&control, &descriptor())
        .await
        .unwrap();
    assert_eq!(before, TestDynamicOwners::bindings(&control).await);
    let path = "/chunkdb/slot_suspect/1";
    assert!(control.get(path.as_bytes()).await.unwrap().value.is_some());
    TestDynamicOwners::register(&control, 1, u64::MAX).await;
    ChunkdbDynamicMonitorDriver
        .tick(&control, &descriptor())
        .await
        .unwrap();
    assert!(control.get(path.as_bytes()).await.unwrap().value.is_none());
    TestDynamicOwners::register(&control, 1, 1).await;
    ChunkdbDynamicMonitorDriver
        .tick(&control, &descriptor())
        .await
        .unwrap();
    assert!(
        TestDynamicOwners::bindings(&control)
            .await
            .iter()
            .find(|binding| binding.owner == 1)
            .unwrap()
            .slots
            .slots()
            .count()
            < 342
    );
}

#[tokio::test]
async fn corrupt_dynamic_snapshot_is_rejected_without_overwriting_it() {
    let (_store, control) = control().await;
    initialize(&control).await;
    TestDynamicOwners::register(&control, 4, u64::MAX).await;
    control
        .put_batch(vec![entry(
            ChunkSlotMapHeadKey::Authority.to_path(),
            &crowdb_protocol::chunk_slot::ChunkSlotMapHead {
                layout_version: 1,
                generation: 2,
                owner_count: 3,
            },
        )])
        .await
        .unwrap();
    let before = control
        .scan_all_prefix(Bytes::from_static(b"/chunkdb/"), 16)
        .await
        .unwrap();
    assert!(ChunkdbDynamicMonitorDriver
        .tick(&control, &descriptor())
        .await
        .is_err());
    assert_eq!(
        before,
        control
            .scan_all_prefix(Bytes::from_static(b"/chunkdb/"), 16)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn replacement_group_zero_tenure_resumes_and_obsolete_control_cannot_publish() {
    use crowdb_kv::cluster::group_election::LeaderElection;
    use crowdb_kv_server::group0_control_plane::Group0ControlPlane;
    let (store, control) = control().await;
    initialize(&control).await;
    for owner in 1..=4 {
        TestDynamicOwners::register(&control, owner, u64::MAX).await;
    }
    ChunkdbDynamicMonitorDriver
        .tick(&control, &descriptor())
        .await
        .unwrap();
    let before = TestDynamicOwners::bindings(&control).await;
    let group = store.get_group(0).unwrap();
    group.local_replica().become_follower(1);
    group.local_replica().become_leader();
    group.stamp_proposing_term(1);
    assert!(ChunkdbDynamicMonitorDriver
        .tick(&control, &descriptor())
        .await
        .is_err());
    let successor = Group0ControlPlane::acquire(&store).await.unwrap();
    for _ in 0..10 {
        ChunkdbDynamicMonitorDriver
            .tick(&successor, &descriptor())
            .await
            .unwrap();
    }
    let after = TestDynamicOwners::bindings(&successor).await;
    assert!(after[0].generation > before[0].generation);
    for binding in after {
        assert_eq!(binding.slots.slots().count(), 256);
    }
}
