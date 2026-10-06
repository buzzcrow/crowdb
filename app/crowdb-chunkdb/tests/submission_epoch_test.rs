// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb::range_guard::RangeGuard;
use crowdb_protocol::chunk_slot::ChunkSlot;
use crowdb_protocol::common::ChunkId;
#[path = "common/slot_groups.rs"]
mod slot_groups;
#[path = "common/submission_epoch.rs"]
mod submission_epoch;
use submission_epoch::TestEpochLayout;

#[tokio::test]
async fn submission_checks_only_its_slot_owner_epoch_and_keeps_the_original_capture() {
    let guard = RangeGuard::new();
    let id = ChunkId { high: 1, low: 5 };
    let slot = ChunkSlot::for_chunk(&id);
    let other = ChunkSlot::all().find(|other| *other != slot).unwrap();
    guard
        .install_dynamic(&TestEpochLayout::map(1, slot, 1, 1, 1), 1)
        .unwrap();
    let captured = guard.capture();
    let other_changed = TestEpochLayout::map(2, other, 2, 2, 1);
    guard.install_dynamic(&other_changed, 1).unwrap();
    assert!(guard.check_submission(&id, &captured).is_ok());
    let regrant = other_changed.reassign(&[(slot, 1)]).unwrap();
    guard.install_dynamic(&regrant, 1).unwrap();
    assert!(guard.check_submission(&id, &captured).is_err());
    let restarted = guard.capture();
    assert!(guard.check_submission(&id, &restarted).is_ok());
    captured
        .clone()
        .scope(async {
            assert_eq!(guard.capture(), captured);
            assert!(guard.check_submission(&id, &guard.capture()).is_err());
        })
        .await;
    let mut head = regrant.head().clone();
    head.generation += 1;
    let bindings = regrant
        .bindings()
        .iter()
        .map(|binding| crowdb_protocol::chunk_slot::ChunkSlotBinding {
            generation: head.generation,
            owner: crowdb_protocol::chunk_slot::ChunkSlotAuthority::new(
                binding.owner.instance_id(),
                crowdb_protocol::chunk_slot::ChunkServiceIncarnation::try_from([2; 16]).unwrap(),
                binding.owner.generation(),
            )
            .unwrap(),
            slots: binding.slots.clone(),
        })
        .collect();
    guard
        .install_dynamic(
            &crowdb_protocol::chunk_slot::ChunkSlotMap::new(head, bindings).unwrap(),
            1,
        )
        .unwrap();
    assert!(guard.check_submission(&id, &restarted).is_ok());
    assert!(guard
        .install_dynamic(&TestEpochLayout::map(5, slot, 1, 1, 1), 1)
        .is_err());
}

#[tokio::test]
async fn stale_execution_cannot_write_metadata_reservations_or_tasks_and_fresh_execution_recovers() {
    use crowdb_chunkdb::routing::{BindingCache, BindingTable};
    use crowdb_chunkdb::storage::{ChunkStore, StoreError};
    use crowdb_chunkdb::task::{TaskStore, TaskStoreError};
    use crowdb_protocol::chunk_domain::ChunkDomain;
    use crowdb_protocol::chunk_slot::{ChunkSlotBootstrap, ChunkStorageGroup};
    use crowdb_protocol::chunkdb::rpc::{Chunk, StripReservationGroup};
    use std::sync::Arc;
    let test = slot_groups::TestGroups::start().await;
    let routes = BindingCache::new();
    routes
        .replace(BindingTable::new(
            ChunkSlotBootstrap {
                service_instances: vec![1],
                storage_groups: vec![ChunkStorageGroup {
                    store_id: 0,
                    group_id: 1,
                }],
            }
            .storage_map()
            .unwrap(),
        ))
        .unwrap();
    for (purpose, domain) in [(1_u64, ChunkDomain::System), (5, ChunkDomain::UserData)] {
        let id = ChunkId {
            high: purpose << 56,
            low: 10,
        };
        let slot = ChunkSlot::for_chunk(&id);
        let guard = Arc::new(RangeGuard::new());
        let initial = TestEpochLayout::map(1, slot, 1, 1, 1);
        guard.install_dynamic(&initial, 1).unwrap();
        let store =
            ChunkStore::new(Arc::clone(&test.kv), routes.clone()).with_scope(Arc::clone(&guard), domain);
        let tasks =
            TaskStore::new(Arc::clone(&test.kv), routes.clone()).with_scope(Arc::clone(&guard), domain);
        let chunk = Chunk {
            id: Some(id),
            modify_ts: 1,
            chunk_type: i32::try_from(purpose).unwrap(),
            ..Default::default()
        };
        store
            .create_chunk_with_finalize_task(&chunk, &slot_groups::finalize(id))
            .await
            .unwrap();
        let stale = guard.capture();
        guard
            .install_dynamic(&initial.reassign(&[(slot, 1)]).unwrap(), 1)
            .unwrap();
        let group = StripReservationGroup {
            chunk_id: Some(id),
            group_id: Some(id),
            ..Default::default()
        };
        stale
            .scope(async {
                let mut changed = chunk.clone();
                changed.modify_ts = 2;
                assert!(matches!(
                    store.put_chunk(&changed).await,
                    Err(StoreError::OwnershipChanged(_))
                ));
                assert!(matches!(
                    store.put_reservation_group(&group).await,
                    Err(StoreError::OwnershipChanged(_))
                ));
                assert!(matches!(
                    tasks.renew_finalize_chunk(&id, 1, 100, 100).await,
                    Err(TaskStoreError::Authority)
                ));
                assert!(matches!(
                    store.delete_chunk(&id).await,
                    Err(StoreError::OwnershipChanged(_))
                ));
            })
            .await;
        assert_eq!(store.get_chunk(&id).await.unwrap().modify_ts, 1);
        assert!(store.get_reservation_group(&id, &id).await.unwrap().is_none());
        guard
            .capture()
            .scope(async {
                let mut changed = chunk.clone();
                changed.modify_ts = 2;
                store.put_chunk(&changed).await.unwrap();
                store.put_reservation_group(&group).await.unwrap();
            })
            .await;
        assert_eq!(store.get_chunk(&id).await.unwrap().modify_ts, 2);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn epoch_publication_does_not_drain_an_accepted_data_write_and_the_final_check_race_is_allowed() {
    use crowdb_kv_client::{ChunkSlotMapClient, GetOutcome, ReadMode};
    use crowdb_protocol::chunk_slot::{ChunkSlotBootstrap, ChunkStorageGroup};
    use std::{sync::Arc, time::Duration};
    let test = slot_groups::TestGroups::start().await;
    let maps = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    maps.initialize_layout(&ChunkSlotBootstrap {
        service_instances: vec![1],
        storage_groups: vec![ChunkStorageGroup {
            store_id: 0,
            group_id: 1,
        }],
    })
    .await
    .unwrap();
    maps.initialize_service_epochs().await.unwrap();
    let initial = maps.read_service_snapshot().await.unwrap();
    let id = ChunkId {
        high: 5 << 56,
        low: 20,
    };
    let guard = RangeGuard::new();
    guard.install_dynamic(initial.authority(), 1).unwrap();
    let capture = guard.capture();
    guard.check_submission(&id, &capture).unwrap();
    let group = test.server.get_group(1).unwrap();
    group.set_coalesce_max_keys_for_tests(32);
    let (release, gate) = tokio::sync::oneshot::channel();
    group.set_coalesce_round_gate_for_tests(gate);
    let kv = Arc::clone(&test.kv);
    let accepted = tokio::spawn(async move { kv.put(0, 1, b"accepted-old-write", b"done", None).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !group.has_coalesce_pending_for_tests() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let moved = initial
        .authority()
        .reassign(&[(ChunkSlot::for_chunk(&id), 2)])
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), maps.publish_service_epochs(&moved))
        .await
        .unwrap()
        .unwrap();
    guard.install_dynamic(&moved, 1).unwrap();
    assert!(guard.check_submission(&id, &capture).is_err());
    assert!(!accepted.is_finished());
    release.send(()).unwrap();
    accepted.await.unwrap().unwrap();
    assert!(
        matches!(test.kv.get(0,1,b"accepted-old-write",ReadMode::Linearizable,None).await.unwrap(),GetOutcome::Found {value,..} if value.as_ref() == b"done")
    );
    // A successful final local check is not a distributed fence: entry can race with publication.
    let successor = RangeGuard::new();
    successor.install_dynamic(&moved, 2).unwrap();
    successor.check_submission(&id, &successor.capture()).unwrap();
    maps.publish_service_epochs(&moved.reassign(&[(ChunkSlot::for_chunk(&id), 1)]).unwrap())
        .await
        .unwrap();
    test.kv
        .put_cas(0, 1, b"final-check-race", b"accepted", 0)
        .await
        .unwrap();
}
