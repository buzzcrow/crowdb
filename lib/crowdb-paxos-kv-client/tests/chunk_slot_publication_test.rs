// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/chunk_slot_publication.rs"]
mod fixture;
#[path = "common/publication_peer.rs"]
mod peer;

use crowdb_paxos_kv::cluster::group::PxGroup;
use crowdb_paxos_kv_client::{ChunkServiceSnapshot, ChunkSlotMapClient, ClientConfig, Error, PaxosKvClient};
use crowdb_protocol::{
    chunk_slot::{ChunkSlot, ChunkSlotAuthority, ChunkSlotMap},
    key::{ChunkSlotMapHeadKey, TextKey},
};
use fixture::TestChunkSlotPublication;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

#[tokio::test]
async fn ambiguous_publication_does_not_rebase_after_busy_reconciliation() {
    let peer = peer::TestPublicationPeer::start().await;
    let kv = Arc::new(PaxosKvClient::new(ClientConfig::new(vec![])));
    kv.seed_leader(0, 0, peer.endpoint.clone());
    let maps = ChunkSlotMapClient::new(kv);
    let before = maps.read_service_snapshot().await.unwrap();
    let slots: Vec<_> = ChunkSlot::all()
        .filter(|slot| before.service().owner(*slot) == 1)
        .map(|slot| (slot, 1))
        .collect();
    let expected = before.authority().reassign(&slots).unwrap();
    let result = maps.regrant_service_epochs(1).await;
    assert!(
        matches!(result, Err(Error::OutcomeUnknown)),
        "ambiguous publication must not become retryable: {result:?}; submissions={}; generation={}",
        peer.submissions.load(Ordering::Acquire),
        peer.committed_generation.load(Ordering::Acquire)
    );
    assert_eq!(peer.submissions.load(Ordering::Acquire), 1);
    assert_complete(&maps.read_service_snapshot().await.unwrap(), &expected);
}

async fn wait_for_head_admission(group: &PxGroup) {
    let key = ChunkSlotMapHeadKey::Service.to_path();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !group.conditional_write_pending_for_tests(key.as_bytes()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("publication must reach the held head admission");
}

fn assert_complete(actual: &ChunkServiceSnapshot, expected: &ChunkSlotMap<ChunkSlotAuthority>) {
    assert_eq!(actual.authority().head(), expected.head());
    assert_eq!(actual.service().head().generation, expected.head().generation);
    for slot in ChunkSlot::all() {
        assert_eq!(actual.authority().owner(slot), expected.owner(slot));
        assert_eq!(actual.service().owner(slot), expected.owner(slot).instance_id());
    }
}

#[tokio::test]
async fn fixed_intent_caller_observes_busy_then_reconciles_before_explicit_republication() {
    let test = TestChunkSlotPublication::start().await;
    let maps = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let before = maps.read_service_snapshot().await.unwrap();
    let storage = maps.read_storage().await.unwrap();
    let revision = test.head_revision().await;
    let next = before
        .authority()
        .reassign(&[(ChunkSlot::try_from(0).unwrap(), 3)])
        .unwrap();
    let group = test.store.get_group(0).unwrap();
    let permits = group.try_acquire_all_inflight_permits();
    let kv = Arc::clone(&test.kv);
    let pending = next.clone();
    let first =
        tokio::spawn(async move { ChunkSlotMapClient::new(kv).publish_service_epochs(&pending).await });
    wait_for_head_admission(&group).await;

    assert!(matches!(
        maps.publish_service_epochs(&next).await,
        Err(Error::CasBusy)
    ));
    assert!(!first.is_finished());
    assert_complete(&maps.read_service_snapshot().await.unwrap(), before.authority());
    assert_eq!(test.head_revision().await, revision);
    assert_eq!(maps.read_storage().await.unwrap().bindings(), storage.bindings());

    drop(permits);
    first.await.unwrap().unwrap();
    let committed = maps.read_service_snapshot().await.unwrap();
    assert_complete(&committed, &next);
    assert_eq!(maps.read_storage().await.unwrap().bindings(), storage.bindings());
    let committed_revision = test.head_revision().await;

    // The caller now has a complete committed witness for its original intent.
    maps.publish_service_epochs(&next).await.unwrap();
    assert_complete(&maps.read_service_snapshot().await.unwrap(), &next);
    assert_eq!(test.head_revision().await, committed_revision);
}

#[tokio::test]
async fn restart_regrant_exhausts_busy_policy_then_recomputes_only_current_owned_slots() {
    let test = TestChunkSlotPublication::start().await;
    let maps = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let before = maps.read_service_snapshot().await.unwrap();
    let storage = maps.read_storage().await.unwrap();
    let revision = test.head_revision().await;
    let next = before
        .authority()
        .reassign(&[(ChunkSlot::try_from(0).unwrap(), 3)])
        .unwrap();
    let group = test.store.get_group(0).unwrap();
    let permits = group.try_acquire_all_inflight_permits();
    let kv = Arc::clone(&test.kv);
    let pending = next.clone();
    let first =
        tokio::spawn(async move { ChunkSlotMapClient::new(kv).publish_service_epochs(&pending).await });
    wait_for_head_admission(&group).await;

    assert!(matches!(
        maps.regrant_service_epochs(2).await,
        Err(Error::CasBusy)
    ));
    assert!(!first.is_finished());
    assert_complete(&maps.read_service_snapshot().await.unwrap(), before.authority());
    assert_eq!(test.head_revision().await, revision);
    assert_eq!(maps.read_storage().await.unwrap().bindings(), storage.bindings());

    drop(permits);
    first.await.unwrap().unwrap();
    assert_complete(&maps.read_service_snapshot().await.unwrap(), &next);
    assert_eq!(maps.read_storage().await.unwrap().bindings(), storage.bindings());
    let slots: Vec<_> = ChunkSlot::all()
        .filter(|slot| next.owner(*slot).instance_id() == 2)
        .map(|slot| (slot, 2))
        .collect();
    let expected = next.reassign(&slots).unwrap();

    // Restart intent is instance-scoped; it deliberately uses the current map.
    maps.regrant_service_epochs(2).await.unwrap();
    assert_complete(&maps.read_service_snapshot().await.unwrap(), &expected);
    assert_eq!(maps.read_storage().await.unwrap().bindings(), storage.bindings());
}
