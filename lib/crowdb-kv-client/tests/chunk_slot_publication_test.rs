// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/handoff_cluster.rs"]
mod handoff_cluster;

use crowdb_kv_client::ChunkSlotMapClient;
use crowdb_protocol::chunk_slot::{
    ChunkServiceIncarnation, ChunkSlot, ChunkSlotAuthority, ChunkSlotBinding, ChunkSlotBitmap, ChunkSlotMap,
};
use crowdb_protocol::key::{ChunkServiceAuthorityKey, TextKey};
use handoff_cluster::TestHandoffCluster;
use std::{collections::HashMap, sync::Arc};

#[tokio::test]
async fn missing_cached_endpoint_refreshes_dynamic_owner_before_routing() {
    let test = TestHandoffCluster::start().await;
    let maps = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    maps.initialize_service_epochs().await.unwrap();
    crowdb_kv_client::ServiceRegistryClient::from_shared(Arc::clone(&test.kv))
        .register_chunkdb(2, "127.0.0.1:17002")
        .await
        .unwrap();
    let before = maps.read_service_snapshot().await.unwrap();
    let chunk = (0..1024)
        .map(|low| crowdb_protocol::common::ChunkId { high: 0, low })
        .find(|id| before.service().owner(ChunkSlot::for_chunk(id)) == 1)
        .unwrap();
    let slot = ChunkSlot::for_chunk(&chunk);
    let routes = crowdb_kv_client::RangeBindingClient::from_shared(Arc::clone(&test.kv));
    routes.refresh().await.unwrap();
    assert!(matches!(
        routes.route_slot(slot),
        Err(crowdb_kv_client::RangeRouteError::NoEndpoint(1))
    ));
    let after = before.authority().reassign(&[(slot, 2)]).unwrap();
    maps.publish_service_epochs(&after).await.unwrap();
    let actual = routes.route(&chunk).await.unwrap();
    assert_eq!(actual.instance_id, 2);
    assert_eq!(actual.rpc_endpoint, "127.0.0.1:17002");
}

#[tokio::test]
async fn stale_epoch_publications_are_conflicts_and_concurrent_regrants_preserve_all_slots() {
    let test = TestHandoffCluster::start().await;
    let client = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    client.initialize_service_epochs().await.unwrap();
    let before = client.read_service_snapshot().await.unwrap();
    let stale = before
        .authority()
        .reassign(&[(ChunkSlot::try_from(0).unwrap(), 3)])
        .unwrap();
    let (first, second) = tokio::join!(client.regrant_service_epochs(1), client.regrant_service_epochs(2));
    first.unwrap();
    second.unwrap();
    assert!(matches!(
        client.publish_service_epochs(&stale).await,
        Err(crowdb_kv_client::Error::CasFailed { .. })
    ));
    let after = client.read_service_snapshot().await.unwrap();
    assert_eq!(
        after.service().head().generation,
        before.service().head().generation + 2
    );
    for slot in ChunkSlot::all() {
        assert_eq!(after.service().owner(slot), before.service().owner(slot));
        assert_eq!(
            after.authority().owner(slot).generation(),
            before.authority().owner(slot).generation() + 1
        );
    }
}

fn epochs(
    service: &crowdb_protocol::chunk_slot::ChunkSlotMap<u64>,
    generation: u64,
    moved: Option<(ChunkSlot, u64, u64)>,
) -> ChunkSlotMap<ChunkSlotAuthority> {
    let mut entries: HashMap<_, ChunkSlotBitmap> = HashMap::new();
    for slot in ChunkSlot::all() {
        let (owner, epoch) = moved
            .filter(|(selected, _, _)| *selected == slot)
            .map_or((service.owner(slot), 1), |(_, owner, epoch)| (owner, epoch));
        let identity =
            ChunkSlotAuthority::new(owner, ChunkServiceIncarnation::try_from([1; 16]).unwrap(), epoch)
                .unwrap();
        entries.entry(identity).or_default().insert(slot);
    }
    let mut bindings: Vec<_> = entries
        .into_iter()
        .map(|(owner, slots)| ChunkSlotBinding {
            generation,
            owner,
            slots,
        })
        .collect();
    bindings.sort_by_key(|binding| {
        ChunkServiceAuthorityKey {
            authority: binding.owner,
        }
        .to_path()
    });
    let mut head = service.head().clone();
    head.generation = generation;
    head.owner_count = u32::try_from(bindings.len()).unwrap();
    ChunkSlotMap::new(head, bindings).unwrap()
}

#[tokio::test]
async fn complete_epoch_publication_is_atomic_idempotent_and_storage_independent() {
    let test = TestHandoffCluster::start().await;
    let client = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let service = client.read_service().await.unwrap();
    let storage = client.read_storage().await.unwrap();
    let bootstrap = epochs(&service, 2, None);
    client.publish_service_epochs(&bootstrap).await.unwrap();
    client.publish_service_epochs(&bootstrap).await.unwrap();
    let slot = ChunkSlot::try_from(0).unwrap();
    let transfer = epochs(&service, 3, Some((slot, 3, 2)));
    let (a, b) = tokio::join!(
        client.publish_service_epochs(&transfer),
        client.publish_service_epochs(&transfer)
    );
    assert!(a.is_ok() && b.is_ok(), "first: {a:?}; second: {b:?}");
    let after = client.read_service_snapshot().await.unwrap();
    for current in ChunkSlot::all() {
        assert_eq!(after.authority().owner(current), transfer.owner(current));
        assert_eq!(
            after.service().owner(current),
            transfer.owner(current).instance_id()
        );
    }
    assert_eq!(
        client.read_storage().await.unwrap().bindings(),
        storage.bindings()
    );
    let reused = epochs(&service, 4, Some((slot, 1, 2)));
    assert!(client.publish_service_epochs(&reused).await.is_err());
    let rollback = epochs(&service, 4, None);
    assert!(client.publish_service_epochs(&rollback).await.is_err());
    let restart = epochs(&service, 4, Some((slot, 3, 3)));
    client.publish_service_epochs(&restart).await.unwrap();
    assert_eq!(
        client
            .read_service_snapshot()
            .await
            .unwrap()
            .authority()
            .owner(slot)
            .generation(),
        3
    );
    let key = ChunkServiceAuthorityKey {
        authority: restart.owner(slot),
    }
    .to_path();
    test.kv.delete(0, 0, key.as_bytes(), None).await.unwrap();
    assert!(client.read_service_snapshot().await.is_err());
}

#[tokio::test]
async fn competing_publications_choose_one_complete_epoch_map_without_changing_data_fences() {
    let test = TestHandoffCluster::start().await;
    let client = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let service = client.read_service().await.unwrap();
    let initial = epochs(&service, 2, None);
    client.publish_service_epochs(&initial).await.unwrap();
    let slot = ChunkSlot::try_from(0).unwrap();
    let a = initial.reassign(&[(slot, 3)]).unwrap();
    let b = initial.reassign(&[(slot, 4)]).unwrap();
    let (first, second) = tokio::join!(
        client.publish_service_epochs(&a),
        client.publish_service_epochs(&b)
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let snapshot = client.read_service_snapshot().await.unwrap();
    assert_eq!(snapshot.service().owner(slot), if first.is_ok() { 3 } else { 4 });
    let path = crowdb_protocol::key::ChunkSlotFenceKey { slot }.to_path();
    let original = test
        .kv
        .get(
            0,
            1,
            path.as_bytes(),
            crowdb_kv_client::ReadMode::Linearizable,
            None,
        )
        .await
        .unwrap();
    assert!(
        matches!(original, crowdb_kv_client::GetOutcome::Found { value, .. } if value.as_ref() == TestHandoffCluster::plan(2).record().transfers[0].previous.unwrap().to_fence_value())
    );
}
