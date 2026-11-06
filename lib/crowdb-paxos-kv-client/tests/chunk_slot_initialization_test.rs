// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/slot_map.rs"]
mod slot_map;

use std::sync::Arc;

use bytes::Bytes;
use crowdb_paxos_kv_client::{BatchOp, ChunkSlotMapClient, Error};
use crowdb_protocol::chunk_slot::{ChunkSlot, ChunkSlotMap};
use crowdb_protocol::key::{ChunkServiceSlotsKey, ChunkSlotMapHeadKey, TextKey};
use slot_map::{services, TestSlotMap};

#[tokio::test]
async fn large_bootstrap_preserves_all_paginated_owners_and_is_idempotent() {
    let test = TestSlotMap::start().await;
    let client = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let map = services(1, 300);
    client.initialize_service(&map).await.unwrap();
    client.initialize_service(&map).await.unwrap();
    let actual = client.read_service().await.unwrap();
    assert_eq!(actual.bindings().len(), 300);
    assert_eq!(
        actual
            .bindings()
            .iter()
            .filter(|entry| entry.slots.is_empty())
            .count(),
        299
    );
    assert!(ChunkSlot::all().all(|slot| actual.owner(slot) == 1));
}

#[tokio::test]
async fn interrupted_bootstrap_is_hidden_resumable_and_fences_stale_pages() {
    let test = TestSlotMap::start().await;
    let map = services(1, 300);
    let mut bindings = map.bindings().to_vec();
    bindings.sort_by_key(|binding| {
        ChunkServiceSlotsKey {
            instance_id: binding.owner,
        }
        .to_path()
    });
    let head = ChunkSlotMapHeadKey::Service.to_path();
    let pending = serde_json::json!({"initializing": {"head": map.head(), "bindings": bindings}, "next": 63});
    let mut operations: Vec<_> = bindings[..63]
        .iter()
        .map(|binding| BatchOp::Put {
            key: Bytes::from(
                ChunkServiceSlotsKey {
                    instance_id: binding.owner,
                }
                .to_path(),
            ),
            value: Bytes::from(serde_json::to_vec(binding).unwrap()),
        })
        .collect();
    operations.push(BatchOp::Put {
        key: Bytes::from(head.clone()),
        value: Bytes::from(serde_json::to_vec(&pending).unwrap()),
    });
    let partial = test
        .kv
        .batch_write_cas(0, 0, &operations, head.as_bytes(), 0)
        .await
        .unwrap();
    let first = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    assert!(first.read_service().await.is_err());
    assert!(first.initialize_service(&services(1000, 300)).await.is_err());
    drop(first);
    let resumed = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    bindings.reverse();
    let reordered = ChunkSlotMap::new(map.head().clone(), bindings).unwrap();
    resumed.initialize_service(&reordered).await.unwrap();
    assert_eq!(resumed.read_service().await.unwrap().bindings().len(), 300);
    assert!(matches!(
        test.kv
            .batch_write_cas(0, 0, &operations, head.as_bytes(), partial.revision)
            .await,
        Err(Error::CasFailed { .. })
    ));
    assert_eq!(resumed.read_service().await.unwrap().bindings().len(), 300);
}

#[tokio::test]
async fn competing_large_and_small_bootstraps_cannot_mix_assignments() {
    for count in [2, 300] {
        let test = TestSlotMap::start().await;
        let client = ChunkSlotMapClient::new(Arc::clone(&test.kv));
        let large = services(1, 300);
        let competitor = services(1000, count);
        let (a, b) = tokio::join!(
            client.initialize_service(&large),
            client.initialize_service(&competitor)
        );
        assert_ne!(a.is_ok(), b.is_ok(), "large={a:?}, competitor={b:?}");
        let actual = client.read_service().await.unwrap();
        let expected = if a.is_ok() { &large } else { &competitor };
        assert_eq!(actual.head(), expected.head());
        assert!(actual
            .bindings()
            .iter()
            .all(|binding| expected.bindings().contains(binding)));
    }
}
