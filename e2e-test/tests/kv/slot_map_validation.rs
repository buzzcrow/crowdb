// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::slot_map::{services, TestGroup0};
use crowdb_paxos_kv_client::{ChunkSlotMapClient, GetOutcome, ReadMode};
use crowdb_protocol::key::{ChunkServiceSlotsKey, ChunkSlotMapHeadKey, TextKey};
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires the owned release-container Group-0 slot map profile"]
async fn legacy_records_are_never_overwritten() {
    let test = TestGroup0::start().await;
    let client = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    test.kv
        .put(0, 0, b"/chunkdb/range_bind/0", b"legacy", None)
        .await
        .unwrap();
    assert!(client.initialize_service(&services(10)).await.is_err());
    assert!(client.read_service().await.is_err());
    assert!(
        matches!(test.kv.get(0, 0, b"/chunkdb/range_bind/0", ReadMode::Linearizable, None).await.unwrap(),
        GetOutcome::Found { value, .. } if value.as_ref() == b"legacy")
    );
    assert!(matches!(
        test.kv
            .get(
                0,
                0,
                ChunkSlotMapHeadKey::Service.to_path().as_bytes(),
                ReadMode::Linearizable,
                None
            )
            .await
            .unwrap(),
        GetOutcome::NotFound
    ));
}

#[tokio::test]
#[ignore = "requires the owned release-container Group-0 slot map profile"]
async fn legacy_chunk_state_without_range_bindings_is_not_reinterpreted() {
    let test = TestGroup0::start().await;
    test.kv
        .put(0, 0, b"/chunk/old-id", b"existing", None)
        .await
        .unwrap();
    let client = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    assert!(client.initialize_service(&services(10)).await.is_err());
    assert!(matches!(
        test.kv
            .get(0, 0, b"/chunk/old-id", ReadMode::Linearizable, None)
            .await
            .unwrap(),
        GetOutcome::Found { value, .. } if value.as_ref() == b"existing"
    ));
}

#[tokio::test]
#[ignore = "requires the owned release-container Group-0 slot map profile"]
async fn corrupt_owner_and_partial_generation_fail_closed() {
    let test = TestGroup0::start().await;
    let client = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    client.initialize_service(&services(10)).await.unwrap();
    let key = ChunkServiceSlotsKey { instance_id: 10 }.to_path();
    let mut binding = services(10).bindings()[0].clone();
    binding.generation = 2;
    test.kv
        .put(0, 0, key.as_bytes(), &serde_json::to_vec(&binding).unwrap(), None)
        .await
        .unwrap();
    assert!(client.read_service().await.is_err());
    binding.generation = 1;
    binding.owner = 12;
    test.kv
        .put(0, 0, key.as_bytes(), &serde_json::to_vec(&binding).unwrap(), None)
        .await
        .unwrap();
    assert!(client.read_service().await.is_err());
}

#[tokio::test]
#[ignore = "requires the owned release-container Group-0 slot map profile"]
async fn orphan_binding_is_preserved_without_creating_a_head() {
    let test = TestGroup0::start().await;
    let client = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let map = services(10);
    let key = ChunkServiceSlotsKey { instance_id: 10 }.to_path();
    let bytes = serde_json::to_vec(&map.bindings()[0]).unwrap();
    test.kv.put(0, 0, key.as_bytes(), &bytes, None).await.unwrap();
    assert!(client.initialize_service(&map).await.is_err());
    let GetOutcome::Found { value, .. } = test
        .kv
        .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("orphan record was lost");
    };
    assert_eq!(value.as_ref(), bytes.as_slice());
    assert!(client.read_service().await.is_err());
}
