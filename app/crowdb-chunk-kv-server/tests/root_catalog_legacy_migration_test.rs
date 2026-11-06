// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_chunk_kv_server::storage::open_root_catalog_for_tests;
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState, Id128, KeyRange, OwnerDescriptor,
    PartitionArtifact,
};
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_tree_ffi::{CtError, RootCatalogObject};
#[path = "common/metadata.rs"]
mod metadata;

fn entry() -> ChunkKvRangeCatalogEntry {
    ChunkKvRangeCatalogEntry {
        partition_id: Id128 { high: 1, low: 1 },
        range: KeyRange {
            start: vec![],
            end: None,
        },
        owner: OwnerDescriptor {
            instance_id: 7,
            rpc_endpoint: "127.0.0.1:1".into(),
        },
        owner_epoch: 7,
        state: ChunkKvRangeCatalogPartitionState::Serving,
        artifact: PartitionArtifact {
            tree_id: 42,
            stream_name: StreamName { high: 1, low: 2 },
            tail_overlay: None,
        },
        transition_id: None,
    }
}
fn manifest() -> &'static [u8] {
    let fixture = include_bytes!("../../../lib/crowdb-tree/tests/data/legacy-chunk-v7.bin");
    let length = usize::try_from(u64::from_le_bytes(fixture[8..16].try_into().unwrap())).unwrap();
    &fixture[16..16 + length]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receipt_is_idempotent_and_survives_old_manifest_retention() {
    let metadata = metadata::TestMetadata::start().await;
    let store = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry())
        .await
        .unwrap();
    store.publish(42, 0, 7, 1, manifest()).unwrap();
    store.prepare_legacy_migration(42, 1, manifest()).unwrap();
    store.prepare_legacy_migration(42, 1, manifest()).unwrap();
    for generation in 2..=4 {
        store
            .publish(42, generation - 1, 7, generation, b"new root")
            .unwrap();
    }
    assert_eq!(store.reclaim_before(42, 4), 2);
    assert!(store.load(42, RootCatalogObject::Manifest(1)).unwrap().is_none());
    assert_eq!(store.load_legacy_migration(42, 1).unwrap().unwrap(), manifest());
    store.prepare_legacy_migration(42, 1, manifest()).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changed_generation_or_corrupt_bytes_cannot_prepare_a_receipt() {
    let metadata = metadata::TestMetadata::start().await;
    let store = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry())
        .await
        .unwrap();
    store.publish(42, 0, 7, 1, manifest()).unwrap();
    assert_eq!(
        store.prepare_legacy_migration(42, 2, manifest()),
        Err(CtError::InvalidArgument)
    );
    let mut damaged = manifest().to_vec();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    assert_eq!(
        store.prepare_legacy_migration(42, 1, &damaged),
        Err(CtError::Corruption)
    );
    store.publish(42, 1, 7, 2, b"replacement root").unwrap();
    assert_eq!(
        store.prepare_legacy_migration(42, 1, manifest()),
        Err(CtError::Unavailable)
    );
    assert!(store.load_legacy_migration(42, 1).unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_generation_pin_and_receipt_preserve_both_authority_updates() {
    let metadata = metadata::TestMetadata::start().await;
    let store = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry())
        .await
        .unwrap();
    store.publish(42, 0, 7, 1, manifest()).unwrap();
    let writer = store.clone();
    let receipt = tokio::task::spawn_blocking(move || writer.prepare_legacy_migration(42, 1, manifest()));
    let reader = store.clone();
    let pin = tokio::task::spawn_blocking(move || reader.pin_generation(42, 9, 3, 1));
    receipt.await.unwrap().unwrap();
    pin.await.unwrap().unwrap();
    assert_eq!(store.load_legacy_migration(42, 1).unwrap().unwrap(), manifest());
    assert_eq!(store.reclaim_before(42, 4), 0);
    store.unpin_generation(42, 9, 3).unwrap();
}
