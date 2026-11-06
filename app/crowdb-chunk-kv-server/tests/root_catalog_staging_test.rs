// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_chunk_kv_server::storage::open_root_catalog_for_tests;
use crowdb_paxos_kv_client::{BatchOp, GetOutcome, ReadMode};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState, Id128, KeyRange, OwnerDescriptor,
    PartitionArtifact,
};
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_tree_ffi::chunk::stage::{StageId, StagePack, StageRecord, StageState};
use crowdb_tree_ffi::{ChunkRootCatalog, CtError, RootCatalogObject};
use std::sync::Arc;
#[path = "common/metadata.rs"]
mod metadata;

fn entry(epoch: u64) -> ChunkKvRangeCatalogEntry {
    ChunkKvRangeCatalogEntry {
        partition_id: Id128 { high: 1, low: 1 },
        range: KeyRange {
            start: vec![],
            end: None,
        },
        owner: OwnerDescriptor {
            instance_id: epoch,
            rpc_endpoint: "127.0.0.1:1".into(),
        },
        owner_epoch: epoch,
        state: ChunkKvRangeCatalogPartitionState::Serving,
        artifact: PartitionArtifact {
            tree_id: 42,
            stream_name: StreamName { high: 1, low: 2 },
            tail_overlay: None,
        },
        transition_id: None,
    }
}
fn record(low: u64) -> StageRecord {
    StageRecord {
        tree_id: 42,
        id: StageId {
            owner_epoch: 1,
            operation_high: 100,
            operation_low: low,
        },
        revision: 1,
        chunk_high: 2_u64 << 56,
        chunk_low: low,
        capacity: 4096,
        purpose: 2,
        state: StageState::Reserved,
        manifest_generation: 0,
        packs: vec![],
    }
}
fn next(old: &StageRecord, state: StageState) -> StageRecord {
    let mut out = old.clone();
    out.revision += 1;
    out.state = state;
    out
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callback_stage_roundtrip_survives_reopen_and_retries_the_same_operation() {
    let metadata = metadata::TestMetadata::start().await;
    let store = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry(1))
        .await
        .unwrap();
    let catalog = ChunkRootCatalog::open_callback(Arc::clone(&store)).unwrap();
    assert!(catalog.supports_staging());
    let first = record(1);
    catalog.compare_stage(42, 1, None, Some(&first)).unwrap();
    // Discarding the first result models a successful commit whose reply was lost.
    catalog.compare_stage(42, 1, None, Some(&first)).unwrap();
    assert_eq!(catalog.load_stage(42, first.id).unwrap(), Some(first.clone()));
    let mut writing = next(&first, StageState::Writing);
    writing.packs.push(StagePack {
        offset: 0,
        length: 4096,
        checksum: 7,
    });
    catalog
        .compare_stage(42, 1, Some(&first), Some(&writing))
        .unwrap();
    catalog
        .compare_stage(42, 1, Some(&first), Some(&writing))
        .unwrap();
    let second = record(2);
    catalog.compare_stage(42, 1, None, Some(&second)).unwrap();
    assert_eq!(catalog.list_stages(42, None, 1).unwrap(), vec![writing.clone()]);
    assert_eq!(catalog.list_stages(42, Some(first.id), 1).unwrap(), vec![second]);
    drop(catalog);
    drop(store);
    let reopened = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry(1))
        .await
        .unwrap();
    assert_eq!(reopened.load_stage(42, first.id).unwrap(), Some(writing));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn epoch_fencing_rejects_stale_transitions_and_keeps_unresolved_allocations() {
    let metadata = metadata::TestMetadata::start().await;
    let old = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry(1))
        .await
        .unwrap();
    let first = record(1);
    old.compare_stage(42, 1, None, Some(&first)).unwrap();
    let writing = next(&first, StageState::Writing);
    let current = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry(2))
        .await
        .unwrap();
    assert_eq!(
        old.compare_stage(42, 1, Some(&first), Some(&writing)),
        Err(CtError::Unavailable)
    );
    assert_eq!(
        current.compare_stage(42, 2, Some(&first), Some(&writing)),
        Err(CtError::Unavailable)
    );
    assert_eq!(current.load_stage(42, first.id).unwrap(), Some(first));
    assert_eq!(current.list_stages(42, None, 64).unwrap().len(), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_cas_and_cleanup_cannot_skip_reclamation_completion() {
    let metadata = metadata::TestMetadata::start().await;
    let store = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry(1))
        .await
        .unwrap();
    let catalog = ChunkRootCatalog::open_callback(store).unwrap();
    let first = record(1);
    catalog.compare_stage(42, 1, None, Some(&first)).unwrap();
    let writing = next(&first, StageState::Writing);
    catalog
        .compare_stage(42, 1, Some(&first), Some(&writing))
        .unwrap();
    let stale_retired = next(&first, StageState::Retired);
    assert_eq!(
        catalog.compare_stage(42, 1, Some(&first), Some(&stale_retired)),
        Err(CtError::Unavailable)
    );
    assert_eq!(
        catalog.compare_stage(42, 1, Some(&writing), None),
        Err(CtError::InvalidArgument)
    );
    let mut last = writing;
    for state in [StageState::Retired, StageState::Deleting, StageState::Reclaimed] {
        let value = next(&last, state);
        catalog.compare_stage(42, 1, Some(&last), Some(&value)).unwrap();
        last = value;
    }
    catalog.compare_stage(42, 1, Some(&last), None).unwrap();
    catalog.compare_stage(42, 1, Some(&last), None).unwrap();
    assert_eq!(catalog.load_stage(42, last.id).unwrap(), None);
    assert!(catalog.list_stages(42, None, 1).unwrap().is_empty());
}
fn key(kind: &[u8], id: u64) -> Vec<u8> {
    let mut key = b"\0crowdb/chunk-kv/root/v1/".to_vec();
    key.extend_from_slice(&42_u64.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(kind);
    key.push(b'/');
    key.extend_from_slice(&id.to_be_bytes());
    key
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pin_invalidates_a_reclamation_proof_at_the_shared_authority_key() {
    let metadata = metadata::TestMetadata::start().await;
    let store = open_root_catalog_for_tests(metadata.client.clone(), 0, 1, &entry(1))
        .await
        .unwrap();
    for generation in 1..=4 {
        store
            .publish(42, generation - 1, 1, generation, b"manifest")
            .unwrap();
    }
    let authority = key(b"authority", 0);
    let GetOutcome::Found { value, revision } = metadata
        .client
        .get(0, 1, &authority, ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("authority must exist");
    };
    store.pin_generation(42, 1, 1, 1).unwrap();
    let ops = [
        BatchOp::Put {
            key: Bytes::from(authority.clone()),
            value,
        },
        BatchOp::Delete {
            key: Bytes::from(key(b"manifest", 1)),
        },
    ];
    assert!(matches!(
        metadata
            .client
            .batch_write_cas(0, 1, &ops, &authority, revision)
            .await,
        Err(crowdb_paxos_kv_client::Error::CasFailed { .. } | crowdb_paxos_kv_client::Error::CasBusy)
    ));
    assert_eq!(store.reclaim_before(42, 4), 0);
    store.unpin_generation(42, 1, 1).unwrap();
    assert_eq!(store.reclaim_before(42, u64::MAX), 2);
    assert_eq!(store.load(42, RootCatalogObject::Manifest(1)).unwrap(), None);
    assert_eq!(store.pin_generation(42, 2, 2, 1), Err(CtError::NotFound));
    assert!(store.load(42, RootCatalogObject::Manifest(3)).unwrap().is_some());
}
