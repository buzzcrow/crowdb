// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv::{
    MutationOperation, Partition, PartitionConfig, PartitionId, PartitionRange, RequestId,
};
use crowdb_chunk_kv_server::{BootstrapPartitionConfig, ChunkKvStorage};
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    ChunkStream, StreamConfig, StreamName, StreamPurpose, StreamRecord, StreamRecordState,
};
use crowdb_protocol::chunk_kv::Id128;
use crowdb_tree_ffi::{ChunkPageStoreOptions, ChunkRootCatalog, PageStore};
use std::sync::Arc;

#[tokio::test]
async fn bootstrap_resume_finishes_its_first_checkpoint_and_does_not_reinitialize_it() {
    let config = BootstrapPartitionConfig {
        partition_id: Id128 { high: 1, low: 11 },
        tree_id: 45,
        stream_name: StreamName { high: 2, low: 11 },
        owner_epoch: 4,
        metadata_group_id: 7,
    };
    let name = StreamName { high: 2, low: 11 };
    let store = Arc::new(MemoryStreamStore::new(4096));
    let stream = ChunkStream::create(
        StreamRecord {
            purpose: StreamPurpose::Wal,
            stream_name: name,
            metadata_group_id: 7,
            record_version: 1,
            state: StreamRecordState::Active,
            consumer_kind: Some("chunk-kv-partition".into()),
            consumer_id: None,
            created_at_ms: StreamRecord::creation_time_ms(),
        },
        4,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    let catalog = Arc::new(ChunkRootCatalog::open_memory(4).unwrap());
    let options = ChunkPageStoreOptions {
        tree_id: 45,
        owner_epoch: 4,
        open_generation: 0,
        pack_bytes: 4096,
        iu_size: 1,
        max_concurrent_packs: 2,
        max_chunk_bytes: 0,
        mirror_copies: 0,
        materialization_bytes_per_pass: 4096,
    };
    let pages = Arc::new(PageStore::open_chunk(options, catalog.clone(), None).unwrap());
    let partition = Partition::open_native(
        PartitionId { high: 1, low: 11 },
        PartitionRange {
            start: Some(Vec::new()),
            end: None,
        },
        4,
        PartitionConfig::default(),
        45,
        crowdb_tree_ffi::Config::default(),
        pages,
        stream,
    )
    .unwrap();
    partition
        .mutate(
            4,
            RequestId {
                client_high: 1,
                client_low: 11,
                client_sequence: 1,
            },
            MutationOperation::Put {
                key: Vec::new(),
                value: b"crowdb-bootstrap".to_vec(),
            },
        )
        .await
        .unwrap();
    partition.suspend_for_transfer(4).await.unwrap();
    drop(partition);
    let stream = reopen(name, &store).await;
    let pages = Arc::new(PageStore::open_chunk(options, catalog.clone(), None).unwrap());
    let recovered = ChunkKvStorage::recover_bootstrap_partition_for_tests(&config, pages.clone(), stream)
        .await
        .unwrap();
    assert!(recovered.get(4, b"", None).await.unwrap().is_none());
    let generation = pages.chunk_manifest_generation().unwrap();
    assert!(generation > 0);
    recovered.suspend_for_transfer(4).await.unwrap();
    drop(recovered);
    let stream = reopen(name, &store).await;
    let pages = Arc::new(PageStore::open_chunk(options, catalog, None).unwrap());
    let recovered = ChunkKvStorage::recover_bootstrap_partition_for_tests(&config, pages.clone(), stream)
        .await
        .unwrap();
    assert_eq!(pages.chunk_manifest_generation().unwrap(), generation);
    recovered.activate_recovered(4).unwrap();
    assert!(recovered.get(4, b"", None).await.unwrap().is_none());
}

async fn reopen(name: StreamName, store: &Arc<MemoryStreamStore>) -> ChunkStream {
    ChunkStream::open(
        name,
        4,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap()
}
