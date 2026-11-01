// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/root_catalog.rs"]
mod root_catalog;

use crowdb_chunk_kv::{CrowdbPartitionTree, MutationOperation, PartitionRange, PartitionTree};
use crowdb_chunk_kv::{Partition, PartitionConfig, PartitionId};
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamConfig, StreamName, StreamPurpose, StreamRecord,
    StreamRecordState,
};
use crowdb_tree_ffi::{ChunkPageStoreOptions, ChunkRootCatalog, Config, PageStore};
use root_catalog::TestRootCatalog;
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn config(tree_id: u64, catalog: &Arc<ChunkRootCatalog>) -> Config {
    Config {
        page_store: Some(Arc::new(
            PageStore::open_chunk(
                ChunkPageStoreOptions {
                    tree_id,
                    owner_epoch: 1,
                    open_generation: 0,
                    pack_bytes: 4096,
                    iu_size: 1,
                    max_concurrent_packs: 2,
                    materialization_bytes_per_pass: 4096,
                    max_chunk_bytes: 1024 * 1024,
                    mirror_copies: 0,
                },
                catalog.clone(),
                None,
            )
            .unwrap(),
        )),
        ..Config::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn range_rebuild_keeps_the_runtime_available_during_catalog_io() {
    let backend = Arc::new(TestRootCatalog::default());
    let catalog = Arc::new(ChunkRootCatalog::open_callback(backend.clone()).unwrap());
    let source = CrowdbPartitionTree::open(91, &config(91, &catalog)).unwrap();
    source
        .apply(
            1,
            &MutationOperation::Put {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            },
        )
        .await
        .unwrap();
    source.checkpoint(0).await.unwrap();
    let child_config = config(92, &catalog);
    let progress = backend.clone();
    let heartbeat = tokio::spawn(async move {
        progress.entered.notified().await;
        progress.async_progress.store(true, Ordering::Release);
    });
    backend.armed.store(true, Ordering::Release);
    let (child, _) = tokio::spawn(async move {
        source
            .rebuild_range(92, &PartitionRange::default(), child_config)
            .await
            .unwrap()
    })
    .await
    .unwrap();
    heartbeat.await.unwrap();
    assert!(
        backend.observed_progress.load(Ordering::Acquire),
        "catalog IO blocked the only async worker"
    );
    assert_eq!(child.get(b"key").await.unwrap().unwrap().value, b"value");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn native_recovery_keeps_the_runtime_available_during_catalog_io() {
    let backend = Arc::new(TestRootCatalog::default());
    let catalog = Arc::new(ChunkRootCatalog::open_callback(backend.clone()).unwrap());
    let config = config(93, &catalog);
    let pages = config.page_store.clone().unwrap();
    let store = Arc::new(MemoryStreamStore::new(4096));
    let stream = ChunkStream::create(
        StreamRecord {
            purpose: StreamPurpose::Wal,
            stream_name: StreamName { high: 1, low: 93 },
            metadata_group_id: 7,
            record_version: 1,
            state: StreamRecordState::Active,
            consumer_kind: Some("chunk-kv-partition".into()),
            consumer_id: None,
            created_at_ms: StreamRecord::creation_time_ms(),
        },
        1,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store,
    )
    .await
    .unwrap();
    let progress = backend.clone();
    let heartbeat = tokio::spawn(async move {
        progress.entered.notified().await;
        progress.async_progress.store(true, Ordering::Release);
    });
    backend.armed.store(true, Ordering::Release);
    let recovered = tokio::spawn(async move {
        Partition::recover_native_latest_prepared_assignment(
            PartitionId { high: 1, low: 93 },
            PartitionRange::default(),
            1,
            93,
            PartitionConfig::default(),
            config,
            pages,
            stream,
        )
        .await
        .unwrap()
    })
    .await
    .unwrap();
    heartbeat.await.unwrap();
    assert!(
        backend.observed_progress.load(Ordering::Acquire),
        "native recovery blocked the only async worker"
    );
    assert_eq!(recovered.snapshot().applied_seq, 0);
}
