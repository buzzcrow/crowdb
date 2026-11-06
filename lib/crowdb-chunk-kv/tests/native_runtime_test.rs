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
use crowdb_tree_ffi::chunk::test_util::TestChunkBackend;
use crowdb_tree_ffi::Config;
use root_catalog::TestCatalogLoad;
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn config(backend: &TestChunkBackend) -> Config {
    Config {
        page_store: Some(backend.page_store(0).unwrap()),
        ..backend.config()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn range_rebuild_keeps_the_runtime_available_during_catalog_io() {
    let observed = Arc::new(TestCatalogLoad::default());
    let source_backend = TestChunkBackend::new(91, 1).unwrap();
    let source = CrowdbPartitionTree::open(91, &config(&source_backend)).unwrap();
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
    let mut child_backend = TestChunkBackend::new(92, 1).unwrap();
    let hook = observed.clone();
    child_backend
        .on_root_load_for_tests(move || hook.wait_for_async_progress())
        .unwrap();
    let child_config = config(&child_backend);
    let progress = observed.clone();
    let heartbeat = tokio::spawn(async move {
        progress.entered.notified().await;
        progress.async_progress.store(true, Ordering::Release);
    });
    observed.armed.store(true, Ordering::Release);
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
        observed.observed_progress.load(Ordering::Acquire),
        "catalog IO blocked the only async worker"
    );
    assert_eq!(child.get(b"key").await.unwrap().unwrap().value, b"value");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn native_recovery_keeps_the_runtime_available_during_catalog_io() {
    let observed = Arc::new(TestCatalogLoad::default());
    let mut backend = TestChunkBackend::new(93, 1).unwrap();
    let hook = observed.clone();
    backend
        .on_root_load_for_tests(move || hook.wait_for_async_progress())
        .unwrap();
    let config = config(&backend);
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
    let progress = observed.clone();
    let heartbeat = tokio::spawn(async move {
        progress.entered.notified().await;
        progress.async_progress.store(true, Ordering::Release);
    });
    observed.armed.store(true, Ordering::Release);
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
        observed.observed_progress.load(Ordering::Acquire),
        "native recovery blocked the only async worker"
    );
    assert_eq!(recovered.snapshot().applied_seq, 0);
    recovered.close().await.unwrap();
}
