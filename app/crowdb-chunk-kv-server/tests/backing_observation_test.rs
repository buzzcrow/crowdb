// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/process.rs"]
mod test_process;

use std::sync::Arc;

use crowdb_chunk_kv::{
    CrowdbPartitionTree, MutationOperation, Partition, PartitionConfig, PartitionId, PartitionRange,
    PartitionTree, StreamPartitionJournal,
};
use crowdb_chunk_kv_server::ChunkKvService;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamConfig, StreamName, StreamPurpose, StreamRecord,
    StreamRecordState,
};
use crowdb_tree_ffi::{chunk::test_util::TestChunkBackend, Config};

async fn partition(id: u64, tree: Arc<dyn PartitionTree>) -> Partition {
    let store = Arc::new(MemoryStreamStore::new(4096));
    let name = StreamName { high: 1, low: id };
    let stream = ChunkStream::create(
        StreamRecord {
            purpose: StreamPurpose::Wal,
            stream_name: name,
            metadata_group_id: 1,
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
    Partition::open(
        PartitionId { high: 1, low: id },
        PartitionRange::default(),
        1,
        PartitionConfig::default(),
        tree,
        Arc::new(StreamPartitionJournal::new(stream, name).unwrap()),
    )
    .unwrap()
}

#[tokio::test]
async fn registry_counts_one_physical_ledger_once_across_partition_views() {
    let backend = TestChunkBackend::new(82, 1).unwrap();
    let tree = Arc::new(
        CrowdbPartitionTree::open(
            82,
            &Config {
                page_store: Some(backend.page_store(0).unwrap()),
                ..backend.config()
            },
        )
        .unwrap(),
    );
    tree.apply(
        1,
        &MutationOperation::Put {
            key: b"key".to_vec(),
            value: vec![1; 8192],
        },
    )
    .await
    .unwrap();
    tree.checkpoint(0).await.unwrap();
    let expected = tree.backing_stats().unwrap().unwrap();
    assert!(expected.allocated_bytes >= 8192);
    assert_eq!(tree.estimated_bytes().unwrap(), expected.allocated_bytes);
    assert!(tree.chunk_stats().is_err());
    let first = partition(1, tree.clone()).await;
    let second = partition(2, tree.clone()).await;
    assert_eq!(
        first.backing_storage_stats().unwrap(),
        second.backing_storage_stats().unwrap()
    );
    let service = ChunkKvService::new(test_process::TestProcess::authority(1), 8).unwrap();
    service.install_partition(&first).unwrap();
    service.install_partition(&second).unwrap();
    assert_eq!(
        service.registry_observation(1 << 20, 0).durable_bytes,
        expected.allocated_bytes
    );
    let observed = service
        .registry_observation_with_load_samples(1 << 20, 0, 0)
        .await
        .unwrap();
    assert_eq!(observed.partition_loads.len(), 2);
    assert!(observed
        .partition_loads
        .iter()
        .all(|load| load.durable_bytes == expected.allocated_bytes));
    first.suspend_for_shutdown(1).await.unwrap();
    second.suspend_for_shutdown(1).await.unwrap();
    drop(service);
    first.close().await.unwrap();
    second.close().await.unwrap();
}
