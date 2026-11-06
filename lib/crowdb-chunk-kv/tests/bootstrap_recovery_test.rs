// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunk_kv::{
    MutationOperation, Partition, PartitionConfig, PartitionId, PartitionRange, RequestId,
};
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    ChunkStream, StreamChunkStore, StreamConfig, StreamMetadataStore, StreamName, StreamPurpose,
    StreamRecord, StreamRecordState, StreamRegistry,
};
use crowdb_tree_ffi::chunk::test_util::TestChunkBackend;

#[tokio::test]
async fn interrupted_bootstrap_replays_wal_before_the_first_tree_checkpoint() {
    for durable_mutations in 0..=2 {
        let identity = PartitionId {
            high: 1,
            low: 12 + durable_mutations,
        };
        let name = StreamName {
            high: identity.high,
            low: identity.low,
        };
        let store = Arc::new(MemoryStreamStore::new(4096));
        let stream = create_stream(name, &store).await;
        let mut backend = TestChunkBackend::new(identity.low, 4).unwrap();
        let pages = backend.page_store(0).unwrap();
        let range = PartitionRange {
            start: Some(Vec::new()),
            end: None,
        };
        let partition = Partition::open_native(
            identity,
            range.clone(),
            4,
            PartitionConfig::default(),
            identity.low,
            backend.config(),
            pages.clone(),
            stream,
        )
        .unwrap();
        for sequence in 1..=durable_mutations {
            partition
                .mutate(4, request(identity, sequence), operation(sequence))
                .await
                .unwrap();
        }
        partition.suspend_for_transfer(4).await.unwrap();
        assert!(matches!(
            pages.chunk_manifest_generation(),
            Ok(0) | Err(crowdb_tree_ffi::CtError::NotFound)
        ));
        partition.close().await.unwrap();
        drop(pages);
        let stream = open_stream(name, &store).await;
        backend.next_writer().unwrap();
        let pages = backend.page_store(0).unwrap();
        let recovered = Partition::recover_native_latest_prepared_assignment(
            identity,
            range,
            4,
            identity.low,
            PartitionConfig::default(),
            backend.config(),
            pages.clone(),
            stream,
        )
        .await
        .unwrap();
        recovered.activate_recovered(4).unwrap();
        let value = recovered
            .get(4, b"", None)
            .await
            .unwrap()
            .map(|entry| entry.value);
        assert_eq!(
            value.as_deref(),
            (durable_mutations == 1).then_some(b"bootstrap".as_slice())
        );
        for sequence in 1..=2 {
            recovered
                .mutate(4, request(identity, sequence), operation(sequence))
                .await
                .unwrap();
        }
        assert!(recovered.get(4, b"", None).await.unwrap().is_none());
        recovered.checkpoint(4).await.unwrap();
        assert!(pages.chunk_manifest_generation().unwrap() > 0);
        recovered.suspend_for_shutdown(4).await.unwrap();
        recovered.close().await.unwrap();
    }
}

fn request(id: PartitionId, client_sequence: u64) -> RequestId {
    RequestId {
        client_high: id.high,
        client_low: id.low,
        client_sequence,
    }
}

fn operation(sequence: u64) -> MutationOperation {
    if sequence == 1 {
        MutationOperation::Put {
            key: Vec::new(),
            value: b"bootstrap".to_vec(),
        }
    } else {
        MutationOperation::Delete { key: Vec::new() }
    }
}

async fn create_stream(name: StreamName, store: &Arc<MemoryStreamStore>) -> ChunkStream {
    ChunkStream::create(
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
        store.clone() as Arc<dyn StreamRegistry>,
        store.clone() as Arc<dyn StreamMetadataStore>,
        store.clone() as Arc<dyn StreamChunkStore>,
    )
    .await
    .unwrap()
}

async fn open_stream(name: StreamName, store: &Arc<MemoryStreamStore>) -> ChunkStream {
    ChunkStream::open(
        name,
        4,
        StreamConfig::default(),
        store.clone() as Arc<dyn StreamRegistry>,
        store.clone() as Arc<dyn StreamMetadataStore>,
        store.clone() as Arc<dyn StreamChunkStore>,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn close_waits_for_the_last_partition_owner_and_releases_the_worker_tree() {
    let store = Arc::new(MemoryStreamStore::new(4096));
    let name = StreamName { high: 3, low: 4 };
    let stream = create_stream(name, &store).await;
    let tree = Arc::new(crowdb_chunk_kv::memory::MemoryPartitionTree::with_tree_id(7));
    let weak_tree = Arc::downgrade(&tree);
    let partition = Partition::open(
        PartitionId { high: 3, low: 4 },
        PartitionRange::default(),
        4,
        PartitionConfig::default(),
        tree,
        Arc::new(crowdb_chunk_kv::StreamPartitionJournal::new(stream, name).unwrap()),
    )
    .unwrap();
    partition.suspend_for_shutdown(4).await.unwrap();
    let other_owner = partition.clone();
    let completion = partition.close();
    futures::pin_mut!(completion);
    assert!(futures::poll!(&mut completion).is_pending());
    assert!(weak_tree.upgrade().is_some());
    drop(other_owner);
    completion.await.unwrap();
    assert!(weak_tree.upgrade().is_none());
}
