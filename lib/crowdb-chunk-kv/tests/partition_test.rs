// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunk_kv::memory::MemoryPartitionTree;
use crowdb_chunk_kv::{
    ChunkKvError, CompareCondition, MutationOperation, MutationResult, Partition, PartitionConfig,
    PartitionId, PartitionJournal, PartitionRange, PartitionTree, RequestId, StreamPartitionJournal,
};
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    ChunkStream, CursorAdvance, StreamBinding, StreamBindingState, StreamChunkStore, StreamConfig,
    StreamMetadataStore, StreamName, StreamRegistry,
};

fn request(sequence: u64) -> RequestId {
    RequestId {
        client_high: 11,
        client_low: 12,
        client_sequence: sequence,
    }
}

async fn partition(
    store: &Arc<MemoryStreamStore>,
    tree: Arc<MemoryPartitionTree>,
    stream_name: StreamName,
    epoch: u64,
    config: PartitionConfig,
) -> Partition {
    let binding = StreamBinding {
        stream_name,
        metadata_group_id: 7,
        binding_generation: 1,
        state: StreamBindingState::Active,
        owner_kind: Some("chunk-kv-partition".into()),
    };
    let registry: Arc<dyn StreamRegistry> = store.clone();
    let metadata: Arc<dyn StreamMetadataStore> = store.clone();
    let chunks: Arc<dyn StreamChunkStore> = store.clone();
    let stream = ChunkStream::create(
        binding,
        epoch,
        StreamConfig::default(),
        registry,
        metadata,
        chunks,
    )
    .await
    .unwrap();
    let journal: Arc<dyn PartitionJournal> = Arc::new(StreamPartitionJournal::new(stream, stream_name));
    let tree: Arc<dyn PartitionTree> = tree;
    Partition::open(
        PartitionId {
            high: stream_name.high,
            low: stream_name.low,
        },
        PartitionRange {
            start: Some(b"a".to_vec()),
            end: Some(b"m".to_vec()),
        },
        epoch,
        config,
        tree,
        journal,
    )
    .unwrap()
}

#[tokio::test]
async fn pending_mutation_is_invisible_until_durable_and_applied() {
    let store = Arc::new(MemoryStreamStore::new(1_024));
    let tree = Arc::new(MemoryPartitionTree::default());
    let partition = partition(
        &store,
        tree,
        StreamName { high: 1, low: 1 },
        4,
        PartitionConfig::default(),
    )
    .await;
    partition
        .mutate(
            4,
            request(1),
            MutationOperation::Put {
                key: b"key".to_vec(),
                value: b"old".to_vec(),
            },
        )
        .await
        .unwrap();

    store.pause_writes();
    let writer = partition.clone();
    let pending = tokio::spawn(async move {
        writer
            .mutate(
                4,
                request(2),
                MutationOperation::Put {
                    key: b"key".to_vec(),
                    value: b"new".to_vec(),
                },
            )
            .await
    });
    store.wait_for_write().await;
    assert_eq!(
        partition.get(4, b"key", None).await.unwrap().unwrap().value,
        b"old"
    );
    store.resume_writes();
    let response = pending.await.unwrap().unwrap();
    assert_eq!(
        partition
            .get(4, b"key", Some(response.journal_position))
            .await
            .unwrap()
            .unwrap()
            .value,
        b"new"
    );
}

#[tokio::test]
async fn same_batch_conditions_observe_preceding_staged_mutations() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 2, low: 2 },
        5,
        PartitionConfig::default(),
    )
    .await;
    store.pause_writes();
    let blocker_partition = partition.clone();
    let blocker = tokio::spawn(async move {
        blocker_partition
            .mutate(
                5,
                request(10),
                MutationOperation::Put {
                    key: b"b".to_vec(),
                    value: b"block".to_vec(),
                },
            )
            .await
    });
    store.wait_for_write().await;

    let first_partition = partition.clone();
    let first = tokio::spawn(async move {
        first_partition
            .mutate(
                5,
                request(11),
                MutationOperation::PutIfAbsent {
                    key: b"k".to_vec(),
                    value: b"one".to_vec(),
                },
            )
            .await
    });
    tokio::task::yield_now().await;
    let second_partition = partition.clone();
    let second = tokio::spawn(async move {
        second_partition
            .mutate(
                5,
                request(12),
                MutationOperation::PutIfAbsent {
                    key: b"k".to_vec(),
                    value: b"two".to_vec(),
                },
            )
            .await
    });
    tokio::task::yield_now().await;
    store.resume_writes();
    blocker.await.unwrap().unwrap();
    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert!(matches!(first.result, MutationResult::Applied { .. }));
    assert!(matches!(
        second.result,
        MutationResult::ConditionFailed {
            observed: Some(ref value)
        } if value.value == b"one" && value.revision == first.mutation_seq
    ));
    assert_eq!(second.mutation_seq, first.mutation_seq + 1);
    assert_eq!(partition.get(5, b"k", None).await.unwrap().unwrap().value, b"one");
}

#[tokio::test]
async fn retry_returns_original_result_and_digest_conflict_does_no_io() {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 3, low: 3 },
        6,
        PartitionConfig::default(),
    )
    .await;
    let operation = MutationOperation::CompareExchange {
        key: b"k".to_vec(),
        condition: CompareCondition::Revision(99),
        value: b"never".to_vec(),
    };
    let original = partition.mutate(6, request(20), operation.clone()).await.unwrap();
    assert!(matches!(
        original.result,
        MutationResult::ConditionFailed { observed: None }
    ));
    partition
        .mutate(
            6,
            request(21),
            MutationOperation::Put {
                key: b"k".to_vec(),
                value: b"later".to_vec(),
            },
        )
        .await
        .unwrap();
    let writes = store.chunk_write_count();
    assert_eq!(
        partition.mutate(6, request(20), operation).await.unwrap(),
        original
    );
    assert_eq!(store.chunk_write_count(), writes);
    assert_eq!(
        partition
            .mutate(6, request(20), MutationOperation::Delete { key: b"k".to_vec() })
            .await,
        Err(ChunkKvError::RequestConflict)
    );
    assert_eq!(store.chunk_write_count(), writes);
}

#[tokio::test]
async fn range_and_epoch_reject_before_journaling() {
    let store = Arc::new(MemoryStreamStore::new(1_024));
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 4, low: 4 },
        7,
        PartitionConfig::default(),
    )
    .await;
    let writes = store.chunk_write_count();
    assert_eq!(
        partition
            .mutate(
                6,
                request(30),
                MutationOperation::Put {
                    key: b"b".to_vec(),
                    value: b"v".to_vec()
                }
            )
            .await,
        Err(ChunkKvError::StaleEpoch)
    );
    assert_eq!(
        partition
            .mutate(
                7,
                request(31),
                MutationOperation::Put {
                    key: b"m".to_vec(),
                    value: b"v".to_vec()
                }
            )
            .await,
        Err(ChunkKvError::OutOfRange)
    );
    assert_eq!(store.chunk_write_count(), writes);
}

#[tokio::test]
async fn journal_uncertainty_stalls_only_writes_and_keeps_applied_reads() {
    let store = Arc::new(MemoryStreamStore::new(1_024));
    let partition = partition(
        &store,
        Arc::new(MemoryPartitionTree::default()),
        StreamName { high: 5, low: 5 },
        8,
        PartitionConfig::default(),
    )
    .await;
    partition
        .mutate(
            8,
            request(40),
            MutationOperation::Put {
                key: b"k".to_vec(),
                value: b"safe".to_vec(),
            },
        )
        .await
        .unwrap();
    store.queue_cursor_outcome(CursorAdvance::Ambiguous, false).await;
    assert_eq!(
        partition
            .mutate(
                8,
                request(41),
                MutationOperation::Put {
                    key: b"k".to_vec(),
                    value: b"unsafe".to_vec(),
                },
            )
            .await,
        Err(ChunkKvError::WriteStalled)
    );
    assert_eq!(
        partition.snapshot().lifecycle,
        crowdb_chunk_kv::PartitionLifecycle::WriteStalled
    );
    assert_eq!(
        partition.get(8, b"k", None).await.unwrap().unwrap().value,
        b"safe"
    );
}
