// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv::{
    memory::MemoryPartitionTree, ChunkKvError, MutationOperation, Partition, PartitionConfig, PartitionId,
    PartitionRange, RequestId, StreamPartitionJournal,
};
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamConfig, StreamName, StreamPurpose, StreamRecord,
    StreamRecordState,
};
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn captured_frontier_publishes_one_exact_prefix_before_later_mutations() {
    let store = Arc::new(MemoryStreamStore::new(1024 * 1024));
    let name = StreamName::generate();
    let stream = ChunkStream::create(
        StreamRecord {
            purpose: StreamPurpose::Wal,
            stream_name: name,
            metadata_group_id: 1,
            record_version: 1,
            state: StreamRecordState::Active,
            consumer_kind: Some("frontier-test".into()),
            consumer_id: None,
            created_at_ms: StreamRecord::creation_time_ms(),
        },
        1,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    let partition = Partition::open(
        PartitionId { high: 1, low: 1 },
        PartitionRange::default(),
        1,
        PartitionConfig::default(),
        Arc::new(MemoryPartitionTree::default()),
        Arc::new(StreamPartitionJournal::new(stream, name).unwrap()),
    )
    .unwrap();
    mutate(&partition, 1).await;
    let first_end = partition.snapshot().journal_durable_offset;
    assert_eq!(
        partition.capture_journal_frontier(2).await,
        Err(ChunkKvError::StaleEpoch)
    );
    store.pause_publication_for_tests();
    let capturing = partition.capture_journal_frontier(1);
    tokio::pin!(capturing);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), capturing.as_mut())
            .await
            .is_err()
    );
    store.wait_for_publication_for_tests().await;
    let later = mutate(&partition, 2);
    tokio::pin!(later);
    assert!(tokio::time::timeout(Duration::from_millis(10), later.as_mut())
        .await
        .is_err());
    store.resume_publication_for_tests();
    let frontier = capturing.await.unwrap();
    later.await;
    assert_eq!(frontier.mutation_seq, 1);
    assert_eq!(frontier.offset, first_end);
    assert_eq!(partition.snapshot().journal_durable_seq, 2);
    assert!(partition.snapshot().journal_durable_offset > frontier.offset);
    let reader = ChunkStream::open_read_only(
        name,
        1,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store,
    )
    .await
    .unwrap();
    assert_eq!(reader.tail(), frontier.offset);
    assert_eq!(reader.record_generation(), frontier.stream_generation);
    let bytes = reader
        .read_at(0, usize::try_from(frontier.offset).unwrap())
        .await
        .unwrap();
    let crowdb_chunk_kv::FrameDecode::Complete(frame) = crowdb_chunk_kv::decode_frame(&bytes).unwrap() else {
        panic!("captured frontier must retain a complete WAL record");
    };
    assert_eq!(frame.record.mutation_seq, frontier.mutation_seq);
    assert_eq!(frame.bytes_consumed, bytes.len());
}

async fn mutate(partition: &Partition, sequence: u64) {
    partition
        .mutate(
            1,
            RequestId {
                client_high: 1,
                client_low: 1,
                client_sequence: sequence,
            },
            MutationOperation::Put {
                key: sequence.to_be_bytes().to_vec(),
                value: vec![7; 16],
            },
        )
        .await
        .unwrap();
}
