// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv::{
    memory::MemoryPartitionTree, MutationOperation, Partition, PartitionConfig, PartitionId, PartitionRange,
    RequestId, StreamPartitionJournal,
};
use crowdb_chunk_kv_server::ChunkKvService;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamBinding, StreamBindingState, StreamConfig, StreamName,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

async fn partition(id: u64, count: u64) -> Partition {
    let store = Arc::new(MemoryStreamStore::new(4096));
    let name = StreamName { high: 1, low: id };
    let stream = ChunkStream::create(
        StreamBinding {
            purpose: crowdb_protocol::chunk_stream::StreamPurpose::Wal,
            stream_name: name,
            metadata_group_id: 1,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("chunk-kv-partition".into()),
        },
        1,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store,
    )
    .await
    .unwrap();
    let partition = Partition::open(
        PartitionId { high: 1, low: id },
        PartitionRange::default(),
        1,
        PartitionConfig::default(),
        Arc::new(MemoryPartitionTree::with_tree_id(id)),
        Arc::new(StreamPartitionJournal::new(stream, name).unwrap()),
    )
    .unwrap();
    for index in 0..count {
        partition
            .mutate(
                1,
                RequestId {
                    client_high: 1,
                    client_low: id,
                    client_sequence: index + 1,
                },
                MutationOperation::Put {
                    key: format!("key-{index:05}").into_bytes(),
                    value: vec![1; 16],
                },
            )
            .await
            .unwrap();
    }
    partition
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observation_bounds_samples_and_does_not_starve_other_partitions() {
    let service = ChunkKvService::new(1, 8).unwrap();
    // One unsplittable partition must not stop observation of a larger valid range.
    let one = partition(1, 1).await;
    let many = partition(2, 1000).await;
    service.install_partition(&one).unwrap();
    service.install_partition(&many).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let started = Instant::now();
            let pending = service.registry_observation_with_load_samples(1024, 0, 256);
            assert!(started.elapsed() < Duration::from_millis(100));
            let observation = tokio::time::timeout(Duration::from_millis(100), pending)
                .await
                .unwrap()
                .unwrap();
            let samples: Vec<_> = observation
                .partition_loads
                .iter()
                .map(|load| load.live_byte_samples.len())
                .collect();
            assert!(samples.iter().all(|count| *count <= 64));
            if samples.contains(&1) && samples.contains(&64) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
