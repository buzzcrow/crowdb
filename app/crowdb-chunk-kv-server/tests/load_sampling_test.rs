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
    partition_with_value_size(id, count, 16).await
}

async fn partition_with_value_size(id: u64, count: u64, value_size: usize) -> Partition {
    let store = Arc::new(MemoryStreamStore::new(
        u64::try_from(value_size.saturating_mul(4).max(4096)).unwrap(),
    ));
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
                    value: vec![1; value_size],
                },
            )
            .await
            .unwrap();
    }
    partition
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_values_still_supply_two_live_split_witnesses() {
    let service = ChunkKvService::new(1, 8).unwrap();
    let partition = partition_with_value_size(3, 4, 64 * 1024).await;
    service.install_partition(&partition).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let observation = service
                .registry_observation_with_load_samples(1024 * 1024, 0, 2)
                .await
                .unwrap();
            let samples = &observation.partition_loads[0].live_byte_samples;
            if samples.len() == 2 {
                assert!(samples[0].0 < samples[1].0);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
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
