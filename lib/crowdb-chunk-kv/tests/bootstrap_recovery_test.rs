// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunk_kv::{
    MutationOperation, Partition, PartitionConfig, PartitionId, PartitionRange, RequestId,
};
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    ChunkStream, StreamBinding, StreamBindingState, StreamChunkStore, StreamConfig, StreamMetadataStore,
    StreamName, StreamPurpose, StreamRegistry,
};
use crowdb_tree_ffi::{ChunkPageStoreOptions, ChunkRootCatalog, PageStore};

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
        let catalog = Arc::new(ChunkRootCatalog::open_memory(4).unwrap());
        let options = ChunkPageStoreOptions {
            tree_id: identity.low,
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
            crowdb_tree_ffi::Config::default(),
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
        drop(partition);
        drop(pages);
        let stream = open_stream(name, &store).await;
        let pages = Arc::new(PageStore::open_chunk(options, catalog, None).unwrap());
        let recovered = Partition::recover_native_latest_prepared_assignment(
            identity,
            range,
            4,
            identity.low,
            PartitionConfig::default(),
            crowdb_tree_ffi::Config::default(),
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
        StreamBinding {
            purpose: StreamPurpose::Wal,
            stream_name: name,
            metadata_group_id: 7,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("chunk-kv-partition".into()),
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
