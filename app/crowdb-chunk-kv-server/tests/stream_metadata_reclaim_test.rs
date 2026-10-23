// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, KvStreamMetadataStore, StreamBinding, StreamBindingState,
    StreamConfig, StreamMetadataStore, StreamName, StreamPurpose,
};
use crowdb_kv_client::{CrowdbKvClient, ReadMode};
use crowdb_protocol::key::{BinaryKey, StreamExtentPageKey};

#[path = "common/metadata.rs"]
mod metadata;

async fn pages(kv: &CrowdbKvClient, name: StreamName) -> Vec<StreamExtentPageKey> {
    kv.scan(
        0,
        1,
        &StreamExtentPageKey::stream_prefix(name),
        &[],
        &[],
        0,
        ReadMode::Linearizable,
        None,
        true,
        None,
    )
    .await
    .unwrap()
    .items
    .into_iter()
    .map(|(key, _)| StreamExtentPageKey::from_bytes(&key).unwrap())
    .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_page_reclaim_batches_only_keys_below_the_retained_generation() {
    let fixture = metadata::TestMetadata::start().await;
    let chunks = Arc::new(MemoryStreamStore::new(512));
    let metadata = Arc::new(KvStreamMetadataStore::new(fixture.client.clone(), 0, 1).unwrap());
    let name = StreamName { high: 81, low: 82 };
    let stream = ChunkStream::create(
        StreamBinding {
            purpose: StreamPurpose::Wal,
            stream_name: name,
            metadata_group_id: 1,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: None,
        },
        7,
        StreamConfig {
            extent_page_entries: 4,
            ..StreamConfig::default()
        },
        chunks.clone(),
        metadata.clone(),
        chunks,
    )
    .await
    .unwrap();
    for byte in 0..16 {
        stream.append(&[Bytes::from(vec![byte])]).await.unwrap();
    }
    let retained = metadata.load_current(name).await.unwrap().unwrap();
    for byte in 16..24 {
        stream.append(&[Bytes::from(vec![byte])]).await.unwrap();
    }
    let before = pages(&fixture.client, name).await.len();
    assert_eq!(
        stream
            .reclaim_metadata_before(retained.generation, 3)
            .await
            .unwrap(),
        3
    );
    assert_eq!(pages(&fixture.client, name).await.len(), before - 3);
    for _ in 0..64 {
        if stream
            .reclaim_metadata_before(retained.generation, 3)
            .await
            .unwrap()
            == 0
        {
            break;
        }
    }
    assert!(pages(&fixture.client, name)
        .await
        .iter()
        .all(|key| key.generation >= retained.generation));
    for fence in retained.extent_pages {
        assert!(metadata
            .load_extent_page(name, retained.writer_epoch, retained.generation, fence.page_index)
            .await
            .unwrap()
            .is_some());
    }
    assert_eq!(
        stream.read_at(0, 24).await.unwrap().as_ref(),
        &(0..24).collect::<Vec<u8>>()
    );
}
