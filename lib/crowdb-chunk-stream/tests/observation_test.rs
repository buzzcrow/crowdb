// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamBinding, StreamBindingState, StreamConfig, StreamName,
};

#[tokio::test]
async fn published_extent_index_is_bounded_fenced_and_reads_no_data() {
    let store = Arc::new(MemoryStreamStore::new(4096));
    let stream = ChunkStream::create(
        StreamBinding {
            stream_name: StreamName { high: 1, low: 2 },
            metadata_group_id: 7,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: None,
        },
        u64::MAX,
        StreamConfig {
            extent_page_entries: 1,
            ..StreamConfig::default()
        },
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    for _ in 0..105 {
        stream.append(&[Bytes::from_static(b"x")]).await.unwrap();
    }
    let before = stream.metrics();
    let publications = store.metadata_publish_count();
    let first = stream.observe_metadata(None, 0).unwrap();
    assert_eq!(first.writer_epoch, u64::MAX);
    assert_eq!(first.extent_pages.len(), 100);
    assert_eq!(first.next_offset, Some(100));
    let second = stream.observe_metadata(Some(first.generation), 100).unwrap();
    assert_eq!(second.extent_pages.len(), 5);
    assert_eq!(second.extent_pages[0].first_logical, 100);
    assert_eq!(second.next_offset, None);
    assert!(stream.observe_metadata(None, 100).is_err());
    assert!(stream
        .observe_metadata(Some(first.generation), usize::MAX)
        .is_err());
    assert_eq!(stream.metrics(), before);
    assert_eq!(store.metadata_publish_count(), publications);
    stream.append(&[Bytes::from_static(b"y")]).await.unwrap();
    assert!(stream.observe_metadata(Some(first.generation), 100).is_err());
    let current = stream.observe_metadata(None, 0).unwrap();
    assert!(current.generation > first.generation);
    assert!(current.active.is_some());
    stream.close().await.unwrap();
    let closed = stream.observe_metadata(None, 0).unwrap();
    assert!(closed.closed);
    assert!(closed.active.is_none());
    assert_eq!(closed.sealed_tail, 106);
}
