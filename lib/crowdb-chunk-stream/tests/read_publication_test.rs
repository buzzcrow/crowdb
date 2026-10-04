// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    ChunkStream, Result, StreamBinding, StreamBindingState, StreamConfig, StreamExtentPage, StreamManifest,
    StreamMetadataStore, StreamName,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::Notify;

struct TestPublicationGate {
    store: Arc<MemoryStreamStore>,
    pause: AtomicBool,
    entered: Notify,
    release: Notify,
}

#[async_trait]
impl StreamMetadataStore for TestPublicationGate {
    async fn load_current(&self, name: StreamName) -> Result<Option<StreamManifest>> {
        self.store.load_current(name).await
    }
    async fn load_extent_page(
        &self,
        name: StreamName,
        epoch: u64,
        generation: u64,
        page: u64,
    ) -> Result<Option<StreamExtentPage>> {
        self.store.load_extent_page(name, epoch, generation, page).await
    }
    async fn publish(
        &self,
        expected: Option<(u64, u64)>,
        manifest: StreamManifest,
        pages: Vec<StreamExtentPage>,
    ) -> Result<()> {
        if self.pause.swap(false, Ordering::AcqRel) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.store.publish(expected, manifest, pages).await
    }
    async fn reclaim_extent_pages_before(
        &self,
        name: StreamName,
        generation: u64,
        max_pages: usize,
    ) -> Result<u64> {
        self.store
            .reclaim_extent_pages_before(name, generation, max_pages)
            .await
    }
}

#[tokio::test]
async fn concurrent_readers_keep_the_published_extent_boundary_until_append_publication() {
    let store = Arc::new(MemoryStreamStore::new(1024));
    let metadata = Arc::new(TestPublicationGate {
        store: store.clone(),
        pause: AtomicBool::new(false),
        entered: Notify::new(),
        release: Notify::new(),
    });
    let name = StreamName { high: 1, low: 1 };
    let writer = ChunkStream::create(
        StreamBinding {
            purpose: crowdb_protocol::chunk_stream::StreamPurpose::Stream,
            stream_name: name,
            metadata_group_id: 7,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("test".into()),
        },
        9,
        StreamConfig::default(),
        store.clone(),
        metadata.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    writer.append(&[Bytes::from_static(b"old")]).await.unwrap();
    metadata.pause.store(true, Ordering::Release);
    let append_writer = writer.clone();
    let append = tokio::spawn(async move { append_writer.append(&[Bytes::from_static(b"new")]).await });
    tokio::time::timeout(std::time::Duration::from_secs(1), metadata.entered.notified())
        .await
        .unwrap();
    // Mirror cursor has advanced; the corresponding directory is still unpublished.
    let reader = ChunkStream::open_read_only(
        name,
        9,
        StreamConfig::default(),
        store.clone(),
        metadata.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    assert_eq!(writer.tail(), 3);
    assert_eq!(reader.tail(), 3);
    assert_eq!(writer.read_at(0, 3).await.unwrap(), Bytes::from_static(b"old"));
    assert_eq!(reader.read_at(0, 3).await.unwrap(), Bytes::from_static(b"old"));
    metadata.release.notify_one();
    append.await.unwrap().unwrap();
    assert_eq!(writer.tail(), 6);
    assert_eq!(writer.read_at(0, 6).await.unwrap(), Bytes::from_static(b"oldnew"));
    assert_eq!(reader.tail(), 3);
}
