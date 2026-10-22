// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/metadata_server.rs"]
mod metadata_server;

use crowdb_chunk_stream::{KvStreamMetadataStore, StreamError, StreamMetadataStore};
use crowdb_protocol::key::{BinaryKey, StreamExtentPageKey};
use metadata_server::{candidate, TestMetadataServer};

#[tokio::test]
async fn successful_publications_reuse_the_cas_revision_and_persist_every_page() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let (initial, _) = candidate(1, 1, 0);
    metadata.publish(None, initial, Vec::new()).await.unwrap();
    assert_eq!(server.client.drain_window().get.len(), 1);

    let (manifest, pages) = candidate(1, 2, 17);
    metadata
        .publish(Some((1, 1)), manifest.clone(), pages.clone())
        .await
        .unwrap();
    assert_eq!(
        server.client.drain_window().get.len(),
        0,
        "successful CAS already returned the next revision"
    );
    let cold = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    assert_eq!(
        cold.load_current(manifest.stream_name).await.unwrap(),
        Some(manifest.clone())
    );
    for page in pages {
        assert_eq!(
            cold.load_extent_page(page.stream_name, 1, 2, page.page_index)
                .await
                .unwrap(),
            Some(page)
        );
    }
    let (next, pages) = candidate(1, 3, 18);
    let _ = server.client.drain_window();
    cold.publish(Some((1, 2)), next, pages).await.unwrap();
    assert_eq!(
        server.client.drain_window().get.len(),
        0,
        "a head load also seeds the revision cache"
    );
}

#[tokio::test]
async fn a_cached_revision_cannot_overwrite_a_new_writer_or_change_purpose() {
    let server = TestMetadataServer::start().await;
    let old = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let (initial, _) = candidate(1, 1, 0);
    old.publish(None, initial, Vec::new()).await.unwrap();
    let current = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let (adopted, _) = candidate(2, 2, 0);
    current
        .publish(Some((1, 1)), adopted.clone(), Vec::new())
        .await
        .unwrap();

    let (stale, _) = candidate(1, 2, 0);
    assert_eq!(
        old.publish(Some((1, 1)), stale, Vec::new()).await,
        Err(StreamError::StaleWriter)
    );
    let mut wrong_purpose = candidate(2, 3, 0).0;
    wrong_purpose.purpose = crowdb_chunk_stream::StreamPurpose::Wal;
    assert!(matches!(
        current.publish(Some((2, 2)), wrong_purpose, Vec::new()).await,
        Err(StreamError::Corruption(_))
    ));
    assert_eq!(
        old.load_current(adopted.stream_name).await.unwrap(),
        Some(adopted)
    );
}

#[tokio::test]
async fn a_conflicting_immutable_page_prevents_head_publication() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let (initial, _) = candidate(1, 1, 0);
    metadata.publish(None, initial.clone(), Vec::new()).await.unwrap();
    let (manifest, pages) = candidate(1, 2, 17);
    let mut conflict = pages[8].clone();
    conflict.physical_offsets[0] = 123;
    let key = StreamExtentPageKey {
        stream_name: conflict.stream_name,
        writer_epoch: 1,
        generation: 2,
        page_index: 8,
    }
    .to_bytes();
    server
        .client
        .put_cas(1, 1, &key, &bincode::serialize(&conflict).unwrap(), 0)
        .await
        .unwrap();
    assert!(matches!(
        metadata.publish(Some((1, 1)), manifest, pages).await,
        Err(StreamError::Corruption(_))
    ));
    assert_eq!(
        metadata.load_current(initial.stream_name).await.unwrap(),
        Some(initial)
    );
    assert_eq!(
        metadata
            .load_extent_page(conflict.stream_name, 1, 2, 8)
            .await
            .unwrap(),
        Some(conflict)
    );
}
