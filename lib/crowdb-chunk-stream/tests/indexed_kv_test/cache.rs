// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_stream::{StreamDirectoryStore, DIRECTORY_PAGE_CACHE_SLOTS};
use crowdb_protocol::chunk_stream::{
    encode_stream_directory_page, StreamDirectoryLeaf, StreamDirectoryPage, MAX_STREAM_DIRECTORY_RECORD_BYTES,
};

use super::*;

#[tokio::test]
async fn verified_page_cache_reuses_old_ids_across_head_generations() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let mutation = Test::empty()
        .prepare_append(&metadata, &Test::locations(0, 1))
        .await
        .unwrap();
    let saved = metadata
        .publish_directory(None, Test::head(&mutation, None), &mutation)
        .await
        .unwrap();
    let root = mutation.index().root().unwrap();
    let _ = server.client.drain_window();
    let first = metadata
        .load_directory_record(Test::NAME, root.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(server.client.drain_window().get.len(), 1);
    assert_eq!(
        metadata
            .load_directory_record(Test::NAME, root.id)
            .await
            .unwrap()
            .unwrap(),
        first
    );
    assert_eq!(server.client.drain_window().get.len(), 0);
    let next = mutation
        .index()
        .prepare_append(&metadata, &Test::locations(1, 1))
        .await
        .unwrap();
    metadata
        .publish_directory(Some(&saved), Test::head(&next, Some(saved.head())), &next)
        .await
        .unwrap();
    let _ = server.client.drain_window();
    assert_eq!(
        metadata
            .load_directory_record(Test::NAME, root.id)
            .await
            .unwrap()
            .unwrap(),
        first
    );
    assert_eq!(server.client.drain_window().get.len(), 0);
    assert!(metadata.directory_cache_snapshot().hits >= 2);
}

#[tokio::test]
async fn cache_residency_is_fixed_even_as_historical_page_count_grows() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let pages: Vec<_> = (0..512)
        .map(|offset| {
            encode_stream_directory_page(&StreamDirectoryPage::Leaf(StreamDirectoryLeaf {
                stream_name: Test::NAME,
                purpose: crowdb_protocol::chunk_stream::StreamPurpose::Stream,
                locations: Test::locations(offset, 1),
            }))
            .unwrap()
        })
        .collect();
    let ops: Vec<_> = pages
        .iter()
        .map(|(reference, bytes)| BatchOp::Put {
            key: Bytes::from(
                StreamDirectoryPageKey {
                    stream_name: Test::NAME,
                    page_id: reference.id,
                }
                .to_bytes(),
            ),
            value: Bytes::copy_from_slice(bytes),
        })
        .collect();
    server.client.batch_write(1, 1, &ops).await.unwrap();
    for (reference, bytes) in &pages {
        assert_eq!(
            metadata
                .load_directory_record(Test::NAME, reference.id)
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            bytes
        );
    }
    let snapshot = metadata.directory_cache_snapshot();
    assert_eq!(snapshot.slot_limit, DIRECTORY_PAGE_CACHE_SLOTS);
    assert!(snapshot.resident_pages <= DIRECTORY_PAGE_CACHE_SLOTS);
    assert!(snapshot.resident_bytes <= DIRECTORY_PAGE_CACHE_SLOTS * MAX_STREAM_DIRECTORY_RECORD_BYTES);
    assert_eq!(snapshot.misses, 512);
    assert!(snapshot.replacements > 0);
    assert_eq!(
        KvStreamMetadataStore::new(server.client.clone(), 1, 1)
            .unwrap()
            .directory_cache_snapshot()
            .resident_pages,
        0
    );
}

#[tokio::test]
async fn corrupt_or_oversized_records_never_enter_the_cache() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let mutation = Test::empty()
        .prepare_append(&metadata, &Test::locations(0, 1))
        .await
        .unwrap();
    let id = mutation.pages()[0].reference().id;
    let key = StreamDirectoryPageKey {
        stream_name: Test::NAME,
        page_id: id,
    }
    .to_bytes();
    let created = server.client.put_cas(1, 1, &key, b"corrupt", 0).await.unwrap();
    assert!(matches!(
        metadata.load_directory_record(Test::NAME, id).await,
        Err(StreamError::Corruption(_))
    ));
    server
        .client
        .put_cas(
            1,
            1,
            &key,
            &vec![0; MAX_STREAM_DIRECTORY_RECORD_BYTES + 1],
            created.revision,
        )
        .await
        .unwrap();
    assert!(matches!(
        metadata.load_directory_record(Test::NAME, id).await,
        Err(StreamError::Corruption(_))
    ));
    assert_eq!(metadata.directory_cache_snapshot().resident_pages, 0);
    assert_eq!(metadata.directory_cache_snapshot().hits, 0);
}
