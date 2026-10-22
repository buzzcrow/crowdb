// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/metadata_server.rs"]
mod metadata_server;

use crowdb_chunk_stream::{KvStreamMetadataStore, StreamError, StreamMetadataStore};
use crowdb_protocol::key::{BinaryKey, StreamExtentPageKey, StreamManifestHeadKey};
use metadata_server::{candidate, TestMetadataServer};

#[tokio::test]
async fn legacy_head_and_pages_are_preserved_until_the_versioned_head_cas() {
    use crowdb_kv_client::{GetOutcome, ReadMode};
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let (initial, pages) = candidate(1, 1, 1);
    let head_key = StreamManifestHeadKey {
        stream_name: initial.stream_name,
    }
    .to_bytes();
    let page = &pages[0];
    let page_key = StreamExtentPageKey {
        stream_name: page.stream_name,
        writer_epoch: 1,
        generation: 1,
        page_index: 0,
    }
    .to_bytes();
    let raw_page = bincode::serialize(page).unwrap();
    server
        .client
        .put_cas(1, 1, &page_key, &raw_page, 0)
        .await
        .unwrap();
    server
        .client
        .put_cas(1, 1, &head_key, &bincode::serialize(&initial).unwrap(), 0)
        .await
        .unwrap();
    assert_eq!(
        metadata.load_current(initial.stream_name).await.unwrap(),
        Some(initial.clone())
    );
    assert_eq!(
        metadata
            .load_extent_page(initial.stream_name, 1, 1, 0)
            .await
            .unwrap(),
        Some(page.clone())
    );
    let (next, next_pages) = candidate(1, 2, 2);
    metadata
        .publish(Some((1, 1)), next.clone(), next_pages)
        .await
        .unwrap();
    let GetOutcome::Found { value, .. } = server
        .client
        .get(1, 1, &head_key, ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("published head missing")
    };
    assert!(value.starts_with(b"CSTR\x01\x00\x01\x00"));
    assert_eq!(
        crowdb_protocol::chunk_stream::decode_stream_manifest_record(&value)
            .unwrap()
            .value,
        next
    );
    let GetOutcome::Found { value, .. } = server
        .client
        .get(1, 1, &page_key, ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("legacy page missing")
    };
    assert_eq!(value.as_ref(), raw_page);
}

#[tokio::test]
async fn arbitrary_legacy_page_identity_is_not_mistaken_for_an_envelope() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let (mut manifest, mut pages) = candidate(1, 1, 1);
    manifest.stream_name.high = u64::from_le_bytes(*b"CSTR\x02\x00\x02\x00");
    pages[0].stream_name = manifest.stream_name;
    metadata
        .publish(None, manifest.clone(), pages.clone())
        .await
        .unwrap();
    let cold = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    assert_eq!(
        cold.load_current(manifest.stream_name).await.unwrap(),
        Some(manifest.clone())
    );
    assert_eq!(
        cold.load_extent_page(manifest.stream_name, 1, 1, 0)
            .await
            .unwrap(),
        Some(pages[0].clone())
    );
}

#[tokio::test]
async fn an_unknown_head_version_cannot_be_reinterpreted_or_overwritten() {
    use crowdb_kv_client::{GetOutcome, ReadMode};
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let (manifest, _) = candidate(1, 1, 0);
    let key = StreamManifestHeadKey {
        stream_name: manifest.stream_name,
    }
    .to_bytes();
    let mut future = crowdb_protocol::chunk_stream::encode_stream_manifest_record(&manifest).unwrap();
    future[4] = 9;
    server.client.put_cas(1, 1, &key, &future, 0).await.unwrap();
    assert!(matches!(
        metadata.load_current(manifest.stream_name).await,
        Err(StreamError::Corruption(_))
    ));
    assert!(matches!(
        metadata.publish(None, manifest, Vec::new()).await,
        Err(StreamError::Corruption(_))
    ));
    let GetOutcome::Found { value, .. } = server
        .client
        .get(1, 1, &key, ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("future head missing")
    };
    assert_eq!(value.as_ref(), future);
}

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
