// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "indexed_kv_test/cache.rs"]
mod cache;
#[path = "common/indexed_metadata.rs"]
mod indexed_metadata;
#[path = "common/metadata_server.rs"]
mod metadata_server;

use bytes::Bytes;
use crowdb_chunk_stream::{DirectoryIndex, KvStreamMetadataStore, StreamError, StreamMetadataStore};
use crowdb_kv_client::{BatchOp, GetOutcome, ReadMode};
use crowdb_protocol::chunk_stream::{
    decode_stream_publication_guard, stream_publication_id, StreamActiveAttachment, StreamDirectoryControl,
    StreamName,
};
use crowdb_protocol::key::{
    BinaryKey, StreamDirectoryControlKey, StreamDirectoryPageKey, StreamPublicationGuardKey,
};
use indexed_metadata::TestIndexedMetadata as Test;
use metadata_server::TestMetadataServer;

async fn get(server: &TestMetadataServer, key: &[u8]) -> Option<(Bytes, u64)> {
    match server
        .client
        .get(1, 1, key, ReadMode::Linearizable, None)
        .await
        .unwrap()
    {
        GetOutcome::Found { value, revision } => Some((value, revision)),
        GetOutcome::NotFound => None,
    }
}

#[tokio::test]
async fn published_paths_retain_old_roots_and_atomically_remove_their_guard() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let mutation = Test::empty()
        .prepare_append(&metadata, &Test::locations(0, 1025))
        .await
        .unwrap();
    let head = Test::head(&mutation, None);
    let saved = metadata
        .publish_directory(None, head.clone(), &mutation)
        .await
        .unwrap();
    for page in mutation.pages() {
        let key = StreamDirectoryPageKey {
            stream_name: Test::NAME,
            page_id: page.reference().id,
        }
        .to_bytes();
        assert_eq!(get(&server, &key).await.unwrap().0, *page.bytes());
    }
    let guard_key = StreamPublicationGuardKey {
        stream_name: Test::NAME,
        publication_id: stream_publication_id(&head).unwrap(),
    }
    .to_bytes();
    assert!(get(&server, &guard_key).await.is_none());
    let cold = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    assert_eq!(
        cold.load_directory_head(Test::NAME)
            .await
            .unwrap()
            .unwrap()
            .head(),
        &head
    );
    let next = mutation
        .index()
        .prepare_append(&cold, &Test::locations(1025, 4))
        .await
        .unwrap();
    let next_head = Test::head(&next, Some(saved.head()));
    let after = cold
        .publish_directory(Some(&saved), next_head.clone(), &next)
        .await
        .unwrap();
    assert_eq!(after.head(), &next_head);
    assert_eq!(
        cold.load_directory_root(Test::NAME, head.identity())
            .await
            .unwrap(),
        Some(head.clone())
    );
    assert_eq!(
        cold.load_directory_root(Test::NAME, next_head.identity())
            .await
            .unwrap(),
        Some(next_head)
    );
    assert_eq!(
        next.index().lookup(&cold, 1028).await.unwrap(),
        Some(Test::locations(1028, 1)[0])
    );
    assert!(
        metadata.load_current(Test::NAME).await.is_err(),
        "legacy API cannot reinterpret an indexed head"
    );
}

#[tokio::test]
async fn conflicting_page_stops_publication_and_keeps_protection_for_retry() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let mutation = Test::empty()
        .prepare_append(&metadata, &Test::locations(0, 1))
        .await
        .unwrap();
    let head = Test::head(&mutation, None);
    let id = mutation.pages()[0].reference().id;
    let key = StreamDirectoryPageKey {
        stream_name: Test::NAME,
        page_id: id,
    }
    .to_bytes();
    server
        .client
        .put_cas(1, 1, &key, b"conflicting bytes", 0)
        .await
        .unwrap();
    assert!(matches!(
        metadata.publish_directory(None, head.clone(), &mutation).await,
        Err(StreamError::Corruption(_))
    ));
    assert!(metadata.load_directory_head(Test::NAME).await.unwrap().is_none());
    let publication_id = stream_publication_id(&head).unwrap();
    let guard_key = StreamPublicationGuardKey {
        stream_name: Test::NAME,
        publication_id,
    }
    .to_bytes();
    let guard = decode_stream_publication_guard(
        &get(&server, &guard_key).await.unwrap().0,
        publication_id,
        Test::NAME,
    )
    .unwrap();
    assert_eq!(guard.pages, vec![id]);
    assert_eq!(guard.head, head);
    server.client.delete(1, 1, &key, None).await.unwrap();
    metadata
        .publish_directory(None, head.clone(), &mutation)
        .await
        .unwrap();
    assert!(get(&server, &guard_key).await.is_none());
    assert_eq!(
        metadata
            .load_directory_head(Test::NAME)
            .await
            .unwrap()
            .unwrap()
            .head(),
        &head
    );
}

#[tokio::test]
async fn candidate_protection_and_head_publication_invalidate_old_gc_revisions() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let initial = Test::empty()
        .prepare_append(&metadata, &Test::locations(0, 1))
        .await
        .unwrap();
    let head = Test::head(&initial, None);
    let saved = metadata
        .publish_directory(None, head.clone(), &initial)
        .await
        .unwrap();
    let control_key = StreamDirectoryControlKey {
        stream_name: Test::NAME,
    }
    .to_bytes();
    let (control_value, before_revision) = get(&server, &control_key).await.unwrap();
    StreamDirectoryControl::decode(
        &control_value,
        StreamDirectoryControl {
            stream_name: Test::NAME,
            purpose: head.purpose,
        },
    )
    .unwrap();
    let next = initial
        .index()
        .prepare_append(&metadata, &Test::locations(1, 1))
        .await
        .unwrap();
    let next_head = Test::head(&next, Some(saved.head()));
    let page_key = StreamDirectoryPageKey {
        stream_name: Test::NAME,
        page_id: next.pages()[0].reference().id,
    }
    .to_bytes();
    server
        .client
        .put_cas(1, 1, &page_key, b"old orphan with a conflicting identity", 0)
        .await
        .unwrap();
    assert!(metadata
        .publish_directory(Some(&saved), next_head.clone(), &next)
        .await
        .is_err());
    let (_, protected_revision) = get(&server, &control_key).await.unwrap();
    assert_ne!(before_revision, protected_revision);
    let gc_ops = [
        BatchOp::Put {
            key: Bytes::copy_from_slice(&control_key),
            value: control_value.clone(),
        },
        BatchOp::Delete {
            key: Bytes::copy_from_slice(&page_key),
        },
    ];
    assert!(matches!(
        server
            .client
            .batch_write_cas(1, 1, &gc_ops, &control_key, before_revision)
            .await,
        Err(crowdb_kv_client::Error::CasFailed { .. })
    ));
    server.client.delete(1, 1, &page_key, None).await.unwrap();
    metadata
        .publish_directory(Some(&saved), next_head, &next)
        .await
        .unwrap();
    assert!(matches!(
        server
            .client
            .batch_write_cas(1, 1, &gc_ops, &control_key, protected_revision)
            .await,
        Err(crowdb_kv_client::Error::CasFailed { .. })
    ));
    assert_eq!(get(&server, &page_key).await.unwrap().0, *next.pages()[0].bytes());
}

#[tokio::test]
async fn stale_snapshot_cannot_overwrite_a_new_owner() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let initial = Test::empty()
        .prepare_append(&metadata, &Test::locations(0, 1))
        .await
        .unwrap();
    let saved = metadata
        .publish_directory(None, Test::head(&initial, None), &initial)
        .await
        .unwrap();
    let next = initial.index().prepare_append(&metadata, &[]).await.unwrap();
    let stale = Test::head(&next, Some(saved.head()));
    let mut adopted = stale.clone();
    adopted.writer_epoch += 1;
    adopted.owner_session = [2; 16];
    metadata
        .publish_directory(Some(&saved), adopted.clone(), &next)
        .await
        .unwrap();
    assert!(matches!(
        metadata.publish_directory(Some(&saved), stale, &next).await,
        Err(StreamError::StaleWriter)
    ));
    assert_eq!(
        metadata
            .load_directory_head(Test::NAME)
            .await
            .unwrap()
            .unwrap()
            .head(),
        &adopted
    );
}

#[tokio::test]
async fn head_root_and_active_checkpoint_are_validated_before_storing_anything() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let locations = Test::locations(0, 1);
    let mutation = Test::empty().prepare_append(&metadata, &locations).await.unwrap();
    let mut head = Test::head(&mutation, None);
    head.active = Some(StreamActiveAttachment {
        chunk_id: locations[0].location.chunk_id,
        logical_start: 0,
        checkpoint_physical: 49,
        checkpoint_logical: 1,
    });
    assert!(metadata
        .publish_directory(None, head.clone(), &mutation)
        .await
        .is_err());
    assert!(metadata.load_directory_head(Test::NAME).await.unwrap().is_none());
    head.active.as_mut().unwrap().checkpoint_physical = 48;
    metadata.publish_directory(None, head, &mutation).await.unwrap();
    let mut wrong = Test::head(&mutation, None);
    wrong.stream_name = StreamName::default();
    assert!(metadata.publish_directory(None, wrong, &mutation).await.is_err());
}

#[tokio::test]
async fn indexed_publication_requires_legacy_migration_instead_of_reinterpreting_it() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let (mut legacy, _) = metadata_server::candidate(1, 1, 0);
    legacy.stream_name = Test::NAME;
    metadata.publish(None, legacy.clone(), vec![]).await.unwrap();
    assert!(matches!(
        metadata.load_directory_head(Test::NAME).await,
        Err(StreamError::InvalidRequest(_))
    ));
    let empty = Test::empty().prepare_append(&metadata, &[]).await.unwrap();
    assert!(metadata
        .publish_directory(None, Test::head(&empty, None), &empty)
        .await
        .is_err());
    assert_eq!(metadata.load_current(Test::NAME).await.unwrap(), Some(legacy));
}

#[tokio::test]
async fn concurrent_identical_candidates_never_misclassify_new_control_as_corruption() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    for low in 100..116 {
        let name = StreamName { low, ..Test::NAME };
        let index =
            DirectoryIndex::new(name, crowdb_protocol::chunk_stream::StreamPurpose::Stream, None).unwrap();
        let mutation = index
            .prepare_append(&metadata, &Test::locations(0, 1))
            .await
            .unwrap();
        let mut head = Test::head(&mutation, None);
        head.stream_name = name;
        let (first, second) = tokio::join!(
            metadata.publish_directory(None, head.clone(), &mutation),
            metadata.publish_directory(None, head.clone(), &mutation)
        );
        assert!(
            first.is_ok() || second.is_ok(),
            "first={first:?}, second={second:?}"
        );
        for result in [first, second] {
            assert!(
                result.is_ok()
                    || matches!(result, Err(StreamError::Backpressure | StreamError::WriteStalled)),
                "{result:?}"
            );
        }
        assert_eq!(
            metadata.load_directory_head(name).await.unwrap().unwrap().head(),
            &head
        );
    }
}
