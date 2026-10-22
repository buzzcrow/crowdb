// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/indexed_metadata.rs"]
mod indexed_metadata;
#[path = "common/metadata_server.rs"]
mod metadata_server;

use crowdb_chunk_stream::{memory::MemoryStreamStore, KvStreamMetadataStore, StreamMetadataStore};
use crowdb_kv_client::{GetOutcome, ReadMode};
use crowdb_protocol::key::{BinaryKey, StreamDirectoryRootKey, StreamManifestHeadKey};
use indexed_metadata::TestIndexedMetadata as Test;
use metadata_server::TestMetadataServer;

#[tokio::test]
async fn failed_head_publication_never_creates_a_retained_root() {
    let memory = MemoryStreamStore::new(1024 * 1024);
    let directory = memory.directory_metadata(1).unwrap();
    let mutation = Test::empty()
        .prepare_append(&directory, &Test::locations(0, 1))
        .await
        .unwrap();
    let head = Test::head(&mutation, None);
    memory.fail_next_publish();
    assert!(directory
        .publish_directory(None, head.clone(), &mutation)
        .await
        .is_err());
    assert!(directory.load_directory_head(Test::NAME).await.unwrap().is_none());
    assert!(directory
        .load_directory_root(Test::NAME, head.identity())
        .await
        .unwrap()
        .is_none());
    assert!(directory
        .scan_directory_roots(Test::NAME, None, 2)
        .await
        .unwrap()
        .entries
        .is_empty());
    assert_eq!(
        directory
            .scan_directory_publications(Test::NAME, None, 2)
            .await
            .unwrap()
            .entries
            .len(),
        1
    );
    let saved = directory
        .publish_directory(None, head.clone(), &mutation)
        .await
        .unwrap();
    assert_eq!(saved.head(), &head);
    assert_eq!(
        directory
            .load_directory_root(Test::NAME, head.identity())
            .await
            .unwrap(),
        Some(head)
    );
    assert!(directory
        .scan_directory_publications(Test::NAME, None, 2)
        .await
        .unwrap()
        .entries
        .is_empty());
    assert_eq!(memory.metadata_publish_count(), 1);
}

#[tokio::test]
async fn kv_root_and_head_commit_together_after_a_revision_conflict() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let first = Test::empty()
        .prepare_append(&metadata, &Test::locations(0, 1))
        .await
        .unwrap();
    let initial = metadata
        .publish_directory(None, Test::head(&first, None), &first)
        .await
        .unwrap();
    let next = first
        .index()
        .prepare_append(&metadata, &Test::locations(1, 1))
        .await
        .unwrap();
    let head = Test::head(&next, Some(initial.head()));
    let head_key = StreamManifestHeadKey {
        stream_name: Test::NAME,
    }
    .to_bytes();
    let GetOutcome::Found { value, revision } = server
        .client
        .get(1, 1, &head_key, ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("initial head missing")
    };
    server
        .client
        .put_cas(1, 1, &head_key, &value, revision)
        .await
        .unwrap();
    assert!(metadata
        .publish_directory(Some(&initial), head.clone(), &next)
        .await
        .is_err());
    assert!(metadata
        .load_directory_root(Test::NAME, head.identity())
        .await
        .unwrap()
        .is_none());
    let fresh = metadata.load_directory_head(Test::NAME).await.unwrap().unwrap();
    assert_eq!(fresh.head(), initial.head());
    metadata
        .publish_directory(Some(&fresh), head.clone(), &next)
        .await
        .unwrap();
    let root_key = StreamDirectoryRootKey {
        stream_name: Test::NAME,
        identity: head.identity(),
    }
    .to_bytes();
    let GetOutcome::Found {
        value: head_bytes,
        revision: head_revision,
    } = server
        .client
        .get(1, 1, &head_key, ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("committed head missing")
    };
    let GetOutcome::Found {
        value: root_bytes,
        revision: root_revision,
    } = server
        .client
        .get(1, 1, &root_key, ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("committed root missing")
    };
    assert_eq!(head_bytes, root_bytes);
    assert_eq!(head_revision, root_revision);
    assert_eq!(
        metadata
            .load_directory_root(Test::NAME, initial.head().identity())
            .await
            .unwrap(),
        Some(initial.head().clone())
    );
}
