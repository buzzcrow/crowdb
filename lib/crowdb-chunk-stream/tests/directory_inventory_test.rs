// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/indexed_metadata.rs"]
mod indexed_metadata;
#[path = "common/metadata_server.rs"]
mod metadata_server;

use crowdb_chunk_stream::{
    memory::MemoryStreamStore, DirectoryHeadSnapshot, DirectoryMetadataStore, KvStreamMetadataStore,
    StreamError, StreamMetadataStore, MAX_DIRECTORY_SCAN_KEYS,
};
use crowdb_protocol::chunk_stream::{StreamName, StreamRootIdentity};
use crowdb_protocol::key::{BinaryKey, StreamDirectoryRootKey};
use indexed_metadata::TestIndexedMetadata as Test;
use metadata_server::TestMetadataServer;

async fn check_inventory(directory: &DirectoryMetadataStore) {
    let mut index = Test::empty();
    let mut previous: Option<DirectoryHeadSnapshot> = None;
    for offset in 0..5 {
        let mutation = index
            .prepare_append(directory, &Test::locations(offset, 1))
            .await
            .unwrap();
        let head = Test::head(&mutation, previous.as_ref().map(DirectoryHeadSnapshot::head));
        let saved = directory
            .publish_directory(previous.as_ref(), head, &mutation)
            .await
            .unwrap();
        index = mutation.index().clone();
        previous = Some(saved);
    }
    let mut roots = Vec::new();
    let mut after = None;
    loop {
        let page = directory
            .scan_directory_roots(Test::NAME, after, 2)
            .await
            .unwrap();
        assert!(page.entries.len() <= 2);
        roots.extend(page.entries);
        let Some(next) = page.next else { break };
        assert_eq!(roots.last(), Some(&next));
        after = Some(next);
    }
    assert_eq!(
        roots,
        (1..=5)
            .map(|generation| StreamRootIdentity {
                writer_epoch: 1,
                generation
            })
            .collect::<Vec<_>>()
    );
    let mut pages = Vec::new();
    let mut after = None;
    loop {
        let page = directory
            .scan_directory_pages(Test::NAME, after, 2)
            .await
            .unwrap();
        assert!(page.entries.len() <= 2);
        pages.extend(page.entries);
        let Some(next) = page.next else { break };
        after = Some(next);
    }
    assert_eq!(pages.len(), 5);
    assert!(pages.windows(2).all(|pair| pair[0] < pair[1]));
    let guards = directory
        .scan_directory_publications(Test::NAME, None, 2)
        .await
        .unwrap();
    assert!(guards.entries.is_empty());
    assert!(guards.next.is_none());
    let absent = directory
        .scan_directory_roots(StreamName { high: 29, low: 12 }, None, 2)
        .await
        .unwrap();
    assert!(absent.entries.is_empty());
    assert!(absent.next.is_none());
    check_invalid_limits(directory).await;
}

async fn check_invalid_limits(directory: &DirectoryMetadataStore) {
    for limit in [0, MAX_DIRECTORY_SCAN_KEYS + 1] {
        assert!(matches!(
            directory.scan_directory_roots(Test::NAME, None, limit).await,
            Err(StreamError::InvalidRequest(_))
        ));
        assert!(matches!(
            directory.scan_directory_pages(Test::NAME, None, limit).await,
            Err(StreamError::InvalidRequest(_))
        ));
        assert!(matches!(
            directory
                .scan_directory_publications(Test::NAME, None, limit)
                .await,
            Err(StreamError::InvalidRequest(_))
        ));
    }
}

#[tokio::test]
async fn memory_inventory_is_bounded_sorted_and_scoped_without_head_mutations() {
    let memory = MemoryStreamStore::new(1024 * 1024);
    check_inventory(&memory.directory_metadata(1).unwrap()).await;
    assert_eq!(memory.metadata_publish_count(), 5);
    let other = memory
        .directory_metadata(7)
        .unwrap()
        .scan_directory_roots(Test::NAME, None, 2)
        .await
        .unwrap();
    assert!(other.entries.is_empty());
}

#[tokio::test]
async fn kv_inventory_pages_keys_without_materializing_or_reinterpreting_values() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    check_inventory(metadata.directory_metadata()).await;
    let id = StreamRootIdentity {
        writer_epoch: 2,
        generation: 6,
    };
    let key = StreamDirectoryRootKey {
        stream_name: Test::NAME,
        identity: id,
    }
    .to_bytes();
    server
        .client
        .put_cas(1, 1, &key, &vec![0x5a; 128 * 1024], 0)
        .await
        .unwrap();
    let page = metadata
        .directory_metadata()
        .scan_directory_roots(
            Test::NAME,
            Some(StreamRootIdentity {
                writer_epoch: 1,
                generation: 5,
            }),
            2,
        )
        .await
        .unwrap();
    assert_eq!(page.entries, vec![id]);
    assert!(page.next.is_none());
    assert!(metadata.load_directory_root(Test::NAME, id).await.is_err());
    let mut malformed = StreamDirectoryRootKey::stream_prefix(Test::NAME);
    malformed.push(0);
    server
        .client
        .put_cas(1, 1, &malformed, b"bad key", 0)
        .await
        .unwrap();
    assert!(matches!(
        metadata
            .directory_metadata()
            .scan_directory_roots(Test::NAME, None, 2)
            .await,
        Err(StreamError::Corruption(_))
    ));
}
