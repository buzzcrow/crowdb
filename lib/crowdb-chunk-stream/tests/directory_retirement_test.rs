// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/indexed_metadata.rs"]
mod indexed_metadata;
#[path = "common/metadata_server.rs"]
mod metadata_server;

use crowdb_chunk_stream::{
    memory::MemoryStreamStore, DirectoryHeadSnapshot, DirectoryIndex, DirectoryMetadataStore,
    KvStreamMetadataStore, StreamError, StreamMetadataStore,
};
use crowdb_protocol::chunk_stream::{StreamRootPin, StreamRootPinId};
use indexed_metadata::TestIndexedMetadata as Test;
use metadata_server::TestMetadataServer;

async fn append_root(directory: &DirectoryMetadataStore) -> DirectoryHeadSnapshot {
    let previous = directory.load_directory_head(Test::NAME).await.unwrap();
    let index = DirectoryIndex::new(
        Test::NAME,
        Test::empty().purpose(),
        previous.as_ref().and_then(|head| head.head().root),
    )
    .unwrap();
    let next = index.root().map_or(0, |root| root.end_logical);
    let mutation = index
        .prepare_append(directory, &Test::locations(next, 1))
        .await
        .unwrap();
    directory
        .publish_directory(
            previous.as_ref(),
            Test::head(&mutation, previous.as_ref().map(DirectoryHeadSnapshot::head)),
            &mutation,
        )
        .await
        .unwrap()
}

async fn check_retirement(directory: &DirectoryMetadataStore) {
    let first = append_root(directory).await;
    let pin = StreamRootPin {
        id: StreamRootPinId([1; 16]),
        head: first.head().clone(),
        needed_offset: 0,
        captured_tail: 1,
        expires_at_ms: None,
    };
    let saved_pin = directory.pin_directory_root(pin.clone()).await.unwrap();
    for _ in 0..4 {
        append_root(directory).await;
    }
    assert!(matches!(
        directory.retire_directory_roots_before(&first, 1, 2).await,
        Err(StreamError::Backpressure)
    ));
    let current = directory.load_directory_head(Test::NAME).await.unwrap().unwrap();
    for (horizon, budget) in [(0, 1), (6, 1), (5, 0), (5, 129)] {
        assert!(matches!(
            directory
                .retire_directory_roots_before(&current, horizon, budget)
                .await,
            Err(StreamError::InvalidRequest(_))
        ));
    }
    for expected in [2, 2, 0] {
        assert_eq!(
            directory
                .retire_directory_roots_before(&current, 5, 2)
                .await
                .unwrap(),
            expected
        );
    }
    let remaining = directory.scan_directory_roots(Test::NAME, None, 8).await.unwrap();
    assert_eq!(remaining.entries, [current.head().identity()]);
    assert_eq!(
        directory
            .load_directory_head(Test::NAME)
            .await
            .unwrap()
            .unwrap()
            .head(),
        current.head()
    );
    assert_eq!(
        directory
            .scan_directory_pages(Test::NAME, None, 8)
            .await
            .unwrap()
            .entries
            .len(),
        5,
        "root-index retirement cannot delete shared or orphan directory pages"
    );
    assert_eq!(
        directory.pin_directory_root(pin.clone()).await.unwrap().pin(),
        &pin
    );
    let index = DirectoryIndex::new(Test::NAME, pin.head.purpose, pin.head.root).unwrap();
    assert_eq!(
        index.lookup(directory, 0).await.unwrap(),
        Some(Test::locations(0, 1)[0])
    );
    let mut late = pin;
    late.id = StreamRootPinId([2; 16]);
    assert!(matches!(
        directory.pin_directory_root(late).await,
        Err(StreamError::ReadUnavailable(_))
    ));
    directory.release_directory_pin(&saved_pin).await.unwrap();
}

#[tokio::test]
async fn memory_root_retirement_is_bounded_and_keeps_exact_pins_and_shared_pages() {
    let memory = MemoryStreamStore::new(1024 * 1024);
    check_retirement(&memory.directory_metadata(1).unwrap()).await;
    assert_eq!(memory.metadata_publish_count(), 5);
}

#[tokio::test]
async fn kv_root_retirement_uses_the_same_bounded_current_head_and_pin_contract() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    check_retirement(metadata.directory_metadata()).await;
}
