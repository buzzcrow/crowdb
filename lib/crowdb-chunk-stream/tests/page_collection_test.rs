// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/directory_collection.rs"]
mod directory_collection;
#[path = "common/indexed_metadata.rs"]
mod indexed_metadata;
#[path = "common/metadata_server.rs"]
mod metadata_server;

use crowdb_chunk_stream::{
    memory::MemoryStreamStore, DirectoryCollectionStep, DirectoryMetadataStore, KvStreamMetadataStore,
    StreamDirectoryStore, StreamError, StreamMetadataStore,
};
use directory_collection::TestCollectionState as State;
use indexed_metadata::TestIndexedMetadata as Test;
use metadata_server::TestMetadataServer;

#[path = "page_collection_test/faults.rs"]
mod faults;
#[path = "page_collection_test/retirement.rs"]
mod retirement;

async fn check_collection(directory: &DirectoryMetadataStore) {
    let state = State::new(directory, 10).await;
    assert!(
        !state.collect(directory, state.orphan).await.0,
        "retained old roots protect pages"
    );
    let pin = directory.pin_directory_root(state.pin()).await.unwrap();
    state.retire_first(directory).await;
    assert!(
        !state.collect(directory, state.orphan).await.0,
        "exact pin survives root-index retirement"
    );
    directory.release_directory_pin(&pin).await.unwrap();
    assert!(directory
        .load_directory_record(Test::NAME, state.orphan)
        .await
        .unwrap()
        .is_some());
    let (deleted, observations) = state.collect(directory, state.orphan).await;
    assert!(deleted);
    assert!(
        observations >= 10,
        "one-observation passes resume through retained roots"
    );
    assert!(directory
        .begin_directory_page_collection(&state.current, state.orphan)
        .await
        .unwrap()
        .is_none());
    assert!(directory
        .load_directory_record(Test::NAME, state.orphan)
        .await
        .unwrap()
        .is_none());
    assert!(
        state.collect(directory, state.old_leaf).await.0,
        "unshared changed leaf is orphaned"
    );
    assert!(
        !state.collect(directory, state.shared).await.0,
        "independent unchanged leaf is shared"
    );
    assert!(
        !state
            .collect(directory, state.current.head().root.unwrap().id)
            .await
            .0
    );
    assert_eq!(
        state.current_index().lookup(directory, 1025).await.unwrap(),
        Some(Test::locations(1025, 1)[0])
    );
    assert_eq!(
        directory
            .load_directory_head(Test::NAME)
            .await
            .unwrap()
            .unwrap()
            .head(),
        state.current.head()
    );
}

#[tokio::test]
async fn memory_collection_resumes_without_history_materialization_and_keeps_shared_pages() {
    let memory = MemoryStreamStore::new(1024 * 1024);
    check_collection(&memory.directory_metadata(1).unwrap()).await;
    assert_eq!(memory.metadata_publish_count(), 10);
}

#[tokio::test]
async fn kv_collection_preserves_pins_and_shared_pages_and_removes_unreachable_records() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    check_collection(metadata.directory_metadata()).await;
}

#[tokio::test]
async fn new_pin_invalidates_an_in_progress_collection_proof() {
    let memory = MemoryStreamStore::new(1024 * 1024);
    let directory = memory.directory_metadata(1).unwrap();
    let state = State::new(&directory, 2).await;
    state.retire_first(&directory).await;
    let cursor = directory
        .begin_directory_page_collection(&state.current, state.orphan)
        .await
        .unwrap()
        .unwrap();
    let DirectoryCollectionStep::Pending(cursor) =
        directory.advance_directory_collection(cursor, 1).await.unwrap()
    else {
        panic!("first bounded pass must stop after current-root observation")
    };
    let mut pin = state.pin();
    pin.head = state.current.head().clone();
    pin.captured_tail = pin.head.published_tail;
    let saved = directory.pin_directory_root(pin).await.unwrap();
    assert!(matches!(
        directory.advance_directory_collection(cursor, 128).await,
        Err(StreamError::Backpressure)
    ));
    assert!(directory
        .load_directory_record(Test::NAME, state.orphan)
        .await
        .unwrap()
        .is_some());
    directory.release_directory_pin(&saved).await.unwrap();
    assert!(state.collect(&directory, state.orphan).await.0);
}
