// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
async fn unpublished_candidate_base_protects_reused_pages_after_root_retirement() {
    let memory = MemoryStreamStore::new(1024 * 1024);
    let directory = memory.directory_metadata(1).unwrap();
    let state = State::new(&directory, 2).await;
    let old_index = crowdb_chunk_stream::DirectoryIndex::new(
        Test::NAME,
        state.first.head().purpose,
        state.first.head().root,
    )
    .unwrap();
    let mutation = old_index
        .prepare_append(&directory, &Test::locations(1025, 2))
        .await
        .unwrap();
    let head = Test::head(&mutation, Some(state.first.head()));
    assert!(directory
        .publish_directory(Some(&state.first), head, &mutation)
        .await
        .is_err());
    assert!(!mutation
        .pages()
        .iter()
        .any(|page| page.reference().id == state.orphan));
    state.retire_first(&directory).await;
    assert!(
        !state.collect(&directory, state.orphan).await.0,
        "the guarded base keeps its reused subtree without a root-index row"
    );
}

#[tokio::test]
async fn unpublished_guard_keeps_candidate_pages_and_invalidates_old_proofs() {
    let memory = MemoryStreamStore::new(1024 * 1024);
    let directory = memory.directory_metadata(1).unwrap();
    let state = State::new(&directory, 2).await;
    state.retire_first(&directory).await;
    let old_cursor = directory
        .begin_directory_page_collection(&state.current, state.orphan)
        .await
        .unwrap()
        .unwrap();
    let mutation = state
        .current_index()
        .prepare_append(&directory, &Test::locations(1026, 1))
        .await
        .unwrap();
    let head = Test::head(&mutation, Some(state.current.head()));
    memory.fail_next_publish();
    assert!(directory
        .publish_directory(Some(&state.current), head.clone(), &mutation)
        .await
        .is_err());
    assert!(matches!(
        directory.advance_directory_collection(old_cursor, 128).await,
        Err(StreamError::Backpressure)
    ));
    for page in mutation.pages() {
        assert!(
            !state.collect(&directory, page.reference().id).await.0,
            "guard protects pages absent from stable roots"
        );
    }
    directory
        .publish_directory(Some(&state.current), head, &mutation)
        .await
        .unwrap();
    let fresh = directory.load_directory_head(Test::NAME).await.unwrap().unwrap();
    let cursor = directory
        .begin_directory_page_collection(&fresh, mutation.index().root().unwrap().id)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        directory.advance_directory_collection(cursor, 128).await.unwrap(),
        DirectoryCollectionStep::Retained
    ));
}

#[tokio::test]
async fn invalid_budgets_or_corrupt_candidate_bytes_cannot_authorize_deletion() {
    use crowdb_kv_client::{GetOutcome, ReadMode};
    use crowdb_protocol::key::{BinaryKey, StreamDirectoryPageKey};

    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let directory = metadata.directory_metadata();
    let state = State::new(directory, 2).await;
    state.retire_first(directory).await;
    for budget in [0, 129] {
        let cursor = directory
            .begin_directory_page_collection(&state.current, state.orphan)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            directory.advance_directory_collection(cursor, budget).await,
            Err(StreamError::InvalidRequest(_))
        ));
    }
    let key = StreamDirectoryPageKey {
        stream_name: Test::NAME,
        page_id: state.orphan,
    }
    .to_bytes();
    let GetOutcome::Found { revision, .. } = server
        .client
        .get(1, 1, &key, ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("orphan page missing before corruption")
    };
    server
        .client
        .put_cas(1, 1, &key, b"damaged page", revision)
        .await
        .unwrap();
    assert!(matches!(
        directory
            .begin_directory_page_collection(&state.current, state.orphan)
            .await,
        Err(StreamError::Corruption(_))
    ));
    assert!(matches!(
        server
            .client
            .get(1, 1, &key, ReadMode::Linearizable, None)
            .await
            .unwrap(),
        GetOutcome::Found { .. }
    ));
}
