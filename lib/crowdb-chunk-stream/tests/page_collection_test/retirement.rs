// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crowdb_protocol::chunk_stream::{stream_publication_id, StreamPublicationId};

async fn old_candidate(directory: &DirectoryMetadataStore, state: &State, count: u64) -> StreamPublicationId {
    let index = crowdb_chunk_stream::DirectoryIndex::new(
        Test::NAME,
        state.first.head().purpose,
        state.first.head().root,
    )
    .unwrap();
    let mutation = index
        .prepare_append(directory, &Test::locations(1025, count))
        .await
        .unwrap();
    let head = Test::head(&mutation, Some(state.first.head()));
    let id = stream_publication_id(&head).unwrap();
    let outcome = directory
        .publish_directory(Some(&state.first), head, &mutation)
        .await;
    if count == 1 {
        assert!(
            outcome.is_ok(),
            "an already committed identical head reconciles successfully"
        );
    } else {
        assert!(
            outcome.is_err(),
            "a competing head at the same generation cannot replace the winner"
        );
    }
    assert!(directory
        .load_directory_publication(Test::NAME, id)
        .await
        .unwrap()
        .is_some());
    id
}

#[tokio::test]
async fn guard_retirement_paginates_completed_and_losing_candidates_but_keeps_the_next_generation() {
    let memory = MemoryStreamStore::new(1024 * 1024);
    let directory = memory.directory_metadata(1).unwrap();
    let state = State::new(&directory, 2).await;
    state.retire_first(&directory).await;
    for count in 1..=4 {
        old_candidate(&directory, &state, count).await;
    }
    let live = state
        .current_index()
        .prepare_append(&directory, &Test::locations(1026, 1))
        .await
        .unwrap();
    let live_head = Test::head(&live, Some(state.current.head()));
    let live_id = stream_publication_id(&live_head).unwrap();
    memory.fail_next_publish();
    assert!(directory
        .publish_directory(Some(&state.current), live_head.clone(), &live)
        .await
        .is_err());
    assert!(matches!(
        directory
            .retire_directory_publications(&state.first, None, 1)
            .await,
        Err(StreamError::Backpressure)
    ));
    for budget in [0, 129] {
        assert!(matches!(
            directory
                .retire_directory_publications(&state.current, None, budget)
                .await,
            Err(StreamError::InvalidRequest(_))
        ));
    }
    let mut after = None;
    let mut removed = 0;
    let mut pages = 0;
    loop {
        let page = directory
            .retire_directory_publications(&state.current, after, 1)
            .await
            .unwrap();
        assert!(page.removed <= 1);
        removed += page.removed;
        pages += 1;
        let Some(next) = page.next else { break };
        after = Some(next);
    }
    assert_eq!(removed, 4);
    assert_eq!(pages, 5);
    assert_eq!(
        directory
            .scan_directory_publications(Test::NAME, None, 8)
            .await
            .unwrap()
            .entries,
        [live_id]
    );
    assert!(
        state.collect(&directory, state.orphan).await.0,
        "retired guards no longer keep their unshared base subtree"
    );
    for page in live.pages() {
        assert!(!state.collect(&directory, page.reference().id).await.0);
    }
    let current = directory
        .publish_directory(Some(&state.current), live_head, &live)
        .await
        .unwrap();
    assert_eq!(
        directory
            .retire_directory_publications(&current, None, 8)
            .await
            .unwrap()
            .removed,
        0
    );
    assert_eq!(memory.metadata_publish_count(), 3);
}

#[tokio::test]
async fn malformed_publication_protection_prevents_retirement_and_page_collection() {
    use crowdb_kv_client::{GetOutcome, ReadMode};
    use crowdb_protocol::key::{BinaryKey, StreamPublicationGuardKey};

    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let directory = metadata.directory_metadata();
    let state = State::new(directory, 2).await;
    state.retire_first(directory).await;
    let id = old_candidate(directory, &state, 2).await;
    let key = StreamPublicationGuardKey {
        stream_name: Test::NAME,
        publication_id: id,
    }
    .to_bytes();
    let GetOutcome::Found { revision, .. } = server
        .client
        .get(1, 1, &key, ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("publication guard missing before corruption")
    };
    server
        .client
        .put_cas(1, 1, &key, b"damaged guard", revision)
        .await
        .unwrap();
    assert!(matches!(
        directory
            .retire_directory_publications(&state.current, None, 128)
            .await,
        Err(StreamError::Corruption(_))
    ));
    let cursor = directory
        .begin_directory_page_collection(&state.current, state.orphan)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        directory.advance_directory_collection(cursor, 128).await,
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
    assert!(directory
        .load_directory_record(Test::NAME, state.orphan)
        .await
        .unwrap()
        .is_some());
}
