// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::chunk_collection::TestChunkCollection as Collection;
use crowdb_chunk_stream::StreamChunkStore;
use crowdb_protocol::chunk_stream::{StreamRootPin, StreamRootPinId};

async fn check_retention(store: &MemoryStreamStore, directory: &DirectoryMetadataStore) {
    let (first, current, span) = Collection::setup(store, directory, true).await;
    let saved = directory
        .pin_directory_root(StreamRootPin {
            id: StreamRootPinId([1; 16]),
            head: first.head().clone(),
            needed_offset: 0,
            captured_tail: 1,
            expires_at_ms: None,
        })
        .await
        .unwrap();
    assert!(Collection::prove(directory, &current, span).await.is_none());
    let partial = Collection::trim(directory, &current, 1).await;
    assert!(Collection::prove(directory, &partial, span).await.is_none());
    let current = Collection::trim(directory, &partial, 2).await;
    assert!(
        Collection::prove(directory, &current, span).await.is_none(),
        "retained roots still protect the whole chunk"
    );
    assert_eq!(
        directory
            .retire_directory_roots_before(&current, current.head().generation, 128)
            .await
            .unwrap(),
        3
    );
    assert!(
        Collection::prove(directory, &current, span).await.is_none(),
        "a pin needing one byte keeps the whole chunk"
    );
    directory.release_directory_pin(&saved).await.unwrap();
    {
        let ticket = Collection::prove(directory, &current, span).await.unwrap();
        assert_eq!(ticket.span(), &span);
        assert!(!store.is_released(span.chunk_id).await);
        assert_eq!(
            directory
                .load_directory_span(Test::NAME, span.chunk_id)
                .await
                .unwrap(),
            Some(span)
        );
    }
    let ticket = Collection::prove(directory, &current, span).await.unwrap();
    let empty = directory
        .pin_directory_root(StreamRootPin {
            id: StreamRootPinId([2; 16]),
            head: current.head().clone(),
            needed_offset: 2,
            captured_tail: 2,
            expires_at_ms: None,
        })
        .await
        .unwrap();
    assert_eq!(
        directory
            .finish_directory_chunk_release(ticket, store)
            .await
            .unwrap()
            .reclaimed_bytes,
        96
    );
    assert!(store.is_released(span.chunk_id).await);
    assert!(!store.is_released(current.head().active.unwrap().chunk_id).await);
    assert!(directory
        .load_directory_span(Test::NAME, span.chunk_id)
        .await
        .unwrap()
        .is_none());
    assert!(directory
        .begin_directory_chunk_collection(&current, span.chunk_id)
        .await
        .unwrap()
        .is_none());
    directory.release_directory_pin(&empty).await.unwrap();
}

#[tokio::test]
async fn memory_whole_chunk_proof_keeps_retained_roots_and_exact_pin_intervals() {
    let store = MemoryStreamStore::new(1024 * 1024);
    check_retention(&store, &store.directory_metadata(1).unwrap()).await;
    assert_eq!(store.metadata_publish_count(), 4);
}

#[tokio::test]
async fn kv_whole_chunk_proof_and_physical_completion_use_the_same_retention_contract() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let store = MemoryStreamStore::new(1024 * 1024);
    check_retention(&store, metadata.directory_metadata()).await;
}

#[tokio::test]
async fn interrupted_physical_release_keeps_durable_retry_work_and_repeats_idempotently() {
    let store = MemoryStreamStore::new(1024 * 1024);
    let directory = store.directory_metadata(1).unwrap();
    let (_, current, span) = Collection::setup(&store, &directory, true).await;
    let current = Collection::trim(&directory, &current, 2).await;
    directory
        .retire_directory_roots_before(&current, current.head().generation, 128)
        .await
        .unwrap();
    {
        let _ticket = Collection::prove(&directory, &current, span).await.unwrap();
        assert_eq!(
            store
                .release_trimmed(span.chunk_id, span.logical_end, span.physical_end)
                .await
                .unwrap()
                .reclaimed_bytes,
            96
        );
    }
    assert_eq!(
        directory
            .load_directory_span(Test::NAME, span.chunk_id)
            .await
            .unwrap(),
        Some(span)
    );
    let cold = store.directory_metadata(1).unwrap();
    let head = cold.load_directory_head(Test::NAME).await.unwrap().unwrap();
    let ticket = Collection::prove(&cold, &head, span).await.unwrap();
    assert_eq!(
        cold.finish_directory_chunk_release(ticket, &store)
            .await
            .unwrap()
            .reclaimed_bytes,
        0
    );
    assert!(cold
        .load_directory_span(Test::NAME, span.chunk_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn physical_active_state_prevents_release_even_after_a_metadata_proof() {
    let store = MemoryStreamStore::new(1024 * 1024);
    let directory = store.directory_metadata(1).unwrap();
    let (_, current, span) = Collection::setup(&store, &directory, false).await;
    let current = Collection::trim(&directory, &current, 2).await;
    directory
        .retire_directory_roots_before(&current, current.head().generation, 128)
        .await
        .unwrap();
    let ticket = Collection::prove(&directory, &current, span).await.unwrap();
    assert!(matches!(
        directory.finish_directory_chunk_release(ticket, &store).await,
        Err(StreamError::Corruption(_))
    ));
    assert!(!store.is_released(span.chunk_id).await);
    assert_eq!(
        directory
            .load_directory_span(Test::NAME, span.chunk_id)
            .await
            .unwrap(),
        Some(span)
    );
    store.seal(span.chunk_id, 1, span.physical_end).await.unwrap();
    let ticket = Collection::prove(&directory, &current, span).await.unwrap();
    directory
        .finish_directory_chunk_release(ticket, &store)
        .await
        .unwrap();
    assert!(store.is_released(span.chunk_id).await);
}

#[tokio::test]
async fn a_new_collection_ticket_supersedes_the_old_one_before_any_physical_delete() {
    let store = MemoryStreamStore::new(1024 * 1024);
    let directory = store.directory_metadata(1).unwrap();
    let (_, current, span) = Collection::setup(&store, &directory, true).await;
    let current = Collection::trim(&directory, &current, 2).await;
    directory
        .retire_directory_roots_before(&current, current.head().generation, 128)
        .await
        .unwrap();
    let old = Collection::prove(&directory, &current, span).await.unwrap();
    let newer = Collection::prove(&directory, &current, span).await.unwrap();
    assert!(matches!(
        directory.finish_directory_chunk_release(old, &store).await,
        Err(StreamError::Backpressure)
    ));
    assert!(!store.is_released(span.chunk_id).await);
    assert_eq!(
        directory
            .finish_directory_chunk_release(newer, &store)
            .await
            .unwrap()
            .reclaimed_bytes,
        span.physical_end
    );
}
