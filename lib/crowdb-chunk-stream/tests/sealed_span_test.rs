// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/chunk_collection.rs"]
mod chunk_collection;
#[path = "common/indexed_metadata.rs"]
mod indexed_metadata;
#[path = "common/metadata_server.rs"]
mod metadata_server;

use crowdb_chunk_stream::{
    memory::MemoryStreamStore, DirectoryHeadSnapshot, DirectoryIndex, DirectoryMetadataStore,
    DirectoryMutation, KvStreamMetadataStore, StreamError, StreamMetadataStore,
};
use crowdb_protocol::chunk_stream::{
    StreamActiveAttachment, StreamDirectoryHead, StreamExtentLocation, StreamSealedChunkSpan,
};
use indexed_metadata::TestIndexedMetadata as Test;
use metadata_server::TestMetadataServer;

#[path = "sealed_span_test/collection.rs"]
mod collection;
#[path = "sealed_span_test/kv.rs"]
mod kv;

fn second_location() -> StreamExtentLocation {
    let mut location = Test::locations(0, 1)[0];
    location.logical_start = 1;
    location.location.frame_offset = 48;
    location
}

async fn initial(directory: &DirectoryMetadataStore) -> DirectoryHeadSnapshot {
    let mutation = Test::empty()
        .prepare_append(directory, &Test::locations(0, 1))
        .await
        .unwrap();
    let mut head = Test::head(&mutation, None);
    head.active = Some(StreamActiveAttachment {
        chunk_id: Test::locations(0, 1)[0].location.chunk_id,
        logical_start: 0,
        checkpoint_physical: 48,
        checkpoint_logical: 1,
    });
    directory.publish_directory(None, head, &mutation).await.unwrap()
}

async fn successor(
    directory: &DirectoryMetadataStore,
    previous: &DirectoryHeadSnapshot,
) -> (DirectoryMutation, StreamDirectoryHead, StreamSealedChunkSpan) {
    let index = DirectoryIndex::new(Test::NAME, previous.head().purpose, previous.head().root).unwrap();
    let mutation = index
        .prepare_append(directory, &[second_location()])
        .await
        .unwrap();
    let mut head = Test::head(&mutation, Some(previous.head()));
    head.active = Some(StreamActiveAttachment {
        chunk_id: Test::locations(1, 1)[0].location.chunk_id,
        logical_start: 2,
        checkpoint_physical: 0,
        checkpoint_logical: 2,
    });
    let span = StreamSealedChunkSpan {
        stream_name: Test::NAME,
        purpose: head.purpose,
        chunk_id: second_location().location.chunk_id,
        logical_start: 0,
        logical_end: 2,
        physical_end: 96,
    };
    (mutation, head, span)
}

async fn check_invalid(directory: &DirectoryMetadataStore) {
    let first = initial(directory).await;
    let (mutation, head, span) = successor(directory, &first).await;
    assert!(directory
        .publish_directory(Some(&first), head, &mutation)
        .await
        .is_err());
    for (logical_start, logical_end, physical_end) in [(1, 2, 96), (0, 1, 48), (0, 2, 95), (0, 2, 97)] {
        let (mutation, head, _) = successor(directory, &first).await;
        let mutation = mutation
            .with_sealed_chunk_span(StreamSealedChunkSpan {
                logical_start,
                logical_end,
                physical_end,
                ..span
            })
            .unwrap();
        assert!(matches!(
            directory.publish_directory(Some(&first), head, &mutation).await,
            Err(StreamError::Corruption(_))
        ));
    }
    assert!(directory
        .load_directory_span(Test::NAME, span.chunk_id)
        .await
        .unwrap()
        .is_none());
    assert!(directory
        .scan_directory_publications(Test::NAME, None, 4)
        .await
        .unwrap()
        .entries
        .is_empty());
    assert_eq!(
        directory
            .load_directory_head(Test::NAME)
            .await
            .unwrap()
            .unwrap()
            .head(),
        first.head()
    );
}

#[tokio::test]
async fn invalid_partial_or_missing_span_never_changes_metadata() {
    let memory = MemoryStreamStore::new(1024 * 1024);
    check_invalid(&memory.directory_metadata(1).unwrap()).await;
    assert_eq!(memory.metadata_publish_count(), 1);
}

#[tokio::test]
async fn failed_head_keeps_no_span_then_retry_atomically_records_exact_whole_chunk_bounds() {
    let memory = MemoryStreamStore::new(1024 * 1024);
    let directory = memory.directory_metadata(1).unwrap();
    let first = initial(&directory).await;
    let (mutation, head, span) = successor(&directory, &first).await;
    let mutation = mutation.with_sealed_chunk_span(span).unwrap();
    memory.fail_next_publish();
    assert!(directory
        .publish_directory(Some(&first), head.clone(), &mutation)
        .await
        .is_err());
    assert!(directory
        .load_directory_span(Test::NAME, span.chunk_id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        directory
            .load_directory_head(Test::NAME)
            .await
            .unwrap()
            .unwrap()
            .head(),
        first.head()
    );
    directory
        .publish_directory(Some(&first), head.clone(), &mutation)
        .await
        .unwrap();
    assert_eq!(
        directory
            .load_directory_span(Test::NAME, span.chunk_id)
            .await
            .unwrap(),
        Some(span)
    );
    assert_eq!(
        directory
            .scan_directory_spans(Test::NAME, None, 1)
            .await
            .unwrap()
            .entries,
        [span.chunk_id]
    );
    assert_eq!(
        directory
            .load_directory_head(Test::NAME)
            .await
            .unwrap()
            .unwrap()
            .head(),
        &head
    );
    assert_eq!(memory.metadata_publish_count(), 2);
}

#[tokio::test]
async fn retiring_an_empty_active_chunk_records_zero_bounds_without_directory_lookup() {
    let memory = MemoryStreamStore::new(1024 * 1024);
    let directory = memory.directory_metadata(1).unwrap();
    let mutation = Test::empty().prepare_append(&directory, &[]).await.unwrap();
    let mut head = Test::head(&mutation, None);
    let old_chunk = Test::locations(0, 1)[0].location.chunk_id;
    head.active = Some(StreamActiveAttachment {
        chunk_id: old_chunk,
        logical_start: 0,
        checkpoint_logical: 0,
        checkpoint_physical: 0,
    });
    let first = directory.publish_directory(None, head, &mutation).await.unwrap();
    let mut head = Test::head(&mutation, Some(first.head()));
    head.active = Some(StreamActiveAttachment {
        chunk_id: Test::locations(1, 1)[0].location.chunk_id,
        logical_start: 0,
        checkpoint_logical: 0,
        checkpoint_physical: 0,
    });
    let span = StreamSealedChunkSpan {
        stream_name: Test::NAME,
        purpose: head.purpose,
        chunk_id: old_chunk,
        logical_start: 0,
        logical_end: 0,
        physical_end: 0,
    };
    let mutation = mutation.with_sealed_chunk_span(span).unwrap();
    directory
        .publish_directory(Some(&first), head, &mutation)
        .await
        .unwrap();
    assert_eq!(
        directory
            .load_directory_span(Test::NAME, old_chunk)
            .await
            .unwrap(),
        Some(span)
    );
}

#[tokio::test]
async fn partial_logical_trim_keeps_the_sealed_chunks_original_base_and_complete_end() {
    let memory = MemoryStreamStore::new(1024 * 1024);
    let directory = memory.directory_metadata(1).unwrap();
    let first = initial(&directory).await;
    let (mutation, mut head, span) = successor(&directory, &first).await;
    head.active = Some(StreamActiveAttachment {
        chunk_id: span.chunk_id,
        logical_start: 0,
        checkpoint_physical: 96,
        checkpoint_logical: 2,
    });
    let checkpoint = directory
        .publish_directory(Some(&first), head, &mutation)
        .await
        .unwrap();
    let trimmed = mutation.index().prepare_trim(&directory, 1).await.unwrap();
    let mut head = Test::head(&trimmed, Some(checkpoint.head()));
    head.trim_offset = 1;
    head.active = checkpoint.head().active;
    let checkpoint = directory
        .publish_directory(Some(&checkpoint), head, &trimmed)
        .await
        .unwrap();
    let final_mutation = trimmed.index().prepare_append(&directory, &[]).await.unwrap();
    let mut head = Test::head(&final_mutation, Some(checkpoint.head()));
    head.trim_offset = 1;
    head.active = Some(StreamActiveAttachment {
        chunk_id: Test::locations(1, 1)[0].location.chunk_id,
        logical_start: 2,
        checkpoint_physical: 0,
        checkpoint_logical: 2,
    });
    let final_mutation = final_mutation.with_sealed_chunk_span(span).unwrap();
    directory
        .publish_directory(Some(&checkpoint), head, &final_mutation)
        .await
        .unwrap();
    assert_eq!(
        directory
            .load_directory_span(Test::NAME, span.chunk_id)
            .await
            .unwrap(),
        Some(span)
    );
    assert_eq!(span.logical_start, 0);
    assert_eq!(span.logical_end, 2);
}
