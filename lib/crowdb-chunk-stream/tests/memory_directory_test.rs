// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/indexed_metadata.rs"]
mod indexed_metadata;

use crowdb_chunk_stream::{memory::MemoryStreamStore, StreamError, StreamMetadataStore};
use indexed_metadata::TestIndexedMetadata as Test;

#[tokio::test]
async fn memory_and_kv_share_immutable_paths_and_failed_publication_protection() {
    let memory = MemoryStreamStore::new(1024 * 1024);
    let directory = memory.directory_metadata(1).unwrap();
    let first = Test::empty()
        .prepare_append(&directory, &Test::locations(0, 1))
        .await
        .unwrap();
    let initial = directory
        .publish_directory(None, Test::head(&first, None), &first)
        .await
        .unwrap();
    let second = first
        .index()
        .prepare_append(&directory, &Test::locations(1, 1))
        .await
        .unwrap();
    let head = Test::head(&second, Some(initial.head()));
    memory.fail_next_publish();
    assert!(matches!(
        directory
            .publish_directory(Some(&initial), head.clone(), &second)
            .await,
        Err(StreamError::Internal(_))
    ));
    assert_eq!(
        directory
            .load_directory_head(Test::NAME)
            .await
            .unwrap()
            .unwrap()
            .head(),
        initial.head()
    );
    let cold = memory.directory_metadata(1).unwrap();
    let published = cold
        .publish_directory(Some(&initial), head.clone(), &second)
        .await
        .unwrap();
    assert_eq!(published.head(), &head);
    assert_eq!(memory.metadata_publish_count(), 2);
    assert_eq!(
        cold.load_directory_root(Test::NAME, initial.head().identity())
            .await
            .unwrap(),
        Some(initial.head().clone())
    );
    assert!(
        memory.load_current(Test::NAME).await.is_err(),
        "legacy APIs cannot reinterpret indexed heads"
    );
    assert!(memory.directory_metadata(0).is_err());
}
