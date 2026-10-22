// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/indexed_metadata.rs"]
mod indexed_metadata;

use crowdb_chunk_stream::{memory::MemoryStreamStore, StreamError, StreamMetadataStore, StreamMigrationStep};
use crowdb_protocol::chunk_stream::{LegacyStreamExtentPage, LegacyStreamManifest, StreamExtentPageFence};
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

#[tokio::test]
async fn memory_legacy_publication_and_conversion_use_the_same_record_namespace() {
    let memory = MemoryStreamStore::new(1024 * 1024);
    let location = Test::locations(0, 1)[0];
    let legacy = LegacyStreamManifest {
        stream_name: Test::NAME,
        metadata_group_id: 7,
        writer_epoch: 3,
        generation: 7,
        sealed_tail: 1,
        extent_pages: vec![StreamExtentPageFence {
            page_index: 11,
            first_logical: 0,
            end_logical: 1,
        }],
        ..LegacyStreamManifest::default()
    };
    let page = LegacyStreamExtentPage {
        stream_name: Test::NAME,
        writer_epoch: 3,
        generation: 7,
        page_index: 11,
        chunk_ids: vec![location.location.chunk_id],
        logical_offsets: vec![0, 1],
        physical_offsets: vec![0],
        frame_lengths: vec![35],
    };
    memory
        .publish(None, legacy.clone(), vec![page.clone()])
        .await
        .unwrap();
    let directory = memory.directory_metadata(7).unwrap();
    let mut snapshot = directory
        .begin_directory_migration(Test::NAME, 4, [9; 16])
        .await
        .unwrap();
    assert!(memory.load_current(Test::NAME).await.is_err());
    assert_eq!(
        memory.publish(Some((3, 7)), legacy, vec![page.clone()]).await,
        Err(StreamError::StaleWriter)
    );
    let published = loop {
        match directory
            .advance_directory_migration(&snapshot, &memory, 1)
            .await
            .unwrap()
        {
            StreamMigrationStep::Progress { snapshot: next, .. } => snapshot = next,
            StreamMigrationStep::Complete(published) => break published,
        }
    };
    assert_eq!(published.head().generation, 8);
    assert_eq!(published.head().published_tail, 1);
    assert_eq!(published.head().metadata_group_id, 7);
    assert_eq!(
        memory.load_extent_page(Test::NAME, 3, 7, 11).await.unwrap(),
        Some(page)
    );
    let stored = directory.load_directory_head(Test::NAME).await.unwrap().unwrap();
    assert_eq!(stored.head(), published.head());
}
