// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_stream::{memory::MemoryStreamStore, StreamChunkStore, StreamMigrationStep};
use crowdb_protocol::chunk_stream::{StreamMigrationPhase, StreamPurpose};
use crowdb_protocol::key::StreamMigrationProgressKey;

use super::*;

#[path = "../common/legacy_tail.rs"]
mod legacy_tail;

#[tokio::test]
async fn bounded_conversion_resumes_after_each_batch_and_retains_both_root_identities() {
    let server = TestMetadataServer::start().await;
    let chunks = MemoryStreamStore::new(1024 * 1024);
    let mut metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let (legacy, pages) = metadata_server::candidate(3, 7, 17);
    metadata
        .publish(None, legacy.clone(), pages.clone())
        .await
        .unwrap();
    let mut snapshot = metadata
        .begin_directory_migration(legacy.stream_name, 4, [9; 16])
        .await
        .unwrap();
    let original = snapshot.clone();
    let mut sequence = 0;
    let published = loop {
        match metadata
            .advance_directory_migration(&snapshot, &chunks, 3)
            .await
            .unwrap()
        {
            StreamMigrationStep::Complete(published) => break published,
            StreamMigrationStep::Progress {
                snapshot: next,
                progress,
            } => {
                sequence += 1;
                assert_eq!(progress.sequence, sequence);
                assert!(progress.root.unwrap().extent_count <= sequence * 3);
                assert!(metadata.load_directory_head(legacy.stream_name).await.is_err());
                if sequence == 1 {
                    assert!(matches!(
                        metadata.advance_directory_migration(&original, &chunks, 3).await,
                        Err(StreamError::WriteStalled)
                    ));
                }
                metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
                snapshot = metadata
                    .begin_directory_migration(legacy.stream_name, 4, [9; 16])
                    .await
                    .unwrap();
                assert_eq!(snapshot.revision(), next.revision());
                assert_eq!(
                    metadata
                        .load_directory_migration_progress(&snapshot)
                        .await
                        .unwrap()
                        .as_ref(),
                    Some(progress.as_ref())
                );
            }
        }
        assert!(sequence <= 7);
    };
    assert_eq!(published.head().published_tail, 17);
    assert_eq!(published.head().writer_epoch, 4);
    assert_eq!(published.head().generation, 8);
    assert!(published.head().active.is_none());
    let archived = metadata
        .load_directory_root(legacy.stream_name, original.marker().legacy)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(archived.published_tail, 17);
    assert_eq!(archived.root, published.head().root);
    let key = StreamMigrationProgressKey {
        stream_name: legacy.stream_name,
        source: original.marker().source,
    }
    .to_bytes();
    assert!(get(&server, &key).await.is_none());
    for page in pages {
        assert_eq!(
            metadata
                .load_extent_page(legacy.stream_name, 3, 7, page.page_index)
                .await
                .unwrap(),
            Some(page)
        );
    }
    let index =
        DirectoryIndex::new(legacy.stream_name, StreamPurpose::Stream, published.head().root).unwrap();
    let scan = index.scan(&metadata, 0..17, 17, 8).await.unwrap();
    assert_eq!(scan.locations.len(), 17);
}

#[tokio::test]
async fn conversion_seals_at_the_legacy_cursor_and_never_infers_an_uncommitted_suffix() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let name = Test::NAME;
    let fixture = legacy_tail::TestLegacyTail::new(&metadata, name).await;
    let chunks = &fixture.chunks;
    let mut snapshot = metadata
        .begin_directory_migration(name, 4, [9; 16])
        .await
        .unwrap();
    let mut corruption_checked = false;
    let published = loop {
        match metadata
            .advance_directory_migration(&snapshot, chunks, 1)
            .await
            .unwrap()
        {
            StreamMigrationStep::Complete(published) => break published,
            StreamMigrationStep::Progress {
                snapshot: next,
                progress,
            } => {
                snapshot = next;
                if matches!(progress.phase, StreamMigrationPhase::Tail { .. }) {
                    chunks
                        .flip_durable_byte(fixture.chunk_id, fixture.first_length + 14)
                        .await;
                    assert!(matches!(
                        metadata.advance_directory_migration(&snapshot, chunks, 1).await,
                        Err(StreamError::Corruption(_))
                    ));
                    assert_eq!(
                        metadata
                            .load_directory_migration_progress(&snapshot)
                            .await
                            .unwrap()
                            .as_ref(),
                        Some(progress.as_ref())
                    );
                    chunks
                        .flip_durable_byte(fixture.chunk_id, fixture.first_length + 14)
                        .await;
                    corruption_checked = true;
                }
            }
        }
    };
    assert!(corruption_checked);
    assert_eq!(published.head().published_tail, 9);
    let durable = chunks.durable_cursor(fixture.chunk_id, 3).await.unwrap();
    assert!(durable.sealed);
    assert_eq!(durable.offset, fixture.persisted);
    let archived = metadata
        .load_directory_root(name, snapshot.marker().legacy)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(archived.published_tail, 3);
    let old_index = DirectoryIndex::new(name, StreamPurpose::Stream, archived.root).unwrap();
    assert!(old_index.lookup(&metadata, 3).await.unwrap().is_none());
    let new_index = DirectoryIndex::new(name, StreamPurpose::Stream, published.head().root).unwrap();
    assert_eq!(
        new_index
            .lookup(&metadata, 8)
            .await
            .unwrap()
            .unwrap()
            .location
            .logical_length,
        6
    );
    assert!(new_index.lookup(&metadata, 9).await.unwrap().is_none());
}
