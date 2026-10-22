// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/directory.rs"]
mod directory;

use crowdb_chunk_stream::{DirectoryIndex, StreamError, MAX_DIRECTORY_MUTATION_EXTENTS};
use crowdb_protocol::chunk_stream::{
    StreamDirectoryPage, StreamExtentLocation, StreamFrameFormat, StreamPurpose,
    MAX_STREAM_DIRECTORY_CHILDREN, MAX_STREAM_DIRECTORY_LOCATIONS,
};
use crowdb_protocol::frame::ChunkLocation;
use directory::{test_locations, TestDirectoryStore};

async fn history(store: &TestDirectoryStore, count: usize) -> DirectoryIndex {
    let mut index = store.empty();
    let mut offset = 0;
    while offset < count {
        let size = (count - offset).min(MAX_DIRECTORY_MUTATION_EXTENTS);
        let mutation = index
            .prepare_append(store, &test_locations(offset as u64, size))
            .await
            .unwrap();
        index = store.install(&mutation);
        offset += size;
    }
    index
}

#[tokio::test]
async fn checkpoints_copy_only_the_tail_path_at_one_sixteen_and_256_segments() {
    for segments in [1, 16, 256] {
        let store = TestDirectoryStore::new();
        let count = segments * MAX_STREAM_DIRECTORY_LOCATIONS;
        let index = history(&store, count).await;
        let old_root = index.root().unwrap();
        let old_ids = store.ids();
        store.reset_reads();
        let mutation = index
            .prepare_append(&store, &test_locations(count as u64, 1))
            .await
            .unwrap();
        assert!(store.read_count() <= old_root.level as usize + 1);
        assert!(mutation.pages().len() <= old_root.level as usize + 2);
        assert!(mutation
            .pages()
            .iter()
            .all(|record| !old_ids.contains(&record.reference().id)));
        assert_eq!(mutation.base(), Some(old_root));
        let next = store.install(&mutation);
        assert_eq!(next.root().unwrap().extent_count, count as u64 + 1);
        assert_eq!(
            index.lookup(&store, count as u64 - 1).await.unwrap(),
            Some(test_locations(count as u64 - 1, 1)[0])
        );
        assert_eq!(index.lookup(&store, count as u64).await.unwrap(), None);
        assert_eq!(
            next.lookup(&store, count as u64).await.unwrap(),
            Some(test_locations(count as u64, 1)[0])
        );
    }
}

#[tokio::test]
async fn unfinished_tail_is_replaced_without_rewriting_completed_leaves() {
    let store = TestDirectoryStore::new();
    let index = history(&store, 2 * MAX_STREAM_DIRECTORY_LOCATIONS + 7).await;
    let root = index.root().unwrap();
    let bytes = crowdb_chunk_stream::StreamDirectoryStore::load_directory_record(&store, store.name, root.id)
        .await
        .unwrap()
        .unwrap();
    let StreamDirectoryPage::Branch(before) = crowdb_protocol::chunk_stream::decode_stream_directory_page(
        &bytes,
        root,
        store.name,
        StreamPurpose::Stream,
    )
    .unwrap() else {
        panic!("expected branch")
    };
    let mutation = index
        .prepare_append(&store, &test_locations(root.end_logical, 2))
        .await
        .unwrap();
    assert_eq!(mutation.pages().len(), 2);
    let index = store.install(&mutation);
    let root = index.root().unwrap();
    let bytes = crowdb_chunk_stream::StreamDirectoryStore::load_directory_record(&store, store.name, root.id)
        .await
        .unwrap()
        .unwrap();
    let StreamDirectoryPage::Branch(after) = crowdb_protocol::chunk_stream::decode_stream_directory_page(
        &bytes,
        root,
        store.name,
        StreamPurpose::Stream,
    )
    .unwrap() else {
        panic!("expected branch")
    };
    assert_eq!(&before.children[..2], &after.children[..2]);
    assert_ne!(before.children[2], after.children[2]);
}

#[tokio::test]
async fn range_scans_are_bounded_and_resume_against_one_immutable_root() {
    let store = TestDirectoryStore::new();
    let index = history(&store, 16 * MAX_STREAM_DIRECTORY_LOCATIONS + 9).await;
    let end = index.root().unwrap().end_logical;
    let mut offset = 1019;
    let mut seen = Vec::new();
    while offset < end {
        store.reset_reads();
        let scan = index.scan(&store, offset..end, 701, 3).await.unwrap();
        assert!(scan.pages_read <= 3);
        assert_eq!(scan.pages_read, store.read_count());
        assert!(scan.locations.len() <= 701);
        assert!(!scan.locations.is_empty());
        seen.extend(scan.locations);
        offset = scan.next_logical.unwrap_or(end);
    }
    assert_eq!(seen, test_locations(1019, usize::try_from(end - 1019).unwrap()));
    assert!(index.scan(&store, 0..1, 0, 3).await.is_err());
    assert!(index.scan(&store, 0..1, 1, 0).await.is_err());
    assert!(index.scan(&store, 0..1, 1, 1).await.is_err());
}

#[tokio::test]
async fn trim_rewrites_only_left_path_and_preserves_old_root_reads() {
    let store = TestDirectoryStore::new();
    let index = history(&store, 256 * MAX_STREAM_DIRECTORY_LOCATIONS).await;
    let before = index.root().unwrap();
    store.reset_reads();
    let watermark = (129 * MAX_STREAM_DIRECTORY_LOCATIONS + 13) as u64;
    let mutation = index.prepare_trim(&store, watermark).await.unwrap();
    assert!(store.read_count() <= before.level as usize + 1);
    assert!(mutation.pages().len() <= before.level as usize + 1);
    let trimmed = store.install(&mutation);
    assert_eq!(trimmed.root().unwrap().first_logical, watermark);
    assert_eq!(trimmed.lookup(&store, watermark - 1).await.unwrap(), None);
    assert!(trimmed.lookup(&store, watermark).await.unwrap().is_some());
    assert!(index.lookup(&store, watermark - 1).await.unwrap().is_some());
    assert_eq!(index.root(), Some(before));
    let dropped = trimmed.prepare_trim(&store, before.end_logical).await.unwrap();
    assert!(dropped.index().root().is_none());
    assert!(dropped.pages().is_empty());
}

#[tokio::test]
async fn compact_groups_and_legacy_short_frames_keep_their_physical_boundaries() {
    let store = TestDirectoryStore::new();
    let mut large = test_locations(0, 1)[0];
    large.location.logical_length = 4 * 1024 * 1024;
    let mut next = large;
    next.logical_start = large.location.logical_length;
    next.location.frame_offset = large.end_physical().unwrap();
    next.location.logical_length = 17;
    let mutation = store
        .empty()
        .prepare_append(&store, &[large, next])
        .await
        .unwrap();
    assert_eq!(mutation.pages().len(), 1);
    let index = store.install(&mutation);
    for offset in [0, 65500, 1024 * 1024, next.logical_start - 1] {
        assert_eq!(index.lookup(&store, offset).await.unwrap(), Some(large));
    }
    assert_eq!(
        index.lookup(&store, next.logical_start + 7).await.unwrap(),
        Some(next)
    );
    let trimmed = index.prepare_trim(&store, 1).await.unwrap();
    assert!(trimmed.pages().is_empty());
    assert_eq!(trimmed.index().root(), index.root());
    let mut legacy = large;
    legacy.format = StreamFrameFormat::LegacyV1;
    legacy.location.logical_length = 3;
    let mut other = legacy;
    other.logical_start = 3;
    other.location.frame_offset = legacy.end_physical().unwrap();
    other.location.logical_length = 5;
    let mutation = store
        .empty()
        .prepare_append(&store, &[legacy, other])
        .await
        .unwrap();
    let index = store.install(&mutation);
    assert_eq!(index.root().unwrap().extent_count, 2);
    assert_eq!(index.lookup(&store, 4).await.unwrap(), Some(other));
}

#[tokio::test]
async fn cross_leaf_gaps_format_changes_and_bad_pages_fail_closed() {
    let store = TestDirectoryStore::new();
    let chunk = test_locations(0, 1)[0].location.chunk_id;
    let locations: Vec<_> = (0..MAX_STREAM_DIRECTORY_LOCATIONS)
        .map(|offset| StreamExtentLocation {
            logical_start: offset as u64,
            location: ChunkLocation {
                chunk_id: chunk,
                frame_offset: offset as u64 * 48,
                logical_length: 1,
            },
            format: StreamFrameFormat::GroupedV2,
        })
        .collect();
    let mutation = store.empty().prepare_append(&store, &locations).await.unwrap();
    let index = store.install(&mutation);
    let mut next = locations[0];
    next.logical_start = locations.len() as u64;
    next.location.frame_offset = locations.len() as u64 * 48;
    assert!(index.prepare_append(&store, &[next]).await.is_ok());
    next.location.frame_offset += 1;
    assert!(index.prepare_append(&store, &[next]).await.is_err());
    next.location.frame_offset -= 1;
    next.format = StreamFrameFormat::LegacyV1;
    assert!(index.prepare_append(&store, &[next]).await.is_err());
    let root = index.root().unwrap();
    store.corrupt(root.id);
    assert!(matches!(
        index.lookup(&store, 0).await,
        Err(StreamError::Corruption(_))
    ));
    store.remove(root.id);
    assert!(matches!(
        index.lookup(&store, 0).await,
        Err(StreamError::ReadUnavailable(_))
    ));
}

#[tokio::test]
async fn mutation_budgets_and_empty_changes_do_not_read_or_copy_history() {
    let store = TestDirectoryStore::new();
    let index = history(
        &store,
        MAX_STREAM_DIRECTORY_LOCATIONS * MAX_STREAM_DIRECTORY_CHILDREN,
    )
    .await;
    store.reset_reads();
    let empty = index.prepare_append(&store, &[]).await.unwrap();
    assert!(empty.pages().is_empty());
    assert_eq!(empty.index().root(), index.root());
    let trim = index.prepare_trim(&store, 0).await.unwrap();
    assert!(trim.pages().is_empty());
    assert_eq!(store.read_count(), 0);
    assert!(matches!(
        index
            .prepare_append(
                &store,
                &test_locations(
                    index.root().unwrap().end_logical,
                    MAX_DIRECTORY_MUTATION_EXTENTS + 1
                )
            )
            .await,
        Err(StreamError::Backpressure)
    ));
    assert_eq!(store.read_count(), 0);
}
