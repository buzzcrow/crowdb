// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_stream::{
    DirectoryCollectionStep, DirectoryHeadSnapshot, DirectoryIndex, DirectoryMetadataStore, DirectoryRecord,
};
use crowdb_protocol::chunk_stream::{StreamDirectoryPageId, StreamRootPin, StreamRootPinId};

use crate::indexed_metadata::TestIndexedMetadata as Test;

pub struct TestCollectionState {
    pub first: DirectoryHeadSnapshot,
    pub current: DirectoryHeadSnapshot,
    pub orphan: StreamDirectoryPageId,
    pub shared: StreamDirectoryPageId,
    pub old_leaf: StreamDirectoryPageId,
}

impl TestCollectionState {
    pub async fn new(directory: &DirectoryMetadataStore, generations: usize) -> Self {
        let mutation = Test::empty()
            .prepare_append(directory, &Test::locations(0, 1025))
            .await
            .unwrap();
        let shared = mutation
            .pages()
            .iter()
            .map(DirectoryRecord::reference)
            .find(|page| page.level == 0 && page.end_logical == 1024)
            .unwrap()
            .id;
        let old_leaf = mutation
            .pages()
            .iter()
            .map(DirectoryRecord::reference)
            .find(|page| page.level == 0 && page.first_logical == 1024)
            .unwrap()
            .id;
        let first = directory
            .publish_directory(None, Test::head(&mutation, None), &mutation)
            .await
            .unwrap();
        let mut current = first.clone();
        let mut index = mutation.index().clone();
        for _ in 1..generations {
            let offset = index.root().unwrap().end_logical;
            let mutation = index
                .prepare_append(directory, &Test::locations(offset, 1))
                .await
                .unwrap();
            current = directory
                .publish_directory(
                    Some(&current),
                    Test::head(&mutation, Some(current.head())),
                    &mutation,
                )
                .await
                .unwrap();
            index = mutation.index().clone();
        }
        let orphan = first.head().root.unwrap().id;
        Self {
            first,
            current,
            orphan,
            shared,
            old_leaf,
        }
    }

    pub fn pin(&self) -> StreamRootPin {
        StreamRootPin {
            id: StreamRootPinId([1; 16]),
            head: self.first.head().clone(),
            needed_offset: 0,
            captured_tail: self.first.head().published_tail,
            expires_at_ms: None,
        }
    }

    pub async fn retire_first(&self, directory: &DirectoryMetadataStore) {
        assert_eq!(
            directory
                .retire_directory_roots_before(&self.current, 2, 1)
                .await
                .unwrap(),
            1
        );
    }

    pub async fn collect(
        &self,
        directory: &DirectoryMetadataStore,
        page: StreamDirectoryPageId,
    ) -> (bool, usize) {
        let mut cursor = directory
            .begin_directory_page_collection(&self.current, page)
            .await
            .unwrap()
            .unwrap();
        for observations in 1..=64 {
            match directory.advance_directory_collection(cursor, 1).await.unwrap() {
                DirectoryCollectionStep::Pending(next) => cursor = next,
                DirectoryCollectionStep::Retained => return (false, observations),
                DirectoryCollectionStep::PageDeleted => return (true, observations),
                DirectoryCollectionStep::ChunkRelease(_) => panic!("page proof returned a chunk release"),
            }
        }
        panic!("bounded page proof did not make progress")
    }

    pub fn current_index(&self) -> DirectoryIndex {
        DirectoryIndex::new(Test::NAME, self.current.head().purpose, self.current.head().root).unwrap()
    }
}
