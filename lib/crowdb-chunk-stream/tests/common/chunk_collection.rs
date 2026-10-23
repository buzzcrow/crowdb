// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{indexed_metadata::TestIndexedMetadata as Test, initial, successor};
use bytes::Bytes;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, DirectoryChunkRelease, DirectoryCollectionStep, DirectoryHeadSnapshot,
    DirectoryIndex, DirectoryMetadataStore, StreamChunkStore,
};
use crowdb_protocol::chunk_stream::StreamSealedChunkSpan;
use crowdb_protocol::frame::encode_append_group_views;

pub struct TestChunkCollection;

impl TestChunkCollection {
    pub async fn setup(
        store: &MemoryStreamStore,
        directory: &DirectoryMetadataStore,
        seal: bool,
    ) -> (
        DirectoryHeadSnapshot,
        DirectoryHeadSnapshot,
        StreamSealedChunkSpan,
    ) {
        let active = store
            .allocate_mirrored(Test::NAME, 1, Test::empty().purpose())
            .await
            .unwrap();
        assert_eq!(active.chunk_id, Test::locations(0, 1)[0].location.chunk_id);
        let first_bytes = Bytes::from(
            encode_append_group_views(active.chunk_id, 0, &Bytes::from_static(b"a"), 1)
                .unwrap()
                .concat(),
        );
        let second_bytes = Bytes::from(
            encode_append_group_views(active.chunk_id, 48, &Bytes::from_static(b"b"), 1)
                .unwrap()
                .concat(),
        );
        let data = Bytes::from([first_bytes.as_ref(), second_bytes.as_ref()].concat());
        store
            .write_groups_for_tests(Test::NAME, 1, active.chunk_id, 0, data)
            .await
            .unwrap();
        store
            .advance_cursor(
                Test::NAME,
                1,
                active.chunk_id,
                0,
                48,
                crc32fast::hash(&first_bytes),
            )
            .await
            .unwrap();
        let first = initial(directory).await;
        if seal {
            store.seal(active.chunk_id, 1, 96).await.unwrap();
        }
        let next = store
            .allocate_mirrored(Test::NAME, 1, Test::empty().purpose())
            .await
            .unwrap();
        assert_eq!(next.chunk_id, Test::locations(1, 1)[0].location.chunk_id);
        let (mutation, head, span) = successor(directory, &first).await;
        let mutation = mutation.with_sealed_chunk_span(span).unwrap();
        let current = directory
            .publish_directory(Some(&first), head, &mutation)
            .await
            .unwrap();
        (first, current, span)
    }

    pub async fn trim(
        directory: &DirectoryMetadataStore,
        current: &DirectoryHeadSnapshot,
        watermark: u64,
    ) -> DirectoryHeadSnapshot {
        let index = DirectoryIndex::new(Test::NAME, current.head().purpose, current.head().root).unwrap();
        let mutation = index.prepare_trim(directory, watermark).await.unwrap();
        let mut head = Test::head(&mutation, Some(current.head()));
        head.trim_offset = watermark;
        head.published_tail = current.head().published_tail;
        head.active = current.head().active;
        directory
            .publish_directory(Some(current), head, &mutation)
            .await
            .unwrap()
    }

    pub async fn prove(
        directory: &DirectoryMetadataStore,
        current: &DirectoryHeadSnapshot,
        span: StreamSealedChunkSpan,
    ) -> Option<DirectoryChunkRelease> {
        let mut cursor = directory
            .begin_directory_chunk_collection(current, span.chunk_id)
            .await
            .unwrap()
            .unwrap();
        for _ in 0..32 {
            match directory.advance_directory_collection(cursor, 1).await.unwrap() {
                DirectoryCollectionStep::Pending(next) => cursor = next,
                DirectoryCollectionStep::Retained => return None,
                DirectoryCollectionStep::ChunkRelease(ticket) => return Some(ticket),
                DirectoryCollectionStep::PageDeleted => panic!("chunk proof deleted a page"),
            }
        }
        panic!("bounded chunk proof made no progress")
    }
}
