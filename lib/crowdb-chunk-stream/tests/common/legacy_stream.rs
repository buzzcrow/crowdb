// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    CursorAdvance, StreamBinding, StreamBindingState, StreamChunkStore, StreamExtentPage,
    StreamExtentPageFence, StreamManifest, StreamMetadataStore, StreamName, StreamPurpose, StreamRegistry,
};
use crowdb_protocol::frame::{encode_frame, encode_frames, FrameMagic};
use std::sync::Arc;

pub struct TestLegacyStream {
    pub store: Arc<MemoryStreamStore>,
    pub name: StreamName,
    pub chunk: crowdb_chunk_stream::ChunkId,
    pub cursor: u64,
}

impl TestLegacyStream {
    pub async fn new() -> Self {
        let store = Arc::new(MemoryStreamStore::new(128 * 1024));
        let name = StreamName { high: 1, low: 77 };
        store
            .create(StreamBinding {
                stream_name: name,
                metadata_group_id: 7,
                state: StreamBindingState::Active,
                binding_generation: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        let mut active = store
            .allocate_mirrored(name, 9, StreamPurpose::Stream)
            .await
            .unwrap();
        let chunk = active.chunk_id;
        let frame = Bytes::from(encode_frame(FrameMagic::StreamV1, chunk, b"old", 42).unwrap());
        let cursor = frame.len() as u64;
        store
            .write_mirrors_with_images(name, 9, chunk, 0, frame.clone(), &[])
            .await
            .unwrap();
        assert_eq!(
            store
                .advance_cursor(name, 9, chunk, 0, cursor, crc32fast::hash(&frame))
                .await
                .unwrap(),
            CursorAdvance::Committed
        );
        active.physical_start = cursor;
        active.acknowledged_cursor = cursor;
        active.logical_start = 3;
        store
            .publish(
                None,
                StreamManifest {
                    stream_name: name,
                    metadata_group_id: 7,
                    writer_epoch: 9,
                    generation: 1,
                    sealed_tail: 3,
                    active: Some(active),
                    extent_pages: vec![StreamExtentPageFence {
                        page_index: 0,
                        first_logical: 0,
                        end_logical: 3,
                    }],
                    ..Default::default()
                },
                vec![StreamExtentPage {
                    stream_name: name,
                    writer_epoch: 9,
                    generation: 1,
                    page_index: 0,
                    chunk_ids: vec![chunk],
                    logical_offsets: vec![0, 3],
                    physical_offsets: vec![0],
                    frame_lengths: vec![u32::try_from(cursor).unwrap()],
                }],
            )
            .await
            .unwrap();
        // These CRC-valid frames were written without the legacy durable-cursor
        // transition. They must never be inferred as acknowledged appends.
        let suffix = Bytes::from(
            encode_frames(FrameMagic::StreamV1, chunk, &vec![0x51; 70 * 1024], 43)
                .unwrap()
                .concat(),
        );
        store
            .write_mirrors_with_images(name, 9, chunk, cursor, suffix, &[])
            .await
            .unwrap();
        Self {
            store,
            name,
            chunk,
            cursor,
        }
    }
}
