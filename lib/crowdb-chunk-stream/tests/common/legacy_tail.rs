// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, CursorAdvance, KvStreamMetadataStore, StreamChunkStore, StreamMetadataStore,
};
use crowdb_protocol::chunk_stream::{
    ActiveChunkDescriptor, LegacyStreamExtentPage, LegacyStreamManifest, StreamExtentPageFence, StreamName,
    StreamPurpose,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{encode_frame, FrameMagic};

pub struct TestLegacyTail {
    pub chunks: MemoryStreamStore,
    pub chunk_id: ChunkId,
    pub first_length: usize,
    pub persisted: u64,
}

impl TestLegacyTail {
    pub async fn new(metadata: &KvStreamMetadataStore, name: StreamName) -> Self {
        let chunks = MemoryStreamStore::new(1024 * 1024);
        let allocated = chunks
            .allocate_mirrored(name, 3, StreamPurpose::Stream)
            .await
            .unwrap();
        let first = encode_frame(FrameMagic::StreamV1, allocated.chunk_id, b"ack", 1).unwrap();
        let second = encode_frame(FrameMagic::StreamV1, allocated.chunk_id, b"cursor", 2).unwrap();
        let third = encode_frame(FrameMagic::StreamV1, allocated.chunk_id, b"suffix", 3).unwrap();
        let persisted = (first.len() + second.len()) as u64;
        let data = Bytes::from([first.as_slice(), second.as_slice(), third.as_slice()].concat());
        let checksum = crc32fast::hash(&data[..usize::try_from(persisted).unwrap()]);
        chunks
            .write_mirrors_with_images(name, 3, allocated.chunk_id, 0, data, &[])
            .await
            .unwrap();
        assert_eq!(
            chunks
                .advance_cursor(name, 3, allocated.chunk_id, 0, persisted, checksum)
                .await
                .unwrap(),
            CursorAdvance::Committed
        );
        let page = LegacyStreamExtentPage {
            stream_name: name,
            writer_epoch: 3,
            generation: 7,
            page_index: 11,
            chunk_ids: vec![allocated.chunk_id],
            logical_offsets: vec![0, 3],
            physical_offsets: vec![0],
            frame_lengths: vec![u32::try_from(first.len()).unwrap()],
        };
        let legacy = LegacyStreamManifest {
            stream_name: name,
            metadata_group_id: 1,
            writer_epoch: 3,
            generation: 7,
            sealed_tail: 3,
            active: Some(ActiveChunkDescriptor {
                physical_start: first.len() as u64,
                acknowledged_cursor: first.len() as u64,
                logical_start: 3,
                ..allocated.clone()
            }),
            extent_pages: vec![StreamExtentPageFence {
                page_index: 11,
                first_logical: 0,
                end_logical: 3,
            }],
            ..LegacyStreamManifest::default()
        };
        metadata.publish(None, legacy, vec![page]).await.unwrap();
        Self {
            chunks,
            chunk_id: allocated.chunk_id,
            first_length: first.len(),
            persisted,
        }
    }
}
