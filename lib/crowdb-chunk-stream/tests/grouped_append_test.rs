// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamBinding, StreamBindingState, StreamConfig,
    StreamMetadataStore, StreamName, StreamPurpose,
};
use crowdb_protocol::frame::{FrameLayout, MAX_FRAME_GROUP_PAYLOAD_BYTES};

#[tokio::test]
async fn every_append_has_one_location_and_subranges_read_only_intersecting_frames() {
    let store = Arc::new(MemoryStreamStore::new(8 * 1024 * 1024));
    let name = StreamName::generate();
    let stream = ChunkStream::create(
        StreamBinding {
            purpose: StreamPurpose::Stream,
            stream_name: name,
            metadata_group_id: 1,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("test".into()),
        },
        1,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    let sizes = [
        1,
        MAX_FRAME_GROUP_PAYLOAD_BYTES,
        MAX_FRAME_GROUP_PAYLOAD_BYTES + 1,
        1024 * 1024,
        4 * 1024 * 1024,
    ];
    let mut physical = 0_u64;
    for size in sizes {
        let payload = Bytes::from(
            (0..size)
                .map(|index| u8::try_from(index % 251).unwrap())
                .collect::<Vec<_>>(),
        );
        let range = stream.append(std::slice::from_ref(&payload)).await.unwrap();
        stream.checkpoint().await.unwrap();
        let manifest = store.load_current(name).await.unwrap().unwrap();
        let active = manifest.active.as_ref().unwrap();
        physical += FrameLayout::StreamGroupV2.physical_length(size as u64).unwrap();
        assert_eq!(active.acknowledged_cursor, physical);
        assert_eq!(stream.metrics().physical_append_bytes, physical);
        for offset in [0, size / 2, size - 1] {
            let count = (size - offset).min(101);
            let before = stream.metrics().physical_read_requests;
            assert_eq!(
                stream.read_at(range.begin + offset as u64, count).await.unwrap(),
                payload.slice(offset..offset + count)
            );
            let frames = (offset + count - 1) / MAX_FRAME_GROUP_PAYLOAD_BYTES
                - offset / MAX_FRAME_GROUP_PAYLOAD_BYTES
                + 1;
            assert_eq!(stream.metrics().physical_read_requests - before, frames as u64);
        }
        if size > MAX_FRAME_GROUP_PAYLOAD_BYTES {
            let offset = MAX_FRAME_GROUP_PAYLOAD_BYTES - 3;
            let count = (size - offset).min(13);
            assert_eq!(
                stream.read_at(range.begin + offset as u64, count).await.unwrap(),
                payload.slice(offset..offset + count)
            );
        }
    }
    let manifest = store.load_current(name).await.unwrap().unwrap();
    let mut locations = 0;
    for fence in &manifest.extent_pages {
        let page = store
            .load_extent_page(name, 1, manifest.generation, fence.page_index)
            .await
            .unwrap()
            .unwrap();
        locations += page.chunk_ids.len();
    }
    assert_eq!(
        locations,
        sizes.len(),
        "frame count cannot multiply append locations"
    );
}
