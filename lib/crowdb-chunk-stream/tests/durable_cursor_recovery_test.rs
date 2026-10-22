// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, CursorAdvance, StreamBinding, StreamBindingState,
    StreamChunkStore, StreamConfig, StreamError, StreamMetadataStore, StreamName, StreamPurpose,
};
use crowdb_protocol::frame::{encode_frames, FrameMagic};

#[tokio::test]
async fn durable_cursor_recovers_frames_without_a_new_directory_publication() {
    check_tail(true, false).await;
}

#[tokio::test]
async fn bytes_beyond_the_durable_cursor_do_not_become_recovered_data() {
    check_tail(false, false).await;
}

#[tokio::test]
async fn a_durable_cursor_does_not_authorize_corrupt_unpublished_frames() {
    check_tail(true, true).await;
}

async fn initial_stream() -> (Arc<MemoryStreamStore>, ChunkStream, StreamName) {
    let store = Arc::new(MemoryStreamStore::new(1024));
    let name = StreamName { high: 51, low: 61 };
    let config = StreamConfig::default();
    let writer = ChunkStream::create(
        StreamBinding {
            purpose: StreamPurpose::Stream,
            stream_name: name,
            metadata_group_id: 1,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("test".into()),
        },
        9,
        config.clone(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    writer.append(&[Bytes::from_static(b"old")]).await.unwrap();
    (store, writer, name)
}

async fn check_tail(advance: bool, corrupt: bool) {
    let (store, writer, name) = initial_stream().await;
    let published = store.load_current(name).await.unwrap().unwrap();
    let active = published.active.as_ref().unwrap();
    let publication_count = store.metadata_publish_count();
    let frames = encode_frames(FrameMagic::StreamV1, active.chunk_id, b"new", 1).unwrap();
    let mut staging = BytesMut::new();
    for frame in frames {
        staging.extend_from_slice(&frame);
    }
    let data = staging.freeze();
    let end = active.acknowledged_cursor + u64::try_from(data.len()).unwrap();
    store
        .write_mirrors_with_images(
            name,
            9,
            active.chunk_id,
            active.acknowledged_cursor,
            data.clone(),
            &[],
        )
        .await
        .unwrap();
    if advance {
        assert_eq!(
            store
                .advance_cursor(
                    name,
                    9,
                    active.chunk_id,
                    active.acknowledged_cursor,
                    end,
                    crc32fast::hash(&data)
                )
                .await
                .unwrap(),
            CursorAdvance::Committed
        );
    }
    if corrupt {
        store
            .flip_durable_byte(
                active.chunk_id,
                usize::try_from(active.acknowledged_cursor).unwrap()
                    + crowdb_protocol::frame::FRAME_HEADER_PREFIX_BYTES,
            )
            .await;
    }
    assert_eq!(store.metadata_publish_count(), publication_count);
    assert_eq!(store.load_current(name).await.unwrap().unwrap(), published);
    let observed = store.durable_cursor(active.chunk_id, 9).await.unwrap();
    assert_eq!(
        observed.offset,
        if advance { end } else { active.acknowledged_cursor }
    );
    assert_eq!(writer.tail(), 3);

    let reopened = ChunkStream::open(
        name,
        10,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store,
    )
    .await;
    if corrupt {
        assert!(matches!(reopened, Err(StreamError::Corruption(_))));
        return;
    }
    let reopened = reopened.unwrap();
    let expected = if advance { &b"oldnew"[..] } else { &b"old"[..] };
    assert_eq!(reopened.tail(), u64::try_from(expected.len()).unwrap());
    assert_eq!(reopened.read_at(0, expected.len()).await.unwrap(), expected);
    assert!(writer.append(&[Bytes::from_static(b"stale")]).await.is_err());
}
