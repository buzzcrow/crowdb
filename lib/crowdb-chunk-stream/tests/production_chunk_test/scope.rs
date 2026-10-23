// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
async fn allocation_rejects_fewer_than_the_configured_copies() {
    for copies in [3, 5] {
        let allocator = Arc::new(Allocator::new());
        allocator.allocation_copy_limit.store(2, Ordering::Release);
        let disks = Arc::new(Disks::default());
        let store = ProductionStreamChunkStore::new_with_mirror_copies(
            allocator,
            disks.clone(),
            30_000,
            ChunkReadPolicy::default(),
            copies,
        )
        .unwrap();
        let result = store
            .allocate_mirrored(
                StreamName { high: 41, low: 43 },
                9,
                crowdb_chunk_stream::StreamPurpose::Stream,
            )
            .await;
        assert!(
            matches!(result, Err(crowdb_chunk_stream::StreamError::ReadUnavailable(_))),
            "{copies}-copy allocation accepted an incomplete replica set: {result:?}"
        );
        assert_eq!(disks.fsyncs.load(Ordering::Acquire), 0);
    }
}

#[tokio::test]
async fn another_stream_cannot_write_or_checkpoint_an_attributed_chunk() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let store = ProductionStreamChunkStore::new(allocator, disks.clone(), 30_000, ChunkReadPolicy::default())
        .unwrap();
    let name = StreamName { high: 81, low: 83 };
    let other = StreamName { high: 81, low: 84 };
    let active = store
        .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Stream)
        .await
        .unwrap();
    let data = Bytes::from(
        crowdb_protocol::frame::encode_append_group_views(
            active.chunk_id,
            0,
            &Bytes::from_static(b"retained"),
            1,
        )
        .unwrap()
        .concat(),
    );
    let end = data.len() as u64;
    let images = [MirrorStripImage {
        block_offset: 0,
        data: data.clone(),
        full_image: data.clone(),
    }];
    assert!(matches!(
        store
            .write_mirrors(
                other,
                9,
                active.chunk_id,
                crowdb_chunk_stream::StreamMirrorWrite {
                    physical_offset: 0,
                    complete_end: end,
                    images: &images
                }
            )
            .await,
        Err(crowdb_chunk_stream::StreamError::StaleWriter)
    ));
    assert_eq!(disks.fsyncs.load(Ordering::Acquire), 2);
    store
        .write_mirrors(
            name,
            9,
            active.chunk_id,
            crowdb_chunk_stream::StreamMirrorWrite {
                physical_offset: 0,
                complete_end: end,
                images: &images,
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        store.advance_cursor(other, 9, active.chunk_id, 0, end, 7).await,
        Err(crowdb_chunk_stream::StreamError::StaleWriter)
    ));
    assert_eq!(store.durable_cursor(active.chunk_id, 9).await.unwrap().offset, 0);
    assert_eq!(
        store
            .advance_cursor(name, 9, active.chunk_id, 0, end, 7)
            .await
            .unwrap(),
        CursorAdvance::Committed
    );
    assert_eq!(disks.fsyncs.load(Ordering::Acquire), 4);
}
