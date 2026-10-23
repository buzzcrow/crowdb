// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
async fn configured_replica_count_cannot_silently_accept_a_two_copy_layout() {
    for copies in [3, 5] {
        let allocator = Arc::new(Allocator::new());
        let disks = Arc::new(Disks::default());
        let store = ProductionStreamChunkStore::new_with_mirror_copies(
            allocator.clone(),
            disks.clone(),
            30_000,
            ChunkReadPolicy::default(),
            copies,
        )
        .unwrap();
        let name = StreamName { high: 41, low: 43 };
        let active = store
            .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Stream)
            .await
            .unwrap();
        {
            let mut guard = allocator.chunk.lock().unwrap();
            let chunk = guard.as_mut().unwrap();
            let Some(Strip::MirrorStrip(mirror)) = &mut chunk.strips[0].strip else {
                panic!("mirror missing")
            };
            mirror.segments.truncate(2);
            chunk.modify_ts += 1;
        }
        store.durable_cursor(active.chunk_id, 9).await.unwrap();
        let data = Bytes::from(
            crowdb_protocol::frame::encode_append_group_views(
                active.chunk_id,
                0,
                &Bytes::from_static(b"record"),
                1,
            )
            .unwrap()
            .concat(),
        );
        let result = store
            .write_mirrors_with_images(
                name,
                9,
                active.chunk_id,
                0,
                data.clone(),
                &[MirrorStripImage {
                    block_offset: 0,
                    data: data.clone(),
                    full_image: data,
                }],
            )
            .await;
        assert!(
            matches!(&result, Err(crowdb_chunk_stream::StreamError::ReadUnavailable(_))),
            "configured {copies} copies: result={result:?}, synchronized copies={}",
            disks.fsyncs.load(Ordering::Relaxed)
        );
        assert_eq!(disks.fsyncs.load(Ordering::Relaxed), 0);
        assert_eq!(store.durable_cursor(active.chunk_id, 9).await.unwrap().offset, 0);
    }
}

#[tokio::test]
async fn grouped_progress_survives_checkpoint_query_renew_growth_and_mirror_repair() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let store = ProductionStreamChunkStore::new(
        allocator.clone(),
        disks.clone(),
        30_000,
        ChunkReadPolicy::default(),
    )
    .unwrap();
    let name = StreamName { high: 7, low: 9 };
    let active = store
        .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Stream)
        .await
        .unwrap();
    let mut strip = TestGroupedStrip::default();
    let first = strip.append(&store, name, 9, active.chunk_id, b"first").await;
    assert_eq!(store.durable_cursor(active.chunk_id, 9).await.unwrap().offset, 0);
    store.renew_liveness(active.chunk_id, 9).await.unwrap();
    let old_layout = allocator.chunk.lock().unwrap().clone();
    let grown = store
        .grow_mirrored(name, 9, active.chunk_id, active.capacity + 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(grown.acknowledged_cursor, first);
    assert_eq!(grown.capacity, 2 * 1024 * 1024);
    *allocator.stale_query_once.lock().unwrap() = old_layout;
    store.durable_cursor(active.chunk_id, 9).await.unwrap();
    assert_eq!(
        store.current_capacity(active.chunk_id).await.unwrap(),
        2 * 1024 * 1024
    );
    disks.fail_disk_once.store(1, Ordering::Release);
    let second = strip.append(&store, name, 9, active.chunk_id, b"second").await;
    assert!(second > first);
    assert_eq!(allocator.replacements.load(Ordering::Acquire), 1);
    assert_eq!(store.durable_cursor(active.chunk_id, 9).await.unwrap().offset, 0);
    let end = strip.append(&store, name, 9, active.chunk_id, b"third").await;
    assert_eq!(
        allocator
            .chunk
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .acknowledged_cursor,
        0
    );
    store.seal(active.chunk_id, 9, end).await.unwrap();
    let durable = store.durable_cursor(active.chunk_id, 9).await.unwrap();
    assert_eq!(durable.offset, end);
    assert!(durable.sealed);
}

#[tokio::test]
async fn a_grouped_chunk_rejects_removed_magic_after_synchronized_progress() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let store = ProductionStreamChunkStore::new(
        allocator.clone(),
        disks.clone(),
        30_000,
        ChunkReadPolicy::default(),
    )
    .unwrap();
    let name = StreamName { high: 71, low: 73 };
    let active = store
        .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Stream)
        .await
        .unwrap();
    let mut strip = TestGroupedStrip::default();
    let offset = strip.append(&store, name, 9, active.chunk_id, b"first").await;
    let mut removed = crowdb_protocol::frame::encode_append_group_views(
        active.chunk_id,
        offset,
        &Bytes::from_static(b"unsupported"),
        1,
    )
    .unwrap()
    .concat();
    removed[..2].copy_from_slice(&0x0301_u16.to_le_bytes());
    let legacy = Bytes::from(removed);
    assert!(matches!(
        store
            .write_mirrors_with_images(
                name,
                9,
                active.chunk_id,
                offset,
                legacy.clone(),
                &[MirrorStripImage {
                    block_offset: offset,
                    data: legacy.clone(),
                    full_image: legacy
                }]
            )
            .await,
        Err(crowdb_chunk_stream::StreamError::InvalidRequest(_))
    ));
    assert_eq!(disks.fsyncs.load(Ordering::Acquire), 2);
    assert_eq!(store.durable_cursor(active.chunk_id, 9).await.unwrap().offset, 0);
}

#[tokio::test]
async fn a_cold_adapter_cannot_overwrite_the_previous_owners_uncheckpointed_group() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let make = || {
        ProductionStreamChunkStore::new(
            allocator.clone(),
            disks.clone(),
            30_000,
            ChunkReadPolicy::default(),
        )
        .unwrap()
    };
    let name = StreamName { high: 17, low: 19 };
    let original = make();
    let active = original
        .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Stream)
        .await
        .unwrap();
    let mut strip = TestGroupedStrip::default();
    strip
        .append(&original, name, 9, active.chunk_id, b"retained")
        .await;
    assert_eq!(
        original.durable_cursor(active.chunk_id, 9).await.unwrap().offset,
        0
    );
    let retained = disks.bytes.lock().unwrap().clone();
    let cold = make();
    let views = crowdb_protocol::frame::encode_append_group_views(
        active.chunk_id,
        0,
        &Bytes::from_static(b"replacement"),
        1,
    )
    .unwrap();
    let bytes = Bytes::from(views.concat());
    assert!(matches!(
        cold.write_mirrors_with_images(
            name,
            9,
            active.chunk_id,
            0,
            bytes.clone(),
            &[MirrorStripImage {
                block_offset: 0,
                data: bytes.clone(),
                full_image: bytes
            }]
        )
        .await,
        Err(crowdb_chunk_stream::StreamError::InvalidRequest(_))
    ));
    assert_eq!(*disks.bytes.lock().unwrap(), retained);
    assert_eq!(disks.fsyncs.load(Ordering::Acquire), 2);
}
