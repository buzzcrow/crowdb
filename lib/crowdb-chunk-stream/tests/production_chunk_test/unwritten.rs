// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crowdb_chunk_client::{select_frame_prefix, MirroredFrameReaders};
use crowdb_protocol::frame::{encode_append_group_views, MAX_FRAME_GROUP_PAYLOAD_BYTES};
use tokio::time::{Duration, Instant};

fn prepared_group(chunk: ChunkId, payload: &Bytes, padding: usize) -> (u64, MirrorStripImage) {
    let mut data = encode_append_group_views(chunk, 0, payload, 1).unwrap().concat();
    let complete_end = data.len() as u64;
    data.resize(data.len() + padding, 0);
    let full_image = Bytes::from(data);
    (
        complete_end,
        MirrorStripImage {
            block_offset: 0,
            data: full_image.clone(),
            full_image,
        },
    )
}

async fn observed_end(allocator: &Allocator, disks: &Arc<Disks>, name: StreamName) -> u64 {
    let chunk = Arc::new(allocator.chunk.lock().unwrap().clone().unwrap());
    let deadline = Instant::now() + Duration::from_secs(5);
    let readers = MirroredFrameReaders::new(chunk, name, disks.clone(), deadline).unwrap();
    let config = readers.scan_config(0, 0, 0, 64 * 1024 * 1024).unwrap();
    select_frame_prefix(readers.readers(), config, 1024 * 1024, deadline)
        .await
        .unwrap()
        .observation()
        .summary()
        .physical_end
}

#[tokio::test]
async fn recycled_bytes_stop_at_the_synchronized_unwritten_header() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    for disk in [1, 2] {
        disks.bytes.lock().unwrap().insert(disk, vec![0xcc; 1024 * 1024]);
    }
    let store = ProductionStreamChunkStore::new(
        allocator.clone(),
        disks.clone(),
        30_000,
        ChunkReadPolicy::default(),
    )
    .unwrap();
    let name = StreamName { high: 101, low: 103 };
    let active = store
        .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Stream)
        .await
        .unwrap();
    assert_eq!(observed_end(&allocator, &disks, name).await, 0);
    {
        let media = disks.bytes.lock().unwrap();
        assert_eq!(&media[&1][..14], &[0; 14]);
        assert!(media[&1].iter().all(|byte| *byte == 0));
    }
    let mut strip = TestGroupedStrip::default();
    let end = strip.append(&store, name, 9, active.chunk_id, b"retained").await;
    assert_eq!(store.durable_cursor(active.chunk_id, 9).await.unwrap().offset, 0);
    assert_eq!(observed_end(&allocator, &disks, name).await, end);
}

#[tokio::test]
async fn bytes_beyond_the_complete_range_are_rejected_before_consuming_the_fresh_writer() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let store = ProductionStreamChunkStore::new(allocator, disks.clone(), 30_000, ChunkReadPolicy::default())
        .unwrap();
    let name = StreamName { high: 107, low: 109 };
    let active = store
        .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Stream)
        .await
        .unwrap();
    let payload = Bytes::from_static(b"retained");
    for padding in [1, 14] {
        let (end, mut image) = prepared_group(active.chunk_id, &payload, padding);
        if padding > 0 {
            let mut bytes = image.full_image.to_vec();
            *bytes.last_mut().unwrap() = 1;
            image.full_image = Bytes::from(bytes);
            image.data = image.full_image.clone();
        }
        let result = store
            .write_mirrors(
                name,
                9,
                active.chunk_id,
                crowdb_chunk_stream::StreamMirrorWrite {
                    physical_offset: 0,
                    complete_end: end,
                    images: &[image],
                },
            )
            .await;
        assert!(
            matches!(result, Err(crowdb_chunk_stream::StreamError::InvalidRequest(_))),
            "{result:?}"
        );
        assert_eq!(disks.fsyncs.load(Ordering::Acquire), 2);
    }
    let mut strip = TestGroupedStrip::default();
    strip.append(&store, name, 9, active.chunk_id, b"retained").await;
    assert_eq!(disks.fsyncs.load(Ordering::Acquire), 4);
}

#[tokio::test]
async fn exact_hard_capacity_needs_no_extra_header() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let store = ProductionStreamChunkStore::new_with_mirror_copies_and_capacity(
        allocator.clone(),
        disks.clone(),
        30_000,
        ChunkReadPolicy::default(),
        2,
        1024 * 1024,
    )
    .unwrap();
    let name = StreamName { high: 113, low: 127 };
    let active = store
        .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Stream)
        .await
        .unwrap();
    let (end, image) = prepared_group(
        active.chunk_id,
        &Bytes::from(vec![0x51; 16 * MAX_FRAME_GROUP_PAYLOAD_BYTES]),
        0,
    );
    assert_eq!(end, active.capacity);
    store
        .write_mirrors(
            name,
            9,
            active.chunk_id,
            crowdb_chunk_stream::StreamMirrorWrite {
                physical_offset: 0,
                complete_end: end,
                images: &[image],
            },
        )
        .await
        .unwrap();
    assert_eq!(observed_end(&allocator, &disks, name).await, end);
}

#[tokio::test]
async fn cross_strip_tail_header_survives_later_growth_and_the_next_append() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    for disk in [1, 2, 11, 12, 21, 22] {
        disks.bytes.lock().unwrap().insert(disk, vec![0xcc; 1024 * 1024]);
    }
    let chunks = Arc::new(
        ProductionStreamChunkStore::new(
            allocator.clone(),
            disks.clone(),
            30_000,
            ChunkReadPolicy::default(),
        )
        .unwrap(),
    );
    let metadata = Arc::new(MemoryStreamStore::new(1));
    let name = StreamName { high: 139, low: 149 };
    let stream = ChunkStream::create(
        StreamBinding {
            purpose: crowdb_chunk_stream::StreamPurpose::Stream,
            stream_name: name,
            metadata_group_id: 1,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("test".into()),
        },
        9,
        StreamConfig::default(),
        metadata.clone(),
        metadata.clone(),
        chunks.clone(),
    )
    .await
    .unwrap();
    let prefix = Bytes::from(vec![0x51; 16 * MAX_FRAME_GROUP_PAYLOAD_BYTES - 7]);
    let range = stream.append(std::slice::from_ref(&prefix)).await.unwrap();
    let chunk_id = range.chunk_id.unwrap();
    let complete_end = 1024 * 1024 - 7;
    assert_eq!(
        chunks.durable_cursor(chunk_id, 9).await.unwrap().offset,
        complete_end
    );
    assert_eq!(chunks.current_capacity(chunk_id).await.unwrap(), 8 * 1024 * 1024);
    assert_eq!(observed_end(&allocator, &disks, name).await, complete_end);
    let publications = metadata.metadata_publish_count();
    chunks
        .grow_mirrored(name, 9, chunk_id, 9 * 1024 * 1024)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metadata.metadata_publish_count(), publications);
    assert_eq!(observed_end(&allocator, &disks, name).await, complete_end);
    stream.append(&[Bytes::from_static(b"next")]).await.unwrap();
    assert_eq!(stream.read_at(0, prefix.len()).await.unwrap(), prefix);
    assert_eq!(
        stream.read_at(range.end, 4).await.unwrap(),
        Bytes::from_static(b"next")
    );
    assert_eq!(observed_end(&allocator, &disks, name).await, complete_end + 51);
}

#[tokio::test]
async fn failed_empty_initialization_cannot_expose_a_writable_allocation() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    disks.fail_fsync_once.store(1, Ordering::Release);
    let store = ProductionStreamChunkStore::new(
        allocator.clone(),
        disks.clone(),
        30_000,
        ChunkReadPolicy::default(),
    )
    .unwrap();
    let name = StreamName { high: 131, low: 137 };
    assert!(store
        .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Stream)
        .await
        .is_err());
    let chunk = allocator.chunk.lock().unwrap().clone().unwrap();
    let (end, image) = prepared_group(chunk.id.unwrap(), &Bytes::from_static(b"blocked"), 0);
    let result = store
        .write_mirrors(
            name,
            9,
            chunk.id.unwrap(),
            crowdb_chunk_stream::StreamMirrorWrite {
                physical_offset: 0,
                complete_end: end,
                images: &[image],
            },
        )
        .await;
    assert!(
        matches!(result, Err(crowdb_chunk_stream::StreamError::InvalidRequest(_))),
        "{result:?}"
    );
    assert_eq!(
        store.durable_cursor(chunk.id.unwrap(), 9).await.unwrap().offset,
        0
    );
    assert_eq!(disks.fsyncs.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn uncertain_growth_initialization_cannot_be_retried_or_written() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let store = ProductionStreamChunkStore::new(
        allocator.clone(),
        disks.clone(),
        30_000,
        ChunkReadPolicy::default(),
    )
    .unwrap();
    let name = StreamName { high: 163, low: 167 };
    let active = store
        .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Stream)
        .await
        .unwrap();
    let mut strip = TestGroupedStrip::default();
    let end = strip.append(&store, name, 9, active.chunk_id, b"retained").await;
    disks.fail_fsync_once.store(11, Ordering::Release);
    assert!(store
        .grow_mirrored(name, 9, active.chunk_id, active.capacity + 1)
        .await
        .is_err());
    // All seven added strips complete their submitted work, including the healthy
    // replica of the failing strip, before uncertain initialization is returned.
    assert_eq!(disks.fsyncs.load(Ordering::Acquire), 17);
    let submissions = disks.writes.lock().unwrap().len();
    assert!(store
        .grow_mirrored(name, 9, active.chunk_id, active.capacity + 1)
        .await
        .is_err());
    assert_eq!(disks.writes.lock().unwrap().len(), submissions);
    assert_eq!(observed_end(&allocator, &disks, name).await, end);
    let (complete_end, image) = prepared_group(active.chunk_id, &Bytes::from(vec![7; 1024 * 1024]), 0);
    assert!(store
        .write_mirrors(
            name,
            9,
            active.chunk_id,
            crowdb_chunk_stream::StreamMirrorWrite {
                physical_offset: end,
                complete_end: end + complete_end,
                images: &[image],
            }
        )
        .await
        .is_err());
    assert_eq!(disks.writes.lock().unwrap().len(), submissions);
}
