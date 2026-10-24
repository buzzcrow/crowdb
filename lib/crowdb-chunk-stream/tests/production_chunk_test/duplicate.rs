// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crowdb_protocol::frame::{parse_header, FrameMagic};

#[tokio::test]
async fn delayed_duplicate_append_preserves_later_acknowledged_groups() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let chunks = Arc::new(
        ProductionStreamChunkStore::new(allocator, disks.clone(), 30_000, ChunkReadPolicy::default())
            .unwrap(),
    );
    let metadata = Arc::new(MemoryStreamStore::new(1));
    let stream = ChunkStream::create(
        StreamRecord {
            purpose: crowdb_chunk_stream::StreamPurpose::Stream,
            stream_name: StreamName { high: 151, low: 157 },
            metadata_group_id: 1,
            record_version: 1,
            state: StreamRecordState::Active,
            consumer_kind: Some("test".into()),
            consumer_id: None,
            created_at_ms: StreamRecord::creation_time_ms(),
        },
        9,
        StreamConfig::default(),
        metadata.clone(),
        metadata,
        chunks,
    )
    .await
    .unwrap();
    stream.append(&[Bytes::from_static(b"first")]).await.unwrap();
    let delayed: Vec<_> = disks
        .writes
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, _, _, data)| parse_header(data).is_ok_and(|header| header.magic == FrameMagic::StreamV2))
        .cloned()
        .collect();
    assert_eq!(delayed.len(), 2);
    stream.append(&[Bytes::from_static(b"second")]).await.unwrap();
    for (segment, unit_bytes, offset, data) in delayed {
        disks
            .write_at_byte_offset(&segment, unit_bytes, offset, data)
            .await
            .unwrap();
    }
    assert_eq!(
        stream.read_at(0, 11).await.unwrap(),
        Bytes::from_static(b"firstsecond")
    );
}
