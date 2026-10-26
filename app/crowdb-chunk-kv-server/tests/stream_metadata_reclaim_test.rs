// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, KvStreamMetadataStore, StreamConfig, StreamMetadataStore,
    StreamName, StreamPurpose, StreamRecord, StreamRecordState,
};
use crowdb_kv_client::{CrowdbKvClient, ReadMode};
use crowdb_protocol::chunk_stream::decode_stream_chunk_record;
use crowdb_protocol::key::{BinaryKey, StreamChunkRecordKey};

#[path = "common/metadata.rs"]
mod metadata;

async fn pages(kv: &CrowdbKvClient, name: StreamName) -> Vec<StreamChunkRecordKey> {
    kv.scan(
        0,
        1,
        &StreamChunkRecordKey::attached_prefix(name),
        &[],
        &[],
        0,
        ReadMode::Linearizable,
        None,
        true,
        None,
    )
    .await
    .unwrap()
    .items
    .into_iter()
    .map(|(key, _)| StreamChunkRecordKey::from_bytes(&key).unwrap())
    .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_record_reclaim_respects_trim_and_retained_generation() {
    let fixture = metadata::TestMetadata::start().await;
    let chunks = Arc::new(MemoryStreamStore::new(512));
    let metadata = Arc::new(KvStreamMetadataStore::new(fixture.client.clone(), 0, 1).unwrap());
    let name = StreamName { high: 81, low: 82 };
    let stream = ChunkStream::create(
        StreamRecord {
            purpose: StreamPurpose::Wal,
            stream_name: name,
            metadata_group_id: 1,
            record_version: 1,
            consumer_id: None,
            created_at_ms: StreamRecord::creation_time_ms(),
            state: StreamRecordState::Active,
            consumer_kind: None,
        },
        7,
        StreamConfig::default(),
        chunks.clone(),
        metadata.clone(),
        chunks.clone(),
    )
    .await
    .unwrap();
    for byte in 0..16 {
        stream.append(&[Bytes::from(vec![byte; 384])]).await.unwrap();
    }
    stream.checkpoint().await.unwrap();
    let retained = stream.record_generation();
    for byte in 16..24 {
        stream.append(&[Bytes::from(vec![byte; 384])]).await.unwrap();
    }
    stream.checkpoint().await.unwrap();
    let before_trim = pages(&fixture.client, name).await;
    assert_eq!(before_trim.len(), 24);
    assert_eq!(stream.reclaim_metadata_before(retained, 3).await.unwrap(), 0);
    assert_eq!(pages(&fixture.client, name).await, before_trim);
    let expected = fixture
        .client
        .scan(
            0,
            1,
            &StreamChunkRecordKey::attached_prefix(name),
            &[],
            &[],
            0,
            ReadMode::Linearizable,
            None,
            false,
            None,
        )
        .await
        .unwrap()
        .items
        .into_iter()
        .filter_map(|(key, value)| {
            let record = decode_stream_chunk_record(&value).unwrap();
            (record.generation >= retained || record.logical_end.unwrap() > 23 * 384)
                .then(|| StreamChunkRecordKey::from_bytes(&key).unwrap())
        })
        .collect::<Vec<_>>();
    stream.trim_prefix(23 * 384).await.unwrap();
    let before = pages(&fixture.client, name).await.len();
    assert_eq!(stream.reclaim_metadata_before(retained, 3).await.unwrap(), 1);
    assert_eq!(pages(&fixture.client, name).await.len(), before - 1);
    for _ in 0..64 {
        if pages(&fixture.client, name).await == expected {
            break;
        }
        assert!(stream.reclaim_metadata_before(retained, 3).await.unwrap() <= 1);
        let present = pages(&fixture.client, name).await;
        assert!(expected.iter().all(|key| present.contains(key)));
    }
    let retained_records = metadata.record_store(1).unwrap();
    assert_eq!(pages(&fixture.client, name).await, expected);
    let tail = retained_records.load_tail(name).await.unwrap().unwrap();
    assert!(tail.record().generation >= retained);
    assert_eq!(stream.read_at(23 * 384, 384).await.unwrap().as_ref(), &[23; 384]);
}
