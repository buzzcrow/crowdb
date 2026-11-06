// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/metadata_server.rs"]
mod metadata_server;
use bytes::Bytes;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, KvStreamMetadataStore, StreamConfig, StreamMetadataStore,
    StreamName, StreamPurpose, StreamRecord, StreamRecordState,
};
use crowdb_paxos_kv_client::ReadMode;
use crowdb_protocol::{
    chunk_stream::{decode_stream_chunk_record, StreamChunkState},
    key::{BinaryKey, StreamChunkRecordKey, StreamChunkRecordOrder},
};
use std::sync::Arc;

#[tokio::test]
async fn actual_kv_reverse_seek_uses_ordered_keys_and_never_updates_historical_records() {
    let server = metadata_server::TestMetadataServer::start().await;
    let chunks = Arc::new(MemoryStreamStore::new(128));
    let metadata = Arc::new(KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap());
    let name = StreamName::generate();
    let stream = ChunkStream::create(
        StreamRecord {
            state: StreamRecordState::Active,
            ..StreamRecord::creating(name, Some("test".into()), StreamPurpose::Stream)
        },
        1,
        StreamConfig::default(),
        chunks.clone(),
        metadata.clone(),
        chunks.clone(),
    )
    .await
    .unwrap();
    for byte in 1..=16 {
        stream.append(&[Bytes::from(vec![byte; 100])]).await.unwrap();
    }
    stream.checkpoint().await.unwrap();
    let records = server
        .client
        .scan(
            1,
            1,
            &StreamChunkRecordKey::attached_prefix(name),
            &[],
            &[],
            32,
            ReadMode::Linearizable,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    assert_eq!(records.items.len(), 16);
    let first = records.items[0].clone();
    let store = metadata.record_store(1).unwrap();
    for index in 0..16 {
        let snapshot = store.locate(name, index * 100).await.unwrap();
        assert_eq!(snapshot.record().logical_start, Some(index * 100));
        assert_eq!(snapshot.record().logical_end, Some((index + 1) * 100));
        assert_eq!(
            stream.read_at(index * 100, 1).await.unwrap(),
            Bytes::from(vec![u8::try_from(index + 1).unwrap()])
        );
    }
    let tail = store.load_tail(name).await.unwrap().unwrap();
    assert_eq!(tail.record().state, StreamChunkState::Active);
    assert_eq!(tail.record().logical_start, Some(1500));
    for byte in 17..=24 {
        stream.append(&[Bytes::from(vec![byte; 100])]).await.unwrap();
    }
    let first_again = server
        .client
        .get(1, 1, &first.0, ReadMode::Linearizable, None)
        .await
        .unwrap();
    let crowdb_paxos_kv_client::GetOutcome::Found { value, .. } = first_again else {
        panic!("sealed record missing")
    };
    assert_eq!(value, first.1);
    let key = StreamChunkRecordKey::from_bytes(&first.0).unwrap();
    assert_eq!(key.order, StreamChunkRecordOrder::Attached(0));
    assert_eq!(decode_stream_chunk_record(&value).unwrap().logical_end, Some(100));
    let captured = stream.capture_read_only().await.unwrap();
    assert_eq!(captured.tail(), 2400);
    stream.close().await.unwrap();
    let reopened = ChunkStream::open(name, 2, StreamConfig::default(), chunks.clone(), metadata, chunks)
        .await
        .unwrap();
    assert_eq!(reopened.tail(), 2400);
    assert_eq!(
        reopened.read_at(2300, 100).await.unwrap(),
        Bytes::from(vec![24; 100])
    );
}

#[tokio::test]
async fn actual_kv_collection_uses_bounded_keys_and_releases_only_trimmed_chunks() {
    let server = metadata_server::TestMetadataServer::start().await;
    let chunks = Arc::new(MemoryStreamStore::new(128));
    let metadata = Arc::new(KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap());
    let name = StreamName::generate();
    let stream = ChunkStream::create(
        StreamRecord {
            state: StreamRecordState::Active,
            ..StreamRecord::creating(name, None, StreamPurpose::Stream)
        },
        1,
        StreamConfig::default(),
        chunks.clone(),
        metadata.clone(),
        chunks.clone(),
    )
    .await
    .unwrap();
    let first = stream.append(&[Bytes::from(vec![1; 100])]).await.unwrap();
    stream.append(&[Bytes::from(vec![2; 100])]).await.unwrap();
    stream.append(&[Bytes::from(vec![3; 100])]).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match stream.trim_prefix(100).await {
                Ok(_) | Err(crowdb_chunk_stream::StreamError::Backpressure) => {}
                Err(error) => panic!("actual KV collection failed: {error}"),
            }
            if chunks.is_released(first.chunk_id.unwrap()).await {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(stream.read_at(100, 100).await.unwrap(), Bytes::from(vec![2; 100]));
    assert!(metadata.record_store(1).unwrap().locate(name, 0).await.is_err());
    stream.close().await.unwrap();
}

#[tokio::test]
async fn checkpoint_publication_waits_for_an_inflight_tail_guard_update() {
    use crowdb_paxos_kv_client::GetOutcome;
    use std::time::Duration;

    let server = metadata_server::TestMetadataServer::start().await;
    let chunks = Arc::new(MemoryStreamStore::new(1024));
    let metadata = Arc::new(KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap());
    let name = StreamName::generate();
    let _stream = ChunkStream::create(
        StreamRecord {
            state: StreamRecordState::Active,
            ..StreamRecord::creating(name, None, StreamPurpose::Stream)
        },
        1,
        StreamConfig::default(),
        chunks.clone(),
        metadata.clone(),
        chunks,
    )
    .await
    .unwrap();
    let store = metadata.record_store(1).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.candidates(name).await.unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let tail = store.load_tail(name).await.unwrap().unwrap();
    let key = StreamChunkRecordKey {
        stream_id: name,
        order: StreamChunkRecordOrder::Attached(0),
    }
    .to_bytes();
    let GetOutcome::Found { value, revision } = server
        .client
        .get(1, 1, &key, ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("tail guard is absent");
    };
    let group = server.server.get_group(1).unwrap();
    let permits = group.try_acquire_all_inflight_permits();
    let client = Arc::clone(&server.client);
    let guard = key.clone();
    let updating = tokio::spawn(async move { client.put_cas(1, 1, &guard, &value, revision).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !group.conditional_write_pending_for_tests(&key) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut checkpoint = tail.record().clone();
    checkpoint.generation += 1;
    let publishing = store.publish(Some(&tail), checkpoint.clone(), None, None);
    let release = async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(permits);
    };
    let (result, ()) = tokio::join!(publishing, release);
    updating.await.unwrap().unwrap();
    let published = result.expect("a short tail-guard CAS must not fail checkpoint publication");
    assert_eq!(published.record(), &checkpoint);
    assert_eq!(
        store.load_tail(name).await.unwrap().unwrap().record(),
        &checkpoint
    );
}

#[tokio::test]
async fn read_pin_waits_for_an_inflight_tail_guard_update() {
    use crowdb_paxos_kv_client::GetOutcome;
    use std::time::Duration;

    let server = metadata_server::TestMetadataServer::start().await;
    let chunks = Arc::new(MemoryStreamStore::new(1024));
    let metadata = Arc::new(KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap());
    let name = StreamName::generate();
    let _stream = ChunkStream::create(
        StreamRecord {
            state: StreamRecordState::Active,
            ..StreamRecord::creating(name, None, StreamPurpose::Stream)
        },
        1,
        StreamConfig::default(),
        chunks.clone(),
        metadata.clone(),
        chunks,
    )
    .await
    .unwrap();
    let store = metadata.record_store(1).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.candidates(name).await.unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let tail = store.load_tail(name).await.unwrap().unwrap();
    let key = StreamChunkRecordKey {
        stream_id: name,
        order: StreamChunkRecordOrder::Attached(0),
    }
    .to_bytes();
    let GetOutcome::Found { value, revision } = server
        .client
        .get(1, 1, &key, ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("tail guard is absent");
    };
    let group = server.server.get_group(1).unwrap();
    let permits = group.try_acquire_all_inflight_permits();
    let client = Arc::clone(&server.client);
    let guard = key.clone();
    let updating = tokio::spawn(async move { client.put_cas(1, 1, &guard, &value, revision).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !group.conditional_write_pending_for_tests(&key) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let pinning = store.pin_read(&tail);
    let release = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(permits);
    };
    let (result, ()) = tokio::join!(pinning, release);
    updating.await.unwrap().unwrap();
    let pin = result.expect("a short tail-guard CAS must not exhaust the read-pin budget");
    pin.check().unwrap();
    assert_eq!(
        store.load_tail(name).await.unwrap().unwrap().record(),
        tail.record()
    );
}
