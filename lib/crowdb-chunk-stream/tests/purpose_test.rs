// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    ChunkStream, StreamConfig, StreamMetadataStore, StreamName, StreamPurpose, StreamRecord,
    StreamRecordState,
};

#[tokio::test]
async fn purpose_survives_rollover_failed_write_and_cold_writer_recovery() {
    for purpose in [StreamPurpose::Wal, StreamPurpose::Stream] {
        let store = Arc::new(MemoryStreamStore::new(128));
        let name = StreamName::generate();
        let binding = StreamRecord {
            stream_name: name,
            state: StreamRecordState::Active,
            ..StreamRecord::creating(name, None, purpose)
        };
        let stream = ChunkStream::create(
            binding,
            1,
            StreamConfig::default(),
            store.clone(),
            store.clone(),
            store.clone(),
        )
        .await
        .unwrap();
        let bytes = Bytes::from(vec![42; 60]);
        let first = stream.append(std::slice::from_ref(&bytes)).await.unwrap();
        let second = stream.append(std::slice::from_ref(&bytes)).await.unwrap();
        assert_ne!(first.chunk_id, second.chunk_id);
        assert!(purpose.matches(first.chunk_id.unwrap()));
        assert!(purpose.matches(second.chunk_id.unwrap()));
        store.fail_next_write();
        let repaired = stream.append(std::slice::from_ref(&bytes)).await.unwrap();
        assert!(purpose.matches(repaired.chunk_id.unwrap()));
        drop(stream);
        let recovered = ChunkStream::open(
            name,
            2,
            StreamConfig::default(),
            store.clone(),
            store.clone(),
            store.clone(),
        )
        .await
        .unwrap();
        assert_eq!(
            recovered.read_at(0, 180).await.unwrap(),
            Bytes::from(vec![42; 180])
        );
        let next = recovered.append(&[bytes]).await.unwrap();
        assert!(purpose.matches(next.chunk_id.unwrap()));
        let snapshot = store.load_tail_for_tests(name).await.unwrap().unwrap();
        assert_eq!(snapshot.record().purpose, purpose);
        let mut changed = snapshot.record().clone();
        changed.purpose = match purpose {
            StreamPurpose::Wal => StreamPurpose::Stream,
            StreamPurpose::Stream => StreamPurpose::Wal,
        };
        changed.generation += 1;
        assert!(store
            .record_store(1)
            .unwrap()
            .publish(Some(&snapshot), changed, None, None)
            .await
            .is_err());
    }
}
