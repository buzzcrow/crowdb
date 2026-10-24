// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
async fn excluded_initializer_cannot_recreate_a_collected_first_record() {
    let store = Arc::new(MemoryStreamStore::new(128));
    let identity = StreamRecord {
        state: StreamRecordState::Active,
        ..StreamRecord::creating(StreamName::generate(), Some("test".into()), StreamPurpose::Stream)
    };
    let name = identity.stream_name;
    store.pause_publication_for_tests();
    let old_store = store.clone();
    let initializing = tokio::spawn(async move {
        ChunkStream::create(
            identity,
            1,
            StreamConfig::default(),
            old_store.clone(),
            old_store.clone(),
            old_store,
        )
        .await
    });
    store.wait_for_publication_for_tests().await;
    let current = ChunkStream::create_registered(
        name,
        2,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    current.append(&[Bytes::from(vec![1; 100])]).await.unwrap();
    current.append(&[Bytes::from(vec![2; 100])]).await.unwrap();
    current.trim_prefix(100).await.unwrap();
    store.resume_publication_for_tests();
    assert!(matches!(
        initializing.await.unwrap(),
        Err(StreamError::StaleWriter)
    ));
    assert!(store
        .chunk_records_for_tests(name)
        .await
        .unwrap()
        .iter()
        .all(|record| record.logical_start != Some(0)));
    assert_eq!(current.tail(), 200);
    assert_eq!(
        current.read_at(100, 100).await.unwrap(),
        Bytes::from(vec![2; 100])
    );
}
