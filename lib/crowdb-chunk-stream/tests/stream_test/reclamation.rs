// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
async fn trim_retains_frames_in_the_active_and_boundary_chunk() {
    let store = Arc::new(MemoryStreamStore::new(128));
    let stream = create_stream(&store, 128, StreamConfig::default()).await;
    let first = stream.append(&[Bytes::from_static(b"abcd")]).await.unwrap();
    let second = stream.append(&[Bytes::from_static(b"efgh")]).await.unwrap();
    assert_eq!(first.chunk_id, second.chunk_id);
    assert_eq!(stream.trim_prefix(4).await.unwrap(), 0);
    assert!(!store.is_released(first.chunk_id.unwrap()).await);
    assert_eq!(stream.read_at(4, 4).await.unwrap(), Bytes::from_static(b"efgh"));
    stream.append(&[Bytes::from_static(b"ijkl")]).await.unwrap();
    let successor = stream.append(&[Bytes::from(vec![b'x'; 64])]).await.unwrap();
    assert_ne!(first.chunk_id, successor.chunk_id);
    assert_eq!(stream.trim_prefix(8).await.unwrap(), 0);
    assert_eq!(stream.read_at(8, 4).await.unwrap(), Bytes::from_static(b"ijkl"));
    assert!(stream.trim_prefix(12).await.unwrap() > 0);
    assert!(store.is_released(first.chunk_id.unwrap()).await);
    assert_eq!(stream.trim_prefix(12).await.unwrap(), 0);
}
