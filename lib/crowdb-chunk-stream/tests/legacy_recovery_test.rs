// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/legacy_stream.rs"]
mod legacy_stream;

use bytes::Bytes;
use crowdb_chunk_stream::{ChunkStream, StreamChunkStore, StreamConfig};
use legacy_stream::TestLegacyStream;

#[tokio::test]
async fn legacy_open_seals_only_the_persisted_cursor_and_attaches_a_fresh_chunk() {
    for epoch in [9, 10] {
        let fixture = TestLegacyStream::new().await;
        let reopened = ChunkStream::open(
            fixture.name,
            epoch,
            StreamConfig::default(),
            fixture.store.clone(),
            fixture.store.clone(),
            fixture.store.clone(),
        )
        .await
        .unwrap();
        assert_eq!(reopened.tail(), 3);
        assert_eq!(reopened.read_at(0, 3).await.unwrap(), b"old".as_slice());
        let legacy = fixture.store.durable_cursor(fixture.chunk, epoch).await.unwrap();
        assert!(legacy.sealed);
        assert_eq!(legacy.offset, fixture.cursor);
        let range = reopened.append(&[Bytes::from_static(b"new")]).await.unwrap();
        assert_ne!(range.chunk_id, Some(fixture.chunk));
        assert_eq!((range.begin, range.end), (3, 6));
        assert_eq!(reopened.read_at(0, 6).await.unwrap(), b"oldnew".as_slice());
        assert!(!fixture.store.is_released(fixture.chunk).await);
        reopened.close().await.unwrap();
    }
}
