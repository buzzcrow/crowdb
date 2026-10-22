// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_chunk_stream::{memory::MemoryStreamStore, StreamChunkStore, StreamName, StreamPurpose};
use crowdb_protocol::frame::encode_append_group_views;

#[tokio::test]
async fn grouped_memory_progress_remains_local_until_an_explicit_seal() {
    let store = MemoryStreamStore::new(1024 * 1024);
    let name = StreamName { high: 7, low: 9 };
    let chunk = store
        .allocate_mirrored(name, 9, StreamPurpose::Stream)
        .await
        .unwrap();
    let mut offset = 0;
    for payload in [b"first".as_slice(), b"second".as_slice()] {
        let views =
            encode_append_group_views(chunk.chunk_id, offset, &Bytes::copy_from_slice(payload), 1).unwrap();
        let data = Bytes::from(views.concat());
        let length = data.len() as u64;
        store
            .write_mirrors_with_images(name, 9, chunk.chunk_id, offset, data, &[])
            .await
            .unwrap();
        offset += length;
        assert_eq!(store.durable_cursor(chunk.chunk_id, 9).await.unwrap().offset, 0);
    }
    assert_eq!(store.cursor_advance_count(), 0);
    store.seal(chunk.chunk_id, 9, offset).await.unwrap();
    assert_eq!(
        store.durable_cursor(chunk.chunk_id, 9).await.unwrap().offset,
        offset
    );
}
