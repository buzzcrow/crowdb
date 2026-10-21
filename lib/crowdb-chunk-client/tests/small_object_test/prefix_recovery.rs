// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{client, policy};
use bytes::Bytes;
use crowdb_chunk_client::{ChunkIoWriter, IoError};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn transient_write_failure_does_not_mark_the_acknowledged_prefix_corrupt() {
    let mut settings = policy();
    settings.mirror_copies = 2;
    let (client, allocator, disk) = client(settings);
    let mut first = client.prepare_small_write(4096).await.unwrap();
    first.on_data(Bytes::from(vec![0x5a; 4096])).await.unwrap();
    let locations = first.on_finish().await.unwrap();
    let id = locations[0].chunk_id.unwrap();
    let before = allocator.state.lock().unwrap().chunks[&(id.high, id.low)].clone();
    assert!(before.acknowledged_cursor > 0);
    disk.fail.store(true, Ordering::Relaxed);
    let mut failed = client.prepare_small_write(4096).await.unwrap();
    failed.on_data(Bytes::from(vec![0xa5; 4096])).await.unwrap();
    assert!(failed.on_finish().await.is_err());
    let after = allocator.state.lock().unwrap().chunks[&(id.high, id.low)].clone();
    assert_eq!(after.acknowledged_cursor, before.acknowledged_cursor);
    assert!(
        after
            .strips
            .iter()
            .all(|strip| strip.unavailable_segments.is_empty()),
        "unknown writes cannot label every retained replica as corrupt"
    );
    disk.fail.store(false, Ordering::Relaxed);
    assert!(matches!(
        client.shutdown_small_writes().await,
        Err(IoError::WriteFailed(_))
    ));
}
