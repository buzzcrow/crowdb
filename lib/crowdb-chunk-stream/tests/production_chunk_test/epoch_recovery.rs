// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::runtime_kv::runtime_kv;
use crowdb_chunk_stream::{StreamError, StreamPurpose};

#[tokio::test]
async fn newer_assignment_drains_the_cached_recovery_writer_before_reopening() {
    let (_server, kv) = runtime_kv().await;
    let chunk_io = ChunkIoClient::from_parts(
        Arc::new(Allocator::new()),
        Arc::new(Disks::default()),
        crowdb_protocol::chunkdb::rpc::ChunkType::Wal,
    );
    let runtime = ProductionStreamRuntime::new(
        kv,
        &chunk_io,
        30_000,
        ChunkReadPolicy::default(),
        StreamConfig::default(),
    )
    .unwrap();
    let name = StreamName::generate();
    runtime
        .registry()
        .create(StreamRecord {
            metadata_group_id: 1,
            state: StreamRecordState::Active,
            ..StreamRecord::creating(name, None, StreamPurpose::Wal)
        })
        .await
        .unwrap();
    let first = runtime.create_registered(name, 0, 1).await.unwrap();
    first.append(&[Bytes::from_static(b"before")]).await.unwrap();
    // A recovered catalog can advance during WAL exclusion/replay. Its next
    // assignment reuses the retained WAL under a higher owner epoch.
    let next = runtime
        .open(name, 0, 2)
        .await
        .expect("new catalog recovery must drain its cached predecessor");
    assert_eq!(
        first.append(&[Bytes::from_static(b"stale")]).await,
        Err(StreamError::WriteStalled)
    );
    assert!(matches!(
        runtime.open(name, 0, 1).await,
        Err(StreamError::StaleWriter)
    ));
    let reused = runtime.open(name, 0, 2).await.unwrap();
    reused.append(&[Bytes::from_static(b"-after")]).await.unwrap();
    assert_eq!(
        next.read_at(0, 12).await.unwrap(),
        Bytes::from_static(b"before-after")
    );
    next.release_writer().await.unwrap();
}
