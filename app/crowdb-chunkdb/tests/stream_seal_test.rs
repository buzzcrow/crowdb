// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod common;

use common::cluster::{seed_hardware, ChunkdbHarness, DiskdbServer, KvCluster};
use crowdb_chunkdb::lifecycle::LifecycleError;
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_protocol::chunkdb::rpc::{ChunkState, ChunkType, StripType};

#[tokio::test]
async fn stream_and_wal_seal_cannot_shorten_a_persisted_byte_cursor() {
    let cluster = KvCluster::start_pinned().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    for purpose in [ChunkType::Stream, ChunkType::Wal] {
        let chunk = harness
            .handler
            .allocate_chunk_owned(
                None,
                4,
                1,
                StripType::Mirror,
                0,
                0,
                2,
                purpose,
                9,
                60_000,
                StreamName {
                    high: 7,
                    low: purpose as u64,
                }
                .chunk_owner_key(),
            )
            .await
            .unwrap();
        let id = chunk.id.unwrap();
        let advanced = harness
            .handler
            .advance_chunk_write(&id, 9, chunk.modify_ts, 17, None, 60_000)
            .await
            .unwrap();
        let error = harness.handler.seal_chunk_bytes(&id, 16).await.unwrap_err();
        assert!(
            matches!(error, LifecycleError::InvalidRequest(_)),
            "{purpose:?}: {error}"
        );
        assert_eq!(
            harness.handler.query_chunk(&id).await.unwrap(),
            advanced,
            "rejected shortening must not mutate state, version, strips or cleanup"
        );
        let sealed = harness.handler.seal_chunk_bytes(&id, 17).await.unwrap();
        assert_eq!(sealed.state, ChunkState::Sealed as i32);
        assert_eq!(sealed.acknowledged_cursor, 17);
        assert!(sealed.modify_ts > advanced.modify_ts);
    }
}

#[tokio::test]
async fn stream_seal_can_publish_a_newer_synchronized_byte_boundary() {
    let cluster = KvCluster::start_pinned().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let chunk = harness
        .handler
        .allocate_chunk_owned(
            None,
            4,
            1,
            StripType::Mirror,
            0,
            0,
            2,
            ChunkType::Stream,
            9,
            60_000,
            StreamName { high: 71, low: 73 }.chunk_owner_key(),
        )
        .await
        .unwrap();
    let id = chunk.id.unwrap();
    assert_eq!(chunk.acknowledged_cursor, 0);
    let sealed = harness.handler.seal_chunk_bytes(&id, 73).await.unwrap();
    assert_eq!(sealed.acknowledged_cursor, 73);
    assert_eq!(sealed.state, ChunkState::Sealed as i32);
    assert_eq!(sealed.sealed_length, 1);
}

#[tokio::test]
async fn exact_empty_seal_records_zero_bytes_and_zero_physical_capacity() {
    let cluster = KvCluster::start_pinned().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let chunk = harness
        .handler
        .allocate_chunk_owned(
            None,
            4,
            1,
            StripType::Mirror,
            0,
            0,
            2,
            ChunkType::Stream,
            9,
            60_000,
            StreamName { high: 79, low: 83 }.chunk_owner_key(),
        )
        .await
        .unwrap();
    let sealed = harness
        .handler
        .seal_chunk_bytes(&chunk.id.unwrap(), 0)
        .await
        .unwrap();
    assert_eq!(sealed.state, ChunkState::Sealed as i32);
    assert_eq!(sealed.acknowledged_cursor, 0);
    assert_eq!(sealed.sealed_length, 0);
    assert_eq!(sealed.capacity, 0);
    assert!(sealed.strips.is_empty());
}
