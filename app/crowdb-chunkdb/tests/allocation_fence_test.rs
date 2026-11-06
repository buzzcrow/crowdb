// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
//! A durable tombstone fences a fixed identity before a delayed allocation.
#[allow(dead_code)]
mod common;

use common::cluster::{seed_hardware, ChunkdbHarness, DiskdbServer, KvCluster};
use crowdb_chunkdb::lifecycle::LifecycleError;
use crowdb_protocol::chunkdb::rpc::{ChunkState, ChunkType, StripType};
use crowdb_protocol::common::ChunkId;
use std::sync::Arc;

#[tokio::test]
async fn delete_before_allocate_fences_the_identity_across_handler_restart() {
    let cluster = KvCluster::start_pinned().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let id = ChunkId {
        high: 2 << 56,
        low: 1007,
    };
    let (send, receive) = tokio::sync::oneshot::channel();
    let handler = Arc::clone(&harness.handler);
    let delayed = tokio::spawn(async move {
        receive.await.unwrap();
        handler
            .allocate_chunk(
                Some(id),
                1,
                1,
                StripType::Mirror,
                0,
                0,
                3,
                ChunkType::BtreePage,
                17,
                30_000,
            )
            .await
    });
    let deleted = harness.handler.delete_chunk_fenced(&id, true).await.unwrap();
    assert_eq!(deleted.state, ChunkState::Deleted as i32);
    assert!(deleted.strips.is_empty());
    assert_eq!(deleted.capacity, 0);
    send.send(()).unwrap();
    assert!(matches!(
        delayed.await.unwrap(),
        Err(LifecycleError::ChunkAlreadyExists)
    ));
    let restarted = ChunkdbHarness::start(&cluster).await;
    assert_eq!(restarted.store.get_chunk(&id).await.unwrap(), deleted);
    assert_eq!(
        restarted.handler.delete_chunk_fenced(&id, true).await.unwrap(),
        deleted
    );
    assert!(restarted
        .handler
        .allocate_chunk(
            Some(id),
            1,
            1,
            StripType::Mirror,
            0,
            0,
            3,
            ChunkType::BtreePage,
            18,
            30_000
        )
        .await
        .is_err());
}

#[tokio::test]
async fn allocated_tree_chunk_delete_releases_strips_before_success() {
    let cluster = KvCluster::start_pinned().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let id = ChunkId {
        high: 3 << 56,
        low: 1011,
    };
    let active = harness
        .handler
        .allocate_chunk(
            Some(id),
            1,
            1,
            StripType::Mirror,
            0,
            0,
            3,
            ChunkType::PageIndex,
            17,
            30_000,
        )
        .await
        .unwrap();
    assert!(!active.strips.is_empty());
    let deleted = harness.handler.delete_chunk_fenced(&id, true).await.unwrap();
    assert_eq!(deleted.state, ChunkState::Deleted as i32);
    assert!(deleted.strips.is_empty());
    assert_eq!(deleted.capacity, 0);
    assert_eq!(harness.store.get_chunk(&id).await.unwrap(), deleted);
    assert_eq!(
        harness.handler.delete_chunk_fenced(&id, true).await.unwrap(),
        deleted
    );
}
