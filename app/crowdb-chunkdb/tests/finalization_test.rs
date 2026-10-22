// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod common;

use common::cluster::{seed_hardware, ChunkdbHarness, DiskdbServer, KvCluster, DATA_GROUP_ID, STORE_ID};
use crowdb_chunkdb::{
    conversion::io::ConversionDiskIo,
    finalize::FinalizeChunkTaskHandler,
    routing::{default_binding_table, BindingCache},
    task::{TaskHandler, TaskOutcome, TaskStore},
};
use crowdb_protocol::{
    chunk_task::TASK_KIND_FINALIZE_CHUNK,
    chunkdb::rpc::{ChunkState, ChunkType, StripType},
};
use std::sync::Arc;

#[tokio::test]
async fn finalizer_seals_a_readable_acknowledged_tree_frame() {
    let cluster = KvCluster::start_pinned().await;
    let hardware = cluster.make_hardware_client();
    seed_hardware(&hardware).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let service = cluster.make_service_registry_client();
    let mut diskio = Vec::new();
    for index in 0..4 {
        diskio.push(
            common::diskio_stub::TestDiskio::start(
                &service,
                9000 + index,
                100 + index,
                10 + index,
                1000 + index,
            )
            .await,
        );
    }
    let harness = ChunkdbHarness::start(&cluster).await;
    let io = Arc::new(ConversionDiskIo::connect(&service, &hardware).await.unwrap());
    let chunk = harness
        .handler
        .allocate_chunk(
            None,
            1,
            1,
            StripType::Mirror,
            0,
            0,
            3,
            ChunkType::PageIndex,
            1,
            60_000,
        )
        .await
        .unwrap();
    let id = chunk.id.unwrap();
    let frame = crowdb_protocol::frame::encode_frame(
        crowdb_protocol::frame::FrameMagic::PageIndexV2,
        id,
        &[0x53; 1024],
        1,
    )
    .unwrap();
    let strip = &chunk.strips[0];
    let Some(crowdb_protocol::chunkdb::rpc::Strip::MirrorStrip(mirrors)) = &strip.strip else {
        panic!("expected mirrors");
    };
    for segment in &mirrors.segments {
        io.write_segment(
            segment,
            u64::from(strip.unit_kb) * 1024,
            bytes::Bytes::from(frame.clone()),
        )
        .await
        .unwrap();
    }
    let cursor = u64::try_from(frame.len()).unwrap();
    harness
        .handler
        .advance_chunk_write(&id, 1, chunk.modify_ts, cursor, None, 60_000)
        .await
        .unwrap();
    let bindings = BindingCache::new();
    bindings
        .replace(default_binding_table(STORE_ID, DATA_GROUP_ID))
        .unwrap();
    let tasks = TaskStore::new(cluster.make_crowdb_client(), bindings);
    let mut task = tasks
        .get(&id, TASK_KIND_FINALIZE_CHUNK, &id)
        .await
        .unwrap()
        .unwrap();
    task.eligible_at_ms = 0;
    let finalizer = FinalizeChunkTaskHandler::new(Arc::clone(&harness.handler), io);
    assert!(matches!(finalizer.execute(&task).await, TaskOutcome::Complete));
    let current = harness.handler.query_chunk(&id).await.unwrap();
    assert_eq!(current.state, ChunkState::Sealed as i32);
    assert_eq!(current.acknowledged_cursor, cursor);
    assert!(!current.strips.is_empty());
}

#[tokio::test]
async fn finalizer_retains_unreadable_acknowledged_frames() {
    let cluster = KvCluster::start_pinned().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let mut chunk = harness
        .handler
        .allocate_chunk(
            None,
            1,
            1,
            StripType::Mirror,
            0,
            0,
            3,
            ChunkType::PageIndex,
            1,
            60_000,
        )
        .await
        .unwrap();
    let id = chunk.id.unwrap();
    chunk = harness
        .handler
        .advance_chunk_write(&id, 1, chunk.modify_ts, 131_174, None, 60_000)
        .await
        .unwrap();
    let bindings = BindingCache::new();
    bindings
        .replace(default_binding_table(STORE_ID, DATA_GROUP_ID))
        .unwrap();
    let tasks = TaskStore::new(cluster.make_crowdb_client(), bindings);
    let mut task = tasks
        .get(&id, TASK_KIND_FINALIZE_CHUNK, &id)
        .await
        .unwrap()
        .unwrap();
    task.eligible_at_ms = 0;
    // The isolated reader returns zeros, modelling acknowledged frames that
    // cannot currently be read. Such a scan does not prove an empty chunk.
    let finalizer = FinalizeChunkTaskHandler::new(
        Arc::clone(&harness.handler),
        Arc::new(ConversionDiskIo::empty_for_tests()),
    );
    let result = finalizer.execute(&task).await;
    match result {
        TaskOutcome::Retry { error, .. } => assert!(error.contains("acknowledged"), "{error}"),
        TaskOutcome::Complete => {
            panic!("finalizer completed after scanning zero readable bytes despite 131174 acknowledged bytes")
        }
        TaskOutcome::Fail { error, .. } => {
            panic!("finalizer must retain acknowledged data and report the inconsistent scan: {error}")
        }
    }
    let current = harness.handler.query_chunk(&id).await.unwrap();
    assert_eq!(current.state, ChunkState::Active as i32);
    assert_eq!(current.acknowledged_cursor, 131_174);
    assert_eq!(current.strips, chunk.strips);
}
