// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/slot_groups.rs"]
#[allow(dead_code)]
mod slot_groups;

use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbClientError, ChunkdbRpcTransport};
use crowdb_kv_client::{ChunkSlotMapClient, ServiceRegistryClient};
use crowdb_protocol::{
    chunk_slot::{ChunkSlotBootstrap, ChunkStorageGroup},
    chunkdb::rpc::{AllocateChunkRequest, ChunkType},
    chunkdb_fb::{
        FBAllocateChunkRequest, FBAllocateChunkResponse, FBAllocateChunkResponseArgs, FBChunk, FBChunkArgs,
        FBChunkType, FBChunkdbRetCode,
    },
    common::ChunkId,
    fb::FBMsgType,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

async fn client(test: &slot_groups::TestGroups, port: i32) -> ChunkdbClient {
    let maps = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    maps.initialize_layout(&ChunkSlotBootstrap {
        service_instances: vec![1],
        storage_groups: vec![ChunkStorageGroup {
            store_id: 0,
            group_id: 1,
        }],
    })
    .await
    .unwrap();
    maps.initialize_service_epochs().await.unwrap();
    let registry = ServiceRegistryClient::from_shared(Arc::clone(&test.kv));
    registry
        .register_chunkdb(1, &format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    let client = ChunkdbClient::new(registry, Arc::new(ChunkdbRpcTransport::new()));
    client.refresh_routes().await.unwrap();
    client
}

fn request() -> AllocateChunkRequest {
    AllocateChunkRequest {
        chunk_id: Some(ChunkId {
            high: 5 << 56,
            low: 123,
        }),
        chunk_type: ChunkType::S3 as i32,
        ..Default::default()
    }
}

#[tokio::test]
async fn known_quorum_unavailability_retries_allocation_with_the_same_chunk_id() {
    let test = slot_groups::TestGroups::start().await;
    let server = crowdb_rpc_ffi::RpcServer::new(None);
    server.listen("127.0.0.1", 0).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    server.register_reply_for_tests(FBMsgType::EAllocateChunkRequest.0 as u16, move |request| {
        let body = flatbuffers::root::<FBAllocateChunkRequest>(request.control()).unwrap();
        let id = body.chunk_id().unwrap();
        assert_eq!((id.high(), id.low()), (5 << 56, 123));
        let first = observed.fetch_add(1, Ordering::Relaxed) == 0;
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let error = first.then(|| builder.create_string("linearizable read: leadership quorum unavailable"));
        let chunk = (!first).then(|| {
            FBChunk::create(
                &mut builder,
                &FBChunkArgs {
                    id: Some(id),
                    chunk_type: FBChunkType::S3,
                    ..Default::default()
                },
            )
        });
        let response = FBAllocateChunkResponse::create(
            &mut builder,
            &FBAllocateChunkResponseArgs {
                id: request.request_id,
                rpc_create_nano: request.rpc_create_nano,
                ret_code: if first {
                    FBChunkdbRetCode::Unavailable
                } else {
                    FBChunkdbRetCode::Success
                },
                error_msg: error,
                chunk,
                ..Default::default()
            },
        );
        builder.finish(response, None);
        (builder.finished_data().to_vec(), Vec::new())
    });
    server.start();
    let result = client(&test, server.port())
        .await
        .allocate_chunk(request())
        .await
        .unwrap();
    assert_eq!(result.chunk.unwrap().id, request().chunk_id);
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    server.stop();
}

#[tokio::test]
async fn an_allocation_with_a_lost_reply_is_not_replayed() {
    let test = slot_groups::TestGroups::start().await;
    let server = crowdb_rpc_ffi::RpcServer::new(None);
    server.listen("127.0.0.1", 0).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    server.register_handler(FBMsgType::EAllocateChunkRequest.0 as u16, move |_| {
        observed.fetch_add(1, Ordering::Relaxed);
    });
    server.start();
    let error = client(&test, server.port())
        .await
        .allocate_chunk(request())
        .await
        .unwrap_err();
    assert!(matches!(error, ChunkdbClientError::OutcomeUnknown(_)), "{error}");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    server.stop();
}
