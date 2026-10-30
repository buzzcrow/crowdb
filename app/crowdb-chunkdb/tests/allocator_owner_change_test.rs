// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[allow(dead_code)]
#[path = "common/slot_groups.rs"]
mod slot_groups;

use crowdb_chunkdb::allocator::DiskdbClientPool;
use crowdb_diskdb_client::DiskdbClientError;
use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::{
    common::ChunkId,
    diskdb_fb::{FBAllocateResponse, FBAllocateResponseArgs, FBDiskdbRetCode},
    fb::FBMsgType,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn endpoint(code: FBDiskdbRetCode) -> (crowdb_rpc_ffi::RpcServer, Arc<AtomicUsize>) {
    let server = crowdb_rpc_ffi::RpcServer::new(None);
    server.listen("127.0.0.1", 0).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    server.register_reply_for_tests(FBMsgType::EAllocateBlocksRequest.0 as u16, move |request| {
        observed.fetch_add(1, Ordering::Relaxed);
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let response = FBAllocateResponse::create(
            &mut builder,
            &FBAllocateResponseArgs {
                id: request.request_id,
                rpc_create_nano: request.rpc_create_nano,
                ret_code: code,
                ..Default::default()
            },
        );
        builder.finish(response, None);
        (builder.finished_data().to_vec(), Vec::new())
    });
    server.start();
    (server, calls)
}

#[tokio::test]
async fn allocation_reroutes_after_an_explicit_old_owner_rejection() {
    let test = slot_groups::TestGroups::start().await;
    let registry = ServiceRegistryClient::from_shared(test.kv.clone());
    let (old, old_calls) = endpoint(FBDiskdbRetCode::NotOwner);
    let (new, new_calls) = endpoint(FBDiskdbRetCode::Success);
    registry
        .register_diskdb(1, &format!("127.0.0.1:{}", old.port()), &[1], &[])
        .await
        .unwrap();
    let pool = DiskdbClientPool::new(registry.clone());
    pool.refresh_endpoints().await.unwrap();
    registry
        .register_diskdb(1, &format!("127.0.0.1:{}", new.port()), &[1], &[])
        .await
        .unwrap();
    let result = pool
        .allocate_blocks(
            1,
            1,
            1,
            &ChunkId {
                high: 5 << 56,
                low: 1,
            },
        )
        .await;
    old.stop();
    new.stop();
    result.unwrap();
    assert_eq!(old_calls.load(Ordering::Relaxed), 1);
    assert_eq!(new_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn allocation_does_not_reroute_an_ambiguous_submitted_request() {
    let test = slot_groups::TestGroups::start().await;
    let registry = ServiceRegistryClient::from_shared(test.kv.clone());
    let old = crowdb_rpc_ffi::RpcServer::new(None);
    old.listen("127.0.0.1", 0).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    old.register_handler(FBMsgType::EAllocateBlocksRequest.0 as u16, move |_| {
        observed.fetch_add(1, Ordering::Relaxed);
    });
    old.start();
    let (new, new_calls) = endpoint(FBDiskdbRetCode::Success);
    registry
        .register_diskdb(1, &format!("127.0.0.1:{}", old.port()), &[1], &[])
        .await
        .unwrap();
    let pool = DiskdbClientPool::new(registry.clone());
    pool.refresh_endpoints().await.unwrap();
    registry
        .register_diskdb(1, &format!("127.0.0.1:{}", new.port()), &[1], &[])
        .await
        .unwrap();
    let error = pool
        .allocate_blocks(
            1,
            1,
            1,
            &ChunkId {
                high: 5 << 56,
                low: 2,
            },
        )
        .await
        .unwrap_err();
    old.stop();
    new.stop();
    assert!(matches!(error, DiskdbClientError::Unreachable(_)), "{error}");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(new_calls.load(Ordering::Relaxed), 0);
}
