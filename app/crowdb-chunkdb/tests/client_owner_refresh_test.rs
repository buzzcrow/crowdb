// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[allow(dead_code)]
#[path = "common/slot_groups.rs"]
mod slot_groups;

use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_paxos_kv_client::{ChunkSlotMapClient, RangeBindingClient, ServiceRegistryClient};
use crowdb_protocol::chunk_slot::{ChunkSlot, ChunkSlotBootstrap, ChunkStorageGroup};
use crowdb_protocol::chunkdb::rpc::AllocateChunkRequest;
use crowdb_protocol::fb::FBMsgType;
use crowdb_rpc_ffi::RpcServer;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registry_refresh_replaces_stale_owner_before_an_idless_allocation() {
    let test = slot_groups::TestGroups::start().await;
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
    let initial = maps.read_service_snapshot().await.unwrap();
    let old = RpcServer::new(None);
    old.listen("127.0.0.1", 0).unwrap();
    let old_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&old_calls);
    old.register_handler(FBMsgType::EAllocateChunkRequest.0 as u16, move |_| {
        calls.fetch_add(1, Ordering::Relaxed);
    });
    old.start();
    let registry = ServiceRegistryClient::from_shared(Arc::clone(&test.kv));
    registry
        .register_chunkdb(1, &format!("127.0.0.1:{}", old.port()))
        .await
        .unwrap();
    let binding = RangeBindingClient::from_shared(Arc::clone(&test.kv));
    binding.refresh().await.unwrap();
    assert_eq!(binding.snapshot()[0].instance_id, 1);
    let moved = initial
        .authority()
        .reassign(&ChunkSlot::all().map(|slot| (slot, 2)).collect::<Vec<_>>())
        .unwrap();
    maps.publish_service_epochs(&moved).await.unwrap();
    let new = RpcServer::new(None);
    new.listen("127.0.0.1", 0).unwrap();
    let received = Arc::new(tokio::sync::Notify::new());
    let signal = Arc::clone(&received);
    new.register_handler(FBMsgType::EAllocateChunkRequest.0 as u16, move |_| {
        signal.notify_one();
    });
    new.start();
    registry
        .register_chunkdb(2, &format!("127.0.0.1:{}", new.port()))
        .await
        .unwrap();
    // A populated old binding must advance before a new mutation is submitted.
    let client =
        ChunkdbClient::new(registry, Arc::new(ChunkdbRpcTransport::new())).with_range_binding(binding);
    let task = tokio::spawn(async move {
        client
            .allocate_chunk(AllocateChunkRequest {
                chunk_type: 5,
                ..Default::default()
            })
            .await
    });
    let delivered = tokio::time::timeout(Duration::from_secs(1), received.notified()).await;
    task.abort();
    let _ = task.await;
    old.stop();
    new.stop();
    assert!(
        delivered.is_ok(),
        "allocation was not submitted to the current owner"
    );
    assert_eq!(old_calls.load(Ordering::Relaxed), 0);
}
