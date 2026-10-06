// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dynamic_runtime.rs"]
mod dynamic_runtime;
#[allow(dead_code)]
#[path = "common/runtime_context.rs"]
mod runtime_context;
#[path = "common/slot_groups.rs"]
mod slot_groups;
use crowdb_chunkdb::{
    routing::{BindingCache, BindingTable},
    storage::ChunkStore,
};
use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_kv_client::{ChunkSlotMapClient, ServiceRegistryClient};
use crowdb_protocol::chunk_slot::{ChunkSlot, ChunkSlotBootstrap, ChunkStorageGroup};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, DeleteChunkRequest};
use crowdb_protocol::common::ChunkId;
use dynamic_runtime::TestDynamicRuntime;
use std::{sync::Arc, time::Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_cached_owner_reroutes_a_mutation_to_the_current_prepared_owner() {
    let test = slot_groups::TestGroups::start().await;
    let maps = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let layout = ChunkSlotBootstrap {
        service_instances: vec![1],
        storage_groups: vec![ChunkStorageGroup {
            store_id: 0,
            group_id: 1,
        }],
    };
    maps.initialize_layout(&layout).await.unwrap();
    maps.initialize_service_epochs().await.unwrap();
    let initial = maps.read_service_snapshot().await.unwrap();
    let routes = BindingCache::new();
    routes
        .replace(BindingTable::new(layout.storage_map().unwrap()))
        .unwrap();
    let store = ChunkStore::new(Arc::clone(&test.kv), routes);
    let id = ChunkId {
        high: 5 << 56,
        low: 90,
    };
    store
        .create_chunk_with_finalize_task(
            &Chunk {
                id: Some(id),
                state: ChunkState::Sealed as i32,
                chunk_type: 5,
                modify_ts: 1,
                ..Default::default()
            },
            &slot_groups::finalize(id),
        )
        .await
        .unwrap();
    let old = TestDynamicRuntime::start(Arc::clone(&test.kv), initial.authority(), 1).await;
    let new = TestDynamicRuntime::start(Arc::clone(&test.kv), initial.authority(), 2).await;
    let registry = ServiceRegistryClient::from_shared(Arc::clone(&test.kv));
    for (owner, runtime) in [(1, &old), (2, &new)] {
        registry
            .register_chunkdb(owner, &format!("127.0.0.1:{}", runtime.server.port()))
            .await
            .unwrap();
    }
    let client = ChunkdbClient::new(registry, Arc::new(ChunkdbRpcTransport::new()));
    client.refresh_routes().await.unwrap();
    let moved = initial
        .authority()
        .reassign(&[(ChunkSlot::for_chunk(&id), 2)])
        .unwrap();
    maps.publish_service_epochs(&moved).await.unwrap();
    old.guard.install_dynamic(&moved, 1).unwrap();
    new.guard.install_dynamic(&moved, 2).unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.delete_chunk(DeleteChunkRequest { chunk_id: Some(id) }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.chunk.unwrap().state, ChunkState::Deleted as i32);
    assert_eq!(
        store.get_chunk(&id).await.unwrap().state,
        ChunkState::Deleted as i32
    );
    old.finish().await;
    new.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupted_same_process_execution_retries_with_a_fresh_epoch_without_backoff() {
    use crowdb_chunkdb::lifecycle::{CacheHint, LockPolicy};
    let test = slot_groups::TestGroups::start().await;
    let maps = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let layout = ChunkSlotBootstrap {
        service_instances: vec![1],
        storage_groups: vec![ChunkStorageGroup {
            store_id: 0,
            group_id: 1,
        }],
    };
    maps.initialize_layout(&layout).await.unwrap();
    maps.initialize_service_epochs().await.unwrap();
    let initial = maps.read_service_snapshot().await.unwrap();
    let routes = BindingCache::new();
    routes
        .replace(BindingTable::new(layout.storage_map().unwrap()))
        .unwrap();
    let store = ChunkStore::new(Arc::clone(&test.kv), routes);
    let id = ChunkId {
        high: 5 << 56,
        low: 91,
    };
    store
        .create_chunk_with_finalize_task(
            &Chunk {
                id: Some(id),
                state: ChunkState::Sealed as i32,
                chunk_type: 5,
                modify_ts: 1,
                ..Default::default()
            },
            &slot_groups::finalize(id),
        )
        .await
        .unwrap();
    let runtime = TestDynamicRuntime::start(Arc::clone(&test.kv), initial.authority(), 1).await;
    let registry = ServiceRegistryClient::from_shared(Arc::clone(&test.kv));
    registry
        .register_chunkdb(1, &format!("127.0.0.1:{}", runtime.server.port()))
        .await
        .unwrap();
    let client = Arc::new(ChunkdbClient::with_retry_config(
        registry,
        crowdb_chunkdb_client::RetryConfig {
            max_retries: 0,
            initial_backoff: Duration::from_secs(30),
        },
        Arc::new(ChunkdbRpcTransport::new()),
    ));
    client.refresh_routes().await.unwrap();
    let held = runtime
        .locks
        .acquire_for_create(&id, &LockPolicy::default(), CacheHint::NoCache)
        .await
        .unwrap();
    let request = tokio::spawn(async move {
        client
            .delete_chunk(DeleteChunkRequest { chunk_id: Some(id) })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while runtime.locks.users_for_tests(&id) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let old = runtime.guard.capture();
    maps.regrant_service_epochs(1).await.unwrap();
    let regrant = maps.read_service_snapshot().await.unwrap();
    runtime.guard.install_dynamic(regrant.authority(), 1).unwrap();
    assert!(runtime.guard.check_submission(&id, &old).is_err());
    drop(held);
    let result = tokio::time::timeout(Duration::from_secs(2), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.chunk.unwrap().state, ChunkState::Deleted as i32);
    runtime.finish().await;
}

#[tokio::test]
async fn a_submitted_mutation_with_no_reply_is_not_blindly_replayed() {
    use crowdb_protocol::fb::FBMsgType;
    use std::sync::atomic::{AtomicUsize, Ordering};
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
    let server = crowdb_rpc_ffi::RpcServer::new(None);
    server.listen("127.0.0.1", 0).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    // The transport accepts the request but loses the response. Its outcome is
    // deliberately unknown, so replay could repeat an already completed effect.
    server.register_handler(FBMsgType::EDeleteChunkRequest.0 as u16, move |_request| {
        observed.fetch_add(1, Ordering::Relaxed);
    });
    server.start();
    let registry = ServiceRegistryClient::from_shared(Arc::clone(&test.kv));
    registry
        .register_chunkdb(1, &format!("127.0.0.1:{}", server.port()))
        .await
        .unwrap();
    let client = ChunkdbClient::new(registry, Arc::new(ChunkdbRpcTransport::new()));
    client.refresh_routes().await.unwrap();
    let error = client
        .delete_chunk(DeleteChunkRequest {
            chunk_id: Some(ChunkId {
                high: 5 << 56,
                low: 92,
            }),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            crowdb_chunkdb_client::ChunkdbClientError::OutcomeUnknown(_)
        ),
        "{error}"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    server.stop();
}
