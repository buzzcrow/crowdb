// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use crowdb_chunkdb::range_guard::RangeGuard;
use crowdb_chunkdb::routing::{BindingCache, BindingTable};
use crowdb_chunkdb::storage::ChunkStore;
use crowdb_chunkdb_client::ChunkdbRpcTransport;
use crowdb_protocol::chunk_domain::ChunkDomain;
use crowdb_protocol::chunk_slot::{ChunkSlotBootstrap, ChunkStorageGroup};
use crowdb_protocol::chunkdb::rpc::{ChunkState, DeleteChunkRequest, ListChunksRequest, QueryChunkRequest};
use crowdb_protocol::{port::alloc::alloc_test_port, ServicePort};

#[path = "common/runtime_context.rs"]
mod runtime_context;
#[path = "common/slot_groups.rs"]
mod slot_groups;
use slot_groups::TestGroups;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn production_runtime_dispatches_all_purposes_and_lists_only_owned_chunks() {
    let test = TestGroups::start().await;
    let layout = ChunkSlotBootstrap {
        service_instances: vec![1, 2],
        storage_groups: (1..=3)
            .map(|group_id| ChunkStorageGroup {
                store_id: 0,
                group_id,
            })
            .collect(),
    };
    let bindings = BindingCache::new();
    bindings
        .replace(BindingTable::new(layout.storage_map().unwrap()))
        .unwrap();
    let guard = Arc::new(RangeGuard::new());
    guard.install(&layout.service_map().unwrap(), 1).unwrap();
    let store = ChunkStore::new(Arc::clone(&test.kv), bindings.clone());
    let (expected, foreign) = runtime_context::seed_runtime_chunks(&store, &guard).await;
    let (context, stop) = runtime_context::runtime_context(Arc::clone(&test.kv), bindings, guard);
    let system = context.clone().start(ChunkDomain::System).await.unwrap();
    let user = context.start(ChunkDomain::UserData).await.unwrap();
    let service = Arc::new(user.rpc_service.with_system_runtime(Arc::new(system.rpc_service)));
    let server = Arc::new(crowdb_rpc_ffi::RpcServer::new(None));
    let port = alloc_test_port(ServicePort::ChunkdbRpc);
    server.listen("127.0.0.1", i32::from(port)).unwrap();
    service.register_handlers(&server);
    server.start();
    let endpoint = format!("http://127.0.0.1:{port}");
    let client = ChunkdbRpcTransport::new();
    let listed = client
        .send_list_chunks(
            &endpoint,
            &ListChunksRequest {
                max_keys: 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        listed
            .chunks
            .iter()
            .map(|chunk| chunk.id.unwrap())
            .collect::<Vec<_>>(),
        expected
    );
    for id in expected {
        let reply = client
            .send_query_chunk(&endpoint, &QueryChunkRequest { chunk_id: Some(id) })
            .await
            .unwrap();
        assert_eq!(reply.chunk.unwrap().id, Some(id));
        client
            .send_delete_chunk(&endpoint, &DeleteChunkRequest { chunk_id: Some(id) })
            .await
            .unwrap();
        assert_eq!(
            store.get_chunk(&id).await.unwrap().state,
            ChunkState::Deleted as i32
        );
    }
    for id in foreign {
        let error = client
            .send_query_chunk(&endpoint, &QueryChunkRequest { chunk_id: Some(id) })
            .await
            .unwrap_err();
        assert!(
            matches!(error, crowdb_chunkdb_client::ChunkdbClientError::NotMyRange(_)),
            "{error}"
        );
        assert_eq!(
            store.get_chunk(&id).await.unwrap().state,
            ChunkState::Sealed as i32
        );
    }
    server.stop();
    stop.send(true).unwrap();
    for worker in system.handles.into_iter().chain(user.handles) {
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn native_snapshots_route_by_service_slot_and_refresh_endpoints_without_storage_map() {
    use crowdb_chunkdb_client::ChunkdbClient;
    use crowdb_kv_client::{ChunkSlotMapClient, ServiceRegistryClient};
    use crowdb_protocol::chunk_slot::ChunkSlot;
    use crowdb_protocol::common::ChunkId;

    let test = TestGroups::start().await;
    let maps = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let services = ChunkSlotBootstrap {
        service_instances: vec![10, 20],
        storage_groups: Vec::new(),
    }
    .service_map()
    .unwrap();
    maps.initialize_service(&services).await.unwrap();
    assert!(maps.read_storage().await.is_err());
    let mut endpoints = Vec::new();
    for _ in 0..3 {
        let server = Arc::new(crowdb_rpc_ffi::RpcServer::new(None));
        server.listen("127.0.0.1", 0).unwrap();
        server.start();
        endpoints.push(server);
    }
    let registry = ServiceRegistryClient::from_shared(Arc::clone(&test.kv));
    registry
        .register_chunkdb(10, &format!("127.0.0.1:{}", endpoints[0].port()))
        .await
        .unwrap();
    registry
        .register_chunkdb(20, &format!("127.0.0.1:{}", endpoints[1].port()))
        .await
        .unwrap();
    let client = Arc::new(ChunkdbClient::new(
        registry.clone(),
        Arc::new(ChunkdbRpcTransport::new()),
    ));
    let native = client.native_routes().await.unwrap();
    let mut ids = Vec::new();
    for owner in [10, 20] {
        ids.push(
            (1..10_000)
                .map(|low| ChunkId { high: 2 << 56, low })
                .find(|id| services.owner(ChunkSlot::for_chunk(id)) == owner)
                .unwrap(),
        );
    }
    let old = native.resolve(Some(ids[0]), false).unwrap();
    let other = native.resolve(Some(ids[1]), false).unwrap();
    assert_ne!(old.raw_handles().2, other.raw_handles().2);
    registry
        .register_chunkdb(10, &format!("127.0.0.1:{}", endpoints[2].port()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if native
                .resolve(Some(ids[0]), true)
                .is_some_and(|route| route.raw_handles().2 != old.raw_handles().2)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        native.resolve(Some(ids[1]), false).unwrap().raw_handles().2,
        other.raw_handles().2
    );
    registry.unregister("chunkdb", 10).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while native.resolve(Some(ids[0]), true).is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        native.resolve(None, false).unwrap().raw_handles().2,
        other.raw_handles().2
    );
    drop(native);
    drop(old);
    drop(other);
    for endpoint in endpoints {
        endpoint.stop();
    }
}
