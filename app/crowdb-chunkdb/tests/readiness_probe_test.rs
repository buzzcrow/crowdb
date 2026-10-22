// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{sync::Arc, time::Duration};

use crowdb_chunkdb::{
    range_guard::RangeGuard,
    routing::{BindingCache, BindingTable},
    storage::ChunkStore,
};
use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbClientError, ChunkdbRpcTransport};
use crowdb_kv::cluster::kv_server::KvServer;
use crowdb_kv_client::{ChunkSlotMapClient, ClientConfig, CrowdbKvClient, ServiceRegistryClient};
use crowdb_protocol::{
    chunk_domain::ChunkDomain,
    chunk_id::{ChunkIdParts, CHUNK_TYPE_S3},
    chunk_slot::{ChunkSlotBootstrap, ChunkStorageGroup},
    chunkdb::rpc::{Chunk, ChunkState},
    common::ChunkId,
};

#[path = "common/runtime_context.rs"]
#[allow(dead_code)]
mod runtime_context;
#[path = "common/slot_groups.rs"]
mod slot_groups;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn readiness_reads_both_domains_without_scanning_or_mutating_the_catalog() {
    let test = slot_groups::TestGroups::start().await;
    let layout = ChunkSlotBootstrap {
        service_instances: vec![1],
        storage_groups: vec![ChunkStorageGroup {
            store_id: 0,
            group_id: 1,
        }],
    };
    ChunkSlotMapClient::new(Arc::clone(&test.kv))
        .initialize_service(&layout.service_map().unwrap())
        .await
        .unwrap();
    let bindings = BindingCache::new();
    bindings
        .replace(BindingTable::new(layout.storage_map().unwrap()))
        .unwrap();
    let guard = Arc::new(RangeGuard::new());
    guard.install(&layout.service_map().unwrap(), 1).unwrap();
    let store = ChunkStore::new(Arc::clone(&test.kv), bindings.clone());
    let (context, stop) = runtime_context::runtime_context(Arc::clone(&test.kv), bindings, guard);
    let system = context.clone().start(ChunkDomain::System).await.unwrap();
    let user = context.start(ChunkDomain::UserData).await.unwrap();
    let service = Arc::new(user.rpc_service.with_system_runtime(Arc::new(system.rpc_service)));
    let server = Arc::new(crowdb_rpc_ffi::RpcServer::new(None));
    server.listen("127.0.0.1", 0).unwrap();
    service.register_handlers(&server);
    server.start();
    stop.send(true).unwrap();
    for worker in system.handles.into_iter().chain(user.handles) {
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap();
    }
    let control = Arc::new(CrowdbKvClient::new(ClientConfig::new(Vec::new())));
    control.seed_leader(0, 0, test.server.listen_addr().unwrap().to_string());
    let registry = ServiceRegistryClient::from_shared(control);
    registry
        .heartbeat_chunkdb(1, &format!("127.0.0.1:{}", server.port()))
        .await
        .unwrap();
    let client = ChunkdbClient::new(registry, Arc::new(ChunkdbRpcTransport::new()));
    client.refresh_routes().await.unwrap();

    for records in [0, 320] {
        seed_catalog(&store, records).await;
        let _ = test.kv.drain_window();
        client.observe_readiness().await.unwrap();
        let work = test.kv.drain_window();
        assert_eq!(work.get.len(), 2, "one point read for each domain");
        assert_eq!(work.scan.len(), 0, "readiness must not enumerate catalog records");
        assert_eq!(work.put.len() + work.batch_write.len() + work.delete.len(), 0);
    }
    // Storage corruption must remain an unhealthy result, rather than being
    // confused with an authoritative absent key in an empty catalog.
    let mut key = b"/chunk/".to_vec();
    key.extend_from_slice(
        &ChunkIdParts {
            high: u64::from(CHUNK_TYPE_S3) << 56,
            low: 0,
        }
        .to_bytes(),
    );
    test.kv
        .put(0, 1, &key, b"corrupt chunk record", None)
        .await
        .unwrap();
    assert!(matches!(
        client.observe_readiness().await,
        Err(ChunkdbClientError::Internal(_))
    ));
    server.stop();
}

async fn seed_catalog(store: &ChunkStore, count: u64) {
    for low in 1..=count {
        let id = ChunkId {
            high: u64::from(CHUNK_TYPE_S3) << 56,
            low,
        };
        let mut task = slot_groups::finalize(id);
        task.eligible_at_ms = u64::MAX;
        store
            .create_chunk_with_finalize_task(
                &Chunk {
                    id: Some(id),
                    modify_ts: 1,
                    chunk_type: i32::from(CHUNK_TYPE_S3),
                    state: ChunkState::Sealed as i32,
                    ..Default::default()
                },
                &task,
            )
            .await
            .unwrap();
    }
}
