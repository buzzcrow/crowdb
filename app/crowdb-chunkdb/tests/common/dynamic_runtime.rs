// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb::{
    lifecycle::ChunkLockMap,
    range_guard::RangeGuard,
    routing::{BindingCache, BindingTable},
};
use crowdb_kv_client::CrowdbKvClient;
use crowdb_protocol::{
    chunk_domain::ChunkDomain,
    chunk_slot::{ChunkSlotAuthority, ChunkSlotBootstrap, ChunkSlotMap, ChunkStorageGroup},
};
use std::sync::Arc;
use std::time::Duration;

pub struct TestDynamicRuntime {
    pub server: Arc<crowdb_rpc_ffi::RpcServer>,
    pub guard: Arc<RangeGuard>,
    pub locks: Arc<ChunkLockMap>,
    stop: tokio::sync::watch::Sender<bool>,
    workers: Vec<tokio::task::JoinHandle<()>>,
}
impl TestDynamicRuntime {
    pub async fn start(kv: Arc<CrowdbKvClient>, map: &ChunkSlotMap<ChunkSlotAuthority>, owner: u64) -> Self {
        let routes = BindingCache::new();
        routes
            .replace(BindingTable::new(
                ChunkSlotBootstrap {
                    service_instances: vec![1],
                    storage_groups: vec![ChunkStorageGroup {
                        store_id: 0,
                        group_id: 1,
                    }],
                }
                .storage_map()
                .unwrap(),
            ))
            .unwrap();
        let guard = Arc::new(RangeGuard::new());
        guard.install_dynamic(map, owner).unwrap();
        let (mut context, stop) = super::runtime_context::runtime_context(kv, routes, Arc::clone(&guard));
        context.config.server.instance_id = Some(owner.to_string());
        let locks = Arc::clone(&context.lock_map);
        let system = context.clone().start(ChunkDomain::System).await.unwrap();
        let user = context.start(ChunkDomain::UserData).await.unwrap();
        let service = Arc::new(user.rpc_service.with_system_runtime(Arc::new(system.rpc_service)));
        let server = Arc::new(crowdb_rpc_ffi::RpcServer::new(None));
        server.listen("127.0.0.1", 0).unwrap();
        service.register_handlers(&server);
        server.start();
        Self {
            server,
            guard,
            locks,
            stop,
            workers: system.handles.into_iter().chain(user.handles).collect(),
        }
    }
    pub async fn finish(self) {
        self.server.stop();
        self.stop.send(true).unwrap();
        for worker in self.workers {
            tokio::time::timeout(Duration::from_secs(5), worker)
                .await
                .unwrap()
                .unwrap();
        }
    }
}
