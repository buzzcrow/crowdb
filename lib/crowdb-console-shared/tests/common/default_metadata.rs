// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::default_preparation::TestPreparationAuthority;
use crowdb_console_shared::deployment::preparation::{self, DefaultPreparation};
use crowdb_kv::{
    cluster::{KvServer, PxKvStore},
    common::config::CrowDBConfig,
};
use crowdb_kv_server::{
    mgmt::{self, operation_registry::AppState},
    store_registry::KvStoreRegistry,
};
use crowdb_protocol::common::KvServerIdentity;
use std::sync::Arc;

pub struct TestDefaultMetadata {
    pub authority: TestPreparationAuthority,
    pub accepted: DefaultPreparation,
    pub stores: Vec<Arc<PxKvStore>>,
    pub endpoints: Vec<String>,
    servers: Vec<tokio::task::JoinHandle<()>>,
    _root: crowdb_test_harness::test_dirs::TestDir,
}

impl TestDefaultMetadata {
    pub async fn start(members: &[u64]) -> Self {
        let authority = TestPreparationAuthority::start(members).await;
        let accepted = preparation::reserve(&authority.ctx, &authority.bootstrap.identity)
            .await
            .unwrap();
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("default-metadata-nodes");
        let mut stores = Vec::new();
        let mut servers = Vec::new();
        let mut endpoints = Vec::new();
        for (index, node) in members.iter().enumerate() {
            let path = root.path().join(format!("node-{node}"));
            let config = CrowDBConfig {
                config_root: path.join("config"),
                data_root: path.join("data"),
                wal_root: path.join("wal"),
                wal_backend: "file".into(),
                crowtree_backend: "file".into(),
                ..CrowDBConfig::for_tests()
            };
            let registry = Arc::new(KvStoreRegistry::with_config(config));
            let store = if index == 0 {
                Arc::clone(&authority.store)
            } else {
                let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
                store.start().await.unwrap();
                store
            };
            registry.add_store(0, &store);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", listener.local_addr().unwrap());
            let router = mgmt::router(AppState::new(registry));
            servers.push(tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            }));
            authority
                .ctx
                .sysmd()
                .register_kv_server(
                    KvServerIdentity {
                        instance_id: *node,
                        node_id: Some(*node),
                    },
                    &origin,
                    &[0],
                    &[],
                    "ok",
                    "",
                )
                .await
                .unwrap();
            endpoints.push(origin);
            stores.push(store);
        }
        Self {
            authority,
            accepted,
            stores,
            endpoints,
            servers,
            _root: root,
        }
    }

    pub async fn ensure(&self) -> crowdb_console_shared::error::Result<bool> {
        crowdb_console_shared::ops::kv_logical::ensure_default_group(&self.authority.ctx, &self.accepted)
            .await
    }
}

impl Drop for TestDefaultMetadata {
    fn drop(&mut self) {
        for server in &self.servers {
            server.abort();
        }
        for store in &self.stores {
            store.stop();
        }
    }
}
