// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_paxos_kv::{
    cluster::{PaxosKvServer, PxKvStore},
    common::config::PaxosKvConfig,
};
use crowdb_paxos_kv_server::{
    mgmt::{self, operation_registry::AppState},
    store_registry::PaxosKvStoreRegistry,
};
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc};

pub struct TestGroupCreation {
    _root: crowdb_test_harness::test_dirs::TestDir,
    pub registry: Arc<PaxosKvStoreRegistry>,
    pub store: Arc<PxKvStore>,
    pub origin: String,
    server: tokio::task::JoinHandle<()>,
}

impl TestGroupCreation {
    pub async fn start(block_data: bool) -> Self {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("native-group-creation");
        let config = PaxosKvConfig {
            data_root: root.path().join("data"),
            wal_root: root.path().join("wal"),
            config_root: root.path().join("config"),
            crowtree_backend: "file".into(),
            wal_backend: "file".into(),
            ..PaxosKvConfig::for_tests()
        };
        if block_data {
            std::fs::write(&config.data_root, b"blocked").unwrap();
        }
        let registry = Arc::new(PaxosKvStoreRegistry::with_config(config));
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        store.start().await.unwrap();
        registry.add_store(0, &store);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let router = mgmt::router(AppState::new(Arc::clone(&registry)));
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            _root: root,
            registry,
            store,
            origin,
            server,
        }
    }

    pub fn ownership(&self) -> PathBuf {
        self.registry.config.config_root.join("group-creation-0-1.json")
    }

    pub async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&Value>,
    ) -> (reqwest::StatusCode, Vec<u8>) {
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let mut request = client.request(method, format!("{}{path}", self.origin));
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await.unwrap();
        let status = response.status();
        let bytes = response.bytes().await.unwrap().to_vec();
        (status, bytes)
    }

    pub async fn create(&self, body: &Value, expected: reqwest::StatusCode) {
        let (status, bytes) = self
            .request(reqwest::Method::POST, "/stores/0/groups", Some(body))
            .await;
        assert_eq!(status, expected, "{}", String::from_utf8_lossy(&bytes));
    }
}

impl Drop for TestGroupCreation {
    fn drop(&mut self) {
        self.server.abort();
        self.store.stop();
    }
}

pub fn request() -> Value {
    json!({"group_id":1,"replica_id":7,"initial_role":"leader","start_election":false,
        "deployment":{"cluster_id":uuid::Uuid::new_v4().to_string(),"operation_id":uuid::Uuid::new_v4().to_string()}})
}
