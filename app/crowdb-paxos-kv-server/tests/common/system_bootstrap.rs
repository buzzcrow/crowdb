// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_paxos_kv::common::config::PaxosKvConfig;
use crowdb_paxos_kv_server::{
    mgmt::{self, operation_registry::AppState},
    store_registry::PaxosKvStoreRegistry,
};
use crowdb_protocol::mgmt::{SystemBootstrapIdentity, SystemPrepareRequest};
use reqwest::StatusCode;
use serde_json::Value;
use std::{path::PathBuf, sync::Arc, time::Duration};

pub struct TestSystemBootstrap {
    _root: crowdb_test_harness::test_dirs::TestDir,
    pub registry: Arc<PaxosKvStoreRegistry>,
    pub origin: String,
    server: tokio::task::JoinHandle<()>,
}

impl TestSystemBootstrap {
    pub async fn start() -> Self {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("system-prepare");
        let config = PaxosKvConfig {
            data_root: root.path().join("data"),
            wal_root: root.path().join("wal"),
            config_root: root.path().join("config"),
            ..PaxosKvConfig::for_tests()
        };
        let registry = Arc::new(PaxosKvStoreRegistry::with_config(config));
        let (origin, server) = Self::serve(&registry).await;
        Self {
            _root: root,
            registry,
            origin,
            server,
        }
    }

    async fn serve(registry: &Arc<PaxosKvStoreRegistry>) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let router = mgmt::router(AppState::new(registry.clone()));
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (origin, server)
    }

    pub async fn restart(&mut self) {
        self.server.abort();
        self.registry = Arc::new(PaxosKvStoreRegistry::with_config(self.registry.config.clone()));
        (self.origin, self.server) = Self::serve(&self.registry).await;
    }

    pub fn ownership(&self) -> PathBuf {
        self.registry.config.config_root.join("system-bootstrap.json")
    }

    pub async fn request(&self, path: &str, body: &Value) -> StatusCode {
        let response = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap()
            .post(format!("{}{path}", self.origin))
            .json(body)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.bytes().await.unwrap();
        assert!(
            matches!(status, StatusCode::OK | StatusCode::CONFLICT),
            "{path}: {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
        status
    }

    pub async fn prepare(&self, replica_id: u64, bootstrap: &SystemBootstrapIdentity) -> StatusCode {
        self.request(
            "/system/prepare",
            &serde_json::to_value(SystemPrepareRequest {
                replica_id,
                bootstrap: bootstrap.clone(),
            })
            .unwrap(),
        )
        .await
    }

    pub fn assert_no_execution(&self) {
        assert_eq!(self.registry.store_count(), 0);
        assert!(!self.registry.config.data_root.exists());
        assert!(!self.registry.config.wal_root.exists());
    }
}

impl Drop for TestSystemBootstrap {
    fn drop(&mut self) {
        self.server.abort();
    }
}

pub fn identity() -> SystemBootstrapIdentity {
    SystemBootstrapIdentity {
        cluster_id: uuid::Uuid::new_v4().to_string(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        configuration_digest: "a".repeat(64),
    }
}

pub async fn proposal(
    nodes: &[TestSystemBootstrap],
    selected: [usize; 3],
    identity: &SystemBootstrapIdentity,
) -> bool {
    let (first, second, third) = tokio::join!(
        nodes[selected[0]].prepare(1, identity),
        nodes[selected[1]].prepare(2, identity),
        nodes[selected[2]].prepare(3, identity)
    );
    [first, second, third]
        .iter()
        .all(|status| *status == StatusCode::OK)
}
