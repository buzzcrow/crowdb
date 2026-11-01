// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{
    body::{to_bytes, Body},
    http::Request,
};
use crowdb_console_shared::{
    cluster::NodeHealth,
    config::{NodeEntry, ServerEntry, ServiceType},
    ConsoleConfig,
};
use crowdb_protocol::mgmt::{
    node::{NodeBinding, NodeServiceAction, NodeServiceIntent},
    SystemBootstrapIdentity,
};
use crowdb_web::{AppState, ServiceObservationSourceForTests, ServiceObserver};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;
use tower::ServiceExt;

pub struct TestSource {
    pub intents: arc_swap::ArcSwap<Vec<NodeServiceIntent>>,
    pub fail_authority: AtomicBool,
    pub hold_authority: AtomicBool,
    pub authority_entered: Notify,
    pub authority_release: Notify,
    pub authority_finished: Notify,
    pub hold_node: AtomicU32,
    pub process_entered: Notify,
    pub process_release: Notify,
    pub process_finished: Notify,
    pub hold_health: AtomicBool,
    pub health_entered: Notify,
    pub health_release: Notify,
    pub pid: AtomicU32,
    pub wrong_bootstrap: AtomicBool,
    pub wrong_operation: AtomicBool,
    pub exhausted: AtomicBool,
    pub calls: AtomicUsize,
}

#[async_trait::async_trait]
impl ServiceObservationSourceForTests for TestSource {
    async fn intents(&self, _: &AppState) -> Result<Vec<NodeServiceIntent>, String> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.fail_authority.load(Ordering::Relaxed) {
            return Err("fixture authority unavailable".into());
        }
        let result = self.intents.load_full().as_ref().clone();
        if self.hold_authority.load(Ordering::Relaxed) {
            self.authority_entered.notify_one();
            self.authority_release.notified().await;
            self.authority_finished.notify_one();
        }
        Ok(result)
    }

    async fn processes(
        &self,
        _: &AppState,
        binding: &NodeBinding,
        node: &NodeEntry,
    ) -> Result<Value, String> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let intent = self
            .intents
            .load_full()
            .iter()
            .find(|intent| intent.node_id == node.id)
            .unwrap()
            .clone();
        let pid = self.pid.load(Ordering::Relaxed) + u32::try_from(node.id).unwrap();
        let wrong_operation = self.wrong_operation.load(Ordering::Relaxed);
        let mut bootstrap = binding.bootstrap.clone();
        if self.wrong_bootstrap.load(Ordering::Relaxed) {
            bootstrap.operation_id = "foreign-bootstrap".into();
        }
        if self.hold_node.load(Ordering::Relaxed) == u32::try_from(node.id).unwrap() {
            self.process_entered.notify_one();
            self.process_release.notified().await;
            self.process_finished.notify_one();
        }
        let operation = if wrong_operation {
            "old-operation"
        } else {
            &intent.operation_id
        };
        Ok(
            json!({"node_id": node.id, "cluster_id": binding.bootstrap.cluster_id, "bootstrap": bootstrap,
            "processes": if self.pid.load(Ordering::Relaxed) == 0 { json!({}) } else { json!({intent.service_id.clone(): pid}) },
            "process_operations": {intent.service_id.clone(): operation},
            "service_failures": {intent.service_id: {"operation_id":operation, "exhausted":self.exhausted.load(Ordering::Relaxed)}} }),
        )
    }

    async fn health(
        &self,
        _: &AppState,
        _: &ConsoleConfig,
        entry: &ServerEntry,
        _: Option<u32>,
        _: &[NodeServiceIntent],
    ) -> NodeHealth {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.hold_health.load(Ordering::Relaxed) && entry.id == "chunkdb-sibling" {
            self.health_entered.notify_one();
            self.health_release.notified().await;
        }
        NodeHealth::Up
    }
}

pub struct TestServiceInventory {
    pub state: AppState,
    pub source: Arc<TestSource>,
    pub observer: Option<ServiceObserver>,
    pub root: crowdb_test_harness::test_dirs::TestDir,
    app: axum::Router,
}

impl TestServiceInventory {
    pub fn new(access: bool) -> Self {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("service-inventory");
        let binding = NodeBinding {
            node_id: 1,
            bootstrap: SystemBootstrapIdentity {
                cluster_id: "inventory-cluster".into(),
                operation_id: "bootstrap".into(),
                configuration_digest: "a".repeat(64),
            },
            management_seeds: vec!["http://127.0.0.1:1".into()],
        };
        std::fs::write(
            root.path().join("node-binding.json"),
            serde_json::to_vec(&binding).unwrap(),
        )
        .unwrap();
        let mut config = ConsoleConfig::default();
        let mut intents = Vec::new();
        for node_id in [1, 2] {
            config.nodes.push(NodeEntry {
                id: node_id,
                rack_id: 1,
                host: format!("127.0.0.{node_id}"),
                ssh_port: 22,
                ssh_user: "fixture".into(),
                ssh_credential_ref: None,
                ssh_key: None,
                ssh_password: None,
            });
            let id = format!("{}-{node_id}", if access { "access" } else { "chunkdb" });
            let mut entry = ServerEntry::new(&id, format!("http://127.0.0.{node_id}:7300"));
            entry.node_id = Some(node_id);
            entry.service_type = if access {
                ServiceType::AccessServer
            } else {
                ServiceType::Chunkdb
            };
            entry.auto_start = true;
            entry.rest_port = Some(7300);
            if access {
                entry.access_origins.insert("iceberg".into(), entry.url.clone());
            } else {
                entry.rpc_url = Some(format!("127.0.0.{node_id}:7400"));
                entry.rpc_port = Some(7400);
            }
            config.servers.push(entry);
            intents.push(NodeServiceIntent { cluster_id: binding.bootstrap.cluster_id.clone(),
                operation_id: format!("operation-{node_id}"), node_id, service_id: id, kind: if access { "access" } else { "chunkdb" }.into(), action: NodeServiceAction::Start,
                configuration: if access { format!("[iceberg]\nlisten = '127.0.0.{node_id}:7300'\n[health]\nlisten = '127.0.0.{node_id}:7500'\n") }
                    else { format!("[server]\nhttp_listen_addr = '127.0.0.{node_id}:7300'\nrpc_listen_addr = '127.0.0.{node_id}:7400'\n") },
                environment: BTreeMap::default() });
        }
        let state = AppState::with_runtime_root(config, root.path().join("console"))
            .with_node_monitor("http://127.0.0.1:1")
            .unwrap();
        let app = crowdb_web::router(state.clone());
        let source = Arc::new(TestSource {
            intents: arc_swap::ArcSwap::from_pointee(intents),
            fail_authority: AtomicBool::new(false),
            hold_authority: AtomicBool::new(false),
            authority_entered: Notify::new(),
            authority_release: Notify::new(),
            authority_finished: Notify::new(),
            hold_node: AtomicU32::new(0),
            process_entered: Notify::new(),
            process_release: Notify::new(),
            process_finished: Notify::new(),
            hold_health: AtomicBool::new(false),
            health_entered: Notify::new(),
            health_release: Notify::new(),
            pid: AtomicU32::new(100),
            wrong_bootstrap: AtomicBool::new(false),
            wrong_operation: AtomicBool::new(false),
            exhausted: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        });
        Self {
            state,
            source,
            observer: None,
            root,
            app,
        }
    }

    pub fn start(&mut self) {
        self.observer =
            Some(crowdb_web::start_service_observer_for_tests(&self.state, self.source.clone()).unwrap());
    }

    pub async fn rows(&self) -> Value {
        let response = tokio::time::timeout(
            Duration::from_millis(200),
            self.app.clone().oneshot(
                Request::builder()
                    .uri("/api/servers")
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .await
        .expect("inventory GET waited for authority, SSH, or endpoint work")
        .unwrap();
        assert!(response.status().is_success());
        serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap()).unwrap()
    }

    pub async fn wait_pid(&self, node: u64, pid: Option<u32>) -> Value {
        let id = self
            .source
            .intents
            .load_full()
            .iter()
            .find(|intent| intent.node_id == node)
            .map(|intent| intent.service_id.clone());
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let rows = self.rows().await;
                let row = rows
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|row| row["node_id"] == node && id.as_ref().map_or(true, |id| row["id"] == *id))
                    .unwrap();
                if row["pid"].as_u64() == pid.map(u64::from) {
                    return row.clone();
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("sampler did not publish the expected monitor PID")
    }

    pub async fn signalled(signal: &Notify) {
        tokio::time::timeout(Duration::from_secs(2), signal.notified())
            .await
            .expect("observation source did not reach its deterministic barrier");
    }
}
