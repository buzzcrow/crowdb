// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::fixture::{TestServiceInventory, TestSource};
use crowdb_console_shared::{
    cluster::NodeHealth,
    config::{NodeEntry, ServerEntry, ServiceType},
    ConsoleConfig,
};
use crowdb_protocol::mgmt::node::{NodeBinding, NodeServiceIntent};
use crowdb_web::{AppState, ServiceObservationSourceForTests};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

pub const RUNNING: u32 = 0;
pub const ABSENT: u32 = 1;
pub const READ_ERROR: u32 = 2;
pub const FOREIGN_BOOTSTRAP: u32 = 3;
pub const FOREIGN_OPERATION: u32 = 4;
pub const EXHAUSTED: u32 = 5;
pub const FOREIGN_EXHAUSTION: u32 = 6;
pub const MISSING_MAP: u32 = 7;
pub const NULL_MAP: u32 = 8;
pub const ARRAY_MAP: u32 = 9;
pub const ZERO_PID: u32 = 10;
pub const STRING_PID: u32 = 11;
pub const FOREIGN_NODE: u32 = 12;
pub const FOREIGN_CLUSTER: u32 = 13;

struct TestProcessStateSource {
    base: Arc<TestSource>,
    mode: Arc<AtomicU32>,
    bootstrap_pkv: bool,
}

#[async_trait::async_trait]
impl ServiceObservationSourceForTests for TestProcessStateSource {
    async fn intents(&self, _: &AppState) -> Result<Vec<NodeServiceIntent>, String> {
        Ok(self
            .base
            .intents
            .load_full()
            .iter()
            .filter(|intent| !(self.bootstrap_pkv && intent.node_id == 1))
            .cloned()
            .collect())
    }

    async fn processes(
        &self,
        _: &AppState,
        binding: &NodeBinding,
        node: &NodeEntry,
    ) -> Result<Value, String> {
        let mode = self.mode.load(Ordering::Relaxed);
        if mode == READ_ERROR {
            return Err("controlled authenticated read unavailable".into());
        }
        let intent = self
            .base
            .intents
            .load_full()
            .iter()
            .find(|i| i.node_id == node.id)
            .unwrap()
            .clone();
        let key = if self.bootstrap_pkv && node.id == 1 {
            "kv".to_owned()
        } else {
            intent.service_id.clone()
        };
        let mut bootstrap = binding.bootstrap.clone();
        if mode == FOREIGN_BOOTSTRAP {
            bootstrap.operation_id = "foreign-bootstrap".into();
        }
        let absent = [ABSENT, EXHAUSTED, FOREIGN_EXHAUSTION].contains(&mode);
        let operation = if [FOREIGN_OPERATION, FOREIGN_EXHAUSTION].contains(&mode) {
            "foreign-operation"
        } else {
            &intent.operation_id
        };
        let mut value = json!({
            "cluster_id": binding.bootstrap.cluster_id, "node_id": node.id, "bootstrap": bootstrap,
            "processes": if absent { json!({}) } else { json!({key.clone(): 100 + node.id}) },
            // Production monitor exposes execution ownership only for live children.
            "process_operations": if absent || (self.bootstrap_pkv && node.id == 1) { json!({}) } else { json!({key.clone(): operation}) },
            "service_failures": if [EXHAUSTED, FOREIGN_EXHAUSTION].contains(&mode) {
                json!({intent.service_id: {"operation_id": operation, "exhausted": true}})
            } else { json!({}) },
        });
        match mode {
            MISSING_MAP => {
                value.as_object_mut().unwrap().remove("processes");
            }
            NULL_MAP => value["processes"] = Value::Null,
            ARRAY_MAP => value["processes"] = json!([]),
            ZERO_PID => value["processes"][&key] = json!(0),
            STRING_PID => value["processes"][&key] = json!("not-a-pid"),
            FOREIGN_NODE => value["node_id"] = json!(99),
            FOREIGN_CLUSTER => value["cluster_id"] = json!("foreign-cluster"),
            _ => {}
        }
        Ok(value)
    }

    async fn health(
        &self,
        _: &AppState,
        _: &ConsoleConfig,
        _: &ServerEntry,
        _: Option<u32>,
        _: &[NodeServiceIntent],
    ) -> NodeHealth {
        NodeHealth::Up
    }
}

pub fn start(fixture: &mut TestServiceInventory, bootstrap_pkv: bool) -> Arc<AtomicU32> {
    if bootstrap_pkv {
        let mut config = fixture.state.config.write().unwrap();
        let entry = config.servers.iter_mut().find(|s| s.node_id == Some(1)).unwrap();
        entry.id = "kv-1".into();
        entry.service_type = ServiceType::PaxosKv;
        entry.url = "http://127.0.0.1:10000".into();
        entry.rpc_url = Some("127.0.0.1:10100".into());
        entry.rest_port = Some(10000);
        entry.rpc_port = Some(10100);
    }
    let mode = Arc::new(AtomicU32::new(RUNNING));
    fixture.observer = Some(
        crowdb_web::start_service_observer_for_tests(
            &fixture.state,
            Arc::new(TestProcessStateSource {
                base: fixture.source.clone(),
                mode: mode.clone(),
                bootstrap_pkv,
            }),
        )
        .unwrap(),
    );
    mode
}

pub async fn wait(fixture: &TestServiceInventory, expected: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let rows = fixture.rows().await;
            let row = rows
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["node_id"] == 1)
                .unwrap();
            let observed = row["process_observed"].as_bool();
            let matches = match expected {
                "running" => observed == Some(true) && row["pid"].as_u64().is_some() && row["health"] == "up",
                "stopped" => observed == Some(true) && row["pid"].is_null(),
                "unknown" => observed == Some(false) && row["pid"].is_null(),
                _ => panic!("unexpected process evidence state"),
            };
            if matches {
                return row.clone();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("production observation did not distinguish validity from PID presence")
}
