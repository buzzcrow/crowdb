// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Host clients of the normal three-node Console deployment surface.

use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient};
use reqwest::{Client, Method};
use serde_json::Value;
use std::{sync::Arc, time::Duration};

pub(super) struct TestNativeContext {
    pub(super) http: Client,
    pub(super) base: String,
    pub(super) kv: Arc<PaxosKvClient>,
    pub(super) addresses: Vec<String>,
}

impl TestNativeContext {
    pub(super) async fn connect() -> Self {
        let addresses = crate::common::management::addresses(3);
        let http = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap();
        let base = format!("http://{}:9090", addresses[0]);
        let group = crate::common::management::request(
            &http,
            &base,
            Method::GET,
            "/api/stores/0/groups/0",
            Value::Null,
            200,
        )
        .await;
        let leader = group["replicas"]
            .as_array()
            .unwrap()
            .iter()
            .find(|replica| replica["role"] == "leader")
            .expect("live Group 0 leader")["node_id"]
            .as_u64()
            .unwrap();
        let index = usize::try_from(leader.checked_sub(1).unwrap()).unwrap();
        assert!(index < addresses.len());
        let kv = Arc::new(PaxosKvClient::new(ClientConfig::new(
            addresses
                .iter()
                .map(|address| format!("http://{address}:10000"))
                .collect(),
        )));
        kv.seed_leader(0, 0, format!("{}:10100", addresses[index]));
        Self {
            http,
            base,
            kv,
            addresses,
        }
    }

    pub(super) async fn api(&self, method: Method, path: &str, body: Value, status: u16) -> Value {
        crate::common::management::request(&self.http, &self.base, method, path, body, status).await
    }
}
