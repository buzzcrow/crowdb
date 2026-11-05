// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Host clients of the normal three-node Console deployment surface.

use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient};
use crowdb_protocol::port::ports::WEB_BASE;
use reqwest::{Client, Method};
use serde_json::Value;
use std::{sync::Arc, time::Duration};

pub(super) struct TestNativeContext {
    pub(super) http: Client,
    pub(super) base: String,
    pub(super) kv: Arc<PaxosKvClient>,
    pub(super) addresses: Vec<String>,
    pub(super) management_seeds: Vec<String>,
}

impl TestNativeContext {
    pub(super) async fn connect() -> Self {
        let addresses = crate::common::management::addresses(3);
        // A restart includes the supervisor's stop grace and startup probe.
        let http = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap();
        let base = format!("http://{}:{WEB_BASE}", addresses[0]);
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
        let mut management_seeds = Vec::with_capacity(addresses.len());
        for node in 1..=addresses.len() {
            let server = crate::common::management::request(
                &http,
                &base,
                Method::GET,
                &format!("/api/nodes/{node}/server"),
                Value::Null,
                200,
            )
            .await;
            management_seeds.push(
                server["url"]
                    .as_str()
                    .expect("deployed KV management URL")
                    .to_owned(),
            );
        }
        let store = crate::common::management::request(
            &http,
            &base,
            Method::GET,
            &format!("/api/nodes/{leader}/stores/0"),
            Value::Null,
            200,
        )
        .await;
        let listener = store["listen_addr"]
            .as_str()
            .expect("live Group 0 listener")
            .parse::<std::net::SocketAddr>()
            .unwrap();
        let kv = Arc::new(PaxosKvClient::new(ClientConfig::new(management_seeds.clone())));
        kv.seed_leader(0, 0, format!("{}:{}", addresses[index], listener.port()));
        Self {
            http,
            base,
            kv,
            addresses,
            management_seeds,
        }
    }

    pub(super) async fn api(&self, method: Method, path: &str, body: Value, status: u16) -> Value {
        crate::common::management::request(&self.http, &self.base, method, path, body, status).await
    }
}
